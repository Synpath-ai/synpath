"""Polymarket (global) combo RFQs, offline (P7 of docs/rfq-combo.md).

Responses are the shapes in the venue's docs (/trading/combos/requesters,
/trading/combos/market-makers, /api-spec/combos-rfq-openapi.yaml), saved as
`tests/samples/polymarket_rfq_*.json` with test ids and addresses. The docs'
example deadline (March 2026) would read as expired today, so the samples
carry 2100-01-01 and the expiry tests move it. Nothing was captured live.

Covered: combos (provisional and venue ids, leg resolution), every
requester and quoter method, side and price mapping, the worst-price
refusal before signing, the EOA refusal, error mapping, the synthetic RFQ
orders and fills, the quoter WebSocket, and one engine end-to-end run
through `engine.rfq` with mocked HTTP.
"""
from __future__ import annotations

import asyncio
import copy
import json
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx
from eth_account import Account as EthAccount
from eth_account.messages import encode_typed_data

from synpath.errors import AuthenticationError, ExchangeNotAvailable, NotSupported, RateLimitExceeded
from synpath.trading import polymarket_rfq as prfq
from synpath.trading import polymarket_v3_signing as v3
from synpath.trading.credentials import PolymarketCredentials
from synpath.trading.errors import InsufficientFunds, InvalidOrder, OrderNotFound, OrderRejected, RfqNotFound
from synpath.trading.limiter import BudgetLimiter
from synpath.trading.polymarket import PolymarketTrading
from synpath.trading.types import (
    AcceptQuote, ComboLeg, MakerQuote, OrderStatus, OrderType, QuoteStatus, RfqCloseReason, RfqStatus, Side,
)

D = Decimal
pytestmark = pytest.mark.anyio

SAMPLES = Path(__file__).parent / "samples"
KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ADDRESS = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
WALLET = "0x1111111111111111111111111111111111111111"
SECRET = "YSB0ZXN0IHNlY3JldCB0aGF0IGlzIG5vdCByZWFsISE="
REQ = prfq.REQUESTER_URL + prfq.REQUESTS_PATH
API = prfq.RFQ_API_URL
GAMMA = "https://gamma-api.polymarket.com"
COND = "0x03" + "ab" * 31
COMBO_ID = f"polymarket:combo-{COND}"
LEGS = ["101", "202"]
PROVISIONAL = "polymarket:combo-legs-65.ca"
BUY_ID = "rfq_0f1e2d3c4b5a69788796a5b4c3d2e1f0"
BUY_QUOTE = "quote_aaaabbbbccccddddeeeeffff00001111"


def sample(name: str) -> dict:
    return json.loads((SAMPLES / f"polymarket_rfq_{name}.json").read_text())


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def mock():
    with respx.mock(assert_all_called=False) as router:
        yield router


def make(signature_type: int = 3, **kw) -> PolymarketTrading:
    creds = PolymarketCredentials(
        private_key=KEY, signature_type=signature_type, funder=None if signature_type == 0 else WALLET,
        api_key="api-key-uuid", api_secret=SECRET, api_passphrase="pass", **kw)
    poly = PolymarketTrading(creds, client=httpx.AsyncClient(),
                             limiter=BudgetLimiter(read_per_second=1000, write_per_second=1000))
    poly.remember_combo_market(sample("combo_markets")["markets"][0])
    poly.remember_combo_market(sample("combo_markets")["markets"][1])
    return poly


@pytest.fixture
async def poly():
    adapter = make()
    yield adapter
    await adapter.close()


def legs() -> list[ComboLeg]:
    return [ComboLeg(market_id="polymarket:1897034", outcome="yes"),
            ComboLeg(market_id="polymarket:1897035", outcome="no")]


def body_of(route, n: int = -1) -> dict:
    return json.loads(route.calls[n].request.content)


def check_l2(request: httpx.Request) -> None:
    import base64
    import hashlib
    import hmac

    stamp = request.headers["POLY_TIMESTAMP"]
    body = request.content.decode() if request.content else ""
    expected = base64.urlsafe_b64encode(hmac.new(
        base64.urlsafe_b64decode(SECRET), f"{stamp}{request.method}{request.url.path}{body}".encode(),
        hashlib.sha256).digest()).decode()
    assert request.headers["POLY_SIGNATURE"] == expected
    assert request.headers["POLY_ADDRESS"] == ADDRESS
    assert request.headers["POLY_API_KEY"] == "api-key-uuid" and request.headers["POLY_PASSPHRASE"] == "pass"


async def created_buy(poly, mock, response: dict | None = None):
    route = mock.post(REQ).mock(return_value=httpx.Response(200, json=response or sample("create_buy")))
    rfq = await poly.create_rfq(PROVISIONAL, amount=D("1"), outcome="yes", action="buy")
    return rfq, route


async def created_sell(poly, mock):
    mock.post(REQ).mock(return_value=httpx.Response(200, json=sample("create_sell")))
    return await poly.create_rfq(PROVISIONAL, quantity=D("2"), outcome="yes", action="sell")


def accept(quote_id: str = BUY_QUOTE, rfq_id: str = BUY_ID, **kw) -> AcceptQuote:
    base = dict(quote_id=quote_id, rfq_id=rfq_id, outcome="yes", action="buy")
    return AcceptQuote(**{**base, **kw})


# -- capability keys ---------------------------------------------------------------


def test_capability_keys():
    for key in ("rfq_taker", "rfq_quoter", "combos"):
        assert PolymarketTrading.has[key] is True
    assert PolymarketTrading.has["rfq"] is False


# -- combos ------------------------------------------------------------------------


class TestCombos:
    async def test_create_combo_is_offline_and_provisional(self, poly, mock):
        combo = await poly.create_combo(legs())
        assert combo.market_id == PROVISIONAL and combo.info["provisional"] is True
        assert combo.info["leg_position_ids"] == LEGS
        assert [(l.market_id, l.outcome, l.info["position_id"]) for l in combo.legs] == [
            ("polymarket:1897034", "yes", "101"), ("polymarket:1897035", "no", "202")]
        assert not mock.calls, "nothing is sent: the venue has no combo create"
        assert prfq.legs_of_provisional(combo.market_id) == LEGS

    async def test_after_an_rfq_the_combo_has_its_venue_id(self, poly, mock):
        rfq, _ = await created_buy(poly, mock)
        assert rfq.combo_id == COMBO_ID
        assert rfq.combo.info["yes_position_id"] == "789" and rfq.combo.info["no_position_id"] == "790"
        assert (await poly.create_combo(legs())).market_id == COMBO_ID
        assert (await poly.fetch_combo(COMBO_ID)).market_id == COMBO_ID
        assert (await poly.fetch_combo(PROVISIONAL)).market_id == COMBO_ID

    async def test_legs_checked(self, poly):
        with pytest.raises(InvalidOrder, match="2 to 50 legs"):
            await poly.create_combo(legs()[:1])
        with pytest.raises(InvalidOrder, match="each market once"):
            await poly.create_combo([legs()[0], legs()[0].model_copy(update={"outcome": "no"})])
        with pytest.raises(InvalidOrder, match="each leg once"):
            prfq.sorted_legs(["1", "01"])

    async def test_leg_from_its_given_position_id(self, poly):
        combo = await poly.create_combo([
            ComboLeg(market_id="polymarket:9", outcome="yes", info={"position_id": "5"}),
            ComboLeg(market_id="polymarket:8", outcome="no", info={"position_id": "3"})])
        assert combo.info["leg_position_ids"] == ["3", "5"]

    async def test_leg_from_gamma_position_ids(self, poly, mock):
        route = mock.get(f"{GAMMA}/markets/77").mock(return_value=httpx.Response(
            200, json={"id": "77", "positionIds": ["771", "772"]}))
        combo = await poly.create_combo([ComboLeg(market_id="polymarket:77", outcome="no"), legs()[0]])
        assert route.called and combo.info["leg_position_ids"] == ["101", "772"]

    async def test_unknown_leg(self, poly, mock):
        mock.get(f"{GAMMA}/markets/66").mock(return_value=httpx.Response(200, json={"id": "66"}))
        with pytest.raises(InvalidOrder, match="no combo position id known"):
            await poly.create_combo([ComboLeg(market_id="polymarket:66", outcome="yes"), legs()[0]])

    async def test_catalogue(self, mock):
        poly = make()
        poly._positions.clear()
        poly._position_of.clear()
        route = mock.get(f"{API}/v1/rfq/combo-markets").mock(return_value=httpx.Response(200, json=sample("combo_markets")))
        page = await poly.fetch_combo_markets(limit=50, exclude=["0x1", "0x2"])
        assert len(page) == 2 and page.next_cursor == "Mg"
        assert dict(route.calls[0].request.url.params) == {"limit": "50", "exclude": "0x1,0x2"}
        assert "POLY_SIGNATURE" not in route.calls[0].request.headers, "the catalogue is public"
        assert poly._positions["102"] == ("polymarket:1897034", "no")
        await poly.close()

    async def test_fetch_unknown_combo_and_no_lookup(self, poly):
        with pytest.raises(RfqNotFound):
            await poly.fetch_combo(COMBO_ID)
        with pytest.raises(NotSupported, match="no combo lookup"):
            await poly.lookup_combo(legs())

    async def test_approvals(self, poly, monkeypatch):
        seen = {}

        async def calls(calls, *, description, **kw):
            seen.update(calls=calls, description=description)
            return {"ok": True}

        monkeypatch.setattr(poly, "execute_wallet_calls", calls)
        await poly.approve_combo_trading()
        assert seen["calls"] == v3.combo_approval_calls()


# -- requester ---------------------------------------------------------------------


class TestCreate:
    async def test_buy_by_amount(self, poly, mock):
        rfq, route = await created_buy(poly, mock)
        sent = body_of(route)
        assert sent == {
            "signer_address": WALLET, "maker_address": WALLET, "signature_type": 3, "leg_position_ids": LEGS,
            "direction": "BUY", "side": "YES", "requested_size": {"unit": "notional", "value_e6": "1000000"},
        }
        check_l2(route.calls[0].request)
        assert rfq.id == BUY_ID and rfq.status == RfqStatus.OPEN and rfq.mine
        assert (rfq.outcome, rfq.action, rfq.amount, rfq.quantity) == ("yes", "buy", D("1"), None)
        assert rfq.expires_at == 4102444800000 and rfq.info["response"]["quote"]["quote_id"] == BUY_QUOTE

    async def test_sell_by_quantity(self, poly, mock):
        rfq = await created_sell(poly, mock)
        assert (rfq.action, rfq.quantity, rfq.amount) == ("sell", D("2"), None)
        assert body_of(mock.routes[0])["requested_size"] == {"unit": "shares", "value_e6": "2000000"}

    async def test_proxy_and_safe_identity(self, mock):
        for kind in (1, 2):
            poly = make(signature_type=kind)
            route = mock.post(REQ).mock(return_value=httpx.Response(200, json=sample("create_buy")))
            await poly.create_rfq(PROVISIONAL, amount=D("1"), action="buy")
            sent = body_of(route)
            assert (sent["signer_address"], sent["maker_address"], sent["signature_type"]) == (ADDRESS, WALLET, kind)
            await poly.close()

    async def test_eoa_is_refused_before_sending(self, mock):
        poly = make(signature_type=0)
        with pytest.raises(InvalidOrder, match="refuse EOA wallets"):
            await poly.create_rfq(PROVISIONAL, amount=D("1"), action="buy")
        assert not mock.calls
        await poly.close()

    @pytest.mark.parametrize("kw, match", [
        (dict(amount=D("1"), action="buy", outcome="no"), "YES only"),
        (dict(amount=D("1")), "one side"),
        (dict(quantity=D("1"), action="buy"), "sized by amount"),
        (dict(amount=D("1"), action="sell"), "sized by quantity"),
        (dict(amount=D("1.0000001"), action="buy"), "six decimals"),
        (dict(amount=D("1"), action="buy", rest_remainder=True), "remainder"),
    ])
    async def test_refusals_send_nothing(self, poly, mock, kw, match):
        with pytest.raises(InvalidOrder, match=match):
            await poly.create_rfq(PROVISIONAL, **kw)
        assert not mock.calls

    async def test_a_single_market_is_refused(self, poly, mock):
        with pytest.raises(InvalidOrder, match="combos only"):
            await poly.create_rfq("polymarket:1897034", amount=D("1"), action="buy")
        with pytest.raises(InvalidOrder, match="not been seen"):
            await poly.create_rfq(COMBO_ID, amount=D("1"), action="buy")

    async def test_by_combo_legs(self, poly, mock):
        route = mock.post(REQ).mock(return_value=httpx.Response(200, json=sample("create_buy")))
        combo = await poly.create_combo(legs())
        rfq = await poly.create_rfq("polymarket:anything", amount=D("1"), action="buy", combo=combo)
        assert body_of(route)["leg_position_ids"] == LEGS and rfq.combo_id == COMBO_ID

    async def test_an_open_rfq_is_not_made_twice(self, poly, mock):
        first, route = await created_buy(poly, mock)
        again = await poly.create_rfq(PROVISIONAL, amount=D("1"), action="buy")
        assert again.id == first.id and route.call_count == 1

    async def test_no_quotes_is_a_closed_rfq(self, poly, mock):
        rfq, _ = await created_buy(poly, mock, sample("create_no_quotes"))
        assert rfq.status == RfqStatus.CLOSED and rfq.closed_reason is None
        assert rfq.info["error"]["code"] == "NO_QUOTES" and rfq.combo_id == PROVISIONAL
        with pytest.raises(RfqNotFound, match="without a trade"):
            await poly.fetch_quotes(rfq.id)

    async def test_unknown_legs_resolved_through_gamma(self, mock):
        poly = make()
        poly._positions.clear()
        poly._position_of.clear()
        keyset = mock.get(f"{GAMMA}/markets/keyset").mock(return_value=httpx.Response(
            200, json={"markets": sample("combo_markets")["markets"]}))
        mock.post(REQ).mock(return_value=httpx.Response(200, json=sample("create_buy")))
        rfq = await poly.create_rfq(PROVISIONAL, amount=D("1"), action="buy")
        assert keyset.called and keyset.calls[0].request.url.params.get_list("position_ids") == LEGS
        assert [l.market_id for l in rfq.combo.legs] == ["polymarket:1897034", "polymarket:1897035"]
        await poly.close()


class TestReads:
    async def test_quote_maps_a_buy_to_the_ask(self, poly, mock):
        await created_buy(poly, mock)
        [quote] = await poly.fetch_quotes(BUY_ID)
        assert (quote.id, quote.rfq_id, quote.market_id) == (BUY_QUOTE, BUY_ID, COMBO_ID)
        assert (quote.ask, quote.bid, quote.ask_quantity) == (D("0.5"), None, D("1.932381"))
        assert quote.status == QuoteStatus.OPEN and not quote.mine
        assert quote.info["total_required"] == "1" and quote.price_for("yes", "buy") == D("0.5")
        assert quote.price_for("yes", "sell") is None

    async def test_quote_maps_a_sell_to_the_bid(self, poly, mock):
        await created_sell(poly, mock)
        [quote] = await poly.fetch_quotes("rfq_sell_1")
        assert (quote.bid, quote.ask, quote.bid_quantity) == (D("0.45"), None, D("2"))

    async def test_no_status_read_before_the_accept(self, poly, mock):
        await created_buy(poly, mock)
        get = mock.get(f"{REQ}/{BUY_ID}")
        assert (await poly.fetch_rfq(BUY_ID)).status == RfqStatus.OPEN
        assert (await poly.fetch_quote(BUY_QUOTE)).id == BUY_QUOTE
        assert not get.called, "the gateway answers 409 before an accept"

    async def test_fetch_rfqs_lists_this_process(self, poly, mock):
        await created_buy(poly, mock)
        await created_sell(poly, mock)
        assert {r.id for r in await poly.fetch_rfqs()} == {BUY_ID, "rfq_sell_1"}
        assert len(await poly.fetch_rfqs(status="closed")) == 0
        assert len(await poly.fetch_rfqs(market_id=COMBO_ID)) == 2
        assert len(await poly.fetch_rfqs(market_id=PROVISIONAL)) == 2
        assert len(await poly.fetch_rfqs(limit=1)) == 1

    async def test_unknown_rfq(self, poly):
        with pytest.raises(RfqNotFound, match="not created by this adapter"):
            await poly.fetch_rfq("rfq_x")
        with pytest.raises(RfqNotFound):
            await poly.fetch_quote("quote_x")

    async def test_expired_window(self, poly, mock):
        response = sample("create_buy")
        response["expires_at"] = 1_000
        await created_buy(poly, mock, response)
        quote = await poly.fetch_quote(BUY_QUOTE, rfq_id=BUY_ID)
        assert quote.status == QuoteStatus.EXPIRED
        rfq = await poly.fetch_rfq(BUY_ID)
        assert rfq.status == RfqStatus.CLOSED and rfq.closed_reason == RfqCloseReason.EXPIRED
        with pytest.raises(OrderRejected, match="not open"):
            await poly.accept_quote(accept())

    async def test_cancel_is_local(self, poly, mock):
        await created_buy(poly, mock)
        await poly.cancel_rfq(BUY_ID)
        assert len(mock.calls) == 1, "the gateway has no requester cancel"
        rfq = await poly.fetch_rfq(BUY_ID)
        assert rfq.status == RfqStatus.CLOSED and rfq.closed_reason == RfqCloseReason.CANCELED
        with pytest.raises(OrderRejected):
            await poly.accept_quote(accept())
        with pytest.raises(RfqNotFound):
            await poly.cancel_rfq("rfq_x")

    async def test_remember_rfq_after_a_restart(self, poly, mock):
        rfq, _ = await created_buy(poly, mock)
        fresh = make()
        fresh.remember_rfq(rfq)
        assert (await fresh.fetch_quote(BUY_QUOTE)).ask == D("0.5")
        await fresh.close()


class TestAccept:
    async def test_signs_an_exchange_v3_order(self, poly, mock):
        await created_buy(poly, mock)
        route = mock.post(f"{REQ}/{BUY_ID}/accept").mock(return_value=httpx.Response(200, json=sample("accept_executing")))
        result = await poly.accept_quote(accept(worst_price=D("0.5")))
        sent = body_of(route)
        order = sent["signed_order"]
        assert sent["quote_id"] == BUY_QUOTE
        assert {k: order[k] for k in ("maker", "signer", "tokenId", "makerAmount", "takerAmount", "side",
                                      "signatureType", "metadata", "builder")} == {
            "maker": WALLET, "signer": WALLET, "tokenId": "789", "makerAmount": "966191", "takerAmount": "1932381",
            "side": 0, "signatureType": 3, "metadata": v3.ZERO_BYTES32, "builder": v3.ZERO_BYTES32}
        assert len(order["timestamp"]) == 10, "Unix seconds"
        check_l2(route.calls[0].request)
        # The inner signature recovers to the signer over the docs' Deposit Wallet payload.
        unsigned = {k: v for k, v in order.items() if k != "signature"}
        inner = bytes.fromhex(order["signature"][2:])[:65]
        assert EthAccount.recover_message(encode_typed_data(full_message=v3.deposit_wallet_typed_data(unsigned)),
                                          signature=inner) == ADDRESS
        assert result.status == QuoteStatus.CONFIRMED and result.accepted_side == Side.BUY
        assert result.info["order_hash"] == v3.order_hash(unsigned)
        assert result.info["accept"]["client_order_id"] == f"rfq-accept:polymarket:{BUY_QUOTE}"

    async def test_a_sell(self, poly, mock):
        await created_sell(poly, mock)
        route = mock.post(f"{REQ}/rfq_sell_1/accept").mock(return_value=httpx.Response(
            200, json={"rfq_id": "rfq_sell_1", "status": "AWAITING_MAKER_CONFIRMATION"}))
        result = await poly.accept_quote(accept("quote_sell_1", "rfq_sell_1", action="sell", worst_price=D("0.45")))
        order = body_of(route)["signed_order"]
        assert (order["side"], order["makerAmount"], order["takerAmount"]) == (1, "2000000", "900000")
        assert result.status == QuoteStatus.ACCEPTED and result.accepted_side == Side.SELL

    async def test_proxy_signs_the_order_directly(self, mock):
        poly = make(signature_type=1)
        await created_buy(poly, mock)
        route = mock.post(f"{REQ}/{BUY_ID}/accept").mock(return_value=httpx.Response(200, json=sample("accept_executing")))
        await poly.accept_quote(accept())
        order = body_of(route)["signed_order"]
        assert (order["maker"], order["signer"], order["signatureType"]) == (WALLET, ADDRESS, 1)
        unsigned = {k: v for k, v in order.items() if k != "signature"}
        assert EthAccount.recover_message(encode_typed_data(full_message=v3.order_typed_data(unsigned)),
                                          signature=order["signature"]) == ADDRESS
        await poly.close()

    @pytest.mark.parametrize("worst", [D("0.49"), D("0.4999")])
    async def test_worst_price_refuses_before_signing(self, poly, mock, worst):
        await created_buy(poly, mock)
        route = mock.post(f"{REQ}/{BUY_ID}/accept")
        with pytest.raises(InvalidOrder, match="worse than the worst price"):
            await poly.accept_quote(accept(worst_price=worst))
        assert not route.called

    async def test_worst_price_on_a_sell(self, poly, mock):
        await created_sell(poly, mock)
        with pytest.raises(InvalidOrder, match="worse than the worst price"):
            await poly.accept_quote(accept("quote_sell_1", "rfq_sell_1", action="sell", worst_price=D("0.46")))

    @pytest.mark.parametrize("kw, error, match", [
        (dict(action="sell"), InvalidOrder, "asked to buy"),
        (dict(outcome="no", action="sell"), InvalidOrder, "outcome='yes'"),
        (dict(quantity=D("1")), InvalidOrder, "full size"),
    ])
    async def test_side_and_size_refusals(self, poly, mock, kw, error, match):
        await created_buy(poly, mock)
        with pytest.raises(error, match=match):
            await poly.accept_quote(accept(**kw))
        assert len(mock.calls) == 1

    async def test_eoa_accept_refused(self, mock):
        poly = make(signature_type=0)
        with pytest.raises(InvalidOrder, match="UNSUPPORTED_REQUESTER_SIGNATURE_TYPE"):
            await poly.accept_quote(accept())
        await poly.close()

    async def test_retry_does_not_resend(self, poly, mock):
        await created_buy(poly, mock)
        route = mock.post(f"{REQ}/{BUY_ID}/accept").mock(return_value=httpx.Response(200, json=sample("accept_executing")))
        mock.get(f"{REQ}/{BUY_ID}").mock(return_value=httpx.Response(200, json=sample("status_mined")))
        await poly.accept_quote(accept())
        again = await poly.accept_quote(accept())
        assert route.call_count == 1 and again.info["accept"]["resent"] is False
        assert again.status == QuoteStatus.CONFIRMED

    async def test_maker_declined_reads_as_canceled(self, poly, mock):
        await created_buy(poly, mock)
        mock.post(f"{REQ}/{BUY_ID}/accept").mock(return_value=httpx.Response(200, json=sample("accept_declined")))
        result = await poly.accept_quote(accept())
        assert result.status == QuoteStatus.CANCELED and result.info["error"]["code"] == "MAKER_DECLINED"

    async def test_follow_to_a_fill(self, poly, mock):
        await created_buy(poly, mock)
        mock.post(f"{REQ}/{BUY_ID}/accept").mock(return_value=httpx.Response(200, json=sample("accept_executing")))
        status = mock.get(f"{REQ}/{BUY_ID}").mock(side_effect=[
            httpx.Response(200, json=sample("status_mined")), httpx.Response(200, json=sample("status_filled"))])
        await poly.accept_quote(accept())
        assert (await poly.fetch_quote(BUY_QUOTE, rfq_id=BUY_ID)).status == QuoteStatus.CONFIRMED
        quote = await poly.fetch_quote(BUY_QUOTE, rfq_id=BUY_ID)
        assert quote.status == QuoteStatus.EXECUTED and quote.taker_order_id == f"rfq:{BUY_ID}"
        check_l2(status.calls[0].request)
        order = await poly.fetch_order(quote.taker_order_id)
        assert (order.status, order.type, order.side, order.market_id) == (
            OrderStatus.CLOSED, OrderType.RFQ_TAKER, Side.BUY, COMBO_ID)
        assert (order.filled, order.price, order.fee, order.outcome, order.action) == (
            D("1.932381"), D("0.5"), D("0.033809"), "yes", "buy")
        [fill] = await poly.fetch_my_trades(market_id=COMBO_ID)
        assert (fill.order_id, fill.quantity, fill.price, fill.info["tx_hash"]) == (order.id, D("1.932381"), D("0.5"), "0xtx1")
        assert len(await poly.fetch_my_trades(order_id=order.id)) == 1
        assert status.call_count == 2, "a final status is not read again"
        assert (await poly.fetch_rfq(BUY_ID)).closed_reason == RfqCloseReason.EXECUTED

    async def test_status_409_keeps_what_is_known(self, poly, mock):
        await created_buy(poly, mock)
        mock.post(f"{REQ}/{BUY_ID}/accept").mock(return_value=httpx.Response(200, json=sample("accept_executing")))
        mock.get(f"{REQ}/{BUY_ID}").mock(return_value=httpx.Response(409, json={"error": "bad state", "code": "INVALID_RFQ_STATE"}))
        await poly.accept_quote(accept())
        assert (await poly.fetch_quote(BUY_QUOTE)).status == QuoteStatus.CONFIRMED

    async def test_unaccepted_rfq_has_no_order(self, poly, mock):
        await created_buy(poly, mock)
        with pytest.raises(OrderNotFound, match="never accepted"):
            await poly.fetch_order(f"rfq:{BUY_ID}")
        with pytest.raises(OrderNotFound):
            await poly.fetch_order("rfq:unknown")


class TestErrors:
    @pytest.mark.parametrize("status, body, error, match", [
        (400, {"error": "unsupported", "code": "UNSUPPORTED_REQUESTER_SIGNATURE_TYPE"}, InvalidOrder, "EOA"),
        (400, {"error": "invalid rfq", "code": "INVALID_RFQ"}, InvalidOrder, "INVALID_RFQ"),
        (400, {"error": "builder", "code": "BUILDER_ATTRIBUTION_NOT_ALLOWED"}, InvalidOrder, "BUILDER"),
        (401, {"error": "unauthenticated", "code": "UNAUTHENTICATED"}, AuthenticationError, "UNAUTHENTICATED"),
        (404, {"error": "unknown rfq", "code": "UNKNOWN_RFQ"}, RfqNotFound, "unknown rfq"),
        (409, {"error": "expired", "code": "EXPIRED_RFQ"}, OrderRejected, "EXPIRED_RFQ"),
        (503, {"error": "reserve", "code": "PRE_EXECUTION_BALANCE_RESERVATION_FAILED"}, InsufficientFunds, "reserve"),
        (503, {"error": "down", "code": "SERVICE_UNAVAILABLE"}, ExchangeNotAvailable, "SERVICE_UNAVAILABLE"),
        (503, {"error": "submit", "code": "TRADE_SUBMISSION_FAILED"}, ExchangeNotAvailable, "TRADE_SUBMISSION_FAILED"),
    ])
    async def test_accept_errors(self, poly, mock, status, body, error, match):
        await created_buy(poly, mock)
        mock.post(f"{REQ}/{BUY_ID}/accept").mock(return_value=httpx.Response(status, json=body))
        with pytest.raises(error, match=match) as caught:
            await poly.accept_quote(accept())
        if isinstance(caught.value, OrderRejected):
            assert caught.value.reason == "expired_rfq"

    async def test_rate_limited(self, poly, mock, monkeypatch):
        async def instant(_):
            return None

        monkeypatch.setattr(asyncio, "sleep", instant)
        mock.post(REQ).mock(return_value=httpx.Response(429, json={"error": "rate limited", "code": "RATE_LIMITED"}))
        with pytest.raises(RateLimitExceeded):
            await poly.create_rfq(PROVISIONAL, amount=D("1"), action="buy")


# -- quoter ------------------------------------------------------------------------


async def session(poly, *names: str):
    """A quoter session with these messages delivered (no socket)."""
    poly.rfq_session = prfq.PolymarketRfqSession(poly)
    for name in names:
        await poly.rfq_session.handle(sample(name))
    return poly.rfq_session


class TestQuoter:
    async def test_no_session_no_incoming(self, poly):
        assert list(await poly.fetch_incoming_rfqs()) == []

    async def test_incoming_from_the_session(self, poly):
        await session(poly, "ws_request_buy", "ws_request_sell")
        rows = {r.id: r for r in await poly.fetch_incoming_rfqs()}
        buy, sell = rows["rfq_in_1"], rows["rfq_in_2"]
        assert (buy.action, buy.amount, buy.combo_id, buy.mine) == ("buy", D("1"), COMBO_ID, False)
        assert (sell.action, sell.quantity) == ("sell", D("1"))
        assert [l.market_id for l in buy.combo.legs] == ["polymarket:1897034", "polymarket:1897035"]
        assert len(await poly.fetch_incoming_rfqs(market_id="polymarket:combo-0xother")) == 0

    async def test_quote_a_buy_request(self, poly, mock):
        await session(poly, "ws_request_buy")
        route = mock.post(f"{API}/v1/maker/quotes").mock(return_value=httpx.Response(200, json=sample("maker_snapshot")))
        quote = await poly.send_quote(MakerQuote(rfq_id="rfq_in_1", ask=D("0.45"), client_order_id="k1",
                                                 expires_at=4102444800000))
        sent = body_of(route)
        check_l2(route.calls[0].request)
        assert sent["quote_id"] == prfq.quote_id_for("k1") and sent["quote_id"].startswith("quote_")
        assert len(sent["quote_id"]) == len("quote_") + 32
        assert (sent["price_e6"], sent["size_e6"], sent["valid_until"]) == ("450000", "2222222", 4102444800000)
        order = sent["signed_order"]
        # Fill a BUY of YES from collateral: buy NO at 1 - 0.45, ceil.
        assert (order["tokenId"], order["side"], order["makerAmount"], order["takerAmount"]) == ("790", 0, "1222223", "2222222")
        assert (sent["signer_address"], sent["maker_address"], sent["signature_type"]) == (WALLET, WALLET, 3)
        assert (quote.ask, quote.bid, quote.ask_quantity, quote.mine, quote.status) == (
            D("0.45"), None, D("2.222222"), True, QuoteStatus.OPEN)
        assert (await poly.fetch_quote(quote.id)).id == quote.id

    async def test_quote_a_sell_request_partly(self, poly, mock):
        await session(poly, "ws_request_sell")
        route = mock.post(f"{API}/v1/maker/quotes").mock(return_value=httpx.Response(200, json=sample("maker_snapshot")))
        quote = await poly.send_quote(MakerQuote(rfq_id="rfq_in_2", bid=D("0.45"), ask=D("0.6"), quantity=D("0.5")))
        order = body_of(route)["signed_order"]
        assert (order["tokenId"], order["makerAmount"], order["takerAmount"]) == ("789", "225000", "500000")
        assert (quote.bid, quote.ask, quote.bid_quantity) == (D("0.45"), None, D("0.5"))

    async def test_eoa_quoter_signs_as_itself(self, mock):
        poly = make(signature_type=0)
        await session(poly, "ws_request_sell")
        route = mock.post(f"{API}/v1/maker/quotes").mock(return_value=httpx.Response(200, json=sample("maker_snapshot")))
        await poly.send_quote(MakerQuote(rfq_id="rfq_in_2", bid=D("0.45")))
        sent = body_of(route)
        assert (sent["signer_address"], sent["maker_address"], sent["signature_type"]) == (ADDRESS, ADDRESS, 0)
        order = sent["signed_order"]
        unsigned = {k: v for k, v in order.items() if k != "signature"}
        assert EthAccount.recover_message(encode_typed_data(full_message=v3.order_typed_data(unsigned)),
                                          signature=order["signature"]) == ADDRESS
        await poly.close()

    @pytest.mark.parametrize("quote, match", [
        (MakerQuote(rfq_id="rfq_in_1", bid=D("0.4")), "quote the ask"),
        (MakerQuote(rfq_id="rfq_in_1", ask=D("0.95")), "0.90909"),
        (MakerQuote(rfq_id="rfq_in_1", ask=D("0.45"), quantity=D("5")), "more than the request"),
        (MakerQuote(rfq_id="rfq_in_1", ask=D("0.45"), post_only=True), "post_only"),
        (MakerQuote(rfq_id="rfq_x", ask=D("0.45")), "not one the quoter session received"),
    ])
    async def test_quote_refusals(self, poly, mock, quote, match):
        await session(poly, "ws_request_buy")
        with pytest.raises(InvalidOrder, match=match):
            await poly.send_quote(quote)
        assert not mock.calls

    async def test_cancel_then_replace(self, poly, mock):
        await session(poly, "ws_request_buy")
        mock.post(f"{API}/v1/maker/quotes").mock(return_value=httpx.Response(200, json=sample("maker_snapshot")))
        cancel = mock.post(f"{API}/v1/maker/quotes/cancel").mock(return_value=httpx.Response(200, json=sample("maker_snapshot")))
        first = await poly.send_quote(MakerQuote(rfq_id="rfq_in_1", ask=D("0.45"), client_order_id="a"))
        second = await poly.replace_quote(first.id, MakerQuote(rfq_id="rfq_in_1", ask=D("0.44"), client_order_id="b"))
        assert body_of(cancel) == {"rfq_id": "rfq_in_1", "quote_id": first.id, "signer_address": WALLET,
                                   "maker_address": WALLET, "signature_type": 3}
        assert second.id != first.id and second.info["replaces"] == first.id
        assert (await poly.fetch_quote(first.id)).status == QuoteStatus.CANCELED
        with pytest.raises(InvalidOrder, match="give rfq_id"):
            await poly.cancel_quote("quote_unknown")

    async def test_last_look_confirm_and_execution(self, poly, mock):
        await session(poly, "ws_request_buy")
        mock.post(f"{API}/v1/maker/quotes").mock(return_value=httpx.Response(200, json=sample("maker_snapshot")))
        confirm = mock.post(f"{API}/v1/maker/confirmations").mock(return_value=httpx.Response(200, json=sample("maker_execution")))
        quote = await poly.send_quote(MakerQuote(rfq_id="rfq_in_1", ask=D("0.45")))
        await poly.rfq_session.handle({"type": "RFQ_CONFIRMATION_REQUEST", "rfq_id": "rfq_in_1", "quote_id": quote.id,
                                       "direction": "BUY", "side": "YES", "fill_size_e6": "2000000",
                                       "price_e6": "450000", "confirm_by": 4102444800000})
        accepted = await poly.fetch_quote(quote.id)
        assert accepted.status == QuoteStatus.ACCEPTED and accepted.accepted_side == Side.BUY
        assert accepted.confirm_by == 4102444800000 and accepted.ask_quantity == D("2")
        confirmed = await poly.confirm_quote(accepted)
        assert body_of(confirm)["decision"] == "CONFIRM" and confirmed.status == QuoteStatus.CONFIRMED
        await poly.rfq_session.handle({"type": "RFQ_EXECUTION_UPDATE", "rfq_id": "rfq_in_1", "status": "CONFIRMED",
                                       "tx_hash": "0xtx2"})
        done = await poly.fetch_quote(quote.id)
        assert done.status == QuoteStatus.EXECUTED and done.maker_order_id == f"rfq:rfq_in_1:{quote.id}"
        order = await poly.fetch_order(done.maker_order_id)
        assert (order.type, order.side, order.filled, order.price, order.outcome, order.action) == (
            OrderType.RFQ_MAKER, Side.SELL, D("2"), D("0.45"), "no", "buy")
        [fill] = await poly.fetch_my_trades(order_id=order.id)
        assert fill.liquidity.value == "maker" and fill.info["tx_hash"] == "0xtx2"
        assert [q.id for q in await poly.fetch_my_quotes(status="executed")] == [quote.id]

    async def test_decline_during_the_last_look(self, poly, mock):
        await session(poly, "ws_request_buy")
        mock.post(f"{API}/v1/maker/quotes").mock(return_value=httpx.Response(200, json=sample("maker_snapshot")))
        confirm = mock.post(f"{API}/v1/maker/confirmations").mock(return_value=httpx.Response(
            200, json={"snapshot": sample("maker_snapshot")}))
        quote = await poly.send_quote(MakerQuote(rfq_id="rfq_in_1", ask=D("0.45")))
        await poly.rfq_session.handle({"type": "RFQ_CONFIRMATION_REQUEST", "rfq_id": "rfq_in_1", "quote_id": quote.id})
        await poly.cancel_quote(quote.id)
        assert body_of(confirm)["decision"] == "DECLINE"
        assert (await poly.fetch_quote(quote.id)).status == QuoteStatus.CANCELED

    async def test_lost_failed_and_stale_quotes(self, poly, mock, monkeypatch):
        await session(poly, "ws_request_buy")
        mock.post(f"{API}/v1/maker/quotes").mock(return_value=httpx.Response(200, json=sample("maker_snapshot")))
        lost = await poly.send_quote(MakerQuote(rfq_id="rfq_in_1", ask=D("0.45")))
        await poly.rfq_session.handle({"type": "RFQ_TRADE", "rfq_id": "rfq_in_1", "price_e6": "440000"})
        assert (await poly.fetch_quote(lost.id)).status == QuoteStatus.CANCELED
        await poly.rfq_session.handle(sample("ws_request_sell"))
        failed = await poly.send_quote(MakerQuote(rfq_id="rfq_in_2", bid=D("0.45")))
        await poly.rfq_session.handle({"type": "RFQ_ERROR", "request_type": "RFQ_QUOTE", "rfq_id": "rfq_in_2",
                                       "quote_id": failed.id, "code": "SUBMISSION_WINDOW_CLOSED", "error": "late"})
        assert (await poly.fetch_quote(failed.id)).status == QuoteStatus.FAILED
        stale = await poly.send_quote(MakerQuote(rfq_id="rfq_in_2", bid=D("0.44"), client_order_id="s"))
        poly._maker[stale.id].sent_at -= 60_000
        assert (await poly.fetch_quote(stale.id)).status == QuoteStatus.EXPIRED

    async def test_maker_error_mapping(self, poly, mock):
        await session(poly, "ws_request_buy")
        mock.post(f"{API}/v1/maker/quotes").mock(return_value=httpx.Response(
            400, json={"error": "allowance", "code": "ALLOWANCE_VALIDATION_FAILED"}))
        with pytest.raises(InsufficientFunds, match="approve_combo_trading"):
            await poly.send_quote(MakerQuote(rfq_id="rfq_in_1", ask=D("0.45")))

    async def test_the_socket_authenticates_first(self, poly):
        sent: list[dict] = []
        incoming = [json.dumps({"type": "auth", "success": True, "address": ADDRESS, "role": "maker"}),
                    json.dumps(sample("ws_request_buy"))]

        class Socket:
            async def send(self, text):
                sent.append(json.loads(text))

            def __aiter__(self):
                return self

            async def __anext__(self):
                if not incoming:
                    raise StopAsyncIteration
                return incoming.pop(0)

            async def close(self):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

        urls = []
        seen = []

        def connect(url):
            urls.append(url)
            return Socket()

        live = await poly.start_rfq_session(connect=connect, on_rfq=lambda rfq: seen.append(rfq.id))
        await asyncio.wait_for(live._task, 1)
        assert urls == [prfq.QUOTER_WS_URL]
        assert sent[0] == {"type": "auth", "auth": {"apiKey": "api-key-uuid", "secret": SECRET, "passphrase": "pass"},
                           "identity": {"signer_address": WALLET, "maker_address": WALLET, "signature_type": 3}}
        assert live.authenticated is True and live.role == "maker" and seen == ["rfq_in_1"]
        await poly.stop_rfq_session()
        assert poly.rfq_session is None


# -- the engine, end to end --------------------------------------------------------


async def test_engine_accept_to_fill_to_position(tmp_path, mock):
    from synpath.engine import Engine, EngineConfig, RiskConfig
    from synpath.engine.storage_types import IntentState

    poly = make()
    engine = Engine({"polymarket": poly},
                    EngineConfig(journal_path=str(tmp_path / "engine.db"), in_doubt_timeout_s=0,
                                 rfq_poll_interval_s=0.01, rfq_incoming_interval_s=None),
                    risk=RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None))
    await engine.start()
    try:
        mock.post(REQ).mock(return_value=httpx.Response(200, json=sample("create_buy")))
        accept_route = mock.post(f"{REQ}/{BUY_ID}/accept").mock(
            return_value=httpx.Response(200, json=sample("accept_executing")))
        mock.get(f"{REQ}/{BUY_ID}").mock(return_value=httpx.Response(200, json=sample("status_filled")))
        combo = await engine.create_combo(legs(), venue="polymarket")
        rfq = await engine.create_rfq(combo.market_id, amount=D("1"), outcome="yes", action="buy", book="alpha")
        assert rfq.combo_id == COMBO_ID and f"polymarket:{rfq.id}" in engine.rfq.rfqs
        await engine.rfq.tick()
        quote = (await poly.fetch_quotes(rfq.id))[0]

        result = await engine.accept_quote(AcceptQuote(quote_id=quote.id, rfq_id=rfq.id, outcome="yes", action="buy",
                                                       worst_price=D("0.51"), book="alpha"))
        assert result.status == QuoteStatus.CONFIRMED and accept_route.call_count == 1
        await engine.rfq.tick()

        [intent] = [i for i in await engine.journal.intents() if i.operation == "accept_quote"]
        assert intent.state == IntentState.SENT and intent.order_id == f"rfq:{BUY_ID}"
        order = await engine.journal.order("polymarket", f"rfq:{BUY_ID}")
        assert order is not None and order.type == OrderType.RFQ_TAKER and order.book == "alpha"
        [position] = [p for p in engine.positions() if p.market_id == COMBO_ID]
        assert position.contracts == D("1.932381")
        assert f"polymarket:{rfq.id}" not in engine.rfq.rfqs, "the RFQ closed as executed"
    finally:
        await engine.journal.close()
        await poly.close()


# -- approvals by role ---------------------------------------------------------------


class TestApprovals:
    PUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
    PM = "0x006F54F7f9A22e0000CC2AB60031000000ae9fEF"
    MAX = "f" * 64

    @staticmethod
    def word(address: str) -> str:
        return "0" * 24 + address[2:].lower()

    def approve(self, spender: str) -> str:
        return "0x095ea7b3" + self.word(spender) + self.MAX           # approve(address,uint256)

    def for_all(self, operator: str) -> str:
        return "0xa22cb465" + self.word(operator) + "0" * 63 + "1"   # setApprovalForAll(address,bool)

    def test_default_is_exchange_v3_only(self):
        calls = v3.combo_approval_calls()
        assert calls == [
            {"target": self.PUSD, "value": "0", "data": self.approve(v3.EXCHANGE_V3)},
            {"target": self.PM, "value": "0", "data": self.for_all(v3.EXCHANGE_V3)},
        ]

    def test_router_and_auto_redeemer(self):
        router, redeemer = "0x12121212006e4CD160D18e3f00711DA5c3372600", "0xa1200000d0002264C9a1698e001292D00E1b00af"
        assert (v3.ROUTER, v3.AUTO_REDEEMER) == (router, redeemer)
        calls = v3.combo_approval_calls(include_router=True, include_auto_redeem=True)
        assert [(c["target"], c["data"]) for c in calls] == [
            (self.PUSD, self.approve(v3.EXCHANGE_V3)), (self.PM, self.for_all(v3.EXCHANGE_V3)),
            (self.PUSD, self.approve(router)), (self.PM, self.for_all(router)),
            (self.PM, self.for_all(redeemer)),
        ]
        assert len(v3.combo_approval_calls(include_router=True)) == 4
        assert [c["data"] for c in v3.combo_approval_calls(include_auto_redeem=True)][-1] == self.for_all(redeemer)

    async def test_the_adapter_passes_the_flags(self, poly, monkeypatch):
        seen = {}

        async def calls(calls, *, description, **kw):
            seen["calls"] = calls
            return {"ok": True}

        monkeypatch.setattr(poly, "execute_wallet_calls", calls)
        await poly.approve_combo_trading(include_router=True)
        assert seen["calls"] == v3.combo_approval_calls(include_router=True)


# -- the engine: pushed quote requests and restarts ------------------------------------


def engine_on(tmp_path, poly, **kw):
    from synpath.engine import Engine, EngineConfig, RiskConfig

    base = dict(journal_path=str(tmp_path / "engine.db"), in_doubt_timeout_s=0, rfq_poll_interval_s=0.01,
                rfq_incoming_interval_s=None)
    return Engine({"polymarket": poly}, EngineConfig(**{**base, **kw}),
                  risk=RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None))


async def test_engine_quotes_a_pushed_request_inside_the_window(tmp_path, mock):
    """Gap 1 on the real adapter: the quoter session's RFQ_REQUEST reaches an
    engine callback, which quotes it, with no poll in between."""
    import time

    poly = make()
    engine = engine_on(tmp_path, poly, rfq_incoming_interval_s=2.0, rfq_stream_incoming=False)
    await engine.start()
    try:
        assert engine.rfq.pushed == {"polymarket"}
        route = mock.post(f"{API}/v1/maker/quotes").mock(return_value=httpx.Response(200, json=sample("maker_snapshot")))
        stamps: dict[str, float] = {}

        async def quoter(rfq):
            stamps["callback"] = time.perf_counter()
            await engine.send_quote(MakerQuote(rfq_id=rfq.id, ask=D("0.45"), book="mm"), venue="polymarket")
            stamps["quoted"] = time.perf_counter()

        engine.rfq.on_incoming_rfq(quoter)
        poly.rfq_session = prfq.PolymarketRfqSession(poly)               # a fake stream: no socket
        start = time.perf_counter()
        await poly.rfq_session.handle(sample("ws_request_buy"))
        await asyncio.gather(*list(poly._listener_tasks))
        to_callback, to_quote = (stamps["callback"] - start) * 1000, (stamps["quoted"] - start) * 1000
        print(f"polymarket RFQ_REQUEST -> callback {to_callback:.3f} ms, -> quote posted {to_quote:.3f} ms")
        assert to_callback < 50 and to_quote < 400, "inside the 400 ms window, no poll delay"
        assert route.call_count == 1 and "polymarket:rfq_in_1" in engine.rfq.incoming
    finally:
        await engine.journal.close()
        await poly.close()


async def accepted_then_crashed(tmp_path, mock, *, accept_fails: bool = False):
    poly = make()
    engine = engine_on(tmp_path, poly)
    await engine.start()
    mock.post(REQ).mock(return_value=httpx.Response(200, json=sample("create_buy")))
    if accept_fails:
        mock.post(f"{REQ}/{BUY_ID}/accept").mock(side_effect=httpx.ConnectError("reset"))
    else:
        mock.post(f"{REQ}/{BUY_ID}/accept").mock(return_value=httpx.Response(200, json=sample("accept_executing")))
    combo = await engine.create_combo(legs(), venue="polymarket")
    rfq = await engine.create_rfq(combo.market_id, amount=D("1"), outcome="yes", action="buy", book="alpha")
    quote = (await poly.fetch_quotes(rfq.id))[0]
    try:
        await engine.accept_quote(AcceptQuote(quote_id=quote.id, rfq_id=rfq.id, outcome="yes", action="buy",
                                              worst_price=D("0.51"), book="alpha"))
    except Exception:
        if not accept_fails:
            raise
    await engine.journal.close()                     # the process dies; the adapter's memory with it
    await poly.close()
    mock.reset()
    return rfq


async def test_restart_follows_an_accept_to_a_position(tmp_path, mock):
    """Gap 2: the gateway lists no RFQs, so the engine re-seeds the adapter
    from its journal and the in-flight accept resolves after the restart."""
    from synpath.engine.storage_types import IntentState

    rfq = await accepted_then_crashed(tmp_path, mock)
    fresh = make()
    status = mock.get(f"{REQ}/{BUY_ID}").mock(return_value=httpx.Response(200, json=sample("status_filled")))
    engine = engine_on(tmp_path, fresh)
    recovery = await engine.start()
    try:
        assert status.called, "the re-seeded RFQ is read as accepted"
        assert recovery.adopted == 1, recovery.details
        [intent] = [i for i in await engine.journal.intents() if i.operation == "accept_quote"]
        assert intent.state == IntentState.SENT and intent.order_id == f"rfq:{BUY_ID}"
        [position] = [p for p in engine.positions() if p.market_id == COMBO_ID]
        assert position.contracts == D("1.932381")
        assert f"polymarket:{rfq.id}" not in engine.rfq.rfqs
    finally:
        await engine.journal.close()
        await fresh.close()


async def test_restart_with_an_accept_that_never_landed_sends_nothing(tmp_path, mock):
    from synpath.engine.storage_types import IntentState

    await accepted_then_crashed(tmp_path, mock, accept_fails=True)
    fresh = make()
    mock.get(f"{REQ}/{BUY_ID}").mock(return_value=httpx.Response(
        409, json={"error": "not accepted", "code": "INVALID_RFQ_STATE"}))
    accept = mock.post(f"{REQ}/{BUY_ID}/accept")
    engine = engine_on(tmp_path, fresh, lost_after_s=0)
    await engine.start()
    try:
        [intent] = [i for i in await engine.journal.intents() if i.operation == "accept_quote"]
        assert intent.state == IntentState.REJECTED, "the quote reads open: the accept never reached the venue"
        assert not accept.called and engine.positions() == []
    finally:
        await engine.journal.close()
        await fresh.close()


async def test_an_open_rfq_can_be_cancelled_after_a_restart(tmp_path, mock):
    poly = make()
    engine = engine_on(tmp_path, poly)
    await engine.start()
    mock.post(REQ).mock(return_value=httpx.Response(200, json=sample("create_buy")))
    rfq = await engine.create_rfq(PROVISIONAL, amount=D("1"), outcome="yes", action="buy", book="alpha")
    await engine.journal.close()
    await poly.close()

    fresh = make()
    engine = engine_on(tmp_path, fresh)
    await engine.start()
    try:
        assert [r.id for r in await fresh.fetch_rfqs(status="open")] == [rfq.id], "re-seeded from the working set"
        await engine.cancel_rfq(rfq.id, venue="polymarket")
        assert (await fresh.fetch_rfq(rfq.id)).closed_reason == RfqCloseReason.CANCELED
        assert engine.rfq.rfqs == {}
    finally:
        await engine.journal.close()
        await fresh.close()


async def test_restart_follows_a_confirm_in_doubt(tmp_path, mock):
    """Our quote's confirm was sent, then the process died: the quote is
    re-seeded `confirmed` (not unknown, which would read as nothing traded)
    and the execution the session reports later is adopted."""
    from synpath.engine.storage_types import IntentState

    poly = make()
    engine = engine_on(tmp_path, poly)
    await engine.start()
    mock.post(f"{API}/v1/maker/quotes").mock(return_value=httpx.Response(200, json=sample("maker_snapshot")))
    mock.post(f"{API}/v1/maker/confirmations").mock(return_value=httpx.Response(
        200, json={"snapshot": sample("maker_snapshot")}))
    await session(poly, "ws_request_buy")
    await engine.rfq.watch_incoming()
    quote = await engine.send_quote(MakerQuote(rfq_id="rfq_in_1", ask=D("0.45"), book="mm"), venue="polymarket")
    await poly.rfq_session.handle({"type": "RFQ_CONFIRMATION_REQUEST", "rfq_id": "rfq_in_1", "quote_id": quote.id,
                                   "fill_size_e6": "2000000", "confirm_by": 4102444800000})
    await engine.confirm_quote(quote.id, venue="polymarket")
    await engine.journal.close()
    await poly.close()

    fresh = make()
    engine = engine_on(tmp_path, fresh, lost_after_s=0)
    await engine.start()
    try:
        [intent] = [i for i in await engine.journal.intents() if i.operation == "confirm_quote"]
        assert intent.state == IntentState.SENDING, "still in doubt, not failed"
        assert (await fresh.fetch_quote(quote.id)).status == QuoteStatus.CONFIRMED
        fresh.rfq_session = prfq.PolymarketRfqSession(fresh)
        await fresh.rfq_session.handle({"type": "RFQ_EXECUTION_UPDATE", "rfq_id": "rfq_in_1", "status": "CONFIRMED",
                                        "tx_hash": "0xtx9"})
        await engine.rfq.tick()
        intent = await engine.journal.intent(intent.client_order_id)
        assert intent.state == IntentState.SENT and intent.order_id == f"rfq:rfq_in_1:{quote.id}"
        [position] = [p for p in engine.positions() if p.market_id == COMBO_ID]
        assert position.contracts == D("2")
    finally:
        await engine.journal.close()
        await fresh.close()
