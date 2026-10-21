"""Polymarket US combos and RFQ on the retail API, offline (P2 of docs/rfq-combo.md).

Nothing here was captured live. Request and response shapes follow the
venue's published docs: the OpenAPI schemas
`api-reference/oapi-schemas/{combos,rfqs}-schema.json`, the examples in
`api-reference/combos/overview.md`, `api-reference/rfqs/overview.md` and
`trader-guide/combos.md`, and `partners/orders/combos-takers.md`. Samples are
in `tests/samples/polymarket_us_rfq_*.json`.
"""
from __future__ import annotations

import base64
import json
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from synpath.errors import AuthenticationError, NotSupported, RateLimitExceeded
from synpath.trading import (
    AcceptQuote, Combo, ComboLeg, MakerQuote, QuoteStatus, Rfq, RfqQuote, RfqStatus, Side,
)
from synpath.trading import polymarket_us as retail
from synpath.trading.base import TradingExchange
from synpath.trading.credentials import PolymarketUSCredentials
from synpath.trading.errors import InsufficientFunds, InvalidOrder, OrderRejected, RfqNotFound
from synpath.trading.limiter import BudgetLimiter

D = Decimal
pytestmark = pytest.mark.anyio
API = retail.API_URL
SAMPLES = Path(__file__).parent / "samples"
ATL = "tec-mlb-nlchamp-2026-09-27-atl"
NYY = "tec-mlb-alchamp-2026-09-27-nyy"
COMBO = "caoc-example"


def sample(name: str):
    return json.loads((SAMPLES / f"polymarket_us_rfq_{name}.json").read_text())


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="module")
def ed_key():
    return ed25519.Ed25519PrivateKey.generate()


@pytest.fixture
def venue():
    with respx.mock(base_url=API, assert_all_called=False) as mock:
        mock.get("/v1/rfqs/user-id").respond(json=sample("user_id"))
        yield mock


@pytest.fixture
def us(venue, ed_key):
    raw = ed_key.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption())
    return retail.PolymarketUSTrading(
        PolymarketUSCredentials(key_id="key-1", secret_key=base64.b64encode(raw).decode()),
        client=httpx.AsyncClient(), limiter=BudgetLimiter(read_per_second=1000, write_per_second=1000),
        rfq_pace_per_second=None,   # TestPacing covers the per-endpoint pace on a fake clock
    )


def body(route) -> dict:
    return json.loads(route.calls.last.request.content)


def query(route) -> dict:
    return dict(route.calls.last.request.url.params)


def accept(**kw) -> AcceptQuote:
    base = dict(quote_id="quote_01", rfq_id="rfq_01", outcome="yes", action="buy")
    base.update(kw)
    return AcceptQuote(**base)


# -- interface ---------------------------------------------------------------------


class TestInterface:
    NAMES = (
        "create_combo", "fetch_combo", "lookup_combo",
        "create_rfq", "fetch_rfq", "fetch_rfqs", "cancel_rfq", "fetch_quotes", "fetch_quote", "accept_quote",
        "fetch_incoming_rfqs", "send_quote", "replace_quote", "cancel_quote", "confirm_quote", "fetch_my_quotes",
    )

    def test_typed_methods_live_on_the_base(self):
        for name in self.NAMES:
            assert name in TradingExchange.__dict__, name

    async def test_base_defaults_are_not_supported(self):
        class Bare(TradingExchange):
            id = "bare"

            async def create_order(self, request): ...
            async def cancel_order(self, order_id, *, market_id=None): ...
            async def fetch_order(self, order_id): ...
            async def fetch_open_orders(self, *, market_id=None): ...
            async def fetch_balance(self, *, account=None): ...
            async def fetch_positions(self, *, market_id=None, event_id=None): ...

        with pytest.raises(NotSupported, match="bare: accept_quote"):
            await Bare().accept_quote(accept())
        with pytest.raises(NotSupported, match="bare: create_combo"):
            await Bare().create_combo([])

    def test_capability_keys(self):
        has = retail.PolymarketUSTrading.has
        assert has["rfq_taker"] is True and has["rfq_quoter"] is True and has["combos"] is True
        assert has["rfq"] is False   # the old key names the raw Kalshi methods


# -- combos ------------------------------------------------------------------------


class TestCombos:
    async def test_create_maps_outcomes_to_leg_sides(self, us, venue, ed_key):
        route = venue.post("/v1/combos").respond(json=sample("create_combo"))
        combo = await us.create_combo([
            ComboLeg(market_id=f"polymarket_us:{ATL}", outcome="yes"), ComboLeg(market_id=NYY, outcome="no"),
        ])
        assert body(route) == {"legs": [{"symbol": ATL, "side": "SIDE_BUY"}, {"symbol": NYY, "side": "SIDE_SELL"}]}
        sent = route.calls.last.request
        ed_key.public_key().verify(
            base64.b64decode(sent.headers["X-PM-Signature"]), f"{sent.headers['X-PM-Timestamp']}POST/v1/combos".encode(),
        )
        assert isinstance(combo, Combo) and combo.market_id == f"polymarket_us:{COMBO}" and combo.venue == "polymarket_us"
        assert [(l.market_id, l.outcome) for l in combo.legs] == [(f"polymarket_us:{ATL}", "yes"), (f"polymarket_us:{NYY}", "no")]
        assert combo.status == "open" and combo.created_at == 1785333600000 and combo.info["tickSize"] == 0.001

    async def test_create_is_idempotent(self, us, venue):
        route = venue.post("/v1/combos").respond(json=sample("create_combo"))
        legs = [ComboLeg(market_id=ATL, outcome="yes"), ComboLeg(market_id=NYY, outcome="no")]
        first, second = await us.create_combo(legs), await us.create_combo(legs)
        assert first.market_id == second.market_id and route.call_count == 2
        assert body(route) == json.loads(route.calls[0].request.content)

    async def test_legs_checked_before_sending(self, us, venue):
        route = venue.post("/v1/combos")
        with pytest.raises(InvalidOrder, match="2 to 50 legs"):
            await us.create_combo([ComboLeg(market_id=ATL, outcome="yes")])
        with pytest.raises(InvalidOrder, match="belongs to kalshi"):
            await us.create_combo([ComboLeg(market_id=ATL, outcome="yes"), ComboLeg(market_id="kalshi:KXA", outcome="yes")])
        with pytest.raises(InvalidOrder, match="each market once"):
            await us.create_combo([ComboLeg(market_id=ATL, outcome="yes"), ComboLeg(market_id=f"polymarket_us:{ATL}", outcome="no")])
        assert not route.called

    async def test_venue_rejection_is_invalid_order(self, us, venue):
        venue.post("/v1/combos").respond(400, json={"code": 3, "message": "legs [0 1] cannot all settle as selected", "details": []})
        with pytest.raises(InvalidOrder, match="cannot all settle as selected"):
            await us.create_combo([ComboLeg(market_id=ATL, outcome="yes"), ComboLeg(market_id=NYY, outcome="yes")])

    async def test_fetch_by_symbol(self, us, venue):
        route = venue.get("/v1/combos").respond(json=sample("get_combos"))
        combo = await us.fetch_combo(f"polymarket_us:{COMBO}")
        assert query(route) == {"symbol": COMBO} and len(combo.legs) == 2

    async def test_unknown_symbol_is_not_found(self, us, venue):
        venue.get("/v1/combos").respond(json=sample("get_combos_empty"))
        with pytest.raises(RfqNotFound, match="no combo"):
            await us.fetch_combo("caoc-nope")

    async def test_no_read_only_lookup(self, us):
        with pytest.raises(NotSupported, match="create_combo returns the existing combo"):
            await us.lookup_combo([ComboLeg(market_id=ATL, outcome="yes"), ComboLeg(market_id=NYY, outcome="no")])


# -- requester (taker) side --------------------------------------------------------


class TestCreateRfq:
    async def test_by_contracts(self, us, venue):
        route = venue.post("/v1/rfqs").respond(json=sample("create_rfq"))
        rfq = await us.create_rfq(f"polymarket_us:{COMBO}", quantity=D("25.50"), rest_remainder=True)
        assert body(route) == {"symbol": COMBO, "restRemainder": True, "qtyDecimal": "25.5"}
        assert isinstance(rfq, Rfq) and rfq.id == "rfq_01" and rfq.status == RfqStatus.OPEN and rfq.mine
        assert rfq.combo_id == f"polymarket_us:{COMBO}" and rfq.market_id is None and rfq.quantity == D("25.50")

    async def test_by_amount_is_cash_order_qty(self, us, venue):
        route = venue.post("/v1/rfqs").respond(json=sample("create_rfq"))
        legs = sample("create_combo")["combo"]
        combo = retail.combo_of(legs)
        rfq = await us.create_rfq(combo.market_id, amount=D("10.00"), combo=combo)
        assert body(route) == {"symbol": COMBO, "restRemainder": False, "cashOrderQty": "10"}
        assert rfq.amount == D("10") and rfq.quantity is None and rfq.combo is combo

    async def test_a_single_market(self, us, venue):
        venue.post("/v1/rfqs").respond(json=sample("create_rfq"))
        rfq = await us.create_rfq(ATL, quantity=D("5"))
        assert rfq.market_id == f"polymarket_us:{ATL}" and rfq.combo_id is None

    async def test_refusals_send_nothing(self, us, venue):
        route = venue.post("/v1/rfqs")
        with pytest.raises(InvalidOrder, match="exactly one"):
            await us.create_rfq(COMBO, quantity=D("1"), amount=D("1"))
        with pytest.raises(InvalidOrder, match="exactly one"):
            await us.create_rfq(COMBO)
        with pytest.raises(InvalidOrder, match="two-sided"):
            await us.create_rfq(COMBO, quantity=D("1"), outcome="yes", action="buy")
        with pytest.raises(InvalidOrder, match="positive"):
            await us.create_rfq(COMBO, amount=D("-1"))
        assert not route.called


class TestReadRfqs:
    async def test_fetch_rfq_normalises_the_documented_rfq(self, us, venue):
        route = venue.get("/v1/rfqs").respond(json={"rfqs": [sample("get_rfqs")["rfqs"][0]], "cursor": ""})
        rfq = await us.fetch_rfq("rfq_01")
        assert query(route) == {"rfqId": "rfq_01"}
        assert rfq.mine and rfq.amount == D("10.0000") and rfq.quantity is None and not rfq.rest_remainder
        assert rfq.combo_id == f"polymarket_us:{COMBO}" and rfq.combo is not None
        assert [(l.market_id, l.outcome) for l in rfq.combo.legs] == [(f"polymarket_us:{ATL}", "yes"), (f"polymarket_us:{NYY}", "no")]
        assert rfq.combo.legs[1].info["settlementPrice"] == "0"   # raw kept, not inverted
        assert rfq.created_at == 1785333600000 and rfq.info["tickSize"] == 0.001 and rfq.account == us.account

    async def test_fetch_rfq_missing(self, us, venue):
        venue.get("/v1/rfqs").respond(json=sample("get_rfqs_empty"))
        with pytest.raises(RfqNotFound):
            await us.fetch_rfq("rfq_nope")

    async def test_fetch_rfqs_is_mine(self, us, venue):
        route = venue.get("/v1/rfqs").respond(json={"rfqs": [sample("get_rfqs")["rfqs"][0]], "cursor": "next-1"})
        page = await us.fetch_rfqs(status="open", market_id=COMBO, limit=10)
        assert query(route) == {"userFilter": "USER_FILTER_SELF", "status": "RFQ_STATUS_OPEN", "symbol": COMBO, "limit": "10"}
        assert [r.id for r in page] == ["rfq_01"] and page.next_cursor == "next-1" and page[0].mine

    async def test_closed_status(self, us, venue):
        closed = {**sample("get_rfqs")["rfqs"][0], "status": "RFQ_STATUS_CLOSED"}
        venue.get("/v1/rfqs").respond(json={"rfqs": [closed]})
        rfq = await us.fetch_rfq("rfq_01")
        assert rfq.status == RfqStatus.CLOSED and rfq.closed_reason is None   # the venue gives no reason

    async def test_cancel_rfq(self, us, venue):
        route = venue.delete("/v1/rfqs/rfq_01").respond(json=sample("empty"))
        assert await us.cancel_rfq("rfq_01") is None
        assert route.called


class TestQuotes:
    async def test_price_mapping(self, us, venue):
        route = venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes"))
        two, one = await us.fetch_quotes("rfq_01")
        assert query(route) == {"rfqId": "rfq_01", "limit": "100"}
        assert isinstance(two, RfqQuote) and two.status == QuoteStatus.OPEN and not two.mine
        # buyPrice is what the requester buys YES at: our ask; sellPrice our bid.
        assert two.ask == D("0.615") and two.bid == D("0.585")
        assert two.ask_quantity == D("16.26") and two.bid_quantity == D("24.09")
        assert two.price_for("yes", "buy") == D("0.615") and two.price_for("no", "buy") == D("0.585")
        assert two.price_for("yes", "sell") == D("0.585") and two.price_for("no", "sell") == D("0.615")
        assert two.market_id == f"polymarket_us:{COMBO}" and two.info["postOnly"] is True
        # "0" marks a side the maker declined.
        assert one.ask == D("0.62") and one.bid is None and one.bid_quantity is None

    async def test_status_filter(self, us, venue):
        route = venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes"))
        await us.fetch_quotes("rfq_01", status=QuoteStatus.OPEN)
        assert query(route)["status"] == "QUOTE_STATUS_ACTIVE"
        with pytest.raises(Exception, match="no 'expired' status"):
            await us.fetch_quotes("rfq_01", status="expired")

    async def test_pages_follow_the_cursor(self, us, venue):
        quotes = sample("get_quotes")["quotes"]
        route = venue.get("/v1/rfqs/quotes")
        route.side_effect = [
            httpx.Response(200, json={"quotes": quotes[:1], "cursor": "c1"}),
            httpx.Response(200, json={"quotes": quotes[1:], "cursor": ""}),
        ]
        assert [q.id for q in await us.fetch_quotes("rfq_01")] == ["quote_01", "quote_02"]
        assert route.calls[1].request.url.params["cursor"] == "c1"

    async def test_executed_records_both_order_ids(self, us, venue):
        venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes_executed"))
        quote = await us.fetch_quote("quote_01", rfq_id="rfq_01")
        assert quote.status == QuoteStatus.EXECUTED and quote.is_terminal
        assert quote.taker_order_id == "ord_requester_01" and quote.maker_order_id == "ord_maker_01"
        assert quote.accepted_side == Side.BUY
        assert quote.confirm_by == 1785333604000 and quote.execute_by == 1785333603000

    async def test_deleted_reads_as_canceled(self, us, venue):
        venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes_deleted"))
        assert (await us.fetch_quote("quote_01", rfq_id="rfq_01")).status == QuoteStatus.CANCELED

    async def test_fetch_quote_needs_its_rfq(self, us, venue):
        with pytest.raises(InvalidOrder, match="give rfq_id"):
            await us.fetch_quote("quote_01")
        venue.get("/v1/rfqs/quotes").respond(json={"quotes": [], "cursor": ""})
        with pytest.raises(RfqNotFound):
            await us.fetch_quote("quote_xx", rfq_id="rfq_01")


class TestAccept:
    @pytest.mark.parametrize("outcome, action, wire", [
        ("yes", "buy", "SIDE_BUY"),    # buy YES at buyPrice
        ("no", "sell", "SIDE_BUY"),    # selling NO is a YES buy
        ("yes", "sell", "SIDE_SELL"),  # sell YES at sellPrice
        ("no", "buy", "SIDE_SELL"),    # buying NO is a YES sell (short) on this netting venue
    ])
    async def test_both_sides(self, us, venue, outcome, action, wire):
        venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes"))
        route = venue.put("/v1/rfqs/rfq_01/quotes/quote_01/accept").respond(json=sample("empty"))
        quote = await us.accept_quote(accept(outcome=outcome, action=action))
        assert body(route) == {"acceptedSide": wire}
        assert quote.status == QuoteStatus.ACCEPTED and quote.accepted_side == Side(wire[5:].lower())
        assert quote.info["accept"]["client_order_id"] == "rfq-accept:polymarket_us:quote_01"
        assert quote.info["accept"]["resent"] is True

    async def test_worst_price_refuses_before_sending(self, us, venue):
        venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes"))
        route = venue.put("/v1/rfqs/rfq_01/quotes/quote_01/accept")
        with pytest.raises(InvalidOrder, match="worse than the worst price 0.6"):
            await us.accept_quote(accept(worst_price=D("0.60")))          # ask 0.615 > 0.60
        with pytest.raises(InvalidOrder, match="worse than the worst price 0.59"):
            await us.accept_quote(accept(action="sell", worst_price=D("0.59")))   # bid 0.585 < 0.59
        with pytest.raises(InvalidOrder, match="worse"):
            await us.accept_quote(accept(outcome="no", action="buy", worst_price=D("0.59")))   # NO at 0.415, YES 0.585
        assert not route.called

    async def test_worst_price_met_is_sent(self, us, venue):
        venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes"))
        route = venue.put("/v1/rfqs/rfq_01/quotes/quote_01/accept").respond(json={})
        await us.accept_quote(accept(worst_price=D("0.615"), client_order_id="mine-1"))
        assert route.called

    async def test_declined_side_is_refused(self, us, venue):
        venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes"))
        route = venue.put("/v1/rfqs/rfq_01/quotes/quote_02/accept")
        with pytest.raises(InvalidOrder, match="declines the sell side"):
            await us.accept_quote(accept(quote_id="quote_02", action="sell"))
        assert not route.called

    async def test_retry_after_accept_does_not_resend(self, us, venue):
        venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes_accepted"))
        route = venue.put("/v1/rfqs/rfq_01/quotes/quote_01/accept")
        quote = await us.accept_quote(accept())
        assert not route.called and quote.status == QuoteStatus.ACCEPTED and quote.info["accept"]["resent"] is False

    async def test_retry_after_execution_returns_the_order_id(self, us, venue):
        venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes_executed"))
        route = venue.put("/v1/rfqs/rfq_01/quotes/quote_01/accept")
        quote = await us.accept_quote(accept())
        assert not route.called and quote.taker_order_id == "ord_requester_01"

    async def test_accepted_on_the_other_side_is_refused(self, us, venue):
        venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes_accepted"))
        with pytest.raises(OrderRejected, match="already accepted") as caught:
            await us.accept_quote(accept(action="sell"))
        assert caught.value.reason == "already_accepted"

    async def test_inactive_quote_is_refused(self, us, venue):
        venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes_deleted"))
        route = venue.put("/v1/rfqs/rfq_01/quotes/quote_01/accept")
        with pytest.raises(OrderRejected, match="QUOTE_STATUS_DELETED, not active"):
            await us.accept_quote(accept())
        assert not route.called

    async def test_full_size_only_and_rfq_id_needed(self, us):
        with pytest.raises(InvalidOrder, match="give rfq_id"):
            await us.accept_quote(accept(rfq_id=None))
        with pytest.raises(InvalidOrder, match="full size"):
            await us.accept_quote(accept(quantity=D("5")))
        with pytest.raises(InvalidOrder, match="full size"):
            await us.accept_quote(accept(amount=D("5")))


# -- quoter (maker) side -----------------------------------------------------------


class TestQuoter:
    async def test_incoming_leaves_out_our_own(self, us, venue):
        route = venue.get("/v1/rfqs").respond(json=sample("get_rfqs"))
        page = await us.fetch_incoming_rfqs()
        assert query(route) == {"status": "RFQ_STATUS_OPEN"}
        assert [r.id for r in page] == ["rfq_02"]
        rfq = page[0]
        assert not rfq.mine and rfq.account is None and rfq.quantity == D("25") and rfq.rest_remainder
        assert rfq.combo_id == "polymarket_us:crossd-example" and rfq.combo is None   # no inline legs: prefix

    async def test_send_quote_maps_ask_and_bid(self, us, venue):
        route = venue.post("/v1/rfqs/quotes").respond(json=sample("create_quote"))
        quote = await us.send_quote(MakerQuote(rfq_id="rfq_02", bid=D("0.5850"), ask=D("0.615"), post_only=True, client_order_id="c1"))
        assert body(route) == {"rfqId": "rfq_02", "buyPrice": "0.615", "sellPrice": "0.585", "restRemainder": False, "postOnly": True}
        assert quote.id == "quote_09" and quote.mine and quote.status == QuoteStatus.OPEN
        assert quote.ask == D("0.615") and quote.bid == D("0.5850") and quote.info["client_order_id"] == "c1"

    async def test_one_sided_quote_sends_zero(self, us, venue):
        route = venue.post("/v1/rfqs/quotes").respond(json=sample("create_quote"))
        await us.send_quote(MakerQuote(rfq_id="rfq_02", bid=D("0.40"), rest_remainder=True))
        assert body(route) == {"rfqId": "rfq_02", "buyPrice": "0", "sellPrice": "0.4", "restRemainder": True}

    async def test_unsupported_fields_refused(self, us, venue):
        route = venue.post("/v1/rfqs/quotes")
        with pytest.raises(InvalidOrder, match="full size"):
            await us.send_quote(MakerQuote(rfq_id="r", bid=D("0.4"), quantity=D("5")))
        with pytest.raises(InvalidOrder, match="no expiry"):
            await us.send_quote(MakerQuote(rfq_id="r", bid=D("0.4"), expires_at=1))
        assert not route.called

    async def test_replace_gets_a_new_id(self, us, venue):
        route = venue.post("/v1/rfqs/quotes").respond(json={"quoteId": "quote_10"})
        new = await us.replace_quote("quote_09", MakerQuote(rfq_id="rfq_02", bid=D("0.58"), ask=D("0.61")))
        assert new.id == "quote_10" and new.info["replaces"] == "quote_09" and body(route)["rfqId"] == "rfq_02"

    async def test_cancel_quote(self, us, venue):
        route = venue.delete("/v1/rfqs/rfq_02/quotes/quote_09").respond(json={})
        assert await us.cancel_quote("quote_09", rfq_id="rfq_02") is None and route.called
        with pytest.raises(InvalidOrder, match="give rfq_id"):
            await us.cancel_quote("quote_09")

    async def test_accepted_then_confirm(self, us, venue):
        venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes_mine_accepted"))
        [mine] = await us.fetch_my_quotes()
        assert mine.mine and mine.account == us.account and mine.status == QuoteStatus.ACCEPTED
        assert mine.accepted_side == Side.SELL          # the requester sells: we buy at our bid
        assert mine.price_for("yes", "sell") == D("0.585") and mine.confirm_by == 1785333664000
        route = venue.put("/v1/rfqs/rfq_02/quotes/quote_09/confirm").respond(json={})
        confirmed = await us.confirm_quote(mine)
        assert body(route) == {} and confirmed.status == QuoteStatus.CONFIRMED

    async def test_my_quotes_filters(self, us, venue):
        route = venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes_mine"))
        await us.fetch_my_quotes(status="open")
        assert query(route) == {"userFilter": "USER_FILTER_SELF", "status": "QUOTE_STATUS_ACTIVE", "limit": "100"}
        await us.fetch_my_quotes(rfq_id="rfq_02")
        assert query(route) == {"rfqId": "rfq_02", "limit": "100"}

    async def test_mine_from_the_user_id(self, us, venue):
        user = venue.get("/v1/rfqs/user-id").respond(json=sample("user_id"))
        venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes_mine"))
        [quote] = await us.fetch_quotes("rfq_02")
        assert quote.mine and quote.ask_quantity == D("25")
        await us.fetch_quotes("rfq_02")
        assert user.call_count == 1   # read once and kept


# -- errors ------------------------------------------------------------------------


class TestErrors:
    async def test_not_found(self, us, venue):
        venue.delete("/v1/rfqs/rfq_x").respond(404, json={"code": 5, "message": "rfq not found"})
        with pytest.raises(RfqNotFound, match="rfq not found"):
            await us.cancel_rfq("rfq_x")

    async def test_bad_request_is_rejected(self, us, venue):
        venue.post("/v1/rfqs/quotes").respond(400, json={"code": 3, "message": "buyPrice is not aligned to tick 0.001"})
        with pytest.raises(OrderRejected, match="not aligned") as caught:
            await us.send_quote(MakerQuote(rfq_id="rfq_02", ask=D("0.6155")))
        assert caught.value.reason == "3"

    async def test_insufficient_funds(self, us, venue):
        venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes"))
        venue.put("/v1/rfqs/rfq_01/quotes/quote_01/accept").respond(400, json={"code": 9, "message": "insufficient buying power"})
        with pytest.raises(InsufficientFunds):
            await us.accept_quote(accept())

    async def test_auth(self, us, venue):
        venue.post("/v1/rfqs").respond(401, json={"message": "invalid signature"})
        with pytest.raises(AuthenticationError):
            await us.create_rfq(COMBO, quantity=D("1"))

    async def test_rate_limited(self, us, venue, monkeypatch):
        import asyncio

        async def no_sleep(_):
            return None
        monkeypatch.setattr(asyncio, "sleep", no_sleep)
        route = venue.post("/v1/combos").respond(429, json={"status": 429, "message": "Too Many Requests"})
        with pytest.raises(RateLimitExceeded):
            await us.create_combo([ComboLeg(market_id=ATL, outcome="yes"), ComboLeg(market_id=NYY, outcome="no")])
        assert route.call_count == 4   # the HTTP client's retries on 429 only


# -- tolerant normalisers (P3 fix) -------------------------------------------------


class TestTolerantNormalisers:
    """One crossed or malformed quote from the venue must not break the list."""

    async def test_a_crossed_quote_is_kept_flagged_and_a_broken_one_skipped(self, us, venue, caplog):
        rows = sample("get_quotes")["quotes"]
        crossed = {**rows[0], "id": "quote_crossed", "buyPrice": "0.55", "sellPrice": "0.60"}
        broken = {**rows[0], "id": "quote_broken", "buyPrice": "0", "sellPrice": "0"}
        venue.get("/v1/rfqs/quotes").respond(json={"quotes": [crossed, broken, rows[1]], "cursor": ""})
        with caplog.at_level("WARNING", logger="synpath.trading.polymarket_us"):
            quotes = await us.fetch_quotes("rfq_01")
        assert [q.id for q in quotes] == ["quote_crossed", "quote_02"]
        assert quotes[0].crossed is True and quotes[0].bid == D("0.60") and quotes[0].ask == D("0.55")
        assert quotes[1].crossed is False
        messages = " ".join(r.getMessage() for r in caplog.records)
        assert "crossed" in messages and "quote_broken" in messages

    def test_a_user_built_crossed_quote_is_still_refused(self):
        with pytest.raises(ValueError, match="above ask"):
            RfqQuote(id="q", rfq_id="r", venue="polymarket_us", bid=D("0.6"), ask=D("0.5"))

    async def test_a_broken_rfq_is_skipped(self, us, venue, caplog):
        good = sample("get_rfqs")["rfqs"][0]
        broken = {**good, "id": "rfq_broken", "qtyDecimal": "0", "cashOrderQty": "0",
                  "rfqCreatorUserId": "rfquser_me"}
        venue.get("/v1/rfqs").respond(json={"rfqs": [broken, good], "cursor": ""})
        with caplog.at_level("WARNING", logger="synpath.trading.polymarket_us"):
            page = await us.fetch_rfqs()
        assert [r.id for r in page] == [good["id"]]
        assert "rfq_broken" in " ".join(r.getMessage() for r in caplog.records)


# -- per-endpoint pacing -------------------------------------------------------------


class FakeClock:
    """`time.monotonic` and `asyncio.sleep` for the limiter: sleeping moves the clock."""

    def __init__(self):
        self.now = 1000.0
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


class TestPacing:
    """The venue limits its RFQ lists and creates to one request a second per
    endpoint; the adapter paces them so the engine's follower never trips a 429."""

    @pytest.fixture
    def clock(self, monkeypatch):
        import types

        from synpath.trading import limiter as limiter_module

        clock = FakeClock()
        monkeypatch.setattr(limiter_module, "time", types.SimpleNamespace(monotonic=clock.monotonic))
        monkeypatch.setattr(limiter_module, "asyncio", types.SimpleNamespace(sleep=clock.sleep))
        return clock

    @pytest.fixture
    def paced(self, venue, ed_key, clock):
        raw = ed_key.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw,
                                   serialization.NoEncryption())
        return retail.PolymarketUSTrading(
            PolymarketUSCredentials(key_id="key-1", secret_key=base64.b64encode(raw).decode()),
            client=httpx.AsyncClient(), limiter=BudgetLimiter(read_per_second=1000, write_per_second=1000),
        )

    def stamps(self, route, clock, payload):
        times: list[float] = []
        route.mock(side_effect=lambda request: (times.append(clock.now), httpx.Response(200, json=payload))[1])
        return times

    async def test_quote_reads_go_out_one_a_second(self, paced, venue, clock):
        times = self.stamps(venue.get("/v1/rfqs/quotes"), clock, sample("get_quotes"))
        for _ in range(4):
            await paced.fetch_quotes("rfq_01")
        assert len(times) == 4
        gaps = [b - a for a, b in zip(times, times[1:])]
        assert all(gap >= 0.999 for gap in gaps), gaps

    async def test_rfq_and_combo_creates_share_one_pace(self, paced, venue, clock):
        times: list[float] = []
        venue.post("/v1/combos").mock(side_effect=lambda r: (times.append(clock.now),
                                                               httpx.Response(200, json=sample("create_combo")))[1])
        venue.post("/v1/rfqs").mock(side_effect=lambda r: (times.append(clock.now),
                                                             httpx.Response(200, json=sample("create_rfq")))[1])
        legs = [ComboLeg(market_id=f"polymarket_us:{ATL}", outcome="yes"),
                ComboLeg(market_id=f"polymarket_us:{NYY}", outcome="no")]
        await paced.create_combo(legs)
        await paced.create_rfq(f"polymarket_us:{COMBO}", amount=D("10"))
        assert len(times) == 2 and times[1] - times[0] >= 0.999

    async def test_groups_do_not_wait_on_each_other_and_writes_are_not_paced(self, paced, venue, clock):
        venue.get("/v1/rfqs").respond(json=sample("get_rfqs"))
        venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes"))
        venue.delete("/v1/rfqs/rfq_01").respond(json=sample("empty"))
        start = clock.now
        await paced.fetch_rfqs()
        await paced.fetch_quotes("rfq_01")
        await paced.cancel_rfq("rfq_01")
        await paced.cancel_rfq("rfq_01")
        assert clock.now == start, "one call per group, and the cancels, go at once"

    async def test_a_queue_longer_than_the_deadline_refuses_before_sending(self, paced, venue, clock):
        from synpath.trading.errors import RateBudgetExceeded

        route = venue.get("/v1/rfqs/quotes").respond(json=sample("get_quotes"))
        pacer = paced.pacers["rfq_quotes"]
        for _ in range(int(retail.PACE_MAX_WAIT_S) + 2):
            pacer.reserve(1, "write")        # others already queued in this group
        with pytest.raises(RateBudgetExceeded):
            await paced.fetch_quotes("rfq_01")
        assert not route.called

    def test_pacing_can_be_turned_off(self, ed_key):
        raw = ed_key.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw,
                                   serialization.NoEncryption())
        us = retail.PolymarketUSTrading(PolymarketUSCredentials(key_id="k", secret_key=base64.b64encode(raw).decode()),
                                        client=httpx.AsyncClient(), rfq_pace_per_second=None)
        assert us.pacers == {}
        assert set(retail.PACED_ENDPOINTS) == {("GET", "/v1/rfqs"), ("GET", "/v1/rfqs/quotes"),
                                               ("POST", "/v1/rfqs"), ("POST", "/v1/combos")}
