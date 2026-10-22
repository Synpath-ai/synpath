"""Kalshi RFQ and combos on the typed shared interface, offline (P6 of docs/rfq-combo.md).

Nothing here was captured live. The samples (`tests/samples/kalshi_rfq_*.json`)
follow https://docs.kalshi.com/openapi.yaml (v3.34.0) field for field, and the
shapes the P0 demo run saw: a quote with `yes_bid_dollars`, `no_bid_dollars`,
`yes_contracts_fp`, `no_contracts_fp`, statuses open, accepted, confirmed,
executed and cancelled, `rfq_creator_order_id` and `creator_order_id`; RFQs
with `mve_collection_ticker`, `mve_selected_legs` and `target_cost_dollars`.
The accept side mapping is the one P0 settled: buying YES sends
`accepted_side=no`.
"""
from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from synpath.errors import BadRequest, MarketNotFound, NotSupported
from synpath.trading import (
    AcceptQuote, Combo, ComboLeg, MakerQuote, QuoteStatus, Rfq, RfqQuote, RfqStatus, Side,
)
from synpath.trading import kalshi as k
from synpath.trading.base import TradingExchange
from synpath.trading.credentials import KalshiCredentials
from synpath.trading.errors import InsufficientFunds, InvalidOrder, OrderRejected, RfqNotFound
from synpath.trading.limiter import BudgetLimiter
from synpath.trading.types import RfqCloseReason

D = Decimal
pytestmark = pytest.mark.anyio
BASE = k.BASE_URLS["demo"]
SAMPLES = Path(__file__).parent / "samples"
COMBO = "KXMVECROSSCATEGORY-S2026GBYNMLIOB4P5GM6R-6TO3QWLMW"
COLL = "KXMVECROSSCATEGORY-R"
LEG_A = "KXNFLGAME-26OCT25KCDEN-KC"
LEG_B = "KXNBAGAME-26OCT25LALBOS-BOS"
EVENT_A, EVENT_B = "KXNFLGAME-26OCT25KCDEN", "KXNBAGAME-26OCT25LALBOS"
RFQ = "5b0e3c1e-8c0f-4c7e-9d1a-2f6a0c3b9e11"
QUOTE = "9d41f0aa-6b2e-4f51-8c33-0e7b5a1d2c44"
INCOMING = "e8d7c6b5-a4f3-4e2d-9c1b-0a9f8e7d6c5b"
TAKER_ORDER = "01a1b2c3-d4e5-7f60-8a9b-0c1d2e3f4a5b"
QUOTE_PATH = f"/communications/rfqs/{RFQ}/quotes/{QUOTE}"


def sample(name: str):
    return json.loads((SAMPLES / f"kalshi_rfq_{name}.json").read_text())


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="module")
def pem() -> bytes:
    return ed25519.Ed25519PrivateKey.generate().private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption(),
    )


@pytest.fixture
def venue():
    with respx.mock(base_url=BASE, assert_all_called=False) as mock:
        mock.get("/communications/id").respond(json=sample("communications_id"))
        yield mock


@pytest.fixture
def kalshi(venue, pem):
    return k.KalshiTrading(
        KalshiCredentials(key_id="key-1", private_key_pem=pem, env="demo"),
        client=httpx.AsyncClient(), limiter=BudgetLimiter(read_per_second=1000, write_per_second=1000),
    )


def body(route) -> dict:
    return json.loads(route.calls.last.request.content)


def query(route) -> dict:
    return dict(route.calls.last.request.url.params)


def accept(**kw) -> AcceptQuote:
    return AcceptQuote(**{"quote_id": QUOTE, "rfq_id": RFQ, "outcome": "yes", "action": "buy", **kw})


def legs(**kw) -> list[ComboLeg]:
    return [ComboLeg(market_id=f"kalshi:{LEG_A}", outcome="yes", event_id=EVENT_A, **kw),
            ComboLeg(market_id=f"kalshi:{LEG_B}", outcome="no", event_id=EVENT_B, **kw)]


# -- interface ---------------------------------------------------------------------


class TestInterface:
    def test_capability_keys(self):
        has = k.KalshiTrading.has
        assert has["rfq_taker"] is True and has["rfq_quoter"] is True and has["combos"] is True
        assert has["rfq"] is True     # the older key stays for one release

    @pytest.mark.parametrize("name", [
        "create_combo", "fetch_combo", "lookup_combo", "create_rfq", "fetch_rfq", "fetch_rfqs", "cancel_rfq",
        "fetch_quotes", "fetch_quote", "accept_quote", "fetch_incoming_rfqs", "send_quote", "replace_quote",
        "cancel_quote", "confirm_quote", "fetch_my_quotes",
    ])
    def test_every_shared_method_is_answered(self, name):
        assert getattr(k.KalshiTrading, name) is not getattr(TradingExchange, name)

    def test_the_raw_methods_are_gone(self):
        assert not hasattr(k.KalshiTrading, "delete_rfq")

    async def test_rfq_calls_are_signed(self, kalshi, venue):
        route = venue.get(f"/communications/rfqs/{RFQ}").respond(json=sample("get_rfq"))
        await kalshi.fetch_rfq(RFQ)
        headers = route.calls.last.request.headers
        assert headers["KALSHI-ACCESS-KEY"] == "key-1" and headers["KALSHI-ACCESS-SIGNATURE"]


# -- normalisers -------------------------------------------------------------------


class TestQuoteOf:
    def test_two_sided_quote_in_yes_prices(self):
        quote = k.quote_of(sample("get_quote_open")["quote"], me="comm-self-7f3a")
        assert quote.bid == D("0.45")            # yes_bid: what we sell YES at
        assert quote.ask == D("0.45")            # 1 - no_bid 0.55: what we buy YES at
        assert quote.bid_quantity == D("5") and quote.ask_quantity == D("5")
        assert quote.status == QuoteStatus.OPEN and quote.market_id == f"kalshi:{COMBO}"
        assert quote.mine is False and quote.rfq_id == RFQ
        assert quote.created_at == 1792594933031

    def test_a_zero_bid_is_a_declined_side(self):
        quote = k.quote_of(sample("get_quote_no_only")["quote"])
        assert quote.bid is None and quote.bid_quantity is None
        assert quote.ask == D("0.408") and quote.ask_quantity == D("5")
        assert quote.price_for("yes", "buy") == D("0.408") and quote.price_for("no", "buy") is None

    @pytest.mark.parametrize("name, status", [
        ("open", QuoteStatus.OPEN), ("accepted", QuoteStatus.ACCEPTED), ("confirmed", QuoteStatus.CONFIRMED),
        ("executed", QuoteStatus.EXECUTED), ("cancelled", QuoteStatus.CANCELED),
    ])
    def test_statuses(self, name, status):
        assert k.quote_of(sample(f"get_quote_{name}")["quote"]).status == status

    def test_accepted_no_is_a_yes_buy(self):
        """P0: the requester that bought YES had sent `accepted_side=no`."""
        assert k.quote_of(sample("get_quote_accepted")["quote"]).accepted_side == Side.BUY
        raw = {**sample("get_quote_accepted")["quote"], "accepted_side": "yes"}
        assert k.quote_of(raw).accepted_side == Side.SELL

    def test_executed_carries_both_order_ids(self):
        quote = k.quote_of(sample("get_quote_executed")["quote"])
        assert quote.taker_order_id == TAKER_ORDER
        assert quote.maker_order_id == "01a1b2c3-d4e5-7f60-8a9b-ffffeeee0001"
        assert quote.is_terminal

    def test_sizes_by_side_on_an_amount_rfq(self):
        quote = k.quote_of(sample("get_quotes_amount")["quotes"][0])
        assert quote.ask_quantity == D("22.72")   # yes_contracts_fp: YES the requester can buy
        assert quote.bid_quantity == D("20.83")   # no_contracts_fp: NO the requester can buy

    def test_mine_from_the_communications_id(self):
        raw = sample("get_quotes_mine")["quotes"][0]
        assert k.quote_of(raw, me="comm-self-7f3a").mine is True
        assert k.quote_of(raw, me="someone").mine is False


class TestRfqOf:
    def test_a_combo_rfq_with_its_legs(self):
        rfq = k.rfq_of(sample("get_rfq")["rfq"], me="comm-self-7f3a")
        assert rfq.combo_id == f"kalshi:{COMBO}" and rfq.market_id is None
        assert rfq.combo.collection_id == COLL
        assert [(l.market_id, l.outcome, l.event_id) for l in rfq.combo.legs] == [
            (f"kalshi:{LEG_A}", "yes", EVENT_A), (f"kalshi:{LEG_B}", "no", EVENT_B)]
        assert rfq.quantity == D("5") and rfq.amount is None
        assert rfq.mine is True and rfq.status == RfqStatus.OPEN and rfq.outcome is None

    def test_sized_by_target_cost(self):
        rfq = k.rfq_of(sample("get_rfq_amount")["rfq"], mine=True)
        assert rfq.amount == D("12.5") and rfq.quantity is None
        assert rfq.market_id == f"kalshi:{LEG_A}" and rfq.combo_id is None
        assert rfq.info["target_cost_excludes_fees"] is True

    def test_closed_reasons(self):
        rfq = k.rfq_of(sample("get_rfq_expired")["rfq"], mine=True)
        assert rfq.status == RfqStatus.CLOSED and rfq.closed_reason == RfqCloseReason.EXPIRED
        cancelled = k.rfq_of(sample("get_rfqs_mine")["rfqs"][1], mine=True)
        assert cancelled.closed_reason == RfqCloseReason.CANCELED

    def test_a_kxmve_ticker_alone_marks_a_combo(self):
        raw = {**sample("get_rfq")["rfq"], "mve_collection_ticker": "", "mve_selected_legs": []}
        rfq = k.rfq_of(raw, mine=True)
        assert rfq.combo_id == f"kalshi:{COMBO}" and rfq.combo is None


class TestComboOf:
    def test_market_to_combo(self):
        combo = k.combo_of(sample("get_market_combo")["market"])
        assert combo.market_id == f"kalshi:{COMBO}" and combo.collection_id == COLL
        assert combo.status == "settled" and len(combo.legs) == 2
        assert combo.legs[1].outcome == "no"

    def test_a_plain_market_is_not_a_combo(self):
        with pytest.raises(RfqNotFound):
            k.combo_of(sample("get_market_leg")["market"])


# -- taker -------------------------------------------------------------------------


class TestCreateRfq:
    async def test_by_contracts(self, kalshi, venue):
        route = venue.post("/communications/rfqs").respond(201, json=sample("create_rfq"))
        rfq = await kalshi.create_rfq(f"kalshi:{COMBO}", quantity=D("5"))
        assert body(route) == {"market_ticker": COMBO, "rest_remainder": False, "contracts_fp": "5.00"}
        assert rfq.id == RFQ and rfq.combo_id == f"kalshi:{COMBO}" and rfq.quantity == D("5")
        assert rfq.status == RfqStatus.OPEN and rfq.mine

    async def test_by_amount_as_target_cost(self, kalshi, venue):
        route = venue.post("/communications/rfqs").respond(201, json=sample("create_rfq"))
        rfq = await kalshi.create_rfq(f"kalshi:{LEG_A}", amount=D("12.50"), rest_remainder=True)
        assert body(route) == {"market_ticker": LEG_A, "rest_remainder": True, "target_cost_dollars": "12.5000"}
        assert rfq.amount == D("12.50") and rfq.quantity is None and rfq.market_id == f"kalshi:{LEG_A}"

    async def test_target_cost_excludes_fees(self, kalshi, venue):
        route = venue.post("/communications/rfqs").respond(201, json=sample("create_rfq"))
        await kalshi.create_rfq(LEG_A, amount=D("10"), target_cost_excludes_fees=True)
        assert body(route)["target_cost_excludes_fees"] is True
        with pytest.raises(InvalidOrder, match="amount"):
            await kalshi.create_rfq(LEG_A, quantity=D("5"), target_cost_excludes_fees=True)

    async def test_refusals_send_nothing(self, kalshi, venue):
        route = venue.post("/communications/rfqs").respond(201, json=sample("create_rfq"))
        with pytest.raises(InvalidOrder, match="two-sided"):
            await kalshi.create_rfq(LEG_A, quantity=D("5"), outcome="yes", action="buy")
        with pytest.raises(InvalidOrder, match="exactly one"):
            await kalshi.create_rfq(LEG_A)
        with pytest.raises(InvalidOrder):
            await kalshi.create_rfq(LEG_A, quantity=D("1.005"))      # off the 0.01 grid
        with pytest.raises(InvalidOrder):
            await kalshi.create_rfq("polymarket_us:caoc-x", quantity=D("1"))
        assert not route.called

    async def test_one_per_market_is_a_rejection(self, kalshi, venue):
        venue.post("/communications/rfqs").respond(409, json={"error": {"code": "conflict", "message": "an open RFQ exists"}})
        with pytest.raises(OrderRejected):
            await kalshi.create_rfq(LEG_A, quantity=D("5"))


class TestReadRfqs:
    async def test_fetch_rfq(self, kalshi, venue):
        venue.get(f"/communications/rfqs/{RFQ}").respond(json=sample("get_rfq"))
        rfq = await kalshi.fetch_rfq(RFQ)
        assert isinstance(rfq, Rfq) and rfq.mine and rfq.combo is not None

    async def test_missing_rfq(self, kalshi, venue):
        venue.get(f"/communications/rfqs/{RFQ}").respond(404, json={"error": {"code": "not_found", "message": "not found"}})
        with pytest.raises(RfqNotFound):
            await kalshi.fetch_rfq(RFQ)

    async def test_fetch_rfqs_is_our_own(self, kalshi, venue):
        route = venue.get("/communications/rfqs").respond(json=sample("get_rfqs_mine"))
        page = await kalshi.fetch_rfqs(status="open", market_id=f"kalshi:{COMBO}", limit=50)
        assert query(route) == {"user_filter": "self", "status": "open", "market_ticker": COMBO, "limit": "50"}
        assert [r.mine for r in page] == [True, True] and page.next_cursor is None

    async def test_cancel_rfq(self, kalshi, venue):
        route = venue.delete(f"/communications/rfqs/{RFQ}").respond(204)
        assert await kalshi.cancel_rfq(RFQ) is None
        assert route.called


class TestReadQuotes:
    async def test_fetch_quotes_on_our_rfq(self, kalshi, venue):
        route = venue.get("/communications/quotes").respond(json=sample("get_quotes"))
        quotes = await kalshi.fetch_quotes(RFQ, status="canceled")
        assert query(route) == {"rfq_user_filter": "self", "rfq_id": RFQ, "status": "cancelled", "limit": "500"}
        assert len(quotes) == 2 and all(isinstance(q, RfqQuote) and not q.mine for q in quotes)
        assert quotes[1].bid is None

    async def test_rows_for_another_rfq_are_dropped(self, kalshi, venue):
        venue.get("/communications/quotes").respond(json=sample("get_quotes_amount"))
        assert await kalshi.fetch_quotes(RFQ) == []

    async def test_pages_are_followed(self, kalshi, venue):
        first = {**sample("get_quotes"), "cursor": "next"}
        route = venue.get("/communications/quotes").mock(side_effect=[
            httpx.Response(200, json=first), httpx.Response(200, json={"quotes": [], "cursor": ""})])
        assert len(await kalshi.fetch_quotes(RFQ)) == 2
        assert route.call_count == 2 and query(route)["cursor"] == "next"

    async def test_statuses_the_venue_lacks_are_refused(self, kalshi, venue):
        with pytest.raises(BadRequest, match="expired"):
            await kalshi.fetch_quotes(RFQ, status="expired")

    async def test_fetch_quote(self, kalshi, venue):
        venue.get(QUOTE_PATH).respond(json=sample("get_quote_open"))
        quote = await kalshi.fetch_quote(QUOTE, rfq_id=RFQ)
        assert quote.id == QUOTE and quote.status == QuoteStatus.OPEN
        with pytest.raises(InvalidOrder, match="rfq_id"):
            await kalshi.fetch_quote(QUOTE)

    async def test_missing_quote(self, kalshi, venue):
        venue.get(QUOTE_PATH).respond(404, json={"error": {"code": "not_found", "message": "not found"}})
        with pytest.raises(RfqNotFound):
            await kalshi.fetch_quote(QUOTE, rfq_id=RFQ)


class TestAccept:
    @pytest.mark.parametrize("outcome, action, wire, price", [
        ("yes", "buy", "no", D("0.45")),     # P0: buy YES takes the maker's NO bid, at 1 - no_bid
        ("no", "sell", "no", D("0.45")),
        ("no", "buy", "yes", D("0.45")),     # buy NO takes the YES bid, at 1 - yes_bid a NO contract
        ("yes", "sell", "yes", D("0.45")),
    ])
    async def test_side_mapping(self, kalshi, venue, outcome, action, wire, price):
        venue.get(QUOTE_PATH).respond(json=sample("get_quote_open"))
        route = venue.put(f"{QUOTE_PATH}/accept").respond(204)
        quote = await kalshi.accept_quote(accept(outcome=outcome, action=action))
        assert body(route) == {"accepted_side": wire}
        assert quote.status == QuoteStatus.ACCEPTED
        assert quote.accepted_side == (Side.BUY if wire == "no" else Side.SELL)
        assert quote.price_for(outcome, action) == price
        assert quote.info["accept"]["client_order_id"] == f"rfq-accept:kalshi:{QUOTE}"

    async def test_buy_yes_at_1_minus_no_bid(self, kalshi, venue):
        venue.get(QUOTE_PATH).respond(json=sample("get_quote_no_only"))
        route = venue.put(f"{QUOTE_PATH}/accept").respond(204)
        await kalshi.accept_quote(accept(worst_price=D("0.41")))
        assert body(route) == {"accepted_side": "no"}

    async def test_a_declined_side_is_refused_before_sending(self, kalshi, venue):
        venue.get(QUOTE_PATH).respond(json=sample("get_quote_no_only"))
        route = venue.put(f"{QUOTE_PATH}/accept").respond(204)
        with pytest.raises(InvalidOrder, match="yes_bid is 0"):
            await kalshi.accept_quote(accept(outcome="no", action="buy"))
        assert not route.called

    @pytest.mark.parametrize("outcome, action, worst", [
        ("yes", "buy", D("0.44")),     # buying YES at 0.45, worst 0.44
        ("yes", "sell", D("0.46")),    # selling YES at 0.45, worst 0.46
        ("no", "buy", D("0.46")),      # buying NO is selling YES at 0.45
    ])
    async def test_worse_than_worst_price_is_refused(self, kalshi, venue, outcome, action, worst):
        venue.get(QUOTE_PATH).respond(json=sample("get_quote_open"))
        route = venue.put(f"{QUOTE_PATH}/accept").respond(204)
        with pytest.raises(InvalidOrder, match="worst price"):
            await kalshi.accept_quote(accept(outcome=outcome, action=action, worst_price=worst))
        assert not route.called

    async def test_at_the_worst_price_is_sent(self, kalshi, venue):
        venue.get(QUOTE_PATH).respond(json=sample("get_quote_open"))
        route = venue.put(f"{QUOTE_PATH}/accept").respond(204)
        await kalshi.accept_quote(accept(worst_price=D("0.45")))
        assert route.called

    async def test_a_retry_on_the_same_side_does_not_resend(self, kalshi, venue):
        venue.get(QUOTE_PATH).respond(json=sample("get_quote_accepted"))
        route = venue.put(f"{QUOTE_PATH}/accept").respond(204)
        quote = await kalshi.accept_quote(accept())
        assert not route.called and quote.info["accept"]["resent"] is False
        with pytest.raises(OrderRejected, match="already accepted"):
            await kalshi.accept_quote(accept(outcome="no", action="buy"))

    async def test_a_closed_quote_is_refused(self, kalshi, venue):
        venue.get(QUOTE_PATH).respond(json=sample("get_quote_cancelled"))
        with pytest.raises(OrderRejected, match="not open"):
            await kalshi.accept_quote(accept())

    async def test_full_size_only_and_rfq_id_required(self, kalshi, venue):
        with pytest.raises(InvalidOrder, match="full size"):
            await kalshi.accept_quote(accept(quantity=D("2")))
        with pytest.raises(InvalidOrder, match="rfq_id"):
            await kalshi.accept_quote(accept(rfq_id=None))

    async def test_venue_refusal_is_typed(self, kalshi, venue):
        venue.get(QUOTE_PATH).respond(json=sample("get_quote_open"))
        venue.put(f"{QUOTE_PATH}/accept").respond(400, json={"error": {"code": "invalid_parameters", "message": "invalid parameters"}})
        with pytest.raises(OrderRejected) as caught:
            await kalshi.accept_quote(accept())
        assert caught.value.reason == "invalid_parameters"


# -- quoter ------------------------------------------------------------------------


class TestQuoter:
    async def test_incoming_leaves_ours_out(self, kalshi, venue):
        route = venue.get("/communications/rfqs").respond(json=sample("get_rfqs_open"))
        page = await kalshi.fetch_incoming_rfqs(market_id=f"kalshi:{LEG_A}")
        assert query(route) == {"status": "open", "market_ticker": LEG_A}
        assert [r.id for r in page] == [INCOMING] and page[0].mine is False and page[0].quantity == D("20")

    @pytest.mark.parametrize("bid, ask, yes_bid, no_bid", [
        (D("0.40"), D("0.45"), "0.4000", "0.5500"),
        (D("0.40"), None, "0.4000", "0.0000"),
        (None, D("0.62"), "0.0000", "0.3800"),
    ])
    async def test_send_quote_price_mapping(self, kalshi, venue, bid, ask, yes_bid, no_bid):
        route = venue.post("/communications/quotes").respond(201, json=sample("create_quote"))
        quote = await kalshi.send_quote(MakerQuote(rfq_id=INCOMING, bid=bid, ask=ask, post_only=True))
        assert body(route) == {"rfq_id": INCOMING, "yes_bid": yes_bid, "no_bid": no_bid,
                               "rest_remainder": False, "post_only": True}
        assert quote.id == "4c5d6e7f-8091-4a2b-b3c4-d5e6f7081920" and quote.mine
        assert quote.bid == bid and quote.ask == ask and quote.status == QuoteStatus.OPEN

    async def test_send_quote_refuses_what_the_venue_lacks(self, kalshi, venue):
        route = venue.post("/communications/quotes").respond(201, json=sample("create_quote"))
        with pytest.raises(InvalidOrder, match="quantity"):
            await kalshi.send_quote(MakerQuote(rfq_id=INCOMING, bid=D("0.4"), quantity=D("5")))
        with pytest.raises(InvalidOrder, match="expiry"):
            await kalshi.send_quote(MakerQuote(rfq_id=INCOMING, bid=D("0.4"), expires_at=1))
        assert not route.called

    async def test_insufficient_balance_is_insufficient_funds(self, kalshi, venue):
        """P0: a demo quote with too little balance answered 400 insufficient_balance."""
        venue.post("/communications/quotes").respond(400, json=sample("error_insufficient_balance"))
        with pytest.raises(InsufficientFunds):
            await kalshi.send_quote(MakerQuote(rfq_id=INCOMING, bid=D("0.01"), ask=D("0.99")))

    async def test_replace_is_cancel_then_send(self, kalshi, venue):
        old = "4c5d6e7f-8091-4a2b-b3c4-d5e6f7081920"
        calls: list[str] = []
        venue.delete(f"/communications/rfqs/{INCOMING}/quotes/{old}").mock(
            side_effect=lambda request: calls.append("delete") or httpx.Response(204))
        venue.post("/communications/quotes").mock(
            side_effect=lambda request: calls.append("post") or httpx.Response(201, json={"id": "new-quote"}))
        quote = await kalshi.replace_quote(old, MakerQuote(rfq_id=INCOMING, bid=D("0.41"), ask=D("0.44")))
        assert calls == ["delete", "post"]
        assert quote.id == "new-quote" and quote.info["replaces"] == old

    async def test_replace_of_a_quote_already_gone_still_sends(self, kalshi, venue):
        old = "gone"
        venue.delete(f"/communications/rfqs/{INCOMING}/quotes/{old}").respond(
            404, json={"error": {"code": "not_found", "message": "not found"}})
        route = venue.post("/communications/quotes").respond(201, json={"id": "new-quote"})
        await kalshi.replace_quote(old, MakerQuote(rfq_id=INCOMING, bid=D("0.41")))
        assert route.called

    async def test_cancel_quote(self, kalshi, venue):
        route = venue.delete(f"/communications/rfqs/{INCOMING}/quotes/q1").respond(204)
        await kalshi.cancel_quote("q1", rfq_id=INCOMING)
        assert route.called
        with pytest.raises(InvalidOrder, match="rfq_id"):
            await kalshi.cancel_quote("q1")

    async def test_confirm_quote(self, kalshi, venue):
        route = venue.put(f"/communications/rfqs/{INCOMING}/quotes/q1/confirm").respond(204)
        quote = RfqQuote(id="q1", rfq_id=INCOMING, venue="kalshi", bid=D("0.4"), status=QuoteStatus.ACCEPTED, mine=True)
        confirmed = await kalshi.confirm_quote(quote)
        assert route.called and body(route) == {}
        assert confirmed.status == QuoteStatus.CONFIRMED

    async def test_fetch_my_quotes(self, kalshi, venue):
        route = venue.get("/communications/quotes").respond(json=sample("get_quotes_mine"))
        quotes = await kalshi.fetch_my_quotes(rfq_id=INCOMING, status="open")
        assert query(route) == {"user_filter": "self", "rfq_id": INCOMING, "status": "open", "limit": "500"}
        assert len(quotes) == 1 and quotes[0].mine and quotes[0].bid == D("0.40") and quotes[0].ask == D("0.45")


# -- combos ------------------------------------------------------------------------


class TestCombos:
    async def test_collections(self, kalshi, venue):
        route = venue.get("/multivariate_event_collections").respond(json=sample("get_collections"))
        page = await kalshi.fetch_combo_collections(event_id=f"kalshi:{EVENT_A}", limit=20)
        assert query(route) == {"status": "open", "associated_event_ticker": EVENT_A, "limit": "20"}
        assert page[0]["collection_ticker"] == COLL and page.next_cursor is None

    async def test_create_in_a_named_collection(self, kalshi, venue):
        route = venue.post(f"/multivariate_event_collections/{COLL}").respond(json=sample("create_combo"))
        combo = await kalshi.create_combo(legs(), collection_id=COLL)
        assert body(route) == {"selected_markets": [
            {"market_ticker": LEG_A, "event_ticker": EVENT_A, "side": "yes"},
            {"market_ticker": LEG_B, "event_ticker": EVENT_B, "side": "no"},
        ], "with_market_payload": True}
        assert isinstance(combo, Combo) and combo.market_id == f"kalshi:{COMBO}"
        assert combo.collection_id == COLL and combo.status == "open"
        assert [l.market_id for l in combo.legs] == [f"kalshi:{LEG_A}", f"kalshi:{LEG_B}"]

    async def test_create_finds_the_one_collection(self, kalshi, venue):
        listed = venue.get("/multivariate_event_collections").respond(json=sample("get_collections"))
        venue.post(f"/multivariate_event_collections/{COLL}").respond(json=sample("create_combo"))
        combo = await kalshi.create_combo(legs())
        assert query(listed)["associated_event_ticker"] == EVENT_A
        assert combo.collection_id == COLL

    async def test_a_leg_without_its_event_reads_the_market(self, kalshi, venue):
        venue.get(f"/markets/{LEG_B}").respond(json=sample("get_market_leg"))
        route = venue.post(f"/multivariate_event_collections/{COLL}").respond(
            json={"event_ticker": "E", "market_ticker": COMBO})
        bare = [legs()[0], ComboLeg(market_id=f"kalshi:{LEG_B}", outcome="no")]
        combo = await kalshi.create_combo(bare, collection_id=COLL)
        assert body(route)["selected_markets"][1]["event_ticker"] == EVENT_B
        assert combo.legs[1].event_id == EVENT_B     # no market payload: the legs sent

    async def test_no_or_several_collections_are_refused(self, kalshi, venue):
        two = sample("get_collections")
        two["multivariate_contracts"].append({**two["multivariate_contracts"][0], "collection_ticker": "KXMVEOTHER-R"})
        venue.get("/multivariate_event_collections").mock(side_effect=[
            httpx.Response(200, json=two), httpx.Response(200, json={"multivariate_contracts": []})])
        with pytest.raises(InvalidOrder, match="2 open collections") as refused:
            await kalshi.create_combo(legs())
        assert f"{COLL}, KXMVEOTHER-R" in str(refused.value) and "collection_id" in str(refused.value)
        assert refused.value.collections == [COLL, "KXMVEOTHER-R"]
        with pytest.raises(InvalidOrder, match="no open collection"):
            await kalshi.create_combo(legs())

    async def test_the_three_live_collections_are_ambiguous_until_named(self, kalshi, venue):
        """Kalshi's three open collections take the same events (2026-10-22),
        so legs alone never pick one; naming it skips the listing."""
        three = sample("get_collections")
        row = three["multivariate_contracts"][0]
        names = ["KXMVESPORTSMULTIGAMEEXTENDED-R", "KXMVECROSSCATEGORY-R", "KXMVECROSSCATEGORY-SHARD1-R"]
        three["multivariate_contracts"] = [{**row, "collection_ticker": n} for n in names]
        listed = venue.get("/multivariate_event_collections").respond(json=three)
        with pytest.raises(InvalidOrder, match="3 open collections") as refused:
            await kalshi.create_combo(legs())
        assert all(n in str(refused.value) for n in names)
        made = venue.post(f"/multivariate_event_collections/{names[2]}").respond(json=sample("create_combo"))
        before = listed.call_count
        combo = await kalshi.create_combo(legs(), collection_id=names[2])
        assert made.called and listed.call_count == before, "a named collection is used as given"
        assert combo.collection_id in (names[2], COLL)

    async def test_collections_always_carry_their_event_tickers(self, kalshi, venue):
        bare = sample("get_collections")
        del bare["multivariate_contracts"][0]["associated_event_tickers"]
        venue.get("/multivariate_event_collections").respond(json=bare)
        page = await kalshi.fetch_combo_collections()
        assert page[0]["associated_event_tickers"] == [EVENT_A, EVENT_B]

    async def test_refused_legs_are_invalid(self, kalshi, venue):
        venue.post(f"/multivariate_event_collections/{COLL}").respond(
            400, json={"error": {"code": "invalid_parameters", "message": "markets cannot be combined"}})
        with pytest.raises(InvalidOrder, match="cannot be combined"):
            await kalshi.create_combo(legs(), collection_id=COLL)

    async def test_too_few_or_repeated_legs(self, kalshi, venue):
        with pytest.raises(InvalidOrder, match="two legs"):
            await kalshi.create_combo(legs()[:1], collection_id=COLL)
        with pytest.raises(InvalidOrder, match="once"):
            await kalshi.create_combo([legs()[0], legs()[0]], collection_id=COLL)

    async def test_fetch_combo(self, kalshi, venue):
        venue.get(f"/markets/{COMBO}").respond(json=sample("get_market_combo"))
        combo = await kalshi.fetch_combo(f"kalshi:{COMBO}")
        assert combo.market_id == f"kalshi:{COMBO}" and combo.status == "settled"

    async def test_fetch_combo_missing_or_not_a_combo(self, kalshi, venue):
        venue.get(f"/markets/{COMBO}").respond(404, json={"error": {"code": "not_found", "message": "not found"}})
        with pytest.raises(RfqNotFound):
            await kalshi.fetch_combo(COMBO)
        venue.get(f"/markets/{LEG_B}").respond(json=sample("get_market_leg"))
        with pytest.raises(RfqNotFound, match="not a combo"):
            await kalshi.fetch_combo(LEG_B)

    async def test_no_lookup_by_legs(self, kalshi):
        with pytest.raises(NotSupported, match="create_combo"):
            await kalshi.lookup_combo(legs())


# -- error mapping -----------------------------------------------------------------


class TestErrors:
    def test_not_found_under_communications_is_rfq_not_found(self):
        err = k.map_error(MarketNotFound("kalshi: not found", status=404), path=f"/communications/rfqs/{RFQ}")
        assert isinstance(err, RfqNotFound)
        err = k.map_error(MarketNotFound("kalshi: not found", status=404), path="/multivariate_event_collections/X")
        assert isinstance(err, RfqNotFound)

    def test_insufficient_balance(self):
        err = k.map_error(BadRequest("kalshi: insufficient balance", body=sample("error_insufficient_balance"), status=400),
                          path="/communications/quotes")
        assert isinstance(err, InsufficientFunds)

    def test_order_paths_are_unchanged(self):
        from synpath.trading.errors import OrderNotFound
        err = k.map_error(MarketNotFound("kalshi: not found", status=404), path="/portfolio/orders/x")
        assert isinstance(err, OrderNotFound) and not isinstance(err, RfqNotFound)


# -- through the engine ------------------------------------------------------------


class TestEngine:
    async def test_accept_to_order_fill_and_position(self, kalshi, venue, tmp_path):
        """`engine.rfq` on the Kalshi adapter: create a combo RFQ, read the
        maker's quote, accept YES (sent as `no`), follow the quote to
        executed, adopt `rfq_creator_order_id` and book its fill."""
        from synpath.engine import Engine, EngineConfig, RiskConfig
        from synpath.engine.storage_types import IntentState
        from synpath.trading.types import PositionSide

        state = {"quote": "open"}

        def quote_now(request):
            name = {"open": "get_quote_open", "accepted": "get_quote_accepted", "executed": "get_quote_executed"}
            return httpx.Response(200, json=sample(name[state["quote"]]))

        def accepted(request):
            assert json.loads(request.content) == {"accepted_side": "no"}
            state["quote"] = "accepted"
            return httpx.Response(204)

        venue.post("/communications/rfqs").respond(201, json=sample("create_rfq"))
        venue.get("/communications/quotes").respond(json={"quotes": [sample("get_quote_open")["quote"]], "cursor": ""})
        venue.get(QUOTE_PATH).mock(side_effect=quote_now)
        venue.put(f"{QUOTE_PATH}/accept").mock(side_effect=accepted)
        venue.get(f"/portfolio/orders/{TAKER_ORDER}").respond(json=sample("get_order_rfq"))
        venue.get("/portfolio/fills").respond(json=sample("get_fills_rfq"))
        venue.get("/portfolio/orders").respond(json={"orders": [], "cursor": ""})
        venue.get("/portfolio/positions").respond(json={"market_positions": [], "cursor": ""})
        venue.get("/portfolio/balance").respond(json={"balance": 10000, "balance_dollars": "100.0000"})

        engine = Engine(
            {"kalshi": kalshi},
            EngineConfig(journal_path=str(tmp_path / "engine.db"), in_doubt_timeout_s=0,
                         rfq_poll_interval_s=0.01, rfq_incoming_interval_s=None),
            risk=RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None),
        )
        await engine.start()
        try:
            combo = Combo(market_id=f"kalshi:{COMBO}", venue="kalshi", collection_id=COLL, legs=legs())
            rfq = await engine.rfq.create_rfq(f"kalshi:{COMBO}", quantity=D("5"), combo=combo, book="alpha")
            assert rfq.id == RFQ and rfq.combo_id == f"kalshi:{COMBO}"
            quotes = await kalshi.fetch_quotes(RFQ)
            result = await engine.rfq.accept_quote(
                AcceptQuote(quote_id=quotes[0].id, rfq_id=RFQ, outcome="yes", action="buy",
                            worst_price=D("0.46"), book="alpha"))
            assert result.status == QuoteStatus.ACCEPTED and result.accepted_side == Side.BUY

            state["quote"] = "executed"
            await engine.rfq.tick()
            intent = await engine.journal.intent(f"rfq-accept:kalshi:{QUOTE}")
            assert intent.state == IntentState.SENT and intent.order_id == TAKER_ORDER
            order = await engine.journal.order("kalshi", TAKER_ORDER)
            assert order.side == Side.BUY and order.filled == D("5") and order.tags["quote_id"] == QUOTE
            [position] = engine.positions()
            assert (position.market_id, position.side, position.contracts, position.entry_price) == \
                (f"kalshi:{COMBO}", PositionSide.LONG, D("5"), D("0.45"))
            assert position.info["book"] == "alpha" and len(position.info["combo"]["legs"]) == 2
        finally:
            await engine.stop()


# -- serve route -------------------------------------------------------------------


class TestCollectionsRoute:
    async def test_get_combo_collections(self, kalshi, venue, tmp_path):
        from fastapi.testclient import TestClient

        from synpath.engine import Engine, EngineConfig, RiskConfig
        from synpath.engine.paper import PaperRfqVenue
        from synpath.server.store import ControlStore
        from synpath.server.trading import create_trading_app
        from synpath.trading.types import Account

        venue.get("/portfolio/orders").respond(json={"orders": [], "cursor": ""})
        venue.get("/portfolio/positions").respond(json={"market_positions": [], "cursor": ""})
        venue.get("/portfolio/balance").respond(json={"balance": 10000, "balance_dollars": "100.0000"})
        route = venue.get("/multivariate_event_collections").respond(json=sample("get_collections"))
        account = Account(venue="kalshi", name="default")
        us = Account(venue="polymarket_us", name="default")
        engine = Engine(
            {"kalshi": kalshi, "polymarket_us": PaperRfqVenue(venue="polymarket_us", account=us)},
            EngineConfig(journal_path=str(tmp_path / "engine.db"), require_lease=True,
                         rfq_incoming_interval_s=None),
            risk=RiskConfig(price_collar=None), accounts={"kalshi": account, "polymarket_us": us},
        )
        await engine.start()
        store = await ControlStore(str(tmp_path / "control.db")).open()
        try:
            owner, _ = await store.bootstrap("owner")
            await store.acting_as(f"user:{owner.id}", "setup")
            viewer = await store.create_user("viewer")
            await store.grant(viewer.id, account.key, "view")
            key = (await store.issue_key(viewer.id)).secret
            outsider = await store.create_user("outsider")
            await store.grant(outsider.id, us.key, "view")
            other = (await store.issue_key(outsider.id)).secret
            with TestClient(create_trading_app(engine, store)) as client:
                answer = client.get("/combos/collections", params={"event_id": f"kalshi:{EVENT_A}"},
                                    headers={"Authorization": f"Bearer {key}"})
                assert answer.status_code == 200, answer.text
                assert answer.json()["data"][0]["collection_ticker"] == COLL
                assert query(route)["associated_event_ticker"] == EVENT_A
                refused = client.get("/combos/collections", headers={"Authorization": f"Bearer {other}"})
                assert refused.status_code == 403
                missing = client.get("/combos/collections", params={"venue": "polymarket_us"},
                                     headers={"Authorization": f"Bearer {other}"})
                assert missing.status_code == 501
        finally:
            await store.close()
            await engine.stop()
