"""The engine's RFQ and combo desk, offline (P3 and P4 of docs/rfq-combo.md).

Everything runs against `PaperRfqVenue`, a paper venue with configurable
makers, on SQLite and (with SYNPATH_TEST_POSTGRES_DSN) PostgreSQL. Covered:
accept to order to fill to position; accept by amount; the worst-price and
risk refusals that send nothing; halt cancelling every RFQ and quote; a
crash between the intent and the call and between the call and its result,
on both sides; the side guard; a combo as its own instrument; the quoter
flows with expiry and confirm through risk; partial fills, a lapsed last
look; and the order the events arrive in.
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from decimal import Decimal as D

import pytest

from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.engine.alerts import Alerts
from synpath.engine.paper import PaperMaker, PaperRfqVenue
from synpath.engine.reconcile import Reconciler
from synpath.engine.storage_types import IntentState
from synpath.errors import BadRequest
from synpath.trading.errors import InvalidOrder, RiskRejected
from synpath.trading.types import (
    AcceptQuote, ComboLeg, MakerQuote, OrderRequest, OrderStatus, OrderType, PositionSide, QuoteStatus, RfqStatus,
    Side,
)

pytestmark = pytest.mark.anyio

VENUE = "polymarket_us"
MARKET = "polymarket_us:tec-nba-a"
RISK = dict(price_collar=None, duplicate_window_ms=0, closing_soon_s=None)


class Crash(BaseException):
    """The process dies: nothing in the engine catches it."""


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(params=["sqlite", "postgresql"])
async def config(request, tmp_path):
    """An `EngineConfig` factory on each backend; PostgreSQL only with a disposable DSN."""
    base = dict(in_doubt_timeout_s=0, rfq_poll_interval_s=0.01, rfq_incoming_interval_s=0)
    if request.param == "sqlite":
        path = str(tmp_path / "engine.db")
        yield lambda **kw: EngineConfig(journal_path=path, **{**base, **kw})
        return
    dsn = os.environ.get("SYNPATH_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set SYNPATH_TEST_POSTGRES_DSN to a disposable PostgreSQL database")
    psycopg = pytest.importorskip("psycopg")
    schema = "synpath_rfq_" + uuid.uuid4().hex
    try:
        yield lambda **kw: EngineConfig(journal_path=dsn, storage_backend="postgresql", storage_schema=schema,
                                        **{**base, **kw})
    finally:
        from psycopg import sql
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as connection:
            await connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


def paper(**kw) -> PaperRfqVenue:
    return PaperRfqVenue(venue=VENUE, **kw)


async def started(config, venue: PaperRfqVenue, *, lost_after_s: float = 60.0, **risk) -> Engine:
    engine = Engine({VENUE: venue}, config(lost_after_s=lost_after_s), risk=RiskConfig(**{**RISK, **risk}))
    await engine.start()
    return engine


async def crashed(engine: Engine, venue: PaperRfqVenue) -> None:
    """The process dies: the journal closes, nothing else runs."""
    venue.quote_listeners.clear()
    await engine.journal.close()


def events(sub, *prefixes: str) -> list:
    out = []
    while not sub.queue.empty():
        event = sub.queue.get_nowait()
        if not prefixes or event.kind.startswith(prefixes):
            out.append(event)
    return out


def kinds(sub, *prefixes: str) -> list[str]:
    return [e.kind for e in events(sub, *prefixes)]


async def first_quote(venue: PaperRfqVenue, rfq_id: str):
    return (await venue.fetch_quotes(rfq_id))[0]


def accept_of(quote, **kw) -> AcceptQuote:
    base = dict(quote_id=quote.id, rfq_id=quote.rfq_id, outcome="yes", action="buy", worst_price=D("0.50"),
                book="alpha")
    return AcceptQuote(**{**base, **kw})


async def execute(engine: Engine, venue: PaperRfqVenue) -> None:
    """The maker's last look, then the execution, each seen by the follower."""
    for _ in range(2):
        venue.advance()
        await engine.rfq.tick()


async def accept_intents(engine: Engine, operation: str = "accept_quote"):
    return [i for i in await engine.journal.intents() if i.operation == operation]


# -- taker -------------------------------------------------------------------------


class TestAccept:
    async def test_accept_becomes_an_order_a_fill_and_a_position_with_events_in_order(self, config):
        venue = paper()
        engine = await started(config, venue)
        sub = engine.bus.subscribe()
        rfq = await engine.create_rfq(MARKET, quantity=D("10"), book="alpha")
        assert rfq.status == RfqStatus.OPEN and f"{VENUE}:{rfq.id}" in engine.rfq.rfqs
        await engine.rfq.tick()
        quote = await first_quote(venue, rfq.id)

        result = await engine.accept_quote(accept_of(quote))
        assert result.status == QuoteStatus.ACCEPTED, "accept returns at once"
        intent = (await accept_intents(engine))[0]
        assert intent.client_order_id == f"rfq-accept:{VENUE}:{quote.id}"
        assert intent.state == IntentState.SENDING and intent.request["type"] == "rfq_taker"
        assert engine.positions() == []

        await execute(engine, venue)
        intent = await engine.journal.intent(intent.client_order_id)
        assert intent.state == IntentState.SENT and intent.order_id
        order = await engine.journal.order(VENUE, intent.order_id)
        assert order.type == OrderType.RFQ_TAKER and order.status == OrderStatus.CLOSED
        assert (order.outcome, order.action, order.book) == ("yes", "buy", "alpha")
        assert order.tags["quote_id"] == quote.id
        [position] = engine.positions()
        assert (position.market_id, position.side, position.contracts, position.entry_price) == \
            (MARKET, PositionSide.LONG, D("10"), D("0.45"))
        assert position.info["book"] == "alpha"
        fills = await engine.journal.fills()
        assert len(fills) == 1 and fills[0].outcome == "yes" and fills[0].action == "buy"

        assert kinds(sub, "rfq.", "quote.", "order.", "fill.") == [
            "rfq.created", "quote.received", "quote.accepted", "quote.confirmed", "quote.executed",
            "order.accepted", "order.closed", "fill.booked", "rfq.closed",
        ]
        assert f"{VENUE}:{rfq.id}" not in engine.rfq.rfqs
        await engine.stop()

    async def test_wait_follows_the_quote_to_its_end(self, config):
        venue = paper(auto_advance=True)
        engine = await started(config, venue)
        rfq = await engine.create_rfq(MARKET, quantity=D("10"))
        quote = await venue.fetch_quote((await venue.fetch_quotes(rfq.id))[0].id)
        result = await engine.accept_quote(accept_of(quote), wait=True, timeout_s=5)
        assert result.status == QuoteStatus.EXECUTED
        assert engine.positions()[0].contracts == D("10")
        await engine.stop()

    async def test_accept_by_amount_sizes_contracts_down(self, config):
        venue = paper(makers=[PaperMaker(quantity=D("100"))])
        engine = await started(config, venue)
        rfq = await engine.create_rfq(MARKET, quantity=D("100"))
        quote = await first_quote(venue, rfq.id)
        await engine.accept_quote(accept_of(quote, amount=D("9.20")))
        # 9.20 / 0.45 = 20.4, down to whole contracts on Polymarket US.
        assert venue.accepted[quote.id]["quantity"] == D("20")
        intent = (await accept_intents(engine))[0]
        assert D(intent.request["quantity"]) == D("20") and intent.request["params"]["amount"] == "9.20"
        await execute(engine, venue)
        assert engine.positions()[0].contracts == D("20")
        await engine.stop()

    async def test_an_amount_below_one_contract_sends_nothing(self, config):
        venue = paper()
        engine = await started(config, venue)
        rfq = await engine.create_rfq(MARKET, quantity=D("10"))
        quote = await first_quote(venue, rfq.id)
        with pytest.raises(InvalidOrder, match="below the minimum"):
            await engine.accept_quote(accept_of(quote, amount=D("0.20")))
        assert "accept_quote" not in venue.calls and await accept_intents(engine) == []
        await engine.stop()

    async def test_a_quote_worse_than_the_worst_price_is_refused_before_sending(self, config):
        venue = paper()
        engine = await started(config, venue)
        sub = engine.bus.subscribe("risk")
        rfq = await engine.create_rfq(MARKET, quantity=D("10"))
        quote = await first_quote(venue, rfq.id)
        with pytest.raises(RiskRejected) as refused:
            await engine.accept_quote(accept_of(quote, worst_price=D("0.44")))
        assert refused.value.rule == "worst_price"
        # Selling YES takes the bid (0.40): a floor of 0.41 refuses it too.
        with pytest.raises(RiskRejected):
            await engine.accept_quote(accept_of(quote, action="sell", worst_price=D("0.41")))
        assert "accept_quote" not in venue.calls and await accept_intents(engine) == []
        [first, second] = events(sub)
        assert first.payload["rule"] == "worst_price" and first.payload["operation"] == "rfq_taker"
        await engine.stop()

    async def test_a_risk_rule_refuses_the_accept_and_sends_nothing(self, config):
        venue = paper()
        engine = await started(config, venue, max_position_contracts=D("5"))
        sub = engine.bus.subscribe("risk")
        rfq = await engine.create_rfq(MARKET, quantity=D("10"))   # an RFQ commits nothing: allowed
        quote = await first_quote(venue, rfq.id)
        with pytest.raises(RiskRejected) as refused:
            await engine.accept_quote(accept_of(quote))
        assert refused.value.rule == "max_position"
        assert "accept_quote" not in venue.calls and await accept_intents(engine) == []
        assert events(sub)[0].payload["operation"] == "rfq_taker"
        await engine.stop()

    async def test_the_kill_switch_refuses_an_accept_and_an_rfq(self, config):
        venue = paper()
        engine = await started(config, venue)
        rfq = await engine.create_rfq(MARKET, quantity=D("10"))
        quote = await first_quote(venue, rfq.id)
        await engine.halt("operator", policy="hold")     # hold leaves the RFQ up
        assert venue.rfqs[rfq.id].status == RfqStatus.OPEN
        with pytest.raises(RiskRejected) as refused:
            await engine.accept_quote(accept_of(quote))
        assert refused.value.rule == "kill_switch"
        with pytest.raises(RiskRejected):
            await engine.create_rfq(MARKET, quantity=D("1"))
        assert venue.calls == ["create_rfq"]
        await engine.stop()

    async def test_a_partial_fill_books_what_filled(self, config):
        venue = paper(makers=[PaperMaker(fill_ratio=D("0.5"))])
        engine = await started(config, venue)
        rfq = await engine.create_rfq(MARKET, quantity=D("10"))
        await engine.accept_quote(accept_of(await first_quote(venue, rfq.id)))
        await execute(engine, venue)
        order = (await engine.journal.open_orders()) or None
        assert order is None, "the IOC remainder was cancelled"
        intent = (await accept_intents(engine))[0]
        stored = await engine.journal.order(VENUE, intent.order_id)
        assert (stored.status, stored.filled) == (OrderStatus.CANCELED, D("5"))
        assert engine.positions()[0].contracts == D("5")
        await engine.stop()

    async def test_a_maker_letting_the_last_look_lapse_fails_the_accept(self, config):
        venue = paper(makers=[PaperMaker(confirms=False)])
        engine = await started(config, venue)
        sub = engine.bus.subscribe("quote")
        rfq = await engine.create_rfq(MARKET, quantity=D("10"))
        await engine.accept_quote(accept_of(await first_quote(venue, rfq.id)))
        venue.advance()
        await engine.rfq.tick()
        intent = (await accept_intents(engine))[0]
        assert intent.state == IntentState.FAILED and "expired" in intent.detail
        failed = [e for e in events(sub) if e.kind == "quote.failed"]
        assert failed and failed[0].payload["reason"] == "expired"
        assert engine.positions() == []
        await engine.stop()

    async def test_a_stream_moves_the_accept_without_the_follower(self, config):
        venue = paper()
        engine = await started(config, venue)
        sub = engine.bus.subscribe()
        rfq = await engine.create_rfq(MARKET, quantity=D("10"), book="alpha")
        await engine.accept_quote(accept_of(await first_quote(venue, rfq.id)))
        for _ in range(2):
            venue.advance()
            await venue.deliver()
        assert engine.positions()[0].contracts == D("10")
        assert kinds(sub, "quote.", "order.", "fill.")[-6:] == [
            "quote.accepted", "quote.confirmed", "quote.executed", "order.accepted", "order.closed", "fill.booked",
        ]
        await engine.stop()

    async def test_a_fill_polled_before_its_order_is_adopted_is_still_attributed(self, config):
        venue = paper()
        engine = await started(config, venue)
        rfq = await engine.create_rfq(MARKET, quantity=D("10"), book="alpha")
        await engine.accept_quote(accept_of(await first_quote(venue, rfq.id)))
        venue.advance()
        venue.advance()                       # executed at the venue, unseen by the engine
        await engine.poll(VENUE)               # the fill poll finds a fill of an unknown order
        [fill] = await engine.journal.fills()
        assert fill.outcome == "yes"
        assert engine.positions()[0].info["book"] == "alpha"
        assert (await accept_intents(engine))[0].state == IntentState.SENT
        await engine.stop()

    async def test_a_retried_accept_is_not_sent_twice(self, config):
        venue = paper()
        engine = await started(config, venue)
        rfq = await engine.create_rfq(MARKET, quantity=D("10"))
        quote = await first_quote(venue, rfq.id)
        await engine.accept_quote(accept_of(quote))
        again = await engine.accept_quote(accept_of(quote))
        assert again.status == QuoteStatus.ACCEPTED and venue.calls.count("accept_quote") == 1
        await engine.stop()

    async def test_an_rfq_type_cannot_be_submitted_as_an_order(self, config):
        engine = await started(config, paper())
        with pytest.raises(BadRequest, match="accept_quote"):
            await engine.submit(OrderRequest(market_id=MARKET, side=Side.BUY, quantity=D("1"), price=D("0.5"),
                                             type=OrderType.RFQ_TAKER))
        await engine.stop()


class TestSideGuard:
    async def test_the_venue_side_is_booked_and_the_mismatch_alerted(self, config):
        venue = paper(makers=[PaperMaker(report_side=Side.SELL)])
        engine = await started(config, venue)
        alerts = Alerts(engine.bus)
        sub = engine.bus.subscribe("rfq.side_mismatch")
        rfq = await engine.create_rfq(MARKET, quantity=D("10"))
        await engine.accept_quote(accept_of(await first_quote(venue, rfq.id), worst_price=None))
        await execute(engine, venue)
        mismatches = events(sub)
        assert len(mismatches) == 1, "published once, though seen at the accept, the quote and the order"
        assert (mismatches[0].payload["expected"], mismatches[0].payload["reported"]) == ("buy", "sell")
        assert any(a.kind == "rfq_side" and a.severity == "critical" for a in alerts.raised)
        [position] = engine.positions()
        assert (position.side, position.contracts) == (PositionSide.SHORT, D("10"))
        order = await engine.journal.order(VENUE, (await accept_intents(engine))[0].order_id)
        assert order.side == Side.SELL and order.outcome is None and order.action is None
        await engine.stop()


class TestRecovery:
    async def test_a_crash_between_the_intent_and_the_call_is_rejected_on_restart(self, config):
        venue = paper()
        engine = await started(config, venue)
        rfq = await engine.create_rfq(MARKET, quantity=D("10"))
        quote = await first_quote(venue, rfq.id)
        real = venue.accept_quote

        async def dies(accept):
            raise Crash()

        venue.accept_quote = dies
        with pytest.raises(Crash):
            await engine.accept_quote(accept_of(quote))
        assert (await accept_intents(engine))[0].state == IntentState.SENDING
        await crashed(engine, venue)

        venue.accept_quote = real
        restarted = await started(config, venue, lost_after_s=0)
        [intent] = await accept_intents(restarted)
        assert intent.state == IntentState.REJECTED and "never took the accept" in intent.detail
        assert venue.quotes[quote.id].status == QuoteStatus.OPEN and "accept_quote" not in venue.calls
        # Nothing traded, so the same accept may be made again.
        result = await restarted.accept_quote(accept_of(quote))
        assert result.status == QuoteStatus.ACCEPTED and venue.calls.count("accept_quote") == 1
        await execute(restarted, venue)
        assert restarted.positions()[0].contracts == D("10")
        await restarted.stop()

    async def test_a_crash_between_the_call_and_its_result_is_adopted_on_restart(self, config):
        venue = paper()
        engine = await started(config, venue)
        rfq = await engine.create_rfq(MARKET, quantity=D("10"), book="alpha")
        quote = await first_quote(venue, rfq.id)
        real = venue.accept_quote

        async def answered_then_died(accept):
            await real(accept)
            raise Crash()

        venue.accept_quote = answered_then_died
        with pytest.raises(Crash):
            await engine.accept_quote(accept_of(quote))
        await crashed(engine, venue)

        venue.accept_quote = real
        restarted = await started(config, venue, lost_after_s=0)
        [intent] = await accept_intents(restarted)
        assert intent.state == IntentState.SENDING, "accepted at the venue: followed, not swept"
        again = await restarted.accept_quote(accept_of(quote))
        assert again.status == QuoteStatus.ACCEPTED and venue.calls.count("accept_quote") == 1
        await execute(restarted, venue)
        intent = await restarted.journal.intent(intent.client_order_id)
        assert intent.state == IntentState.SENT
        [position] = restarted.positions()
        assert (position.contracts, position.info["book"]) == (D("10"), "alpha")
        await restarted.stop()

    async def test_an_executed_quote_found_at_start_is_adopted_by_recover(self, config):
        venue = paper()
        engine = await started(config, venue)
        rfq = await engine.create_rfq(MARKET, quantity=D("10"))
        await engine.accept_quote(accept_of(await first_quote(venue, rfq.id)))
        await crashed(engine, venue)
        venue.advance()
        venue.advance()
        restarted = Engine({VENUE: venue}, config(), risk=RiskConfig(**RISK))
        recovery = await restarted.start()
        assert recovery.adopted == 1 and recovery.in_doubt == 1
        assert restarted.positions()[0].contracts == D("10")
        await restarted.stop()


class TestCombo:
    async def test_a_combo_position_lives_on_its_own_instrument(self, config):
        venue = paper()
        engine = await started(config, venue)
        legs = [ComboLeg(market_id="polymarket_us:tec-a", outcome="yes"),
                ComboLeg(market_id="polymarket_us:tec-b", outcome="no")]
        combo = await engine.create_combo(legs)
        rfq = await engine.create_rfq(combo.market_id, quantity=D("4"), combo=combo, book="parlay")
        assert rfq.combo_id == combo.market_id and rfq.market_id is None
        await engine.accept_quote(accept_of(await first_quote(venue, rfq.id), book="parlay"))
        await execute(engine, venue)
        [position] = engine.positions()
        assert position.market_id == combo.market_id and position.contracts == D("4")
        assert [l["market_id"] for l in position.info["combo"]["legs"]] == ["polymarket_us:tec-a", "polymarket_us:tec-b"]
        report = (await Reconciler(engine).run())[0]
        assert report.positions == [] and report.checked_positions == 1
        await engine.stop()

        restarted = await started(config, venue)
        assert restarted.positions()[0].info["combo"]["legs"][1]["outcome"] == "no"
        assert (await restarted.rfq.combo(combo.market_id)).market_id == combo.market_id
        await restarted.stop()


# -- quoter ------------------------------------------------------------------------


def maker_quote(rfq, **kw) -> MakerQuote:
    base = dict(rfq_id=rfq.id, bid=D("0.40"), ask=D("0.46"), book="mm")
    return MakerQuote(**{**base, **kw})


class TestQuoter:
    async def test_incoming_quote_confirm_order_fill_position(self, config):
        venue = paper(maker_fill_ratio=D("0.5"))
        engine = await started(config, venue)
        sub = engine.bus.subscribe("rfq", "quote", "order", "fill")
        incoming = venue.add_incoming_rfq(MARKET, quantity=D("10"))
        await engine.rfq.tick()
        assert f"{VENUE}:{incoming.id}" in engine.rfq.incoming

        quote = await engine.send_quote(maker_quote(incoming, expires_at=int(time.time() * 1000) + 60_000))
        assert quote.mine and quote.expires_at is not None
        assert venue.quotes[quote.id].expires_at is None, "the engine holds the expiry"
        venue.requester_accepts(quote.id, outcome="yes", action="buy")
        await engine.rfq.tick()
        confirmed = await engine.confirm_quote(quote.id)
        assert confirmed.status == QuoteStatus.CONFIRMED
        [intent] = await accept_intents(engine, "confirm_quote")
        assert intent.state == IntentState.SENDING and intent.request["side"] == "sell"
        assert D(intent.request["price"]) == D("0.46") and intent.request["type"] == "rfq_maker"

        venue.advance()
        await engine.rfq.tick()
        assert (await engine.journal.intent(intent.client_order_id)).state == IntentState.SENT
        [position] = engine.positions()
        assert (position.side, position.contracts, position.info["book"]) == (PositionSide.SHORT, D("5"), "mm")
        assert kinds(sub) == [
            "rfq.incoming", "quote.sent", "quote.accepted", "quote.confirmed", "quote.executed",
            "order.accepted", "order.canceled", "fill.booked",
        ]
        assert engine.rfq.maker_quotes == {}
        await engine.stop()

    async def test_confirm_goes_through_risk_and_the_last_look_lapses(self, config):
        now = [1_000.0]
        venue = paper(clock=lambda: now[0])
        engine = await started(config, venue, max_position_contracts=D("5"))
        incoming = venue.add_incoming_rfq(MARKET, quantity=D("10"))
        quote = await engine.send_quote(maker_quote(incoming))     # an offer: the position rule waits
        venue.requester_accepts(quote.id)
        with pytest.raises(RiskRejected) as refused:
            await engine.confirm_quote(quote.id)
        assert refused.value.rule == "max_position"
        assert "confirm_quote" not in venue.calls and await accept_intents(engine, "confirm_quote") == []
        now[0] += 10
        venue.advance()
        assert venue.quotes[quote.id].status == QuoteStatus.EXPIRED
        await engine.rfq.tick()
        assert engine.rfq.maker_quotes == {}
        await engine.stop()

    async def test_sending_a_quote_checks_the_size_limits(self, config):
        venue = paper()
        engine = await started(config, venue, max_order_contracts=D("5"))
        incoming = venue.add_incoming_rfq(MARKET, quantity=D("10"))
        with pytest.raises(RiskRejected) as refused:
            await engine.send_quote(maker_quote(incoming))
        assert refused.value.rule == "max_order_contracts"
        await engine.set_risk(RiskConfig(**RISK, max_order_notional=D("4")))
        with pytest.raises(RiskRejected) as notional:
            await engine.send_quote(maker_quote(incoming))   # selling YES at 0.46 holds 10 NO at 0.54
        assert notional.value.rule == "max_order_notional"
        assert "send_quote" not in venue.calls
        await engine.stop()

    async def test_a_quote_expires_at_its_expiry(self, config):
        venue = paper()
        engine = await started(config, venue)
        sub = engine.bus.subscribe("quote.expired")
        incoming = venue.add_incoming_rfq(MARKET, quantity=D("10"))
        quote = await engine.send_quote(maker_quote(incoming, expires_at=int(time.time() * 1000) + 50))
        await asyncio.sleep(0.08)
        await engine.rfq.tick()
        assert venue.quotes[quote.id].status == QuoteStatus.CANCELED
        assert engine.rfq.maker_quotes == {} and len(events(sub)) == 1
        cancel = await engine.journal.intent(f"rfq-quote-cancel:{VENUE}:{quote.id}")
        assert cancel.state == IntentState.SETTLED and cancel.detail == "expired"
        await engine.stop()

    async def test_replace_swaps_the_quote(self, config):
        venue = paper()
        engine = await started(config, venue)
        sub = engine.bus.subscribe("quote.replaced")
        incoming = venue.add_incoming_rfq(MARKET, quantity=D("10"))
        old = await engine.send_quote(maker_quote(incoming))
        new = await engine.replace_quote(old.id, maker_quote(incoming, bid=D("0.41"), ask=D("0.45")))
        assert new.id != old.id and venue.quotes[old.id].status == QuoteStatus.CANCELED
        assert list(engine.rfq.maker_quotes) == [f"{VENUE}:{new.id}"]
        assert events(sub)[0].payload["replaced"] == old.id
        await engine.cancel_quote(new.id)
        assert venue.quotes[new.id].status == QuoteStatus.CANCELED and engine.rfq.maker_quotes == {}
        await engine.stop()

    async def test_a_crash_between_the_confirm_intent_and_the_call_lets_the_last_look_lapse(self, config):
        venue = paper()
        engine = await started(config, venue)
        incoming = venue.add_incoming_rfq(MARKET, quantity=D("10"))
        quote = await engine.send_quote(maker_quote(incoming))
        venue.requester_accepts(quote.id)
        real = venue.confirm_quote

        async def dies(q):
            raise Crash()

        venue.confirm_quote = dies
        with pytest.raises(Crash):
            await engine.confirm_quote(quote.id)
        await crashed(engine, venue)
        venue.confirm_quote = real
        restarted = await started(config, venue, lost_after_s=0)
        [intent] = await accept_intents(restarted, "confirm_quote")
        assert intent.state == IntentState.REJECTED and "last look lapses" in intent.detail
        assert f"{VENUE}:{quote.id}" in restarted.rfq.maker_quotes, "the working set survived the restart"
        await restarted.stop()

    async def test_a_crash_after_the_confirm_is_adopted_on_restart(self, config):
        venue = paper()
        engine = await started(config, venue)
        incoming = venue.add_incoming_rfq(MARKET, quantity=D("10"))
        quote = await engine.send_quote(maker_quote(incoming))
        venue.requester_accepts(quote.id, outcome="no", action="buy")    # the requester sells YES at our bid
        real = venue.confirm_quote

        async def confirmed_then_died(q):
            await real(q)
            raise Crash()

        venue.confirm_quote = confirmed_then_died
        with pytest.raises(Crash):
            await engine.confirm_quote(quote.id)
        await crashed(engine, venue)
        venue.confirm_quote = real
        restarted = await started(config, venue, lost_after_s=0)
        assert (await accept_intents(restarted, "confirm_quote"))[0].state == IntentState.SENDING
        venue.advance()
        await restarted.rfq.tick()
        [position] = restarted.positions()
        assert (position.side, position.contracts, position.entry_price) == (PositionSide.LONG, D("10"), D("0.40"))
        assert venue.calls.count("confirm_quote") == 1
        await restarted.stop()


class TestIncomingPush:
    """Gap 1: a venue that pushes incoming RFQs reaches the quoting callback
    at once, not at the next 2 s poll."""

    async def test_a_pushed_rfq_reaches_the_callback_without_a_poll(self, config):
        venue = paper(stream_incoming=True)
        polls = []
        original = venue.fetch_incoming_rfqs

        async def counted(**kw):
            polls.append(kw)
            return await original(**kw)

        venue.fetch_incoming_rfqs = counted
        engine = Engine({VENUE: venue}, config(rfq_incoming_interval_s=2.0), risk=RiskConfig(**RISK))
        await engine.start()
        assert engine.rfq.pushed == {VENUE}
        sub = engine.bus.subscribe("rfq", "quote")
        timings: dict[str, float] = {}
        order: list[str] = []

        async def quoter(rfq):
            timings["callback"] = time.perf_counter()
            order.append("callback")
            assert f"{VENUE}:{rfq.id}" in engine.rfq.incoming, "recorded before the callback"
            assert [e.kind for e in events(sub)] == ["rfq.incoming"], "published before the callback"
            await engine.send_quote(maker_quote(rfq))
            timings["quoted"] = time.perf_counter()

        engine.rfq.on_incoming_rfq(quoter)
        pushed_at = time.perf_counter()
        rfq = await venue.push_incoming_rfq(MARKET, quantity=D("10"))
        to_callback_ms = (timings["callback"] - pushed_at) * 1000
        to_quote_ms = (timings["quoted"] - pushed_at) * 1000
        print(f"pushed RFQ -> callback {to_callback_ms:.3f} ms, -> quote sent {to_quote_ms:.3f} ms")
        assert to_callback_ms < 50, "no poll delay: far below the 400 ms window"
        assert to_quote_ms < 400
        assert polls == [], "nothing polled"
        assert [q.rfq_id for q in venue.quotes.values() if q.mine] == [rfq.id]
        assert order == ["callback"]
        # The loop skips the pushed venue's poll; starting the stream reads once.
        await engine.rfq.tick()
        await engine.rfq.tick()
        assert venue.stream_started and len(polls) == 1
        assert await engine.rfq.on_incoming(VENUE, rfq) is False, "a repeat is not heard twice"
        await engine.stop()

    async def test_a_venue_without_a_stream_is_polled_and_calls_back_too(self, config):
        venue = paper()
        engine = await started(config, venue)
        assert engine.rfq.pushed == set()
        heard = []
        unsubscribe = engine.rfq.on_incoming_rfq(lambda rfq: heard.append(rfq.id))
        incoming = venue.add_incoming_rfq(MARKET, quantity=D("10"))
        await engine.rfq.tick()
        assert heard == [incoming.id]
        unsubscribe()
        venue.add_incoming_rfq(MARKET, quantity=D("5"))
        await engine.rfq.tick()
        assert heard == [incoming.id]
        await engine.stop()

    async def test_a_failing_callback_does_not_stop_the_others(self, config):
        venue = paper(stream_incoming=True)
        engine = await started(config, venue)
        heard = []

        def broken(rfq):
            raise RuntimeError("strategy bug")

        engine.rfq.on_incoming_rfq(broken)
        engine.rfq.on_incoming_rfq(lambda rfq: heard.append(rfq.id))
        rfq = await venue.push_incoming_rfq(MARKET, quantity=D("10"))
        assert heard == [rfq.id]
        await engine.stop()


class TestHalt:
    async def test_halt_cancels_every_rfq_and_quote_on_every_venue(self, config):
        venue = paper()
        engine = await started(config, venue)
        sub = engine.bus.subscribe("rfq.closed", "quote.canceled")
        ours = [await engine.create_rfq(MARKET, quantity=D("5")) for _ in range(2)]
        stray = await venue.create_rfq(MARKET, quantity=D("3"))       # made outside this engine
        incoming = venue.add_incoming_rfq(MARKET, quantity=D("10"))
        mine = await engine.send_quote(maker_quote(incoming))
        result = await engine.halt("operator")
        assert result["rfq"] == {"rfqs": {VENUE: 3}, "quotes": {VENUE: 1}}
        assert all(venue.rfqs[r.id].status == RfqStatus.CLOSED for r in [*ours, stray])
        assert venue.quotes[mine.id].status == QuoteStatus.CANCELED
        assert engine.rfq.rfqs == {} and engine.rfq.maker_quotes == {}
        got = events(sub)
        assert sorted(e.kind for e in got).count("rfq.closed") == 3
        assert any(e.kind == "quote.canceled" and e.payload.get("reason") == "halt" for e in got)
        with pytest.raises(RiskRejected):
            await engine.send_quote(maker_quote(incoming))
        await engine.stop()
