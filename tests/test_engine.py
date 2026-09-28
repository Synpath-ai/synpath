"""The engine core, offline.

Everything here runs against a scripted venue and a temporary journal, so
the tests prove the engine's own behaviour rather than a venue's: the
crash-and-restart path, the rules that refuse an order, the reconciliation
findings, the ledger's arithmetic, the paper fill simulator's queue, and the
refusal of a second engine to trade the same journal.

The acceptance criteria this file covers, in the plan's words: a process
killed mid-submit restarts with no duplicate and no orphan; orphan and ghost
orders surface as events; intents that never reach a venue are swept after a
timeout; every risk rule rejects once; a second instance does not place
orders; the ledger reconciles against the venue; and a strategy runs a week
of paper trading on an accelerated clock.
"""
from __future__ import annotations

import asyncio
import random
from decimal import Decimal as D
from pathlib import Path

import pytest

from synpath.engine import (
    Engine, EngineConfig, EventBus, Journal, Ledger, LeaseLost, RiskConfig, RiskEngine,
)
from synpath.engine.alerts import Alerts, AlertRules
from synpath.engine.eod import EndOfDay
from synpath.engine.journal import IntentState
from synpath.engine.paper import PaperVenue, quadratic_fee
from synpath.engine.reconcile import Reconciler
from synpath.errors import NetworkError
from synpath.trading.base import TradingExchange
from synpath.trading.errors import OrderNotFound, OrderRejected, RiskRejected
from synpath.trading.types import (
    Account, Balance, Fill, Liquidity, Order, OrderRequest, OrderStatus, OrderType, Position, PositionSide, Settlement,
    SettlementState, Side, TimeInForce,
)

pytestmark = pytest.mark.anyio
ACCOUNT = Account(venue="kalshi", name="default")


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def journal_path(tmp_path: Path) -> str:
    return str(tmp_path / "engine.db")


def request(**kw) -> OrderRequest:
    base = dict(market_id="kalshi:KX-A", side=Side.BUY, amount=D("5"), price=D("0.40"), book="alpha",
                trader="tester", account=ACCOUNT)
    return OrderRequest(**{**base, **kw})


class ScriptedVenue(TradingExchange):
    """A venue that does what the test tells it to, including failing."""

    id = "kalshi"
    has = {
        "create_order": True, "cancel_order": True, "cancel_all_orders": True, "edit_order": True,
        "fetch_order": True, "fetch_open_orders": True, "fetch_orders": True, "fetch_my_trades": True,
        "fetch_positions": True, "fetch_balance": True, "fetch_settlements": True,
    }

    def __init__(self):
        self.orders: dict[str, Order] = {}
        self.fills: list[Fill] = []
        self.positions: list[Position] = []
        self.settlements: list[Settlement] = []
        self.balance = D("1000")
        self.sent: list[str] = []
        self.fail_next: str | None = None
        self.n = 0

    async def create_order(self, request: OrderRequest) -> Order:
        self.sent.append(request.client_order_id or "")
        if self.fail_next:
            mode, self.fail_next = self.fail_next, None
            if mode == "arrived":      # the venue has it; the answer was lost
                self._store(request)
                raise NetworkError("connection reset")
            if mode == "lost":         # nothing arrived
                raise NetworkError("no route to host")
            if mode == "rejected":
                raise OrderRejected("price out of bounds", reason="price")
        return self._store(request)

    def _store(self, request: OrderRequest) -> Order:
        self.n += 1
        order = Order(
            id=f"V{self.n}", client_order_id=request.client_order_id, venue="kalshi", account=request.account,
            market_id=request.market_id, side=request.side,
            type=OrderType.LIMIT, time_in_force=TimeInForce.GTC, status=OrderStatus.OPEN, price=request.price,
            amount=request.amount, book=request.book, trader=request.trader,
        )
        self.orders[order.id] = order
        return order

    async def cancel_order(self, order_id, *, market_id=None, current=None):
        if order_id not in self.orders:
            raise OrderNotFound(order_id)
        self.orders[order_id] = self.orders[order_id].model_copy(update={"status": OrderStatus.CANCELED})
        return self.orders[order_id]

    async def cancel_all_orders(self, *, market_id=None):
        count = 0
        for key, order in list(self.orders.items()):
            if not order.is_terminal:
                self.orders[key] = order.model_copy(update={"status": OrderStatus.CANCELED})
                count += 1
        return count

    async def edit_order(self, request, *, current=None):
        order = self.orders[request.order_id]
        updated = order.model_copy(update={
            "price": request.price if request.price is not None else order.price,
            "amount": request.amount if request.amount is not None else order.amount,
            "queue_priority_preserved": request.price is None,
        })
        self.orders[request.order_id] = updated
        return updated

    async def fetch_order(self, order_id):
        if order_id not in self.orders:
            raise OrderNotFound(order_id)
        return self.orders[order_id]

    async def fetch_open_orders(self, *, market_id=None):
        return [o for o in self.orders.values() if not o.is_terminal]

    async def fetch_orders(self, *, market_id=None, status=None, since=None, until=None, limit=None, cursor=None):
        return list(self.orders.values())

    async def fetch_my_trades(self, *, market_id=None, since=None, until=None, limit=None, cursor=None):
        return [f for f in self.fills if since is None or f.timestamp >= since]

    async def fetch_positions(self, *, market_id=None, event_id=None):
        return list(self.positions)

    async def fetch_balance(self, *, account=None):
        return Balance(venue="kalshi", account=account or ACCOUNT, currency="USD", total=self.balance,
                       available=self.balance)

    async def fetch_settlements(self, *, market_id=None, since=None, until=None, limit=None, cursor=None):
        return list(self.settlements)

    async def close(self):
        return None


async def engine_for(journal_path: str, venue: ScriptedVenue, *, lost_after_s: float = 60.0, **risk) -> Engine:
    config = EngineConfig(journal_path=journal_path, in_doubt_timeout_s=0, require_lease=True,
                          lost_after_s=lost_after_s)
    rules = RiskConfig(price_collar=None, duplicate_window_ms=0, **risk)
    engine = Engine({"kalshi": venue}, config, risk=rules, accounts={"kalshi": ACCOUNT})
    await engine.start()
    return engine


# ---------------------------------------------------------------------------
# The journal
# ---------------------------------------------------------------------------

class TestJournal:
    async def test_an_intent_is_on_disk_before_the_order_is_sent(self, journal_path):
        async with Journal(journal_path) as journal:
            await journal.record_intent(request(), client_order_id="c1", venue="kalshi", account=ACCOUNT)
            await journal.mark_intent("c1", IntentState.SENDING)
        # A different process reading the same file sees it.
        async with Journal(journal_path, owner="reader") as reader:
            intents = await reader.in_doubt()
            assert [i.client_order_id for i in intents] == ["c1"]
            assert intents[0].request["price"] == "0.40" and intents[0].book == "alpha"

    async def test_a_fill_is_recorded_once(self, journal_path):
        fill = Fill(id="F1", order_id="V1", venue="kalshi", account=ACCOUNT, market_id="kalshi:KX-A", side=Side.BUY, price=D("0.4"), amount=D("5"), timestamp=1)
        async with Journal(journal_path) as journal:
            assert await journal.record_fill(fill) is True
            assert await journal.record_fill(fill) is False
            settled = fill.model_copy(update={"settlement": SettlementState.CONFIRMED})
            assert await journal.record_fill(settled) is False
            assert len(await journal.fills()) == 1

    async def test_the_second_engine_refuses_the_journal(self, journal_path):
        async with Journal(journal_path, owner="first") as first:
            await first.acquire_lease(ttl_ms=60_000)
            async with Journal(journal_path, owner="second") as second:
                with pytest.raises(LeaseLost, match="first"):
                    await second.acquire_lease(ttl_ms=60_000)

    async def test_an_expired_lease_can_be_taken(self, journal_path):
        async with Journal(journal_path, owner="first") as first:
            await first.acquire_lease(ttl_ms=1)
            await asyncio.sleep(0.01)
            async with Journal(journal_path, owner="second") as second:
                assert (await second.acquire_lease(ttl_ms=60_000)).owner == "second"
                assert await first.renew_lease() is False

    async def test_replay_is_ordered_and_filterable(self, journal_path):
        async with Journal(journal_path) as journal:
            await journal.append("a.one", {"n": 1})
            await journal.append("b.two", {"n": 2})
            await journal.append("a.three", {"n": 3})
            everything = [e.kind async for e in journal.replay()]
            assert everything == ["a.one", "b.two", "a.three"]
            filtered = [e.payload["n"] async for e in journal.replay(kinds=["a.one", "a.three"])]
            assert filtered == [1, 3]


# ---------------------------------------------------------------------------
# Crash and recovery
# ---------------------------------------------------------------------------

class TestRecovery:
    async def test_a_process_killed_mid_submit_adopts_the_order(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        await engine.submit(request())
        venue.fail_next = "arrived"
        with pytest.raises(NetworkError):
            await engine.submit(request(price=D("0.41")))
        assert len(await engine.journal.in_doubt()) == 1
        await engine.journal.close()          # the process dies here

        restarted = await engine_for(journal_path, venue)
        recovery = await restarted.recover()
        assert (recovery.in_doubt, recovery.adopted, recovery.swept) == (0, 0, 0), "start() already resolved it"
        assert len(venue.sent) == 2, "the order was not sent again"
        assert sorted(o.id for o in restarted.open_orders()) == ["V1", "V2"]
        adopted = await restarted.journal.intent(venue.sent[1])
        assert adopted.state == IntentState.SENT and adopted.order_id == "V2"
        await restarted.stop()

    async def test_an_order_that_never_arrived_is_swept(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        venue.fail_next = "lost"
        with pytest.raises(NetworkError):
            await engine.submit(request())
        await engine.journal.close()

        # `lost_after_s=0`: the venue has had its chance to list the order.
        restarted = await engine_for(journal_path, venue, lost_after_s=0)
        intents = await restarted.journal.intents(state=IntentState.LOST)
        assert len(intents) == 1 and intents[0].detail == "not found at the venue"
        assert restarted.open_orders() == []
        await restarted.stop()

    async def test_an_order_the_venue_has_not_listed_yet_stays_in_doubt(self, journal_path):
        """A venue that accepts an order but does not list it for a few hundred
        milliseconds must not have that order declared lost."""
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue, lost_after_s=60)
        venue.fail_next = "lost"
        with pytest.raises(NetworkError):
            await engine.submit(request())
        [outcome] = await engine.sweep()
        assert outcome["result"] == "pending"
        assert await engine.journal.intents(state=IntentState.LOST) == []
        assert len(await engine.journal.in_doubt()) == 1
        await engine.stop()

    async def test_the_sweep_resolves_in_doubt_intents_on_a_timer(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        venue.fail_next = "arrived"
        with pytest.raises(NetworkError):
            await engine.submit(request())
        swept = await engine.sweep()
        assert [row["result"] for row in swept] == ["adopted"]
        assert len(await engine.journal.in_doubt()) == 0
        await engine.stop()

    async def test_the_ledger_is_rebuilt_from_the_journal(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        order = await engine.submit(request())
        await engine.on_fill(Fill(id="F1", order_id=order.id, venue="kalshi", account=ACCOUNT,
                                  market_id="kalshi:KX-A", side=Side.BUY, price=D("0.40"),
                                  amount=D("5"), fee=D("0.02"), timestamp=1))
        await engine.journal.close()

        restarted = await engine_for(journal_path, venue)
        state = restarted.ledger.position("kalshi:default", "alpha", "kalshi", "kalshi:KX-A")
        assert state.contracts == D("5") and state.average_cost == D("0.40") and state.realized == D("-0.02")
        await restarted.stop()

    async def test_a_second_engine_does_not_trade(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        second = Engine({"kalshi": venue}, EngineConfig(journal_path=journal_path), accounts={"kalshi": ACCOUNT})
        with pytest.raises(LeaseLost):
            await second.start()
        assert venue.sent == []
        await second.journal.close()
        await engine.stop()


# ---------------------------------------------------------------------------
# Submitting
# ---------------------------------------------------------------------------

class TestSubmit:
    async def test_the_order_carries_the_book_and_the_client_id(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        order = await engine.submit(request())
        assert order.book == "alpha" and order.trader == "tester"
        assert order.client_order_id and order.client_order_id.startswith("sp-")
        stored = await engine.journal.order("kalshi", order.id)
        assert stored.book == "alpha"
        intent = await engine.journal.intent(order.client_order_id)
        assert intent.state == IntentState.SENT and intent.order_id == order.id
        await engine.stop()

    async def test_a_venue_rejection_is_final_and_recorded(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        venue.fail_next = "rejected"
        with pytest.raises(OrderRejected):
            await engine.submit(request())
        intents = await engine.journal.intents(state=IntentState.REJECTED)
        assert len(intents) == 1 and "price out of bounds" in intents[0].detail
        assert await engine.journal.in_doubt() == []
        await engine.stop()

    async def test_cancel_and_edit_carry_their_own_keys(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        order = await engine.submit(request())
        from synpath.trading.types import EditRequest

        edited = await engine.edit(EditRequest(order_id=order.id, amount=D("3")))
        assert edited.amount == D("3") and edited.queue_priority_preserved is True
        canceled = await engine.cancel(order.id)
        assert canceled.status == OrderStatus.CANCELED
        keys = {i.client_order_id: i for i in await engine.journal.intents()}
        assert f"cancel:kalshi:{order.id}" in keys
        assert any(k.startswith("edit:kalshi:") for k in keys)
        assert engine.open_orders() == []
        await engine.stop()

    async def test_cancelling_something_already_gone_is_not_an_error(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        order = await engine.submit(request())
        venue.orders.pop(order.id)
        canceled = await engine.cancel(order.id)
        assert canceled.status == OrderStatus.CANCELED
        await engine.stop()


# ---------------------------------------------------------------------------
# Risk
# ---------------------------------------------------------------------------

class TestRisk:
    @pytest.mark.parametrize("rule,config,kwargs", [
        ("max_order_contracts", {"max_order_contracts": D("1")}, {}),
        ("max_order_notional", {"max_order_notional": D("1")}, {}),
        ("price_bounds", {}, {"price": D("1.4")}),
        ("max_open_orders", {"max_open_orders": 0}, {}),
        ("restricted", {"restricted": ["KX-A*"]}, {}),
    ])
    async def test_each_rule_refuses_before_anything_is_sent(self, journal_path, rule, config, kwargs):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue, **config)
        with pytest.raises(RiskRejected) as caught:
            await engine.submit(request(**kwargs))
        assert caught.value.rule == rule
        assert venue.sent == [], "risk must refuse before the venue is called"
        events = [e.kind async for e in engine.journal.replay(kinds=["risk.rejected"])]
        assert events == ["risk.rejected"]
        await engine.stop()

    async def test_the_price_collar_uses_the_stored_fair_value(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        engine.risk.config = engine.risk.config.model_copy(update={"price_collar": D("0.05")})
        await engine.fair_values.set("kalshi:default", "kalshi:KX-A", D("0.60"), source="mid")
        with pytest.raises(RiskRejected) as caught:
            await engine.submit(request(price=D("0.40")))
        assert caught.value.rule == "price_collar"
        assert (await engine.submit(request(price=D("0.58")))).status == OrderStatus.OPEN
        await engine.stop()

    async def test_the_kill_switch_cancels_and_blocks(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        await engine.submit(request())
        await engine.submit(request(price=D("0.39")))
        result = await engine.halt("operator", policy="cancel")
        assert result["canceled"] == {"kalshi": 2}
        assert all(o.is_terminal for o in venue.orders.values())
        with pytest.raises(RiskRejected) as caught:
            await engine.submit(request(price=D("0.38")))
        assert caught.value.rule == "kill_switch"
        await engine.resume()
        assert (await engine.submit(request(price=D("0.38")))).status == OrderStatus.OPEN
        await engine.stop()

    async def test_a_paused_book_may_cancel_but_not_open(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        order = await engine.submit(request())
        await engine.pause_book("alpha")
        with pytest.raises(RiskRejected) as caught:
            await engine.submit(request(price=D("0.39")))
        assert caught.value.rule == "book_paused"
        assert (await engine.cancel(order.id)).status == OrderStatus.CANCELED
        await engine.stop()

    async def test_the_risk_configuration_is_versioned(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        first = engine.risk.config_version
        second = await engine.set_risk(RiskConfig(max_order_contracts=D("2"), price_collar=None, duplicate_window_ms=0))
        assert second > first
        with pytest.raises(RiskRejected):
            await engine.submit(request(amount=D("5")))
        stored = await engine.journal.config("risk", second)
        assert stored[1]["max_order_contracts"] == "2"
        await engine.stop()


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

class TestReconcile:
    async def test_an_orphan_is_reported_not_adopted(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        venue._store(request(client_order_id="somebody-else"))     # placed outside the engine
        report = (await Reconciler(engine).run())[0]
        assert [d.kind for d in report.orphans] == ["orphan"] and report.orphans[0].action == "reported"
        assert engine.open_orders() == []
        kinds = [e.kind async for e in engine.journal.replay(kinds=["reconcile.orphan"])]
        assert kinds == ["reconcile.orphan"]
        await engine.stop()

    async def test_an_orphan_can_be_adopted(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        venue._store(request(client_order_id="somebody-else"))
        report = (await Reconciler(engine, orphan_policy="adopt").run())[0]
        assert report.orphans[0].action == "adopted"
        assert [o.id for o in engine.open_orders()] == ["V1"]
        await engine.stop()

    async def test_a_ghost_is_closed_from_the_venue(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        order = await engine.submit(request())
        venue.orders.pop(order.id)                                  # gone at the venue
        report = (await Reconciler(engine).run())[0]
        assert [d.kind for d in report.ghosts] == ["ghost"]
        assert engine.open_orders() == []
        await engine.stop()

    async def test_drift_takes_the_venue_s_word(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        order = await engine.submit(request())
        venue.orders[order.id] = venue.orders[order.id].model_copy(update={"filled": D("2"), "remaining": D("3")})
        report = (await Reconciler(engine).run())[0]
        assert [d.kind for d in report.drifts] == ["drift"]
        assert engine.open_orders()[0].filled == D("2")
        await engine.stop()

    async def test_a_position_difference_is_reported(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        venue.positions = [Position(venue="kalshi", account=ACCOUNT, market_id="kalshi:KX-A",
                                    side=PositionSide.LONG, contracts=D("7"))]
        report = (await Reconciler(engine).run())[0]
        assert [d.kind for d in report.positions] == ["position"]
        assert report.positions[0].detail["difference"] == "-7"
        await engine.stop()

    async def test_a_fill_the_engine_missed_is_booked(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        venue.fills = [Fill(id="F9", order_id="V1", venue="kalshi", account=ACCOUNT, market_id="kalshi:KX-A", side=Side.BUY, price=D("0.4"), amount=D("5"), timestamp=5)]
        report = (await Reconciler(engine).run())[0]
        assert report.new_fills == 1
        assert engine.ledger.total().realized == D("0")
        assert engine.ledger.open_positions()[0].contracts == D("5")
        await engine.stop()

    async def test_cash_that_no_fill_explains_is_reported(self, journal_path):
        venue = ScriptedVenue()
        engine = await engine_for(journal_path, venue)
        await Reconciler(engine).run()            # first pass records the balance
        venue.balance = D("1500")                 # a deposit nobody told the engine about
        report = (await Reconciler(engine).run())[0]
        assert [d.kind for d in report.balances] == ["balance"]
        assert report.balances[0].detail["unexplained"] == "500"
        await engine.stop()


# ---------------------------------------------------------------------------
# The ledger
# ---------------------------------------------------------------------------

class TestLedger:
    def test_average_cost_realized_and_crossing_through_zero(self):
        ledger = Ledger()

        def fill(i, side, price, amount, market="kalshi:KX-A", fee="0"):
            return Fill(id=f"F{i}", order_id="V1", venue="kalshi", account=ACCOUNT,
                        market_id=market, side=side, price=D(price), amount=D(amount), fee=D(fee), timestamp=i)

        ledger.apply_fill(fill(1, Side.BUY, "0.40", "10", fee="0.05"))
        ledger.apply_fill(fill(2, Side.BUY, "0.50", "10"))
        state = ledger.position("kalshi:default", "default", "kalshi", "kalshi:KX-A")
        assert state.average_cost == D("0.45") and state.contracts == D("20") and state.realized == D("-0.05")

        assert ledger.apply_fill(fill(3, Side.SELL, "0.60", "5")) == D("0.75")
        # A sell is the NO side at the YES price: 20 at 0.70 closes the rest and crosses to short.
        assert ledger.apply_fill(fill(4, Side.SELL, "0.70", "20")) == D("3.75")
        assert state.contracts == D("-5") and state.average_cost == D("0.70")
        assert state.unrealized(D("0.65")) == D("0.25")
        assert ledger.apply_fill(fill(1, Side.BUY, "0.40", "10")) == D("0"), "the same fill id twice is once"

    def test_settlement_closes_the_position(self):
        ledger = Ledger()
        ledger.apply_fill(Fill(id="F1", order_id="V1", venue="kalshi", account=ACCOUNT, market_id="kalshi:KX-A", side=Side.BUY, price=D("0.40"), amount=D("10"), timestamp=1))
        realized = ledger.apply_settlement(Settlement(venue="kalshi", market_id="kalshi:KX-A", result="yes"))
        assert realized == D("6.0")
        assert ledger.open_positions() == []

    def test_three_levels_and_the_firm_total(self):
        ledger = Ledger()
        for i, (book, market) in enumerate([("alpha", "kalshi:KX-A"), ("beta", "kalshi:KX-A"), ("alpha", "kalshi:KX-B")]):
            ledger.apply_fill(Fill(id=f"F{i}", order_id="V1", venue="kalshi", account=ACCOUNT,
                                   market_id=market, side=Side.BUY,
                                   price=D("0.50"), amount=D("10"), fee=D("0.10"), timestamp=i), book=book)
        marks = {("kalshi:default", "kalshi:KX-A"): D("0.55"), ("kalshi:default", "kalshi:KX-B"): D("0.45")}
        assert set(ledger.rollup("market", marks)) == {"kalshi:KX-A", "kalshi:KX-B"}
        assert set(ledger.rollup("book", marks)) == {"alpha", "beta"}
        assert set(ledger.rollup("account", marks)) == {"kalshi:default"}
        total = ledger.total(marks)
        assert total.realized == D("-0.30") and total.unrealized == D("0.5") and total.volume == D("30")

    def test_a_position_without_a_mark_reports_no_unrealized(self):
        ledger = Ledger()
        ledger.apply_fill(Fill(id="F1", order_id="V1", venue="kalshi", account=ACCOUNT, market_id="kalshi:KX-A", side=Side.BUY, price=D("0.40"), amount=D("10"), timestamp=1))
        row = ledger.rollup("book", {})["default"]
        assert row.unrealized is None and row.cost == D("4.0") and row.marked == 0


# ---------------------------------------------------------------------------
# Paper trading
# ---------------------------------------------------------------------------

class TestPaper:
    async def test_a_resting_order_waits_its_turn_in_the_queue(self):
        paper = PaperVenue(venue="kalshi", fees=quadratic_fee())
        paper.set_book("kalshi:KX-A", bids=[(D("0.41"), D("500"))], asks=[(D("0.43"), D("300"))])
        order = await paper.create_order(request(price=D("0.41"), amount=D("100")))
        assert await paper.fetch_queue_position(order.id) == D("500")
        assert paper.on_trade("kalshi:KX-A", price=D("0.41"), amount=D("450"), taker_side=Side.SELL) == []
        assert await paper.fetch_queue_position(order.id) == D("50")
        fills = paper.on_trade("kalshi:KX-A", price=D("0.41"), amount=D("120"), taker_side=Side.SELL)
        assert [f.amount for f in fills] == [D("70")] and fills[0].liquidity == Liquidity.MAKER

    async def test_taking_walks_the_book_and_pays_the_fee(self):
        paper = PaperVenue(venue="kalshi", fees=quadratic_fee(D("0.07")))
        paper.set_book("kalshi:KX-A", bids=[], asks=[(D("0.43"), D("300")), (D("0.45"), D("400"))])
        order = await paper.create_order(request(price=D("0.46"), amount=D("500")))
        assert order.filled == D("500") and order.average_price == D("0.438")
        assert order.fee > 0 and order.status == OrderStatus.CLOSED
        assert paper.positions_held["kalshi:KX-A"] == D("500")

    async def test_fill_or_kill_leaves_nothing_behind(self):
        paper = PaperVenue(venue="kalshi")
        paper.set_book("kalshi:KX-A", asks=[(D("0.43"), D("100"))])
        order = await paper.create_order(request(price=D("0.50"), amount=D("500"), time_in_force=TimeInForce.FOK))
        assert order.status == OrderStatus.CANCELED and order.filled == D("0")
        assert paper.positions_held.get("kalshi:KX-A", D("0")) == D("0") and paper.fills == []

    async def test_post_only_does_not_cross(self):
        paper = PaperVenue(venue="kalshi")
        paper.set_book("kalshi:KX-A", bids=[(D("0.41"), D("10"))], asks=[(D("0.43"), D("100"))])
        order = await paper.create_order(request(price=D("0.44"), amount=D("10"), post_only=True))
        assert order.filled == D("0") and order.status == OrderStatus.CANCELED

    async def test_a_week_of_paper_trading_on_an_accelerated_clock(self, journal_path):
        """Seven days of a mean-reverting strategy: the engine, the journal,
        the risk rules and the ledger all run, only the clock is fake."""
        clock = {"t": 1_800_000_000.0}
        paper = PaperVenue(venue="kalshi", fees=quadratic_fee(), cash=D("1000"), clock=lambda: clock["t"])
        engine = Engine({"kalshi": paper}, EngineConfig(journal_path=journal_path, require_lease=True),
                        risk=RiskConfig(price_collar=None, duplicate_window_ms=0, max_order_contracts=D("50"),
                                        max_orders_per_minute=None, closing_soon_s=None),
                        accounts={"kalshi": Account(venue="kalshi", name="paper")}, clock=lambda: clock["t"])
        await engine.start()
        random.seed(7)
        price = D("0.50")
        submitted = canceled = 0
        for minute in range(7 * 24 * 60):
            clock["t"] += 60
            move = D(random.choice(["-0.02", "-0.01", "0", "0.01", "0.02"]))
            price = min(max(price + move, D("0.05")), D("0.95"))
            paper.set_book("kalshi:KX-A", bids=[(price - D("0.01"), D("200"))], asks=[(price + D("0.01"), D("200"))])
            for market_id, book in (("kalshi:KX-A", None),):
                await engine.fair_values.set("kalshi:paper", market_id, price, source="mid", persist=False)
            if minute % 30 == 0:
                # Half the hours the order fills, half it is still resting and
                # gets pulled: both paths run, over a week of them.
                for order in engine.open_orders():
                    await engine.cancel(order.id)
                    canceled += 1
            if minute % 60 == 0:
                side = Side.BUY if price < D("0.50") else Side.SELL
                limit = (price - D("0.01")) if side == Side.BUY else (price + D("0.01"))
                order = await engine.submit(OrderRequest(
                    market_id="kalshi:KX-A", side=side, amount=D("10"), price=limit, book="mean-reversion",
                    account=Account(venue="kalshi", name="paper"),
                ))
                submitted += 1
                if minute % 120 == 0:
                    # The tape trades through our price: the order fills.
                    for fill in paper.on_trade("kalshi:KX-A", price=limit, amount=D("400"),
                                               taker_side=Side.SELL if side == Side.BUY else Side.BUY):
                        await engine.on_fill(fill)
        assert submitted == 7 * 24
        total = engine.ledger.total(engine.fair_values.marks())
        # Maker fills on this fee model cost nothing, which is the venues'
        # own schedule; the taker path is checked on its own above.
        assert total.volume > 0 and total.fees == D("0") and total.unrealized is not None
        assert canceled > 0, "some orders rest until they are pulled"
        assert len(await engine.journal.fills()) == total.volume / D("10")
        # Everything that happened is in the journal, in order.
        kinds = [e.kind async for e in engine.journal.replay()]
        assert kinds.count("intent.planned") == submitted + canceled
        report = await EndOfDay(engine).run()
        assert report.fills > 0 and report.by_book["mean-reversion"]["volume"] == str(total.volume)
        await engine.stop()


# ---------------------------------------------------------------------------
# Events and alerts
# ---------------------------------------------------------------------------

class TestEventsAndAlerts:
    async def test_a_slow_reader_loses_its_oldest_events_not_the_engine(self, journal_path):
        async with Journal(journal_path) as journal:
            bus = EventBus(journal)
            sub = bus.subscribe(maxsize=2)
            for i in range(5):
                await bus.publish("test.event", {"n": i}, persist=False)
            assert sub.dropped == 3
            assert [sub.queue.get_nowait().payload["n"] for _ in range(2)] == [3, 4]

    async def test_subscribers_filter_by_prefix(self, journal_path):
        async with Journal(journal_path) as journal:
            bus = EventBus(journal)
            orders = bus.subscribe("order")
            everything = bus.subscribe()
            await bus.publish("order.accepted", {}, persist=False)
            await bus.publish("risk.rejected", {}, persist=False)
            assert orders.queue.qsize() == 1 and everything.queue.qsize() == 2

    async def test_alerts_fire_on_what_matters(self, journal_path):
        async with Journal(journal_path) as journal:
            bus = EventBus(journal)
            seen = []
            alerts = Alerts(bus, rules=AlertRules(reject_burst=2, repeat_after_s=0), sinks=[seen.append])
            await bus.publish("reconcile.orphan", {"venue": "kalshi", "key": "V1", "market_id": "kalshi:KX-A"}, persist=False)
            await bus.publish("engine.halted", {"policy": "cancel", "reason": "operator"}, persist=False)
            await bus.publish("order.rejected", {}, persist=False)
            await bus.publish("order.rejected", {}, persist=False)
            kinds = [a.kind for a in seen]
            assert kinds == ["orphan", "halt", "rejects"]
            assert [a.severity for a in seen] == ["warning", "critical", "warning"]
            assert alerts.check_daily_loss(D("-9"), D("10")).severity == "warning"
            assert alerts.check_daily_loss(D("-11"), D("10")).severity == "critical"
