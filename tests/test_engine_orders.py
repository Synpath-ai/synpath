"""Engine-held order types, against the paper venue.

Each type is driven through the real engine: the journal records it, the risk
rules see its children, the ledger books its fills. The venue is
`PaperVenue`, so a book and a tape can be arranged exactly, and the clock is
a dial the test turns.

What each test is for, in the plan's words: the stop family with a
configurable trigger source, an iceberg with a display size and a reload
delay, OCO and bracket linked by size, TWAP across a window, a peg with
bounds, a minimum stay and a level cap, and a taker that walks the book
inside a bound. Plus the two things all of them must do: survive a restart,
and stand down on a halt.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal as D
from pathlib import Path

import pytest

from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.engine.orders import session_expiry
from synpath.engine.orders.day import as_gtd
from synpath.engine.paper import PaperVenue, quadratic_fee
from synpath.trading.types import (
    Account, OrderRequest, OrderStatus, OrderType, Side, TimeInForce,
)

pytestmark = pytest.mark.anyio
ACCOUNT = Account(venue="kalshi", name="paper")
SYM = "kalshi:KX-A"


@pytest.fixture
def anyio_backend():
    return "asyncio"


class Clock:
    """A dial the tests turn, so a ten-minute TWAP takes no time."""

    def __init__(self, start: float = 1_800_000_000.0):
        self.t = start

    def __call__(self) -> float:
        return self.t

    def tick(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
async def setup(tmp_path: Path):
    clock = Clock()
    paper = PaperVenue(venue="kalshi", fees=quadratic_fee(), cash=D("10000"), clock=clock)
    engine = Engine(
        {"kalshi": paper},
        EngineConfig(journal_path=str(tmp_path / "orders.db"), require_lease=True, managed_tick_s=0.01),
        risk=RiskConfig(price_collar=None, duplicate_window_ms=0, max_orders_per_minute=None, closing_soon_s=None,
                        max_open_orders=None),
        accounts={"kalshi": ACCOUNT}, clock=clock,
    )
    paper.subscribe(engine.on_fill)            # the venue reports fills, as a stream would
    paper.subscribe_orders(engine.on_order)    # and the order updates behind them
    await engine.start()
    yield engine, paper, clock
    await engine.stop()


def book(paper: PaperVenue, engine: Engine, *, bids, asks, instrument: str = SYM) -> None:
    """Set the venue's book and hand the engine the same view."""
    paper.set_book(instrument, bids=bids, asks=asks)
    engine.set_book(instrument, paper.books[instrument])


def request(**kw) -> OrderRequest:
    base = dict(market_id=SYM, side=Side.BUY, amount=D("10"), account=ACCOUNT, book="alpha")
    return OrderRequest(**{**base, **kw})


async def feed_book(engine: Engine, instrument: str = SYM, paper: PaperVenue | None = None) -> None:
    """A book update reaches the engine, then whatever it caused is reported."""
    await engine.on_book({"market_id": instrument})
    if paper is not None:
        await paper.deliver()


async def tape(paper: PaperVenue, *, price, amount, taker_side, instrument: str = SYM) -> None:
    """A print on the tape, and the fills it causes, delivered to the engine."""
    paper.on_trade(instrument, price=price, amount=amount, taker_side=taker_side)
    await paper.deliver()


# ---------------------------------------------------------------------------
# Stops
# ---------------------------------------------------------------------------

class TestStops:
    async def test_a_sell_stop_watches_the_bid_and_fires_once(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.45"), D("500"))], asks=[(D("0.47"), D("500"))])
        parent = await engine.submit(request(side=Side.SELL, type=OrderType.STOP_MARKET, stop_price=D("0.40"),
                                             params={"max_slippage": "0.05"}))
        assert parent.status == OrderStatus.WAITING and parent.held_by.value == "engine"

        book(paper, engine, bids=[(D("0.42"), D("500"))], asks=[(D("0.44"), D("500"))])
        await feed_book(engine, paper=paper)
        assert engine.orders.get(parent.id).state == "waiting", "0.42 is above the stop"

        book(paper, engine, bids=[(D("0.39"), D("500"))], asks=[(D("0.41"), D("500"))])
        await feed_book(engine, paper=paper)
        managed = engine.orders.get(parent.id)
        assert managed.state in ("working", "done") and managed.triggered_at is not None
        assert managed.filled == D("10"), "the child took the bid"
        assert [f.side for f in paper.fills] == [Side.SELL]

        # A price that comes back does not un-trigger it.
        book(paper, engine, bids=[(D("0.50"), D("500"))], asks=[(D("0.52"), D("500"))])
        await feed_book(engine, paper=paper)
        assert len(paper.fills) == 1

    async def test_a_buy_stop_watches_the_ask(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.40"), D("100"))], asks=[(D("0.44"), D("100"))])
        parent = await engine.submit(request(type=OrderType.STOP_LIMIT, stop_price=D("0.50"), price=D("0.52")))
        book(paper, engine, bids=[(D("0.48"), D("100"))], asks=[(D("0.51"), D("100"))])
        await feed_book(engine, paper=paper)
        managed = engine.orders.get(parent.id)
        assert managed.state == "done" and managed.filled == D("10")
        assert paper.fills[0].price == D("0.51"), "a stop-limit takes what is inside its limit"

    async def test_the_trigger_source_can_be_the_last_trade(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.30"), D("100"))], asks=[(D("0.70"), D("100"))])
        parent = await engine.submit(request(side=Side.SELL, type=OrderType.STOP_MARKET, stop_price=D("0.40"),
                                             params={"trigger_source": "last", "protection": "0.25"}))
        await feed_book(engine, paper=paper)
        assert engine.orders.get(parent.id).state == "waiting", "a wide book must not fire a last-trade stop"
        await engine.on_trade({"market_id": SYM, "price": "0.38", "amount": "5"})
        assert engine.orders.get(parent.id).state in ("working", "done")

    async def test_a_trailing_stop_follows_one_way_only(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.50"), D("500"))], asks=[(D("0.52"), D("500"))])
        parent = await engine.submit(request(side=Side.SELL, type=OrderType.TRAILING_STOP, stop_price=D("0.47"),
                                             params={"trail": "0.03"}))
        managed = engine.orders.get(parent.id)
        await feed_book(engine, paper=paper)
        assert managed.stop_price == D("0.47")

        book(paper, engine, bids=[(D("0.60"), D("500"))], asks=[(D("0.62"), D("500"))])
        await feed_book(engine, paper=paper)
        assert managed.stop_price == D("0.57"), "the stop follows the bid up"

        book(paper, engine, bids=[(D("0.58"), D("500"))], asks=[(D("0.60"), D("500"))])
        await feed_book(engine, paper=paper)
        assert managed.stop_price == D("0.57"), "and never back down"

        book(paper, engine, bids=[(D("0.56"), D("500"))], asks=[(D("0.58"), D("500"))])
        await feed_book(engine, paper=paper)
        assert managed.state in ("working", "done") and managed.filled == D("10")

    async def test_a_stop_with_no_book_and_no_protection_refuses(self, setup):
        engine, paper, clock = setup
        parent = await engine.submit(request(side=Side.SELL, type=OrderType.STOP_MARKET, stop_price=D("0.40"),
                                             params={"trigger_source": "last"}))
        await engine.on_trade({"market_id": SYM, "price": "0.35", "amount": "1"})
        managed = engine.orders.get(parent.id)
        assert managed.state == "rejected" and "protection" in managed.detail


# ---------------------------------------------------------------------------
# Iceberg
# ---------------------------------------------------------------------------

class TestIceberg:
    async def test_it_shows_a_slice_and_reloads_after_the_delay(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.40"), D("0"))], asks=[(D("0.45"), D("100"))])
        parent = await engine.submit(request(amount=D("30"), price=D("0.40"), type=OrderType.ICEBERG,
                                             params={"display": "10", "reload_delay_s": 5}))
        managed = engine.orders.get(parent.id)
        assert len(managed.children) == 1 and managed.children[0].amount == D("10")
        assert await paper.fetch_open_orders() and (await paper.fetch_open_orders())[0].amount == D("10")

        await tape(paper, price=D("0.40"), amount=D("10"), taker_side=Side.SELL)
        assert managed.filled == D("10") and len(managed.live_children) == 0

        await engine.orders.on_timer()
        assert len(managed.children) == 1, "the reload waits for its delay"
        clock.tick(6)
        await engine.orders.on_timer()
        assert len(managed.children) == 2 and managed.reloads == 1

        await tape(paper, price=D("0.40"), amount=D("10"), taker_side=Side.SELL)
        clock.tick(6)
        await engine.orders.on_timer()
        await tape(paper, price=D("0.40"), amount=D("10"), taker_side=Side.SELL)
        assert managed.filled == D("30") and managed.state == "done"

    async def test_the_hidden_size_is_never_at_the_venue(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[], asks=[(D("0.45"), D("100"))])
        await engine.submit(request(amount=D("100"), price=D("0.40"), type=OrderType.ICEBERG, params={"display": "5"}))
        resting = await paper.fetch_open_orders()
        assert sum(o.amount for o in resting) == D("5")


# ---------------------------------------------------------------------------
# OCO and bracket
# ---------------------------------------------------------------------------

class TestLinked:
    async def test_one_leg_filling_cancels_the_other(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.30"), D("100"))], asks=[(D("0.70"), D("100"))])
        parent = await engine.submit(request(
            amount=D("10"), type=OrderType.OCO,
            params={"legs": [{"type": "limit", "side": "buy", "price": "0.31"},
                             {"type": "limit", "side": "buy", "price": "0.29"}]},
        ))
        await paper.deliver()
        managed = engine.orders.get(parent.id)
        assert len(managed.live_children) == 2
        first = managed.children[0]
        await tape(paper, price=D("0.31"), amount=D("10"), taker_side=Side.SELL)
        assert managed.filled == D("10") and managed.state == "done"
        assert await paper.fetch_open_orders() == [], "the other leg was pulled"

    async def test_a_partial_fill_resizes_the_other_leg(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.30"), D("100"))], asks=[(D("0.70"), D("100"))])
        parent = await engine.submit(request(
            amount=D("10"), type=OrderType.OCO,
            params={"legs": [{"type": "limit", "side": "buy", "price": "0.31"},
                             {"type": "limit", "side": "buy", "price": "0.29"}]},
        ))
        managed = engine.orders.get(parent.id)
        await tape(paper, price=D("0.31"), amount=D("4"), taker_side=Side.SELL)
        assert managed.filled == D("4")
        others = [o for o in await paper.fetch_open_orders() if o.price == D("0.29")]
        assert [o.amount for o in others] == [D("6")], "the other leg now covers what is left"

    async def test_a_bracket_protects_what_the_entry_fills(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.40"), D("100"))], asks=[(D("0.60"), D("100"))])
        parent = await engine.submit(request(
            amount=D("10"), type=OrderType.BRACKET,
            params={"entry": {"type": "limit", "price": "0.41"},
                    "take_profit": {"type": "limit", "price": "0.55"},
                    "stop_loss": {"type": "stop_market", "stop_price": "0.35", "params": {"protection": "0.30"}}},
        ))
        managed = engine.orders.get(parent.id)
        assert managed.entered == D("0") and len(managed.live_children) == 1

        await tape(paper, price=D("0.41"), amount=D("6"), taker_side=Side.SELL)
        assert managed.entered == D("6") and managed.protected == D("6")
        protection = [c for c in managed.children if managed.leg_of.get(c.order_id) in ("take_profit", "stop_loss")]
        assert [c.amount for c in protection] == [D("6"), D("6")]
        assert engine.orders.get(protection[1].order_id).kind == "stop_market", "the stop-loss is its own managed order"

        await tape(paper, price=D("0.41"), amount=D("4"), taker_side=Side.SELL)
        assert managed.entered == D("10") and managed.protected == D("10")

    async def test_a_bracket_whose_entry_never_fills_ends_with_it(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.40"), D("100"))], asks=[(D("0.60"), D("100"))])
        parent = await engine.submit(request(
            amount=D("10"), type=OrderType.BRACKET,
            params={"entry": {"type": "limit", "price": "0.20"}, "take_profit": {"type": "limit", "price": "0.55"}},
        ))
        managed = engine.orders.get(parent.id)
        entry = managed.leg_children("entry")[0]
        await engine.cancel(entry.order_id)
        assert managed.state == "canceled" and "entry" in managed.detail


# ---------------------------------------------------------------------------
# TWAP
# ---------------------------------------------------------------------------

class TestTWAP:
    async def test_it_slices_across_the_window(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.40"), D("100"))], asks=[(D("0.42"), D("100"))])
        parent = await engine.submit(request(amount=D("10"), price=D("0.41"), type=OrderType.TWAP,
                                             params={"window_s": 100, "slices": 5, "style": "limit"}))
        managed = engine.orders.get(parent.id)
        assert managed.sent_slices == 1 and managed.children[0].amount == D("2")

        clock.tick(21)
        await engine.orders.on_timer()
        assert managed.sent_slices == 2 and len(managed.children) == 2

        clock.tick(60)
        await engine.orders.on_timer()
        assert managed.sent_slices == 5, "a late tick catches up rather than skipping"

    async def test_the_window_ends_by_taking_the_rest(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.40"), D("100"))], asks=[(D("0.42"), D("100"))])
        parent = await engine.submit(request(amount=D("10"), price=D("0.45"), type=OrderType.TWAP,
                                             params={"window_s": 60, "slices": 3, "style": "taker",
                                                     "limit": "0.45", "finish": "complete"}))
        await paper.deliver()
        managed = engine.orders.get(parent.id)
        assert managed.filled > 0, "a taker slice crosses at once"
        clock.tick(61)
        await engine.orders.on_timer()
        await paper.deliver()
        assert managed.filled == D("10") and managed.state == "done"

    async def test_finish_stop_leaves_the_rest_undone(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.40"), D("100"))], asks=[(D("0.42"), D("100"))])
        parent = await engine.submit(request(amount=D("10"), price=D("0.10"), type=OrderType.TWAP,
                                             params={"window_s": 30, "slices": 3, "style": "limit",
                                                     "limit": "0.10", "finish": "stop"}))
        managed = engine.orders.get(parent.id)
        clock.tick(31)
        await engine.orders.on_timer()
        assert managed.state in ("canceled", "done") and managed.filled == D("0")
        assert await paper.fetch_open_orders() == []


# ---------------------------------------------------------------------------
# Peg
# ---------------------------------------------------------------------------

class TestPeg:
    async def test_it_follows_the_touch_within_its_bounds(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.40"), D("100"))], asks=[(D("0.44"), D("100"))])
        parent = await engine.submit(request(amount=D("10"), price=D("0.40"), type=OrderType.PEG,
                                             params={"reference": "near", "offset": "0", "min_stay_s": 0,
                                                     "max_price": "0.45", "level_cap": 3}))
        managed = engine.orders.get(parent.id)
        assert managed.resting_price == D("0.40")

        book(paper, engine, bids=[(D("0.41"), D("100"))], asks=[(D("0.44"), D("100"))])
        await feed_book(engine, paper=paper)
        assert managed.resting_price == D("0.41") and managed.moves == 1
        assert [o.price for o in await paper.fetch_open_orders()] == [D("0.41")]

        book(paper, engine, bids=[(D("0.50"), D("100"))], asks=[(D("0.52"), D("100"))])
        await feed_book(engine, paper=paper)
        assert managed.resting_price == D("0.45"), "the bound caps the chase"

    async def test_the_minimum_stay_and_the_level_cap_hold_it_still(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.40"), D("100"))], asks=[(D("0.44"), D("100"))])
        parent = await engine.submit(request(amount=D("10"), price=D("0.40"), type=OrderType.PEG,
                                             params={"reference": "near", "min_stay_s": 10, "level_cap": 2}))
        managed = engine.orders.get(parent.id)
        book(paper, engine, bids=[(D("0.41"), D("100"))], asks=[(D("0.44"), D("100"))])
        await feed_book(engine, paper=paper)
        assert managed.resting_price == D("0.40"), "it has not sat long enough to move"

        clock.tick(11)
        await feed_book(engine, paper=paper)
        assert managed.resting_price == D("0.41") and managed.moves == 1

        clock.tick(11)
        book(paper, engine, bids=[(D("0.42"), D("100"))], asks=[(D("0.44"), D("100"))])
        await feed_book(engine, paper=paper)
        clock.tick(11)
        book(paper, engine, bids=[(D("0.43"), D("100"))], asks=[(D("0.44"), D("100"))])
        await feed_book(engine, paper=paper)
        assert managed.moves == 2 and managed.resting_price == D("0.42"), "the level cap stopped the chase"


# ---------------------------------------------------------------------------
# Taking
# ---------------------------------------------------------------------------

class TestTaker:
    async def test_a_walked_market_order_stops_at_the_slippage_bound(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[], asks=[(D("0.42"), D("3")), (D("0.44"), D("3")), (D("0.60"), D("50"))])
        parent = await engine.submit(request(amount=D("20"), type=OrderType.MARKET,
                                             params={"walk": True, "max_slippage": "0.03"}))
        await paper.deliver()
        managed = engine.orders.get(parent.id)
        assert managed.filled == D("6"), "0.42 and 0.44 are inside 0.45; 0.60 is not"
        assert managed.state in ("done", "canceled") and "unfilled" in managed.detail

    async def test_a_smart_taker_clips_over_time(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[], asks=[(D("0.42"), D("100"))])
        parent = await engine.submit(request(amount=D("10"), type=OrderType.SMART_TAKER,
                                             params={"clip": "4", "interval_s": 5, "limit": "0.45"}))
        await paper.deliver()
        managed = engine.orders.get(parent.id)
        assert managed.filled == D("4") and managed.clips == 1

        await engine.orders.on_timer()
        assert managed.clips == 1, "the interval has not passed"
        clock.tick(6)
        await engine.orders.on_timer()
        await paper.deliver()
        assert managed.clips == 2 and managed.filled == D("8")
        clock.tick(6)
        await engine.orders.on_timer()
        await paper.deliver()
        assert managed.filled == D("10") and managed.state == "done"

    async def test_a_smart_taker_waits_rather_than_paying_up(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[], asks=[(D("0.60"), D("100"))])
        parent = await engine.submit(request(amount=D("10"), type=OrderType.SMART_TAKER,
                                             params={"clip": "5", "interval_s": 1, "limit": "0.45"}))
        managed = engine.orders.get(parent.id)
        assert managed.filled == D("0") and managed.state == "working"
        book(paper, engine, bids=[], asks=[(D("0.44"), D("100"))])
        clock.tick(2)
        await engine.orders.on_timer()
        await paper.deliver()
        assert managed.filled == D("5")


# ---------------------------------------------------------------------------
# Day, restarts and halts
# ---------------------------------------------------------------------------

class TestLifecycle:
    def test_day_becomes_a_venue_held_expiry(self):
        request_day = OrderRequest(market_id=SYM, side=Side.BUY, amount=D("1"), price=D("0.4"),
                                   time_in_force=TimeInForce.DAY)
        rewritten = as_gtd(request_day, now_s=1_800_000_000.0, timezone_name="America/New_York",
                           session_end="23:59:59")
        assert rewritten.time_in_force == TimeInForce.GTD
        assert rewritten.expires_at == session_expiry(1_800_000_000.0, timezone_name="America/New_York")
        assert rewritten.params["requested_time_in_force"] == "day"
        # The zone matters: London's day ends five hours earlier than New York's.
        london = session_expiry(1_800_000_000.0, timezone_name="Europe/London")
        new_york = session_expiry(1_800_000_000.0, timezone_name="America/New_York")
        assert new_york - london == 5 * 3600 * 1000

    async def test_a_day_order_reaches_the_venue_as_gtd(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.30"), D("10"))], asks=[(D("0.60"), D("10"))])
        order = await engine.submit(request(price=D("0.31"), time_in_force=TimeInForce.DAY))
        assert order.time_in_force == TimeInForce.GTD and order.expires_at is not None
        stored = await engine.journal.order("kalshi", order.id)
        assert stored.expires_at == order.expires_at

    async def test_a_restart_brings_the_parents_back(self, setup, tmp_path):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.45"), D("500"))], asks=[(D("0.47"), D("500"))])
        parent = await engine.submit(request(side=Side.SELL, type=OrderType.TRAILING_STOP, stop_price=D("0.40"),
                                             params={"trail": "0.03"}))
        book(paper, engine, bids=[(D("0.60"), D("500"))], asks=[(D("0.62"), D("500"))])
        await feed_book(engine, paper=paper)
        assert engine.orders.get(parent.id).stop_price == D("0.57")
        path = engine.config.journal_path
        await engine.journal.close()

        restarted = Engine({"kalshi": paper}, EngineConfig(journal_path=path, managed_tick_s=0.01),
                           risk=RiskConfig(price_collar=None, duplicate_window_ms=0), accounts={"kalshi": ACCOUNT},
                           clock=clock)
        paper.listeners.clear()          # the old engine's journal is closed
        paper.order_listeners.clear()
        paper.subscribe(restarted.on_fill)
        paper.subscribe_orders(restarted.on_order)
        recovery = await restarted.start()
        try:
            assert recovery.managed == 1
            revived = restarted.orders.get(parent.id)
            assert revived.stop_price == D("0.57"), "the trail resumes where the market left it"
            restarted.set_book(SYM, paper.books[SYM])
            book(paper, restarted, bids=[(D("0.56"), D("500"))], asks=[(D("0.58"), D("500"))])
            await feed_book(restarted, paper=paper)
            assert revived.state in ("working", "done") and revived.filled == D("10")
        finally:
            await restarted.stop()

    async def test_a_halt_stands_every_parent_down(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.40"), D("100"))], asks=[(D("0.60"), D("100"))])
        await engine.submit(request(amount=D("10"), price=D("0.35"), type=OrderType.ICEBERG, params={"display": "2"}))
        await engine.submit(request(side=Side.SELL, type=OrderType.STOP_MARKET, stop_price=D("0.20"),
                                    params={"protection": "0.15"}))
        result = await engine.halt("drill", policy="cancel")
        assert result["managed"] == 2
        assert all(not p.live for p in engine.orders.parents.values())
        assert await paper.fetch_open_orders() == []

    async def test_cancelling_a_parent_pulls_its_children(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.40"), D("100"))], asks=[(D("0.60"), D("100"))])
        parent = await engine.submit(request(amount=D("10"), price=D("0.35"), type=OrderType.ICEBERG,
                                             params={"display": "4"}))
        assert len(await paper.fetch_open_orders()) == 1
        canceled = await engine.cancel(parent.id)
        assert canceled.status == OrderStatus.CANCELED
        assert await paper.fetch_open_orders() == []
        assert engine.orders.get(parent.id).state == "canceled"

    async def test_parents_are_not_reported_as_ghosts(self, setup):
        """A parent is not an order at the venue, so reconciliation must not
        report it missing."""
        from synpath.engine.reconcile import Reconciler

        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.40"), D("100"))], asks=[(D("0.60"), D("100"))])
        parent = await engine.submit(request(side=Side.SELL, type=OrderType.STOP_MARKET, stop_price=D("0.10"),
                                             params={"protection": "0.05"}))
        report = (await Reconciler(engine).run("kalshi"))[0]
        assert [d.key for d in report.ghosts] == []
        assert engine.orders.get(parent.id).state == "waiting"


class TestOrderFields:
    """`reduce_only`, `post_only` and `expires_at` on engine-held orders: honoured or refused, never ignored."""

    @staticmethod
    def record(paper: PaperVenue) -> list[OrderRequest]:
        sent: list[OrderRequest] = []
        original = paper.create_order

        async def create_order(req):
            sent.append(req)
            return await original(req)
        paper.create_order = create_order  # type: ignore[method-assign]
        return sent

    async def test_a_reduce_only_stop_sends_reduce_only_children(self, setup):
        engine, paper, clock = setup
        sent = self.record(paper)
        book(paper, engine, bids=[(D("0.45"), D("500"))], asks=[(D("0.47"), D("500"))])
        await engine.submit(request(side=Side.SELL, type=OrderType.STOP_MARKET, stop_price=D("0.40"), reduce_only=True,
                                    params={"max_slippage": "0.05"}))
        book(paper, engine, bids=[(D("0.39"), D("500"))], asks=[(D("0.41"), D("500"))])
        await feed_book(engine, paper=paper)
        assert sent and all(r.reduce_only for r in sent), "a protective stop must not open a position"

    async def test_bracket_legs_inherit_reduce_only_unless_they_say_otherwise(self, setup):
        from synpath.engine.orders.oco import leg_request

        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.45"), D("500"))], asks=[(D("0.47"), D("500"))])
        parent = await engine.submit(request(side=Side.SELL, type=OrderType.OCO, reduce_only=True, params={"legs": [
            {"side": "sell", "type": "limit", "price": "0.60"}, {"side": "sell", "type": "limit", "price": "0.65"}]}))
        managed = engine.orders.get(parent.id)
        assert leg_request({"type": "limit", "price": "0.6"}, parent=managed).reduce_only is True
        assert leg_request({"type": "limit", "price": "0.6", "reduce_only": False}, parent=managed).reduce_only is False

    async def test_post_only_reaches_the_slices_of_types_that_rest(self, setup):
        engine, paper, clock = setup
        sent = self.record(paper)
        book(paper, engine, bids=[(D("0.40"), D("100"))], asks=[(D("0.45"), D("100"))])
        await engine.submit(request(amount=D("30"), price=D("0.40"), type=OrderType.ICEBERG, post_only=True,
                                    params={"display": "10"}))
        assert sent and all(r.post_only for r in sent)

    async def test_post_only_on_a_type_that_takes_is_refused(self, setup):
        from synpath.errors import BadRequest

        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.45"), D("500"))], asks=[(D("0.47"), D("500"))])
        with pytest.raises(BadRequest, match="takes liquidity"):
            await engine.submit(request(side=Side.SELL, type=OrderType.STOP_MARKET, stop_price=D("0.40"),
                                        post_only=True, params={"max_slippage": "0.05"}))
        with pytest.raises(BadRequest, match="takes liquidity"):
            await engine.submit(request(amount=D("20"), price=D("0.47"), type=OrderType.TWAP, post_only=True,
                                        params={"window_s": 60, "slices": 2, "style": "taker"}))
        with pytest.raises(BadRequest, match="legs"):
            await engine.submit(request(type=OrderType.OCO, post_only=True, params={"legs": []}))

    async def test_a_parent_ends_at_its_expiry_and_pulls_its_children(self, setup):
        engine, paper, clock = setup
        book(paper, engine, bids=[(D("0.40"), D("0"))], asks=[(D("0.45"), D("100"))])
        expires = int((clock() + 60) * 1000)
        parent = await engine.submit(request(amount=D("30"), price=D("0.40"), type=OrderType.ICEBERG,
                                             expires_at=expires, params={"display": "10"}))
        managed = engine.orders.get(parent.id)
        assert len(managed.live_children) == 1
        await engine.orders.on_timer()
        assert managed.live, "not yet"
        clock.tick(61)
        await engine.orders.on_timer()
        assert managed.state == "canceled" and not managed.live_children
        assert await paper.fetch_open_orders() == []

    async def test_an_expiry_already_past_is_refused(self, setup):
        from synpath.errors import BadRequest

        engine, paper, clock = setup
        with pytest.raises(BadRequest, match="past"):
            await engine.submit(request(amount=D("30"), price=D("0.40"), type=OrderType.ICEBERG,
                                        expires_at=int((clock() - 1) * 1000), params={"display": "10"}))
