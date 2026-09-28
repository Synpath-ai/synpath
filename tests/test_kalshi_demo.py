"""Kalshi order entry against the demo environment.

Deselected by default; run with `pytest -m demo`. Needs `KALSHI_KEY_ID`,
`KALSHI_PRIVATE_KEY_PATH` (or `KALSHI_PRIVATE_KEY`) and `KALSHI_ENV=demo`
in the environment or in `synpath/.env`. Refuses to run against `prod`.

Every order here rests far from the touch (buy YES at 0.02, sell YES at
0.98) so nothing fills by accident, and every test leaves the account with
no resting orders. The demo's order store lags order entry by a beat, so
reads after writes are given a moment.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

from synpath.errors import ExchangeNotAvailable
from synpath.trading import (
    EditRequest, OrderRequest, OrderStatus, OrderType, Side, TimeInForce,
)
from synpath.trading.credentials import load_credentials
from synpath.trading.errors import InvalidOrder, OrderNotFound, OrderRejected
from synpath.trading.kalshi import KalshiTrading

pytestmark = pytest.mark.demo
D = Decimal
DOTENV = Path(__file__).resolve().parents[1] / ".env"
LAG = 1.0


@pytest.fixture(scope="module")
def creds():
    loaded = load_credentials(dotenv=DOTENV if DOTENV.exists() else None)
    kalshi = loaded.get("kalshi")
    if kalshi is None:
        pytest.skip("no Kalshi credentials configured")
    if kalshi.env != "demo":
        pytest.skip("demo tests only run against KALSHI_ENV=demo")
    return kalshi


@pytest.fixture(scope="module")
def loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
def kalshi(creds, loop):
    k = KalshiTrading(creds)
    loop.run_until_complete(k.fetch_limits())
    yield k
    loop.run_until_complete(k.cancel_all_orders())
    loop.run_until_complete(k.close())


@pytest.fixture(scope="module")
def market(kalshi, loop) -> str:
    """An open market with a two-sided book: a 0.02 bid and a 0.98 ask rest
    without crossing as long as the touch sits strictly inside."""

    async def find():
        cursor = None
        for _ in range(10):
            page = await kalshi._call("GET", "/markets", params={"status": "open", "limit": 200, "cursor": cursor})
            for m in page["markets"]:
                bid, ask = m.get("yes_bid_dollars"), m.get("yes_ask_dollars")
                if bid and ask and D("0") < D(bid) < D("0.50") < D(ask) < D("1"):
                    return m["ticker"]
            cursor = page.get("cursor")
            if not cursor:
                break
        pytest.skip("no demo market with a two-sided book")

    return loop.run_until_complete(find())


@pytest.fixture(autouse=True)
def clean(kalshi, loop):
    """Leave no resting order behind. The venue's cancel-all answers 503 now
    and then; one retry, then the batch path, so a venue blip does not turn
    every later test into an error."""
    yield
    for attempt in range(2):
        try:
            loop.run_until_complete(kalshi.cancel_all_orders())
            break
        except ExchangeNotAvailable:
            time.sleep(2 * LAG)
    else:
        for order in loop.run_until_complete(kalshi.fetch_open_orders()):
            loop.run_until_complete(kalshi.cancel_order(order.id, market_id=order.market_id))
    time.sleep(LAG)


def run(loop, coro):
    return loop.run_until_complete(coro)


def far(market: str, instrument: str, side: Side, amount="1", **kw) -> OrderRequest:
    """A limit that rests: YES bids at 0.02, YES asks at 0.98, and the NO
    equivalents at the same YES prices."""
    outcome = instrument
    yes_side = side if outcome == "yes" else (Side.SELL if side == Side.BUY else Side.BUY)
    yes_price = D("0.02") if yes_side == Side.BUY else D("0.98")
    price = yes_price if outcome == "yes" else D("1") - yes_price
    return OrderRequest(instrument_id=f"{market}:{outcome}", side=side, amount=D(amount), price=price, **kw)


class TestAccount:
    def test_balance_is_positive_demo_cash(self, kalshi, loop):
        bal = run(loop, kalshi.fetch_balance())
        assert bal.currency == "USD" and bal.available > 0 and bal.locked is None

    def test_limits_shape_the_limiter(self, kalshi):
        snap = kalshi.limiter.snapshot()
        assert snap["read"]["rate"] >= 100 and snap["write"]["rate"] >= 50

    def test_positions_and_settlements_read(self, kalshi, loop):
        run(loop, kalshi.fetch_positions())
        run(loop, kalshi.fetch_settlements(limit=5))


class TestLifecycle:
    def test_place_fetch_edit_cancel(self, kalshi, loop, market):
        placed = run(loop, kalshi.create_order(far(market, "yes", Side.BUY, "2")))
        assert placed.status == OrderStatus.OPEN and placed.remaining == D("2.00")
        time.sleep(LAG)
        read = run(loop, kalshi.fetch_order(placed.id))
        assert read.price == D("0.0200") and read.side == Side.BUY and read.status == OrderStatus.OPEN
        assert run(loop, kalshi.fetch_queue_position(placed.id)) >= 0

        moved = run(loop, kalshi.edit_order(EditRequest(order_id=placed.id, price=D("0.03")), current=read))
        assert moved.queue_priority_preserved is False and moved.price == D("0.0300") and moved.amount == D("2.00")
        smaller = run(loop, kalshi.edit_order(EditRequest(order_id=placed.id, amount=D("1")), current=moved))
        assert smaller.queue_priority_preserved is True and smaller.remaining == D("1.00")
        time.sleep(LAG)
        read = run(loop, kalshi.fetch_order(placed.id))
        assert read.price == D("0.0300") and read.remaining == D("1.00")

        gone = run(loop, kalshi.cancel_order(placed.id, market_id=market))
        assert gone.status == OrderStatus.CANCELED and gone.remaining == 0
        time.sleep(LAG)
        assert run(loop, kalshi.fetch_order(placed.id)).status == OrderStatus.CANCELED

    @pytest.mark.parametrize("outcome,side,book_side,yes_price", [
        ("yes", Side.BUY, "bid", "0.0200"),
        ("yes", Side.SELL, "ask", "0.9800"),
        ("no", Side.BUY, "ask", "0.9800"),
        ("no", Side.SELL, "bid", "0.0200"),
    ])
    def test_every_side_and_outcome_reads_back_on_the_yes_leg(self, kalshi, loop, market, outcome, side, book_side, yes_price):
        placed = run(loop, kalshi.create_order(far(market, outcome, side)))
        assert placed.info["request"]["side"] == book_side and placed.info["request"]["price"] == yes_price
        time.sleep(LAG)
        read = run(loop, kalshi.fetch_order(placed.id))
        assert read.instrument_id == f"{market}:yes"
        assert read.side == (Side.BUY if book_side == "bid" else Side.SELL)
        assert read.price == D(yes_price)
        assert read.info["book_side"] == book_side

    def test_time_in_force(self, kalshi, loop, market):
        gtc = run(loop, kalshi.create_order(far(market, "yes", Side.BUY)))
        assert gtc.status == OrderStatus.OPEN and gtc.time_in_force == TimeInForce.GTC

        ioc = run(loop, kalshi.create_order(far(market, "yes", Side.BUY, time_in_force=TimeInForce.IOC)))
        assert ioc.status == OrderStatus.CANCELED and ioc.filled == 0

        with pytest.raises(OrderRejected) as info:
            run(loop, kalshi.create_order(far(market, "yes", Side.BUY, time_in_force=TimeInForce.FOK)))
        assert info.value.reason == "fill_or_kill_insufficient_resting_volume"

        expires = int(time.time() * 1000) + 3_600_000
        gtd = run(loop, kalshi.create_order(far(market, "yes", Side.BUY, time_in_force=TimeInForce.GTD, expires_at=expires)))
        assert gtd.time_in_force == TimeInForce.GTD and gtd.expires_at == (expires // 1000) * 1000
        time.sleep(LAG)
        read = run(loop, kalshi.fetch_order(gtd.id))
        assert read.time_in_force == TimeInForce.GTD and read.expires_at == gtd.expires_at

    def test_market_order_is_ioc_at_the_protection_price(self, kalshi, loop, market):
        order = run(loop, kalshi.create_order(OrderRequest(
            instrument_id=f"{market}:yes", side=Side.BUY, amount=D("1"), type=OrderType.MARKET, price=D("0.02"),
        )))
        assert order.info["request"]["time_in_force"] == "immediate_or_cancel"
        assert order.status == OrderStatus.CANCELED  # nothing at 0.02 to take

    def test_batch_create_and_cancel(self, kalshi, loop, market):
        results = run(loop, kalshi.create_orders([
            far(market, "yes", Side.BUY),
            far(market, "no", Side.SELL),
            OrderRequest(instrument_id=f"{market}:yes", side=Side.BUY, amount=D("1"), price=D("0.025")),
        ]))
        assert isinstance(results[2], InvalidOrder)
        ids = [r.id for r in results[:2]]
        assert all(ids)
        time.sleep(LAG)
        open_ids = {o.id for o in run(loop, kalshi.fetch_open_orders(market_id=market))}
        assert set(ids) <= open_ids
        cancelled = run(loop, kalshi.cancel_orders(ids, market_id=market))
        assert all(c.status == OrderStatus.CANCELED for c in cancelled)

    def test_cancel_all_on_a_market_counts(self, kalshi, loop, market):
        run(loop, kalshi.create_orders([far(market, "yes", Side.BUY), far(market, "yes", Side.SELL)]))
        time.sleep(LAG)
        assert run(loop, kalshi.cancel_all_orders(market_id=market)) == 2
        time.sleep(LAG)
        assert run(loop, kalshi.fetch_open_orders(market_id=market)) == []

    def test_cancel_all_account_wide(self, kalshi, loop, market):
        run(loop, kalshi.create_order(far(market, "yes", Side.BUY)))
        assert run(loop, kalshi.cancel_all_orders()) is None
        time.sleep(2 * LAG)
        assert run(loop, kalshi.fetch_open_orders()) == []

    def test_off_tick_is_refused_locally_and_by_the_venue(self, kalshi, loop, market):
        with pytest.raises(InvalidOrder):
            run(loop, kalshi.create_order(OrderRequest(instrument_id=f"{market}:yes", side=Side.BUY, amount=D("1"), price=D("0.025"))))
        with pytest.raises(OrderRejected):
            run(loop, kalshi._call("POST", "/portfolio/events/orders", json={
                "ticker": market, "client_order_id": str(uuid.uuid4()), "side": "bid", "count": "1.00",
                "price": "0.0250", "time_in_force": "good_till_canceled", "self_trade_prevention_type": "taker_at_cross",
            }, kind="write"))

    def test_unknown_order(self, kalshi, loop):
        with pytest.raises(OrderNotFound):
            run(loop, kalshi.fetch_order(str(uuid.uuid4()), attempts=1))

    def test_client_order_id_round_trips(self, kalshi, loop, market):
        mine = f"synpath-{uuid.uuid4()}"
        placed = run(loop, kalshi.create_order(far(market, "yes", Side.BUY, client_order_id=mine)))
        assert placed.client_order_id == mine
        time.sleep(LAG)
        assert run(loop, kalshi.fetch_order(placed.id)).client_order_id == mine


class TestOrderGroups:
    def test_trip_and_reset(self, kalshi, loop, market):
        group = run(loop, kalshi.create_order_group(5))
        try:
            placed = run(loop, kalshi.create_order(far(market, "yes", Side.BUY, params={"order_group_id": group})))
            assert placed.status == OrderStatus.OPEN
            run(loop, kalshi.trigger_order_group(group))
            assert run(loop, kalshi.fetch_order_group(group))["is_auto_cancel_enabled"] is True
            time.sleep(LAG)
            assert run(loop, kalshi.fetch_order(placed.id)).status == OrderStatus.CANCELED
            run(loop, kalshi.reset_order_group(group))
            assert run(loop, kalshi.fetch_order_group(group))["is_auto_cancel_enabled"] is False
        finally:
            run(loop, kalshi.delete_order_group(group))


class TestRFQ:
    def test_create_read_delete(self, kalshi, loop, market):
        rfq_id = run(loop, kalshi.create_rfq(market, 10))
        assert rfq_id
        try:
            rfq = run(loop, kalshi.fetch_rfq(rfq_id))
            assert rfq["market_ticker"] == market and rfq["status"] == "open" and rfq["contracts_fp"] == "10.00"
            assert run(loop, kalshi.fetch_quotes(rfq_id)) == []  # nobody quotes on the demo
        finally:
            run(loop, kalshi.delete_rfq(rfq_id))
        assert run(loop, kalshi.fetch_rfq(rfq_id))["status"] != "open"


class TestBudget:
    def test_a_burst_of_reads_never_sees_a_429(self, kalshi, loop):
        from synpath.errors import RateLimitExceeded

        async def burst():
            await asyncio.gather(*(kalshi.fetch_balance() for _ in range(40)))

        try:
            run(loop, burst())
        except RateLimitExceeded as exc:  # pragma: no cover - the assertion
            pytest.fail(f"the limiter let a 429 through: {exc}")

    def test_fee_estimate(self, kalshi, loop, market):
        fee = run(loop, kalshi.fetch_fee_estimate(f"{market}:yes", D("0.50"), D("10")))
        assert fee.taker_fee is not None and fee.taker_fee > 0
