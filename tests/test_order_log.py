"""The order log: the journal's order events read back as one log, repeats folded."""
from __future__ import annotations

from decimal import Decimal as D
from pathlib import Path

import pytest

from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.engine.order_log import order_log
from synpath.engine.paper import PaperVenue
from synpath.trading.types import Account, OrderRequest

pytestmark = pytest.mark.anyio
K = "kalshi:KX-LOG"
ACCOUNT = Account(venue="kalshi", name="paper")


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def engine(tmp_path: Path):
    venue = PaperVenue(venue="kalshi")
    venue.set_book(K, bids=[(D("0.40"), D("100"))], asks=[(D("0.45"), D("100"))])
    made = Engine({"kalshi": venue}, EngineConfig(journal_path=str(tmp_path / "log.db"), require_lease=True),
                  risk=RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None),
                  accounts={"kalshi": ACCOUNT})
    await made.start()
    yield made
    await made.stop()


async def test_a_placed_then_cancelled_order_reads_as_its_steps(engine):
    order = await engine.submit(OrderRequest(market_id=K, outcome="yes", action="buy", quantity=D("2"),
                                             price=D("0.30"), client_order_id="log-1", account=ACCOUNT))
    await engine.cancel(order.id)
    rows, cursor = await order_log(engine, order_id=order.id)
    kinds = [row["kind"] for row in reversed(rows)]
    assert kinds[:2] == ["submission.reserved", "intent.planned"] and "order.canceled" in kinds and cursor is None
    created = next(row for row in rows if row["kind"] == "intent.planned" and row["detail"] == "create")
    assert (created["client_order_id"], created["outcome"], created["action"], created["detail"]) == \
        ("log-1", "yes", "buy", "create")
    assert any(row["kind"] == "intent.planned" and row["detail"] == "cancel" for row in rows), "the cancel is logged"


async def test_the_account_log_pages_newest_first_and_folds_repeats(engine):
    for n in range(3):
        await engine.submit(OrderRequest(market_id=K, side="buy", quantity=D("1"), price=D("0.3") + D(n) / 100,
                                         client_order_id=f"page-{n}", account=ACCOUNT))
    first, cursor = await order_log(engine, limit=4)
    assert [row["seq"] for row in first] == sorted((row["seq"] for row in first), reverse=True)
    assert cursor is not None
    rest, _ = await order_log(engine, limit=200, before=cursor)
    assert all(row["seq"] < first[-1]["seq"] for row in rest)
    signatures = [(row["kind"], row["order_id"] or row["client_order_id"], row["status"], row["filled"])
                  for row in first + rest]
    assert all(a != b for a, b in zip(signatures, signatures[1:])), "a repeat is shown once"


def test_a_cancel_s_intent_is_read_as_its_venue_and_order():
    from synpath.engine.order_log import entry
    from synpath.engine.storage_types import JournalEvent
    for kind, payload in (("intent.sending", {"order_id": None}), ("intent.settled", {"order_id": "o-1"})):
        row = entry(JournalEvent(1, 0, kind, "cancel:kalshi:o-1", payload))
        assert (row["venue"], row["order_id"]) == ("kalshi", "o-1")
