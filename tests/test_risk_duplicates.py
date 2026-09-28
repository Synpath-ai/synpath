"""The duplicate window is for an accidental second click, not for the engine's own slices."""
from __future__ import annotations

from decimal import Decimal as D

from synpath.engine.risk import RiskConfig, RiskEngine
from synpath.trading.types import Account, OrderRequest, OrderType, Side

ACCOUNT = Account(venue="kalshi")


class Clock:
    t = 1_800_000_000.0

    def __call__(self):
        return self.t


def req(**kw) -> OrderRequest:
    return OrderRequest(**{"market_id": "kalshi:KX-A", "side": Side.BUY, "amount": D("1"), "type": OrderType.LIMIT,
                           "price": D("0.40"), **kw})


def risk() -> RiskEngine:
    engine = RiskEngine(RiskConfig(price_collar=None, closing_soon_s=None), clock=Clock())
    engine.record_sent(req())
    return engine


def test_the_same_order_from_outside_is_refused_within_the_window():
    decision = risk().check(req(), venue="kalshi", account=ACCOUNT)
    assert not decision.ok and decision.rule == "duplicate"


def test_an_engine_child_that_looks_the_same_is_not_a_duplicate():
    decision = risk().check(req(tags={"parent": "mo-2"}), venue="kalshi", account=ACCOUNT)
    assert decision.ok, decision.message


def test_a_caller_who_names_the_client_order_id_is_not_clicking_twice():
    decision = risk().check(req(client_order_id="mine-2"), venue="kalshi", account=ACCOUNT, deliberate=True)
    assert decision.ok, decision.message
