"""An order says what it does: outcome (YES/NO) and action (buy/sell), beside its YES-leg side."""
from decimal import Decimal as D

import pytest

from synpath.trading.types import Order, OrderRequest, OrderStatus, OrderType, Side, TimeInForce, yes_side


def request(**changes):
    return OrderRequest(**{"market_id": "polymarket:1", "quantity": D("5"), "price": D("0.40"), **changes})


@pytest.mark.parametrize("outcome,action,side", [
    ("yes", "buy", Side.BUY), ("yes", "sell", Side.SELL), ("no", "buy", Side.SELL), ("no", "sell", Side.BUY)])
def test_outcome_and_action_set_the_yes_leg_side(outcome, action, side):
    made = request(outcome=outcome, action=action)
    assert made.side == side == yes_side(outcome, action) and made.leg == (outcome, action)


def test_a_side_alone_reads_as_opening_and_a_disagreeing_side_is_refused():
    assert request(side=Side.BUY).leg == ("yes", "buy")
    assert request(side=Side.SELL).leg == ("no", "buy")
    with pytest.raises(ValueError, match="is not sell yes"):
        request(side=Side.BUY, outcome="yes", action="sell")
    with pytest.raises(ValueError, match="together"):
        request(side=Side.BUY, outcome="yes")


def test_reduce_only_is_refused_on_a_new_request_and_read_as_its_meaning_from_storage():
    with pytest.raises(ValueError, match="reduce_only is removed"):
        request(side=Side.SELL, reduce_only=True)
    stored = {"market_id": "polymarket:1", "quantity": "5", "price": "0.40", "side": "sell", "reduce_only": True}
    assert OrderRequest.stored(stored).leg == ("yes", "sell")
    assert OrderRequest.stored({**stored, "side": "buy"}).leg == ("no", "sell")
    assert OrderRequest.stored({**stored, "reduce_only": False}).leg == ("no", "buy")
    order = Order.model_validate({"id": "o", "venue": "polymarket", "market_id": "polymarket:1", "side": "sell",
                                  "type": "limit", "time_in_force": "gtc", "status": "open", "quantity": "5",
                                  "reduce_only": True})
    assert (order.outcome, order.action) == ("yes", "sell")
