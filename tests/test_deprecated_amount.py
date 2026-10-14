"""`amount` still works for one release, with a warning; `quantity` is the name.

TODO(0.9): delete this file with `synpath._deprecated`.
"""
from __future__ import annotations

import warnings
from decimal import Decimal as D

import pytest

from synpath import EditRequest, Fill, Order, OrderRequest, Precision
from synpath.bucket import BucketOrderReport
from synpath.engine.paper import PaperVenue
from synpath.engine.router import Leg, Plan
from synpath.server.trading import EditBody
from synpath.trading.money import validate_amount
from synpath.types import HistoricalTrade, Trade
from synpath.ws.base import TradeEvent

DEPRECATED = pytest.warns(DeprecationWarning, match="`amount` is deprecated, use `quantity`")


def order(**kw) -> Order:
    return Order(**{"id": "1", "venue": "kalshi", "market_id": "kalshi:X", "side": "buy", "type": "limit",
                    "time_in_force": "gtc", "status": "open", "quantity": D("5"), **kw})


class TestModelInput:
    def test_order_request_takes_amount_and_warns(self):
        with DEPRECATED:
            request = OrderRequest(market_id="kalshi:X", side="buy", amount=D("3"), price=D("0.5"))
        assert request.quantity == D("3")
        assert "amount" not in request.model_dump() and "amount" not in request.model_dump_json()

    def test_edit_request_and_the_rest_body(self):
        with DEPRECATED:
            assert EditRequest(order_id="1", amount=D("2")).quantity == D("2")
        with DEPRECATED:
            assert EditBody.model_validate({"amount": "4"}).quantity == D("4")

    def test_quantity_needs_no_shim(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            OrderRequest(market_id="kalshi:X", side="buy", quantity=D("3"), price=D("0.5"))
            order().quantity

    def test_hosted_history_amount_is_read_quietly(self):
        row = {"id": "1", "market_id": "kalshi:X", "timestamp": 1, "datetime": "x", "price": 0.5, "amount": 3,
               "observed_at_ms": 1, "timestamp_source": "venue"}
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            trade = HistoricalTrade.model_validate(row)
        assert trade.quantity == 3 and "amount" not in trade.model_dump()


class TestAttributeReads:
    def test_order_and_fill(self):
        o = order(last_fill_quantity=D("1"))
        with DEPRECATED:
            assert o.amount == D("5")
        with pytest.warns(DeprecationWarning, match="last_fill_amount"):
            assert o.last_fill_amount == D("1")
        fill = Fill(id="f", order_id="1", venue="kalshi", market_id="kalshi:X", side="buy", price=D("0.5"),
                    quantity=D("2"), timestamp=1)
        with DEPRECATED:
            assert fill.amount == D("2")

    def test_trade_precision_and_bucket_report(self):
        trade = Trade(id="1", market_id="kalshi:X", timestamp=1, datetime="x", price=0.5, quantity=3)
        with DEPRECATED:
            assert trade.amount == 3
        precision = Precision(tick=D("0.01"), min_quantity=D("1"), quantity_step=D("1"))
        with pytest.warns(DeprecationWarning, match="min_amount"):
            assert precision.min_amount == D("1")
        with pytest.warns(DeprecationWarning, match="amount_step"):
            assert precision.amount_step == D("1")
        report = BucketOrderReport(order_id="o", bucket_id="b", status="open", side="buy", quantity=D("5"),
                                   worst_price=D("0.5"), filled=D("0"), unfilled=D("5"), average_price=None,
                                   fees_paid=D("0"), rounds=0, stop_reason=None, per_venue=[])
        with DEPRECATED:
            assert report.amount == D("5")
        assert "amount" not in report.model_dump()

    def test_stream_events_and_route_plans(self):
        event = TradeEvent(venue="kalshi", market_id="kalshi:X", id=None, price=D("0.5"), quantity=D("7"))
        with DEPRECATED:
            assert event.amount == D("7")
        plan = Plan(side="buy", quantity=D("3"), limit=D("0.5"))
        leg = Leg(market_id="kalshi:X", flip=False, side="buy", price=D("0.5"), quantity=D("3"),
                  bucket_price=D("0.5"), net_price=D("0.5"))
        with DEPRECATED:
            assert plan.amount == D("3") and leg.amount == D("3")


class TestKeywordArguments:
    async def test_fetch_fee_estimate_and_paper_tape(self):
        paper = PaperVenue(venue="kalshi")
        with DEPRECATED:
            estimate = await paper.fetch_fee_estimate("kalshi:X", "buy", D("0.5"), amount=D("10"))
        assert estimate.quantity == D("10")
        with DEPRECATED:
            assert paper.on_trade("kalshi:X", price=D("0.5"), amount=D("1")) == []
        with pytest.raises(TypeError, match="both"):
            paper.on_trade("kalshi:X", price=D("0.5"), amount=D("1"), quantity=D("1"))

    def test_validate_amount(self):
        with pytest.warns(DeprecationWarning, match="validate_amount"):
            assert validate_amount(D("2"), Precision(tick=D("0.01"))) == D("2")


@pytest.fixture
def anyio_backend():
    return "asyncio"


pytestmark = pytest.mark.anyio
