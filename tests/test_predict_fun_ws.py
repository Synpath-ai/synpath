"""predict.fun's streams. Market messages are recorded from the live venue;
wallet events follow the venue's documented examples (they need a wallet
JWT, which the trading adapter creates)."""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest

from synpath import AuthenticationError, PredictFunMarketStream, PredictFunUserStream
from synpath.trading.predict_fun import fill_of_event, order_of_event
from synpath.trading.types import Liquidity, OrderStatus, OrderType, SettlementState, Side
from synpath.ws.base import (
    BookEvent, FillEvent, MarketStatusEvent, OrderEvent, QuoteEvent, StreamStatusEvent,
)

from conftest import load

WS = load("predict_fun_ws.json")
BOOK = WS["predictOrderbook"]
NATIVE = BOOK["topic"].split("/")[1]
MARKET = f"predict_fun:{NATIVE}"

BASE_EVENT = {
    "type": "orderAccepted", "orderId": "123456789", "orderHash": "0xabc", "walletAddress": "0xdef",
    "timestamp": 1736696400000,
    "details": {"marketId": 123, "outcomeIndex": 0, "marketQuestion": "Will X happen?", "outcome": "YES",
                "quoteType": "BID", "quantity": "100.000", "quantityFilled": "0.000", "price": "0.620",
                "value": "62.00", "valueFilled": "0.00", "strategyType": "LIMIT", "categorySlug": "politics"},
}
"""The venue's documented example of a wallet event."""


def event(kind: str, **extra) -> dict:
    return {**BASE_EVENT, "type": kind, **extra}


def details(**changes) -> dict:
    return {**BASE_EVENT["details"], **changes}


def drain(stream) -> list:
    out = []
    while not stream._queue.empty():
        out.append(stream._queue.get_nowait())
    return out


class FakeSocket:
    def __init__(self):
        self.sent: list[dict] = []

    async def send(self, frame):
        self.sent.append(json.loads(frame))


async def watching(stream, *calls):
    stream._ws = FakeSocket()
    for call in calls:
        await call
    return stream._ws.sent


def market_stream() -> PredictFunMarketStream:
    return PredictFunMarketStream(api_key="test-key")


class TestRecords:
    def test_an_accepted_bid_on_yes_is_an_open_buy(self):
        order = order_of_event(BASE_EVENT)
        assert (order.id, order.market_id, order.side, order.price) == ("0xabc", "predict_fun:123", Side.BUY, Decimal("0.620"))
        assert order.status == OrderStatus.OPEN and order.remaining == Decimal("100") and order.type == OrderType.LIMIT

    def test_a_bid_on_no_is_a_sell_on_yes(self):
        order = order_of_event(event("orderAccepted", details=details(outcome="NO", price="0.300")))
        assert order.side == Side.SELL and order.price == Decimal("0.700")

    @pytest.mark.parametrize("kind, status", [
        ("orderNotAccepted", OrderStatus.REJECTED), ("orderExpired", OrderStatus.EXPIRED),
        ("orderCancelled", OrderStatus.CANCELED),
    ])
    def test_order_states(self, kind, status):
        assert order_of_event(event(kind)).status == status

    def test_a_fully_filled_order_is_closed(self):
        done = event("orderTransactionSuccess", details=details(quantityFilled="100.000"))
        assert order_of_event(done).status == OrderStatus.CLOSED

    def test_a_fill_moves_from_matched_to_confirmed_under_one_id(self):
        fill = {"executedPriceWei": str(62 * 10**16), "executedSizeWei": str(40 * 10**18), "executedValueWei": str(int(24.8 * 10**18))}
        submitted = fill_of_event(event("orderTransactionSubmitted", settlementId="s1", fill=fill))
        success = fill_of_event(event("orderTransactionSuccess", settlementId="s1", fill=fill, isMaker=False,
                                      fee={"amountWei": str(76 * 10**14), "type": "COLLATERAL"}))
        failed = fill_of_event(event("orderTransactionFailed", settlementId="s1", fill=fill))
        assert submitted.id == success.id == failed.id == "s1"
        assert (submitted.settlement, success.settlement, failed.settlement) == (
            SettlementState.MATCHED, SettlementState.CONFIRMED, SettlementState.FAILED)
        assert success.amount == Decimal("40") and success.price == Decimal("0.62")
        assert success.fee == Decimal("0.0076") and success.fee_currency == "USDT" and success.liquidity == Liquidity.TAKER

    def test_an_order_event_is_not_a_fill(self):
        assert fill_of_event(BASE_EVENT) is None


class TestMarketStream:
    def test_a_key_is_required(self, monkeypatch):
        monkeypatch.delenv("PREDICT_FUN_API_KEY", raising=False)
        monkeypatch.chdir("/")
        with pytest.raises(AuthenticationError, match="developers.predict.fun"):
            PredictFunMarketStream()

    def test_the_key_is_in_the_handshake(self):
        assert market_stream().headers() == {"x-api-key": "test-key"}

    def test_subscriptions_are_one_topic_each_and_sent_once(self):
        stream = market_stream()
        sent = asyncio.run(watching(stream, stream.watch_order_book([MARKET]), stream.watch_ticker([MARKET]),
                                    stream.watch_market_status([MARKET])))
        assert [f["params"] for f in sent] == [
            [f"predictOrderbook/{NATIVE}"], [f"predictMarketStatus/{NATIVE}"], [f"predictTradingStatus/{NATIVE}"]]
        assert [f["requestId"] for f in sent] == [1, 2, 3] and all(f["method"] == "subscribe" for f in sent)

    def test_every_book_message_is_a_snapshot_in_yes_prices(self):
        stream = market_stream()
        asyncio.run(watching(stream, stream.watch_order_book([MARKET])))
        stream._dispatch(json.dumps(BOOK))
        [book_event] = [e for e in drain(stream) if isinstance(e, BookEvent)]
        assert book_event.kind == "snapshot" and book_event.market_id == MARKET
        book = stream.book(MARKET)
        assert book.best_bid == Decimal(str(BOOK["data"]["bids"][0][0]))
        assert stream.book(MARKET, "no").best_ask == 1 - book.best_bid

    def test_the_ticker_reports_a_new_last_price_once(self):
        stream = market_stream()
        asyncio.run(watching(stream, stream.watch_ticker([MARKET])))
        stream._dispatch(json.dumps(BOOK))
        stream._dispatch(json.dumps(BOOK))
        first, second = [e for e in drain(stream) if isinstance(e, QuoteEvent)]
        settled = BOOK["data"]["lastOrderSettled"]
        expected = Decimal(settled["price"]) if settled["outcome"] == "Yes" else 1 - Decimal(settled["price"])
        assert first.last == expected and second.last is None
        assert first.bid == Decimal(str(BOOK["data"]["bids"][0][0]))

    def test_market_and_trading_status(self):
        stream = market_stream()
        stream._dispatch(json.dumps(WS["predictMarketStatus"]))
        stream._dispatch(json.dumps(WS["predictTradingStatus"]))
        states = [e.state for e in drain(stream) if isinstance(e, MarketStatusEvent)]
        assert states == ["open", "open"]

    def test_heartbeats_are_echoed_exactly(self):
        stream = market_stream()

        async def run():
            stream._ws = FakeSocket()
            stream._dispatch(json.dumps(WS["heartbeat"]))
            await asyncio.sleep(0)
            return stream._ws.sent

        assert asyncio.run(run()) == [{"method": "heartbeat", "data": WS["heartbeat"]["data"]}]

    def test_a_refused_subscription_is_reported(self):
        stream = market_stream()
        asyncio.run(watching(stream, stream.watch_order_book([MARKET])))
        stream._dispatch(json.dumps({"type": "R", "requestId": 1, "success": False,
                                     "error": {"code": "invalid_topic", "message": "unknown topic"}}))
        assert [e.state for e in drain(stream) if isinstance(e, StreamStatusEvent)] == ["error"]

    def test_a_reconnect_marks_books_not_ready(self):
        stream = market_stream()
        asyncio.run(watching(stream, stream.watch_order_book([MARKET])))
        stream._dispatch(json.dumps(BOOK))
        stream.on_disconnect()
        assert not stream.book(MARKET).ready


class TestUserStream:
    def test_the_jwt_is_the_topic_and_never_reported(self):
        stream = PredictFunUserStream(api_key="test-key", jwt="secret.jwt.value")
        sent = asyncio.run(watching(stream, stream.watch_orders()))
        assert sent[0]["params"] == ["predictWalletEvents/secret.jwt.value"]
        stream.status("error", "refused predictWalletEvents/secret.jwt.value")
        assert "secret.jwt.value" not in drain(stream)[-1].detail

    def test_events_become_orders_and_fills(self):
        stream = PredictFunUserStream(api_key="test-key", jwt="j")
        fill = {"executedPriceWei": str(62 * 10**16), "executedSizeWei": str(10**19), "executedValueWei": "0"}
        stream._dispatch(json.dumps({"type": "M", "topic": "predictWalletEvents/j",
                                     "data": event("orderTransactionSuccess", settlementId="s", fill=fill)}))
        events = drain(stream)
        assert [type(e) for e in events] == [OrderEvent, FillEvent]
        assert events[1].fill.amount == Decimal("10")

    def test_an_expired_jwt_is_renewed_from_its_source(self):
        tokens = iter(["old", "new"])
        stream = PredictFunUserStream(api_key="test-key", jwt=lambda: next(tokens))

        async def run():
            sent = await watching(stream, stream.watch_orders())
            request_id = sent[0]["requestId"]
            stream._dispatch(json.dumps({"type": "R", "requestId": request_id, "success": False,
                                         "error": {"code": "invalid_credentials"}}))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            return stream._ws.sent

        sent = asyncio.run(run())
        assert [f["params"] for f in sent] == [["predictWalletEvents/old"], ["predictWalletEvents/new"]]


def test_a_source_that_takes_fresh_is_asked_for_a_new_login():
    asked = []

    async def source(*, fresh: bool = False):
        asked.append(fresh)
        return "new" if fresh else "old"

    stream = PredictFunUserStream(api_key="test-key", jwt=source)

    async def run():
        sent = await watching(stream, stream.watch_orders())
        stream._dispatch(json.dumps({"type": "R", "requestId": sent[0]["requestId"], "success": False,
                                     "error": {"code": "invalid_credentials"}}))
        for _ in range(3):
            await asyncio.sleep(0)
        return stream._ws.sent

    sent = asyncio.run(run())
    assert asked == [False, True] and sent[-1]["params"] == ["predictWalletEvents/new"]
