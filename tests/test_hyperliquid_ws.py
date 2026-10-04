"""Hyperliquid's streams and record mapping, against messages and records
recorded from the live venue."""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest

from synpath import BadRequest, HyperliquidMarketStream, HyperliquidUserStream
from synpath.trading.hyperliquid import fill_of, order_of, outcome_coin, status_of
from synpath.trading.types import Liquidity, OrderStatus, OrderType, Side, TimeInForce
from synpath.ws.base import (
    BookEvent, FillEvent, OrderEvent, QuoteEvent, StreamStatusEvent, TradeEvent, VenueEvent,
)

from conftest import load

WS = load("hyperliquid_ws.json")
FILLS = load("hyperliquid_fills.json")
ORDERS = load("hyperliquid_orders.json")
MARKET = "hyperliquid:7544"


def drain(stream) -> list:
    events = []
    while not stream._queue.empty():
        events.append(stream._queue.get_nowait())
    return events


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


class TestRecords:
    def test_coins(self):
        assert outcome_coin("#75440") == ("7544", "yes")
        assert outcome_coin("#75441") == ("7544", "no")
        assert outcome_coin("BTC") is None and outcome_coin("@107") is None

    def test_buying_yes_is_a_buy_and_buying_no_a_sell(self):
        yes, no = fill_of(FILLS[0]), fill_of(FILLS[1])
        assert (yes.market_id, yes.side, yes.price) == (MARKET, Side.BUY, Decimal(FILLS[0]["px"]))
        assert (no.market_id, no.side) == (MARKET, Side.SELL)
        assert no.price == 1 - Decimal(FILLS[1]["px"])

    def test_selling_no_is_a_buy_on_the_yes_leg(self):
        fill = fill_of(FILLS[3])
        assert fill.side == Side.BUY and fill.price == 1 - Decimal(FILLS[3]["px"])
        assert fill.liquidity == Liquidity.MAKER and fill.fee == Decimal(FILLS[3]["fee"])
        assert fill.fee_currency == "USDC" and fill.order_id == str(FILLS[3]["oid"])

    def test_fees_are_charged_on_closing_only(self):
        """Recorded: opening fills paid nothing; the closing ones paid the
        schedule's rate (a maker close on a protocol market at 4 bps)."""
        assert fill_of(FILLS[0]).fee == 0 and fill_of(FILLS[1]).fee == 0
        close = FILLS[3]
        assert Decimal(close["fee"]) / (Decimal(close["px"]) * Decimal(close["sz"])) == Decimal("0.0004")

    @pytest.mark.parametrize("index", [4, 5, 6])
    def test_splits_merges_and_settlements_are_not_trades(self, index):
        assert fill_of(FILLS[index]) is None

    def test_order_states(self):
        open_, filled, canceled, rejected, no_side = (order_of(row) for row in ORDERS)
        assert open_.status == OrderStatus.OPEN and open_.remaining == open_.amount and open_.filled == 0
        assert filled.status == OrderStatus.CLOSED and filled.filled == filled.amount and filled.remaining == 0
        assert canceled.status == OrderStatus.CANCELED and canceled.remaining == 0
        assert rejected.status == OrderStatus.REJECTED
        assert rejected.info["native_status"] == "insufficientSpotBalanceRejected"
        assert no_side.side == Side.BUY  # selling NO
        assert no_side.price == 1 - Decimal(ORDERS[4]["order"]["limitPx"])
        assert open_.type == OrderType.LIMIT and open_.time_in_force == TimeInForce.GTC

    def test_status_families(self):
        assert status_of("reduceOnlyCanceled") == OrderStatus.CANCELED
        assert status_of("tickRejected") == OrderStatus.REJECTED
        assert status_of("triggered") == OrderStatus.OPEN

    def test_post_only_is_alo(self):
        row = {**ORDERS[0], "order": {**ORDERS[0]["order"], "tif": "Alo"}}
        assert order_of(row).post_only is True

    def test_a_perp_order_is_not_an_outcome_order(self):
        assert order_of({"order": {"coin": "BTC", "side": "B", "limitPx": "1", "sz": "1", "oid": 1}}) is None


class TestMarketStream:
    def test_subscriptions_use_the_yes_coin_and_are_sent_once(self):
        stream = HyperliquidMarketStream()
        sent = asyncio.run(watching(
            stream, stream.watch_order_book([MARKET, "7544"]), stream.watch_trades([MARKET]), stream.watch_ticker([MARKET]),
        ))
        kinds = [(frame["subscription"]["type"], frame["subscription"]["coin"]) for frame in sent]
        assert kinds == [("l2Book", "#75440"), ("trades", "#75440"), ("bbo", "#75440"), ("activeAssetCtx", "#75440")]

    def test_a_question_id_is_refused(self):
        stream = HyperliquidMarketStream()
        with pytest.raises(BadRequest):
            asyncio.run(watching(stream, stream.watch_order_book(["hyperliquid:q198"])))

    def test_every_book_message_is_a_snapshot(self):
        stream = HyperliquidMarketStream()
        asyncio.run(watching(stream, stream.watch_order_book([MARKET])))
        stream._dispatch(json.dumps(WS["l2Book"]))
        stream._dispatch(json.dumps(WS["l2Book"]))
        events = [e for e in drain(stream) if isinstance(e, BookEvent)]
        assert [e.kind for e in events] == ["snapshot", "snapshot"]
        book = stream.book(MARKET)
        assert book.ready and book.best_bid == Decimal(WS["l2Book"]["data"]["levels"][0][0]["px"])
        assert stream.book(MARKET, "no").best_ask == 1 - book.best_bid

    def test_the_book_object_stays_the_same(self):
        """The engine is handed the stream's book once and reads it in place."""
        stream = HyperliquidMarketStream()
        asyncio.run(watching(stream, stream.watch_order_book([MARKET])))
        before = stream.book(MARKET)
        stream._dispatch(json.dumps(WS["l2Book"]))
        assert stream.book(MARKET) is before

    def test_a_reconnect_marks_books_not_ready_until_the_next_message(self):
        stream = HyperliquidMarketStream()
        asyncio.run(watching(stream, stream.watch_order_book([MARKET])))
        stream._dispatch(json.dumps(WS["l2Book"]))
        stream.on_disconnect()
        assert not stream.book(MARKET).ready
        drain(stream)
        stream._dispatch(json.dumps(WS["l2Book"]))
        states = [e.state for e in drain(stream) if isinstance(e, StreamStatusEvent)]
        assert stream.book(MARKET).ready and states == ["resynced"]

    def test_prints_from_before_the_subscription_are_dropped(self):
        stream = HyperliquidMarketStream()
        asyncio.run(watching(stream, stream.watch_trades([MARKET])))
        stream._dispatch(json.dumps(WS["trades"]))
        assert not [e for e in drain(stream) if isinstance(e, TradeEvent)]
        stream.trades_since["#75440"] = 0
        stream._dispatch(json.dumps(WS["trades"]))
        trades = [e for e in drain(stream) if isinstance(e, TradeEvent)]
        assert len(trades) == len(WS["trades"]["data"])
        row = WS["trades"]["data"][0]
        assert trades[0].taker_side == (Side.BUY if row["side"] == "B" else Side.SELL)
        assert trades[0].price == Decimal(row["px"]) and trades[0].market_id == MARKET

    def test_quotes_and_volume(self):
        stream = HyperliquidMarketStream()
        asyncio.run(watching(stream, stream.watch_ticker([MARKET])))
        stream._dispatch(json.dumps(WS["bbo"]))
        stream._dispatch(json.dumps(WS["activeSpotAssetCtx"]))
        bbo, ctx = [e for e in drain(stream) if isinstance(e, QuoteEvent)]
        assert bbo.bid == Decimal(WS["bbo"]["data"]["bbo"][0]["px"])
        assert bbo.ask == Decimal(WS["bbo"]["data"]["bbo"][1]["px"])
        assert ctx.volume == Decimal(WS["activeSpotAssetCtx"]["data"]["ctx"]["dayBaseVlm"])

    def test_acknowledgements_and_pongs(self):
        stream = HyperliquidMarketStream()
        stream._dispatch(json.dumps(WS["subscriptionResponse"]))
        stream._dispatch(json.dumps(WS["pong"]))
        stream._dispatch(json.dumps({"channel": "error", "data": "Invalid subscription"}))
        states = [e.state for e in drain(stream) if isinstance(e, StreamStatusEvent)]
        assert states == ["subscribed", "error"]

    def test_heartbeat(self):
        stream = HyperliquidMarketStream()
        assert stream.app_ping == '{"method":"ping"}' and stream.data_heartbeat


class TestUserStream:
    def user(self, **kwargs) -> HyperliquidUserStream:
        return HyperliquidUserStream(address="0xB13cec4F6B61E0E5e46eEC8AAb608e74b276a665", **kwargs)

    def test_an_address_is_required(self, monkeypatch):
        monkeypatch.delenv("HYPERLIQUID_ACCOUNT_ADDRESS", raising=False)
        with pytest.raises(BadRequest):
            HyperliquidUserStream()

    def test_subscriptions_name_the_address(self):
        stream = self.user()
        sent = asyncio.run(watching(stream, stream.watch_orders(), stream.watch_my_trades()))
        assert [frame["subscription"] for frame in sent] == [
            {"type": "orderUpdates", "user": "0xb13cec4f6b61e0e5e46eec8aab608e74b276a665"},
            {"type": "userFills", "user": "0xb13cec4f6b61e0e5e46eec8aab608e74b276a665"},
        ]
        assert stream.private

    def test_the_fill_snapshot_is_reported_not_replayed(self):
        stream = self.user()
        stream._dispatch(json.dumps(WS["userFills_snapshot"]))
        events = drain(stream)
        assert not [e for e in events if isinstance(e, FillEvent)]
        assert [e.state for e in events if isinstance(e, StreamStatusEvent)] == ["subscribed"]

    def test_new_fills_and_transfers(self):
        stream = self.user()
        stream._dispatch(json.dumps({"channel": "userFills", "data": {"user": stream.address, "fills": FILLS}}))
        events = drain(stream)
        fills = [e for e in events if isinstance(e, FillEvent)]
        transfers = [e for e in events if isinstance(e, VenueEvent)]
        assert len(fills) == 4
        assert [e.name for e in transfers] == ["Merge Outcome", "Split Outcome", "Settlement"]

    def test_order_updates(self):
        stream = self.user()
        stream._dispatch(json.dumps({"channel": "orderUpdates", "data": ORDERS}))
        orders = [e for e in drain(stream) if isinstance(e, OrderEvent)]
        assert [e.native for e in orders] == [row["status"] for row in ORDERS]

    def test_a_market_filter_narrows(self):
        stream = self.user()
        asyncio.run(watching(stream, stream.watch_orders(["hyperliquid:3600"])))
        stream._dispatch(json.dumps({"channel": "orderUpdates", "data": ORDERS}))
        assert not [e for e in drain(stream) if isinstance(e, OrderEvent)]


class TestEngineFeeds:
    def test_books_need_no_credentials_and_the_account_needs_an_address(self):
        from types import SimpleNamespace

        from synpath.engine.feeds import default_streams

        public = default_streams({"hyperliquid": object()}, {})["hyperliquid"]
        assert isinstance(public.market, HyperliquidMarketStream) and public.private is None
        creds = SimpleNamespace(address="0xb13cec4f6b61e0e5e46eec8aab608e74b276a665", testnet=True)
        both = default_streams({"hyperliquid": object()}, {"hyperliquid": creds})["hyperliquid"]
        assert isinstance(both.private, HyperliquidUserStream)
        assert "testnet" in both.market.url and "testnet" in both.private.url
