"""Gemini Predictions' WebSocket, against a fake socket. Market frames were
recorded from wss://ws.gemini.com?snapshot=-1 on 2026-10-23
(`tests/samples/gemini_ws.json`, "recorded"); account frames, and the trade
and contract-status frames no busy market printed while recording, are the
venue's documented examples ("documented")."""
from __future__ import annotations

import asyncio
import base64
import copy
import json
from decimal import Decimal

import pytest

import synpath
from synpath.trading.credentials import GeminiCredentials
from synpath.trading.types import Liquidity, OrderStatus, PositionSide, Side
from synpath.ws.base import (
    BalanceEvent, BookEvent, FillEvent, MarketStatusEvent, OrderEvent, PositionEvent, QuoteEvent, StreamStatusEvent,
    TradeEvent,
)
from synpath.ws.gemini import GeminiMarketStream, GeminiUserStream, ns_to_ms

from conftest import load

SAMPLE = load("gemini_ws.json")
REC, DOC = SAMPLE["recorded"], SAMPLE["documented"]
SNAPSHOT, *DELTAS = REC["depth"]
MARKET = "gemini:GEMI-BTC2610232100-HI82000"
UP = "gemini:GEMI-BTC05M2606011000-UP"
CREDS = GeminiCredentials(api_key="account-test0000000000000000", api_secret="synpath-test-secret")


class FakeSocket:
    def __init__(self):
        self.sent: list = []

    async def send(self, text):
        self.sent.append(json.loads(text))


def drain(stream) -> list:
    out = []
    while not stream._queue.empty():
        out.append(stream._queue.get_nowait())
    return out


async def watching(stream, *market_ids):
    stream._ws = FakeSocket()
    await stream.watch_order_book(list(market_ids))
    return stream._ws


class TestMarketStream:
    def test_it_is_exported_and_public(self):
        assert synpath.GeminiMarketStream is GeminiMarketStream
        stream = GeminiMarketStream()
        assert stream.url == "wss://ws.gemini.com?snapshot=-1" and stream.headers() == {}
        assert GeminiMarketStream(sandbox=True).url == "wss://ws.sandbox.gemini.com?snapshot=-1"

    def test_subscribing_names_the_depth_stream(self):
        async def go():
            stream = GeminiMarketStream()
            socket = await watching(stream, MARKET)
            await stream.watch_trades([MARKET])
            await stream.watch_ticker([MARKET])
            await stream.watch_market_status()
            return socket.sent

        sent = asyncio.run(go())
        assert sent[0] == {"id": 1, "method": "SUBSCRIBE", "params": ["GEMI-BTC2610232100-HI82000@depth@100ms"]}
        assert [s["params"] for s in sent[1:]] == [["GEMI-BTC2610232100-HI82000@trade"],
                                                   ["GEMI-BTC2610232100-HI82000@bookTicker"], ["contractStatus"]]

    def test_the_first_frame_is_the_book_then_deltas_apply(self):
        async def go():
            stream = GeminiMarketStream()
            await watching(stream, MARKET)
            events = [e for frame in REC["depth"] for e in stream.handle(copy.deepcopy(frame))]
            return stream, events

        stream, events = asyncio.run(go())
        snapshot, *deltas = events
        assert isinstance(snapshot, BookEvent) and snapshot.kind == "snapshot" and snapshot.market_id == MARKET
        assert snapshot.best_bid == Decimal("0.1600") and snapshot.best_ask == Decimal("0.2100")
        assert snapshot.timestamp == ns_to_ms(SNAPSHOT["E"])
        assert [d.kind for d in deltas] == ["delta"] * 3
        book = stream.book(MARKET)
        # Delta 1 removed 0.15 and 0.21; delta 2 set 0.16 to 350 and 0.22 to 991; delta 3 set 0.16 back to 100.
        assert Decimal("0.1500") not in book.bids and Decimal("0.2100") not in book.asks
        assert book.bids[Decimal("0.1600")] == Decimal("100.00") and book.best_ask == Decimal("0.2200")
        assert stream.book(MARKET, "no").best_bid == Decimal("1") - Decimal("0.2200")

    def test_a_skipped_update_drops_the_book_and_subscribes_again(self):
        async def go():
            stream = GeminiMarketStream()
            socket = await watching(stream, MARKET)
            stream.handle(copy.deepcopy(SNAPSHOT))
            skipped = {**DELTAS[1]}                    # U is past the snapshot's u
            assert stream.handle(skipped) == []
            await asyncio.sleep(0)
            again = stream.handle(copy.deepcopy(SNAPSHOT))
            return stream, socket.sent, again

        stream, sent, again = asyncio.run(go())
        statuses = [e for e in drain(stream) if isinstance(e, StreamStatusEvent)]
        assert [s.state for s in statuses] == ["gap", "resynced"]
        assert sent[-2:] == [
            {"id": 2, "method": "UNSUBSCRIBE", "params": ["GEMI-BTC2610232100-HI82000@depth@100ms"]},
            {"id": 3, "method": "SUBSCRIBE", "params": ["GEMI-BTC2610232100-HI82000@depth@100ms"]},
        ]
        assert again[0].kind == "snapshot" and stream.book(MARKET).ready

    def test_a_frame_already_applied_is_ignored(self):
        async def go():
            stream = GeminiMarketStream()
            await watching(stream, MARKET)
            stream.handle(copy.deepcopy(SNAPSHOT))
            stream.handle(copy.deepcopy(DELTAS[0]))
            return stream.handle(copy.deepcopy(DELTAS[0]))

        assert asyncio.run(go()) == []

    def test_a_reconnect_waits_for_a_new_snapshot(self):
        async def go():
            stream = GeminiMarketStream()
            await watching(stream, MARKET)
            stream.handle(copy.deepcopy(SNAPSHOT))
            stream.on_disconnect()
            await stream.on_connect()
            assert not stream.book(MARKET).ready
            return stream.handle(copy.deepcopy(DELTAS[0]))

        [event] = asyncio.run(go())
        assert event.kind == "snapshot"    # whatever arrives first after a connect is the book

    def test_the_book_ticker_is_a_quote(self):
        [quote] = GeminiMarketStream().handle(REC["book_ticker"])
        assert isinstance(quote, QuoteEvent) and quote.market_id == MARKET
        assert (quote.bid, quote.ask, quote.bid_size, quote.last) == (
            Decimal("0.1600"), Decimal("0.2100"), Decimal("100.00"), Decimal("0.2200"))

    def test_a_print_names_the_taker(self):
        [trade] = GeminiMarketStream().handle(DOC["trade"])
        assert isinstance(trade, TradeEvent) and trade.market_id == UP
        assert trade.taker_side == Side.SELL           # the buyer was the maker
        assert trade.price == Decimal("0.50") and trade.timestamp == 1759873803503

    def test_contract_status(self):
        [event] = GeminiMarketStream().handle(DOC["contract_status"])
        assert isinstance(event, MarketStatusEvent) and event.state == "created" and event.native == "Approved"
        assert event.market_id == "gemini:GEMI-BTC15M2604221545-HI78999D63" and event.timestamp == 1776871540195

    def test_answers_and_refusals(self):
        stream = GeminiMarketStream()
        assert stream.handle(REC["ack"]) == [] and stream.handle(REC["error"]) == []
        states = [e.state for e in drain(stream)]
        assert states == ["subscribed", "error"]


class TestUserStream:
    def test_the_handshake_is_signed(self):
        stream = GeminiUserStream(CREDS)
        headers = stream.headers()
        assert headers["X-GEMINI-APIKEY"] == CREDS.api_key
        assert base64.b64decode(headers["X-GEMINI-PAYLOAD"]).decode() == headers["X-GEMINI-NONCE"]
        assert stream.private and synpath.GeminiUserStream is GeminiUserStream

    def test_subscriptions(self):
        async def go():
            stream = GeminiUserStream(CREDS)
            stream._ws = FakeSocket()
            await stream.watch_orders()
            await stream.watch_my_trades()
            await stream.watch_positions()
            await stream.watch_balance()
            return stream._ws.sent

        assert [s["params"] for s in asyncio.run(go())] == [["orders@account"], ["positions@account"], ["balances@account"]]

    def test_an_order_keeps_its_fields_across_frames(self):
        stream = GeminiUserStream(CREDS)
        [new] = stream.handle(DOC["order_new"])
        [cancelled] = stream.handle(DOC["order_canceled"])
        assert isinstance(new, OrderEvent) and new.order.status == OrderStatus.OPEN and new.order.side == Side.BUY
        assert new.order.price == Decimal("0.48000") and new.order.quantity == Decimal("10")
        assert cancelled.order.status == OrderStatus.CANCELED and cancelled.order.price == Decimal("0.48000")
        assert cancelled.order.client_order_id == "btc-5m-quote-001" and cancelled.order.filled == Decimal("0")

    def test_a_fill_rides_on_the_order_frame(self):
        stream = GeminiUserStream(CREDS)
        stream.handle({**DOC["order_new"], "O": "NO", "p": "0.40", "q": "10", "z": "10"})
        fill_frame = {"e": "orderUpdate", "E": 1759291847800000000, "s": "GEMI-BTC05M2606011000-UP",
                      "i": 73797746498585286, "X": "PARTIALLY_FILLED", "q": "10", "z": "6", "Z": "4", "L": "0.40",
                      "t": 2840140956529700, "n": "0.07", "m": False, "T": 1759291847800000000}
        order_event, fill_event = stream.handle(fill_frame)
        order, fill = order_event.order, fill_event.fill
        assert order.side == Side.SELL and order.price == Decimal("0.60") and order.filled == Decimal("4")
        assert isinstance(fill_event, FillEvent)
        assert (fill.price, fill.quantity, fill.fee, fill.liquidity) == (Decimal("0.60"), Decimal("4"), Decimal("0.07"), Liquidity.TAKER)
        assert fill.id == "2840140956529700" and fill.timestamp == 1759291847800

    def test_positions_and_a_settlement(self):
        stream = GeminiUserStream(CREDS)
        [event] = stream.handle(DOC["positions"])
        assert isinstance(event, PositionEvent) and event.position.side == PositionSide.LONG
        assert event.position.contracts == Decimal("2.5") and event.position.market_id == UP
        settled = {**DOC["positions"], "P": [{"t": "ec", "s": "GEMI-BTC05M2606011000-UP", "a": [
            {"t": "position", "v": "0"}, {"t": "settlement_payout", "v": "2.50", "c": "usd", "o": "YES"}]}]}
        [close] = stream.handle(settled)
        assert close.position.resolved and close.position.payout == Decimal("2.50") and close.position.won
        assert close.position.info["result"] == "yes"

    def test_a_balance(self):
        [event] = GeminiUserStream(CREDS).handle(DOC["balance"])
        assert isinstance(event, BalanceEvent) and event.balance.available == Decimal("207.39")
        assert event.balance.currency == "USD" and event.balance.locked == Decimal("0")

    def test_narrowed_to_some_markets(self):
        async def go():
            stream = GeminiUserStream(CREDS)
            stream._ws = FakeSocket()
            await stream.watch_orders([MARKET])
            return stream

        stream = asyncio.run(go())
        assert stream.handle(DOC["order_new"]) == []


@pytest.mark.parametrize("value, expected", [(1759291847686856569, 1759291847686), (1776871540195, 1776871540195), (None, None)])
def test_timestamps(value, expected):
    assert ns_to_ms(value) == expected
