"""ProphetX's streams: the Pusher wire protocol and ProphetX's registration,
spoken against a fake socket. Frames are the examples in ProphetX's WebSocket
guides (docs.prophetx.co): the connection config, `connection_established`,
the sanitized registration answer, a `trade_execution` and a `health_check`;
`market_selections` carries the documented payload fields with the documented
markets' values."""
from __future__ import annotations

import asyncio
import base64
import json
from decimal import Decimal

import pytest

from synpath import AuthenticationError, ProphetXMarketStream, ProphetXUserStream
from synpath.prophetx import lines_of, markets_of, probability
from synpath.trading.types import OrderStatus, Side
from synpath.ws.base import BookEvent, FillEvent, OrderEvent, QuoteEvent, StreamStatusEvent, TradeEvent
from synpath.ws.prophetx import decode, payload_of, ws_url

from conftest import load

WS = load("prophetx_ws.json")
V3 = load("prophetx_markets_v3.json")
LIONS = "90de769f4f4977a04446ed203e957351"
VIKINGS = "7d75b0c55f5578219f534413d0bd872b"
MARKET = f"prophetx:18925-{LIONS}"


def drain(stream) -> list:
    out = []
    while not stream._queue.empty():
        out.append(stream._queue.get_nowait())
    return out


class FakeSocket:
    def __init__(self):
        self.sent: list = []

    async def send(self, text):
        self.sent.append(json.loads(text))


class Catalog:
    def __init__(self, lines=None):
        self._lines = lines

    def lines(self, event_id):
        if self._lines is not None:
            return self._lines
        return [{**ln, "event_id": event_id} for m in markets_of(V3) for ln in lines_of(m)]

    def fetch_sport_events(self, *, event_ids=None, tournament_id=None):
        return [{"event_id": int(e), "tournament_id": 31} for e in event_ids or []]


def registration(event_id="18925"):
    """The documented registration answer, its event channel on our event."""
    return json.loads(json.dumps(WS["register"]["data"]).replace("10078829", event_id))


def frame(event, channel, payload, change_type="market_selections"):
    data = {"timestamp": 1717689526365275600, "change_type": change_type, "op": "u",
            "payload": base64.b64encode(json.dumps(payload).encode()).decode()}
    return json.dumps({"event": event, "channel": channel, "data": json.dumps(data)})


def selections(sequence, *, lions_price=-470, lions_value=470):
    """A `market_selections` payload for the documented moneyline (market 219)."""
    moneyline = markets_of(V3)[0]
    one, two = moneyline["selections"]
    return {"sport_event_id": 18925, "market_id": 219, "info": {
        "id": 219, "name": "Moneyline", "type": "moneyline", "strike": 0, "sequence_number": sequence,
        "selections": [[{**one, "price": lions_price, "value": lions_value}], [two]]}}


async def settle():
    for _ in range(10):
        await asyncio.sleep(0)


async def handshake(stream):
    stream._ws = FakeSocket()
    stream._dispatch(json.dumps(WS["connection_established"]))
    await settle()
    return stream


EVENT_CHANNEL = "private-broadcast-service=6-device_type=5-event=18925"
PRIVATE_CHANNEL = "private-service=6-device_type=5-user=<your_account_id>"
BROADCAST_CHANNEL = "private-broadcast-service=6-device_type=5"


class TestProtocol:
    def test_the_url_is_the_ws_host_or_the_cluster(self):
        assert ws_url({"key": "k", "ws_host": "talaria.example"}) == "wss://talaria.example/app/k?protocol=7&client=synpath&version=1"
        assert ws_url({"key": "k", "cluster": "us2"}).startswith("wss://ws-us2.pusher.com/app/k?protocol=7")
        with pytest.raises(AuthenticationError):
            ws_url({"cluster": "us2"})

    def test_data_is_json_in_a_string_and_the_payload_base64(self):
        assert decode(WS["connection_established"]["data"]) == {"socket_id": "123.456"}
        assert payload_of(WS["health_check"]["data"]) == {}

    def test_a_stream_needs_the_key_pair(self):
        with pytest.raises(AuthenticationError, match="PROPHETX_ACCESS_KEY"):
            ProphetXMarketStream()


class TestMarketStream:
    def stream(self, registered):
        async def register(body):
            registered.append(body)
            return registration()

        return ProphetXMarketStream(config=WS["connection_config"], register=register, catalog=Catalog())

    def test_the_handshake_registers_signs_in_and_then_subscribes(self):
        registered: list = []

        async def go():
            stream = self.stream(registered)
            await stream.watch_order_book([MARKET])
            await handshake(stream)
            before = list(stream._ws.sent)
            stream._dispatch(json.dumps({"event": "pusher:signin_success", "data": {}}))
            await settle()
            return before, stream._ws.sent

        before, sent = asyncio.run(go())
        assert registered == [{"socket_id": "123.456", "service": "pusher",
                               "subscriptions": [{"type": "event", "ids": ["18925"]}]}]
        assert [f["event"] for f in before] == ["pusher:signin"]
        assert before[0]["data"]["auth"] == "<app_key>:<hmac_signature_5>"
        [subscribe] = [f for f in sent if f["event"] == "pusher:subscribe"]
        assert subscribe["data"] == {"auth": "<app_key>:<hmac_signature_3>", "channel": EVENT_CHANNEL}

    def test_a_joined_event_seeds_its_books_and_updates_replace_them(self):
        registered: list = []

        async def go():
            stream = self.stream(registered)
            await stream.watch_order_book([MARKET])
            await stream.watch_ticker([MARKET])
            await handshake(stream)
            stream._dispatch(json.dumps({"event": "pusher:signin_success", "data": {}}))
            await settle()
            stream._dispatch(json.dumps({"event": "pusher_internal:subscription_succeeded", "data": "{}",
                                         "channel": EVENT_CHANNEL}))
            await settle()
            seeded = drain(stream)
            stream._dispatch(frame("v2.market_selections", EVENT_CHANNEL, selections(5, lions_price=-450, lions_value=90)))
            stream._dispatch(frame("market_selections", EVENT_CHANNEL, selections(6)))
            stream._dispatch(frame("v2.market_selections", EVENT_CHANNEL, selections(4)))
            return seeded, drain(stream)

        seeded, updates = asyncio.run(go())
        books = [e for e in seeded if isinstance(e, BookEvent)]
        assert books and books[0].market_id == MARKET and books[0].best_ask == Decimal(str(probability(-470)))
        [book, quote] = updates
        assert isinstance(book, BookEvent) and book.kind == "snapshot" and book.sequence == 5
        assert book.best_ask == Decimal(str(probability(-450)))
        assert book.asks[0].size == Decimal(str(round(90 / probability(-450), 6)))
        assert book.best_bid == Decimal(str(round(1 - probability(-136), 8)))
        assert isinstance(quote, QuoteEvent) and quote.ask == book.best_ask

    def test_trades_come_from_the_tournament_tape(self):
        registered: list = []
        tape = {**WS["trade_execution"], "info": {**WS["trade_execution"]["info"], "strike_id": VIKINGS,
                                                  "sport_event_id": 18925}}

        async def go():
            stream = self.stream(registered)
            await stream.watch_trades([MARKET])
            await handshake(stream)
            stream._dispatch(frame("v2.tournament_31", BROADCAST_CHANNEL, tape, change_type="trade_execution"))
            stream._dispatch(frame("v2.tournament_31", BROADCAST_CHANNEL, WS["trade_execution"],
                                   change_type="trade_execution"))
            return drain(stream)

        events = asyncio.run(go())
        assert registered[0]["subscriptions"] == [{"type": "event", "ids": ["18925"]},
                                                  {"type": "tournament", "ids": ["31"]}]
        [trade] = [e for e in events if isinstance(e, TradeEvent)]
        # 45.30 staked on the Vikings at +124 returns 45.30 * 224/100 contracts; a NO buy is a YES sell.
        assert trade.market_id == MARKET and trade.taker_side == Side.SELL
        assert trade.price == 1 - Decimal(100) / Decimal(224) and trade.quantity == Decimal("101.472000")

    def test_ping_is_answered_and_a_disconnect_invalidates_books(self):
        registered: list = []

        async def go():
            stream = self.stream(registered)
            await stream.watch_order_book([MARKET])
            stream._ws = FakeSocket()
            stream._dispatch(json.dumps({"event": "pusher:ping", "data": {}}))
            await settle()
            stream.books["18925-" + LIONS].replace([], [(Decimal("0.5"), Decimal("1"))])
            stream.on_disconnect()
            return stream

        stream = asyncio.run(go())
        assert stream._ws.sent == [{"event": "pusher:pong", "data": {}}]
        assert not stream.books["18925-" + LIONS].ready and stream.socket_id is None and not stream.live


class TestUserStream:
    def test_orders_and_fills_arrive_on_the_private_channel(self):
        registered: list = []

        async def register(body):
            registered.append(body)
            return registration()

        async def resolver(strike_id, *, event_id=None):
            return (MARKET, strike_id == LIONS)

        row = {"id": "partner_1", "external_id": "c-1", "strike_id": VIKINGS, "sport_event_id": 18925, "market_id": 219,
               "price": 124, "quantity": 10.0, "filled_quantity": 4.0, "fill_price": 124, "total_filled_quantity": 4.0,
               "open_quantity": 6.0, "matching_status": "partially_matched", "status": "open", "update_type": "matching",
               "matching_id": 991, "aggressive": False, "sequence_number": 3}

        async def go():
            stream = ProphetXUserStream(config=WS["connection_config"], register=register, resolver=resolver)
            await stream.watch_orders()
            await stream.watch_my_trades()
            await handshake(stream)
            stream._dispatch(json.dumps({"event": "pusher:signin_success", "data": {}}))
            await settle()
            stream._dispatch(frame("orders", PRIVATE_CHANNEL, {"info": row}, change_type="orders"))
            stream._dispatch(frame("health_check", PRIVATE_CHANNEL, {}, change_type="private_system_signal"))
            await settle()
            return stream._ws.sent, drain(stream)

        sent, events = asyncio.run(go())
        assert [f["data"]["channel"] for f in sent if f["event"] == "pusher:subscribe"] == [PRIVATE_CHANNEL]
        order, fill = [e for e in events if not isinstance(e, StreamStatusEvent)]
        assert isinstance(order, OrderEvent) and order.order.status == OrderStatus.OPEN and order.order.side == Side.SELL
        assert order.order.market_id == MARKET and order.native == "matching:open"
        assert isinstance(fill, FillEvent) and fill.fill.id == "991" and fill.fill.quantity == Decimal("8.96")
