"""predict.fun's WebSocket: books, prices and market state, and a wallet's orders and fills.

One endpoint, `wss://ws.predict.fun/ws`, authenticated by the API key in the
handshake (`x-api-key`). Subscriptions are one topic per request
(`predictOrderbook/123`), acknowledged by a response carrying the request id.

**Heartbeat.** The server sends `{"topic": "heartbeat", "data": <ms>}` every
15 seconds and closes the connection unless that exact value is echoed back
(`{"method": "heartbeat", "data": <ms>}`). The stream echoes each one.

**Books.** `predictOrderbook` sends the whole book, in YES prices, on every
change, and a snapshot on subscribing; a missed message is repaired by the
next. The NO view is the mirror.

**Prices, not prints.** The venue has no public trades channel. Each book
carries the last order that settled, which becomes `QuoteEvent.last` when it
changes; the tape itself is read over REST (`PredictFun.fetch_trades`).

**Account.** `predictWalletEvents/{jwt}` carries the wallet's order and
settlement events and needs a wallet JWT (from signing the venue's auth
message; see `synpath.trading.predict_fun`). Fills arrive as matched and again
as confirmed or failed. Nothing is replayed after a reconnect.
"""
from __future__ import annotations

import inspect
from decimal import Decimal
from typing import Any, Awaitable, Callable

from .. import ids
from ..base import Capability
from ..errors import AuthenticationError, BadRequest
from ..predict_fun import api_key_from
from ..trading.predict_fun import fill_of_event, order_of_event
from ..trading.types import Account
from .base import (
    BookEvent, Event, FillEvent, LocalBook, MarketStatusEvent, OrderEvent, QuoteEvent, Stream, VenueEvent, maybe_D,
)

VENUE = "predict_fun"
WS_URL = "wss://ws.predict.fun/ws"
"""Mainnet. The venue documents no test-network WebSocket; pass `url=` for one."""

MARKET_STATE = {
    "REGISTERED": "open", "OPEN": "open", "UNPAUSED": "open", "PAUSED": "paused",
    "PRICE_PROPOSED": "determined", "PRICE_DISPUTED": "updated", "RESOLVED": "settled", "REMOVED": "closed",
}
TRADING_STATE = {"OPEN": "open", "MATCHING_NOT_ENABLED": "paused", "CANCEL_ONLY": "paused", "CLOSED": "closed"}


def native_of(market_id: str) -> str:
    native = ids.native(VENUE, market_id)
    if not native.isdigit():
        raise BadRequest(f"{VENUE}: {native!r} is not a market id")
    return native


class _PredictFunStream(Stream):
    """The connection, key, heartbeat and subscription bookkeeping both streams share."""

    venue = VENUE
    data_heartbeat = True

    def __init__(self, *, api_key: str | None = None, url: str | None = None, **kwargs: Any):
        key = api_key_from(api_key)
        if not key and url is None:
            raise AuthenticationError(
                f"{VENUE}: the WebSocket needs an API key -- pass api_key= or set PREDICT_FUN_API_KEY "
                f"(create one at https://developers.predict.fun)"
            )
        super().__init__(url or WS_URL, **kwargs)
        self._key = key
        self.topics: dict[str, int] = {}
        """Topic -> the request id it was last subscribed with; sent again on every connect."""
        self._next_id = 1

    def headers(self) -> dict[str, str]:
        return {"x-api-key": self._key} if self._key else {}

    def status(self, state: Any, detail: str = "", **kwargs: Any) -> None:
        # A wallet JWT travels in a topic string, so it is kept out of anything reported.
        jwt = getattr(self, "_jwt", None)
        super().status(state, detail.replace(jwt, "***") if jwt and detail else detail, **kwargs)

    async def _subscribe(self, topic: str) -> None:
        if topic in self.topics:
            return
        request_id = self._next_id
        self._next_id += 1
        self.topics[topic] = request_id
        await self.send({"method": "subscribe", "requestId": request_id, "params": [topic]})

    async def _unsubscribe(self, topic: str) -> None:
        if self.topics.pop(topic, None) is not None:
            request_id = self._next_id
            self._next_id += 1
            await self.send({"method": "unsubscribe", "requestId": request_id, "params": [topic]})

    async def on_connect(self) -> None:
        for topic in list(self.topics):
            request_id = self._next_id
            self._next_id += 1
            self.topics[topic] = request_id
            await self.send({"method": "subscribe", "requestId": request_id, "params": [topic]})

    def _envelope(self, message: Any) -> tuple[str, Any] | None:
        """`(topic, data)` for a pushed message; answers heartbeats and
        reports refusals. `None` for anything that is not data."""
        if not isinstance(message, dict):
            return None
        if message.get("type") == "R":
            if message.get("success") is False:
                error = message.get("error") or {}
                topic = next((t for t, rid in self.topics.items() if rid == message.get("requestId")), "")
                self._refused(topic, str(error.get("code") or ""), str(error.get("message") or ""))
            else:
                topic = next((t for t, rid in self.topics.items() if rid == message.get("requestId")), None)
                if topic:
                    self.status("subscribed", topic.split("/")[0], key=topic.split("/")[-1] if "Wallet" not in topic else None)
            return None
        topic = str(message.get("topic") or "")
        if topic == "heartbeat":
            self._later(self.send({"method": "heartbeat", "data": message.get("data")}))
            return None
        return topic, message.get("data")

    def _refused(self, topic: str, code: str, detail: str) -> None:
        self.status("error", f"venue refused {topic.split('/')[0] or 'a request'}: {code} {detail}".strip())

    def _later(self, coroutine: Any) -> None:
        import asyncio

        asyncio.get_running_loop().create_task(coroutine)


class PredictFunMarketStream(_PredictFunStream):
    """Books, last prices and market state for predict.fun markets. Needs an API key.

    ```python
    async with PredictFunMarketStream() as stream:
        await stream.watch_order_book(["predict_fun:2880279"])
        async for event in stream:
            ...
    ```
    """

    name = "market"
    has: dict[str, Capability] = {
        "watch_order_book": True,
        # Top of book from each book, and the last settled price when it changes.
        "watch_ticker": "partial",
        # No public trades channel; the tape is over REST.
        "watch_trades": False,
        "watch_market_status": True,
    }

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self.books: dict[str, LocalBook] = {}
        """Native market id -> its YES book."""
        self.tickers: set[str] = set()
        self._last: dict[str, str] = {}
        """Native market id -> the id of the last settled order seen."""

    async def watch_order_book(self, market_ids: list[str]) -> None:
        """Each market's whole book, on every change."""
        for market_id in market_ids:
            native = native_of(market_id)
            self.books.setdefault(native, LocalBook())
            await self._subscribe(f"predictOrderbook/{native}")

    async def watch_ticker(self, market_ids: list[str]) -> None:
        """Top of book, and the last settled price as it changes."""
        for market_id in market_ids:
            native = native_of(market_id)
            self.tickers.add(native)
            await self._subscribe(f"predictOrderbook/{native}")

    async def watch_market_status(self, market_ids: list[str]) -> None:
        """Lifecycle (open, paused, decided, settled) and whether matching is on."""
        for market_id in market_ids:
            native = native_of(market_id)
            await self._subscribe(f"predictMarketStatus/{native}")
            await self._subscribe(f"predictTradingStatus/{native}")

    async def unwatch(self, market_ids: list[str]) -> None:
        for market_id in market_ids:
            native = native_of(market_id)
            for kind in ("predictOrderbook", "predictMarketStatus", "predictTradingStatus"):
                await self._unsubscribe(f"{kind}/{native}")
            self.books.pop(native, None)
            self.tickers.discard(native)

    def book(self, market_id: str, side: str = "yes") -> LocalBook | None:
        book = self.books.get(native_of(market_id))
        if book is None:
            return None
        return book if side != "no" else book.mirrored()

    def on_disconnect(self) -> None:
        for book in self.books.values():
            book.invalidate()

    def handle(self, message: Any) -> list[Event]:
        envelope = self._envelope(message)
        if envelope is None:
            return []
        topic, data = envelope
        kind, _, native = topic.partition("/")
        if not isinstance(data, dict):
            return []
        if kind == "predictOrderbook":
            return self._on_book(native, data)
        if kind == "predictMarketStatus":
            return [self._state(native, MARKET_STATE.get(str(data.get("status")), "updated"), data, "status")]
        if kind == "predictTradingStatus":
            return [self._state(native, TRADING_STATE.get(str(data.get("tradingStatus")), "updated"), data, "tradingStatus")]
        return [VenueEvent(venue=VENUE, name=kind, payload=data)]

    def _on_book(self, native: str, data: dict[str, Any]) -> list[Event]:
        market_id = ids.qualify(VENUE, native)
        events: list[Event] = []
        book = self.books.get(native)
        if book is not None:
            recovering = not book.ready and book.timestamp is not None
            book.replace(_levels(data.get("bids")), _levels(data.get("asks")))
            book.timestamp = int(data["updateTimestampMs"]) if data.get("updateTimestampMs") else None
            if recovering:
                self.status("resynced", f"book {market_id}", key=market_id)
            bids, asks = book.levels()
            events.append(BookEvent(
                venue=VENUE, market_id=market_id, side="yes", kind="snapshot", bids=bids, asks=asks,
                best_bid=book.best_bid, best_ask=book.best_ask, timestamp=book.timestamp,
                info={"orderCount": data.get("orderCount"), "settlementsPending": data.get("settlementsPending")},
            ))
        if native in self.tickers:
            bids, asks = _levels(data.get("bids")), _levels(data.get("asks"))
            best_bid = max(bids, default=None, key=lambda level: level[0])
            best_ask = min(asks, default=None, key=lambda level: level[0])
            settled = data.get("lastOrderSettled") or {}
            last = None
            if settled.get("id") and settled.get("id") != self._last.get(native):
                self._last[native] = str(settled["id"])
                price = maybe_D(settled.get("price"))
                if price is not None:
                    last = price if str(settled.get("outcome") or "Yes").lower() == "yes" else Decimal("1") - price
            events.append(QuoteEvent(
                venue=VENUE, market_id=market_id,
                bid=best_bid[0] if best_bid else None, bid_size=best_bid[1] if best_bid else None,
                ask=best_ask[0] if best_ask else None, ask_size=best_ask[1] if best_ask else None,
                last=last, timestamp=int(data["updateTimestampMs"]) if data.get("updateTimestampMs") else None,
                info={"lastOrderSettled": settled or None},
            ))
        return events

    def _state(self, native: str, state: str, data: dict[str, Any], field: str) -> MarketStatusEvent:
        return MarketStatusEvent(
            venue=VENUE, market_id=ids.qualify(VENUE, native), state=state, native=str(data.get(field) or ""),  # type: ignore[arg-type]
            timestamp=int(data["tsMs"]) if data.get("tsMs") else None, info=data,
        )


JwtSource = Callable[[], "str | Awaitable[str]"]


class PredictFunUserStream(_PredictFunStream):
    """A wallet's orders and fills. Needs an API key and a wallet JWT: pass
    `jwt=` (a string, or a function returning one, called again when the
    venue says the JWT has expired). `PredictFunTrading.jwt` is such a function."""

    name = "user"
    private = True
    has: dict[str, Capability] = {
        "watch_orders": True,
        "watch_my_trades": True,
        "watch_positions": False,
        "watch_balance": False,
    }

    def __init__(self, *, jwt: str | JwtSource, account_name: str = "default", **kwargs: Any):
        super().__init__(**kwargs)
        self._jwt_source = jwt
        self._jwt: str | None = jwt if isinstance(jwt, str) else None
        self.account = Account(venue=VENUE, name=account_name)
        self.watching = False

    async def _token(self, *, fresh: bool = False) -> str:
        if isinstance(self._jwt_source, str):
            return self._jwt_source
        if fresh or self._jwt is None:
            value = self._jwt_source(fresh=True) if fresh and _takes_fresh(self._jwt_source) else self._jwt_source()
            self._jwt = await value if inspect.isawaitable(value) else value  # type: ignore[assignment]
        return self._jwt  # type: ignore[return-value]

    async def watch_orders(self, market_ids: list[str] | None = None) -> None:
        """Every order and settlement event for the wallet; the venue has no per-market filter."""
        self.watching = True
        await self._subscribe(f"predictWalletEvents/{await self._token()}")

    watch_my_trades = watch_orders

    def _refused(self, topic: str, code: str, detail: str) -> None:
        if code == "invalid_credentials" and topic.startswith("predictWalletEvents/") and not isinstance(self._jwt_source, str):
            self.topics.pop(topic, None)
            self._later(self._renew())
            return
        if code == "invalid_credentials":
            self.status("failed", "the wallet JWT was refused (expired or revoked); pass a fresh one")
            self._later(self.close())
            return
        super()._refused(topic, code, detail)

    async def _renew(self) -> None:
        await self._subscribe(f"predictWalletEvents/{await self._token(fresh=True)}")

    def handle(self, message: Any) -> list[Event]:
        envelope = self._envelope(message)
        if envelope is None:
            return []
        topic, data = envelope
        if not topic.startswith("predictWalletEvents/") or not isinstance(data, dict):
            return []
        events: list[Event] = [OrderEvent(venue=VENUE, order=order_of_event(data, account=self.account), native=str(data.get("type") or ""))]
        fill = fill_of_event(data, account=self.account)
        if fill is not None:
            events.append(FillEvent(venue=VENUE, fill=fill))
        return events


def _levels(rows: Any) -> list[tuple[Decimal, Decimal]]:
    out = []
    for row in rows or []:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            price, size = maybe_D(row[0]), maybe_D(row[1])
            if price is not None and size is not None and size > 0:
                out.append((price, size))
    return out


def _takes_fresh(source: Any) -> bool:
    """Whether a token source takes `fresh=` (as `PredictFunTrading.jwt`
    does), to skip a token it still holds and log in again."""
    try:
        return "fresh" in inspect.signature(source).parameters
    except (TypeError, ValueError):
        return False
