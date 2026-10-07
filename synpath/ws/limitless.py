"""Limitless's WebSocket: books and market resolutions, and an account's orders and fills.

One endpoint, `wss://ws.limitless.exchange`, speaking Socket.IO (Engine.IO
v4) on the `/markets` namespace. This stream speaks it directly, frame by
frame, so no Socket.IO client is needed: `0` opens the session, `40/markets,`
joins the namespace, the server's `2` pings are answered `3`, and every event
is `42/markets,["name", data]`.

**Books.** `subscribe_market_prices` with the markets' slugs; each
`orderbookUpdate` is the whole YES book, coalesced, with a snapshot on every
subscribe. A missed frame is repaired by the next. A subscription *replaces*
the last one, so every call sends the whole set. NO is the mirror.

**No public prints.** The venue streams no trades; the tape is over REST
(`Limitless.fetch_trades`). A market's resolution arrives on the same
subscription as its book.

**Account.** `subscribe_order_events` needs a scoped API token: the handshake
carries an HMAC signature of a fixed message, made fresh for every connection
(the venue closes every connection after 24 hours). Order-engine frames name a
token, not a market; the stream places them from the markets it was asked to
watch, then from one walk of the open catalog.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from .. import ids
from ..base import Capability
from ..errors import AuthenticationError
from ..trading.limitless import fill_of_event, order_of_event
from ..trading.types import Account
from .base import BookEvent, Event, FillEvent, LocalBook, MarketStatusEvent, OrderEvent, QuoteEvent, Stream

VENUE = "limitless"
NAMESPACE = "/markets"
WS_URL = "wss://ws.limitless.exchange/socket.io/?EIO=4&transport=websocket"
HANDSHAKE_PATH = "/socket.io/?EIO=4&transport=websocket"
SCALE = Decimal(1_000_000)


def slug_of(market_id: str) -> str:
    return ids.native(VENUE, market_id)


def _levels(rows: Any) -> list[tuple[Decimal, Decimal]]:
    out = []
    for row in rows or []:
        if not isinstance(row, dict) or row.get("price") in (None, "") or row.get("size") in (None, ""):
            continue
        size = Decimal(str(row["size"])) / SCALE
        if size > 0:
            out.append((Decimal(str(row["price"])), size))
    return out


def auth_headers(token_id: str, secret: str, *, now: datetime | None = None) -> dict[str, str]:
    """The `lmts-*` handshake headers: HMAC-SHA256 of
    `{ISO timestamp}\\nGET\\n/socket.io/?EIO=4&transport=websocket\\n` under the
    base64-decoded secret."""
    stamp = (now or datetime.now(timezone.utc)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    message = f"{stamp}\nGET\n{HANDSHAKE_PATH}\n"
    signature = base64.b64encode(hmac.new(base64.b64decode(secret), message.encode(), hashlib.sha256).digest()).decode()
    return {"lmts-api-key": token_id, "lmts-timestamp": stamp, "lmts-signature": signature}


class _LimitlessStream(Stream):
    """The Socket.IO framing, namespace join and heartbeat both streams share."""

    venue = VENUE
    data_heartbeat = True

    def __init__(self, *, url: str = WS_URL, **kwargs: Any):
        super().__init__(url, **kwargs)
        self.joined = False

    # -- Socket.IO framing --------------------------------------------------

    def handle_raw(self, raw: str) -> list[Event] | None:
        if raw.startswith("0"):
            self._later(self.send(f"40{NAMESPACE},"))
            return []
        if raw == "2":
            self._later(self.send("3"))
            return []
        if raw.startswith("3") or raw == "6":
            return []
        prefix = f"4{{}}{NAMESPACE}"
        if raw.startswith(prefix.format("0")):
            self.joined = True
            self.status("subscribed", NAMESPACE)
            self._later(self.subscribe())
            return []
        if raw.startswith(prefix.format("4")):
            detail = raw[len(prefix.format("4")) + 1:]
            self.status("error", f"venue refused the connection: {detail}")
            return []
        if raw.startswith(prefix.format("1")):
            self.joined = False
            return []
        if raw.startswith(prefix.format("2")):
            body = raw[len(prefix.format("2")) + 1:].lstrip("0123456789")
            payload = json.loads(body)
            if not isinstance(payload, list) or not payload:
                return []
            return self.on_event(str(payload[0]), payload[1] if len(payload) > 1 else None)
        return []

    async def emit_event(self, name: str, data: Any = None) -> bool:
        """Send one Socket.IO event, once the namespace is joined."""
        if not self.joined:
            return False
        args = [name] if data is None else [name, data]
        return await self.send(f"42{NAMESPACE}," + json.dumps(args))

    def on_disconnect(self) -> None:
        self.joined = False

    async def subscribe(self) -> None:
        """Send the subscriptions this stream holds; called on joining."""

    def on_event(self, name: str, data: Any) -> list[Event]:
        if name in ("exception", "error"):
            detail = data.get("message") if isinstance(data, dict) else data
            self.status("error", f"venue error: {detail}")
        return []

    def _later(self, coroutine: Any) -> None:
        try:
            asyncio.get_running_loop().create_task(coroutine)
        except RuntimeError:
            coroutine.close()


class LimitlessMarketStream(_LimitlessStream):
    """Books and resolutions for Limitless markets. No key needed.

    ```python
    async with LimitlessMarketStream() as stream:
        await stream.watch_order_book(["limitless:btc-up-or-down-daily-p-1791308105925"])
        async for event in stream:
            ...
    ```
    """

    name = "market"
    has: dict[str, Capability] = {
        "watch_order_book": True,
        # Top of book from each book; no last price.
        "watch_ticker": "partial",
        # No public trades channel; the tape is over REST.
        "watch_trades": False,
        # Resolutions only, on the markets whose books are watched.
        "watch_market_status": "partial",
    }

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self.books: dict[str, LocalBook] = {}
        """Slug -> its YES book."""
        self.tickers: set[str] = set()
        self.statuses: set[str] = set()
        self._versions: dict[str, int] = {}

    def watched(self) -> list[str]:
        return sorted(set(self.books) | self.tickers | self.statuses)

    async def subscribe(self) -> None:
        if self.watched():
            await self.emit_event("subscribe_market_prices", {"marketSlugs": self.watched()})

    async def watch_order_book(self, market_ids: list[str]) -> None:
        """Each market's whole book, on every change."""
        for market_id in market_ids:
            self.books.setdefault(slug_of(market_id), LocalBook())
        await self.subscribe()

    async def watch_ticker(self, market_ids: list[str]) -> None:
        """Top of book on every change."""
        self.tickers.update(slug_of(m) for m in market_ids)
        await self.subscribe()

    async def watch_market_status(self, market_ids: list[str]) -> None:
        """The market's resolution, when it comes."""
        self.statuses.update(slug_of(m) for m in market_ids)
        await self.subscribe()

    async def unwatch(self, market_ids: list[str]) -> None:
        """Stop watching; the venue has no unsubscribe, so the smaller set is sent."""
        for market_id in market_ids:
            slug = slug_of(market_id)
            self.books.pop(slug, None)
            self.tickers.discard(slug)
            self.statuses.discard(slug)
            self._versions.pop(slug, None)
        await self.emit_event("subscribe_market_prices", {"marketSlugs": self.watched()})

    def book(self, market_id: str, side: str = "yes") -> LocalBook | None:
        book = self.books.get(slug_of(market_id))
        if book is None:
            return None
        return book if side != "no" else book.mirrored()

    def on_disconnect(self) -> None:
        super().on_disconnect()
        self._versions.clear()
        for book in self.books.values():
            book.invalidate()

    def on_event(self, name: str, data: Any) -> list[Event]:
        if not isinstance(data, dict):
            return super().on_event(name, data)
        if name == "orderbookUpdate":
            return self._on_book(data)
        if name == "marketResolved":
            slug = str(data.get("slug") or "")
            winner = str(data.get("winningOutcome") or "").lower() or None
            return [MarketStatusEvent(
                venue=VENUE, market_id=ids.qualify(VENUE, slug), state="settled", native="marketResolved",
                result=winner, timestamp=_ms(data.get("resolutionDate")), info=data,
            )]
        return super().on_event(name, data)

    def _on_book(self, data: dict[str, Any]) -> list[Event]:
        slug = str(data.get("marketSlug") or "")
        version = data.get("version")
        if isinstance(version, int):
            if version <= self._versions.get(slug, -1):
                return []
            self._versions[slug] = version
        orderbook = data.get("orderbook") or {}
        bids, asks = _levels(orderbook.get("bids")), _levels(orderbook.get("asks"))
        market_id = ids.qualify(VENUE, slug)
        stamp = _ms(data.get("timestamp"))
        events: list[Event] = []
        book = self.books.get(slug)
        if book is not None:
            recovering = not book.ready and book.timestamp is not None
            book.replace(bids, asks)
            book.timestamp = stamp
            if recovering:
                self.status("resynced", f"book {market_id}", key=market_id)
            levels_bids, levels_asks = book.levels()
            events.append(BookEvent(
                venue=VENUE, market_id=market_id, side="yes", kind="snapshot", bids=levels_bids, asks=levels_asks,
                best_bid=book.best_bid, best_ask=book.best_ask, sequence=version if isinstance(version, int) else None,
                timestamp=stamp, info={"midpoint": orderbook.get("midpoint")},
            ))
        if slug in self.tickers:
            best_bid = max(bids, default=None, key=lambda level: level[0])
            best_ask = min(asks, default=None, key=lambda level: level[0])
            events.append(QuoteEvent(
                venue=VENUE, market_id=market_id,
                bid=best_bid[0] if best_bid else None, bid_size=best_bid[1] if best_bid else None,
                ask=best_ask[0] if best_ask else None, ask_size=best_ask[1] if best_ask else None,
                timestamp=stamp, info={"midpoint": orderbook.get("midpoint")},
            ))
        return events


class LimitlessUserStream(_LimitlessStream):
    """An account's orders and fills. Needs a scoped API token: `token_id`
    and `secret` (base64), created at limitless.exchange under profile, API
    keys.

    Order-engine frames name a token, not a market. Markets passed to
    `watch_orders` are placed at once; anything else is placed from one walk
    of the open catalog (`catalog=`, a `Limitless`, made when not given).
    """

    name = "user"
    private = True
    has: dict[str, Capability] = {"watch_orders": True, "watch_my_trades": True}

    def __init__(self, *, token_id: str, secret: str, catalog: Any = None, account_name: str = "default", **kwargs: Any):
        if not token_id or not secret:
            raise AuthenticationError(f"{VENUE}: the account stream needs an API token id and secret")
        super().__init__(**kwargs)
        self._token_id, self._secret = token_id, secret
        self.catalog = catalog
        self.account = Account(venue=VENUE, name=account_name)
        self.watching = False
        self.tokens: dict[str, tuple[str, str]] = {}
        """Token id -> (market id, "yes" or "no")."""
        self._indexed = False
        self._waiting: list[dict[str, Any]] = []

    def headers(self) -> dict[str, str]:
        return auth_headers(self._token_id, self._secret)

    async def subscribe(self) -> None:
        if self.watching:
            await self.emit_event("subscribe_order_events")

    async def watch_orders(self, market_ids: list[str] | None = None) -> None:
        """Every order and fill on the account; the venue has no per-market
        filter. Markets named here are placed without a catalog walk."""
        for market_id in market_ids or []:
            await self._learn(market_id)
        self.watching = True
        await self.subscribe()

    watch_my_trades = watch_orders

    def learn(self, market_id: str, yes_token: str | None, no_token: str | None) -> None:
        """Record a market's tokens, so its order frames can be placed."""
        if yes_token:
            self.tokens[str(yes_token)] = (market_id, "yes")
        if no_token:
            self.tokens[str(no_token)] = (market_id, "no")

    async def _learn(self, market_id: str) -> None:
        market = await asyncio.to_thread(self._catalog().fetch_market, market_id)
        self.learn(market.id, market.yes.venue_token_id, market.no.venue_token_id)

    def _catalog(self) -> Any:
        if self.catalog is None:
            from ..limitless import Limitless

            self.catalog = Limitless()
        return self.catalog

    def on_event(self, name: str, data: Any) -> list[Event]:
        if name == "system":
            return []
        if name != "orderEvent" or not isinstance(data, dict):
            return super().on_event(name, data)
        placed = self._place(data)
        if placed is None:
            self._waiting.append(data)
            if len(self._waiting) == 1:
                self._later(self._resolve_waiting())
            return []
        return self._events(data, *placed)

    def _place(self, data: dict[str, Any]) -> tuple[str, str] | None:
        """(market id, outcome) for a frame, or `None` until its token is known."""
        token = str(data.get("tokenId") or data.get("token") or "")
        if token in self.tokens:
            return self.tokens[token]
        slug = data.get("marketSlug")
        if slug and str(data.get("token") or "").upper() in ("YES", "NO"):
            return ids.qualify(VENUE, str(slug)), str(data["token"]).lower()
        return None

    async def _resolve_waiting(self) -> None:
        """Place frames whose token was unknown: by their slug where they carry
        one, else from one walk of the open catalog."""
        try:
            slugs = {str(d["marketSlug"]) for d in self._waiting if d.get("marketSlug")}
            for slug in slugs:
                await self._learn(ids.qualify(VENUE, slug))
            if any(self._place(d) is None for d in self._waiting) and not self._indexed:
                self._indexed = True
                self.tokens.update(await asyncio.to_thread(self._catalog().token_index))
        except Exception as exc:  # pragma: no cover - the catalog being down
            self.status("error", f"could not place order events: {type(exc).__name__}: {exc}")
        waiting, self._waiting = self._waiting, []
        for data in waiting:
            placed = self._place(data)
            if placed is None:
                self.status("error", f"order event for an unknown token: {data.get('orderId')}")
                continue
            for event in self._events(data, *placed):
                self.emit(event)

    def _events(self, data: dict[str, Any], market_id: str, outcome: str) -> list[Event]:
        if data.get("source") == "SETTLEMENT":
            fill = fill_of_event(data, market_id=market_id, outcome=outcome, account=self.account)
            return [FillEvent(venue=VENUE, fill=fill)] if fill else []
        order = order_of_event(data, market_id=market_id, outcome=outcome, account=self.account)
        return [OrderEvent(venue=VENUE, order=order, native=f"OME:{data.get('type')}")]


def _ms(value: Any) -> int | None:
    from ..limitless import iso_ms

    return iso_ms(value)
