"""Opinion's WebSocket: public market channels and this account's channels.

One endpoint, `wss://ws.opinion.trade`, authenticated by the API key in the
URL; every channel needs a key, the public ones included. Heartbeat is the
frame `{"action":"HEARTBEAT"}`, sent every 25 seconds here (the venue asks
for one at least every 30).

**Books.** `market.depth.diff` sends one changed level per message: token,
side, price and the level's size after the change. There is no snapshot on
subscribing and no sequence number, so a book here is the REST book read
after the subscription is live, with every change that arrived during the
read replayed on top. A change is read as the level's whole size (the venue
documents `size` as the level's shares), so replaying one the snapshot
already includes is harmless. With nothing in the feed to
detect a missed message, the stream re-reads every book on a timer
(`resync_interval`): a book that disagrees with the venue while no change
was in flight is reported as a gap and replaced.

**Trades and prices.** `market.last.trade` and `market.last.price` are
subscribed per binary market, and per topic for an option of a categorical
topic (the venue sends every option's messages on the topic's
subscription); messages for markets not watched are dropped.

**Account.** `trade.order.update` (placement, match, cancellation, chain
confirmation) and `trade.record.new` (a fill once the chain confirms it) are
per market or topic, like trades: the venue has no all-markets subscription.
Nothing is replayed after a reconnect.
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

from .. import _native, ids
from ..base import Capability
from ..errors import AuthenticationError
from ..trading.opinion import fill_of, order_of, outcome_of
from ..trading.types import Account, Side
from .base import (
    ONE, BookEvent, BookLevel, Event, FillEvent, LocalBook, OrderEvent, QuoteEvent, Stream, TradeEvent, VenueEvent,
)

VENUE = "opinion"
WS_URL = "wss://ws.opinion.trade"
HEARTBEAT = '{"action":"HEARTBEAT"}'

RESYNC_INTERVAL = 60.0
"""Seconds between re-reads of every watched book. Each costs two requests
per market against the venue's 5-a-second public REST limit."""


def api_key_from(api_key: str | None) -> str:
    """The key given, else `OPINION_API_KEY` from the environment or a `.env`
    in the working directory."""
    if api_key:
        return api_key
    found = os.environ.get("OPINION_API_KEY")
    if not found and os.path.exists(".env"):
        from ..trading.credentials import read_dotenv

        found = read_dotenv(".env").get("OPINION_API_KEY")
    if not found:
        raise AuthenticationError(
            "opinion: the WebSocket needs an API key -- pass api_key= or set OPINION_API_KEY. "
            "Create one at https://docs.opinion.trade/developer-guide/opinion-open-api/authentication"
        )
    return found


@dataclass(frozen=True)
class Watched:
    """One market the stream follows, and what the venue keys it on."""

    market_id: str
    native: str
    yes_token: str
    no_token: str
    topic: str | None
    """The categorical topic's id for an option; `None` for a binary topic."""

    def side_of(self, token: str) -> str:
        return "no" if token == self.no_token else "yes"


class _OpinionStream(Stream):
    """The connection, key and subscription bookkeeping both streams share."""

    venue = VENUE
    app_ping = HEARTBEAT

    def __init__(
        self, *, api_key: str | None = None, url: str = WS_URL, catalog: Any = None,
        ping_interval: float = 25.0, **kwargs: Any,
    ):
        key = api_key_from(api_key)
        super().__init__(f"{url}?{urlencode({'apikey': key})}", ping_interval=ping_interval, **kwargs)
        self._key = key
        if catalog is None:
            from ..opinion import Opinion

            catalog = Opinion()
        self.catalog = catalog
        """The read adapter: which tokens and topic a market has, and books."""
        self.watched: dict[str, Watched] = {}
        self.subscriptions: set[tuple[str, str, int]] = set()
        """`(channel, "marketId" | "rootMarketId", id)`, sent again on every connect."""

    def status(self, state: Any, detail: str = "", **kwargs: Any) -> None:
        # The key travels in the URL, so it is kept out of anything reported.
        super().status(state, detail.replace(self._key, "***") if detail else detail, **kwargs)

    async def _open(self) -> Any:
        if self._connect is not None:
            return await self._connect(self.url, additional_headers=self.headers())
        import websockets

        # Protocol pings as well as the venue's heartbeat frame: the heartbeat
        # keeps the session, the pings notice a dead peer.
        return await websockets.connect(self.url, ping_interval=20, max_size=None)

    async def _watch(self, market_id: str) -> Watched:
        native = ids.native(VENUE, market_id)
        if native not in self.watched:
            market = await asyncio.to_thread(self.catalog.fetch_market, native)
            event_native = ids.native(VENUE, market.event_id) if market.event_id else native
            self.watched[native] = Watched(
                market_id=market.id, native=native,
                yes_token=market.yes.venue_token_id or "", no_token=market.no.venue_token_id or "",
                topic=event_native if event_native != native else None,
            )
        return self.watched[native]

    async def _subscribe(self, channel: str, watched: Watched, *, per_topic: bool) -> None:
        """Subscribe once; an option's per-topic channels go to its topic."""
        field, ident = ("rootMarketId", watched.topic) if per_topic and watched.topic else ("marketId", watched.native)
        key = (channel, field, int(ident))
        if key in self.subscriptions:
            return
        self.subscriptions.add(key)
        await self.send({"action": "SUBSCRIBE", "channel": channel, field: int(ident)})

    async def _unsubscribe(self, key: tuple[str, str, int]) -> None:
        if key in self.subscriptions:
            self.subscriptions.discard(key)
            channel, field, ident = key
            await self.send({"action": "UNSUBSCRIBE", "channel": channel, field: ident})

    async def on_connect(self) -> None:
        for channel, field, ident in sorted(self.subscriptions):
            await self.send({"action": "SUBSCRIBE", "channel": channel, field: ident})

    def _venue_error(self, message: dict[str, Any]) -> list[Event]:
        """A message without `msgType`: an acknowledgement, or the venue
        refusing something. Only the refusals are reported."""
        code = message.get("code", message.get("errno", 0))
        if message.get("error") or (isinstance(code, int) and code not in (0, 200)):
            self.status("error", f"venue: {message}")
        return []


class OpinionMarketStream(_OpinionStream):
    """Books, trades and last prices for a set of markets. Needs an API key."""

    name = "market"
    has: dict[str, Capability] = {
        "watch_order_book": True,
        "watch_trades": True,
        # The last traded price only; the venue streams no top of book.
        "watch_ticker": "partial",
        "watch_market_status": False,
    }

    def __init__(self, *, resync_interval: float | None = RESYNC_INTERVAL, **kwargs: Any):
        super().__init__(**kwargs)
        self.resync_interval = resync_interval
        self.books: dict[str, LocalBook] = {}
        """Token id -> its book. Both sides are read, not mirrored."""
        self.tokens: dict[str, Watched] = {}
        self.depth: set[str] = set()
        self.trades: set[str] = set()
        self.prices: set[str] = set()
        self._pending: dict[str, list[dict[str, Any]]] = {}
        """Token -> changes that arrived while its snapshot was being read."""
        self._resyncer: asyncio.Task | None = None

    async def watch_order_book(self, market_ids: list[str]) -> None:
        """Both sides' books for these markets. Each market not yet known
        costs one catalog read; each book one REST read once connected."""
        for market_id in market_ids:
            watched = await self._watch(market_id)
            for token in (watched.yes_token, watched.no_token):
                self.tokens[token] = watched
                self.books.setdefault(token, LocalBook())
            new = watched.native not in self.depth
            self.depth.add(watched.native)
            await self._subscribe("market.depth.diff", watched, per_topic=False)
            if new and self._ws is not None:
                await self._snapshot(watched)

    async def watch_trades(self, market_ids: list[str]) -> None:
        for market_id in market_ids:
            watched = await self._watch(market_id)
            self.trades.add(watched.native)
            await self._subscribe("market.last.trade", watched, per_topic=True)

    async def watch_ticker(self, market_ids: list[str]) -> None:
        """The last traded price, on both sides, as `QuoteEvent.last`."""
        for market_id in market_ids:
            watched = await self._watch(market_id)
            self.prices.add(watched.native)
            await self._subscribe("market.last.price", watched, per_topic=True)

    async def unwatch(self, market_ids: list[str]) -> None:
        for market_id in market_ids:
            native = ids.native(VENUE, market_id)
            watched = self.watched.get(native)
            if watched is None:
                continue
            self.depth.discard(native)
            self.trades.discard(native)
            self.prices.discard(native)
            for token in (watched.yes_token, watched.no_token):
                self.books.pop(token, None)
                self.tokens.pop(token, None)
                self._pending.pop(token, None)
            await self._unsubscribe(("market.depth.diff", "marketId", int(native)))
            if watched.topic is None:
                for channel in ("market.last.trade", "market.last.price"):
                    await self._unsubscribe((channel, "marketId", int(native)))
            else:
                # The topic's subscription serves its other options too.
                siblings = [w for w in self.watched.values()
                            if w.topic == watched.topic and w.native != native
                            and (w.native in self.trades or w.native in self.prices)]
                if not siblings:
                    for channel in ("market.last.trade", "market.last.price"):
                        await self._unsubscribe((channel, "rootMarketId", int(watched.topic)))
            self.watched.pop(native, None)

    def book(self, market_id: str, side: str = "yes") -> LocalBook | None:
        watched = self.watched.get(ids.native(VENUE, market_id))
        if watched is None:
            return None
        return self.books.get(watched.no_token if side == "no" else watched.yes_token)

    # -- connection -----------------------------------------------------------

    async def on_connect(self) -> None:
        await super().on_connect()
        for native in sorted(self.depth):
            self._later(self._snapshot(self.watched[native]))
        if self.resync_interval:
            self._resyncer = asyncio.get_running_loop().create_task(self._resync_loop())

    def on_disconnect(self) -> None:
        if self._resyncer is not None:
            self._resyncer.cancel()
            self._resyncer = None
        self._pending.clear()
        for book in self.books.values():
            book.invalidate()

    async def close(self) -> None:
        if self._resyncer is not None:
            self._resyncer.cancel()
        await super().close()

    def _later(self, coroutine: Any) -> None:
        asyncio.get_running_loop().create_task(coroutine)

    # -- books ----------------------------------------------------------------

    async def _snapshot(self, watched: Watched, *, check: bool = False) -> None:
        """Read both of a market's books and make them the local books, with
        the changes that arrived meanwhile replayed on top. With `check`, a
        ready book that disagrees with the venue while no change was in
        flight is reported as a gap first."""
        tokens = (watched.yes_token, watched.no_token)
        for token in tokens:
            self._pending[token] = []
        try:
            raws = await asyncio.gather(*(
                asyncio.to_thread(self.catalog.fetch_order_book, watched.native, side=side)
                for side in ("yes", "no")
            ))
        except Exception as exc:
            for token in tokens:
                self._pending.pop(token, None)
            self.status("error", f"book {watched.market_id}: snapshot failed: {type(exc).__name__}: {exc}",
                        key=watched.market_id)
            return
        for token, side, snapshot in zip(tokens, ("yes", "no"), raws):
            book = self.books.get(token)
            pending = self._pending.pop(token, [])
            if book is None:
                continue  # unwatched while reading
            bids = [(Decimal(str(l["price"])), Decimal(str(l["size"]))) for l in snapshot.info.get("bids") or []]
            asks = [(Decimal(str(l["price"])), Decimal(str(l["size"]))) for l in snapshot.info.get("asks") or []]
            recovering = not book.ready and book.timestamp is not None
            if check and book.ready and not pending:
                if dict(bids) != book.bids or dict(asks) != book.asks:
                    self.status(
                        "gap", f"book {watched.market_id} {side}: local top {book.best_bid}/{book.best_ask} "
                               f"differs from the venue's", key=watched.market_id,
                    )
                    recovering = True
                else:
                    continue
            book.replace([l for l in bids if l[1] > 0], [l for l in asks if l[1] > 0])
            book.timestamp = snapshot.timestamp
            for change in pending:
                self._apply(book, change)
            if recovering:
                self.status("resynced", f"book {watched.market_id} {side}", key=watched.market_id)
            levels_bids, levels_asks = book.levels()
            self.emit(BookEvent(
                venue=VENUE, market_id=watched.market_id, side=side, kind="snapshot",  # type: ignore[arg-type]
                bids=levels_bids, asks=levels_asks, best_bid=book.best_bid, best_ask=book.best_ask,
                timestamp=book.timestamp, info={"replayed": len(pending)},
            ))

    async def _resync_loop(self) -> None:
        while True:
            await asyncio.sleep(self.resync_interval or 60.0)
            for native in sorted(self.depth):
                watched = self.watched.get(native)
                if watched is not None:
                    await self._snapshot(watched, check=True)

    @staticmethod
    def _apply(book: LocalBook, change: dict[str, Any]) -> tuple[str, Decimal, Decimal]:
        side = "bid" if str(change.get("side")).lower() == "bids" else "ask"
        price = Decimal(str(change["price"]))
        size = book.set(side, price, Decimal(str(change["size"])))  # type: ignore[arg-type]
        return side, price, size

    # -- reading --------------------------------------------------------------

    def handle_raw(self, raw: str) -> list[Event] | None:
        """A depth change, decoded and applied by the Rust core on the same
        book. A change held back while a snapshot is fetched stays a dict
        for the replay, so that, everything else, and everything when the
        core is not built, goes to `handle`."""
        if _native.core is None:
            return None
        change = _native.core.opinion_depth(raw)
        if change is None:
            return None
        token = change.token
        watched = self.tokens.get(token)
        if watched is None:
            return []
        if token in self._pending:
            return None
        book = self.books.get(token)
        if book is None or not book.ready:
            return []
        if not isinstance(book, _native.core.LocalBook):
            return None
        bids, asks, best_bid, best_ask = change.apply(book)
        return [BookEvent(
            venue=VENUE, market_id=watched.market_id, side=watched.side_of(token), kind="delta",  # type: ignore[arg-type]
            bids=bids, asks=asks, best_bid=best_bid, best_ask=best_ask,
        )]

    def handle(self, message: Any) -> list[Event]:
        if isinstance(message, list):
            events: list[Event] = []
            for item in message:
                events.extend(self.handle(item))
            return events
        if not isinstance(message, dict):
            return []
        kind = message.get("msgType")
        if kind == "market.depth.diff":
            return self._on_depth(message)
        if kind == "market.last.trade":
            return self._on_trade(message)
        if kind == "market.last.price":
            return self._on_price(message)
        if kind is None:
            return self._venue_error(message)
        return []

    def _on_depth(self, message: dict[str, Any]) -> list[Event]:
        token = str(message.get("tokenId") or "")
        watched = self.tokens.get(token)
        if watched is None:
            return []
        if token in self._pending:
            self._pending[token].append(message)
            return []
        book = self.books.get(token)
        if book is None or not book.ready:
            return []
        side, price, size = self._apply(book, message)
        level = (BookLevel(price, size),)
        return [BookEvent(
            venue=VENUE, market_id=watched.market_id, side=watched.side_of(token), kind="delta",  # type: ignore[arg-type]
            bids=level if side == "bid" else (), asks=level if side == "ask" else (),
            best_bid=book.best_bid, best_ask=book.best_ask,
        )]

    def _on_trade(self, message: dict[str, Any]) -> list[Event]:
        native = str(message.get("marketId") or "")
        watched = self.watched.get(native)
        if watched is None or native not in self.trades:
            return []
        venue_side = str(message.get("side") or "").lower()
        if venue_side not in ("buy", "sell"):
            return [VenueEvent(venue=VENUE, name=venue_side or "trade", payload=message)]
        price = Decimal(str(message["price"]))
        buying = venue_side == "buy"
        if outcome_of(message) == "no":
            price, buying = ONE - price, not buying
        return [TradeEvent(
            venue=VENUE, market_id=watched.market_id, id=None, price=price,
            amount=Decimal(str(message.get("shares") or "0")),
            taker_side=Side.BUY if buying else Side.SELL, info=message,
        )]

    def _on_price(self, message: dict[str, Any]) -> list[Event]:
        native = str(message.get("marketId") or "")
        watched = self.watched.get(native)
        if watched is None or native not in self.prices:
            return []
        return [QuoteEvent(
            venue=VENUE, market_id=watched.market_id, side=outcome_of(message),  # type: ignore[arg-type]
            last=Decimal(str(message["price"])), info=message,
        )]


class OpinionUserStream(_OpinionStream):
    """This account's orders and confirmed fills, for the markets asked for.

    The API key names the account. Fills arrive once the chain confirms
    them; an order update's filled figures move at the same moment.
    """

    name = "user"
    private = True
    has: dict[str, Capability] = {
        "watch_orders": True,
        "watch_my_trades": True,
        "watch_positions": False,
        "watch_balance": False,
    }

    def __init__(self, *, account_name: str = "default", **kwargs: Any):
        super().__init__(**kwargs)
        self.account = Account(venue=VENUE, name=account_name)

    async def watch_orders(self, market_ids: list[str]) -> None:
        """Orders and fills in these markets. An option of a categorical topic
        brings the topic's other options too: the venue subscribes per topic."""
        for market_id in market_ids:
            watched = await self._watch(market_id)
            await self._subscribe("trade.order.update", watched, per_topic=True)
            await self._subscribe("trade.record.new", watched, per_topic=True)

    watch_my_trades = watch_orders

    def handle(self, message: Any) -> list[Event]:
        if isinstance(message, list):
            events: list[Event] = []
            for item in message:
                events.extend(self.handle(item))
            return events
        if not isinstance(message, dict):
            return []
        kind = message.get("msgType")
        if kind == "trade.order.update":
            return [OrderEvent(venue=VENUE, order=order_of(message, account=self.account),
                               native=message.get("orderUpdateType"))]
        if kind == "trade.record.new":
            fill = fill_of(message, account=self.account)
            if fill is None:
                return [VenueEvent(venue=VENUE, name=str(message.get("side") or "").lower(), payload=message)]
            return [FillEvent(venue=VENUE, fill=fill)]
        if kind is None:
            return self._venue_error(message)
        return []
