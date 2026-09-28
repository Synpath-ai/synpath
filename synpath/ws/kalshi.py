"""Kalshi's WebSocket: books, tickers, trades, and the account's own activity.

One authenticated connection carries every channel. The handshake is signed
like a REST request (`timestamp + GET + /trade-api/ws/v2`), even for public
market data.

**Sequence numbers.** Every message on a subscription that carries `seq`
(books, public trades, market lifecycle, order groups) numbers it, one
sequence per subscription across all its markets; a snapshot fetched later
takes its own number in the same sequence. A number that is not one more
than the last is a gap. For a book the stream asks for fresh snapshots of
that subscription's markets and ignores deltas until they arrive; for
trades and lifecycle events there is nothing to fetch again, so the gap is
reported for the caller to read over REST.

**Books are kept on the YES leg.** Kalshi publishes YES bids and NO bids;
a NO bid at `p` is a YES ask at `1 - p`. Book events are for `{ticker}:yes`;
`book("TICKER:no")` returns the mirrored view.

**Keepalive is protocol pings.** Kalshi pings every ten seconds at the
WebSocket layer, which the library answers and does not surface, so a demo
market can be silent for minutes on a healthy connection. The stream
therefore does not treat silence as death; the library's own ping/pong does
that.

**Private channels have no sequence.** Orders, fills and positions cannot be
checked for gaps and are not replayed after a reconnect, so a reconnect with
any of them subscribed is reported with `reconcile_required`.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from .. import ids
from ..base import Capability
from ..kalshi import parse_ts
from ..trading.credentials import KalshiCredentials
from ..trading.kalshi import KalshiSigner, fill_of, order_of, position_of
from ..trading.types import Account, Side
from .base import (
    ONE, BookEvent, BookLevel, D, Event, LocalBook, MarketStatusEvent, OrderEvent, FillEvent, PositionEvent,
    QuoteEvent, Stream, TradeEvent, VenueEvent, maybe_D,
)

VENUE = "kalshi"
PATH = "/trade-api/ws/v2"
URLS = {
    "demo": "wss://external-api-ws.demo.kalshi.co" + PATH,
    "prod": "wss://external-api-ws.kalshi.com" + PATH,
}

PRIVATE_CHANNELS = frozenset({"fill", "user_orders", "market_positions", "order_group_updates"})
MARKET_SCOPED = frozenset({"orderbook_delta", "ticker", "trade", "fill", "user_orders", "market_positions"})
"""Channels that take a market filter; the rest take none."""

RESYNC_AFTER_S = 10.0
"""Ask again for a snapshot that has not arrived after this long."""

LIFECYCLE_STATE = {
    "created": "created",
    "activated": "open",
    "deactivated": "paused",
    "close_date_updated": "updated",
    "determined": "determined",
    "settled": "settled",
    "price_level_structure_updated": "updated",
}


@dataclass
class Channel:
    """One Kalshi channel's subscription: what was asked for, and what the
    server calls it on this connection."""

    name: str
    tickers: set[str] | None
    """`None` means every market."""
    sid: int | None = None
    command_id: int | None = None
    last_seq: int | None = None
    pending_add: set[str] = field(default_factory=set)


class KalshiStream(Stream):
    """Kalshi market data and account activity on one connection.

    ```python
    async with KalshiStream(creds) as stream:
        await stream.watch_order_book(["KXBTCD-26SEP1717-T115000"])
        await stream.watch_orders()
        async for event in stream:
            ...
    ```
    """

    venue = VENUE
    name = "trade-api"
    has: dict[str, Capability] = {
        "watch_order_book": True,
        "watch_ticker": True,
        "watch_trades": True,
        "watch_market_status": True,
        "watch_orders": True,
        "watch_my_trades": True,
        "watch_positions": True,
        # No balance channel; balance is read over REST.
        "watch_balance": False,
    }

    def __init__(self, credentials: KalshiCredentials, *, url: str | None = None, account_name: str = "default", **kwargs: Any):
        super().__init__(url or URLS[credentials.env], **kwargs)
        self.signer = KalshiSigner(credentials.key_id, credentials.private_key_pem)
        self.account = Account(venue=VENUE, name=account_name)
        self.channels: dict[str, Channel] = {}
        self.books: dict[str, LocalBook] = {}
        self._by_sid: dict[int, Channel] = {}
        self._by_command: dict[int, Channel] = {}
        self._next_id = 1
        self._snapshot_requested: dict[str, float] = {}

    @property
    def private(self) -> bool:  # type: ignore[override]
        return any(name in PRIVATE_CHANNELS for name in self.channels)

    def headers(self) -> dict[str, str]:
        return self.signer.headers("GET", PATH)

    # -- subscribing ----------------------------------------------------------

    async def watch_order_book(self, market_ids: list[str]) -> None:
        """Books for these markets: a snapshot each, then deltas."""
        tickers = [ids.native(VENUE, m) for m in market_ids]
        for ticker in tickers:
            self.books.setdefault(ticker, LocalBook())
        await self._watch("orderbook_delta", tickers)

    async def watch_ticker(self, market_ids: list[str] | None = None) -> None:
        await self._watch("ticker", _tickers(market_ids))

    async def watch_trades(self, market_ids: list[str] | None = None) -> None:
        await self._watch("trade", _tickers(market_ids))

    async def watch_market_status(self) -> None:
        """Every market's lifecycle: created, paused and resumed, closed, determined, settled."""
        await self._watch("market_lifecycle_v2", None)

    async def watch_orders(self, market_ids: list[str] | None = None) -> None:
        tickers = _tickers(market_ids)
        await self._watch("user_orders", tickers)

    async def watch_my_trades(self, market_ids: list[str] | None = None) -> None:
        tickers = _tickers(market_ids)
        await self._watch("fill", tickers)

    async def watch_positions(self, market_ids: list[str] | None = None) -> None:
        tickers = _tickers(market_ids)
        await self._watch("market_positions", tickers)

    async def watch_order_groups(self) -> None:
        await self._watch("order_group_updates", None)

    async def _watch(self, name: str, tickers: list[str] | None) -> None:
        channel = self.channels.get(name)
        if channel is None:
            channel = self.channels[name] = Channel(name, set(tickers) if tickers else None)
            await self._subscribe(channel)
            return
        if channel.tickers is None:
            return
        if not tickers:
            # Widening a market-filtered channel to every market.
            channel.tickers = None
            self._restart(channel)
            return
        new = set(tickers) - channel.tickers
        if not new:
            return
        channel.tickers |= new
        if channel.sid is None:
            # Subscribe still in flight: add once the server has named it.
            channel.pending_add |= new
        else:
            await self._send_command("update_subscription", {
                "sids": [channel.sid], "market_tickers": sorted(new), "action": "add_markets",
            })

    async def _subscribe(self, channel: Channel) -> None:
        params: dict[str, Any] = {"channels": [channel.name]}
        if channel.tickers and channel.name in MARKET_SCOPED:
            params["market_tickers"] = sorted(channel.tickers)
        command_id = self._allocate()
        channel.command_id, channel.sid, channel.last_seq = command_id, None, None
        self._by_command[command_id] = channel
        await self.send({"id": command_id, "cmd": "subscribe", "params": params})

    async def _send_command(self, cmd: str, params: dict[str, Any]) -> None:
        await self.send({"id": self._allocate(), "cmd": cmd, "params": params})

    def _allocate(self) -> int:
        command_id = self._next_id
        self._next_id += 1
        return command_id

    async def on_connect(self) -> None:
        for channel in self.channels.values():
            channel.pending_add.clear()
            await self._subscribe(channel)

    def on_disconnect(self) -> None:
        self._by_sid.clear()
        self._by_command.clear()
        self._snapshot_requested.clear()
        for channel in self.channels.values():
            channel.sid = channel.command_id = channel.last_seq = None
        for book in self.books.values():
            book.invalidate()

    def book(self, market_id: str, side: str = "yes") -> LocalBook | None:
        """The local book for a market, on either side; `None` if not watched."""
        book = self.books.get(ids.native(VENUE, market_id))
        if book is None:
            return None
        return book if side != "no" else book.mirrored()

    # -- reading --------------------------------------------------------------

    def handle(self, message: Any) -> list[Event]:
        if not isinstance(message, dict):
            return []
        kind = message.get("type")
        sid = message.get("sid")
        channel = self._by_sid.get(sid) if sid is not None else None
        if kind == "subscribed":
            return self._on_subscribed(message)
        if kind == "error":
            return self._on_error(message)
        if channel is not None and message.get("seq") is not None:
            self._check_sequence(channel, int(message["seq"]), kind)
        body = message.get("msg") or {}
        if kind == "orderbook_snapshot":
            return self._on_snapshot(message, body, channel)
        if kind == "orderbook_delta":
            return self._on_delta(message, body)
        if kind == "ticker":
            return [self._quote(body)]
        if kind == "trade":
            return [self._trade(body, message)]
        if kind == "fill":
            fill = fill_of(body, account=self.account)
            stamp = body.get("ts_ms") or parse_ts(body.get("ts"))
            return [FillEvent(venue=VENUE, fill=fill.model_copy(update={"timestamp": int(stamp or 0), "client_order_id": body.get("client_order_id")}))]
        if kind == "user_order":
            return [OrderEvent(venue=VENUE, order=order_of(body, account=self.account), native=str(body.get("status") or ""))]
        if kind == "market_position":
            row = {
                "ticker": body.get("market_ticker"), "position_fp": body.get("position_fp"),
                "market_exposure_dollars": body.get("position_cost_dollars"),
                "realized_pnl_dollars": body.get("realized_pnl_dollars"), "fees_paid_dollars": body.get("fees_paid_dollars"),
            }
            position = position_of(row, account=self.account)
            return [PositionEvent(venue=VENUE, position=position.model_copy(update={"info": body}))]
        if kind == "order_group_updates":
            return [VenueEvent(venue=VENUE, name="order_group", payload=body, timestamp=body.get("ts_ms"))]
        if kind == "market_lifecycle_v2":
            return [self._lifecycle(body)]
        if kind in ("event_lifecycle", "multivariate_market_lifecycle", "multivariate_event_lifecycle"):
            return [VenueEvent(venue=VENUE, name=kind, payload=body)]
        return []

    def _on_subscribed(self, message: dict[str, Any]) -> list[Event]:
        body = message.get("msg") or {}
        channel = self._by_command.pop(message.get("id"), None) or self.channels.get(str(body.get("channel")))
        if channel is None:
            return []
        channel.sid = int(body["sid"])
        self._by_sid[channel.sid] = channel
        self.status("subscribed", channel.name, key=channel.name)
        if channel.pending_add:
            added, channel.pending_add = sorted(channel.pending_add), set()
            self._later(self._send_command("update_subscription", {
                "sids": [channel.sid], "market_tickers": added, "action": "add_markets",
            }))
        return []

    def _on_error(self, message: dict[str, Any]) -> list[Event]:
        body = message.get("msg") or {}
        code = body.get("code")
        detail = f"code {code}: {body.get('msg')}"
        channel = self._by_command.pop(message.get("id"), None) or self._by_sid.get(message.get("sid"))
        if code == 25 and channel is not None:
            # The server dropped messages for this subscription: start it again.
            self.status("gap", f"{channel.name}: {detail}", key=channel.name, reconcile=channel.name in PRIVATE_CHANNELS)
            self._restart(channel)
            return []
        self.status("error", detail, key=channel.name if channel else None)
        return []

    def _check_sequence(self, channel: Channel, seq: int, kind: str | None) -> None:
        last = channel.last_seq
        channel.last_seq = seq
        if last is None or seq == last + 1:
            return
        self.status(
            "gap", f"{channel.name}: expected seq {last + 1}, got {seq}", key=channel.name,
            reconcile=channel.name in PRIVATE_CHANNELS,
        )
        if channel.name == "orderbook_delta":
            tickers = sorted(channel.tickers or [])
            for ticker in tickers:
                if ticker in self.books:
                    self.books[ticker].invalidate()
            self._request_snapshots(channel, tickers)

    def _request_snapshots(self, channel: Channel, tickers: list[str]) -> None:
        now = time.monotonic()
        wanted = [t for t in tickers if now - self._snapshot_requested.get(t, 0) > RESYNC_AFTER_S]
        if not wanted or channel.sid is None:
            return
        for ticker in wanted:
            self._snapshot_requested[ticker] = now
        self._later(self._send_command("update_subscription", {
            "sids": [channel.sid], "market_tickers": wanted, "action": "get_snapshot",
        }))

    def _restart(self, channel: Channel) -> None:
        if channel.sid is not None:
            self._by_sid.pop(channel.sid, None)
            self._later(self._send_command("unsubscribe", {"sids": [channel.sid]}))
        if channel.name == "orderbook_delta":
            for ticker in channel.tickers or []:
                if ticker in self.books:
                    self.books[ticker].invalidate()
        self._later(self._subscribe(channel))

    def _later(self, coroutine: Any) -> None:
        import asyncio

        asyncio.get_running_loop().create_task(coroutine)

    def _on_snapshot(self, message: dict[str, Any], body: dict[str, Any], channel: Channel | None) -> list[Event]:
        ticker = str(body.get("market_ticker") or "")
        book = self.books.setdefault(ticker, LocalBook())
        yes = [(D(p), D(s)) for p, s in body.get("yes_dollars_fp") or []]
        no = [(ONE - D(p), D(s)) for p, s in body.get("no_dollars_fp") or []]
        recovering = not book.ready and book.sequence is not None
        book.replace(yes, no)
        book.sequence = message.get("seq")
        self._snapshot_requested.pop(ticker, None)
        if recovering:
            self.status("resynced", f"orderbook {ticker}", key=ticker)
        bids, asks = book.levels()
        return [BookEvent(
            venue=VENUE, market_id=ids.qualify(VENUE, ticker), kind="snapshot", bids=bids, asks=asks,
            best_bid=book.best_bid, best_ask=book.best_ask, sequence=book.sequence, info={"market_id": body.get("market_id")},
        )]

    def _on_delta(self, message: dict[str, Any], body: dict[str, Any]) -> list[Event]:
        ticker = str(body.get("market_ticker") or "")
        book = self.books.get(ticker)
        seq = message.get("seq")
        if book is None or not book.ready:
            if book is not None:
                book.sequence = seq
            return []
        price = D(body["price_dollars"])
        delta = D(body["delta_fp"])
        if body.get("side") == "yes":
            size = book.add("bid", price, delta)
            bids, asks = (BookLevel(price, size),), ()
        else:
            price = ONE - price
            size = book.add("ask", price, delta)
            bids, asks = (), (BookLevel(price, size),)
        book.sequence = seq
        book.timestamp = body.get("ts_ms")
        return [BookEvent(
            venue=VENUE, market_id=ids.qualify(VENUE, ticker), kind="delta", bids=bids, asks=asks,
            best_bid=book.best_bid, best_ask=book.best_ask, sequence=seq, timestamp=body.get("ts_ms"),
            info={"client_order_id": body["client_order_id"]} if body.get("client_order_id") else {},
        )]

    def _quote(self, body: dict[str, Any]) -> QuoteEvent:
        ticker = str(body.get("market_ticker") or "")
        return QuoteEvent(
            venue=VENUE, market_id=ids.qualify(VENUE, ticker),
            bid=maybe_D(body.get("yes_bid_dollars")), ask=maybe_D(body.get("yes_ask_dollars")),
            bid_size=maybe_D(body.get("yes_bid_size_fp")), ask_size=maybe_D(body.get("yes_ask_size_fp")),
            last=maybe_D(body.get("price_dollars")), volume=maybe_D(body.get("volume_fp")),
            open_interest=maybe_D(body.get("open_interest_fp")), timestamp=body.get("ts_ms"), info=body,
        )

    def _trade(self, body: dict[str, Any], message: dict[str, Any]) -> TradeEvent:
        ticker = str(body.get("market_ticker") or "")
        book_side = body.get("taker_book_side")
        return TradeEvent(
            venue=VENUE, market_id=ids.qualify(VENUE, ticker), id=body.get("trade_id"),
            price=D(body["yes_price_dollars"]), amount=D(body["count_fp"]),
            taker_side=Side.BUY if book_side == "bid" else Side.SELL if book_side == "ask" else None,
            timestamp=body.get("ts_ms"), info={**body, "seq": message.get("seq")},
        )

    def _lifecycle(self, body: dict[str, Any]) -> MarketStatusEvent:
        native = str(body.get("event_type") or "")
        state = LIFECYCLE_STATE.get(native, "updated")
        if native in ("activated", "deactivated") and "is_deactivated" in body:
            state = "paused" if body["is_deactivated"] else "open"
        stamp = body.get("settled_ts") or body.get("determination_ts")
        return MarketStatusEvent(
            venue=VENUE, market_id=ids.qualify(VENUE, str(body.get("market_ticker") or "")), state=state, native=native,  # type: ignore[arg-type]
            result=body.get("result") or None, timestamp=int(stamp) * 1000 if stamp else None, info=body,
        )


def _tickers(market_ids: list[str] | None) -> list[str] | None:
    return None if market_ids is None else [ids.native(VENUE, m) for m in market_ids]
