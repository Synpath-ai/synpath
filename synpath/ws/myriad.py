"""Myriad's WebSocket: order books, prices, the tape, market changes, and an account's orders.

One endpoint, `wss://ws.myriadprotocol.com/ws`, speaking Centrifugo's JSON
protocol directly: a `connect` command (anonymous: empty), one `subscribe`
per channel, publications as `push` messages, and the server's empty `{}`
pings answered with `{}`. Several messages can share one frame, a line each.

**Public channels need nothing.** `orderbook:{network}:{market}`,
`prices:...`, `trades:...` and `markets:{network}` are open to anyone, as is
`trades:{network}:*:{wallet}` (one wallet's matches on every market). The
account's `orders:` and `positions:` channels each need a short-lived
subscription token from the REST API, for the signed-in wallet's own
channels only; the user stream asks the trading adapter for one per channel.

**Books.** An order-book delta carries the new total at each changed price
level, for both outcomes; `"0"` removes the level. Outcome 0's book is YES and
outcome 1's NO, each already holding what the other side's orders imply.
Both are seeded from the REST snapshot once the subscription is confirmed;
deltas that arrive before it are replayed on top, which is safe because each
one is a level's absolute size. Prices are 1e18 fractions, sizes 18-decimal
shares. Publications are coalesced into windows of about 100 ms.

Order books only: AMM markets publish nothing here.
"""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any

from .. import ids
from ..base import Capability
from ..errors import AuthenticationError, BadRequest
from ..trading.types import Side
from .base import (
    BookEvent, BookLevel, Event, FillEvent, LocalBook, MarketStatusEvent, OrderEvent, PositionEvent, QuoteEvent, Stream,
    TradeEvent, now_ms,
)

VENUE = "myriad"
WS_URL = "wss://ws.myriadprotocol.com/ws"
SCALE = Decimal(10) ** 18
MAX_CHANNELS = 512


def ref_of(market_id: str) -> tuple[int, int]:
    """`(network, market)` of an order-book market id."""
    from ..myriad import parse_id

    native = ids.native(VENUE, market_id)
    ref = parse_id(native)
    if ref is None:
        raise BadRequest(f"{VENUE}: streams take a market id (myriad:56-2385), not a slug: {native!r}")
    if ref.amm:
        raise BadRequest(f"{VENUE}: {native} is an AMM market; the venue streams order books only")
    return ref.network, ref.market


def market_id_of(network: Any, market: Any) -> str:
    return ids.qualify(VENUE, f"{int(network)}-{int(market)}")


def _wei(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(int(str(value))) / SCALE
    except ValueError:
        return None


def _price(value: Any) -> Decimal | None:
    """A 1e18 price, rid of the float artifacts the venue sometimes sends
    (`329999999999999936`)."""
    price = _wei(value)
    return price.quantize(Decimal("0.000001")) if price is not None else None


def book_levels(rows: Any) -> list[tuple[Decimal, Decimal]]:
    """REST `[[price, amount], ...]` as (price, shares) levels."""
    out = []
    for row in rows or []:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            price, size = _price(row[0]), _wei(row[1])
            if price is not None and size is not None and size > 0:
                out.append((price, size))
    return out


def trade_event(data: dict[str, Any]) -> TradeEvent | None:
    """One match from `trades:`, as the taker's print in the YES price."""
    taker = data.get("taker") or {}
    price, quantity = _price(taker.get("averagePrice")), _wei(taker.get("totalAmount"))
    if price is None or not quantity or data.get("marketId") is None:
        return None
    side = str(taker.get("side") or "").lower()
    buying = side in ("buy", "split")
    if int(taker.get("outcome") or 0) == 1:
        price, buying = Decimal("1") - price, not buying
    return TradeEvent(
        venue=VENUE, market_id=market_id_of(data.get("networkId"), data["marketId"]), id=data.get("txHash"),
        price=price, quantity=quantity, taker_side=Side.BUY if buying else Side.SELL,
        timestamp=int(data["ts"]) if data.get("ts") else None, info=data,
    )


def status_event(data: dict[str, Any]) -> MarketStatusEvent | None:
    """A `markets:` event (created, resolved) as the market's new state."""
    if data.get("marketId") is None:
        return None
    kind = str(data.get("type") or "")
    state = {"created": "created", "resolved": "settled"}.get(kind, "updated")
    return MarketStatusEvent(
        venue=VENUE, market_id=market_id_of(data.get("networkId"), data["marketId"]), state=state,  # type: ignore[arg-type]
        native=kind or None, timestamp=int(data["ts"]) if data.get("ts") else None, info=data,
    )


class _CentrifugoStream(Stream):
    """Centrifugo's JSON protocol: connect, subscribe (with a per-channel
    token where the channel needs one), ping."""

    venue = VENUE
    data_heartbeat = True

    def __init__(self, *, url: str = WS_URL, **kwargs: Any):
        super().__init__(url, **kwargs)
        self.ready = False
        self.channels: set[str] = set()
        self._live: set[str] = set()
        self._next_id = 0
        self._pending: dict[int, tuple[str, str]] = {}
        self._pong = True

    def _command(self, kind: str, body: dict[str, Any], *, channel: str = "") -> dict[str, Any]:
        self._next_id += 1
        self._pending[self._next_id] = (kind, channel)
        return {"id": self._next_id, kind: body}

    async def on_connect(self) -> None:
        self.ready = False
        await self.send(self._command("connect", {}))

    def on_disconnect(self) -> None:
        self.ready = False
        self._live.clear()
        self._pending.clear()

    def handle_raw(self, raw: str) -> list[Event] | None:
        events: list[Event] = []
        for line in raw.split("\n"):
            if not line.strip():
                continue
            try:
                message = json.loads(line)
            except ValueError:
                continue
            if isinstance(message, dict):
                events += self._message(message)
        return events

    def _message(self, message: dict[str, Any]) -> list[Event]:
        if not message:
            if self._pong:
                self._later(self.send({}))
            return []
        if "id" in message:
            kind, channel = self._pending.pop(int(message["id"]), ("", ""))
            if message.get("error"):
                error = message["error"]
                self.status("error", f"venue refused {kind} {channel}: {error.get('code')} {error.get('message')}".strip())
                if kind == "subscribe":
                    self._live.discard(channel)
                return []
            if kind == "connect":
                self.ready = True
                self._pong = bool((message.get("connect") or {}).get("pong", True))
                self._later(self.resubscribe())
            elif kind == "subscribe":
                self.status("subscribed", channel, key=channel)
                return self.on_subscribed(channel)
            return []
        push = message.get("push")
        if isinstance(push, dict):
            channel = str(push.get("channel") or "")
            publication = push.get("pub")
            if isinstance(publication, dict) and channel:
                return self.on_publication(channel, publication.get("data"))
            if push.get("disconnect"):
                self.status("error", f"venue disconnected the stream: {push['disconnect']}")
        return []

    async def channel_token(self, channel: str) -> str | None:
        """The subscription token a channel needs, or `None` for a public one."""
        return None

    async def resubscribe(self) -> None:
        """Send every channel not yet live on this connection."""
        if not self.ready:
            return
        for channel in sorted(self.channels - self._live):
            if len(self._live) >= MAX_CHANNELS:
                self.status("error", f"more than {MAX_CHANNELS} channels on one connection; {channel} not subscribed")
                return
            try:
                token = await self.channel_token(channel)
            except Exception as exc:
                self.status("error", f"no subscription token for {channel}: {type(exc).__name__}: {exc}")
                continue
            self._live.add(channel)
            body: dict[str, Any] = {"channel": channel}
            if token:
                body["token"] = token
            await self.send(self._command("subscribe", body, channel=channel))

    async def add_channels(self, channels: list[str]) -> None:
        self.channels.update(channels)
        await self.resubscribe()

    async def drop_channels(self, channels: list[str]) -> None:
        for channel in channels:
            self.channels.discard(channel)
            if channel in self._live and self.ready:
                self._live.discard(channel)
                await self.send(self._command("unsubscribe", {"channel": channel}, channel=channel))

    def on_subscribed(self, channel: str) -> list[Event]:
        return []

    def on_publication(self, channel: str, data: Any) -> list[Event]:
        return []

    def _later(self, coroutine: Any) -> asyncio.Task | None:
        try:
            return asyncio.get_running_loop().create_task(coroutine)
        except RuntimeError:
            coroutine.close()
            return None


class MyriadMarketStream(_CentrifugoStream):
    """Books, prices, the tape and market changes for Myriad order-book
    markets. Public: no key needed.

    ```python
    async with MyriadMarketStream() as stream:
        await stream.watch_order_book(["myriad:56-2385"])
        async for event in stream:
            ...
    ```
    """

    name = "market"
    has: dict[str, Capability] = {
        "watch_order_book": True,
        "watch_ticker": True,
        "watch_trades": True,
        # Markets created and resolved, per network.
        "watch_market_status": "partial",
    }

    def __init__(self, *, catalog: Any = None, **kwargs: Any):
        super().__init__(**kwargs)
        self.catalog = catalog
        """A `Myriad` for seeding books from REST; made when first needed."""
        self.books: dict[tuple[str, int], LocalBook] = {}
        """(market id, outcome) -> that outcome's book: 0 is YES, 1 is NO."""
        self._pending_deltas: dict[str, list[list[dict[str, Any]]]] = {}
        """Deltas that arrived before a market's snapshot, replayed on it."""
        self.tickers: set[str] = set()
        self.trades: set[str] = set()
        self.statuses: set[str] | None = None

    # -- intents --------------------------------------------------------------

    async def watch_order_book(self, market_ids: list[str]) -> None:
        """Each market's YES and NO books, seeded from REST and kept by deltas."""
        channels = []
        for market_id in market_ids:
            network, market = ref_of(market_id)
            key = market_id_of(network, market)
            for outcome in (0, 1):
                self.books.setdefault((key, outcome), LocalBook())
            channels.append(f"orderbook:{network}:{market}")
        await self.add_channels(channels)

    async def watch_ticker(self, market_ids: list[str]) -> None:
        """Best bid, ask and last on each outcome, on every change."""
        channels = []
        for market_id in market_ids:
            network, market = ref_of(market_id)
            self.tickers.add(market_id_of(network, market))
            channels.append(f"prices:{network}:{market}")
        await self.add_channels(channels)

    async def watch_trades(self, market_ids: list[str]) -> None:
        """Every match on these markets, as the taker's print."""
        channels = []
        for market_id in market_ids:
            network, market = ref_of(market_id)
            self.trades.add(market_id_of(network, market))
            channels.append(f"trades:{network}:{market}")
        await self.add_channels(channels)

    async def watch_market_status(self, market_ids: list[str] | None = None, *, network: int = 56) -> None:
        """Markets created and resolved on a network, or only these markets'."""
        if market_ids:
            self.statuses = (self.statuses or set()) | {market_id_of(*ref_of(m)) for m in market_ids}
            networks = {ref_of(m)[0] for m in market_ids}
        else:
            self.statuses = set()
            networks = {network}
        await self.add_channels([f"markets:{n}" for n in sorted(networks)])

    async def unwatch(self, market_ids: list[str]) -> None:
        dropped = []
        for market_id in market_ids:
            network, market = ref_of(market_id)
            key = market_id_of(network, market)
            for outcome in (0, 1):
                self.books.pop((key, outcome), None)
            self._pending_deltas.pop(key, None)
            self.tickers.discard(key)
            self.trades.discard(key)
            dropped += [f"orderbook:{network}:{market}", f"prices:{network}:{market}", f"trades:{network}:{market}"]
        await self.drop_channels(dropped)

    def book(self, market_id: str, side: str = "yes") -> LocalBook | None:
        network, market = ref_of(market_id)
        return self.books.get((market_id_of(network, market), 1 if side == "no" else 0))

    # -- messages -------------------------------------------------------------

    def on_disconnect(self) -> None:
        super().on_disconnect()
        self._pending_deltas.clear()
        for book in self.books.values():
            book.invalidate()

    def on_subscribed(self, channel: str) -> list[Event]:
        if channel.startswith("orderbook:"):
            _, network, market = channel.split(":")
            key = market_id_of(network, market)
            self._pending_deltas[key] = []
            self._later(self._seed(key, int(network), int(market)))
        return []

    def _catalog(self) -> Any:
        if self.catalog is None:
            from ..myriad import Myriad

            self.catalog = Myriad()
        return self.catalog

    async def _seed(self, key: str, network: int, market: int) -> None:
        """Both outcomes' REST snapshots, with any deltas since replayed on them."""
        try:
            catalog = self._catalog()
            snapshots = [
                await asyncio.to_thread(catalog._get, f"/markets/{market}/orderbook",
                                        {"network_id": network, "outcome": outcome})
                for outcome in (0, 1)
            ]
        except Exception as exc:  # pragma: no cover - the venue's API down
            self.status("error", f"could not seed book {key}: {type(exc).__name__}: {exc}")
            return
        for event in self.apply_snapshot(key, snapshots):
            self.emit(event)

    def apply_snapshot(self, key: str, snapshots: list[Any]) -> list[Event]:
        """Seed both books from `[outcome 0, outcome 1]` REST books and replay
        the deltas held for them."""
        buffered = self._pending_deltas.pop(key, [])
        events: list[Event] = []
        for outcome, payload in enumerate(snapshots):
            book = self.books.get((key, outcome))
            if book is None or not isinstance(payload, dict):
                continue
            recovering = not book.ready and book.timestamp is not None
            book.replace(book_levels(payload.get("bids")), book_levels(payload.get("asks")))
            for changes in buffered:
                for change in changes:
                    self._set(book, outcome, change)
            book.timestamp = now_ms()
            if recovering:
                self.status("resynced", f"book {key}", key=key)
            bids, asks = book.levels()
            events.append(BookEvent(
                venue=VENUE, market_id=key, side="yes" if outcome == 0 else "no", kind="snapshot",
                bids=bids, asks=asks, best_bid=book.best_bid, best_ask=book.best_ask,
                info={"replayed": len(buffered)},
            ))
        return events

    @staticmethod
    def _set(book: Any, outcome: int, change: dict[str, Any]) -> tuple[Decimal, Decimal, str] | None:
        if int(change.get("outcome") or 0) != outcome:
            return None
        side = "bid" if str(change.get("side")) == "bid" else "ask"
        price, size = _price(change.get("price")), _wei(change.get("amount"))
        if price is None or size is None:
            return None
        book.set(side, price, size)
        return price, size, side

    def on_publication(self, channel: str, data: Any) -> list[Event]:
        if not isinstance(data, dict):
            return []
        if channel.startswith("orderbook:"):
            return self._on_delta(data)
        if channel.startswith("prices:"):
            return self._on_prices(data)
        if channel.startswith("trades:"):
            event = trade_event(data)
            return [event] if event and event.market_id in self.trades else []
        if channel.startswith("markets:") and self.statuses is not None:
            event = status_event(data)
            if event is None or (self.statuses and event.market_id not in self.statuses):
                return []
            return [event]
        return []

    def _on_delta(self, data: dict[str, Any]) -> list[Event]:
        if data.get("marketId") is None:
            return []
        key = market_id_of(data.get("networkId"), data["marketId"])
        changes = [c for c in data.get("changes") or [] if isinstance(c, dict)]
        if key in self._pending_deltas:
            self._pending_deltas[key].append(changes)
            return []
        events: list[Event] = []
        stamp = int(data["ts"]) if data.get("ts") else None
        for outcome in (0, 1):
            book = self.books.get((key, outcome))
            if book is None or not book.ready:
                continue
            bids, asks = [], []
            for change in changes:
                applied = self._set(book, outcome, change)
                if applied is None:
                    continue
                price, size, side = applied
                (bids if side == "bid" else asks).append(BookLevel(price, size))
            if not bids and not asks:
                continue
            book.timestamp = now_ms()
            events.append(BookEvent(
                venue=VENUE, market_id=key, side="yes" if outcome == 0 else "no", kind="delta",
                bids=tuple(sorted(bids, key=lambda level: level.price, reverse=True)),
                asks=tuple(sorted(asks, key=lambda level: level.price)),
                best_bid=book.best_bid, best_ask=book.best_ask, timestamp=stamp,
            ))
        return events

    def _on_prices(self, data: dict[str, Any]) -> list[Event]:
        if data.get("marketId") is None:
            return []
        key = market_id_of(data.get("networkId"), data["marketId"])
        if key not in self.tickers:
            return []
        stamp = int(data["ts"]) if data.get("ts") else None
        events: list[Event] = []
        for row in data.get("outcomes") or []:
            if not isinstance(row, dict):
                continue
            outcome = int(row.get("outcome") or 0)
            events.append(QuoteEvent(
                venue=VENUE, market_id=key, side="yes" if outcome == 0 else "no",
                bid=_price(row.get("bestBid")), ask=_price(row.get("bestAsk")), last=_price(row.get("last")),
                timestamp=stamp, info=row,
            ))
        return events


class MyriadUserStream(_CentrifugoStream):
    """An account's orders, fills and share balances on the order book.

    Orders and positions are the wallet's own channels: each subscription
    carries a token from `token(channel)` (`MyriadTrading.subscription_token`,
    signed with the account's API key or session). Fills come from the public
    per-wallet tape, `trades:{network}:*:{wallet}`, with no token.

    A position update names one outcome's new balance; the position emitted
    nets it with the other outcome's last balance seen on this stream.
    """

    name = "user"
    private = True
    has: dict[str, Capability] = {
        "watch_orders": True,
        "watch_my_trades": True,
        # Balances as they change; the other outcome's only once it has changed too.
        "watch_positions": "partial",
    }

    def __init__(self, *, address: str, token: Any = None, network: int = 56, account_name: str = "default",
                 **kwargs: Any):
        if not address:
            raise AuthenticationError(f"{VENUE}: the account stream needs the wallet's address")
        super().__init__(**kwargs)
        from ..trading.types import Account

        self.address = address.lower()
        self.network = int(network)
        self._token = token
        """A callable (sync or async) taking a channel and returning its subscription token."""
        self.account = Account(venue=VENUE, name=account_name)
        self._balances: dict[tuple[str, int], Decimal] = {}

    async def channel_token(self, channel: str) -> str | None:
        if not (channel.startswith("orders:") or channel.startswith("positions:")):
            return None
        if self._token is None:
            raise AuthenticationError(f"{VENUE}: {channel} needs a subscription token; pass token= "
                                      f"(MyriadTrading.subscription_token)")
        value = self._token(channel)
        return await value if asyncio.iscoroutine(value) else value

    async def watch_orders(self, market_ids: list[str] | None = None) -> None:
        """Every order on the account; the venue has no per-market channel."""
        await self.add_channels([f"orders:{self.network}:{self.address}"])

    async def watch_my_trades(self, market_ids: list[str] | None = None) -> None:
        """Every match the wallet takes part in, on every market."""
        await self.add_channels([f"trades:{self.network}:*:{self.address}"])

    async def watch_positions(self, market_ids: list[str] | None = None) -> None:
        await self.add_channels([f"positions:{self.network}:{self.address}"])

    def on_publication(self, channel: str, data: Any) -> list[Event]:
        if not isinstance(data, dict):
            return []
        from ..trading.myriad import fills_of_trade, order_of_update

        if channel.startswith("orders:"):
            order = order_of_update(data, account=self.account)
            return [OrderEvent(venue=VENUE, order=order, native=str(data.get("type") or "") or None)]
        if channel.startswith("trades:"):
            return [FillEvent(venue=VENUE, fill=fill) for fill in fills_of_trade(data, address=self.address, account=self.account)]
        if channel.startswith("positions:"):
            return self._on_position(data)
        return []

    def _on_position(self, data: dict[str, Any]) -> list[Event]:
        from ..trading.types import Position, PositionSide

        if data.get("marketId") is None or data.get("balance") in (None, ""):
            return []
        key = market_id_of(data.get("networkId"), data["marketId"])
        outcome = int(data.get("outcome") or 0)
        self._balances[(key, outcome)] = _wei(data.get("balance")) or Decimal("0")
        held_yes, held_no = self._balances.get((key, 0)), self._balances.get((key, 1))
        net = (held_yes or Decimal("0")) - (held_no or Decimal("0"))
        average = _price(data.get("averageEntryPrice"))
        return [PositionEvent(venue=VENUE, position=Position(
            venue=VENUE, account=self.account, market_id=key,
            side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
            contracts=abs(net), inventory_yes=held_yes, inventory_no=held_no,
            entry_price=(average if outcome == 0 else Decimal("1") - average) if average is not None and (
                (outcome == 0) == (net >= 0)) else None,
            timestamp=int(data["ts"]) if data.get("ts") else None, info=data,
        ))]
