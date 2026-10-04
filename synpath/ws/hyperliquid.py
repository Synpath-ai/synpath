"""Hyperliquid's WebSocket: outcome books, prints and quotes, and an account's
orders and fills.

One public endpoint, `wss://api.hyperliquid.xyz/ws`, no key. Account
channels are keyed by wallet address and readable by anyone, so the user
stream needs an address and nothing else. Heartbeat is `{"method":"ping"}`,
answered by a `pong` message; the venue closes a connection that sends
nothing for a minute.

**Books.** `l2Book` sends the whole book (up to 20 levels a side) every time
it changes, never a delta, so every message is a snapshot and a missed one
costs nothing: the next one is complete. Books are kept on the YES coin and
`book(market, "no")` is the mirror, as the venue's own NO coin shows it.

**Prints and quotes.** `trades` is the public tape on the YES coin (every
print appears on both coins, mirrored). The venue opens a subscription by
replaying recent prints; those older than the subscription are dropped, so
a stop never fires on a print from before it was watching. `bbo` is the top
of book and `activeAssetCtx` the 24h figures; both become `QuoteEvent`s.

**Account.** `orderUpdates` and `userFills` follow the address on every
coin; records for anything but an outcome coin are dropped. `userFills`
opens with a snapshot of recent fills, which is reported, not replayed: the
engine already has those fills, and reading them again would book them
twice. Nothing is replayed after a reconnect, so one is reported with
`reconcile_required`.
"""
from __future__ import annotations

import os
from decimal import Decimal
from typing import Any

from .. import ids
from ..base import Capability
from ..errors import BadRequest
from ..trading.hyperliquid import fill_of, order_of, outcome_coin
from ..trading.types import Account, Side
from .base import (
    BookEvent, BookLevel, Event, FillEvent, LocalBook, OrderEvent, QuoteEvent, Stream, TradeEvent, VenueEvent,
    maybe_D, now_ms,
)

VENUE = "hyperliquid"
WS_URL = "wss://api.hyperliquid.xyz/ws"
TESTNET_WS_URL = "wss://api.hyperliquid-testnet.xyz/ws"
PING = '{"method":"ping"}'


def yes_coin(market_id: str) -> str:
    """The YES coin of a market: `hyperliquid:7544` -> `#75440`."""
    native = ids.native(VENUE, market_id)
    if not native.isdigit():
        raise BadRequest(f"{VENUE}: {native!r} is not an outcome id; a question's outcomes are the markets")
    return f"#{10 * int(native)}"


def levels_of(rows: Any) -> list[tuple[Decimal, Decimal]]:
    out = []
    for row in rows or []:
        price, size = maybe_D(row.get("px")), maybe_D(row.get("sz"))
        if price is not None and size is not None and size > 0:
            out.append((price, size))
    return out


class _HyperliquidStream(Stream):
    """The connection and subscription bookkeeping both streams share."""

    venue = VENUE
    app_ping = PING
    data_heartbeat = True

    def __init__(self, *, testnet: bool = False, url: str | None = None, ping_interval: float = 30.0, **kwargs: Any):
        super().__init__(url or (TESTNET_WS_URL if testnet else WS_URL), ping_interval=ping_interval, **kwargs)
        self.subscriptions: dict[str, dict[str, Any]] = {}
        """Every subscription asked for, keyed for de-duplication, sent again
        on every connect."""

    async def _subscribe(self, subscription: dict[str, Any]) -> None:
        key = repr(sorted(subscription.items()))
        if key in self.subscriptions:
            return
        self.subscriptions[key] = subscription
        await self.send({"method": "subscribe", "subscription": subscription})

    async def _unsubscribe(self, subscription: dict[str, Any]) -> None:
        key = repr(sorted(subscription.items()))
        if self.subscriptions.pop(key, None) is not None:
            await self.send({"method": "unsubscribe", "subscription": subscription})

    async def on_connect(self) -> None:
        for subscription in self.subscriptions.values():
            await self.send({"method": "subscribe", "subscription": subscription})

    def _common(self, channel: Any, message: dict[str, Any]) -> list[Event] | None:
        """Acknowledgements, pongs and refusals; `None` for anything else."""
        if channel in ("pong", "subscriptionResponse"):
            if channel == "subscriptionResponse":
                sub = (message.get("data") or {}).get("subscription") or {}
                self.status("subscribed", str(sub.get("type") or ""), key=str(sub.get("coin") or sub.get("user") or ""))
            return []
        if channel == "error":
            self.status("error", f"venue: {message.get('data')}")
            return []
        return None


class HyperliquidMarketStream(_HyperliquidStream):
    """Books, prints and quotes for outcome markets. No key.

    ```python
    async with HyperliquidMarketStream() as stream:
        await stream.watch_order_book(["hyperliquid:7544"])
        async for event in stream:
            ...
    ```
    """

    name = "market"
    has: dict[str, Capability] = {
        "watch_order_book": True,
        "watch_trades": True,
        # Top of book from `bbo`, 24h volume from the asset context.
        "watch_ticker": True,
        # No lifecycle channel: an outcome leaves the listing when it settles.
        "watch_market_status": False,
    }

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self.books: dict[str, LocalBook] = {}
        """YES coin -> its book."""
        self.markets: dict[str, str] = {}
        """YES coin -> Synpath market id."""
        self.trades_since: dict[str, int] = {}
        """YES coin -> when its prints were last subscribed, in ms: the
        replayed prints before it are dropped."""

    def _track(self, market_id: str) -> str:
        coin = yes_coin(market_id)
        self.markets[coin] = ids.qualify(VENUE, ids.native(VENUE, market_id))
        return coin

    async def watch_order_book(self, market_ids: list[str]) -> None:
        """The book of each market, whole on every change."""
        for market_id in market_ids:
            coin = self._track(market_id)
            self.books.setdefault(coin, LocalBook())
            await self._subscribe({"type": "l2Book", "coin": coin})

    async def watch_trades(self, market_ids: list[str]) -> None:
        """Prints from now on, in the YES price."""
        for market_id in market_ids:
            coin = self._track(market_id)
            self.trades_since.setdefault(coin, now_ms())
            await self._subscribe({"type": "trades", "coin": coin})

    async def watch_ticker(self, market_ids: list[str]) -> None:
        """Top of book as it changes, and the 24h volume."""
        for market_id in market_ids:
            coin = self._track(market_id)
            await self._subscribe({"type": "bbo", "coin": coin})
            await self._subscribe({"type": "activeAssetCtx", "coin": coin})

    async def unwatch(self, market_ids: list[str]) -> None:
        for market_id in market_ids:
            coin = yes_coin(market_id)
            for kind in ("l2Book", "trades", "bbo", "activeAssetCtx"):
                await self._unsubscribe({"type": kind, "coin": coin})
            self.books.pop(coin, None)
            self.markets.pop(coin, None)
            self.trades_since.pop(coin, None)

    def book(self, market_id: str, side: str = "yes") -> LocalBook | None:
        """The local book on either side; `None` if not watched."""
        book = self.books.get(yes_coin(market_id))
        if book is None:
            return None
        return book if side != "no" else book.mirrored()

    async def on_connect(self) -> None:
        # A reconnect replays recent prints again; only the new ones count.
        for coin in self.trades_since:
            self.trades_since[coin] = now_ms()
        await super().on_connect()

    def on_disconnect(self) -> None:
        for book in self.books.values():
            book.invalidate()

    # -- reading --------------------------------------------------------------

    def handle(self, message: Any) -> list[Event]:
        if not isinstance(message, dict):
            return []
        channel = message.get("channel")
        common = self._common(channel, message)
        if common is not None:
            return common
        data = message.get("data")
        if channel == "l2Book" and isinstance(data, dict):
            return self._on_book(data)
        if channel == "trades" and isinstance(data, list):
            return [event for row in data if (event := self._trade(row)) is not None]
        if channel == "bbo" and isinstance(data, dict):
            return self._on_bbo(data)
        if channel in ("activeSpotAssetCtx", "activeAssetCtx") and isinstance(data, dict):
            return self._on_ctx(data)
        return []

    def _on_book(self, data: dict[str, Any]) -> list[Event]:
        coin = str(data.get("coin") or "")
        book = self.books.get(coin)
        if book is None:
            return []
        rows = data.get("levels") or [[], []]
        recovering = not book.ready and book.timestamp is not None
        book.replace(levels_of(rows[0] if rows else []), levels_of(rows[1] if len(rows) > 1 else []))
        book.timestamp = int(data["time"]) if data.get("time") else None
        if recovering:
            self.status("resynced", f"book {self.markets.get(coin, coin)}", key=self.markets.get(coin))
        bids, asks = book.levels()
        return [BookEvent(
            venue=VENUE, market_id=self.markets.get(coin, coin), side="yes", kind="snapshot",
            bids=bids, asks=asks, best_bid=book.best_bid, best_ask=book.best_ask, timestamp=book.timestamp,
        )]

    def _trade(self, row: dict[str, Any]) -> TradeEvent | None:
        coin = str(row.get("coin") or "")
        market_id = self.markets.get(coin)
        if market_id is None:
            return None
        stamp = int(row["time"]) if row.get("time") else None
        if stamp is not None and stamp < self.trades_since.get(coin, 0):
            return None
        side = row.get("side")
        return TradeEvent(
            venue=VENUE, market_id=market_id, id=str(row.get("tid") or row.get("hash") or ""),
            price=Decimal(str(row["px"])), amount=Decimal(str(row["sz"])),
            taker_side=Side.BUY if side == "B" else Side.SELL if side == "A" else None,
            timestamp=stamp, info=row,
        )

    def _on_bbo(self, data: dict[str, Any]) -> list[Event]:
        market_id = self.markets.get(str(data.get("coin") or ""))
        if market_id is None:
            return []
        bid, ask = (list(data.get("bbo") or []) + [None, None])[:2]
        return [QuoteEvent(
            venue=VENUE, market_id=market_id,
            bid=maybe_D(bid.get("px")) if bid else None, bid_size=maybe_D(bid.get("sz")) if bid else None,
            ask=maybe_D(ask.get("px")) if ask else None, ask_size=maybe_D(ask.get("sz")) if ask else None,
            timestamp=int(data["time"]) if data.get("time") else None, info=data,
        )]

    def _on_ctx(self, data: dict[str, Any]) -> list[Event]:
        market_id = self.markets.get(str(data.get("coin") or ""))
        if market_id is None:
            return []
        ctx = data.get("ctx") or {}
        return [QuoteEvent(venue=VENUE, market_id=market_id, volume=maybe_D(ctx.get("dayBaseVlm")), info=data)]


class HyperliquidUserStream(_HyperliquidStream):
    """One address's orders and fills on outcome coins.

    The venue serves any address's activity without a signature, so this
    takes the address only: `address=`, else `HYPERLIQUID_ACCOUNT_ADDRESS`.
    """

    name = "user"
    private = True
    has: dict[str, Capability] = {
        "watch_orders": True,
        "watch_my_trades": True,
        "watch_positions": False,
        "watch_balance": False,
    }

    def __init__(self, *, address: str | None = None, account_name: str = "default", **kwargs: Any):
        super().__init__(**kwargs)
        found = address or os.environ.get("HYPERLIQUID_ACCOUNT_ADDRESS")
        if not found:
            raise BadRequest(f"{VENUE}: the user stream needs the account's address -- pass address= "
                             f"or set HYPERLIQUID_ACCOUNT_ADDRESS")
        self.address = found.lower()
        self.account = Account(venue=VENUE, name=account_name)
        self.only: set[str] | None = None
        """Markets to report, or `None` for every outcome."""

    async def watch_orders(self, market_ids: list[str] | None = None) -> None:
        """This address's orders, on every outcome or only these markets."""
        self._narrow(market_ids)
        await self._subscribe({"type": "orderUpdates", "user": self.address})

    async def watch_my_trades(self, market_ids: list[str] | None = None) -> None:
        """This address's fills from now on; the venue's opening snapshot of
        recent fills is reported, not replayed."""
        self._narrow(market_ids)
        await self._subscribe({"type": "userFills", "user": self.address})

    def _narrow(self, market_ids: list[str] | None) -> None:
        if market_ids is None:
            self.only = None
        elif self.only is not None or not self.subscriptions:
            self.only = (self.only or set()) | {ids.qualify(VENUE, ids.native(VENUE, m)) for m in market_ids}

    def _wanted(self, market_id: str) -> bool:
        return self.only is None or market_id in self.only

    def handle(self, message: Any) -> list[Event]:
        if not isinstance(message, dict):
            return []
        channel = message.get("channel")
        common = self._common(channel, message)
        if common is not None:
            return common
        data = message.get("data")
        if channel == "orderUpdates" and isinstance(data, list):
            events: list[Event] = []
            for row in data:
                order = order_of(row, account=self.account)
                if order is not None and self._wanted(order.market_id):
                    events.append(OrderEvent(venue=VENUE, order=order, native=str(row.get("status") or "")))
            return events
        if channel == "userFills" and isinstance(data, dict):
            return self._on_fills(data)
        return []

    def _on_fills(self, data: dict[str, Any]) -> list[Event]:
        rows = data.get("fills") or []
        if data.get("isSnapshot"):
            outcome_rows = sum(1 for row in rows if outcome_coin(str(row.get("coin") or "")))
            self.status("subscribed", f"userFills: snapshot of {outcome_rows} recent outcome fills not replayed",
                        key="userFills")
            return []
        events: list[Event] = []
        for row in rows:
            coin = outcome_coin(str(row.get("coin") or ""))
            if coin is None or not self._wanted(ids.qualify(VENUE, coin[0])):
                continue
            fill = fill_of(row, account=self.account)
            if fill is None:
                # A split, a merge or a settlement: no trade, but the account moved.
                events.append(VenueEvent(venue=VENUE, name=str(row.get("dir") or "transfer"), payload=row,
                                         timestamp=int(row["time"]) if row.get("time") else None))
            else:
                events.append(FillEvent(venue=VENUE, fill=fill))
        return events
