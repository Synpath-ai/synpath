"""Polymarket US's retail WebSockets: markets and private.

Both live at `wss://api.polymarket.us/v1/ws/...` and authenticate the
handshake with the retail API key, signing `timestamp + GET + path` as REST
does. Subscriptions take at most 100 markets each; the stream splits larger
sets. The server sends `{"heartbeat": {}}` periodically, so a silent
connection is a dead one.

**Markets.** Full market data arrives as the whole visible book each time,
not as deltas, so there is nothing to fall out of step with: every message
is a snapshot for `{slug}:yes`. A change of market state (open, suspended,
halted, expired) is reported when it is first seen. Trades carry the maker
and taker sides but no sequence number.

**Private.** Orders (a snapshot of open orders, then executions), positions
and balances. Nothing is replayed after a reconnect; the order subscription
sends a fresh snapshot of open orders when it is made again, but fills and
position changes missed in between are gone, hence `reconcile_required`.

The documentation spells the envelopes two ways (`orderSubscriptionSnapshot`
and `ordersSnapshot`, and so on) and the venue's own SDK accepts both; so
does this stream.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from .. import ids
from ..base import Capability
from ..polymarket_us import parse_ts
from ..trading.credentials import PolymarketUSCredentials
from ..trading.polymarket_us import (
    PolymarketUSSigner, balance_of, order_of, position_of,
)
from ..trading.types import Account, Fill, Liquidity, Side
from .base import (
    BookEvent, D, Event, FillEvent, LocalBook, MarketStatusEvent, OrderEvent, PositionEvent, QuoteEvent, Stream,
    TradeEvent, BalanceEvent, maybe_D,
)

VENUE = "polymarket_us"
BASE_URL = "wss://api.polymarket.us"
MARKETS_PATH = "/v1/ws/markets"
PRIVATE_PATH = "/v1/ws/private"
MAX_SLUGS = 100

MARKET_STATE = {
    "MARKET_STATE_OPEN": "open",
    "MARKET_STATE_PREOPEN": "paused",
    "MARKET_STATE_SUSPENDED": "paused",
    "MARKET_STATE_HALTED": "paused",
    "MARKET_STATE_MATCH_AND_CLOSE_AUCTION": "closed",
    "MARKET_STATE_EXPIRED": "closed",
    "MARKET_STATE_TERMINATED": "settled",
}


def _first(message: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in message:
            return message[name]
    return None


class _SignedStream(Stream):
    path = ""
    data_heartbeat = True

    def __init__(self, credentials: PolymarketUSCredentials, *, base_url: str = BASE_URL, account_name: str = "default", **kwargs: Any):
        super().__init__(base_url + self.path, **kwargs)
        self.signer = PolymarketUSSigner(credentials.key_id, credentials.secret_key)
        self.account = Account(venue=VENUE, name=account_name)
        self._subscriptions: dict[str, dict[str, Any]] = {}
        self._next = 1

    def headers(self) -> dict[str, str]:
        return self.signer.headers("GET", self.path)

    async def _subscribe(self, kind: str, slugs: list[str] | None) -> None:
        chunks = [slugs[i:i + MAX_SLUGS] for i in range(0, len(slugs), MAX_SLUGS)] if slugs else [None]
        for chunk in chunks:
            request_id = f"{kind.lower().removeprefix('subscription_type_')}-{self._next}"
            self._next += 1
            body: dict[str, Any] = {"requestId": request_id, "subscriptionType": kind}
            if chunk:
                body["marketSlugs"] = chunk
            self._subscriptions[request_id] = body
            await self.send({"subscribe": body})

    async def on_connect(self) -> None:
        for body in self._subscriptions.values():
            await self.send({"subscribe": body})

    def _common(self, message: dict[str, Any]) -> list[Event] | None:
        if "heartbeat" in message:
            return []
        if "error" in message:
            self.status("error", str(message.get("error")), key=message.get("requestId") or message.get("request_id"))
            return []
        return None


class PolymarketUSMarketStream(_SignedStream):
    """Books, top of book and trades for Polymarket US markets."""

    venue = VENUE
    name = "markets"
    path = MARKETS_PATH
    has: dict[str, Capability] = {
        "watch_order_book": True,
        "watch_ticker": True,
        "watch_trades": True,
        # Market state rides on full market data; there is no lifecycle
        # channel for markets not subscribed.
        "watch_market_status": "partial",
    }

    def __init__(self, credentials: PolymarketUSCredentials, **kwargs: Any):
        super().__init__(credentials, **kwargs)
        self.books: dict[str, LocalBook] = {}
        self.states: dict[str, str] = {}

    async def watch_order_book(self, market_ids: list[str], *, debounced: bool = False) -> None:
        slugs = _slugs(market_ids)
        """Full visible book and market state for these markets."""
        for slug in slugs:
            self.books.setdefault(slug, LocalBook())
        await self._subscribe("SUBSCRIPTION_TYPE_MARKET_DATA", slugs)
        if debounced:
            for body in list(self._subscriptions.values())[-((len(slugs) - 1) // MAX_SLUGS + 1):]:
                body["responsesDebounced"] = True

    async def watch_ticker(self, market_ids: list[str]) -> None:
        await self._subscribe("SUBSCRIPTION_TYPE_MARKET_DATA_LITE", _slugs(market_ids))

    async def watch_trades(self, market_ids: list[str]) -> None:
        await self._subscribe("SUBSCRIPTION_TYPE_TRADE", _slugs(market_ids))

    def on_disconnect(self) -> None:
        for book in self.books.values():
            book.invalidate()

    def book(self, market_id: str, side: str = "yes") -> LocalBook | None:
        book = self.books.get(ids.native(VENUE, market_id))
        return None if book is None else book if side != "no" else book.mirrored()

    def handle(self, message: Any) -> list[Event]:
        if not isinstance(message, dict):
            return []
        common = self._common(message)
        if common is not None:
            return common
        data = _first(message, "marketData", "market_data")
        if data is not None:
            return self._market_data(data)
        lite = _first(message, "marketDataLite", "market_data_lite")
        if lite is not None:
            slug = str(lite.get("marketSlug") or "")
            return [QuoteEvent(
                venue=VENUE, market_id=ids.qualify(VENUE, slug),
                bid=maybe_D(lite.get("bestBid")), ask=maybe_D(lite.get("bestAsk")),
                last=maybe_D(lite.get("lastTradePx")), volume=maybe_D(lite.get("sharesTraded")),
                open_interest=maybe_D(lite.get("openInterest")), info=lite,
            )]
        trade = message.get("trade")
        if trade is not None:
            slug = str(trade.get("marketSlug") or "")
            taker = (trade.get("taker") or {}).get("side")
            return [TradeEvent(
                venue=VENUE, market_id=ids.qualify(VENUE, slug), id=trade.get("tradeId") or trade.get("id"),
                price=D(trade["price"]), amount=D(trade.get("quantity") or trade.get("qty")),
                taker_side=Side.BUY if taker == "ORDER_SIDE_BUY" else Side.SELL if taker == "ORDER_SIDE_SELL" else None,
                timestamp=parse_ts(trade.get("tradeTime")), info=trade,
            )]
        return []

    def _market_data(self, data: dict[str, Any]) -> list[Event]:
        slug = str(data.get("marketSlug") or "")
        book = self.books.setdefault(slug, LocalBook())
        book.replace(
            [(D(level["px"]), D(level["qty"])) for level in data.get("bids") or []],
            [(D(level["px"]), D(level["qty"])) for level in data.get("offers") or []],
        )
        stamp = parse_ts(data.get("transactTime"))
        book.timestamp = stamp
        bids, asks = book.levels()
        events: list[Event] = [BookEvent(
            venue=VENUE, market_id=ids.qualify(VENUE, slug), kind="snapshot", bids=bids, asks=asks,
            best_bid=book.best_bid, best_ask=book.best_ask, timestamp=stamp,
            info={"state": data.get("state"), "stats": data.get("stats")},
        )]
        native = data.get("state")
        if native and self.states.get(slug) != native:
            self.states[slug] = native
            events.append(MarketStatusEvent(
                venue=VENUE, market_id=ids.qualify(VENUE, slug), state=MARKET_STATE.get(native, "updated"),  # type: ignore[arg-type]
                native=native, timestamp=stamp, info={"state": native},
            ))
        return events


class PolymarketUSPrivateStream(_SignedStream):
    """This account's orders, fills, positions and balance."""

    venue = VENUE
    name = "private"
    path = PRIVATE_PATH
    private = True
    has: dict[str, Capability] = {
        "watch_orders": True,
        "watch_my_trades": True,
        "watch_positions": True,
        "watch_balance": True,
    }

    async def watch_orders(self, market_ids: list[str] | None = None) -> None:
        slugs = _slugs(market_ids)
        """Open orders as a snapshot, then every execution: acceptance, fills,
        cancels, replaces, rejects. Fills arrive as `FillEvent`s too."""
        await self._subscribe("SUBSCRIPTION_TYPE_ORDER", slugs)

    watch_my_trades = watch_orders

    async def watch_positions(self, market_ids: list[str] | None = None) -> None:
        await self._subscribe("SUBSCRIPTION_TYPE_POSITION", _slugs(market_ids))

    async def watch_balance(self) -> None:
        await self._subscribe("SUBSCRIPTION_TYPE_ACCOUNT_BALANCE", None)

    def handle(self, message: Any) -> list[Event]:
        if not isinstance(message, dict):
            return []
        common = self._common(message)
        if common is not None:
            return common
        snapshot = _first(message, "orderSubscriptionSnapshot", "ordersSnapshot")
        if snapshot is not None:
            return [OrderEvent(venue=VENUE, order=order_of(row, account=self.account), native="snapshot") for row in snapshot.get("orders") or []]
        update = _first(message, "orderSubscriptionUpdate", "orderUpdate")
        if update is not None:
            return self._execution(update.get("execution") or update)
        position = _first(message, "positionSubscription", "positionSubscriptionUpdate", "positionUpdate")
        if position is not None:
            return self._position(position)
        positions = _first(message, "positionSubscriptionSnapshot", "positionsSnapshot")
        if positions is not None:
            rows = positions.get("positions") or {}
            return [PositionEvent(venue=VENUE, position=position_of(slug, row, account=self.account)) for slug, row in rows.items()]
        balances = _first(
            message, "accountBalancesSnapshot", "accountBalanceSubscriptionSnapshot",
            "accountBalancesUpdate", "accountBalanceSubscriptionUpdate", "accountBalanceUpdate",
        )
        if balances is not None:
            return self._balance(balances)
        return []

    def _execution(self, execution: dict[str, Any]) -> list[Event]:
        raw_order = execution.get("order") or {}
        order = order_of(raw_order, account=self.account)
        kind = str(execution.get("type") or "")
        events: list[Event] = [OrderEvent(venue=VENUE, order=order, native=kind)]
        if kind in ("EXECUTION_TYPE_PARTIAL_FILL", "EXECUTION_TYPE_FILL") and execution.get("lastShares"):
            wire_price = maybe_D(execution.get("lastPx"))
            aggressor = execution.get("aggressor")
            events.append(FillEvent(venue=VENUE, fill=Fill(
                id=str(execution.get("tradeId") or execution.get("id") or ""), order_id=order.id, venue=VENUE,
                account=self.account, market_id=order.market_id, side=order.side,
                price=wire_price if wire_price is not None else Decimal("0"),
                amount=D(execution["lastShares"]),
                fee=maybe_D(execution.get("commissionNotionalCollected")), fee_currency="USD",
                liquidity=Liquidity.TAKER if aggressor is True else Liquidity.MAKER if aggressor is False else Liquidity.UNKNOWN,
                timestamp=parse_ts(execution.get("transactTime")) or 0, info=execution,
            )))
        return events

    def _position(self, body: dict[str, Any]) -> list[Event]:
        row = body.get("afterPosition") or body.get("position") or {}
        slug = str(body.get("marketSlug") or (row.get("marketMetadata") or {}).get("slug") or "")
        position = position_of(slug, row, account=self.account)
        stamp = parse_ts(body.get("updateTime"))
        return [PositionEvent(venue=VENUE, position=position.model_copy(update={
            "timestamp": stamp or position.timestamp, "info": body,
        }))]

    def _balance(self, body: dict[str, Any]) -> list[Event]:
        if "balances" in body:
            return [BalanceEvent(venue=VENUE, balance=balance_of(body, account=self.account))]
        after = (body.get("balanceChange") or {}).get("afterBalance")
        if after is not None:
            return [BalanceEvent(venue=VENUE, balance=balance_of({"balances": [after]}, account=self.account))]
        if "balance" in body or "buyingPower" in body:
            row = {"currentBalance": body.get("balance"), "buyingPower": body.get("buyingPower"), "currency": "USD"}
            return [BalanceEvent(venue=VENUE, balance=balance_of({"balances": [row]}, account=self.account))]
        return []


def _slugs(market_ids: list[str] | None) -> list[str] | None:
    return None if market_ids is None else [ids.native(VENUE, m) for m in market_ids]
