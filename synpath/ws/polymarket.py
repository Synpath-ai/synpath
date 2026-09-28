"""Polymarket's CLOB WebSockets: the public market channel and the user channel.

**Market channel.** Subscribing to token ids brings a `book` snapshot for
each, then `price_change` messages that set a level's absolute size. There
are no sequence numbers. What there is: every change carries the venue's
best bid and ask for that token. Those figures describe the book after
*every* change with the same timestamp -- one trade can arrive as two
messages stamped alike, and the first message's top of book already
reflects the second -- so the stream applies a timestamp's changes and
checks its own top of book against the venue's only once a later timestamp
arrives for that token (or half a second passes on the feed). A
disagreement means a message was missed: the stream reports a gap and
resubscribes the token, which brings a fresh snapshot. Heartbeat is the
text frame `PING` every ten seconds.

**User channel.** Authenticated with the CLOB API credentials in the
subscription frame. Order events (placement, update, cancellation) and trade
events that repeat as a trade's settlement moves from `MATCHED` through
`MINED` to `CONFIRMED` or `FAILED`. Nothing is replayed after a reconnect.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx

from .. import ids
from ..base import Capability
from ..trading.polymarket import GAMMA_URL, MarketTokens, fills_of, order_of
from ..trading.types import Account, OrderStatus, Side
from .base import (
    ONE,
    BookEvent, BookLevel, D, Event, FillEvent, LocalBook, MarketStatusEvent, OrderEvent, QuoteEvent, Stream,
    TradeEvent, VenueEvent, maybe_D,
)

VENUE = "polymarket"
MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
USER_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"


class PolymarketMarketStream(Stream):
    """Books, trades and market lifecycle for a set of tokens. No credentials."""

    venue = VENUE
    name = "market"
    app_ping = "PING"
    data_heartbeat = True
    has: dict[str, Capability] = {
        "watch_order_book": True,
        "watch_ticker": True,
        "watch_trades": True,
        # New markets and resolutions, for the tokens subscribed; the venue
        # publishes no pause or halt on this channel.
        "watch_market_status": "partial",
    }

    def __init__(self, *, url: str = MARKET_URL, gamma_url: str = GAMMA_URL, **kwargs: Any):
        super().__init__(url, **kwargs)
        self.tokens: set[str] = set()
        self.books: dict[str, LocalBook] = {}
        self.markets: dict[str, str] = {}
        """Token id -> condition id, learned from the stream."""
        self.catalog = TokenCatalog(gamma_url)
        """Which token is which side of which market, from the Gamma catalog."""
        self._unverified: dict[str, tuple[int | None, Any, Any]] = {}
        """Token -> (timestamp, venue best bid, venue best ask) not yet checked."""

    async def watch_order_book(self, market_ids: list[str]) -> None:
        """Books, trades, top of book and lifecycle for these markets, both
        sides: the market channel sends all of them for whatever token it is
        subscribed to. Each market not yet known costs one catalog read."""
        token_ids = []
        for market_id in market_ids:
            tokens = await self.catalog.resolve(market_id)
            token_ids += [tokens.yes_token, tokens.no_token]
        await self.watch_tokens(token_ids)

    async def watch_tokens(self, token_ids: list[str]) -> None:
        """Subscribe by CLOB token id, for a caller that already holds them."""
        new = [t for t in token_ids if t not in self.tokens]
        if not new:
            return
        self.tokens.update(new)
        for token in new:
            self.books.setdefault(token, LocalBook())
        await self.send({"assets_ids": new, "operation": "subscribe"})

    watch_trades = watch_order_book
    watch_ticker = watch_order_book

    async def unwatch(self, market_ids: list[str]) -> None:
        token_ids = []
        for market_id in market_ids:
            tokens = await self.catalog.resolve(market_id)
            token_ids += [tokens.yes_token, tokens.no_token]
        gone = [t for t in token_ids if t in self.tokens]
        self.tokens.difference_update(gone)
        for token in gone:
            self.books.pop(token, None)
        if gone:
            await self.send({"assets_ids": gone, "operation": "unsubscribe"})

    async def on_connect(self) -> None:
        if self.tokens:
            await self.send({"assets_ids": sorted(self.tokens), "type": "market", "custom_feature_enabled": True})

    def on_disconnect(self) -> None:
        self._unverified.clear()
        for book in self.books.values():
            book.invalidate()

    def book(self, market_id: str, side: str = "yes") -> LocalBook | None:
        """The local book for a market on either side; `None` if not watched.
        Both sides are real books here, not mirrors."""
        tokens = self.catalog.cached(market_id)
        if tokens is None:
            return None
        return self.books.get(tokens.token(side))

    def _where(self, token: str, condition: str) -> tuple[str, str]:
        """`(market_id, side)` for a token the stream reported."""
        tokens = self.catalog.cached(token)
        if tokens is not None:
            return tokens.market_id, tokens.outcome_of(token)
        return ids.qualify(VENUE, condition), "yes"

    # -- reading --------------------------------------------------------------

    def handle(self, message: Any) -> list[Event]:
        if isinstance(message, list):
            events: list[Event] = []
            for item in message:
                events.extend(self.handle(item))
            return events
        if not isinstance(message, dict):
            return []  # PONG
        kind = message.get("event_type")
        if kind == "book":
            return self._on_book(message)
        if kind == "price_change":
            return self._on_price_change(message)
        if kind == "last_trade_price":
            token = str(message.get("asset_id") or "")
            market_id, side = self._where(token, str(message.get("market") or ""))
            price = D(message["price"])
            taker = str(message.get("side") or "").upper()
            taker_side = Side.BUY if taker == "BUY" else Side.SELL if taker == "SELL" else None
            if side == "no":
                price = ONE - price
                taker_side = {Side.BUY: Side.SELL, Side.SELL: Side.BUY}.get(taker_side)  # type: ignore[arg-type]
            return [TradeEvent(
                venue=VENUE, market_id=market_id,
                id=message.get("transaction_hash") or None, price=price, amount=D(message["size"]),
                taker_side=taker_side, timestamp=_ms(message.get("timestamp")), info=message,
            )]
        if kind == "best_bid_ask":
            token = str(message.get("asset_id") or "")
            market_id, side = self._where(token, str(message.get("market") or ""))
            return [QuoteEvent(
                venue=VENUE, market_id=market_id, side=side,  # type: ignore[arg-type]
                bid=maybe_D(message.get("best_bid")), ask=maybe_D(message.get("best_ask")),
                timestamp=_ms(message.get("timestamp")), info=message,
            )]
        if kind == "tick_size_change":
            return [VenueEvent(venue=VENUE, name="tick_size_change", payload=message, timestamp=_ms(message.get("timestamp")))]
        if kind == "new_market":
            return [MarketStatusEvent(
                venue=VENUE, market_id=self._market_of(str(message.get("market") or "")), state="created", native=kind,
                timestamp=_ms(message.get("timestamp")), info=message,
            )]
        if kind == "market_resolved":
            return [MarketStatusEvent(
                venue=VENUE, market_id=self._market_of(str(message.get("market") or "")), state="determined", native=kind,
                result=message.get("winning_asset_id") or None, timestamp=_ms(message.get("timestamp")), info=message,
            )]
        return []

    def _on_book(self, message: dict[str, Any]) -> list[Event]:
        token = str(message.get("asset_id") or "")
        market = str(message.get("market") or "")
        self.markets[token] = market
        book = self.books.setdefault(token, LocalBook())
        recovering = not book.ready and book.timestamp is not None
        book.replace(
            [(D(level["price"]), D(level["size"])) for level in message.get("bids") or []],
            [(D(level["price"]), D(level["size"])) for level in message.get("asks") or []],
        )
        book.timestamp = _ms(message.get("timestamp"))
        self._unverified.pop(token, None)
        if recovering:
            self.status("resynced", f"book {token}", key=token)
        bids, asks = book.levels()
        market_id, side = self._where(token, market)
        return [BookEvent(
            venue=VENUE, market_id=market_id, side=side, kind="snapshot", bids=bids, asks=asks,  # type: ignore[arg-type]
            best_bid=book.best_bid, best_ask=book.best_ask, timestamp=book.timestamp,
            info={"hash": message.get("hash"), "tick_size": message.get("tick_size"), "last_trade_price": message.get("last_trade_price")},
        )]

    def _on_price_change(self, message: dict[str, Any]) -> list[Event]:
        market = str(message.get("market") or "")
        stamp = _ms(message.get("timestamp"))
        self._verify_older_than(stamp)
        changed: dict[str, tuple[list[BookLevel], list[BookLevel]]] = {}
        for change in message.get("price_changes") or []:
            token = str(change.get("asset_id") or "")
            book = self.books.get(token)
            if book is None or not book.ready:
                continue
            pending = self._unverified.get(token)
            if pending is not None and pending[0] != stamp and not self._verify(token):
                continue
            price, size = D(change["price"]), D(change["size"])
            side = "bid" if str(change.get("side")).upper() == "BUY" else "ask"
            new = book.set(side, price, size)
            bids, asks = changed.setdefault(token, ([], []))
            (bids if side == "bid" else asks).append(BookLevel(price, new))
            self._unverified[token] = (stamp, change.get("best_bid"), change.get("best_ask"))
            book.timestamp = stamp
        events: list[Event] = []
        for token, (bids, asks) in changed.items():
            market_id, side = self._where(token, market)
            events.append(BookEvent(
                venue=VENUE, market_id=market_id, side=side, kind="delta", bids=tuple(bids), asks=tuple(asks),  # type: ignore[arg-type]
                best_bid=self.books[token].best_bid, best_ask=self.books[token].best_ask, timestamp=stamp,
            ))
        return events

    def _market_of(self, condition: str) -> str:
        tokens = self.catalog.cached(condition)
        return tokens.market_id if tokens is not None else ids.qualify(VENUE, condition)

    def _verify_older_than(self, stamp: int | None, window_ms: int = 500) -> None:
        if stamp is None:
            return
        for token, (pending_stamp, _, _) in list(self._unverified.items()):
            if pending_stamp is not None and stamp - pending_stamp > window_ms:
                self._verify(token)

    def _verify(self, token: str) -> bool:
        """Check the token's book against the venue's last top of book. On a
        mismatch, report the gap, mark the book not ready and resubscribe."""
        pending = self._unverified.pop(token, None)
        book = self.books.get(token)
        if pending is None or book is None or not book.ready:
            return True
        _, best_bid, best_ask = pending
        if _agrees(book.best_bid, best_bid) and _agrees(book.best_ask, best_ask):
            return True
        self.status(
            "gap", f"book {token}: local top {book.best_bid}/{book.best_ask}, venue {best_bid}/{best_ask}", key=token,
        )
        book.invalidate()
        self._later(self._resubscribe(token))
        return False

    async def _resubscribe(self, token: str) -> None:
        await self.send({"assets_ids": [token], "operation": "unsubscribe"})
        await self.send({"assets_ids": [token], "operation": "subscribe"})

    def _later(self, coroutine: Any) -> None:
        import asyncio

        asyncio.get_running_loop().create_task(coroutine)


class PolymarketUserStream(Stream):
    """This account's orders and trades, with settlement state.

    Takes the CLOB API credentials directly, or a `PolymarketTrading`
    adapter to derive them from.
    """

    venue = VENUE
    name = "user"
    app_ping = "PING"
    data_heartbeat = True
    private = True
    has: dict[str, Capability] = {
        "watch_orders": True,
        "watch_my_trades": True,
        "watch_positions": False,
        "watch_balance": False,
    }

    def __init__(
        self, *, api_key: str | None = None, api_secret: str | None = None, api_passphrase: str | None = None,
        trading: Any = None, wallets: set[str] | None = None, url: str = USER_URL, account_name: str = "default",
        **kwargs: Any,
    ):
        super().__init__(url, **kwargs)
        self.trading = trading
        self.api_key, self.api_secret, self.api_passphrase = api_key, api_secret, api_passphrase
        self.wallets = set(wallets or ())
        if trading is not None:
            self.wallets |= {trading.wallet, trading.signer.address}
        self.markets: set[str] | None = None
        self.account = Account(venue=VENUE, name=account_name)
        self.catalog = TokenCatalog(kwargs.get("gamma_url") or GAMMA_URL)

    async def watch_orders(self, market_ids: list[str] | None = None) -> None:
        """Orders and trades for these markets, or for every market."""
        if market_ids is None:
            self.markets = None
        else:
            markets = [(await self.catalog.resolve(m)).condition_id for m in market_ids]
            new = set(markets) - (self.markets or set())
            self.markets = (self.markets or set()) | set(markets)
            if new:
                await self.send({"operation": "subscribe", "markets": sorted(new)})
                return
        if self._ws is not None:
            await self.on_connect()

    watch_my_trades = watch_orders

    async def on_connect(self) -> None:
        if self.trading is not None and not self.api_key:
            await self.trading.ensure_api_credentials()
            self.api_key, self.api_secret, self.api_passphrase = (
                self.trading._api_key, self.trading._api_secret, self.trading._api_passphrase,
            )
        frame: dict[str, Any] = {
            "auth": {"apiKey": self.api_key, "secret": self.api_secret, "passphrase": self.api_passphrase},
            "type": "user",
        }
        if self.markets:
            frame["markets"] = sorted(self.markets)
        await self.send(frame)

    def handle(self, message: Any) -> list[Event]:
        if isinstance(message, list):
            events: list[Event] = []
            for item in message:
                events.extend(self.handle(item))
            return events
        if not isinstance(message, dict):
            return []
        kind = message.get("event_type")
        if kind == "order":
            native = str(message.get("type") or "")
            market_id, side = self._where(message)
            order = order_of(message, market_id=market_id, outcome=side, account=self.account)
            if native == "CANCELLATION" and not order.is_terminal:
                order = order.model_copy(update={"status": OrderStatus.CANCELED, "remaining": Decimal("0")})
            return [OrderEvent(venue=VENUE, order=order, native=native)]
        if kind == "trade":
            market_id, _ = self._where(message)
            return [FillEvent(venue=VENUE, fill=fill) for fill in fills_of(
                message, api_key=self.api_key or "", wallets=self.wallets, market_id=market_id,
                outcome_of=self._outcome_of, account=self.account,
            )]
        return []

    def _where(self, message: dict[str, Any]) -> tuple[str, str]:
        """`(market_id, side)` for an order or trade message. The user channel
        names the outcome in words, so a market never seen by the catalog is
        still placed on the right side."""
        token = str(message.get("asset_id") or "")
        tokens = self.catalog.cached(token) or self.catalog.cached(str(message.get("market") or ""))
        if tokens is not None:
            return tokens.market_id, tokens.outcome_of(token) if token in (tokens.yes_token, tokens.no_token) else _outcome_word(message)
        return ids.qualify(VENUE, str(message.get("market") or "")), _outcome_word(message)

    def _outcome_of(self, token: str) -> str:
        tokens = self.catalog.cached(token)
        return tokens.outcome_of(token) if tokens is not None else "yes"


def _outcome_word(message: dict[str, Any]) -> str:
    """The venue's `outcome` label, when it is a plain Yes/No; the first
    outcome otherwise."""
    return "no" if str(message.get("outcome") or "").strip().lower() == "no" else "yes"


class TokenCatalog:
    """Which token is which side of which market, read from Gamma once per
    market and kept by Gamma id, condition id and token id."""

    def __init__(self, gamma_url: str = GAMMA_URL):
        self.gamma_url = gamma_url.rstrip("/")
        self._by_key: dict[str, MarketTokens] = {}

    def remember(self, tokens: MarketTokens) -> MarketTokens:
        for key in (tokens.gamma_id, tokens.condition_id, tokens.yes_token, tokens.no_token):
            if key:
                self._by_key[key] = tokens
        return tokens

    def cached(self, key: str) -> MarketTokens | None:
        """By Synpath id, Gamma id, condition id or token id, if seen."""
        _, native = ids.split(key)
        return self._by_key.get(native)

    async def resolve(self, market_id: str) -> MarketTokens:
        native = ids.native(VENUE, market_id)
        cached = self._by_key.get(native)
        if cached is not None:
            return cached
        async with httpx.AsyncClient(base_url=self.gamma_url, timeout=15) as client:
            if native.startswith("0x"):
                response = await client.get("/markets", params={"condition_ids": native})
                rows = response.json() if response.status_code == 200 else []
                raw = rows[0] if isinstance(rows, list) and rows else None
            else:
                response = await client.get(f"/markets/{native}")
                raw = response.json() if response.status_code == 200 else None
        if not isinstance(raw, dict) or not raw.get("id"):
            from ..errors import MarketNotFound
            raise MarketNotFound(f"polymarket: no market {native}")
        return self.remember(MarketTokens.from_gamma(raw))


def _ms(value: Any) -> int | None:
    if value in (None, ""):
        return None
    number = int(str(value))
    return number if number > 1e11 else number * 1000


def _agrees(local: Decimal | None, venue: Any) -> bool:
    """A venue top of book of `0`, `1` or empty means no level on that side."""
    if venue in (None, ""):
        return True
    price = Decimal(str(venue))
    if local is None:
        return price in (Decimal("0"), Decimal("1"))
    return local == price
