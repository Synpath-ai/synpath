"""SX Bet's WebSocket: books, the public tape and market changes.

One endpoint, `wss://realtime.sx.bet/connection/websocket`, speaking
Centrifugo's JSON protocol. This stream speaks it directly, command by command,
so no Centrifugo client is needed: a `connect` command carries a token, each
channel is a `subscribe`, publications arrive as `push` messages, and the
server's empty `{}` pings are answered with `{}`. Several messages can share
one frame, a line each.

**Every channel needs a token, public ones included.** The token comes from the
venue's REST API for an SX Bet API key (`/user/realtime-token-v3/api-key`),
self-service at sx.bet under Account. It expires; the stream renews it before
it does, and fetches a fresh one on every reconnect.

**Books.** `orderbook_v3:{marketHash}` carries the whole book on every change,
with a version string that only grows; an older one is dropped. A market that
does not change publishes nothing, so each book is seeded from the REST
snapshot once its subscription is confirmed. Read on YES like the REST book:
orders on outcome one are bids, orders on outcome two asks at one minus their
price, sizes in contracts.

**Trades and markets are global channels.** `recent_trades_v3:global` is the
whole venue's tape of taker bets and `markets:global` every market change; the
stream filters both down to the markets asked for (or passes everything when
asked for none).
"""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any

from .. import ids
from ..base import Capability
from ..errors import AuthenticationError
from ..trading.types import Side
from .base import BookEvent, Event, LocalBook, MarketStatusEvent, QuoteEvent, Stream, TradeEvent, now_ms

VENUE = "sx_bet"
WS_URL = "wss://realtime.sx.bet/connection/websocket"
TESTNET_WS_URL = "wss://realtime.toronto.sx.bet/connection/websocket"
API_URL = "https://api.sx.bet"
TESTNET_API_URL = "https://api.toronto.sx.bet"
TOKEN_PATH = "/user/realtime-token-v3/api-key"

ODDS_SCALE = Decimal(10) ** 20
USDC = Decimal(1_000_000)
MAX_CHANNELS = 512
"""Subscriptions one connection takes."""


def hash_of(market_id: str) -> str:
    return ids.native(VENUE, market_id).lower()


def levels_of(rows: Any, *, outcome_one: bool) -> list[tuple[Decimal, Decimal]]:
    """Resting orders on one outcome as YES (price, contracts) levels."""
    out = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        try:
            price = Decimal(int(row["percentageOdds"])) / ODDS_SCALE
            stake = Decimal(int(row["size"])) / USDC
        except (KeyError, TypeError, ValueError):
            continue
        if price <= 0 or stake <= 0:
            continue
        out.append((price if outcome_one else 1 - price, stake / price))
    return out


def trade_event(row: dict[str, Any]) -> TradeEvent | None:
    """One taker bet from the tape, in the YES price with the taker's side."""
    if row.get("isParlay") or not row.get("marketHash"):
        return None
    try:
        price = Decimal(int(row["weightedAverageOdds"])) / ODDS_SCALE
        amount = Decimal(int(row["totalReturn"])) / USDC
    except (KeyError, TypeError, ValueError):
        return None
    buying = bool(row.get("isBettingOutcomeOne"))
    from ..sx_bet import iso_ms

    return TradeEvent(
        venue=VENUE, market_id=ids.qualify(VENUE, str(row["marketHash"]).lower()), id=row.get("tradeId"),
        price=price if buying else 1 - price, amount=amount, taker_side=Side.BUY if buying else Side.SELL,
        timestamp=iso_ms(row.get("betTime")), info={k: v for k, v in row.items() if k not in ("userAddress", "username")},
    )


def status_event(row: dict[str, Any]) -> MarketStatusEvent | None:
    """A market change as its new state."""
    if not row.get("marketHash"):
        return None
    from ..sx_bet import WINNERS, iso_ms, status_of

    status, native, _ = status_of(row)
    result = WINNERS.get(row["outcome"]) if row.get("outcome") is not None else None
    return MarketStatusEvent(
        venue=VENUE, market_id=ids.qualify(VENUE, str(row["marketHash"]).lower()),
        state=status,  # type: ignore[arg-type]
        native=native or None, result=result, timestamp=iso_ms(row.get("reportedDate")), info=row,
    )


class _CentrifugoStream(Stream):
    """Centrifugo's JSON protocol: the token, connect, subscribe, ping and
    token renewal both streams share."""

    venue = VENUE
    data_heartbeat = True

    def __init__(
        self, *, api_key: str | None = None, testnet: bool = False, url: str | None = None,
        api_url: str | None = None, token: Any = None, **kwargs: Any,
    ):
        if not api_key and token is None:
            raise AuthenticationError(
                f"{VENUE}: every SX Bet channel needs a token, made from an SX Bet API key "
                "(sx.bet, Account, API key): pass api_key= or SX_BET_API_KEY"
            )
        super().__init__(url or (TESTNET_WS_URL if testnet else WS_URL), **kwargs)
        self.api_url = api_url or (TESTNET_API_URL if testnet else API_URL)
        self._api_key = api_key
        self._token_source = token
        """A token string, or a callable (sync or async) returning one; for tests
        and for callers who fetch tokens themselves."""
        self.ready = False
        self.channels: set[str] = set()
        self._live: set[str] = set()
        self._next_id = 0
        self._pending: dict[int, tuple[str, str]] = {}
        self._renewer: asyncio.Task | None = None
        self._pong = True

    # -- token ----------------------------------------------------------------

    async def fetch_token(self) -> str:
        source = self._token_source
        if isinstance(source, str):
            return source
        if callable(source):
            value = source()
            return await value if asyncio.iscoroutine(value) else value
        return await asyncio.to_thread(self._fetch_token_sync)

    def _fetch_token_sync(self) -> str:
        import httpx

        response = httpx.get(self.api_url + TOKEN_PATH, headers={"x-sx-api-key": self._api_key or ""}, timeout=15)
        if response.status_code in (401, 403):
            raise AuthenticationError(f"{VENUE}: the API key was refused for a realtime token ({response.status_code})")
        response.raise_for_status()
        token = (response.json() or {}).get("token")
        if not token:
            raise AuthenticationError(f"{VENUE}: no realtime token in the venue's answer")
        return str(token)

    # -- protocol -------------------------------------------------------------

    def _command(self, kind: str, body: dict[str, Any], *, channel: str = "") -> dict[str, Any]:
        self._next_id += 1
        self._pending[self._next_id] = (kind, channel)
        return {"id": self._next_id, kind: body}

    async def on_connect(self) -> None:
        self.ready = False
        token = await self.fetch_token()
        await self.send(self._command("connect", {"token": token, "name": "synpath"}))

    def on_disconnect(self) -> None:
        self.ready = False
        self._live.clear()
        self._pending.clear()
        if self._renewer is not None:
            self._renewer.cancel()
            self._renewer = None

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
                self._on_connected(message.get("connect") or {})
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

    def _on_connected(self, reply: dict[str, Any]) -> None:
        self.ready = True
        self._pong = bool(reply.get("pong", True))
        if reply.get("expires") and reply.get("ttl"):
            self._renewer = self._later(self._renew(int(reply["ttl"])))
        self._later(self.resubscribe())

    async def _renew(self, ttl: int) -> None:
        """Send a fresh token before the current one expires, and again each time."""
        while True:
            await asyncio.sleep(max(5.0, ttl * 0.8))
            try:
                token = await self.fetch_token()
            except Exception as exc:  # pragma: no cover - the venue's API down
                self.status("error", f"could not renew the realtime token: {type(exc).__name__}: {exc}")
                continue
            await self.send(self._command("refresh", {"token": token}))

    async def resubscribe(self) -> None:
        """Send every channel not yet live on this connection."""
        if not self.ready:
            return
        for channel in sorted(self.channels - self._live):
            if len(self._live) >= MAX_CHANNELS:
                self.status("error", f"more than {MAX_CHANNELS} channels on one connection; {channel} not subscribed")
                return
            self._live.add(channel)
            await self.send(self._command("subscribe", {"channel": channel}, channel=channel))

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


class SxBetMarketStream(_CentrifugoStream):
    """Books, the tape and market changes for SX Bet markets. Needs an SX Bet
    API key, as every channel on this venue does.

    ```python
    async with SxBetMarketStream(api_key="...") as stream:
        await stream.watch_order_book(["sx_bet:0x2d6e…"])
        async for event in stream:
            ...
    ```
    """

    name = "market"
    has: dict[str, Capability] = {
        "watch_order_book": True,
        # Top of book from each book; the last price is on the tape.
        "watch_ticker": "partial",
        "watch_trades": True,
        "watch_market_status": True,
    }

    def __init__(self, *, catalog: Any = None, **kwargs: Any):
        super().__init__(**kwargs)
        self.catalog = catalog
        """An `SxBet` for seeding books from REST; made when first needed."""
        self.books: dict[str, LocalBook] = {}
        """Hash -> its YES book."""
        self.tickers: set[str] = set()
        self.trades: set[str] | None = None
        """Hashes whose bets are passed on; `None` while not watching, empty for all."""
        self.statuses: set[str] | None = None
        self._versions: dict[str, str] = {}

    # -- intents --------------------------------------------------------------

    async def watch_order_book(self, market_ids: list[str]) -> None:
        """Each market's whole book, on every change, seeded from REST."""
        hashes = [hash_of(m) for m in market_ids]
        for market_hash in hashes:
            self.books.setdefault(market_hash, LocalBook())
        await self.add_channels([f"orderbook_v3:{h}" for h in hashes])

    async def watch_ticker(self, market_ids: list[str]) -> None:
        """Top of book on every change."""
        hashes = [hash_of(m) for m in market_ids]
        self.tickers.update(hashes)
        await self.add_channels([f"orderbook_v3:{h}" for h in hashes])

    async def watch_trades(self, market_ids: list[str] | None = None) -> None:
        """Taker bets on these markets, or on every market when none are named."""
        if market_ids:
            self.trades = (self.trades or set()) | {hash_of(m) for m in market_ids}
        else:
            self.trades = set()
        await self.add_channels(["recent_trades_v3:global"])

    async def watch_market_status(self, market_ids: list[str] | None = None) -> None:
        """Changes to these markets (listed, closed, settled), or to every market."""
        if market_ids:
            self.statuses = (self.statuses or set()) | {hash_of(m) for m in market_ids}
        else:
            self.statuses = set()
        await self.add_channels(["markets:global"])

    async def unwatch(self, market_ids: list[str]) -> None:
        dropped = []
        for market_id in market_ids:
            market_hash = hash_of(market_id)
            self.books.pop(market_hash, None)
            self.tickers.discard(market_hash)
            self._versions.pop(market_hash, None)
            if self.trades:
                self.trades.discard(market_hash)
            if self.statuses:
                self.statuses.discard(market_hash)
            dropped.append(f"orderbook_v3:{market_hash}")
        await self.drop_channels(dropped)

    def book(self, market_id: str, side: str = "yes") -> LocalBook | None:
        book = self.books.get(hash_of(market_id))
        if book is None:
            return None
        return book if side != "no" else book.mirrored()

    # -- messages -------------------------------------------------------------

    def on_disconnect(self) -> None:
        super().on_disconnect()
        self._versions.clear()
        for book in self.books.values():
            book.invalidate()

    def on_subscribed(self, channel: str) -> list[Event]:
        if channel.startswith("orderbook_v3:"):
            self._later(self._seed(channel.split(":", 1)[1]))
        return []

    async def _seed(self, market_hash: str) -> None:
        """The REST snapshot, applied if no newer publication has arrived."""
        try:
            catalog = self._catalog()
            payload = await asyncio.to_thread(
                catalog._get, "/orderbook-v3/snapshot", {"marketHash": market_hash},
            )
        except Exception as exc:  # pragma: no cover - the venue's API down
            self.status("error", f"could not seed book {market_hash}: {type(exc).__name__}: {exc}")
            return
        if isinstance(payload, dict):
            for event in self._on_book({**payload, "marketHash": market_hash}):
                self.emit(event)

    def _catalog(self) -> Any:
        if self.catalog is None:
            from ..sx_bet import SxBet

            self.catalog = SxBet(api_url=self.api_url)
        return self.catalog

    def on_publication(self, channel: str, data: Any) -> list[Event]:
        if channel.startswith("orderbook_v3:") and isinstance(data, dict):
            return self._on_book({**data, "marketHash": data.get("marketHash") or channel.split(":", 1)[1]})
        if channel == "recent_trades_v3:global" and isinstance(data, dict) and self.trades is not None:
            row = data.get("trade") if isinstance(data.get("trade"), dict) else data
            if self.trades and str(row.get("marketHash") or "").lower() not in self.trades:
                return []
            event = trade_event(row)
            return [event] if event else []
        if channel == "markets:global" and self.statuses is not None:
            rows = data if isinstance(data, list) else [data]
            events: list[Event] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                if self.statuses and str(row.get("marketHash") or "").lower() not in self.statuses:
                    continue
                event = status_event(row)
                if event:
                    events.append(event)
            return events
        return []

    def _on_book(self, data: dict[str, Any]) -> list[Event]:
        market_hash = str(data.get("marketHash") or "").lower()
        version = str(data.get("version") or "")
        if version:
            if version <= self._versions.get(market_hash, ""):
                return []
            self._versions[market_hash] = version
        bids = levels_of(data.get("outcomeOne"), outcome_one=True)
        asks = levels_of(data.get("outcomeTwo"), outcome_one=False)
        market_id = ids.qualify(VENUE, market_hash)
        events: list[Event] = []
        book = self.books.get(market_hash)
        if book is not None:
            recovering = not book.ready and book.timestamp is not None
            book.replace(bids, asks)
            book.timestamp = now_ms()
            if recovering:
                self.status("resynced", f"book {market_id}", key=market_id)
            levels_bids, levels_asks = book.levels()
            events.append(BookEvent(
                venue=VENUE, market_id=market_id, side="yes", kind="snapshot", bids=levels_bids, asks=levels_asks,
                best_bid=book.best_bid, best_ask=book.best_ask, info={"version": version or None},
            ))
        if market_hash in self.tickers:
            best_bid = max(bids, default=None, key=lambda level: level[0])
            best_ask = min(asks, default=None, key=lambda level: level[0])
            events.append(QuoteEvent(
                venue=VENUE, market_id=market_id,
                bid=best_bid[0] if best_bid else None, bid_size=best_bid[1] if best_bid else None,
                ask=best_ask[0] if best_ask else None, ask_size=best_ask[1] if best_ask else None,
                info={"version": version or None},
            ))
        return events


class SxBetUserStream(_CentrifugoStream):
    """An account's orders and fills. Needs the SX Bet API key, and the
    account's address (the wallet, never its proxy): the venue's account
    channels are named by it, checksummed, and only its own token may join.

    A fill arrives first as `matched`, the moment the engine fills it and
    before the funds are escrowed on chain, then again as `confirmed` once they
    are (or `failed`), under the same id.
    """

    name = "user"
    private = True
    has: dict[str, Capability] = {"watch_orders": True, "watch_my_trades": True}

    def __init__(self, *, address: str, account_name: str = "default", **kwargs: Any):
        if not address:
            raise AuthenticationError(f"{VENUE}: the account stream needs the account's address")
        super().__init__(**kwargs)
        from eth_utils import to_checksum_address

        from ..trading.types import Account

        self.address = to_checksum_address(address)
        self.account = Account(venue=VENUE, name=account_name)

    async def watch_orders(self, market_ids: list[str] | None = None) -> None:
        """Every order on the account; the venue has no per-market channel."""
        await self.add_channels([f"account:orders_v3_#{self.address}"])

    async def watch_my_trades(self, market_ids: list[str] | None = None) -> None:
        """Every fill on the account, matched and then escrowed."""
        await self.add_channels([f"account:fills_v3_#{self.address}"])

    def on_publication(self, channel: str, data: Any) -> list[Event]:
        if not isinstance(data, dict):
            return []
        from ..trading.sx_bet import fill_of_row, order_of_row
        from .base import FillEvent, OrderEvent

        if channel.startswith("account:orders_v3_") and isinstance(data.get("order"), dict):
            row = data["order"]
            order = order_of_row(row, account=self.account)
            return [OrderEvent(venue=VENUE, order=order, native=f"{row.get('status')}:{row.get('inactiveReason') or ''}")]
        if channel.startswith("account:fills_v3_") and isinstance(data.get("fill"), dict):
            fill = fill_of_row(data["fill"], account=self.account)
            return [FillEvent(venue=VENUE, fill=fill)] if fill else []
        return []
