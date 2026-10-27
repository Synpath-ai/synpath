"""ProphetX's WebSocket: books, the public tape, and the account's orders.

ProphetX streams over Talaria, its own server for the Pusher wire protocol
(protocol 7), so this module speaks that protocol directly and needs no Pusher
client. The handshake, as ProphetX documents it:

1. `GET /websocket/connection-config` (bearer token) names the host: `ws_host`,
   else the Pusher cluster. Fetched again on every connect.
2. The socket opens at `wss://<host>/app/<key>?protocol=7` and announces
   `pusher:connection_established` with a `socket_id`.
3. `POST /v4/mm/websocket` (bearer token, `X-CFTC-Terminology: true`) registers
   the subscriptions wanted, always the complete set, and answers each channel
   the account may join with its own `auth` signature and the event names to
   bind.
4. `pusher:signin` with the answer's `authenticated` block, then one
   `pusher:subscribe` per channel with that channel's `auth`.

Every message's `data` is JSON in a string: `{"timestamp", "change_type",
"payload", "op"}`, where `payload` is base64-encoded JSON. Every channel is
private, so the stream needs the Trading API key pair (a `ProphetXSession`);
the Market Data API's affiliate key has no stream.

**Books.** An event channel (`{"type": "event"}`) carries `v2.market_selections`
for every main market of the event: each message is one line's two sides, the
ten best levels a side, read on YES like the REST book (side one's levels YES
asks, side two's YES bids at one minus their probability, sizes `value / P`
contracts). A line that does not change publishes nothing, so each book is
seeded from REST once its channel is joined.

**Trades** are on the tournament channel (`{"type": "tournament"}`), as
`trade_execution` under `v2.tournament_<id>`: anonymous fills with the strike,
its price and the stake.

**The account** is the private user channel: `orders` on every change to one
of its orders, one message per fill. ProphetX also sends `health_check` there
every five seconds.
"""
from __future__ import annotations

import asyncio
import base64
import json
from decimal import Decimal
from typing import Any, Awaitable, Callable

from .. import ids
from ..base import Capability
from ..errors import AuthenticationError
from ..prophetx import (
    API_URL, SANDBOX_URL, VENUE, ProphetXSession, book_of, implied_probability, lines_of, line_native_id,
    parse_line_id, unwrap,
)
from ..trading.types import Side
from .base import BookEvent, Event, LocalBook, QuoteEvent, Stream, TradeEvent

PROTOCOL = 7
PING = json.dumps({"event": "pusher:ping", "data": {}})
"""The client's keepalive; Talaria answers `pusher:pong`."""

SIGNIN_TIMEOUT_S = 5.0
"""How long the channels wait for `pusher:signin_success`."""


def decode(data: Any) -> Any:
    """A frame's `data`, which Pusher sends as JSON inside a string."""
    if isinstance(data, str):
        try:
            return json.loads(data)
        except ValueError:
            return data
    return data


def payload_of(envelope: Any) -> Any:
    """ProphetX's message: the base64 `payload` decoded to JSON."""
    if not isinstance(envelope, dict):
        return None
    raw = envelope.get("payload")
    if isinstance(raw, (dict, list)):
        return raw
    if not raw:
        return None
    try:
        return json.loads(base64.b64decode(raw))
    except (ValueError, TypeError):
        return None


def ws_url(config: dict[str, Any]) -> str:
    """The socket's URL from the connection config: its `ws_host` where
    there is one, else the Pusher cluster's host."""
    key = config.get("key")
    if not key:
        raise AuthenticationError(f"{VENUE}: the connection config has no key: {sorted(config)}")
    host = str(config.get("ws_host") or "").strip().rstrip("/")
    if not host:
        cluster = config.get("cluster") or "mt1"
        host = f"ws-{cluster}.pusher.com"
    base = host if host.startswith(("ws://", "wss://")) else f"wss://{host}"
    return f"{base}/app/{key}?protocol={PROTOCOL}&client=synpath&version=1"


def levels(line: dict[str, Any]) -> tuple[list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]]:
    bids, asks = book_of(line)
    return ([(Decimal(str(b.price)), Decimal(str(b.size))) for b in bids],
            [(Decimal(str(a.price)), Decimal(str(a.size))) for a in asks])


class _ProphetXStream(Stream):
    """The Pusher protocol and ProphetX's registration both streams share."""

    venue = VENUE
    data_heartbeat = True
    app_ping = PING

    def __init__(
        self, *, session: ProphetXSession | None = None, credentials: Any = None, sandbox: bool | None = None,
        api_url: str | None = None, config: Any = None, register: Any = None, ping_interval: float = 30.0,
        **kwargs: Any,
    ):
        """`config` and `register` replace the two REST calls of the handshake
        (a dict, or a callable returning one, sync or async); for tests."""
        if sandbox is None:
            sandbox = bool(getattr(credentials, "sandbox", False))
        self.api_url = (api_url or (SANDBOX_URL if sandbox else API_URL)).rstrip("/")
        if session is None and getattr(credentials, "access_key", None) and getattr(credentials, "secret_key", None):
            session = ProphetXSession(access_key=credentials.access_key, secret_key=credentials.secret_key,
                                      base_url=self.api_url)
        if session is None and config is None:
            raise AuthenticationError(
                f"{VENUE}: every ProphetX channel is private; the stream needs the Trading API key pair "
                "(PROPHETX_ACCESS_KEY, PROPHETX_SECRET_KEY)"
            )
        super().__init__("", ping_interval=ping_interval, **kwargs)
        self.session = session
        self._config_source = config
        self._register_source = register
        self.socket_id: str | None = None
        self.live: dict[str, dict[str, Any]] = {}
        """Channel name -> its authorized entry, joined on this connection."""
        self._requested: set[str] = set()
        self._signin: str | None = None
        """`None`, `pending` (sent, unanswered) or `done`, on this connection."""
        self._held: list[dict[str, Any]] = []
        """Subscribe frames waiting for the sign-in to succeed."""

    # -- handshake ------------------------------------------------------------

    async def _source(self, source: Any, *args: Any) -> Any:
        value = source(*args) if callable(source) else source
        return await value if asyncio.iscoroutine(value) else value

    async def fetch_config(self) -> dict[str, Any]:
        if self._config_source is not None:
            return await self._source(self._config_source) or {}
        return await asyncio.to_thread(self._rest, "GET", "/websocket/connection-config", None)

    def _rest(self, method: str, path: str, body: Any) -> Any:
        import httpx

        assert self.session is not None
        headers = {**self.session.headers(), "Content-Type": "application/json", "X-CFTC-Terminology": "true"}
        response = httpx.request(method, self.api_url + path, json=body, headers=headers, timeout=15)
        if response.status_code == 401:
            self.session.invalidate()
            headers.update(self.session.headers())
            response = httpx.request(method, self.api_url + path, json=body, headers=headers, timeout=15)
        if response.status_code in (401, 403):
            raise AuthenticationError(f"{VENUE}: {path} refused the session ({response.status_code})")
        response.raise_for_status()
        return unwrap(response.json()) if path != "/websocket/connection-config" else response.json()

    async def _open(self) -> Any:
        self.url = ws_url(await self.fetch_config())
        return await super()._open()

    def subscriptions(self) -> list[dict[str, Any]]:
        """The complete set of subscriptions to register."""
        return []

    def wants(self, entry: dict[str, Any]) -> bool:
        """Whether to join an authorized channel."""
        return False

    async def register(self) -> None:
        """Register the complete set of subscriptions for this socket, sign
        in, and join every authorized channel wanted and not yet joined."""
        if not self.socket_id:
            return
        body = {"socket_id": self.socket_id, "service": "pusher", "subscriptions": self.subscriptions()}
        try:
            if self._register_source is not None:
                answer = await self._source(self._register_source, body)
            else:
                answer = await asyncio.to_thread(self._rest, "POST", "/v4/mm/websocket", body)
        except Exception as exc:
            self.status("error", f"could not register the subscriptions: {type(exc).__name__}: {exc}")
            return
        answer = unwrap(answer) if isinstance(answer, dict) and "authorized_channel" not in answer else answer
        answer = answer or {}
        for rejected in answer.get("rejected") or []:
            self.status("error", f"subscription refused: {rejected}")
        signin = answer.get("authenticated")
        if self._signin is None and isinstance(signin, dict) and signin.get("auth"):
            # Sign in first, and join the channels once it succeeds.
            self._signin = "pending"
            await self.send({"event": "pusher:signin", "data": {"auth": signin["auth"],
                                                                "user_data": signin.get("user_data") or ""}})
            self._later(self._signin_deadline())
        for entry in answer.get("authorized_channel") or []:
            if not isinstance(entry, dict):
                continue
            channel = str(entry.get("channel_name") or "")
            if not channel or channel in self._requested or not self.wants(entry):
                continue
            self._requested.add(channel)
            self.live[channel] = entry
            frame = {"event": "pusher:subscribe", "data": {"auth": entry.get("auth"), "channel": channel}}
            if self._signin == "pending":
                self._held.append(frame)
            else:
                await self.send(frame)

    async def _signed_in(self) -> None:
        self._signin = "done"
        held, self._held = self._held, []
        for frame in held:
            await self.send(frame)

    async def _signin_deadline(self) -> None:
        """Join the channels anyway when the sign-in is not answered in time:
        each channel carries its own signature."""
        await asyncio.sleep(SIGNIN_TIMEOUT_S)
        if self._signin == "pending":
            self.status("error", f"no answer to pusher:signin in {SIGNIN_TIMEOUT_S:g}s; joining the channels anyway")
            await self._signed_in()

    def on_disconnect(self) -> None:
        self.socket_id = None
        self.live.clear()
        self._requested.clear()
        self._signin = None
        self._held = []

    # -- messages -------------------------------------------------------------

    def handle(self, message: Any) -> list[Event]:
        if not isinstance(message, dict):
            return []
        name = str(message.get("event") or "")
        channel = str(message.get("channel") or "")
        data = decode(message.get("data"))
        if name == "pusher:connection_established":
            self.socket_id = str((data or {}).get("socket_id") or "") or None
            self._later(self.register())
            return []
        if name == "pusher:ping":
            self._later(self.send({"event": "pusher:pong", "data": {}}))
            return []
        if name == "pusher:signin_success":
            self._later(self._signed_in())
            return []
        if name == "pusher:pong":
            return []
        if name == "pusher:error":
            self.status("error", f"venue error: {data}")
            if self._signin == "pending":
                self._later(self._signed_in())
            return []
        if name == "pusher_internal:subscription_succeeded":
            self.status("subscribed", channel, key=channel)
            return self.on_subscribed(channel)
        if name.startswith("pusher"):
            return []
        return self.on_message(name, channel, data if isinstance(data, dict) else {}, payload_of(data))

    def on_subscribed(self, channel: str) -> list[Event]:
        return []

    def on_message(self, name: str, channel: str, envelope: dict[str, Any], payload: Any) -> list[Event]:
        return []

    def _binds(self, channel: str) -> set[str]:
        entry = self.live.get(channel) or {}
        return {str(b.get("name")) for b in entry.get("binding_events") or [] if isinstance(b, dict)}

    def _later(self, coroutine: Any) -> asyncio.Task | None:
        try:
            return asyncio.get_running_loop().create_task(coroutine)
        except RuntimeError:
            coroutine.close()
            return None


class ProphetXMarketStream(_ProphetXStream):
    """Books and the public tape for ProphetX lines. Needs the Trading API
    key pair: every ProphetX channel is private.

    ```python
    async with ProphetXMarketStream(credentials=creds) as stream:
        await stream.watch_order_book(["prophetx:44285780-90de769f4f4977a04446ed203e957351"])
        async for event in stream:
            ...
    ```
    """

    name = "market"
    has: dict[str, Capability] = {
        "watch_order_book": True,
        # Top of book from each book; ProphetX has no last price.
        "watch_ticker": "partial",
        "watch_trades": True,
        "watch_market_status": False,
    }

    def __init__(self, *, catalog: Any = None, **kwargs: Any):
        super().__init__(**kwargs)
        self.catalog = catalog
        """A `ProphetX` for seeding books and finding a line's tournament; made
        when first needed, on this stream's session."""
        self.books: dict[str, LocalBook] = {}
        """Native line id -> its YES book."""
        self.tickers: set[str] = set()
        self.trades: set[str] = set()
        self.events: set[str] = set()
        self.tournaments: set[str] = set()
        self._sequences: dict[str, int] = {}
        self._strikes: dict[str, tuple[str, bool]] = {}
        """strike_id -> (native line id, is the YES side), for the tape."""

    def _catalog(self) -> Any:
        if self.catalog is None:
            from ..prophetx import ProphetX

            self.catalog = ProphetX(session=self.session, base_url=self.api_url)
        return self.catalog

    # -- intents --------------------------------------------------------------

    async def watch_order_book(self, market_ids: list[str]) -> None:
        """Each line's two sides on every change, seeded from REST."""
        for native in self._natives(market_ids):
            self.books.setdefault(native, LocalBook())
        await self.register()

    async def watch_ticker(self, market_ids: list[str]) -> None:
        """Top of book on every change."""
        self.tickers.update(self._natives(market_ids))
        await self.register()

    async def watch_trades(self, market_ids: list[str]) -> None:
        """Fills on these lines, from their tournaments' tape."""
        natives = self._natives(market_ids)
        self.trades.update(natives)
        events = {parse_line_id(n)[0] for n in natives}
        rows = await asyncio.to_thread(self._catalog().fetch_sport_events, event_ids=sorted(events))
        self.tournaments.update(str(r["tournament_id"]) for r in rows if r.get("tournament_id") is not None)
        for event_id in events:
            await self._learn(event_id)
        await self.register()

    async def unwatch(self, market_ids: list[str]) -> None:
        for native in self._natives(market_ids, track=False):
            self.books.pop(native, None)
            self.tickers.discard(native)
            self.trades.discard(native)
            self._sequences.pop(native, None)

    def _natives(self, market_ids: list[str], *, track: bool = True) -> list[str]:
        natives = []
        for market_id in market_ids:
            native = ids.native(VENUE, market_id)
            event_id, _ = parse_line_id(native)
            if track:
                self.events.add(event_id)
            natives.append(native)
        return natives

    def book(self, market_id: str, side: str = "yes") -> LocalBook | None:
        book = self.books.get(ids.native(VENUE, market_id))
        if book is None:
            return None
        return book if side != "no" else book.mirrored()

    # -- registration ---------------------------------------------------------

    def subscriptions(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        if self.events:
            out.append({"type": "event", "ids": sorted(self.events)})
        if self.tournaments:
            out.append({"type": "tournament", "ids": sorted(self.tournaments)})
        return out

    def wants(self, entry: dict[str, Any]) -> bool:
        scope = entry.get("scope") if isinstance(entry.get("scope"), dict) else {}
        if scope.get("event_id") is not None and not scope.get("sub_type"):
            return str(scope["event_id"]) in self.events
        binds = {str(b.get("name")) for b in entry.get("binding_events") or [] if isinstance(b, dict)}
        return bool(self.trades) and any(b.startswith(("v2.tournament_", "tournament_")) for b in binds)

    # -- messages -------------------------------------------------------------

    def on_disconnect(self) -> None:
        super().on_disconnect()
        self._sequences.clear()
        for book in self.books.values():
            book.invalidate()

    def on_subscribed(self, channel: str) -> list[Event]:
        scope = (self.live.get(channel) or {}).get("scope") or {}
        if isinstance(scope, dict) and scope.get("event_id") is not None:
            self._later(self._seed(str(scope["event_id"])))
        return []

    async def _seed(self, event_id: str) -> None:
        """The REST lines of an event, applied to books no message has filled yet."""
        try:
            lines = await asyncio.to_thread(self._catalog().lines, event_id)
        except Exception as exc:  # pragma: no cover - the venue's API down
            self.status("error", f"could not seed event {event_id}: {type(exc).__name__}: {exc}")
            return
        for line in lines:
            self._remember(event_id, line)
            native = line_native_id(event_id, line["_yes"])
            book = self.books.get(native)
            if (book is not None and not book.ready) or (book is None and native in self.tickers):
                for event in self._apply(native, line, sequence=None):
                    self.emit(event)

    async def _learn(self, event_id: str) -> None:
        try:
            lines = await asyncio.to_thread(self._catalog().lines, event_id)
        except Exception as exc:  # pragma: no cover - the venue's API down
            self.status("error", f"could not read event {event_id}: {type(exc).__name__}: {exc}")
            return
        for line in lines:
            self._remember(event_id, line)

    def _remember(self, event_id: str, line: dict[str, Any]) -> None:
        native = line_native_id(event_id, line["_yes"])
        self._strikes[line["_yes"]] = (native, True)
        self._strikes[line["_no"]] = (native, False)

    def on_message(self, name: str, channel: str, envelope: dict[str, Any], payload: Any) -> list[Event]:
        binds = self._binds(channel)
        if name.endswith("market_selections"):
            # Bind the CFTC-vocabulary event; the legacy one carries the same
            # updates in older field names.
            if name != "v2.market_selections" and "v2.market_selections" in binds:
                return []
            return self._on_selections(payload)
        if envelope.get("change_type") == "trade_execution" and isinstance(payload, dict):
            return self._on_trade(payload, envelope)
        return []

    def _on_selections(self, payload: Any) -> list[Event]:
        if not isinstance(payload, dict) or not isinstance(payload.get("info"), dict):
            return []
        info = payload["info"]
        event_id = str(payload.get("sport_event_id") or info.get("sport_event_id") or "")
        sequence = info.get("sequence_number")
        events: list[Event] = []
        for line in lines_of({**info, "id": payload.get("market_id") or info.get("id")}):
            self._remember(event_id, line)
            native = line_native_id(event_id, line["_yes"])
            if native in self.books or native in self.tickers:
                events += self._apply(native, line, sequence=int(sequence) if isinstance(sequence, (int, float)) else None)
        return events

    def _apply(self, native: str, line: dict[str, Any], *, sequence: int | None) -> list[Event]:
        if sequence is not None:
            if sequence <= self._sequences.get(native, -1):
                return []
            self._sequences[native] = sequence
        bids, asks = levels(line)
        market_id = ids.qualify(VENUE, native)
        events: list[Event] = []
        book = self.books.get(native)
        if book is not None:
            recovering = not book.ready and book.timestamp is not None
            book.replace(bids, asks)
            from .base import now_ms

            book.timestamp = now_ms()
            book.sequence = sequence
            if recovering:
                self.status("resynced", f"book {market_id}", key=market_id)
            level_bids, level_asks = book.levels()
            events.append(BookEvent(
                venue=VENUE, market_id=market_id, side="yes", kind="snapshot", bids=level_bids, asks=level_asks,
                best_bid=book.best_bid, best_ask=book.best_ask, sequence=sequence,
                info={"yes_strike_id": line["_yes"], "no_strike_id": line["_no"]},
            ))
        if native in self.tickers:
            bid = max(bids, default=None, key=lambda level: level[0])
            ask = min(asks, default=None, key=lambda level: level[0])
            events.append(QuoteEvent(
                venue=VENUE, market_id=market_id, bid=bid[0] if bid else None, bid_size=bid[1] if bid else None,
                ask=ask[0] if ask else None, ask_size=ask[1] if ask else None, info={"sequence": sequence},
            ))
        return events

    def _on_trade(self, payload: dict[str, Any], envelope: dict[str, Any]) -> list[Event]:
        """One fill from the tape on a watched line, in the YES price. The
        stake bought the strike's side at `fill_price`, so it is `stake / P`
        contracts. `aggressive` says whether that side was the taker."""
        info = payload.get("info") if isinstance(payload.get("info"), dict) else payload
        strike = str(info.get("strike_id") or "")
        known = self._strikes.get(strike)
        if known is None or known[0] not in self.trades:
            return []
        native, is_yes = known
        probability = implied_probability(info.get("fill_price", info.get("price")))
        try:
            stake = Decimal(str(info.get("filled_quantity")))
        except (ArithmeticError, ValueError):
            return []
        if probability is None or stake <= 0:
            return []
        price = Decimal(probability.numerator) / Decimal(probability.denominator)
        aggressive = info.get("aggressive")
        taker = None
        if isinstance(aggressive, bool):
            buying_yes = is_yes if aggressive else not is_yes
            taker = Side.BUY if buying_yes else Side.SELL
        from ..prophetx import iso_ms

        return [TradeEvent(
            venue=VENUE, market_id=ids.qualify(VENUE, native),
            id=str(info.get("sequence_number")) if info.get("sequence_number") is not None else None,
            price=price if is_yes else 1 - price, quantity=(stake / price).quantize(Decimal("0.000001")),
            taker_side=taker, timestamp=iso_ms(envelope.get("timestamp")), info=info,
        )]


Resolver = Callable[..., Awaitable[tuple[str, bool]]]


class ProphetXUserStream(_ProphetXStream):
    """An account's orders and fills, from its private channel. Needs the
    Trading API key pair. `resolver` (the trading adapter's `side`) says which
    line and side an order's strike is; without one, the stream learns it from
    the events' lines on its own session."""

    name = "user"
    private = True
    has: dict[str, Capability] = {"watch_orders": True, "watch_my_trades": True}

    def __init__(self, *, resolver: Resolver | None = None, catalog: Any = None, account_name: str = "default",
                 **kwargs: Any):
        super().__init__(**kwargs)
        from ..trading.types import Account

        self.resolver = resolver
        self.catalog = catalog
        self.account = Account(venue=VENUE, name=account_name)
        self.orders = False
        self.fills = False
        self._sides: dict[str, tuple[str, bool]] = {}
        self._chain: asyncio.Task | None = None

    async def watch_orders(self, market_ids: list[str] | None = None) -> None:
        """Every order on the account; ProphetX has no per-line channel."""
        self.orders = True
        await self.register()

    async def watch_my_trades(self, market_ids: list[str] | None = None) -> None:
        """Every fill on the account, one message each."""
        self.fills = True
        await self.register()

    def wants(self, entry: dict[str, Any]) -> bool:
        binds = {str(b.get("name")) for b in entry.get("binding_events") or [] if isinstance(b, dict)}
        return "orders" in binds and (self.orders or self.fills)

    def on_message(self, name: str, channel: str, envelope: dict[str, Any], payload: Any) -> list[Event]:
        if name != "orders" or not isinstance(payload, dict) or not isinstance(payload.get("info"), dict):
            return []
        row = payload["info"]
        previous = self._chain
        # Resolving a strike may need a REST read: queue it behind the last
        # message so events keep the venue's order.
        self._chain = self._later(self._emit_order(row, previous))
        return []

    async def _emit_order(self, row: dict[str, Any], previous: asyncio.Task | None) -> None:
        if previous is not None:
            try:
                await previous
            except Exception:  # pragma: no cover - reported by that task
                pass
        try:
            for event in await self.events_of(row):
                self.emit(event)
        except Exception as exc:
            self.status("error", f"unreadable order update: {type(exc).__name__}: {exc}")

    async def events_of(self, row: dict[str, Any]) -> list[Event]:
        """The order, and its fill when the update is one."""
        from ..trading.prophetx import fill_of_row, order_of_row
        from .base import FillEvent, OrderEvent

        market_id, is_yes = await self._side(str(row.get("strike_id") or ""), row.get("sport_event_id"))
        events: list[Event] = []
        if self.orders:
            order = order_of_row(row, market_id=market_id, is_yes=is_yes, account=self.account, stream=True)
            events.append(OrderEvent(venue=VENUE, order=order,
                                     native=f"{row.get('update_type') or ''}:{row.get('status') or ''}"))
        if self.fills and row.get("update_type") == "matching" and row.get("fill_price") and row.get("filled_quantity"):
            fill = fill_of_row(row, market_id=market_id, is_yes=is_yes, order_id=str(row.get("id") or ""),
                               account=self.account)
            events.append(FillEvent(venue=VENUE, fill=fill))
        return events

    async def _side(self, strike_id: str, event_id: Any) -> tuple[str, bool]:
        if self.resolver is not None:
            return await self.resolver(strike_id, event_id=event_id)
        if strike_id not in self._sides and event_id not in (None, ""):
            if self.catalog is None:
                from ..prophetx import ProphetX

                self.catalog = ProphetX(session=self.session, base_url=self.api_url)
            for line in await asyncio.to_thread(self.catalog.lines, str(event_id)):
                native = ids.qualify(VENUE, line_native_id(event_id, line["_yes"]))
                self._sides[line["_yes"]] = (native, True)
                self._sides[line["_no"]] = (native, False)
        return self._sides.get(strike_id) or (ids.qualify(VENUE, line_native_id(event_id or "0", strike_id)), True)


__all__ = ["ProphetXMarketStream", "ProphetXUserStream", "ws_url", "decode", "payload_of"]
