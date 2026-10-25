"""Gemini Predictions' WebSocket: books, prints, quotes and contract status,
and an account's orders, fills, positions and balances.

One endpoint, `wss://ws.gemini.com`. Market data is public and needs no key.
The account streams authenticate on the handshake with an account-scoped,
time-based API key (`X-GEMINI-APIKEY`, `-NONCE`, `-PAYLOAD`, `-SIGNATURE`);
nothing can authenticate a connection after it opens. Requests are
`{"id", "method": "SUBSCRIBE", "params": [stream names]}`, answered
`{"id", "status": 200}` or with an `error`. Gemini sends symbols lower case;
they are read back upper case, as REST spells them.

**Books are differential, in YES space.** `{symbol}@depth@100ms` sends the
levels that changed every 100 ms. The market stream connects with
`snapshot=-1`, so the first frame after subscribing is the whole book. Frames
chain by update id: a frame's first id `U` is the last id `u` of the frame
before it (ids are the exchange's, shared across books). A frame whose `U`
is past the last applied `u` is a gap: the book is dropped and the stream
subscribed again for a fresh snapshot. A NO view is the YES book mirrored.

**Quotes and prints.** `@bookTicker` is the top of book with the last trade
price once there is one; `@trade` the public tape, `m` saying whether the
buyer was the maker, so the taker sold when it is true.

**Account.** `orders@account` reports each order's transitions; a frame
carries only what changed, so the stream keeps each order's last known
fields. Fills ride on `FILLED` and `PARTIALLY_FILLED` frames (`Z` the
execution's quantity, `L` its price, `n` its fee). Nothing is replayed after
a reconnect, so one is reported with `reconcile_required`.

**Combo RFQ, the maker side** (`GeminiRfqStream`). Gemini runs combo RFQ as
a sealed auction on this connection: a requester opens one in the Gemini app
(there is no API for that), `requestForQuote` broadcasts it to everyone,
makers answer with `rfq.submit_quote` inside a one-second window (`w`), and
at close only the best eligible quote is disclosed, privately, to the
requester, who has five seconds to accept. The winning maker then has one
second for its last look (`rfq.confirm_quote`). Quotes are single-sided (the
price the combo's YES is sold at), immutable, one per account per RFQ, and
withdrawing one does not free the slot. The private deliveries
(`requestForQuote@account`) are at least once and keyed by a delivery id,
which this stream deduplicates. Quote methods are never queued for a later
connection and never resent: an answer that does not come is an unknown
outcome to reconcile on the delivery stream.
"""
from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from decimal import Decimal
from typing import Any

from .. import ids
from ..base import Capability
from ..errors import (
    AuthenticationError, BadRequest, ExchangeError, ExchangeNotAvailable, NetworkError, RateLimitExceeded,
    RequestTimeout,
)
from ..gemini import native_symbol
from ..trading.credentials import GeminiCredentials
from ..trading.errors import CredentialsMissing, InvalidOrder, OrderRejected, PermissionDenied
from ..trading.gemini import DEFAULT_PRECISION, WS_STATUS, GeminiSigner, yes_leg, yes_price
from ..trading.money import D, validate_price, validate_quantity
from ..trading.types import (
    Account, Balance, Fill, Liquidity, Order, OrderStatus, OrderType, Position, PositionSide, Side, TimeInForce,
)
from .base import (
    BalanceEvent, BookEvent, BookLevel, Event, FillEvent, LocalBook, MarketStatusEvent, OrderEvent, PositionEvent,
    QuoteEvent, Stream, TradeEvent, VenueEvent, maybe_D,
)

VENUE = "gemini"
WS_URL = "wss://ws.gemini.com"
SANDBOX_WS_URL = "wss://ws.sandbox.gemini.com"

DEPTH = "@depth@100ms"

CONTRACT_STATE = {
    "awaiting approval": "created",
    "approved": "created",
    "active": "open",
    "closed": "closed",
    "under review": "closed",
    "under_review": "closed",
    "settled": "settled",
    "invalid": "settled",
}
"""A contract's new status as the shared market state."""

ORDER_TYPE = {"LIMIT": OrderType.LIMIT, "MARKET": OrderType.MARKET, "STOP_LIMIT": OrderType.STOP_LIMIT,
              "STOP_MARKET": OrderType.STOP_MARKET}

ORDER_MEMORY = 4096
"""Orders whose last fields the account stream keeps, most recent first."""


def ns_to_ms(value: Any) -> int | None:
    """Gemini stamps stream events in nanoseconds (contract status in ms)."""
    if value in (None, ""):
        return None
    stamp = int(value)
    return stamp // 1_000_000 if stamp > 10**15 else stamp


def symbol_of(market_id: str) -> str:
    return native_symbol(ids.native(VENUE, market_id))


def levels_of(rows: Any) -> list[tuple[Decimal, Decimal]]:
    out = []
    for row in rows or []:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            price, size = maybe_D(row[0]), maybe_D(row[1])
            if price is not None and size is not None:
                out.append((price, size))
    return out


def outcome_word(value: Any) -> str:
    """`YES` / `NO`, or the booleans the AsyncAPI document uses, as `yes` / `no`."""
    if isinstance(value, bool):
        return "yes" if value else "no"
    return "no" if str(value).strip().lower() in ("no", "false") else "yes"


class _GeminiStream(Stream):
    """Subscriptions by stream name, sent again on every connect."""

    venue = VENUE

    def __init__(self, url: str, **kwargs: Any):
        super().__init__(url, **kwargs)
        self.streams: dict[str, None] = {}
        self._next_id = 1

    def _allocate(self) -> int:
        number = self._next_id
        self._next_id += 1
        return number

    async def _subscribe(self, names: list[str]) -> None:
        new = [name for name in names if name not in self.streams]
        for name in new:
            self.streams[name] = None
        if new:
            await self.send({"id": self._allocate(), "method": "SUBSCRIBE", "params": new})

    async def _unsubscribe(self, names: list[str]) -> None:
        gone = [name for name in names if self.streams.pop(name, 0) is None]
        if gone:
            await self.send({"id": self._allocate(), "method": "UNSUBSCRIBE", "params": gone})

    async def on_connect(self) -> None:
        if self.streams:
            await self.send({"id": self._allocate(), "method": "SUBSCRIBE", "params": list(self.streams)})

    def _reply(self, message: dict[str, Any]) -> list[Event] | None:
        """A request's answer; `None` for anything else."""
        if "id" not in message or "status" not in message or "e" in message:
            return None
        if message.get("status") == 200:
            self.status("subscribed", str(message.get("id")), key=str(message.get("id")))
            return []
        error = message.get("error") or {}
        self.status("error", f"venue: {error.get('code')} {error.get('msg')}".strip(), key=str(message.get("id")))
        return []


class GeminiMarketStream(_GeminiStream):
    """Books, prints, quotes and contract status. No key.

    ```python
    async with GeminiMarketStream() as stream:
        await stream.watch_order_book(["gemini:GEMI-NBA-2610232000-CHI-MIL-M-MIL"])
        async for event in stream:
            ...
    ```
    """

    name = "market"
    has: dict[str, Capability] = {
        "watch_order_book": True,
        "watch_trades": True,
        "watch_ticker": True,
        "watch_market_status": True,
    }

    def __init__(self, *, sandbox: bool = False, url: str | None = None, **kwargs: Any):
        base = url or (SANDBOX_WS_URL if sandbox else WS_URL)
        super().__init__(base if "snapshot=" in base else f"{base}{'&' if '?' in base else '?'}snapshot=-1", **kwargs)
        self.books: dict[str, LocalBook] = {}
        """Instrument symbol -> its YES book."""
        self._awaiting: set[str] = set()
        """Symbols whose next depth frame is a snapshot."""
        self._last: dict[str, int] = {}

    async def watch_order_book(self, market_ids: list[str]) -> None:
        """Each contract's whole book, then its changes."""
        names = []
        for market_id in market_ids:
            symbol = symbol_of(market_id)
            if symbol not in self.books:
                self.books[symbol] = LocalBook()
                self._awaiting.add(symbol)
            names.append(f"{symbol}{DEPTH}")
        await self._subscribe(names)

    async def watch_trades(self, market_ids: list[str]) -> None:
        """Public prints from now on, in the YES price."""
        await self._subscribe([f"{symbol_of(m)}@trade" for m in market_ids])

    async def watch_ticker(self, market_ids: list[str]) -> None:
        """Top of book as it changes, with the last trade price."""
        await self._subscribe([f"{symbol_of(m)}@bookTicker" for m in market_ids])

    async def watch_market_status(self) -> None:
        """Every contract's status changes, and strikes as they are set."""
        await self._subscribe(["contractStatus"])

    async def unwatch(self, market_ids: list[str]) -> None:
        for market_id in market_ids:
            symbol = symbol_of(market_id)
            await self._unsubscribe([f"{symbol}{DEPTH}", f"{symbol}@trade", f"{symbol}@bookTicker"])
            self.books.pop(symbol, None)
            self._awaiting.discard(symbol)
            self._last.pop(symbol, None)

    def book(self, market_id: str, side: str = "yes") -> LocalBook | None:
        """The local book on either side; `None` if not watched."""
        book = self.books.get(symbol_of(market_id))
        if book is None:
            return None
        return book if side != "no" else book.mirrored()

    async def on_connect(self) -> None:
        # Every depth subscription answers a reconnect with a snapshot.
        self._awaiting = set(self.books)
        self._last.clear()
        await super().on_connect()

    def on_disconnect(self) -> None:
        for book in self.books.values():
            book.invalidate()

    # -- reading --------------------------------------------------------------

    def handle(self, message: Any) -> list[Event]:
        if not isinstance(message, dict):
            return []
        reply = self._reply(message)
        if reply is not None:
            return reply
        kind = message.get("e")
        if kind == "depthUpdate":
            return self._on_depth(message)
        if kind == "contractStatus":
            return [self._status_event(message)]
        if kind is None and "t" in message and "p" in message and "q" in message:
            return [self._trade(message)]
        if kind is None and isinstance(message.get("b"), str) and "s" in message:
            return [self._quote(message)]
        return []

    def _on_depth(self, message: dict[str, Any]) -> list[Event]:
        symbol = native_symbol(str(message.get("s") or ""))
        book = self.books.get(symbol)
        if book is None:
            return []
        market_id = ids.qualify(VENUE, symbol)
        first, last = message.get("U"), message.get("u")
        stamp = ns_to_ms(message.get("E"))
        bids, asks = levels_of(message.get("b")), levels_of(message.get("a"))
        if symbol in self._awaiting:
            recovering = book.sequence is not None
            book.replace(bids, asks)
            book.sequence, book.timestamp = last, stamp
            self._awaiting.discard(symbol)
            self._last[symbol] = int(last) if last is not None else 0
            if recovering:
                self.status("resynced", f"book {market_id}", key=market_id)
            levels_bid, levels_ask = book.levels()
            return [BookEvent(venue=VENUE, market_id=market_id, kind="snapshot", bids=levels_bid, asks=levels_ask,
                              best_bid=book.best_bid, best_ask=book.best_ask, sequence=last, timestamp=stamp)]
        if not book.ready:
            return []
        previous = self._last.get(symbol)
        if previous is not None and last is not None and int(last) <= previous:
            return []   # already applied
        if previous is not None and first is not None and int(first) > previous:
            self.status("gap", f"book {market_id}: update {first} after {previous}", key=market_id)
            book.invalidate()
            self._awaiting.add(symbol)
            self._later(self._resubscribe(f"{symbol}{DEPTH}"))
            return []
        changed_bids = tuple(BookLevel(p, book.set("bid", p, s)) for p, s in bids)
        changed_asks = tuple(BookLevel(p, book.set("ask", p, s)) for p, s in asks)
        book.sequence, book.timestamp = last, stamp
        if last is not None:
            self._last[symbol] = int(last)
        return [BookEvent(venue=VENUE, market_id=market_id, kind="delta", bids=changed_bids, asks=changed_asks,
                          best_bid=book.best_bid, best_ask=book.best_ask, sequence=last, timestamp=stamp)]

    async def _resubscribe(self, name: str) -> None:
        self.streams.pop(name, None)
        await self.send({"id": self._allocate(), "method": "UNSUBSCRIBE", "params": [name]})
        await self._subscribe([name])

    def _later(self, coroutine: Any) -> None:
        import asyncio

        asyncio.get_running_loop().create_task(coroutine)

    def _trade(self, message: dict[str, Any]) -> TradeEvent:
        symbol = native_symbol(str(message.get("s") or ""))
        maker_bought = message.get("m")
        return TradeEvent(
            venue=VENUE, market_id=ids.qualify(VENUE, symbol), id=str(message.get("t") or "") or None,
            price=Decimal(str(message["p"])), quantity=Decimal(str(message["q"])),
            taker_side=None if maker_bought is None else (Side.SELL if maker_bought else Side.BUY),
            timestamp=ns_to_ms(message.get("E")), info=message,
        )

    def _quote(self, message: dict[str, Any]) -> QuoteEvent:
        symbol = native_symbol(str(message.get("s") or ""))
        return QuoteEvent(
            venue=VENUE, market_id=ids.qualify(VENUE, symbol),
            bid=maybe_D(message.get("b")) or None, ask=maybe_D(message.get("a")) or None,
            bid_size=maybe_D(message.get("B")), ask_size=maybe_D(message.get("A")),
            last=maybe_D(message.get("c")), timestamp=ns_to_ms(message.get("E")), info=message,
        )

    def _status_event(self, message: dict[str, Any]) -> MarketStatusEvent:
        symbol = native_symbol(str(message.get("s") or ""))
        new = str(message.get("n") or "")
        old = str(message.get("o") or "")
        state = CONTRACT_STATE.get(new.lower(), "updated") if new.lower() != old.lower() else "updated"
        return MarketStatusEvent(
            venue=VENUE, market_id=ids.qualify(VENUE, symbol), state=state,  # type: ignore[arg-type]
            native=new or None, timestamp=ns_to_ms(message.get("E")), info=message,
        )


class GeminiUserStream(_GeminiStream):
    """This account's orders, fills, positions and balances.

    Needs an account-scoped API key with a time-based nonce (Auditor is
    enough to read positions; the stream sends no orders).

    ```python
    async with GeminiUserStream(creds) as stream:
        await stream.watch_orders()
        await stream.watch_positions()
        async for event in stream:
            ...
    ```
    """

    name = "account"
    private = True
    has: dict[str, Capability] = {
        "watch_orders": True,
        "watch_my_trades": True,
        "watch_positions": True,
        "watch_balance": True,
    }

    def __init__(self, credentials: GeminiCredentials, *, url: str | None = None, account_name: str = "default",
                 signer: GeminiSigner | None = None, **kwargs: Any):
        super().__init__(url or (SANDBOX_WS_URL if credentials.env == "sandbox" else WS_URL), **kwargs)
        self.signer = signer or GeminiSigner(credentials.api_key, credentials.api_secret)
        self.account = Account(venue=VENUE, name=account_name)
        self.only: set[str] | None = None
        self._orders: OrderedDict[str, dict[str, Any]] = OrderedDict()

    def headers(self) -> dict[str, str]:
        return self.signer.ws_headers()

    async def watch_orders(self, market_ids: list[str] | None = None) -> None:
        """This account's orders on every contract, or only these."""
        self._narrow(market_ids)
        await self._subscribe(["orders@account"])

    async def watch_my_trades(self, market_ids: list[str] | None = None) -> None:
        """This account's executions, read off its order events."""
        self._narrow(market_ids)
        await self._subscribe(["orders@account"])

    async def watch_positions(self, market_ids: list[str] | None = None) -> None:
        """Position changes, settlement payouts included. The first frame is
        the open positions."""
        self._narrow(market_ids)
        await self._subscribe(["positions@account"])

    async def watch_balance(self) -> None:
        await self._subscribe(["balances@account"])

    def _narrow(self, market_ids: list[str] | None) -> None:
        if market_ids is None:
            self.only = None
        elif self.only is not None or not self.streams:
            self.only = (self.only or set()) | {ids.qualify(VENUE, symbol_of(m)) for m in market_ids}

    def _wanted(self, market_id: str) -> bool:
        return self.only is None or market_id in self.only

    def handle(self, message: Any) -> list[Event]:
        if not isinstance(message, dict):
            return []
        reply = self._reply(message)
        if reply is not None:
            return reply
        kind = message.get("e")
        if kind == "orderUpdate":
            return self._on_order(message)
        if kind == "positionReport":
            return self._on_positions(message)
        if kind == "balanceUpdate":
            return self._on_balances(message)
        return []

    def _remember(self, frame: dict[str, Any]) -> dict[str, Any]:
        """The order's fields so far, this frame's over the last ones."""
        key = str(frame.get("i") or "")
        merged = {**self._orders.get(key, {}), **{k: v for k, v in frame.items() if v not in (None, "")}}
        self._orders[key] = merged
        self._orders.move_to_end(key)
        while len(self._orders) > ORDER_MEMORY:
            self._orders.popitem(last=False)
        return merged

    def _on_order(self, frame: dict[str, Any]) -> list[Event]:
        merged = self._remember(frame)
        order = order_of_frame(merged, frame, account=self.account)
        if not self._wanted(order.market_id):
            return []
        events: list[Event] = [OrderEvent(venue=VENUE, order=order, native=str(frame.get("X") or ""))]
        fill = fill_of_frame(merged, frame, account=self.account)
        if fill is not None:
            events.append(FillEvent(venue=VENUE, fill=fill))
        return events

    def _on_positions(self, frame: dict[str, Any]) -> list[Event]:
        stamp = ns_to_ms(frame.get("E"))
        events: list[Event] = []
        for row in frame.get("P") or []:
            if row.get("t") not in (None, "ec"):
                continue
            position = position_of_row(row, account=self.account, timestamp=stamp)
            if self._wanted(position.market_id):
                events.append(PositionEvent(venue=VENUE, position=position))
        return events

    def _on_balances(self, frame: dict[str, Any]) -> list[Event]:
        stamp = ns_to_ms(frame.get("E"))
        events: list[Event] = []
        for row in frame.get("B") or []:
            confirmed, available = maybe_D(row.get("c")), maybe_D(row.get("f"))
            if confirmed is None and available is None:
                continue
            total = confirmed if confirmed is not None else available
            free = available if available is not None else confirmed
            assert total is not None and free is not None
            events.append(BalanceEvent(venue=VENUE, balance=Balance(
                venue=VENUE, account=self.account, currency=str(row.get("a") or "USD").upper(),
                total=total, available=free, locked=total - free, timestamp=stamp, info=row,
            )))
        return events


def order_of_frame(merged: dict[str, Any], frame: dict[str, Any], *, account: Account | None = None) -> Order:
    """An `orderUpdate` (with the order's earlier fields merged in) on the YES leg."""
    outcome = outcome_word(merged.get("O", "YES"))
    venue_side = str(merged.get("S") or "BUY")
    status_word = str(frame.get("X") or merged.get("X") or "").upper()
    quantity = maybe_D(merged.get("q"))
    remaining = maybe_D(merged.get("z"))
    executed = maybe_D(frame.get("Z"))
    if quantity is not None and remaining is not None and status_word in ("FILLED", "PARTIALLY_FILLED", "OPEN", "NEW"):
        filled = quantity - remaining
    elif status_word == "CANCELED" and executed is not None:
        filled = executed        # cumulative on a cancel
    elif quantity is not None and remaining is not None:
        filled = quantity - remaining
    else:
        filled = Decimal("0")
    if quantity is None:
        quantity = filled + (remaining or Decimal("0"))
    status = WS_STATUS.get(status_word, OrderStatus.OPEN)
    if status == OrderStatus.CANCELED:
        remaining = Decimal("0")
    stop = maybe_D(merged.get("P"))
    first = ns_to_ms(merged.get("_created") or merged.get("T"))
    merged.setdefault("_created", merged.get("T"))
    return Order(
        id=str(merged.get("i") or ""),
        client_order_id=merged.get("c") or None,
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, native_symbol(str(merged.get("s") or ""))),
        side=yes_leg(venue_side, outcome),
        type=ORDER_TYPE.get(str(merged.get("o") or "LIMIT").upper(), OrderType.LIMIT),
        time_in_force=TimeInForce.GTC,
        status=status,
        price=yes_price(merged.get("p"), outcome),
        stop_price=yes_price(stop, outcome) if stop else None,
        quantity=quantity,
        filled=filled,
        remaining=remaining if remaining is not None else quantity - filled,
        average_price=yes_price(merged.get("a"), outcome) if merged.get("a") not in (None, "", "0") else None,
        fee_currency="USD",
        last_fill_price=yes_price(frame.get("L"), outcome) if frame.get("L") else None,
        last_fill_quantity=executed if status_word in ("FILLED", "PARTIALLY_FILLED") else None,
        reduce_only=venue_side.upper() == "SELL",
        created_at=first,
        updated_at=ns_to_ms(frame.get("T") or frame.get("E")),
        info={**frame, "venue_side": venue_side.lower(), "venue_outcome": outcome,
              "rejection_reason": frame.get("r")},
    )


def fill_of_frame(merged: dict[str, Any], frame: dict[str, Any], *, account: Account | None = None) -> Fill | None:
    """The execution on a `FILLED` or `PARTIALLY_FILLED` frame, else `None`."""
    if str(frame.get("X") or "").upper() not in ("FILLED", "PARTIALLY_FILLED"):
        return None
    quantity, price = maybe_D(frame.get("Z")), frame.get("L")
    if quantity is None or quantity <= 0 or price in (None, ""):
        return None
    outcome = outcome_word(merged.get("O", "YES"))
    maker = frame.get("m")
    fee = maybe_D(frame.get("n"))
    return Fill(
        id=str(frame.get("t") or f"{merged.get('i')}-{frame.get('T')}"),
        order_id=str(merged.get("i") or ""),
        client_order_id=merged.get("c") or None,
        venue=VENUE, account=account,
        market_id=ids.qualify(VENUE, native_symbol(str(merged.get("s") or ""))),
        side=yes_leg(str(merged.get("S") or "BUY"), outcome),
        price=yes_price(price, outcome),  # type: ignore[arg-type]
        quantity=quantity,
        fee=fee, fee_currency="USD",
        liquidity=Liquidity.UNKNOWN if maker is None else (Liquidity.MAKER if maker else Liquidity.TAKER),
        timestamp=ns_to_ms(frame.get("T") or frame.get("E")) or 0,
        info=frame,
    )


def position_of_row(row: dict[str, Any], *, account: Account | None = None, timestamp: int | None = None) -> Position:
    """One `positionReport` row: a signed position (positive holds YES) and,
    on a settlement close, the payout and the outcome."""
    amounts = {str(a.get("t")): a for a in row.get("a") or [] if isinstance(a, dict)}
    signed = maybe_D((amounts.get("position") or {}).get("v")) or Decimal("0")
    payout_row = amounts.get("settlement_payout")
    payout = maybe_D(payout_row.get("v")) if payout_row else None
    result = str(payout_row.get("o") or "").lower() if payout_row else None
    return Position(
        venue=VENUE, account=account, market_id=ids.qualify(VENUE, native_symbol(str(row.get("s") or ""))),
        side=PositionSide.LONG if signed > 0 else PositionSide.SHORT if signed < 0 else PositionSide.FLAT,
        contracts=abs(signed),
        resolved=payout_row is not None, final=payout_row is not None,
        won=(payout > 0) if payout is not None else None,
        payout=payout, timestamp=timestamp,
        info={**row, "result": result if result in ("yes", "no") else None},
    )


# ---------------------------------------------------------------------------
# Combo RFQ (maker side)
# ---------------------------------------------------------------------------

RFQ_STATES = frozenset({"OPEN", "PENDING_ACCEPTANCE", "CONFIRMING", "FINALIZING", "FINALIZED", "CANCELLED",
                        "EXPIRED", "FAILED"})
RFQ_TERMINAL = frozenset({"FINALIZED", "CANCELLED", "EXPIRED", "FAILED"})
"""An RFQ in one of these is over. Only `FINALIZED` traded."""
DELIVERY_SCOPES = ("account", "session")
DELIVERY_MEMORY = 10_000
"""Private delivery ids remembered for deduplication, most recent first."""
RFQ_MEMORY = 10_000
"""Public RFQs whose last state the stream keeps."""
CLIENT_ID_MAX = 36

WS_ERRORS: dict[int, type[ExchangeError] | type[NetworkError]] = {
    -1002: AuthenticationError,
    -1004: PermissionDenied,
    -1013: BadRequest,
    -2010: OrderRejected,
    -1003: RateLimitExceeded,
    -1101: ExchangeNotAvailable,
}
"""Gemini's WebSocket error codes for the quote methods. -1000 (internal
error) and anything unlisted is an `ExchangeError`; its outcome is unknown."""


def rfq_of(event: dict[str, Any]) -> dict[str, Any]:
    """A public `requestForQuote` event: the auction and its state, never a
    quote or a participant. `market_id` is the combo's own instrument where
    Gemini names it; each leg is Gemini's contract id (`c`) with the outcome
    the combo takes on it, and the leg's own instrument where named. Exactly
    one of `notional` (USD) and `quantity` (contracts) sizes it;
    `fill_quantity` appears once the requester accepts and counts only on
    `FINALIZED`. Times are milliseconds: `quote_deadline` (`w`, the end of
    the one-second quoting window), `expires_at` (`x`), `created_at` (`c`)."""
    symbol = event.get("s")
    return {
        "rfq_id": str(event.get("r") or ""),
        "market_id": ids.qualify(VENUE, native_symbol(str(symbol))) if symbol else None,
        "legs": [
            {
                "contract_id": str(leg.get("c") or ""),
                "outcome": outcome_word(leg.get("o")),
                "market_id": ids.qualify(VENUE, native_symbol(str(leg["s"]))) if leg.get("s") else None,
            }
            for leg in event.get("l") or [] if isinstance(leg, dict)
        ],
        "notional": maybe_D(event.get("n")),
        "quantity": maybe_D(event.get("q")),
        "fill_quantity": maybe_D(event.get("f")),
        "state": str(event.get("S") or ""),
        "quote_deadline": event.get("w"),
        "expires_at": event.get("x"),
        "created_at": event.get("c"),
        "timestamp": event.get("E"),
        "info": event,
    }


def delivery_of(event: dict[str, Any]) -> dict[str, Any]:
    """A private `requestForQuote@account` delivery. `transition` is what
    happened: `CLOSED` discloses the winning quote to the requester,
    `ACCEPTED` tells the winning maker to confirm within a second, then
    `CONFIRMED` / `DECLINED` and `FINALIZED` / `FAILED`. The quote fields
    (`quote_id`, `price`, `size`, `quote_status`, `valid_until`) are absent on
    an RFQ-level delivery. `delivery_id` is Gemini's idempotency key."""
    return {
        "delivery_id": str(event.get("i") or ""),
        "rfq_id": str(event.get("r") or ""),
        "transition": str(event.get("x") or ""),
        "state": str(event.get("S") or ""),
        "quote_id": event.get("q"),
        "price": maybe_D(event.get("p")),
        "size": maybe_D(event.get("sz")),
        "quote_status": event.get("qs"),
        "valid_until": event.get("vu"),
        "timestamp": event.get("E"),
        "info": event,
    }


def quote_params(rfq_id: str, price: Any, quantity: Any, *, valid_until: int | None = None,
                 client_id: str | None = None, now_ms: int | None = None) -> dict[str, Any]:
    """`rfq.submit_quote` params, checked before anything is sent: a price on
    the cent tick strictly inside (0, 1), a positive quantity on the
    hundredth-contract grid (an RFQ's grid is whole contracts or hundredths;
    the venue refuses what is off its own), `valid_until` in the future, a
    `client_id` of at most 36 printable ASCII characters."""
    if not str(rfq_id or "").strip():
        raise InvalidOrder(f"{VENUE}: rfq_id is required")
    price_d = validate_price(D(price), DEFAULT_PRECISION)
    quantity_d = validate_quantity(D(quantity), DEFAULT_PRECISION)
    params: dict[str, Any] = {"rfqId": str(rfq_id), "price": format(price_d, "f"), "quantity": format(quantity_d, "f")}
    if valid_until is not None:
        if isinstance(valid_until, bool) or not isinstance(valid_until, int):
            raise InvalidOrder(f"{VENUE}: valid_until is a Unix time in milliseconds, got {valid_until!r}")
        if valid_until <= (now_ms if now_ms is not None else int(time.time() * 1000)):
            raise InvalidOrder(f"{VENUE}: valid_until must be in the future")
        params["validUntil"] = valid_until
    if client_id is not None:
        if not client_id or len(client_id) > CLIENT_ID_MAX or not all(" " <= ch <= "~" for ch in client_id):
            raise InvalidOrder(f"{VENUE}: client_id is 1 to {CLIENT_ID_MAX} printable ASCII characters")
        params["clientId"] = client_id
    return params


def ws_error(message: dict[str, Any], *, method: str) -> Exception:
    """A WebSocket error answer as the typed error, Gemini's code and text kept."""
    error = message.get("error") if isinstance(message.get("error"), dict) else {}
    code, text = error.get("code"), str(error.get("msg") or "")
    label = f"{VENUE}: {method} refused: {code} {text}".rstrip()
    kind = WS_ERRORS.get(code) if isinstance(code, int) else None
    if kind is RateLimitExceeded:
        return RateLimitExceeded(label)
    if kind is OrderRejected:
        return OrderRejected(label, reason=str(code), info=message, body=message, status=message.get("status"))
    if kind is not None:
        return kind(label, body=message, status=message.get("status"))  # type: ignore[call-arg]
    return ExchangeError(f"{label} (outcome unknown: reconcile before acting again)", body=message,
                         status=message.get("status"))


class GeminiRfqStream(_GeminiStream):
    """Gemini's combo RFQ auctions, and quoting them as a market maker.

    Discovery (`watch_rfqs`) is public. Deliveries (`watch_deliveries`) and
    the quote methods need an account-scoped key with a time-based nonce, the
    handshake signed; Gemini also requires an account eligible to quote.

    ```python
    async with GeminiRfqStream(creds) as stream:
        await stream.watch_rfqs()
        await stream.watch_deliveries()
        async for event in stream:
            if getattr(event, "name", None) == "rfq" and event.payload["state"] == "OPEN":
                answer = await stream.submit_quote(event.payload["rfq_id"], "0.55", "100")
            elif getattr(event, "name", None) == "rfq_delivery" and event.payload["transition"] == "ACCEPTED":
                await stream.confirm_quote(event.payload["rfq_id"], event.payload["quote_id"], True)
    ```

    Events are `VenueEvent`s: `name="rfq"` with `rfq_of(...)` as the payload,
    `name="rfq_delivery"` with `delivery_of(...)`, each delivery once.
    """

    name = "rfq"
    has: dict[str, Capability] = {"rfq": True}

    def __init__(self, credentials: GeminiCredentials | None = None, *, sandbox: bool = False, url: str | None = None,
                 signer: GeminiSigner | None = None, reply_timeout: float = 5.0, clock: Any = time.time,
                 **kwargs: Any):
        sandboxed = sandbox or (credentials is not None and credentials.env == "sandbox")
        super().__init__(url or (SANDBOX_WS_URL if sandboxed else WS_URL), **kwargs)
        if signer is None and credentials is not None:
            signer = GeminiSigner(credentials.api_key, credentials.api_secret)
        self.signer = signer
        self.private = signer is not None
        self.reply_timeout = reply_timeout
        self._clock = clock
        self.rfqs: OrderedDict[str, dict[str, Any]] = OrderedDict()
        """RFQ id -> its last public state (`rfq_of`)."""
        self.quotes: dict[str, str] = {}
        """RFQ id -> the id of the quote this account made on it, this session."""
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._pending: dict[int, tuple[str, asyncio.Future]] = {}

    def headers(self) -> dict[str, str]:
        return self.signer.ws_headers() if self.signer is not None else {}

    def _need_key(self, what: str) -> None:
        if self.signer is None:
            raise CredentialsMissing(f"{VENUE}: {what} needs an account-scoped API key (GEMINI_API_KEY, "
                                     f"GEMINI_API_SECRET) on the connection's handshake")

    async def watch_rfqs(self) -> None:
        """Every RFQ auction as it opens and ends (public, anonymous). Not
        replayed after a reconnect: auctions last seconds."""
        await self._subscribe(["requestForQuote"])

    async def watch_deliveries(self, scope: str = "account") -> None:
        """This account's private RFQ deliveries: as a maker, the acceptance
        of its quote and the outcome; as a requester (an RFQ opened in the
        Gemini app), the winning quote and the outcome. `scope` is `account`
        or `session`, the same deliveries; a connection takes one of them."""
        self._need_key("watch_deliveries")
        if scope not in DELIVERY_SCOPES:
            raise BadRequest(f"{VENUE}: scope is account or session, got {scope!r}")
        other = f"requestForQuote@{'session' if scope == 'account' else 'account'}"
        if other in self.streams:
            raise BadRequest(f"{VENUE}: {other} is already watched; a connection takes one delivery scope")
        await self._subscribe([f"requestForQuote@{scope}"])

    # -- quote methods ----------------------------------------------------------

    async def submit_quote(self, rfq_id: str, price: Any, quantity: Any, *, valid_until: int | None = None,
                           client_id: str | None = None) -> dict[str, Any]:
        """Quote an open RFQ: sell its combo's YES at `price` a contract, for
        up to `quantity` contracts (which must cover the whole request to be
        eligible: `quantity * price >= notional` on a notional-sized RFQ).
        Strictly before the RFQ's `quote_deadline`; immutable; one per account
        per RFQ. `valid_until` (ms) defaults to the deadline. `client_id`
        comes back as the maker fill's client order id. Returns Gemini's
        `{"rfqId", "quoteId"}`."""
        self._need_key("submit_quote")
        now = int(self._clock() * 1000)
        params = quote_params(rfq_id, price, quantity, valid_until=valid_until, client_id=client_id, now_ms=now)
        known = self.rfqs.get(str(rfq_id))
        if known is not None:
            deadline = known.get("quote_deadline")
            if known.get("state") != "OPEN" or (deadline is not None and int(deadline) <= now):
                raise InvalidOrder(f"{VENUE}: RFQ {rfq_id} is no longer quotable ({known.get('state')}, "
                                   f"window closed at {deadline})")
        result = await self._request("rfq.submit_quote", params)
        if result.get("quoteId"):
            self.quotes[str(rfq_id)] = str(result["quoteId"])
        return result

    async def withdraw_quote(self, rfq_id: str, quote_id: str) -> dict[str, Any]:
        """Pull this account's quote before the window closes. Final: the
        quote cannot come back and the RFQ takes no other from this account.
        Needs the session's `cancel_orders` capability."""
        self._need_key("withdraw_quote")
        if not str(rfq_id or "").strip() or not str(quote_id or "").strip():
            raise InvalidOrder(f"{VENUE}: rfq_id and quote_id are required")
        return await self._request("rfq.withdraw_quote", {"rfqId": str(rfq_id), "quoteId": str(quote_id)})

    async def confirm_quote(self, rfq_id: str, quote_id: str, confirm: bool) -> dict[str, Any]:
        """The winning maker's last look, within one second of the RFQ
        entering `CONFIRMING` (the `ACCEPTED` delivery): `True` trades,
        `False` declines and fails the RFQ. There is no default: Gemini
        refuses an omitted decision. Confirming commits capital, and the
        maker's order rests on the book first, so it can fill in part even if
        the RFQ then fails; watch `GeminiUserStream` for the maker fills."""
        self._need_key("confirm_quote")
        if not isinstance(confirm, bool):
            raise InvalidOrder(f"{VENUE}: confirm is True or False, got {confirm!r}")
        if not str(rfq_id or "").strip() or not str(quote_id or "").strip():
            raise InvalidOrder(f"{VENUE}: rfq_id and quote_id are required")
        return await self._request("rfq.confirm_quote",
                                   {"rfqId": str(rfq_id), "quoteId": str(quote_id), "confirm": confirm})

    async def _request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """Send one method on the live connection and wait for its answer.
        Never queued for a later connection and never resent."""
        if self._ws is None:
            raise NetworkError(f"{VENUE}: {method} not sent: the RFQ stream is not connected "
                               f"(a quote is never queued for a later connection)")
        number = self._allocate()
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[number] = (method, future)
        try:
            await self.send({"id": number, "method": method, "params": params})
            return await asyncio.wait_for(asyncio.shield(future), timeout=self.reply_timeout)
        except asyncio.TimeoutError:
            raise RequestTimeout(f"{VENUE}: {method} sent, no answer in {self.reply_timeout}s: outcome unknown; "
                                 f"reconcile on the delivery stream, do not resend") from None
        finally:
            self._pending.pop(number, None)

    def on_disconnect(self) -> None:
        for method, future in list(self._pending.values()):
            if not future.done():
                future.set_exception(NetworkError(
                    f"{VENUE}: {method} sent, connection lost before the answer: outcome unknown; reconcile "
                    f"on the delivery stream, do not resend"))
        self._pending.clear()

    # -- reading ----------------------------------------------------------------

    def handle(self, message: Any) -> list[Event]:
        if not isinstance(message, dict):
            return []
        number = message.get("id")
        if "e" not in message and isinstance(number, int) and number in self._pending:
            method, future = self._pending[number]
            if not future.done():
                if message.get("status") == 200 and isinstance(message.get("result"), dict):
                    future.set_result(message["result"])
                elif message.get("status") == 200:
                    future.set_exception(ExchangeError(f"{VENUE}: {method} answered without a result", body=message,
                                                       status=200))
                else:
                    future.set_exception(ws_error(message, method=method))
            return []
        reply = self._reply(message)
        if reply is not None:
            return reply
        if message.get("e") != "requestForQuote":
            return []
        if "i" in message and isinstance(message.get("x"), str):
            return self._on_delivery(message)
        return self._on_rfq(message)

    def _on_rfq(self, message: dict[str, Any]) -> list[Event]:
        rfq = rfq_of(message)
        if not rfq["rfq_id"] or rfq["state"] not in RFQ_STATES:
            self.status("error", f"unreadable RFQ event: {message!r}")
            return []
        known = self.rfqs.get(rfq["rfq_id"])
        if known is not None:
            # A later event may leave out what the opening one said (`w`, `x`, `s`).
            rfq = {**known, **{k: v for k, v in rfq.items() if v not in (None, [])}}
        self.rfqs[rfq["rfq_id"]] = rfq
        self.rfqs.move_to_end(rfq["rfq_id"])
        while len(self.rfqs) > RFQ_MEMORY:
            self.rfqs.popitem(last=False)
        if rfq["state"] in RFQ_TERMINAL:
            self.quotes.pop(rfq["rfq_id"], None)
        return [VenueEvent(venue=VENUE, name="rfq", payload=rfq, timestamp=rfq["timestamp"])]

    def _on_delivery(self, message: dict[str, Any]) -> list[Event]:
        delivery = delivery_of(message)
        key = delivery["delivery_id"]
        if key in self._seen:
            return []      # at least once: the same transition again
        self._seen[key] = None
        while len(self._seen) > DELIVERY_MEMORY:
            self._seen.popitem(last=False)
        return [VenueEvent(venue=VENUE, name="rfq_delivery", payload=delivery, timestamp=delivery["timestamp"])]
