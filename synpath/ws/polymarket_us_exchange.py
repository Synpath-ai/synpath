"""Polymarket US exchange API streams, over gRPC.

`grpc-api.{preprod,prod}.polymarketexchange.com:443`, authenticated with the
same Auth0 access token as the exchange REST API (sent as `authorization:
Bearer` metadata, with `x-participant-id` on account data). Each stream
here is one server-streaming RPC:

| Stream | RPC | Carries |
|---|---|---|
| `PolymarketUSExchangeOrderStream` | `OrderEntryAPI.CreateOrderSubscription` | open orders as a snapshot, then every execution |
| `PolymarketUSExchangeDropCopyStream` | `DropCopyAPI.CreateDropCopySubscription` | every execution in the firm, resumable |
| `PolymarketUSExchangeTradeCaptureStream` | `DropCopyAPI.CreateTradeCaptureReportSubscription` | trades through clearing, busts included, resumable |
| `PolymarketUSExchangePositionChangeStream` | `DropCopyAPI.CreatePositionChangeSubscription` | position changes, resumable |
| `PolymarketUSExchangeInstrumentStream` | `DropCopyAPI.CreateInstrumentStateChangeSubscription` | instrument state (open, halted, expired ...), resumable |
| `PolymarketUSExchangePositionStream` | `PositionAPI.CreatePositionSubscription` | positions as a snapshot, then changes |
| `PolymarketUSExchangeMarketDataStream` | `MarketDataSubscriptionAPI.CreateMarketDataSubscription` | books to a depth, statistics |
| `PolymarketUSExchangeBalanceLedgerStream` | `FundingAPI.CreateBalanceLedgerSubscription` | balance ledger entries, replayable by time |

What shapes them:

**The venue's protos are needed and not shipped.** See `synpath.ws.grpc`.

**Delivery is at least once.** Executions are deduplicated by id; trade
reports by id *and state*, because a trade is sent again under the same id
when it clears or is busted.

**Drop copy resumes.** Every drop-copy response carries a resume token. A
reconnect sends the last one, so nothing is missed and no reconciliation is
asked for; a token the venue refuses falls back to resuming from the time of
the last event. After each batch that moves the token a
`VenueEvent(name="checkpoint")` carries it: persist it once the events
before it are handled, and pass it back as `resume_token=` after a restart.

**A busted trade is a failed fill.** Trade capture reports become `FillEvent`s
whose `settlement` is `matched` while the trade is on its way through
clearing, `confirmed` once cleared, and `failed` when it is busted or the
clearing house rejects it -- the same fill id each time, as on Polymarket.

**Integers everywhere.** Orders carry the scales they were entered with, so
order and execution streams decode themselves. Positions and books carry
none; their scales come from reference data (six requests a minute, firm
wide), read once per symbol and cached on the trading adapter.

**Twenty streams a firm.** Every stream here is one of them.

Built from the published protos and documentation. A market-data update is
read as the whole book to the requested depth, since the message carries no
way to mark a level removed, and heartbeat intervals are not published, so
heartbeat streams reconnect after two minutes of silence.
"""
from __future__ import annotations

import base64
from decimal import Decimal
from typing import Any, Callable

from .. import ids
from ..base import Capability
from ..errors import ExchangeError
from ..polymarket_us import parse_ts
from ..trading.credentials import PolymarketUSExchangeCredentials
from ..trading.polymarket_us_exchange import (
    InstrumentScale, PolymarketUSExchangeTrading, fill_of, order_of, position_of, rfc3339, scale_from_order,
)
from ..trading.types import Account, Fill, Position, PositionSide, SettlementState
from .base import (
    BookEvent, Event, FillEvent, LocalBook, MarketStatusEvent, OrderEvent, PositionEvent, QuoteEvent, VenueEvent, now_ms,
)
from .grpc import GrpcStream, Outcome, ProtoBundle, RecentIds

VENUE = "polymarket_us"
TARGETS = {
    "preprod": "grpc-api.preprod.polymarketexchange.com:443",
    "prod": "grpc-api.prod.polymarketexchange.com:443",
}
ANCHOR = "polymarket/v1/trading.proto"
PROTO_FILES = (
    "polymarket/v1/trading.proto",
    "polymarket/v1/dropcopy.proto",
    "polymarket/v1/positions.proto",
    "polymarket/v1/marketdatasubscription.proto",
    "polymarket/v1/funding.proto",
)
MAX_SYMBOLS = 1000
HEARTBEAT_IDLE_S = 120.0

FILL_TYPES = {"EXECUTION_TYPE_FILL", "EXECUTION_TYPE_PARTIAL_FILL"}

INSTRUMENT_STATE = {
    "INSTRUMENT_STATE_PENDING": "created",
    "INSTRUMENT_STATE_OPEN": "open",
    "INSTRUMENT_STATE_PREOPEN": "paused",
    "INSTRUMENT_STATE_SUSPENDED": "paused",
    "INSTRUMENT_STATE_HALTED": "paused",
    "INSTRUMENT_STATE_MATCH_AND_CLOSE_AUCTION": "closed",
    "INSTRUMENT_STATE_CLOSED": "closed",
    "INSTRUMENT_STATE_EXPIRED": "closed",
    "INSTRUMENT_STATE_TERMINATED": "settled",
}

TRADE_SETTLEMENT = {
    "TRADE_STATE_CLEARED": SettlementState.CONFIRMED,
    "TRADE_STATE_BUSTED": SettlementState.FAILED,
    "TRADE_STATE_REJECTED": SettlementState.FAILED,
}
"""Every other state (new, in flight, pending, acknowledged, retrying, and any
state added later) is a trade still on its way: `matched`."""


def load_protos(path: Any = None, **kwargs: Any) -> ProtoBundle:
    """The exchange API's protos from `path` (a directory or the zip), or from
    `SYNPATH_POLYMARKET_US_PROTOS`."""
    return ProtoBundle.load(path, anchor=ANCHOR, files=PROTO_FILES, **kwargs)


# ---------------------------------------------------------------------------
# Pure normalizers, on messages as dicts
# ---------------------------------------------------------------------------

def _enum_word(value: Any, prefix: str) -> str:
    text = str(value or "")
    return text[len(prefix):].lower() if text.startswith(prefix) else text.lower()


def _carries_scale(order: dict[str, Any]) -> bool:
    """Whether an order carries its scales; unset int64s arrive as "0"."""
    return int(order.get("priceScale") or 0) != 0


def _require(scale: InstrumentScale | None, symbol: str) -> InstrumentScale:
    if scale is None:
        raise LookupError(f"no reference data for {symbol!r}, so its integers cannot be read")
    return scale


def execution_events(execution: dict[str, Any], scale: InstrumentScale | None, *, account: Account | None) -> list[Event]:
    """An execution report as the order it leaves behind and, for a fill, the fill."""
    raw_order = execution.get("order") or {}
    symbol = str(raw_order.get("symbol") or "")
    scale = _require(scale_from_order(raw_order, scale), symbol)
    kind = str(execution.get("type") or "EXECUTION_TYPE_NEW")
    order = order_of(raw_order, scale, account=account)
    detail = {k: execution[k] for k in ("id", "type", "text", "transactTime") if execution.get(k) not in (None, "")}
    # Both reasons default to a real-looking value; they mean something only
    # on a reject or an unsolicited cancel.
    if kind == "EXECUTION_TYPE_REJECTED" and execution.get("orderRejectReason"):
        detail["orderRejectReason"] = execution["orderRejectReason"]
    if str(execution.get("unsolicitedCancelReason") or "").removesuffix("_UNDEFINED") not in ("", "UNSOLICITED_CXL_REASON"):
        detail["unsolicitedCancelReason"] = execution["unsolicitedCancelReason"]
    order = order.model_copy(update={"info": {**order.info, "execution": detail}})
    events: list[Event] = [OrderEvent(venue=VENUE, order=order, native=_enum_word(kind, "EXECUTION_TYPE_"))]
    if kind in FILL_TYPES and scale.qty_from_wire(execution.get("lastShares")) != 0:
        events.append(FillEvent(venue=VENUE, fill=fill_of(execution, scale, account=account)))
    return events


def trade_fills(
    trade: dict[str, Any],
    scale_for: Callable[[str], InstrumentScale | None],
    *,
    account_for: Callable[[str], Account | None],
) -> list[Fill]:
    """A trade capture report as this firm's fills, settled as far as clearing
    has got. `reportingCounterparty` names the firm's side; where it is not
    given, every execution that names an account is the firm's."""
    state = str(trade.get("state") or "TRADE_STATE_UNDEFINED")
    settlement = TRADE_SETTLEMENT.get(state, SettlementState.MATCHED)
    reporting = str(trade.get("reportingCounterparty") or "")
    fills: list[Fill] = []
    for role in ("aggressor", "passive"):
        execution = trade.get(role)
        if not execution:
            continue
        raw_order = execution.get("order") or {}
        if reporting in ("SIDE_BUY", "SIDE_SELL"):
            if raw_order.get("side") != reporting:
                continue
        elif not raw_order.get("account"):
            continue
        symbol = str(raw_order.get("symbol") or "")
        scale = _require(scale_from_order(raw_order, scale_for(symbol)), symbol)
        fill = fill_of({**execution, "tradeId": execution.get("tradeId") or trade.get("id")}, scale, account=account_for(str(raw_order.get("account") or "")))
        fills.append(fill.model_copy(update={
            "settlement": settlement,
            "info": {**fill.info, "trade": {k: v for k, v in trade.items() if k not in ("aggressor", "passive")}, "role": role},
        }))
    return fills


def market_status_of(instrument: dict[str, Any]) -> MarketStatusEvent:
    native = str(instrument.get("state") or "INSTRUMENT_STATE_CLOSED")
    return MarketStatusEvent(
        venue=VENUE, market_id=ids.qualify(VENUE, str(instrument.get("symbol") or "")), state=INSTRUMENT_STATE.get(native, "updated"),  # type: ignore[arg-type]
        native=native, timestamp=parse_ts(instrument.get("updateTime")),
        info={k: instrument[k] for k in ("priceScale", "fractionalQtyScale", "nonTradable", "description") if k in instrument},
    )


def flat_position(account: Account | None, symbol: str) -> Position:
    return Position(venue=VENUE, account=account, market_id=ids.qualify(VENUE, symbol), side=PositionSide.FLAT)


def ledger_event(entry: dict[str, Any]) -> VenueEvent:
    """A balance ledger entry. Balances are decimal strings on this message."""
    before = Decimal(str(entry.get("beforeBalance") or "0"))
    after = Decimal(str(entry.get("afterBalance") or "0"))
    kind = str(entry.get("entryType") or "")
    word = _enum_word(kind, "LEDGER_ENTRY_TYPE_BALANCE_") if kind.startswith("LEDGER_ENTRY_TYPE_BALANCE_") else _enum_word(kind, "LEDGER_ENTRY_TYPE_")
    stamp = parse_ts(entry.get("updateTime"))
    return VenueEvent(venue=VENUE, name="balance_ledger", timestamp=stamp, payload={
        "id": str(entry.get("id") or ""), "account": str(entry.get("account") or ""),
        "currency": str(entry.get("currency") or ""), "before": before, "after": after, "change": after - before,
        "entry_type": word, "symbol": entry.get("symbol") or None, "description": entry.get("description") or "",
        "business_date": entry.get("updateBusinessDate") or None, "timestamp": stamp, "info": entry,
    })


def book_sides(update: dict[str, Any], scale: InstrumentScale) -> tuple[list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]]:
    def side(rows: Any) -> list[tuple[Decimal, Decimal]]:
        out = []
        for row in rows or []:
            price = scale.price_from_wire(row.get("px"))
            if price is not None:
                out.append((price, scale.qty_from_wire(row.get("qty"))))
        return out
    return side(update.get("bids")), side(update.get("offers"))


# ---------------------------------------------------------------------------
# Streams
# ---------------------------------------------------------------------------

class _ExchangeStream(GrpcStream):
    venue = VENUE
    account_scoped = True
    """Sends `x-participant-id`; reference and market data need none."""

    def __init__(
        self,
        source: PolymarketUSExchangeCredentials | PolymarketUSExchangeTrading,
        *,
        protos: ProtoBundle | Any = None,
        target: str | None = None,
        **kwargs: Any,
    ):
        if isinstance(source, PolymarketUSExchangeTrading):
            self.trading, self._owns_trading = source, False
        else:
            self.trading, self._owns_trading = PolymarketUSExchangeTrading(source), True
        if isinstance(protos, ProtoBundle):
            bundle: ProtoBundle | None = protos
        elif protos is None and kwargs.get("call") is not None:
            bundle = None
        else:
            bundle = load_protos(protos)
        if self.data_heartbeat:
            kwargs.setdefault("idle_timeout", HEARTBEAT_IDLE_S)
        super().__init__(target or TARGETS[self.trading.credentials.env], protos=bundle, **kwargs)
        self.seen = RecentIds()

    async def metadata(self) -> list[tuple[str, str]]:
        metadata = [("authorization", f"Bearer {await self.trading.tokens.token()}")]
        if self.account_scoped:
            metadata.append(("x-participant-id", self.trading.credentials.participant_id))
        return metadata

    async def on_reauthenticate(self) -> None:
        self.trading.tokens.invalidate()

    async def close(self) -> None:
        await super().close()
        if self._owns_trading:
            await self.trading.close()

    def account_for(self, name: str) -> Account | None:
        if not name or name == self.trading.trading_account:
            return self.trading.account
        return Account(venue=VENUE, name=name)

    # -- reference data -------------------------------------------------------

    def symbols_needing_scales(self, message: dict[str, Any]) -> list[str]:
        return []

    async def prepare(self, message: dict[str, Any]) -> None:
        missing = [s for s in dict.fromkeys(self.symbols_needing_scales(message)) if s and self.trading.cached_scale(s) is None]
        if missing:
            try:
                await self.trading.load_instruments(missing, strict=False)
            except ExchangeError as exc:
                self.status("error", f"reference data for {missing[:3]} failed: {exc}")

    def _each(self, items: Any, read: Callable[[dict[str, Any]], list[Event]]) -> list[Event]:
        """Read items one at a time, so one unreadable item costs only itself."""
        events: list[Event] = []
        for item in items or []:
            try:
                events.extend(read(item))
            except Exception as exc:
                self.status("error", f"unreadable item: {type(exc).__name__}: {exc}")
        return events


class _ResumableStream(_ExchangeStream):
    """A drop-copy RPC: every response carries a token to resume after it."""

    items_field = ""
    refused_token_codes = {"INVALID_ARGUMENT", "FAILED_PRECONDITION", "OUT_OF_RANGE", "NOT_FOUND"}

    def __init__(
        self,
        source: PolymarketUSExchangeCredentials | PolymarketUSExchangeTrading,
        *,
        symbols: list[str] | None = None,
        firms: list[str] | None = None,
        resume_token: bytes | str | None = None,
        resume_time: int | None = None,
        **kwargs: Any,
    ):
        super().__init__(source, **kwargs)
        self.symbols = list(symbols or [])
        self.firms = list(firms or [])
        if isinstance(resume_token, bytes):
            resume_token = base64.b64encode(resume_token).decode()
        self.resume_token: str | None = resume_token or None
        """The last token, base64 as the venue's JSON mapping writes bytes."""
        self.resume_time: int | None = resume_time
        """Where to resume from without a token, in milliseconds."""
        self.last_event_time: int | None = None

    request_fields: tuple[str, ...] = ("symbols", "firms")

    def request(self) -> dict[str, Any] | None:
        request: dict[str, Any] = {"symbols": self.symbols}
        if "firms" in self.request_fields:
            request["firms"] = self.firms
        if self.resume_token:
            request["resumeToken"] = self.resume_token
        elif self.resume_time is not None:
            request["resumeTime"] = rfc3339(self.resume_time)
        if self.resume_time is None and not self.resume_token:
            # Nothing to resume from yet: a reconnect before the first token
            # replays from here.
            self.resume_time = now_ms()
        return request

    def reconcile_on_reconnect(self) -> bool:
        return False

    def classify(self, code: str | None) -> Outcome:
        if code in self.refused_token_codes and self.resume_token:
            since = self.last_event_time if self.last_event_time is not None else self.resume_time
            self.resume_token = None
            self.resume_time = since
            self.status("error", f"the venue refused the resume token ({code}); resuming from {rfc3339(since) if since else 'now'}")
            return "retry"
        return super().classify(code)

    def event_time(self, item: dict[str, Any]) -> int | None:
        return None

    def read(self, item: dict[str, Any]) -> list[Event]:
        return []

    def handle(self, message: dict[str, Any]) -> list[Event]:
        def one(item: dict[str, Any]) -> list[Event]:
            stamp = self.event_time(item)
            if stamp is not None and (self.last_event_time is None or stamp > self.last_event_time):
                self.last_event_time = stamp
            return self.read(item)

        events = self._each(message.get(self.items_field), one)
        token = message.get("resumeToken") or None
        if token and token != self.resume_token:
            self.resume_token = token
            events.append(VenueEvent(venue=VENUE, name="checkpoint", timestamp=self.last_event_time, payload={
                "stream": self.name, "resume_token": token,
            }))
        return events


class PolymarketUSExchangeDropCopyStream(_ResumableStream):
    """Every execution in the firm, from the post-trade copy: orders and fills
    for all accounts, resumable across reconnects and restarts."""

    name = "drop_copy"
    method = "polymarket.v1.DropCopyAPI/CreateDropCopySubscription"
    items_field = "executions"
    private = True
    has: dict[str, Capability] = {"watch_orders": True, "watch_my_trades": True}

    def symbols_needing_scales(self, message: dict[str, Any]) -> list[str]:
        return [str((e.get("order") or {}).get("symbol") or "") for e in message.get("executions") or [] if not _carries_scale(e.get("order") or {})]

    def event_time(self, item: dict[str, Any]) -> int | None:
        return parse_ts(item.get("transactTime"))

    def read(self, item: dict[str, Any]) -> list[Event]:
        key = item.get("id")
        if key and self.seen.seen(key):
            return []
        raw_order = item.get("order") or {}
        symbol = str(raw_order.get("symbol") or "")
        return execution_events(item, self.trading.cached_scale(symbol), account=self.account_for(str(raw_order.get("account") or "")))


class PolymarketUSExchangeTradeCaptureStream(_ResumableStream):
    """Trades as they move through clearing. Each report is the firm's fills
    with `settlement` updated; a bust arrives as the same fill `failed`."""

    name = "trade_capture"
    method = "polymarket.v1.DropCopyAPI/CreateTradeCaptureReportSubscription"
    items_field = "tradeCaptureReports"
    private = True
    has: dict[str, Capability] = {"watch_my_trades": True}

    def symbols_needing_scales(self, message: dict[str, Any]) -> list[str]:
        out = []
        for trade in message.get("tradeCaptureReports") or []:
            for role in ("aggressor", "passive"):
                order = (trade.get(role) or {}).get("order") or {}
                if order and not _carries_scale(order):
                    out.append(str(order.get("symbol") or ""))
        return out

    def event_time(self, item: dict[str, Any]) -> int | None:
        stamps = [parse_ts((item.get(role) or {}).get("transactTime")) for role in ("aggressor", "passive")]
        return max((s for s in stamps if s is not None), default=None)

    def read(self, item: dict[str, Any]) -> list[Event]:
        key = (item.get("id"), item.get("state"), item.get("reportingCounterparty"))
        if item.get("id") and self.seen.seen(key):
            return []
        return [FillEvent(venue=VENUE, fill=f) for f in trade_fills(item, self.trading.cached_scale, account_for=self.account_for)]


class PolymarketUSExchangePositionChangeStream(_ResumableStream):
    """Position changes across the firm, resumable."""

    name = "position_changes"
    method = "polymarket.v1.DropCopyAPI/CreatePositionChangeSubscription"
    items_field = "positionChanges"
    private = True
    has: dict[str, Capability] = {"watch_positions": True}

    def symbols_needing_scales(self, message: dict[str, Any]) -> list[str]:
        return [str((c.get("position") or {}).get("symbol") or "") for c in message.get("positionChanges") or []]

    def event_time(self, item: dict[str, Any]) -> int | None:
        return parse_ts(item.get("changeTime"))

    def read(self, item: dict[str, Any]) -> list[Event]:
        raw = item.get("position") or {}
        symbol = str(raw.get("symbol") or "")
        scale = _require(self.trading.cached_scale(symbol), symbol)
        return [PositionEvent(venue=VENUE, position=position_of(raw, scale, account=self.account_for(str(raw.get("account") or ""))))]


class PolymarketUSExchangeInstrumentStream(_ResumableStream):
    """Instrument state changes: pending, open, halted, closed, expired,
    terminated. Needs no participant id."""

    name = "instruments"
    method = "polymarket.v1.DropCopyAPI/CreateInstrumentStateChangeSubscription"
    items_field = "instruments"
    account_scoped = False
    request_fields = ("symbols",)
    has: dict[str, Capability] = {"watch_market_status": True}

    def event_time(self, item: dict[str, Any]) -> int | None:
        return parse_ts(item.get("updateTime"))

    def read(self, item: dict[str, Any]) -> list[Event]:
        event = market_status_of(item)
        if self.seen.seen((event.market_id, event.native, item.get("updateTime"))):
            return []
        if _carries_scale(item) and item.get("symbol"):
            # Reference data for free: remember the scales it carries.
            self.trading.remember_instrument(InstrumentScale.from_instrument(item))
        return [event]


class PolymarketUSExchangeOrderStream(_ExchangeStream):
    """This participant's orders: a snapshot of open orders on every connect,
    then executions -- acceptance, fills, cancels, replaces, rejects -- with
    fills also as `FillEvent`s, and refused cancels as
    `VenueEvent(name="cancel_rejected")`. Not replayed after a reconnect."""

    name = "orders"
    method = "polymarket.v1.OrderEntryAPI/CreateOrderSubscription"
    private = True
    data_heartbeat = True
    has: dict[str, Capability] = {"watch_orders": True, "watch_my_trades": True}

    def __init__(self, source: Any, *, symbols: list[str] | None = None, accounts: list[str] | None = None, **kwargs: Any):
        super().__init__(source, **kwargs)
        self.symbols = list(symbols or [])
        self.accounts = list(accounts or [])
        self.session_id: str | None = None

    async def watch_orders(self, market_ids: list[str] | None = None, accounts: list[str] | None = None) -> None:
        symbols = None if market_ids is None else [ids.native(VENUE, m) for m in market_ids]
        """Narrow (or widen, with `None`) what the stream carries; empty means everything."""
        symbols, accounts = list(symbols or []), list(accounts or [])
        if (symbols, accounts) != (self.symbols, self.accounts):
            self.symbols, self.accounts = symbols, accounts
            self.resubscribe()
        self.start()

    watch_my_trades = watch_orders

    def request(self) -> dict[str, Any] | None:
        return {"symbols": self.symbols, "accounts": self.accounts, "snapshotOnly": False}

    def symbols_needing_scales(self, message: dict[str, Any]) -> list[str]:
        orders = list((message.get("snapshot") or {}).get("orders") or [])
        orders += [e.get("order") or {} for e in (message.get("update") or {}).get("executions") or []]
        return [str(o.get("symbol") or "") for o in orders if not _carries_scale(o)]

    def _order(self, raw: dict[str, Any]) -> list[Event]:
        symbol = str(raw.get("symbol") or "")
        scale = _require(scale_from_order(raw, self.trading.cached_scale(symbol)), symbol)
        return [OrderEvent(venue=VENUE, order=order_of(raw, scale, account=self.account_for(str(raw.get("account") or ""))), native="snapshot")]

    def _execution(self, item: dict[str, Any]) -> list[Event]:
        if item.get("id") and self.seen.seen(item["id"]):
            return []
        raw_order = item.get("order") or {}
        return execution_events(item, self.trading.cached_scale(str(raw_order.get("symbol") or "")), account=self.account_for(str(raw_order.get("account") or "")))

    def handle(self, message: dict[str, Any]) -> list[Event]:
        self.session_id = message.get("sessionId") or self.session_id
        if "snapshot" in message:
            return self._each((message["snapshot"] or {}).get("orders"), self._order)
        update = message.get("update")
        if update is None:
            return []
        events = self._each(update.get("executions"), self._execution)
        reject = update.get("cancelReject")
        if reject:
            events.append(VenueEvent(venue=VENUE, name="cancel_rejected", timestamp=parse_ts(reject.get("transactTime")), payload={
                "order_id": reject.get("orderId") or None, "client_order_id": reject.get("clordId") or None,
                "reason": reject.get("rejectReason"), "text": reject.get("text") or "", "is_replace": bool(reject.get("isReplace")),
            }))
        return events


class PolymarketUSExchangePositionStream(_ExchangeStream):
    """Positions: a snapshot on every connect, then changes. The snapshot
    restates everything, so a reconnect needs no reconciliation; a position
    held before and missing from a new snapshot is reported flat."""

    name = "positions"
    method = "polymarket.v1.PositionAPI/CreatePositionSubscription"
    private = True
    data_heartbeat = True
    has: dict[str, Capability] = {"watch_positions": True}

    def __init__(self, source: Any, *, accounts: list[str] | None = None, **kwargs: Any):
        super().__init__(source, **kwargs)
        self.accounts = list(accounts or [])
        self._held: dict[tuple[str, str], Account | None] = {}

    def request(self) -> dict[str, Any] | None:
        return {"accounts": self.accounts}

    def reconcile_on_reconnect(self) -> bool:
        return False

    def symbols_needing_scales(self, message: dict[str, Any]) -> list[str]:
        body = message.get("snapshot") or message.get("update") or {}
        return [str(p.get("symbol") or "") for p in body.get("positions") or []]

    def _position(self, raw: dict[str, Any]) -> list[Event]:
        symbol = str(raw.get("symbol") or "")
        scale = _require(self.trading.cached_scale(symbol), symbol)
        name = str(raw.get("account") or "")
        position = position_of(raw, scale, account=self.account_for(name))
        key = (name, symbol)
        if position.side == PositionSide.FLAT:
            self._held.pop(key, None)
        else:
            self._held[key] = position.account
        return [PositionEvent(venue=VENUE, position=position)]

    def handle(self, message: dict[str, Any]) -> list[Event]:
        if "snapshot" in message:
            before = dict(self._held)
            self._held.clear()
            events = self._each((message["snapshot"] or {}).get("positions"), self._position)
            for (name, symbol), account in before.items():
                if (name, symbol) not in self._held:
                    events.append(PositionEvent(venue=VENUE, position=flat_position(account, symbol)))
            return events
        update = message.get("update")
        return self._each((update or {}).get("positions"), self._position)


class PolymarketUSExchangeMarketDataStream(_ExchangeStream):
    """Books to a depth and market statistics for up to a thousand symbols.

    Each update is taken as the whole book to the subscribed depth and
    replaces the local one, kept on the YES leg: `book("<symbol>:no")` is the
    mirrored view. Adding symbols reopens the call with the full set."""

    name = "market_data"
    method = "polymarket.v1.MarketDataSubscriptionAPI/CreateMarketDataSubscription"
    account_scoped = False
    data_heartbeat = True
    has: dict[str, Capability] = {"watch_order_book": True, "watch_ticker": True, "watch_trades": False, "watch_market_status": False}

    def __init__(self, source: Any, *, depth: int = 10, **kwargs: Any):
        super().__init__(source, **kwargs)
        self.depth = depth
        self.symbols: list[str] = []
        self.books: dict[str, LocalBook] = {}

    async def watch_order_book(self, market_ids: list[str]) -> None:
        symbols = [ids.native(VENUE, m) for m in market_ids]
        added = [s for s in dict.fromkeys(symbols) if s not in self.symbols]
        if len(self.symbols) + len(added) > MAX_SYMBOLS:
            raise ValueError(f"polymarket_us: one market-data stream carries at most {MAX_SYMBOLS} symbols; open another")
        if added:
            self.symbols.extend(added)
            self.resubscribe()
        self.start()

    watch_ticker = watch_order_book

    def book(self, market_id: str, side: str = "yes") -> LocalBook | None:
        book = self.books.get(ids.native(VENUE, market_id))
        if book is None or side != "no":
            return book
        return book.mirrored()

    def request(self) -> dict[str, Any] | None:
        if not self.symbols:
            return None
        return {"symbols": list(self.symbols), "depth": self.depth, "unaggregated": False, "snapshotOnly": False}

    async def before_call(self, request: dict[str, Any]) -> dict[str, Any] | None:
        missing = [s for s in request["symbols"] if self.trading.cached_scale(s) is None]
        if missing:
            await self.trading.load_instruments(missing, strict=False)
        unknown = [s for s in request["symbols"] if self.trading.cached_scale(s) is None]
        if unknown:
            self.status("error", f"no reference data for {unknown[:5]}; not subscribed")
            self.symbols = [s for s in self.symbols if s not in unknown]
            request = {**request, "symbols": [s for s in request["symbols"] if s not in unknown]}
        return request if request["symbols"] else None

    def on_disconnect(self) -> None:
        for book in self.books.values():
            book.invalidate()

    def handle(self, message: dict[str, Any]) -> list[Event]:
        update = message.get("update")
        if not update:
            return []
        symbol = str(update.get("symbol") or "")
        scale = _require(self.trading.cached_scale(symbol), symbol)
        stamp = parse_ts(update.get("transactTime"))
        book = self.books.setdefault(symbol, LocalBook())
        bids, asks = book_sides(update, scale)
        hidden = bool(update.get("bookHidden"))
        book.replace(bids, asks)
        book.timestamp = stamp
        if hidden:
            book.invalidate()
        levels_bids, levels_asks = book.levels()
        events: list[Event] = [BookEvent(
            venue=VENUE, market_id=ids.qualify(VENUE, symbol), kind="snapshot", bids=levels_bids,
            asks=levels_asks, best_bid=book.best_bid, best_ask=book.best_ask, timestamp=stamp,
            info={"book_hidden": True} if hidden else {},
        )]
        stats = update.get("stats")
        if stats:
            events.append(QuoteEvent(
                venue=VENUE, market_id=ids.qualify(VENUE, symbol),
                bid=book.best_bid, ask=book.best_ask,
                bid_size=book.bids.get(book.best_bid) if book.best_bid is not None else None,
                ask_size=book.asks.get(book.best_ask) if book.best_ask is not None else None,
                last=scale.price_from_wire(stats.get("lastTradePx")),
                volume=scale.qty_from_wire(stats["sharesTraded"]) if "sharesTraded" in stats else None,
                open_interest=scale.qty_from_wire(stats["openInterest"]) if "openInterest" in stats else None,
                timestamp=stamp, info=stats,
            ))
        return events


class PolymarketUSExchangeBalanceLedgerStream(_ExchangeStream):
    """Every change to one account's balance -- deposits, withdrawals,
    executions, commissions, resolutions -- as
    `VenueEvent(name="balance_ledger")` with `before`, `after` and `change`.

    Replayable by time: a reconnect asks again from the last entry's time and
    duplicates are dropped by id, so nothing is missed. Pass `resume_time=`
    (milliseconds) to replay after a restart; the venue keeps entries from
    2026-05-01."""

    name = "balance_ledger"
    method = "polymarket.v1.FundingAPI/CreateBalanceLedgerSubscription"
    private = True
    has: dict[str, Capability] = {
        # Ledger entries carry the cash balance, not what is available or
        # locked: `fetch_balance` remains the source for those.
        "watch_balance": "partial",
    }

    def __init__(
        self,
        source: Any,
        *,
        account: str | None = None,
        currency: str = "",
        entry_types: list[str] | None = None,
        resume_time: int | None = None,
        **kwargs: Any,
    ):
        super().__init__(source, **kwargs)
        self.ledger_account = account
        self.currency = currency
        self.entry_types = list(entry_types or [])
        self.resume_time = resume_time
        self._started_at: int | None = None

    def request(self) -> dict[str, Any] | None:
        request: dict[str, Any] = {"account": self.ledger_account or "", "currency": self.currency, "entryTypes": self.entry_types}
        since = self.resume_time if self.resume_time is not None else (self._started_at if self.stats.connects else None)
        if since is not None:
            request["resumeTime"] = rfc3339(since)
        if self._started_at is None:
            self._started_at = now_ms()
        return request

    async def before_call(self, request: dict[str, Any]) -> dict[str, Any] | None:
        if not request["account"]:
            self.ledger_account = await self.trading._account()
            request = {**request, "account": self.ledger_account}
        return request

    def reconcile_on_reconnect(self) -> bool:
        return False

    def _entry(self, entry: dict[str, Any]) -> list[Event]:
        stamp = parse_ts(entry.get("updateTime"))
        if stamp is not None and (self.resume_time is None or stamp > self.resume_time):
            self.resume_time = stamp
        if entry.get("id") and self.seen.seen(entry["id"]):
            return []
        return [ledger_event(entry)]

    def handle(self, message: dict[str, Any]) -> list[Event]:
        return self._each(message.get("entries"), self._entry)
