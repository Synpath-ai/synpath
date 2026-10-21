"""Polymarket (global) combo RFQs: requester, quoter and combos (docs/rfq-combo.md, P7).

`PolymarketTrading` answers the typed RFQ and combo methods through this
mixin. Sources: https://docs.polymarket.com/trading/combos/overview.md,
/trading/combos/requesters.md, /trading/combos/market-makers.md, the RFQ
OpenAPI (/api-spec/combos-rfq-openapi.yaml) and the venue's Python SDK
(`polymarket-client`, `_internal/actions/combo_rfq.py`) for the request and
response shapes.

**Combos are RFQ-only.** A combo is a conjunction of 2-50 leg outcomes (each
a market's YES or NO *position id*, which is not its CLOB token id) with its
own YES/NO position pair on the PositionManager. The venue has no endpoint
to create or look one up: an RFQ names its legs, and the answer names the
combo (`condition_id`, `yes_position_id`, `no_position_id`). So:

* a combo the venue has named is `polymarket:combo-<condition_id>`;
* before that, `create_combo(legs)` (offline, no quota) returns it under a
  *provisional* id that spells its legs, `polymarket:combo-legs-<hex>.<hex>`
  (each leg position id in hex, ascending), which `create_rfq` reads back
  with no state. The RFQ's answer then gives the real id.

**One side, one quote.** A requester asks for one direction on the combo's
YES (`side` is YES only today): BUY sized by `amount` (pUSD, fees
included), SELL sized by `quantity` (shares). The gateway holds the create
call through the quote competition and answers with the best quote, which
must be accepted within its `expires_at` (five seconds from quote
readiness; the overview says ten). The quote is `ask` (a BUY) or `bid` (a
SELL), the blended YES price; `ask_quantity`/`bid_quantity` is the shares
traded. Accepting signs an Exchange v3 order (`polymarket_v3_signing`)
with the quote's exact maker and taker amounts.

**What the gateway does not offer**, and what this adapter does instead:

* No list of a requester's RFQs and no RFQ cancel: `fetch_rfqs` lists the
  RFQs this adapter created in this process, and `cancel_rfq` forgets one
  locally (an unaccepted RFQ costs nothing; it lapses at `expires_at`).
* Status reads before an accept answer 409, so an RFQ is read from what its
  create returned until it is accepted, then from `GET /requests/{id}`.
* **No idempotency key on create.** A create that timed out may have made
  an RFQ whose id never arrived. It cannot be accepted (the id is unknown),
  so nothing trades, and it lapses within seconds; a retry may make a
  second RFQ (the cost is the 15-a-minute create budget). Guards: the
  engine journals every create (`rfq-create:...`) and raises
  `SubmissionInDoubt`, and `create_rfq` returns an open RFQ this adapter
  already holds for the same legs, direction and size instead of creating
  another.
* After a restart the adapter holds no RFQs; `remember_rfq(rfq)` re-seeds
  one from `Rfq.info` (the engine keeps it in its working set).

**Quoting runs on a WebSocket.** Quote requests arrive only on the quoter
session (`wss://combos-rfq-gateway-quoter.polymarket.com/ws/rfq`), with a
400 ms submission window; `start_rfq_session()` opens it, and
`fetch_incoming_rfqs` reads what it has received. Quotes, cancels and
last-look confirmations go over REST (`https://combos-rfq-api.polymarket.com`),
each signed with the CLOB L2 headers.

**Orders.** An RFQ trade settles on chain, not on the CLOB, so its orders
have synthetic ids: the requester's is `rfq:<rfq_id>`, a quoter's
`rfq:<rfq_id>:<quote_id>`. `fetch_order` and `fetch_my_trades` answer them
from the RFQ's state: filled once the gateway reports FILLED/CONFIRMED.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import secrets
import time
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Awaitable, Callable

from .. import ids
from ..base import AsyncHttpClient
from ..errors import (
    AuthenticationError, BadRequest, ExchangeError, ExchangeNotAvailable, MarketNotFound, NotSupported,
    RateLimitExceeded,
)
from ..types import Page
from . import polymarket_signing as sig
from . import polymarket_v3_signing as v3
from .errors import InsufficientFunds, InvalidOrder, OrderNotFound, OrderRejected, RfqNotFound
from .limiter import Priority
from .types import (
    AcceptQuote, Combo, ComboLeg, Fill, Liquidity, MakerQuote, Order, OrderStatus, OrderType, QuoteStatus, Rfq,
    RfqCloseReason, RfqQuote, RfqStatus, SettlementState, Side, TimeInForce,
)

log = logging.getLogger("synpath.trading.polymarket")

VENUE = "polymarket"

REQUESTER_URL = "https://combos-rfq-gateway-requester-api.polymarket.com"
REQUESTS_PATH = "/v1/requester/rfq/requests"
RFQ_API_URL = "https://combos-rfq-api.polymarket.com"
"""The public combo catalogue and the quoter's REST commands."""
QUOTER_WS_URL = "wss://combos-rfq-gateway-quoter.polymarket.com/ws/rfq"

MIN_LEGS, MAX_LEGS = 2, 50
E6 = Decimal(1_000_000)
BUY_PRICE_CEILING = Decimal("0.90909")
"""The quoter's BUY-request safety maximum (`QUOTED_PRICE_ABOVE_SAFETY_THRESHOLD`)."""
MAKER_QUOTE_STALE_S = 30.0
"""A quote of ours not selected this long after it was sent is read as
expired: the competition (400 ms) and the requester's window (5-10 s) are over."""

COMBO_PREFIX = "combo-"
PROVISIONAL_PREFIX = "combo-legs-"

PRE_ACCEPT = frozenset({"CREATED", "COLLECTING_QUOTES", "AWAITING_REQUESTER_ACCEPTANCE"})
IN_FLIGHT = frozenset({"AWAITING_MAKER_CONFIRMATION", "EXECUTING", "MATCHED", "MINED", "RETRYING"})
DONE = frozenset({"FILLED", "CONFIRMED"})
NO_TRADE = frozenset({"FAILED", "EXPIRED", "CANCELED", "REJECTED"})

QUOTE_STATUS = {
    "CREATED": QuoteStatus.OPEN,
    "COLLECTING_QUOTES": QuoteStatus.OPEN,
    "AWAITING_REQUESTER_ACCEPTANCE": QuoteStatus.OPEN,
    "AWAITING_MAKER_CONFIRMATION": QuoteStatus.ACCEPTED,
    # The maker confirmed (or has no last look); the trade is on its way to
    # the chain. Not a fill until FILLED/CONFIRMED.
    "EXECUTING": QuoteStatus.CONFIRMED,
    "MATCHED": QuoteStatus.CONFIRMED,
    "MINED": QuoteStatus.CONFIRMED,
    "RETRYING": QuoteStatus.CONFIRMED,
    "FILLED": QuoteStatus.EXECUTED,
    "CONFIRMED": QuoteStatus.EXECUTED,
    "FAILED": QuoteStatus.FAILED,
    "REJECTED": QuoteStatus.FAILED,
    "EXPIRED": QuoteStatus.EXPIRED,
    "CANCELED": QuoteStatus.CANCELED,
}
"""The gateway's RFQ status as the status of its one quote. FAILED with
`MAKER_DECLINED` reads as `canceled` (declined in the last look)."""

CLOSE_REASON = {
    "FILLED": RfqCloseReason.EXECUTED,
    "CONFIRMED": RfqCloseReason.EXECUTED,
    "EXPIRED": RfqCloseReason.EXPIRED,
    "CANCELED": RfqCloseReason.CANCELED,
}


# ---------------------------------------------------------------------------
# Ids and amounts
# ---------------------------------------------------------------------------

def combo_market_id(condition_id: str) -> str:
    return ids.qualify(VENUE, f"{COMBO_PREFIX}{condition_id.lower()}")


def provisional_combo_id(position_ids: list[str]) -> str:
    """A combo's id before the venue names it: its leg position ids, in hex,
    ascending, so the id alone gives the legs back."""
    return ids.qualify(VENUE, PROVISIONAL_PREFIX + ".".join(format(int(p), "x") for p in sorted_legs(position_ids)))


def legs_of_provisional(combo_id: str) -> list[str] | None:
    native = ids.native(VENUE, combo_id)
    if not native.startswith(PROVISIONAL_PREFIX):
        return None
    try:
        return sorted_legs([str(int(part, 16)) for part in native[len(PROVISIONAL_PREFIX):].split(".")])
    except ValueError:
        raise InvalidOrder(f"polymarket: {combo_id!r} is not a combo id") from None


def is_combo_id(market_id: str) -> bool:
    try:
        return ids.native(VENUE, market_id).startswith(COMBO_PREFIX)
    except BadRequest:
        return False


def sorted_legs(position_ids: list[str]) -> list[str]:
    """Leg position ids as the gateway takes them: decimal, unique, ascending."""
    out = []
    for raw in position_ids:
        text = str(raw).strip()
        if not text.isdecimal():
            raise InvalidOrder(f"polymarket: a combo leg is a numeric position id, not {raw!r}")
        out.append(str(int(text)))
    if len(set(out)) != len(out):
        raise InvalidOrder("polymarket: a combo names each leg once")
    if not MIN_LEGS <= len(out) <= MAX_LEGS:
        raise InvalidOrder(f"polymarket: a combo has {MIN_LEGS} to {MAX_LEGS} legs, got {len(out)}")
    return sorted(out, key=int)


def to_e6(value: Decimal, what: str) -> int:
    scaled = Decimal(value) * E6
    if scaled != scaled.to_integral_value():
        raise InvalidOrder(f"polymarket: {what} {value} has more than six decimals")
    if scaled <= 0:
        raise InvalidOrder(f"polymarket: {what} must be positive, got {value}")
    return int(scaled)


def from_e6(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value)) / E6


def _now_ms() -> int:
    return int(time.time() * 1000)


def quote_id_for(key: str | None) -> str:
    """A REST quote id: `quote_` and 32 lowercase hex, as the venue makes
    them. Derived from the engine's key when there is one, so a retried send
    reuses its id."""
    if not key:
        return "quote_" + secrets.token_hex(16)
    return "quote_" + hashlib.sha256(key.encode()).hexdigest()[:32]


def taker_order_id(rfq_id: str) -> str:
    return f"rfq:{rfq_id}"


def maker_order_id(rfq_id: str, quote_id: str) -> str:
    return f"rfq:{rfq_id}:{quote_id}"


def parse_rfq_order_id(order_id: str) -> tuple[str, str | None] | None:
    """`(rfq_id, quote_id or None)` for an RFQ trade's synthetic order id."""
    if not order_id.startswith("rfq:"):
        return None
    rest = order_id[4:]
    rfq_id, _, quote_id = rest.partition(":")
    return (rfq_id, quote_id or None) if rfq_id else None


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

REJECT_CODES = {
    "EXPIRED_RFQ", "INVALID_RFQ_STATE", "QUOTE_MISMATCH", "REQUEST_FAILED", "INVALID_SIGNATURE",
    "INVALID_ACCEPTANCE", "SUBMISSION_WINDOW_CLOSED", "MAKER_QUOTE_LIMITED", "MAKER_NOT_REQUIRED",
    "MAKER_ALREADY_RESPONDED", "INVALID_CONFIRMATION", "INVALID_QUOTE", "UNAUTHORIZED_ROLE",
}


def rfq_error_of(exc: ExchangeError | RateLimitExceeded | ExchangeNotAvailable) -> Exception:
    """The gateway's `{error, code}` body as a typed error. Codes are
    strings, and new ones appear: an unknown code keeps its HTTP mapping."""
    body = getattr(exc, "body", None)
    code = str(body.get("code") or "") if isinstance(body, dict) else ""
    detail = str(body.get("error") or "") if isinstance(body, dict) else ""
    message = f"polymarket: {detail or exc}" + (f" ({code})" if code else "")
    status = getattr(exc, "status", None)
    if code == "UNKNOWN_RFQ" or isinstance(exc, MarketNotFound):
        return RfqNotFound(message, body=body, status=status)
    if code == "UNSUPPORTED_REQUESTER_SIGNATURE_TYPE":
        return InvalidOrder(
            message + " -- combo RFQs refuse EOA wallets; use a Deposit Wallet (3), a proxy (1) or a Safe (2)",
            body=body, status=status)
    if code in ("PRE_EXECUTION_BALANCE_RESERVATION_FAILED", "BALANCE_VALIDATION_FAILED"):
        return InsufficientFunds(message, body=body, status=status)
    if code == "ALLOWANCE_VALIDATION_FAILED":
        return InsufficientFunds(message + " -- an approval is missing; run approve_combo_trading() once",
                                 body=body, status=status)
    if isinstance(exc, (AuthenticationError, RateLimitExceeded, ExchangeNotAvailable)):
        # 401/403 (UNAUTHENTICATED, ADDRESS_MISMATCH), 429 and 5xx
        # (SERVICE_UNAVAILABLE, TRADE_SUBMISSION_FAILED) keep their types: a
        # 5xx is in doubt, and a retried accept never executes twice.
        if code and not str(exc).endswith(f"({code})"):
            exc.args = (f"{exc} ({code})",)
        return exc
    if code in REJECT_CODES or (code and code.startswith(("SIGNED_ORDER_", "MISSING_", "QUOTED_PRICE_", "ORDER_SIDE_"))):
        return OrderRejected(message, reason=code.lower(), info=body if isinstance(body, dict) else {},
                             body=body, status=status)
    if isinstance(exc, BadRequest):
        return InvalidOrder(message, body=body, status=status)
    return exc


# ---------------------------------------------------------------------------
# What the adapter holds about each RFQ
# ---------------------------------------------------------------------------

@dataclass
class RfqRecord:
    """One RFQ this adapter created, as the gateway described it."""

    rfq_id: str
    request: dict[str, Any]
    """The create body as sent."""
    response: dict[str, Any]
    """The create answer: status, expiry, the echoed request, the quote."""
    status: str
    created_at: int
    combo: Combo | None = None
    accept_body: dict[str, Any] | None = None
    """The signed accept, kept so a retry sends the same order."""
    accept_side: Side | None = None
    accepted_at: int | None = None
    order_hash: str | None = None
    last: dict[str, Any] = field(default_factory=dict)
    """The latest status answer after the accept."""
    canceled: bool = False

    @property
    def echoed(self) -> dict[str, Any]:
        return self.response.get("request") or {}

    @property
    def quote(self) -> dict[str, Any] | None:
        return self.response.get("quote") or None

    @property
    def direction(self) -> str:
        return str(self.echoed.get("direction") or self.request.get("direction"))

    @property
    def expires_at(self) -> int | None:
        value = self.response.get("expires_at")
        return int(value) if value not in (None, "") else None

    @property
    def error(self) -> dict[str, Any] | None:
        return self.last.get("error") or self.response.get("error") or None

    def wire_status(self, now_ms: int | None = None) -> str:
        """The gateway's status, with an unaccepted quote past its deadline
        read as EXPIRED and a locally cancelled one as CANCELED."""
        if self.accept_body is None and self.status in PRE_ACCEPT:
            if self.canceled:
                return "CANCELED"
            if self.expires_at is not None and (now_ms or _now_ms()) >= self.expires_at:
                return "EXPIRED"
        return self.status


@dataclass
class MakerRecord:
    """One quote of ours on someone else's RFQ."""

    quote: RfqQuote
    rfq_id: str
    request: dict[str, Any]
    sent_at: int
    status: str = "OPEN"
    """OPEN, ACCEPTED (a last-look request), CONFIRMED (handed to execution),
    EXECUTED, FAILED, CANCELED, EXPIRED."""
    fill_size_e6: int | None = None
    tx_hash: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)


MAKER_STATUS = {
    "OPEN": QuoteStatus.OPEN, "ACCEPTED": QuoteStatus.ACCEPTED, "CONFIRMED": QuoteStatus.CONFIRMED,
    "EXECUTED": QuoteStatus.EXECUTED, "FAILED": QuoteStatus.FAILED, "CANCELED": QuoteStatus.CANCELED,
    "EXPIRED": QuoteStatus.EXPIRED,
}
EXECUTION_TO_MAKER = {"MATCHED": "CONFIRMED", "MINED": "CONFIRMED", "RETRYING": "CONFIRMED",
                      "EXECUTING": "CONFIRMED", "CONFIRMED": "EXECUTED", "FILLED": "EXECUTED", "FAILED": "FAILED"}


# ---------------------------------------------------------------------------
# The quoter session (WebSocket)
# ---------------------------------------------------------------------------

class PolymarketRfqSession:
    """The quoter's WebSocket: authenticates, then hands every message to
    the adapter. Quote requests (`RFQ_REQUEST`) become incoming RFQs; a
    last-look request marks our quote accepted; execution updates and trade
    broadcasts move our quotes on. The gateway's ping frames are answered by
    the WebSocket library. `on_rfq` (optional) is called with each new
    incoming `Rfq`, for a quoter that must answer inside 400 ms."""

    def __init__(self, adapter: "PolymarketRfqMixin", *, url: str = QUOTER_WS_URL,
                 connect: Callable[[str], Any] | None = None,
                 on_rfq: Callable[[Rfq], Awaitable[None] | None] | None = None):
        self.adapter = adapter
        self.url = url
        self._connect = connect
        self.on_rfq = on_rfq
        self.authenticated: bool | None = None
        self.auth_error: str | None = None
        self.role: str | None = None
        self.errors: list[dict[str, Any]] = []
        self._task: asyncio.Task | None = None
        self._ws: Any = None

    def auth_message(self) -> dict[str, Any]:
        a = self.adapter
        signer, maker = v3.order_identity(a.credentials.signature_type, signer=a.signer.address, wallet=a.wallet)
        return {
            "type": "auth",
            "auth": {"apiKey": a._api_key or "", "secret": a._api_secret or "", "passphrase": a._api_passphrase or ""},
            "identity": {"signer_address": signer, "maker_address": maker,
                         "signature_type": a.credentials.signature_type},
        }

    async def run(self) -> None:
        """Connect, authenticate, read until closed."""
        await self.adapter.ensure_api_credentials()
        connect = self._connect
        if connect is None:
            import websockets

            connect = websockets.connect
        async with connect(self.url) as ws:
            self._ws = ws
            await ws.send(json.dumps(self.auth_message()))
            async for raw in ws:
                await self.handle(json.loads(raw) if isinstance(raw, (str, bytes)) else raw)
        self._ws = None

    def start(self) -> asyncio.Task:
        if self._task is None or self._task.done():
            self._task = asyncio.get_running_loop().create_task(self.run(), name="synpath-polymarket-rfq")
        return self._task

    async def close(self) -> None:
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:  # pragma: no cover - closing a dead socket
                pass
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    async def handle(self, message: dict[str, Any]) -> None:
        kind = message.get("type")
        a = self.adapter
        if kind == "auth":
            self.authenticated = bool(message.get("success"))
            self.role = message.get("role")
            self.auth_error = None if self.authenticated else str(message.get("error") or "refused")
            if not self.authenticated:
                log.error("polymarket: the RFQ quoter session was refused: %s", self.auth_error)
        elif kind == "RFQ_REQUEST":
            rfq = a._incoming_from(message)
            if rfq is not None:
                # The engine's listeners first (they are scheduled, never
                # awaited here, so the socket keeps reading), then `on_rfq`.
                a._tell_rfq_listeners(rfq)
                if self.on_rfq is not None:
                    result = self.on_rfq(rfq)
                    if asyncio.iscoroutine(result):
                        await result
        elif kind == "RFQ_CONFIRMATION_REQUEST":
            a._maker_event(message, "ACCEPTED")
        elif kind == "RFQ_EXECUTION_UPDATE":
            a._maker_execution(message)
        elif kind == "RFQ_TRADE":
            a._maker_trade(message)
        elif kind == "RFQ_ERROR":
            self.errors.append(message)
            log.warning("polymarket: RFQ session error on %s: %s (%s)", message.get("request_type"),
                        message.get("error"), message.get("code"))
            a._maker_error(message)


# ---------------------------------------------------------------------------
# The mixin
# ---------------------------------------------------------------------------

class PolymarketRfqMixin:
    """Combo RFQs on `PolymarketTrading`. See the module doc."""

    # Set by `PolymarketTrading.__init__` through `_init_rfq`.
    requester: AsyncHttpClient
    rfq_api: AsyncHttpClient

    def _init_rfq(self, shared: Any, *, requester_url: str = REQUESTER_URL, rfq_api_url: str = RFQ_API_URL) -> None:
        self.requester = AsyncHttpClient(requester_url, limiter=None, client=shared, venue=VENUE)
        self.rfq_api = AsyncHttpClient(rfq_api_url, limiter=None, client=shared, venue=VENUE)
        self._rfq_records: dict[str, RfqRecord] = {}
        self._quote_to_rfq: dict[str, str] = {}
        self._combos: dict[str, Combo] = {}
        """By combo id (real and provisional) and by its sorted leg set."""
        self._positions: dict[str, tuple[str, str]] = {}
        """Leg position id -> (Synpath market id, outcome)."""
        self._position_of: dict[tuple[str, str], str] = {}
        self._incoming: dict[str, tuple[Rfq, dict[str, Any]]] = {}
        self._maker: dict[str, MakerRecord] = {}
        self.rfq_session: PolymarketRfqSession | None = None
        self._rfq_listeners: list[Callable[[Rfq], Any]] = []
        self._listener_tasks: set[asyncio.Task] = set()

    # -- signing and calls ----------------------------------------------------

    def _rfq_identity(self) -> tuple[str, str, int]:
        kind = self.credentials.signature_type
        signer, maker = v3.order_identity(kind, signer=self.signer.address, wallet=self.wallet)
        return signer, maker, kind

    async def _gateway(
        self, client: AsyncHttpClient, method: str, path: str, *, body: Any = None,
        kind: str = "read", priority: Priority = Priority.NORMAL,
    ) -> Any:
        """One CLOB-L2-signed call to an RFQ gateway, errors typed."""
        from .polymarket import serialize

        await self.ensure_api_credentials()
        await self.limiter.acquire(cost=1, kind=kind, priority=priority)  # type: ignore[arg-type]
        text = serialize(body) if body is not None else None
        headers = sig.l2_headers(
            address=self.signer.address, api_key=self._api_key or "", secret=self._api_secret or "",
            passphrase=self._api_passphrase or "", timestamp=self._now_s(), method=method, path=path, body=text,
        )
        if text is not None:
            headers["Content-Type"] = "application/json"
        headers["Accept"] = "application/json"
        try:
            return await client.request(method, path, content=text, headers=headers)
        except (ExchangeError, RateLimitExceeded, ExchangeNotAvailable) as exc:
            raise rfq_error_of(exc) from None

    # -- legs and combos ------------------------------------------------------

    def remember_combo_market(self, raw: dict[str, Any]) -> None:
        """Index one combo-enabled market (a catalogue row, or a Gamma row
        with `positionIds`): position id <-> market and outcome."""
        gamma_id = str(raw.get("id") or "")
        positions = raw.get("position_ids") or raw.get("positionIds") or []
        if isinstance(positions, str):
            positions = json.loads(positions or "[]")
        if not gamma_id or len(positions) != 2:
            return
        market = ids.qualify(VENUE, gamma_id)
        for outcome, pid in zip(("yes", "no"), positions):
            self._positions[str(int(pid))] = (market, outcome)
            self._position_of[(market, outcome)] = str(int(pid))

    async def fetch_combo_markets(
        self, *, limit: int | None = None, cursor: str | None = None, exclude: list[str] | None = None,
    ) -> Page[dict[str, Any]]:
        """The public catalogue of combo-enabled markets, by volume (`GET
        /v1/rfq/combo-markets`, no auth). `position_ids[0]` is the YES leg,
        `[1]` the NO leg; every row is indexed for leg lookups."""
        await self.limiter.acquire(cost=1, kind="read")
        raw = await self.rfq_api.get("/v1/rfq/combo-markets", {
            "limit": limit, "cursor": cursor, "exclude": ",".join(exclude) if exclude else None,
        })
        rows = (raw or {}).get("markets") or []
        for row in rows:
            self.remember_combo_market(row)
        return Page(rows, next_cursor=(raw or {}).get("next_cursor") or None)

    async def leg_position_id(self, leg: ComboLeg) -> str:
        """A leg's position id: `leg.info["position_id"]`, else the index,
        else the market's Gamma row (`positionIds`, UNVERIFIED on `/markets/{id}`)."""
        given = leg.info.get("position_id")
        if given:
            pid = str(int(str(given)))
            self._positions.setdefault(pid, (leg.market_id, leg.outcome))
            self._position_of.setdefault((leg.market_id, leg.outcome), pid)
            return pid
        found = self._position_of.get((leg.market_id, leg.outcome))
        if found:
            return found
        native = ids.native(VENUE, leg.market_id)
        if native.isdecimal():
            await self.limiter.acquire(cost=1, kind="read")
            try:
                raw = await self.gamma.get(f"/markets/{native}")
            except MarketNotFound:
                raw = None
            if isinstance(raw, dict):
                self.remember_combo_market(raw)
                found = self._position_of.get((leg.market_id, leg.outcome))
                if found:
                    return found
        raise InvalidOrder(
            f"polymarket: no combo position id known for {leg.market_id} {leg.outcome}; load the catalogue "
            "(fetch_combo_markets) or give leg.info['position_id']"
        )

    def _leg_of(self, position_id: str) -> ComboLeg:
        known = self._positions.get(position_id)
        if known is None:
            # Not in the index: named by its position id until it is resolved.
            return ComboLeg(market_id=ids.qualify(VENUE, f"position-{position_id}"), outcome="yes",
                            info={"position_id": position_id, "resolved": False})
        return ComboLeg(market_id=known[0], outcome=known[1], info={"position_id": position_id})

    async def _resolve_positions(self, position_ids: list[str]) -> None:
        """Index unknown leg position ids through Gamma (`GET /markets/keyset?position_ids=`)."""
        missing = [p for p in position_ids if p not in self._positions]
        if not missing:
            return
        await self.limiter.acquire(cost=1, kind="read")
        try:
            raw = await self.gamma.get("/markets/keyset", [("position_ids", p) for p in missing])
        except Exception as exc:  # display only: a leg stays named by its position id
            log.warning("polymarket: resolving combo legs %s failed: %s", missing, exc)
            return
        rows = raw if isinstance(raw, list) else (raw or {}).get("markets") or (raw or {}).get("data") or []
        for row in rows:
            if isinstance(row, dict):
                self.remember_combo_market(row)

    def _combo(self, position_ids: list[str], *, condition_id: str | None = None,
               extra: dict[str, Any] | None = None) -> Combo:
        legs = sorted_legs(position_ids)
        market_id = combo_market_id(condition_id) if condition_id else provisional_combo_id(legs)
        info = {"leg_position_ids": legs, **(extra or {})}
        if condition_id is None:
            info["provisional"] = True
        combo = Combo(market_id=market_id, venue=VENUE, legs=[self._leg_of(p) for p in legs],
                      status="open" if condition_id else None, info=info)
        self._combos[market_id] = combo
        if condition_id:
            self._combos["legs:" + ",".join(legs)] = combo
            self._combos[provisional_combo_id(legs)] = combo
        return combo

    async def create_combo(self, legs: list[ComboLeg]) -> Combo:
        """The combo for these legs. The venue has no create (an RFQ names a
        combo), so nothing is sent and no quota is used: each leg's position
        id is resolved, and the combo comes back under its venue id if an RFQ
        has named it, else under its provisional id (module doc)."""
        if not MIN_LEGS <= len(legs) <= MAX_LEGS:
            raise InvalidOrder(f"polymarket: a combo has {MIN_LEGS} to {MAX_LEGS} legs, got {len(legs)}")
        if len({leg.market_id for leg in legs}) != len(legs):
            raise InvalidOrder("polymarket: a combo names each market once")
        positions = sorted_legs([await self.leg_position_id(leg) for leg in legs])
        known = self._combos.get("legs:" + ",".join(positions))
        return known or self._combo(positions)

    async def fetch_combo(self, combo_id: str) -> Combo:
        """A combo this adapter has seen, or one named by its provisional id."""
        combo_id = ids.qualify(VENUE, combo_id)
        if combo_id in self._combos:
            return self._combos[combo_id]
        legs = legs_of_provisional(combo_id)
        if legs is not None:
            await self._resolve_positions(legs)
            return self._combo(legs)
        raise RfqNotFound(f"polymarket: combo {combo_id} has not been seen in this session; the venue has no "
                          "combo lookup -- use its legs (create_combo)")

    async def lookup_combo(self, legs: list[ComboLeg]) -> Combo | None:
        raise NotSupported(
            "polymarket: the venue has no combo lookup; create_combo answers offline (no quota) with the "
            "combo's id once an RFQ has named it, else its provisional id"
        )

    async def approve_combo_trading(self, *, include_router: bool = False,
                                    include_auto_redeem: bool = False) -> dict[str, Any]:
        """Approve the combo contracts from the wallet (`maker_address`),
        once per wallet, through the existing wallet-call path.

        Always: pUSD `approve(Exchange v3)` and PositionManager
        `setApprovalForAll(Exchange v3)`, which a quoter's orders need to move
        collateral and combo positions. `include_router` adds pUSD
        `approve(Router)` and PositionManager `setApprovalForAll(Router)`, for
        splitting pUSD into positions and managing or redeeming them through
        the Router; `include_auto_redeem` adds PositionManager
        `setApprovalForAll(AutoRedeemer)`, for automatic redemption. The
        venue's docs list these for market makers; whether a requester needs
        any of them is UNVERIFIED (docs/rfq-combo.md)."""
        return await self.execute_wallet_calls(
            v3.combo_approval_calls(include_router=include_router, include_auto_redeem=include_auto_redeem),
            description="Approve Combo RFQ contracts")

    # -- normalisers ----------------------------------------------------------

    def _rfq_from_record(self, rec: RfqRecord) -> Rfq:
        wire = rec.wire_status()
        echoed = {**rec.request, **rec.echoed}
        condition = echoed.get("condition_id")
        combo = rec.combo
        if condition and (combo is None or combo.market_id != combo_market_id(condition)):
            extra = {k: echoed[k] for k in ("condition_id", "yes_position_id", "no_position_id") if echoed.get(k)}
            combo = self._combos.get(combo_market_id(condition)) or self._combo(
                echoed.get("leg_position_ids") or rec.request["leg_position_ids"], condition_id=condition, extra=extra)
            rec.combo = combo
        size = echoed.get("requested_size") or {}
        value = from_e6(size.get("value_e6"))
        closed = wire in DONE or wire in NO_TRADE
        return Rfq(
            id=rec.rfq_id, venue=VENUE, account=self.account,
            combo_id=combo.market_id if combo else provisional_combo_id(rec.request["leg_position_ids"]),
            combo=combo,
            outcome="yes" if str(echoed.get("side") or "YES") == "YES" else "no",
            action="buy" if rec.direction == "BUY" else "sell",
            amount=value if size.get("unit") == "notional" else None,
            quantity=value if size.get("unit") == "shares" else None,
            status=RfqStatus.CLOSED if closed else RfqStatus.OPEN,
            closed_reason=CLOSE_REASON.get(wire) if closed else None,
            mine=True, created_at=int(echoed.get("created_at") or rec.created_at),
            expires_at=rec.expires_at, updated_at=rec.accepted_at,
            info={"request": rec.request, "response": rec.response, "status": wire, **({"last": rec.last} if rec.last else {}),
                  **({"error": rec.error} if rec.error else {})},
        )

    def _quote_from_record(self, rec: RfqRecord) -> RfqQuote | None:
        quote = rec.quote
        if not quote:
            return None
        wire = rec.wire_status()
        status = QUOTE_STATUS.get(wire, QuoteStatus.OPEN)
        error = rec.error or {}
        if wire == "FAILED" and error.get("code") == "MAKER_DECLINED":
            status = QuoteStatus.CANCELED
        price = from_e6(quote.get("blended_price_e6"))
        maker_amount, taker_amount = int(quote["maker_amount_e6"]), int(quote["taker_amount_e6"])
        buy = rec.direction == "BUY"
        shares = from_e6(taker_amount if buy else maker_amount)
        combo = rec.combo
        accepted = rec.accept_side if rec.accept_body is not None else None
        return RfqQuote.from_venue(dict(
            id=str(quote.get("quote_id") or ""), rfq_id=rec.rfq_id, venue=VENUE,
            market_id=combo.market_id if combo else None,
            ask=price if buy else None, bid=None if buy else price,
            ask_quantity=shares if buy else None, bid_quantity=None if buy else shares,
            status=status, accepted_side=accepted, expires_at=rec.expires_at,
            taker_order_id=taker_order_id(rec.rfq_id) if status == QuoteStatus.EXECUTED else None,
            mine=False, created_at=int(rec.echoed.get("created_at") or rec.created_at), updated_at=rec.accepted_at,
            info={"quote": quote, "status": wire, "direction": rec.direction,
                  "total_required": str(from_e6(quote.get("total_required_e6"))),
                  "net_receive": str(from_e6(quote.get("net_receive_e6"))),
                  **({"order_hash": rec.order_hash} if rec.order_hash else {}),
                  **({"taker_order_hash": rec.last.get("taker_order_hash")} if rec.last.get("taker_order_hash") else {}),
                  **({"tx_hash": rec.last.get("tx_hash")} if rec.last.get("tx_hash") else {}),
                  **({"error": error} if error else {})},
        ))

    def remember_rfq(self, rfq: Rfq) -> None:
        """Re-seed an RFQ this adapter created (after a restart) from the
        `Rfq` it returned, whose `info` keeps the request and the answer."""
        request, response = rfq.info.get("request"), rfq.info.get("response")
        if not isinstance(request, dict) or not isinstance(response, dict):
            raise BadRequest("polymarket: this Rfq has no request and response in info to rebuild from")
        rec = RfqRecord(rfq_id=rfq.id, request=request, response=response, status=str(response.get("status") or ""),
                        created_at=rfq.created_at or _now_ms(), combo=rfq.combo)
        if rfq.info.get("status") not in (None, *PRE_ACCEPT):
            # It was accepted: the status read works now.
            rec.accept_body = {"quote_id": (response.get("quote") or {}).get("quote_id")}
            rec.accept_side = Side.BUY if rec.direction == "BUY" else Side.SELL
            rec.status = str(rfq.info.get("status"))
        self._keep(rec)

    def restore_rfqs(self, rfqs: list[Rfq], quotes: list[RfqQuote] | None = None) -> None:
        """After a restart (the engine calls this from `recover()`): re-seed
        the RFQs this account created, since the gateway lists none, and
        the quotes whose accept or confirm was in doubt.

        * An RFQ already held is kept as it is (this process knows more).
        * A quote of someone else's (`mine=False`) on one of those RFQs,
          reported `accepted` or later, marks that RFQ accepted: its status
          is then read from `GET /requests/{id}`, so the accept is followed
          to FILLED or FAILED. If the accept never reached the gateway, the
          read answers 409 and the quote stays `open` (never accepted).
        * A quote of ours (`mine=True`, the `RfqQuote` that `send_quote`
          returned, with `info["request"]`) is re-seeded at its status, so a
          confirm in doubt reads `accepted`/`confirmed` rather than unknown.
          Its execution arrives only on the quoter session."""
        for rfq in rfqs:
            if rfq.id in self._rfq_records:
                continue
            try:
                self.remember_rfq(rfq)
            except BadRequest as exc:
                log.warning("polymarket: RFQ %s cannot be re-seeded: %s", rfq.id, exc)
        for quote in quotes or ():
            if quote.mine:
                self._remember_maker_quote(quote)
                continue
            rec = self._rfq_records.get(quote.rfq_id)
            if rec is None or rec.accept_body is not None or quote.status == QuoteStatus.OPEN:
                continue
            raw = rec.quote or {}
            if str(raw.get("quote_id") or "") != quote.id:
                continue
            rec.accept_body = {"quote_id": quote.id, "restored": True}
            rec.accept_side = quote.accepted_side or (Side.BUY if rec.direction == "BUY" else Side.SELL)
            rec.accepted_at = quote.updated_at or rec.created_at

    def _remember_maker_quote(self, quote: RfqQuote) -> None:
        if quote.id in self._maker:
            return
        request = quote.info.get("request")
        if not isinstance(request, dict) or not request.get("size_e6"):
            log.warning("polymarket: our quote %s has no request in info; it cannot be re-seeded", quote.id)
            return
        status = {QuoteStatus.OPEN: "OPEN", QuoteStatus.ACCEPTED: "ACCEPTED", QuoteStatus.CONFIRMED: "CONFIRMED",
                  QuoteStatus.EXECUTED: "EXECUTED", QuoteStatus.FAILED: "FAILED", QuoteStatus.CANCELED: "CANCELED",
                  QuoteStatus.EXPIRED: "EXPIRED"}[quote.status]
        info = {k: v for k, v in quote.info.items()
                if k not in ("status", "confirmation_request", "execution", "confirm", "cancel", "restored")}
        base = quote.model_copy(update={"status": QuoteStatus.OPEN, "accepted_side": None, "info": info})
        held = MakerRecord(quote=base, rfq_id=quote.rfq_id, request=request, sent_at=quote.created_at or _now_ms(),
                           status=status, detail={"restored": True})
        shares = quote.ask_quantity if quote.ask is not None else quote.bid_quantity
        if shares is not None and status != "OPEN":
            size = int(Decimal(shares) * E6)
            if size != int(request["size_e6"]):
                held.fill_size_e6 = size       # the part the requester took (`fill_size_e6`)
        if quote.confirm_by is not None:
            held.detail["confirm_by"] = int(quote.confirm_by)
        self._maker[quote.id] = held

    def _keep(self, rec: RfqRecord) -> None:
        self._rfq_records[rec.rfq_id] = rec
        quote = rec.quote
        if quote and quote.get("quote_id"):
            self._quote_to_rfq[str(quote["quote_id"])] = rec.rfq_id

    # -- requester ------------------------------------------------------------

    async def create_rfq(
        self, instrument_id: str, *, quantity: Decimal | None = None, amount: Decimal | None = None,
        outcome: str | None = None, action: str | None = None, rest_remainder: bool = False,
        combo: Combo | None = None,
    ) -> Rfq:
        """`POST /v1/requester/rfq/requests`: ask the quoters for one side of
        a combo's YES. `action="buy"` is sized by `amount` (pUSD, fees
        included), `action="sell"` by `quantity` (shares). `instrument_id` is
        a combo id (venue or provisional); `combo` gives the legs instead.
        The gateway answers after the competition with the best quote, which
        `fetch_quotes` returns; an answer with no usable quote is a closed RFQ
        (`info["error"]`, e.g. `NO_QUOTES`). No idempotency key: see the module doc."""
        v3.check_requester(self.credentials.signature_type)
        if rest_remainder:
            raise InvalidOrder("polymarket: a combo RFQ cannot rest a remainder; there is no combo book")
        if outcome not in (None, "yes"):
            raise InvalidOrder("polymarket: a combo RFQ trades the combo's YES only (the venue's side is YES today)")
        if action not in ("buy", "sell"):
            raise InvalidOrder("polymarket: a combo RFQ asks one side: give action='buy' or 'sell'")
        if action == "buy":
            if amount is None or quantity is not None:
                raise InvalidOrder("polymarket: a BUY combo RFQ is sized by amount (pUSD, fees included), not quantity")
            requested = {"unit": "notional", "value_e6": str(to_e6(amount, "amount"))}
        else:
            if quantity is None or amount is not None:
                raise InvalidOrder("polymarket: a SELL combo RFQ is sized by quantity (shares), not amount")
            requested = {"unit": "shares", "value_e6": str(to_e6(quantity, "quantity"))}
        legs = await self._legs_for(instrument_id, combo)
        direction = "BUY" if action == "buy" else "SELL"
        # The guard for a create with no idempotency key: an open RFQ we
        # already hold for the same request is returned, not made again.
        for held in (await self.fetch_rfqs(status=RfqStatus.OPEN)):
            rec = self._rfq_records.get(held.id)
            if rec is not None and rec.accept_body is None and rec.request.get("leg_position_ids") == legs \
                    and rec.request.get("direction") == direction and rec.request.get("requested_size") == requested:
                log.info("polymarket: RFQ %s is already open for this request; returned instead of a new one", rec.rfq_id)
                return held
        await self._resolve_positions(legs)
        signer, maker, kind = self._rfq_identity()
        body = {
            "signer_address": signer, "maker_address": maker, "signature_type": kind,
            "leg_position_ids": legs, "direction": direction, "side": "YES", "requested_size": requested,
        }
        raw = await self._gateway(self.requester, "POST", REQUESTS_PATH, body=body, kind="write")
        if not isinstance(raw, dict) or not raw.get("rfq_id"):
            raise ExchangeError("polymarket: the RFQ create answered with no rfq_id", body=raw)
        provisional = combo if combo is not None and not combo.info.get("condition_id") else None
        rec = RfqRecord(rfq_id=str(raw["rfq_id"]), request=body, response=raw, status=str(raw.get("status") or ""),
                        created_at=_now_ms(), combo=None)
        self._keep(rec)
        rfq = self._rfq_from_record(rec)
        if rec.combo is None:
            rec.combo = provisional or self._combo(legs)
            rfq = self._rfq_from_record(rec)
        return rfq

    async def _legs_for(self, instrument_id: str, combo: Combo | None) -> list[str]:
        if combo is not None:
            given = combo.info.get("leg_position_ids")
            if given:
                return sorted_legs(list(given))
            return sorted_legs([await self.leg_position_id(leg) for leg in combo.legs])
        instrument_id = ids.qualify(VENUE, instrument_id)
        known = self._combos.get(instrument_id)
        if known is not None:
            return sorted_legs(list(known.info.get("leg_position_ids") or []))
        legs = legs_of_provisional(instrument_id)
        if legs is not None:
            return legs
        if is_combo_id(instrument_id):
            raise InvalidOrder(f"polymarket: combo {instrument_id} has not been seen in this session; give combo= "
                               "(its legs) or its provisional id from create_combo")
        raise InvalidOrder("polymarket: RFQs quote combos only; a single market trades on the CLOB")

    def _record(self, rfq_id: str | None = None, quote_id: str | None = None) -> RfqRecord:
        rec = self._rfq_records.get(rfq_id or "") or self._rfq_records.get(self._quote_to_rfq.get(quote_id or "", ""))
        if rec is None:
            raise RfqNotFound(
                f"polymarket: RFQ {rfq_id or quote_id} was not created by this adapter in this process; the gateway "
                "lists no RFQs and reads none before its accept (remember_rfq re-seeds one)"
            )
        return rec

    async def _refresh(self, rec: RfqRecord) -> RfqRecord:
        """Read an accepted RFQ's status (`GET /requests/{id}`); before the
        accept the gateway answers 409, so the create's answer stands."""
        if rec.accept_body is None or rec.status in DONE or rec.status in NO_TRADE:
            return rec
        try:
            raw = await self._gateway(self.requester, "GET", f"{REQUESTS_PATH}/{rec.rfq_id}")
        except (OrderRejected, InvalidOrder) as exc:
            if exc.status == 409:
                return rec
            raise
        if isinstance(raw, dict) and raw.get("status"):
            if raw.get("rfq_id") not in (None, rec.rfq_id):
                raise ExchangeError(f"polymarket: asked for RFQ {rec.rfq_id}, got {raw.get('rfq_id')}", body=raw)
            rec.status = str(raw["status"])
            rec.last = {**rec.last, **raw}
        return rec

    async def fetch_rfq(self, rfq_id: str) -> Rfq:
        return self._rfq_from_record(await self._refresh(self._record(rfq_id)))

    async def fetch_rfqs(
        self, *, status: RfqStatus | str | None = None, market_id: str | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page[Rfq]:
        """The RFQs this adapter created in this process (the gateway lists none)."""
        rows = [self._rfq_from_record(rec) for rec in self._rfq_records.values()]
        if status is not None:
            rows = [r for r in rows if r.status == RfqStatus(status)]
        if market_id is not None:
            wanted = ids.qualify(VENUE, market_id)
            rows = [r for r in rows if r.combo_id == wanted
                    or (r.combo is not None and provisional_combo_id(r.combo.info.get("leg_position_ids") or []) == wanted)]
        rows.sort(key=lambda r: r.created_at or 0, reverse=True)
        return Page(rows[:limit] if limit else rows, next_cursor=None)

    async def cancel_rfq(self, rfq_id: str) -> None:
        """The gateway has no cancel for a requester. An unaccepted RFQ is
        dropped here (it lapses at `expires_at`, and nothing trades until
        accepted); an accepted one cannot be withdrawn."""
        rec = self._record(rfq_id)
        if rec.accept_body is not None and rec.status not in NO_TRADE:
            raise OrderRejected(f"polymarket: RFQ {rfq_id} was accepted ({rec.status}); it cannot be cancelled",
                                reason="already_accepted")
        rec.canceled = True

    async def fetch_quotes(self, rfq_id: str, *, status: QuoteStatus | str | None = None) -> list[RfqQuote]:
        """The RFQ's quote (the gateway returns the best one only). An RFQ
        that ended without a trade (expired, cancelled, failed, no quotes)
        is `RfqNotFound`, as the gateway itself no longer serves it."""
        rec = await self._refresh(self._record(rfq_id))
        wire = rec.wire_status()
        if wire in NO_TRADE:
            raise RfqNotFound(f"polymarket: RFQ {rfq_id} ended without a trade ({wire})")
        quote = self._quote_from_record(rec)
        quotes = [quote] if quote is not None else []
        if status is not None:
            quotes = [q for q in quotes if q.status == QuoteStatus(status)]
        return quotes

    async def fetch_quote(self, quote_id: str, *, rfq_id: str | None = None) -> RfqQuote:
        if quote_id in self._maker:
            return self._maker_quote(self._maker[quote_id])
        rec = await self._refresh(self._record(rfq_id, quote_id))
        quote = self._quote_from_record(rec)
        if quote is None or quote.id != quote_id:
            raise RfqNotFound(f"polymarket: no quote {quote_id} on RFQ {rec.rfq_id}")
        return quote

    async def accept_quote(self, accept: AcceptQuote) -> RfqQuote:
        """`POST /requests/{rfq}/accept` with a signed Exchange v3 order.

        The order buys (or sells) the combo's YES position for exactly the
        quote's `maker_amount_e6`/`taker_amount_e6`, signed for the wallet
        type (`polymarket_v3_signing`), with a zero builder (the requester
        gateway refuses attribution). Refused before anything is signed when
        the quote is not open, past its `expires_at`, on the other side, or
        worse than `worst_price` -- either its blended price or the price its
        order commits to (`maker/taker`), whichever is worse; fees come on
        top (`info["total_required"]`). Full size only.

        A retry is safe: an accepted quote is returned without sending again,
        and the gateway does not execute the same acceptance twice. Returns at
        once; `executed` comes when the gateway reports FILLED/CONFIRMED, and
        `taker_order_id` is then `rfq:<rfq_id>`.
        """
        v3.check_requester(self.credentials.signature_type)
        if accept.quantity is not None or accept.amount is not None:
            raise InvalidOrder("polymarket: a combo quote is accepted for its full size; the venue takes no partial size")
        rec = self._record(accept.rfq_id, accept.quote_id)
        quote_raw = rec.quote
        if not quote_raw or str(quote_raw.get("quote_id")) != accept.quote_id:
            raise RfqNotFound(f"polymarket: no quote {accept.quote_id} on RFQ {rec.rfq_id}")
        side = accept.side
        if rec.accept_body is not None and rec.accept_body.get("restored"):
            await self._refresh(rec)
            if rec.status in PRE_ACCEPT:
                # Re-seeded as possibly accepted, but the gateway still has it
                # before the accept (409): the accept never landed; send it.
                rec.accept_body, rec.accept_side, rec.accepted_at = None, None, None
        if rec.accept_body is not None:
            await self._refresh(rec)
            current = self._quote_from_record(rec)
            assert current is not None
            if rec.accept_side == side:
                return current.model_copy(update={"info": {**current.info, "accept": {"resent": False,
                                                  "client_order_id": accept.client_order_id or accept_key(accept.quote_id)}}})
            raise OrderRejected(f"polymarket: quote {accept.quote_id} was already accepted on the {rec.accept_side} side",
                                reason="already_accepted", info=current.info)
        wanted = Side.BUY if rec.direction == "BUY" else Side.SELL
        if accept.outcome != "yes" or side != wanted:
            raise InvalidOrder(
                f"polymarket: RFQ {rec.rfq_id} asked to {wanted.value} the combo's YES; accept it with "
                f"outcome='yes', action='{wanted.value}'")
        quote = self._quote_from_record(rec)
        assert quote is not None
        if quote.status != QuoteStatus.OPEN:
            raise OrderRejected(f"polymarket: quote {quote.id} is {quote.status.value}, not open "
                                f"(the acceptance window ends at expires_at)", reason="quote_not_active", info=quote.info)
        price = quote.price_for(accept.outcome, accept.action)
        maker_amount, taker_amount = int(quote_raw["maker_amount_e6"]), int(quote_raw["taker_amount_e6"])
        order_side = v3.BUY if wanted == Side.BUY else v3.SELL
        # The order's own price, to the base unit (its amounts are rounded to
        # whole base units, so it differs from the blended price below 1e-6).
        implied = v3.implied_price(order_side, maker_amount, taker_amount).quantize(Decimal("0.000001"), ROUND_HALF_UP)
        worse = max(price, implied) if wanted == Side.BUY else min(price, implied)  # type: ignore[type-var]
        if accept.refuses(worse):
            raise InvalidOrder(
                f"polymarket: quote {quote.id} {wanted.value}s YES at {worse} (blended {price}, order {implied}), "
                f"worse than the worst price {accept.worst_price}")
        signer, maker, kind = self._rfq_identity()
        token = str((rec.echoed or {}).get("yes_position_id") or "")
        if not token:
            raise ExchangeError(f"polymarket: RFQ {rec.rfq_id} answered with no combo YES position id", body=rec.response)
        order = v3.build_order(
            maker=maker, signer=signer, token_id=token, maker_amount=maker_amount, taker_amount=taker_amount,
            side=order_side, signature_type=kind, timestamp_s=self._now_s(), salt=sig.new_salt(),
        )
        body = {"quote_id": accept.quote_id, "signed_order": v3.signed_order(self.signer, order)}
        rec.accept_body, rec.accept_side, rec.accepted_at = body, side, _now_ms()
        rec.order_hash = v3.order_hash(order)
        raw = await self._gateway(self.requester, "POST", f"{REQUESTS_PATH}/{rec.rfq_id}/accept", body=body,
                                  kind="write", priority=Priority.HIGH)
        if isinstance(raw, dict) and raw.get("status"):
            rec.status = str(raw["status"])
            rec.last = {**rec.last, **raw}
            echoed = raw.get("taker_order_hash")
            if echoed and str(echoed).lower() != rec.order_hash.lower():
                log.warning("polymarket: RFQ %s taker_order_hash %s is not our Exchange v3 order hash %s",
                            rec.rfq_id, echoed, rec.order_hash)
        else:
            rec.status = "AWAITING_MAKER_CONFIRMATION"
        result = self._quote_from_record(rec)
        assert result is not None
        return result.model_copy(update={"info": {**result.info, "accept": {
            "resent": True, "client_order_id": accept.client_order_id or accept_key(accept.quote_id),
            "order": order, "response": raw}}})

    # -- RFQ trades as orders and fills ---------------------------------------

    async def fetch_rfq_order(self, order_id: str) -> Order:
        """The synthetic order of an RFQ trade (`rfq:<rfq_id>`, ours as the
        requester, or `rfq:<rfq_id>:<quote_id>`, ours as a quoter)."""
        parsed = parse_rfq_order_id(order_id)
        if parsed is None:
            raise OrderNotFound(f"polymarket: {order_id} is not an RFQ order id")
        rfq_id, quote_id = parsed
        if quote_id is not None:
            maker = self._maker.get(quote_id)
            if maker is None:
                raise OrderNotFound(f"polymarket: no quote {quote_id} of ours on RFQ {rfq_id} in this process")
            return self._maker_order(maker)
        try:
            rec = await self._refresh(self._record(rfq_id))
        except RfqNotFound as exc:
            raise OrderNotFound(str(exc)) from None
        if rec.accept_body is None or not rec.quote:
            raise OrderNotFound(f"polymarket: RFQ {rfq_id} was never accepted; it made no order")
        return self._taker_order(rec)

    def _taker_order(self, rec: RfqRecord) -> Order:
        quote = rec.quote or {}
        buy = rec.direction == "BUY"
        wire = rec.wire_status()
        maker_amount, taker_amount = int(quote["maker_amount_e6"]), int(quote["taker_amount_e6"])
        shares = from_e6(taker_amount if buy else maker_amount)
        assert shares is not None
        filled = shares if wire in DONE else Decimal(0)
        price = from_e6(quote.get("blended_price_e6"))
        fee = self._taker_fee(rec)
        status = (OrderStatus.CLOSED if wire in DONE else OrderStatus.REJECTED if wire in NO_TRADE
                  else OrderStatus.PENDING)
        return Order(
            id=taker_order_id(rec.rfq_id), venue=VENUE, account=self.account,
            market_id=(rec.combo.market_id if rec.combo else provisional_combo_id(rec.request["leg_position_ids"])),
            side=Side.BUY if buy else Side.SELL, type=OrderType.RFQ_TAKER, time_in_force=TimeInForce.FOK,
            status=status, price=price, quantity=shares, filled=filled,
            average_price=price if filled else None, fee=fee if filled else None, fee_currency="pUSD",
            outcome="yes", action="buy" if buy else "sell",
            created_at=rec.accepted_at, updated_at=_now_ms(),
            info={"rfq_id": rec.rfq_id, "status": wire, "order_hash": rec.order_hash,
                  **({"tx_hash": rec.last.get("tx_hash")} if rec.last.get("tx_hash") else {})},
        )

    @staticmethod
    def _taker_fee(rec: RfqRecord) -> Decimal | None:
        """BUY: what the budget pays beyond the order's collateral; SELL: the
        order's gross proceeds less what arrives."""
        quote = rec.quote or {}
        try:
            if rec.direction == "BUY":
                return (Decimal(quote["total_required_e6"]) - Decimal(quote["maker_amount_e6"])) / E6
            return (Decimal(quote["taker_amount_e6"]) - Decimal(quote["net_receive_e6"])) / E6
        except (KeyError, TypeError, ArithmeticError):
            return None

    def rfq_fills(self, *, market_id: str | None = None, order_id: str | None = None) -> list[Fill]:
        """Fills of RFQ trades this adapter knows of, as the requester or a quoter."""
        fills: list[Fill] = []
        for rec in self._rfq_records.values():
            if rec.accept_body is None or rec.wire_status() not in DONE:
                continue
            order = self._taker_order(rec)
            fills.append(Fill(
                id=f"{order.id}:fill", order_id=order.id, venue=VENUE, account=self.account, market_id=order.market_id,
                side=order.side, outcome="yes", action=order.action, price=order.price or Decimal(0),
                quantity=order.filled, fee=order.fee, fee_currency="pUSD", liquidity=Liquidity.TAKER,
                settlement=SettlementState.CONFIRMED, timestamp=rec.accepted_at or _now_ms(),
                info={"rfq_id": rec.rfq_id, **({"tx_hash": rec.last.get("tx_hash")} if rec.last.get("tx_hash") else {})},
            ))
        for maker in self._maker.values():
            if maker.status != "EXECUTED":
                continue
            order = self._maker_order(maker)
            fills.append(Fill(
                id=f"{order.id}:fill", order_id=order.id, venue=VENUE, account=self.account, market_id=order.market_id,
                side=order.side, outcome=order.outcome, action=order.action, price=order.price or Decimal(0),
                quantity=order.filled, fee=Decimal(0), fee_currency="pUSD", liquidity=Liquidity.MAKER,
                settlement=SettlementState.CONFIRMED, timestamp=maker.sent_at,
                info={"rfq_id": maker.rfq_id, "quote_id": maker.quote.id,
                      **({"tx_hash": maker.tx_hash} if maker.tx_hash else {})},
            ))
        if market_id is not None:
            wanted = ids.qualify(VENUE, market_id)
            fills = [f for f in fills if f.market_id == wanted]
        if order_id is not None:
            fills = [f for f in fills if f.order_id == order_id]
        return fills

    # -- quoter ---------------------------------------------------------------

    async def start_rfq_session(
        self, *, url: str = QUOTER_WS_URL, connect: Callable[[str], Any] | None = None,
        on_rfq: Callable[[Rfq], Awaitable[None] | None] | None = None,
    ) -> PolymarketRfqSession:
        """Open the quoter WebSocket (auth with the CLOB credentials and the
        wallet identity). Quote requests arrive only here, 400 ms before the
        competition closes; `on_rfq` sees each one as it lands."""
        await self.ensure_api_credentials()
        if self.rfq_session is None:
            self.rfq_session = PolymarketRfqSession(self, url=url, connect=connect, on_rfq=on_rfq)
        self.rfq_session.start()
        return self.rfq_session

    def subscribe_incoming_rfqs(self, listener: Callable[[Rfq], Any]) -> Callable[[], None]:
        """Call `listener(rfq)` (sync or async) for every quote request the
        quoter session receives, as it lands (the 400 ms window). An async
        listener is scheduled, so the socket keeps reading. Starts nothing:
        `start_incoming_rfqs()` (or `start_rfq_session()`) opens the session.
        Returns the unsubscribe."""
        self._rfq_listeners.append(listener)
        return lambda: self._rfq_listeners.remove(listener) if listener in self._rfq_listeners else None

    async def start_incoming_rfqs(self) -> None:
        """Open the quoter session if it is not running (the engine calls
        this when its incoming feed starts)."""
        task = self.rfq_session._task if self.rfq_session is not None else None
        if task is None or task.done():
            await self.start_rfq_session()

    def _tell_rfq_listeners(self, rfq: Rfq) -> None:
        for listener in list(self._rfq_listeners):
            try:
                result = listener(rfq)
                if asyncio.iscoroutine(result):
                    task = asyncio.get_running_loop().create_task(result)
                    self._listener_tasks.add(task)
                    task.add_done_callback(self._listener_done)
            except Exception:
                log.exception("polymarket: an incoming-RFQ listener failed")

    def _listener_done(self, task: asyncio.Task) -> None:
        self._listener_tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            log.error("polymarket: an incoming-RFQ listener failed", exc_info=task.exception())

    async def stop_rfq_session(self) -> None:
        if self.rfq_session is not None:
            await self.rfq_session.close()
            self.rfq_session = None

    def _incoming_from(self, message: dict[str, Any]) -> Rfq | None:
        rfq_id = str(message.get("rfq_id") or "")
        legs = message.get("leg_position_ids") or []
        if not rfq_id or len(legs) < MIN_LEGS:
            return None
        condition = message.get("condition_id")
        try:
            combo = self._combo(legs, condition_id=condition, extra={
                k: message[k] for k in ("yes_position_id", "no_position_id", "condition_id") if message.get(k)})
        except (InvalidOrder, ValueError) as exc:
            log.warning("polymarket: skipped an RFQ request that does not validate (%s): %r", exc, message)
            return None
        size = message.get("requested_size") or {}
        value = from_e6(size.get("value_e6"))
        rfq = Rfq(
            id=rfq_id, venue=VENUE, combo_id=combo.market_id, combo=combo,
            outcome="yes" if str(message.get("side") or "YES") == "YES" else "no",
            action="buy" if message.get("direction") == "BUY" else "sell",
            amount=value if size.get("unit") == "notional" else None,
            quantity=value if size.get("unit") == "shares" else None,
            status=RfqStatus.OPEN, mine=False, created_at=_now_ms(),
            expires_at=int(message["submission_deadline"]) if message.get("submission_deadline") else None,
            info=message,
        )
        self._incoming[rfq_id] = (rfq, message)
        return rfq

    async def fetch_incoming_rfqs(
        self, *, market_id: str | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Rfq]:
        """Quote requests the quoter session received whose submission window
        is still open. Empty, with a warning, when no session is running."""
        if self.rfq_session is None:
            if not getattr(self, "_warned_no_session", False):
                self._warned_no_session = True
                log.warning("polymarket: no RFQ quoter session; call start_rfq_session() to receive quote requests")
            return Page([], next_cursor=None)
        now = _now_ms()
        for rfq_id in [k for k, (r, _) in self._incoming.items() if r.expires_at is not None and r.expires_at + 60_000 < now]:
            self._incoming.pop(rfq_id, None)
        rows = [r for r, _ in self._incoming.values() if r.expires_at is None or r.expires_at > now]
        if market_id is not None:
            rows = [r for r in rows if r.combo_id == ids.qualify(VENUE, market_id)]
        return Page(rows[:limit] if limit else rows, next_cursor=None)

    async def send_quote(self, quote: MakerQuote) -> RfqQuote:
        """`POST /v1/maker/quotes`: price the request's one side and sign the
        Exchange v3 order that fills it from collateral -- to fill a BUY of
        YES we sell YES at `ask` by buying NO at `1 - ask`; to fill a SELL we
        buy YES at `bid`. `quantity` (shares) defaults to the request's full
        size; `expires_at` is sent as `valid_until`. The quote id is ours
        (`quote_` + 32 hex), derived from the engine's key."""
        held = self._incoming.get(quote.rfq_id)
        if held is None:
            raise InvalidOrder(f"polymarket: RFQ {quote.rfq_id} is not one the quoter session received")
        rfq, message = held
        if quote.rest_remainder or quote.post_only:
            raise InvalidOrder("polymarket: a combo quote has no rest_remainder or post_only")
        direction = str(message.get("direction"))
        price = quote.ask if direction == "BUY" else quote.bid
        if price is None:
            raise InvalidOrder(f"polymarket: RFQ {quote.rfq_id} asks to {direction.lower()} YES; quote the "
                               f"{'ask' if direction == 'BUY' else 'bid'}")
        if direction == "BUY" and price > BUY_PRICE_CEILING:
            raise InvalidOrder(f"polymarket: a BUY quote above {BUY_PRICE_CEILING} is refused by the venue "
                               "(QUOTED_PRICE_ABOVE_SAFETY_THRESHOLD)")
        price_e6 = to_e6(price, "price")
        size = message.get("requested_size") or {}
        full = v3.quote_size(str(size.get("unit")), int(size.get("value_e6") or 0), price_e6)
        size_e6 = to_e6(quote.quantity, "quantity") if quote.quantity is not None else full
        if size_e6 > full:
            raise InvalidOrder(f"polymarket: {Decimal(size_e6) / E6} shares is more than the request ({Decimal(full) / E6})")
        token_side, maker_amount, taker_amount = v3.maker_quote_amounts(direction, price_e6, size_e6)
        token = str(message.get("no_position_id" if token_side == "no" else "yes_position_id") or "")
        if not token:
            raise InvalidOrder(f"polymarket: RFQ {quote.rfq_id} names no {token_side.upper()} position id")
        signer, maker, kind = self._rfq_identity()
        order = v3.build_order(maker=maker, signer=signer, token_id=token, maker_amount=maker_amount,
                               taker_amount=taker_amount, side=v3.BUY, signature_type=kind,
                               timestamp_s=self._now_s(), salt=sig.new_salt())
        quote_id = quote_id_for(quote.client_order_id)
        body: dict[str, Any] = {
            "quote_id": quote_id, "rfq_id": quote.rfq_id, "signer_address": signer, "maker_address": maker,
            "signature_type": kind, "price_e6": str(price_e6), "size_e6": str(size_e6),
            "signed_order": v3.signed_order(self.signer, order),
        }
        if quote.expires_at is not None:
            body["valid_until"] = int(quote.expires_at)
        raw = await self._gateway(self.rfq_api, "POST", "/v1/maker/quotes", body=body, kind="write")
        shares = Decimal(size_e6) / E6
        result = RfqQuote(
            id=quote_id, rfq_id=quote.rfq_id, venue=VENUE, account=quote.account or self.account,
            market_id=rfq.combo_id,
            ask=price if direction == "BUY" else None, bid=price if direction == "SELL" else None,
            ask_quantity=shares if direction == "BUY" else None, bid_quantity=shares if direction == "SELL" else None,
            status=QuoteStatus.OPEN, mine=True, expires_at=quote.expires_at, created_at=_now_ms(),
            info={"request": body, "response": raw, "client_order_id": quote.client_order_id,
                  "order_hash": v3.order_hash(order)},
        )
        self._maker[quote_id] = MakerRecord(quote=result, rfq_id=quote.rfq_id, request=body, sent_at=_now_ms(),
                                            detail={"snapshot": raw} if raw else {})
        return result

    async def replace_quote(self, quote_id: str, quote: MakerQuote) -> RfqQuote:
        """The venue has no replace: cancel the old quote, then send the new
        one (a new id). A cancel the venue no longer accepts is ignored."""
        try:
            await self.cancel_quote(quote_id, rfq_id=quote.rfq_id)
        except (RfqNotFound, OrderRejected) as exc:
            log.info("polymarket: replacing quote %s, its cancel was refused (%s); sending the new one", quote_id, exc)
        new = await self.send_quote(quote)
        return new.model_copy(update={"info": {**new.info, "replaces": quote_id}})

    async def cancel_quote(self, quote_id: str, *, rfq_id: str | None = None) -> None:
        """Withdraw a quote (`POST /v1/maker/quotes/cancel`), or, while its
        last look is open, decline it (`POST /v1/maker/confirmations` with
        `DECLINE`). A cancel acknowledged after selection may not withdraw it."""
        held = self._maker.get(quote_id)
        rfq_id = rfq_id or (held.rfq_id if held else None)
        if not rfq_id:
            raise InvalidOrder("polymarket: a quote is cancelled under its RFQ; give rfq_id")
        signer, maker, kind = self._rfq_identity()
        ident = {"rfq_id": rfq_id, "quote_id": quote_id, "signer_address": signer, "maker_address": maker,
                 "signature_type": kind}
        if held is not None and held.status == "ACCEPTED":
            raw = await self._gateway(self.rfq_api, "POST", "/v1/maker/confirmations", body={**ident, "decision": "DECLINE"},
                                      kind="write", priority=Priority.HIGH)
        else:
            raw = await self._gateway(self.rfq_api, "POST", "/v1/maker/quotes/cancel", body=ident, kind="write",
                                      priority=Priority.HIGH)
        if held is not None:
            held.status = "CANCELED"
            held.detail["cancel"] = raw

    async def confirm_quote(self, quote: RfqQuote) -> RfqQuote:
        """The last look: `POST /v1/maker/confirmations` with `CONFIRM`.
        Answers `execution` when ours completed the bundle (the trade goes
        to the chain), else a `snapshot` still waiting on other makers."""
        held = self._maker.get(quote.id)
        signer, maker, kind = self._rfq_identity()
        body = {"rfq_id": quote.rfq_id, "quote_id": quote.id, "signer_address": signer, "maker_address": maker,
                "signature_type": kind, "decision": "CONFIRM"}
        raw = await self._gateway(self.rfq_api, "POST", "/v1/maker/confirmations", body=body, kind="write",
                                  priority=Priority.HIGH)
        if held is not None:
            held.status = "CONFIRMED"
            held.detail["confirm"] = raw
            return self._maker_quote(held)
        return quote.model_copy(update={"status": QuoteStatus.CONFIRMED, "info": {**quote.info, "confirm": raw}})

    async def fetch_my_quotes(
        self, *, rfq_id: str | None = None, status: QuoteStatus | str | None = None,
    ) -> list[RfqQuote]:
        """Our quotes in this process, as the session and the REST answers left them."""
        quotes = [self._maker_quote(m) for m in self._maker.values() if rfq_id is None or m.rfq_id == rfq_id]
        if status is not None:
            quotes = [q for q in quotes if q.status == QuoteStatus(status)]
        return quotes

    def _maker_quote(self, held: MakerRecord) -> RfqQuote:
        if held.status == "OPEN" and (_now_ms() - held.sent_at) > MAKER_QUOTE_STALE_S * 1000:
            held.status = "EXPIRED"
            held.detail["inferred"] = "not selected within the competition and acceptance windows"
        status = MAKER_STATUS[held.status]
        # The requester's side: it buys YES when we quoted an ask.
        accepted = None if held.status in ("OPEN", "EXPIRED") else (Side.BUY if held.quote.ask is not None else Side.SELL)
        update: dict[str, Any] = {"status": status, "accepted_side": accepted,
                                  "info": {**held.quote.info, **held.detail, "status": held.status}}
        if held.fill_size_e6 is not None:
            shares = Decimal(held.fill_size_e6) / E6
            update["ask_quantity" if held.quote.ask is not None else "bid_quantity"] = shares
        if status == QuoteStatus.EXECUTED:
            update["maker_order_id"] = maker_order_id(held.rfq_id, held.quote.id)
        if held.detail.get("confirm_by"):
            update["confirm_by"] = int(held.detail["confirm_by"])
        return held.quote.model_copy(update=update)

    def _maker_order(self, held: MakerRecord) -> Order:
        quote = self._maker_quote(held)
        buy_request = quote.ask is not None   # the requester buys YES; we sell YES (buying NO)
        price = quote.ask if buy_request else quote.bid
        size = Decimal(held.fill_size_e6 or int(held.request["size_e6"])) / E6
        filled = size if held.status == "EXECUTED" else Decimal(0)
        status = (OrderStatus.CLOSED if held.status == "EXECUTED"
                  else OrderStatus.REJECTED if held.status in ("FAILED", "CANCELED", "EXPIRED") else OrderStatus.PENDING)
        return Order(
            id=maker_order_id(held.rfq_id, held.quote.id), venue=VENUE, account=self.account,
            market_id=quote.market_id or provisional_combo_id(held.request.get("leg_position_ids") or []),
            side=Side.SELL if buy_request else Side.BUY, type=OrderType.RFQ_MAKER, time_in_force=TimeInForce.FOK,
            status=status, price=price, quantity=size, filled=filled, average_price=price if filled else None,
            outcome="no" if buy_request else "yes", action="buy", fee=Decimal(0) if filled else None,
            fee_currency="pUSD", created_at=held.sent_at, updated_at=_now_ms(),
            info={"rfq_id": held.rfq_id, "quote_id": held.quote.id, "status": held.status,
                  **({"tx_hash": held.tx_hash} if held.tx_hash else {})},
        )

    def _makers_on(self, rfq_id: str) -> list[MakerRecord]:
        return [m for m in self._maker.values() if m.rfq_id == rfq_id]

    def _maker_event(self, message: dict[str, Any], status: str) -> None:
        held = self._maker.get(str(message.get("quote_id") or ""))
        if held is None or held.status in ("EXECUTED", "FAILED"):
            return
        held.status = status
        if message.get("fill_size_e6"):
            held.fill_size_e6 = int(message["fill_size_e6"])
        if message.get("confirm_by"):
            held.detail["confirm_by"] = int(message["confirm_by"])
        held.detail["confirmation_request"] = message

    def _maker_execution(self, message: dict[str, Any]) -> None:
        status = EXECUTION_TO_MAKER.get(str(message.get("status") or ""))
        for held in self._makers_on(str(message.get("rfq_id") or "")):
            # Sent only for a quote of ours that was selected, so it overrides
            # an inferred `lost` or `expired`; a final status stays final, and
            # a quote we cancelled (a replaced one on the same RFQ) is left out.
            if status is None or held.status in ("EXECUTED", "FAILED") or "cancel" in held.detail:
                continue
            held.status = status
            held.tx_hash = message.get("tx_hash") or held.tx_hash
            held.detail["execution"] = message

    def _maker_trade(self, message: dict[str, Any]) -> None:
        """A trade broadcast: an RFQ we quoted traded. Without an execution
        update of ours it went to another maker."""
        for held in self._makers_on(str(message.get("rfq_id") or "")):
            if held.status == "OPEN":
                held.status = "CANCELED"
                held.detail["lost"] = "the RFQ traded with another maker"

    def _maker_error(self, message: dict[str, Any]) -> None:
        held = self._maker.get(str(message.get("quote_id") or ""))
        if held is not None and message.get("request_type") == "RFQ_QUOTE" and held.status == "OPEN":
            held.status = "FAILED"
            held.detail["error"] = message


def accept_key(quote_id: str) -> str:
    """The engine's idempotency key for an accept (docs/rfq-combo.md §5)."""
    return f"rfq-accept:{VENUE}:{quote_id}"
