"""Gemini Predictions order entry, on the private REST API (`api.gemini.com`).

Trading needs a Gemini account with Prediction Markets access (US) and an
API key from exchange.gemini.com/settings/api: account-scoped, with the
Trader role, and a **time-based nonce** (the WebSocket accepts no other
kind). The account must also have accepted Gemini's current Prediction
Markets terms; `fetch_terms_status` says whether it has.

Facts that shape the adapter:

**Signing.** A private request sends no body. Its JSON payload, the request
path and a nonce merged into the endpoint's fields, goes base64-encoded in
`X-GEMINI-PAYLOAD`, and `X-GEMINI-SIGNATURE` is the hex HMAC-SHA384 of that
base64 string under the API secret. The nonce is Unix seconds (a time-based
key accepts it within 30 seconds of the server's clock). Signed requests
are sent one at a time, as Gemini's own SDK does, and a write is never
retried: a resend would be a second order.

**An order names its outcome.** The wire carries `side` (`buy`, `sell`) and
`outcome` (`yes`, `no`), with the price in that outcome's terms; the book is
YES space. An order is sent as its own `outcome` and `action`, at that
outcome's price: Buy NO at a YES price *p* goes as buying NO at `1 - p`,
Sell YES as selling YES at *p* (Gemini's `sell` closes a position). With
neither, the YES-leg side decides: `buy` buys YES, `sell` buys NO. Read back, every order is on
the YES leg again, the venue's own side and outcome kept in `info`.

**Limit orders only.** The REST API takes `limit` (and `stop-limit`, which
the engine holds here). A market order is an immediate-or-cancel limit at the
caller's protection price, and one without a price is refused. Post-only is
`makerOrCancel`. There is no good-till-date and no client order id on the
wire; a `client_order_id` is kept on the returned `Order` only.

**Fills.** The REST API has no fills endpoint. `fetch_my_trades` reads one
aggregate fill per order from the order history; the account stream
(`synpath.ws.gemini.GeminiUserStream`) delivers each execution.

**Combos and their RFQ.** A combo (two to six contracts, each on a YES or NO
outcome, settling YES only if every leg does) is its own instrument,
`GEMI-CMB-...`, with its own book: `create_order`, the order history and
`fetch_positions` take it like any contract. `fetch_combos`, `fetch_combo`
and `create_combo` are Gemini's combo routes, which its documentation says
are not enabled in production yet; there they fail with `CombosUnavailable`
carrying Gemini's own answer. Combo RFQ is a sealed one-second auction run
on the WebSocket. Gemini publishes the maker side
(`synpath.ws.gemini.GeminiRfqStream`); the requester side, opening an RFQ
and accepting its winning quote, is in the Gemini app and website only, with
no API, so `create_rfq` and `accept_quote` raise `NotSupported`.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
from decimal import Decimal
from typing import Any

from .. import ids
from ..base import AsyncHttpClient, Capability
from ..errors import AuthenticationError, BadRequest, ExchangeError, MarketNotFound, NotSupported, RateLimitExceeded
from ..gemini import (
    API_URL, SANDBOX_URL, candidate_event_tickers, fee_schedule_of, native_symbol, parse_ts,
)
from ..types import Page
from .base import TradingExchange
from .credentials import GeminiCredentials
from .errors import InsufficientFunds, InvalidOrder, MarketHalted, OrderNotFound, OrderRejected, PermissionDenied
from .limiter import BudgetLimiter, Priority
from .money import D, complement, validate_price, validate_quantity
from .types import (
    Account, Balance, FeeEstimate, Fill, HeldBy, Liquidity, Order, OrderRequest, OrderStatus, OrderType, Position,
    PositionSide, Precision, Settlement, SettlementState, Side, TimeInForce, VENUE_ORDER_TYPES,
)

VENUE = "gemini"
BASE_URLS = {"prod": API_URL, "sandbox": SANDBOX_URL}
PREFIX = "/v1/prediction-markets"

BATCH = 20
"""Orders per batch place or cancel: the venue's limit."""

ACTIVE_PAGE = 100
"""Most active orders per request."""

HISTORY_PAGE = 1000
"""Most historical orders per request."""

READ_ATTEMPTS = 3
"""Tries for a signed read that is rate limited; a write is sent once."""

DEFAULT_PRECISION = Precision(
    tick=Decimal("0.01"), min_quantity=Decimal("0.01"), quantity_step=Decimal("0.01"), whole_contracts=False,
)
"""What an order is checked against without an instrument spec: the cent
tick and hundredth-contract grid of the contracts read so far. A contract
publishes its own (`priceIncrement`, `quantityIncrement`,
`quantityMinimum`); pass a `Precision` built from them for a finer grid."""

TIF_TO_WIRE = {
    TimeInForce.GTC: "good-til-cancel",
    TimeInForce.IOC: "immediate-or-cancel",
    TimeInForce.FOK: "fill-or-kill",
}
TIF_FROM_WIRE = {wire: tif for tif, wire in TIF_TO_WIRE.items()} | {"maker-or-cancel": TimeInForce.GTC}

STATUS = {
    "open": OrderStatus.OPEN,
    "filled": OrderStatus.CLOSED,
    "closed": OrderStatus.CLOSED,
    "cancelled": OrderStatus.CANCELED,
    "canceled": OrderStatus.CANCELED,
}

WS_STATUS = {
    "NEW": OrderStatus.OPEN,
    "OPEN": OrderStatus.OPEN,
    "PARTIALLY_FILLED": OrderStatus.OPEN,
    "MODIFIED": OrderStatus.OPEN,
    "FILLED": OrderStatus.CLOSED,
    "CANCELED": OrderStatus.CANCELED,
    "CANCELLED": OrderStatus.CANCELED,
    "REJECTED": OrderStatus.REJECTED,
}


# ---------------------------------------------------------------------------
# Signing
# ---------------------------------------------------------------------------

class GeminiSigner:
    """Holds the API secret and produces Gemini's private-API headers.

    REST: the payload is `{"request": <path>, "nonce": <nonce>, ...fields}`
    as JSON, base64-encoded into `X-GEMINI-PAYLOAD`; the signature is
    `hex(HMAC_SHA384(base64 payload, secret))`. WebSocket: the payload is
    `base64(str(nonce))`, signed the same way, with the nonce also in
    `X-GEMINI-NONCE`. The nonce is Unix seconds, what a time-based key takes.
    """

    def __init__(self, api_key: str, api_secret: str, *, account: str | None = None, clock: Any = time.time):
        self.api_key = api_key
        self._secret = api_secret.encode()
        self.account = account
        self._clock = clock

    def __repr__(self) -> str:
        return f"GeminiSigner(api_key={self.api_key[:12]!r}..., secret=***)"

    def nonce(self) -> str:
        return str(int(self._clock()))

    def sign(self, b64_payload: str) -> str:
        return hmac.new(self._secret, b64_payload.encode(), hashlib.sha384).hexdigest()

    def payload(self, path: str, fields: dict[str, Any] | None = None, *, nonce: str | None = None) -> str:
        """The base64 payload for `path` (no query string) and the endpoint's fields."""
        body: dict[str, Any] = {"request": path, "nonce": nonce if nonce is not None else self.nonce()}
        if self.account and "account" not in (fields or {}):
            body["account"] = self.account
        body.update(fields or {})
        return base64.b64encode(json.dumps(body, separators=(",", ":")).encode()).decode()

    def headers(self, path: str, fields: dict[str, Any] | None = None, *, nonce: str | None = None) -> dict[str, str]:
        encoded = self.payload(path, fields, nonce=nonce)
        return {
            "Content-Type": "text/plain",
            "Content-Length": "0",
            "X-GEMINI-APIKEY": self.api_key,
            "X-GEMINI-PAYLOAD": encoded,
            "X-GEMINI-SIGNATURE": self.sign(encoded),
            "Cache-Control": "no-cache",
        }

    def ws_headers(self, *, nonce: str | None = None) -> dict[str, str]:
        stamp = nonce if nonce is not None else self.nonce()
        encoded = base64.b64encode(stamp.encode()).decode()
        return {
            "X-GEMINI-APIKEY": self.api_key,
            "X-GEMINI-NONCE": stamp,
            "X-GEMINI-PAYLOAD": encoded,
            "X-GEMINI-SIGNATURE": self.sign(encoded),
        }


# ---------------------------------------------------------------------------
# Pure translation and normalizers
# ---------------------------------------------------------------------------

def symbol_of(market_id: str) -> str:
    """The instrument symbol behind a Synpath id (`gemini:GEMI-...`) or a bare
    symbol. Another venue's id is refused before anything is signed."""
    try:
        symbol = native_symbol(ids.native(VENUE, market_id))
    except BadRequest as exc:
        raise InvalidOrder(str(exc)) from None
    if not symbol.startswith("GEMI-"):
        raise InvalidOrder(f"{VENUE}: {symbol!r} is not an instrument symbol (GEMI-...)")
    return symbol


def _text(value: Decimal) -> str:
    """A decimal as the venue's string, without an exponent."""
    return format(value, "f")


def wire_leg(request: OrderRequest) -> tuple[str, str]:
    """(venue side, outcome): the order's own action and outcome
    (`OrderRequest.leg`). See the module docstring."""
    outcome, action = request.leg
    return action, outcome


def yes_leg(venue_side: str, outcome: str) -> Side:
    """The YES-leg side of a venue order: buying NO or selling YES is a sell."""
    buying = str(venue_side).lower() == "buy"
    yes = str(outcome).lower() in ("yes", "true")
    return Side.BUY if buying == yes else Side.SELL


def yes_price(price: Any, outcome: str) -> Decimal | None:
    if price in (None, ""):
        return None
    value = D(price)
    return value if str(outcome).lower() in ("yes", "true") else complement(value)


def translate_order(request: OrderRequest, *, precision: Precision | None = None) -> dict[str, Any]:
    """An `OrderRequest` as the `POST /v1/prediction-markets/order` fields.

    Validated here, before anything is signed: an order type the venue holds,
    a price on the tick inside (0, 1), a quantity on the grid, a time in
    force the venue knows."""
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"{VENUE}: {request.type.value} is held by the execution engine, not the venue; submit it through the engine"
        )
    if request.time_in_force in (TimeInForce.DAY, TimeInForce.GTD):
        raise InvalidOrder(f"{VENUE}: no good-till-date orders; use gtc and cancel, or the engine's expiry")
    if request.price is None:
        raise InvalidOrder(
            f"{VENUE}: a price is required -- the venue takes limit orders only, so a market order is an "
            f"immediate-or-cancel limit at the protection price you give"
        )
    symbol = symbol_of(request.market_id)
    spec = precision or DEFAULT_PRECISION
    price = validate_price(D(request.price), spec)
    quantity = validate_quantity(D(request.quantity), spec)
    venue_side, outcome = wire_leg(request)
    wire_price = price if outcome == "yes" else complement(price)
    if request.type == OrderType.MARKET:
        tif = TimeInForce.FOK if request.time_in_force == TimeInForce.FOK else TimeInForce.IOC
    else:
        tif = request.time_in_force
    if request.post_only and tif != TimeInForce.GTC:
        raise InvalidOrder(f"{VENUE}: a post-only (maker-or-cancel) order rests, so it is gtc")
    body: dict[str, Any] = {
        "symbol": symbol,
        "orderType": "limit",
        "side": venue_side,
        "outcome": outcome,
        "quantity": _text(quantity),
        "price": _text(wire_price),
        "timeInForce": TIF_TO_WIRE[tif],
    }
    if request.post_only:
        body["makerOrCancel"] = True
    return body


def order_of(raw: dict[str, Any], *, account: Account | None = None, request: OrderRequest | None = None,
             body: dict[str, Any] | None = None) -> Order:
    """A REST order row (`OrderResponse`) on the YES leg: an order the venue
    holds as buying NO at 0.30 reads as a sell at 0.70. `request` and `body`
    (what was sent) fill in what a placement answer leaves out."""
    body = body or {}
    outcome = str(raw.get("outcome") or body.get("outcome") or "yes")
    venue_side = str(raw.get("side") or body.get("side") or "buy")
    quantity = D(raw.get("quantity") or body.get("quantity") or "0")
    filled = D(raw.get("filledQuantity") or "0")
    remaining = raw.get("remainingQuantity")
    wire_tif = str(raw.get("timeInForce") or body.get("timeInForce") or "")
    tif = TIF_FROM_WIRE.get(wire_tif, TimeInForce.GTC)
    average = yes_price(raw.get("avgExecutionPrice"), outcome)
    status = STATUS.get(str(raw.get("status") or "").lower(), OrderStatus.OPEN)
    order = Order(
        id=str(raw.get("orderId") or ""),
        client_order_id=raw.get("clientOrderId") or (request.client_order_id if request else None),
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, native_symbol(str(raw.get("symbol") or body.get("symbol") or ""))),
        side=yes_leg(venue_side, outcome),
        type=OrderType.LIMIT if request is None else request.type,
        time_in_force=tif,
        status=status,
        held_by=HeldBy.VENUE,
        price=yes_price(raw.get("price") or body.get("price"), outcome),
        stop_price=yes_price(raw.get("stopPrice"), outcome),
        quantity=quantity,
        filled=filled,
        remaining=D(remaining) if remaining not in (None, "") else None,
        average_price=average if filled > 0 else None,
        cost=(D(raw["avgExecutionPrice"]) * filled).quantize(Decimal("0.0001"))
        if filled > 0 and raw.get("avgExecutionPrice") not in (None, "") else None,
        fee_currency="USD",
        post_only=wire_tif == "maker-or-cancel" or bool(body.get("makerOrCancel")),
        outcome="yes" if outcome.lower() in ("yes", "true") else "no",
        action="sell" if venue_side.lower() == "sell" else "buy",
        created_at=parse_ts(raw.get("createdAt")),
        updated_at=parse_ts(raw.get("cancelledAt") or raw.get("updatedAt")),
        info={**raw, "venue_side": venue_side, "venue_outcome": outcome},
    )
    if request is not None:
        order = order.model_copy(update={"book": request.book, "trader": request.trader, "tags": request.tags})
    return order


def fill_of_order(raw: dict[str, Any], *, account: Account | None = None) -> Fill | None:
    """One aggregate fill for an order that has filled, at its average price.
    `None` for an order with nothing filled."""
    filled = D(raw.get("filledQuantity") or "0")
    outcome = str(raw.get("outcome") or "yes")
    price = yes_price(raw.get("avgExecutionPrice") or raw.get("price"), outcome)
    if filled <= 0 or price is None:
        return None
    order_id = str(raw.get("orderId") or "")
    return Fill(
        id=order_id, order_id=order_id, client_order_id=raw.get("clientOrderId"), venue=VENUE, account=account,
        market_id=ids.qualify(VENUE, native_symbol(str(raw.get("symbol") or ""))),
        side=yes_leg(str(raw.get("side") or "buy"), outcome), price=price, quantity=filled,
        fee=None, fee_currency="USD", liquidity=Liquidity.UNKNOWN, settlement=SettlementState.CONFIRMED,
        timestamp=parse_ts(raw.get("updatedAt") or raw.get("createdAt")) or 0,
        info={**raw, "aggregate": True},
    )


def positions_of(rows: list[dict[str, Any]], *, account: Account | None = None) -> list[Position]:
    """Position rows (one per symbol and outcome) as one netted `Position` per
    symbol: YES held is long, NO held is short, with both inventories kept.
    `entry_price` is on the YES leg (a NO bought at 0.36 is 0.64)."""
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_symbol.setdefault(native_symbol(str(row.get("symbol") or "")), []).append(row)
    out: list[Position] = []
    for symbol, held in by_symbol.items():
        inventory = {"yes": Decimal("0"), "no": Decimal("0")}
        entry: dict[str, Decimal] = {}
        realized = Decimal("0")
        unrealized: Decimal | None = None
        for row in held:
            outcome = "no" if str(row.get("outcome")).lower() in ("no", "false") else "yes"
            inventory[outcome] += D(row.get("totalQuantity") or "0")
            if row.get("avgPrice") not in (None, ""):
                entry[outcome] = D(row["avgPrice"])
            if row.get("realizedPl") not in (None, ""):
                realized += D(row["realizedPl"])
            if row.get("unrealizedPnl") not in (None, ""):
                unrealized = (unrealized or Decimal("0")) + D(row["unrealizedPnl"])
        net = inventory["yes"] - inventory["no"]
        side = PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT
        price = (entry.get("yes") if net > 0 else complement(entry["no"]) if net < 0 and "no" in entry else None)
        resolution = next((r.get("resolutionSide") for r in held if r.get("resolutionSide")), None)
        out.append(Position(
            venue=VENUE, account=account, market_id=ids.qualify(VENUE, symbol), side=side, contracts=abs(net),
            inventory_yes=inventory["yes"], inventory_no=inventory["no"],
            entry_price=price, unrealized_pnl=unrealized, realized_pnl=realized,
            resolved=resolution is not None, final=resolution is not None,
            won=(resolution == ("yes" if net > 0 else "no")) if resolution and net != 0 else None,
            info={"rows": held},
        ))
    return out


def settlement_of(raw: dict[str, Any], *, account: Account | None = None) -> Settlement:
    """A settled position. `position` is signed: positive held YES."""
    signed = D(raw.get("position") or "0")
    outcome = str(raw.get("outcome") or ("yes" if signed >= 0 else "no")).lower()
    result = str(raw.get("resolutionSide") or "").lower() or None
    payout = D(raw["payout"]) if raw.get("payout") not in (None, "") else None
    return Settlement(
        venue=VENUE, account=account,
        market_id=ids.qualify(VENUE, native_symbol(str(raw.get("instrumentSymbol") or ""))),
        held=PositionSide.LONG if outcome == "yes" else PositionSide.SHORT,
        result=result,
        won=(outcome == result) if result in ("yes", "no") else None,
        quantity=D(raw.get("positionQuantity") or abs(signed)),
        cost=D(raw["costBasis"]) if raw.get("costBasis") not in (None, "") else None,
        payout=payout,
        pnl=D(raw["netProfit"]) if raw.get("netProfit") not in (None, "") else None,
        timestamp=parse_ts(raw.get("settledAt")),
        info=raw,
    )


def balance_of(rows: list[dict[str, Any]], *, account: Account, currency: str = "USD") -> Balance:
    """The USD row of `/v1/balances`: `amount` is the confirmed total,
    `available` what can be traded now."""
    row = next((r for r in rows or [] if str(r.get("currency") or "").upper() == currency), None)
    if row is None:
        return Balance(venue=VENUE, account=account, currency=currency, total=Decimal("0"), available=Decimal("0"),
                       info={"balances": rows})
    total, available = D(row.get("amount") or "0"), D(row.get("available") or "0")
    return Balance(
        venue=VENUE, account=account, currency=currency, total=total, available=available,
        locked=total - available, timestamp=parse_ts(row.get("_timestamp")), info=row,
    )


# ---------------------------------------------------------------------------
# Combos
# ---------------------------------------------------------------------------

COMBO_LEGS = (2, 6)
"""Fewest and most legs a combo takes."""

COMBO_PAGE = 50
"""Combos per listing page unless `limit` says otherwise."""

RFQ_APP_ONLY = (
    f"{VENUE}: combo RFQs are opened, and their winning quote accepted, in the Gemini app and website; Gemini "
    f"publishes no API for the requester side (developer.gemini.com/prediction-markets/combos-rfq/maker-integration). "
    f"Trade a combo on its own book with create_order on its GEMI-CMB symbol, or quote RFQs as a maker with "
    f"synpath.ws.gemini.GeminiRfqStream"
)


class CombosUnavailable(ExchangeError):
    """Gemini answered that combos are not available on this host. Its
    documentation says the combo routes are not enabled in production yet;
    `body` and `status` are Gemini's own answer. The sandbox has them."""


def _outcome_word(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text not in ("yes", "no"):
        raise InvalidOrder(f"{VENUE}: a combo leg's outcome is yes or no, got {value!r}")
    return text


def _instrument(value: Any) -> str | None:
    """A Synpath id for a contract ticker that is an instrument symbol."""
    text = native_symbol(str(value or ""))
    return ids.qualify(VENUE, text) if text.startswith("GEMI-") else None


def combo_legs_of(legs: list[Any]) -> list[dict[str, str]]:
    """`create_combo` legs as the wire's `[{"contractId", "requiredOutcome"}]`.

    A leg is a `(contract_id, outcome)` pair or a dict with `contract_id` (or
    the wire's `contractId`) and `outcome` (or `requiredOutcome`). The id is
    Gemini's numeric contract id (a combo leg's `contractId`, an RFQ leg's
    `c`), not an instrument symbol. Two to six legs, each contract once,
    checked before anything is signed."""
    out: list[dict[str, str]] = []
    for leg in legs:
        if isinstance(leg, dict):
            contract, outcome = leg.get("contract_id", leg.get("contractId")), leg.get("outcome", leg.get("requiredOutcome"))
        else:
            contract, outcome = leg
        contract = str(contract or "").strip()
        if not contract.isdigit():
            raise InvalidOrder(f"{VENUE}: a combo leg names Gemini's numeric contract id, got {contract!r}")
        out.append({"contractId": contract, "requiredOutcome": _outcome_word(outcome).capitalize()})
    low, high = COMBO_LEGS
    if not low <= len(out) <= high:
        raise InvalidOrder(f"{VENUE}: a combo takes {low} to {high} legs, got {len(out)}")
    if len({leg["contractId"] for leg in out}) != len(out):
        raise InvalidOrder(f"{VENUE}: a combo names each contract once")
    return out


def combo_of(raw: dict[str, Any]) -> dict[str, Any]:
    """A combo as one shape, from either of Gemini's: a listing or lookup row
    (`{"contract", "legs"}`) or a create answer's `combo` (`ComboSummary`).

    `market_id` is the combo's own instrument (`gemini:GEMI-CMB-...`), `None`
    while Gemini has not registered one; each leg carries its `contract_id`,
    the `outcome` the combo needs from it (`yes` / `no`), its own `market_id`
    where Gemini names its symbol, and `result` once it has resolved."""
    contract = raw.get("contract") if isinstance(raw.get("contract"), dict) else None
    if contract is not None:
        symbol = contract.get("contractTicker")
        combo_id, title, status = contract.get("contractId"), contract.get("contractName"), contract.get("contractStatus")
        registered = _instrument(symbol) is not None
        expiry = contract.get("expiryDate")
    else:
        symbol = raw.get("instrumentSymbol")
        combo_id, title, status = raw.get("id"), raw.get("displayName"), raw.get("status")
        registered = bool(raw.get("instrumentRegistered"))
        expiry = raw.get("latestExpiryDate")
    legs = []
    for leg in sorted(raw.get("legs") or [], key=lambda row: int(row.get("legIndex") or 0)):
        inner = leg.get("contract") if isinstance(leg.get("contract"), dict) else {}
        result = leg.get("legOutcome")
        legs.append({
            "contract_id": str(leg.get("contractId") or ""),
            "outcome": _outcome_word(leg.get("requiredOutcome")),
            "market_id": _instrument(inner.get("contractTicker")),
            "title": inner.get("contractName"),
            "result": str(result).lower() if result else None,
            "resolved_at": parse_ts(leg.get("resolvedAt")),
        })
    return {
        "market_id": _instrument(symbol) if registered or contract is not None else None,
        "combo_id": str(combo_id) if combo_id not in (None, "") else None,
        "title": title,
        "status": str(status).lower() if status else None,
        "registered": registered,
        "expires_at": parse_ts(expiry),
        "legs": legs,
        "info": raw,
    }


def _combo_error(exc: ExchangeError, *, base_url: str) -> ExchangeError:
    """Gemini's documented "combos unavailable" answer (a 404 with no code
    and the generic `Not Found`, where a missing combo says `Combo not found`
    and a missing leg `COMBO_LEG_NOT_FOUND`) as `CombosUnavailable`; anything
    else as it came. A 503 stays `ExchangeNotAvailable`: temporary."""
    body = exc.body if isinstance(exc.body, dict) else {}
    if not (exc.status == 404 and not body.get("code") and str(body.get("message") or "") == "Not Found"):
        return exc
    host = "production" if base_url.rstrip("/") == API_URL.rstrip("/") else base_url
    said = " ".join(str(part) for part in (exc.status, body.get("error"), body.get("message")) if part)
    return CombosUnavailable(
        f"{VENUE}: combos are not available on {host} (Gemini answered {said or exc}); Gemini's documentation says "
        f"the combo routes are not enabled in production yet; the sandbox (env sandbox) has them",
        body=exc.body, status=exc.status,
    )


def _code(body: Any) -> tuple[str, str]:
    """(the venue's error code, its message) from Gemini's error payloads:
    `{"result": "error", "reason", "message"}` (authentication),
    `{"error", "message"}` (Prediction Markets), `{"error", "code"}`."""
    if not isinstance(body, dict):
        return "", ""
    code = str(body.get("code") or body.get("reason") or body.get("error") or "")
    message = str(body.get("message") or body.get("error") or "")
    return code, message


AUTH_CODES = {
    "InvalidSignature", "InvalidNonce", "InvalidApiKey", "MissingApikeyHeader", "MissingPayloadHeader",
    "MissingSignatureHeader", "MissingNonce", "AmbiguousAuthentication", "ApiKeyIpFilteringFailure",
}


def map_error(exc: ExchangeError, *, path: str = "") -> ExchangeError:
    """The venue's error code as the typed error a caller branches on."""
    code, message = _code(exc.body)
    text = f"{code} {message} {exc}".lower()
    label = f"{VENUE}: {message or code or exc}"
    if code in AUTH_CODES or code.startswith("Missing") and "header" in code.lower():
        return AuthenticationError(f"{VENUE}: credentials refused: {code}: {message}".rstrip(": "),
                                   body=exc.body, status=exc.status)
    if code == "TERMS_NOT_ACCEPTED":
        return PermissionDenied(
            f"{VENUE}: the account has not accepted Gemini's latest Prediction Markets terms; read them with "
            f"fetch_terms() and accept on gemini.com or with accept_terms()", body=exc.body, status=exc.status,
        )
    if code in ("ACCOUNT_GROUP_BLOCKED", "ACCOUNT_RESTRICTED_SELL_ONLY") or "not permitted to trade" in text:
        return PermissionDenied(label, body=exc.body, status=exc.status)
    if isinstance(exc, AuthenticationError):
        return exc
    if "insufficientfunds" in text.replace(" ", "") or "insufficient funds" in text:
        return InsufficientFunds(label, body=exc.body, status=exc.status)
    if "marketnotopen" in text.replace(" ", "") or "market is closed" in text or "paused" in text:
        return MarketHalted(label, body=exc.body, status=exc.status)
    if code == "OrderNotFound" or (isinstance(exc, MarketNotFound) and "/order" in path):
        return OrderNotFound(label, body=exc.body, status=exc.status)
    if isinstance(exc, MarketNotFound):
        return exc
    if isinstance(exc, BadRequest):
        return OrderRejected(label, reason=code or None, info=exc.body if isinstance(exc.body, dict) else {},
                             body=exc.body, status=exc.status)
    return exc


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class GeminiTrading(TradingExchange):
    """Gemini Predictions order entry.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.gemini import GeminiTrading

    creds = require("gemini", load_credentials())
    async with GeminiTrading(creds) as gemini:
        balance = await gemini.fetch_balance()
    ```
    """

    id = VENUE
    name = "Gemini"
    has: dict[str, Capability] = {
        "create_order": True,
        "create_orders": True,
        "cancel_order": True,
        "cancel_orders": True,
        # No cancel-all route: the active orders are listed and cancelled in batches.
        "cancel_all_orders": True,
        "edit_order": False,
        # No single-order route: active orders, then the history, are searched.
        "fetch_order": True,
        "fetch_open_orders": True,
        "fetch_orders": True,
        # No fills route: one aggregate fill per order, from the history.
        "fetch_my_trades": "partial",
        "fetch_positions": True,
        "fetch_balance": True,
        "fetch_settlements": True,
        "fetch_queue_position": False,
        "fetch_fee_estimate": True,
        # Combo RFQ is a sealed WebSocket auction: the maker side is
        # `synpath.ws.gemini.GeminiRfqStream`; requesting is app-only.
        "rfq": "partial",
        "split_merge": False,
        # The account streams are `synpath.ws.gemini.GeminiUserStream`.
        "watch_orders": False,
        "watch_my_trades": False,
        "watch_positions": False,
        "watch_balance": False,
    }

    def __init__(
        self,
        credentials: GeminiCredentials,
        *,
        account_name: str = "default",
        base_url: str | None = None,
        limiter: BudgetLimiter | None = None,
        timeout: float = 30.0,
        client: Any = None,
        clock: Any = time.time,
    ):
        self.credentials = credentials
        self.env = credentials.env
        self.account = Account(venue=VENUE, name=account_name, subaccount=credentials.account)
        self.base_url = (base_url or BASE_URLS[credentials.env]).rstrip("/")
        self.signer = GeminiSigner(credentials.api_key, credentials.api_secret, account=credentials.account,
                                   clock=clock)
        # 600 private requests a minute, five a second recommended; reads and
        # writes share the venue's count, so each gets half of ten a second.
        self.limiter = limiter or BudgetLimiter(read_per_second=5, write_per_second=5)
        self.http = AsyncHttpClient(self.base_url, limiter=None, timeout=timeout, client=client, venue=VENUE,
                                    attempts=1)
        self._gate = asyncio.Lock()
        """One signed request at a time: a nonce that arrives after a later
        one from the same key is refused."""

    # -- transport ------------------------------------------------------------

    async def _call(
        self, path: str, fields: dict[str, Any] | None = None, *, method: str = "POST",
        query: dict[str, Any] | None = None, kind: str = "read", priority: Priority = Priority.NORMAL,
    ) -> Any:
        """One signed call, paced by the budget, errors mapped. A read that is
        rate limited is signed again and retried; a write is sent once."""
        attempts = READ_ATTEMPTS if kind == "read" else 1
        delay = 0.5
        for attempt in range(attempts):
            await self.limiter.acquire(cost=1, kind=kind, priority=priority)  # type: ignore[arg-type]
            async with self._gate:
                headers = self.signer.headers(path, fields)
                try:
                    return await self.http.request(
                        method, path, params={k: v for k, v in (query or {}).items() if v is not None} or None,
                        headers=headers,
                    )
                except RateLimitExceeded:
                    if attempt == attempts - 1:
                        raise
                except ExchangeError as exc:
                    raise map_error(exc, path=path) from None
            await asyncio.sleep(delay)
            delay *= 2
        raise AssertionError("unreachable")  # pragma: no cover

    async def _public(self, path: str, params: dict[str, Any] | None = None) -> Any:
        await self.limiter.acquire(cost=1, kind="read")
        return await self.http.request("GET", path, params=params)

    # -- terms ----------------------------------------------------------------

    async def fetch_terms_status(self) -> dict[str, Any]:
        """`{"hasAcceptedLatest", "acceptedVersion", "latestVersion"}` for the
        account group. Orders are refused until the latest is accepted."""
        return await self._call(f"{PREFIX}/terms/status", method="GET") or {}

    async def fetch_terms(self) -> dict[str, Any]:
        """The current Prediction Markets terms (public)."""
        return await self._public(f"{PREFIX}/terms") or {}

    async def accept_terms(self) -> dict[str, Any]:
        """Accept the latest Prediction Markets terms for the account group.
        A legal agreement made on the account's behalf: read `fetch_terms()`
        first. Never called by this library on its own."""
        return await self._call(f"{PREFIX}/terms/accept", kind="write") or {}

    # -- orders ---------------------------------------------------------------

    async def create_order(self, request: OrderRequest, *, precision: Precision | None = None) -> Order:
        """Place one order. See the module docstring for how a sell and a
        market order reach the wire."""
        body = translate_order(request, precision=precision)
        raw = await self._call(f"{PREFIX}/order", body, kind="write")
        order = order_of(raw or {}, account=request.account or self.account, request=request, body=body)
        return order.model_copy(update={"info": {**order.info, "request": body}})

    async def create_orders(self, requests: list[OrderRequest], *, precision: Precision | None = None) -> list[Order | Exception]:
        """Up to twenty orders a request, more in several. The venue checks a
        whole batch before sending any of it: one invalid entry fails that
        batch, which comes back as that error for each of its requests."""
        results: list[Order | Exception] = []
        bodies: list[dict[str, Any] | Exception] = []
        for request in requests:
            try:
                bodies.append(translate_order(request, precision=precision))
            except InvalidOrder as exc:
                bodies.append(exc)
        results = list(bodies)  # type: ignore[arg-type]
        sendable = [i for i, b in enumerate(bodies) if isinstance(b, dict)]
        for start in range(0, len(sendable), BATCH):
            chunk = sendable[start:start + BATCH]
            try:
                raw = await self._call(f"{PREFIX}/order/batch", {"orders": [bodies[i] for i in chunk]}, kind="write")
            except ExchangeError as exc:
                for index in chunk:
                    results[index] = exc
                continue
            answers = list((raw or {}).get("results") or [])
            for position, index in enumerate(chunk):
                answer = answers[position] if position < len(answers) else {}
                if isinstance(answer.get("order"), dict):
                    results[index] = order_of(answer["order"], account=requests[index].account or self.account,
                                              request=requests[index], body=bodies[index])  # type: ignore[arg-type]
                else:
                    error = map_error(BadRequest(str(answer.get("message") or answer.get("error") or "rejected"),
                                                 body=answer, status=200))
                    results[index] = error
        return results

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        """Cancel one order and read it back, so `filled` shows what matched
        before the cancel landed."""
        raw = await self._call(f"{PREFIX}/order/cancel", {"orderId": _order_id(order_id)}, kind="write",
                               priority=Priority.HIGH)
        return await self._after_cancel(order_id, raw or {}, market_id=market_id)

    async def _after_cancel(self, order_id: str, raw: dict[str, Any], *, market_id: str | None) -> Order:
        try:
            order = await self.fetch_order(order_id)
        except OrderNotFound:
            return Order(
                id=str(order_id), venue=VENUE, account=self.account,
                market_id=ids.qualify(VENUE, symbol_of(market_id)) if market_id else "",
                side=Side.BUY, type=OrderType.LIMIT, time_in_force=TimeInForce.GTC,
                status=OrderStatus.CANCELED, quantity=Decimal("0"), info={"cancel": raw},
            )
        status = order.status if order.status == OrderStatus.CLOSED else OrderStatus.CANCELED
        return order.model_copy(update={"status": status, "info": {**order.info, "cancel": raw}})

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        """Twenty a request. Each cancelled order is read back; one the venue
        refused comes back as its error."""
        results: list[Order | Exception] = []
        for start in range(0, len(order_ids), BATCH):
            chunk = order_ids[start:start + BATCH]
            raw = await self._call(f"{PREFIX}/order/batch/cancel", {"orderIds": [_order_id(o) for o in chunk]},
                                   kind="write", priority=Priority.HIGH)
            answers = {str(a.get("orderId")): a for a in (raw or {}).get("results") or []}
            for order_id in chunk:
                answer = answers.get(str(order_id), {})
                if answer.get("error"):
                    message = f"{VENUE}: {answer.get('message') or answer['error']}"
                    results.append(OrderNotFound(message, body=answer) if answer["error"] == "OrderNotFound"
                                   else OrderRejected(message, reason=str(answer["error"]), info=answer, body=answer))
                else:
                    results.append(await self._after_cancel(order_id, answer, market_id=market_id))
        return results

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int:
        """Every active order, or every one on a market, cancelled in batches.
        Returns how many the venue cancelled."""
        open_orders = await self.fetch_open_orders(market_id=market_id)
        cancelled = 0
        ids_ = [o.id for o in open_orders]
        for start in range(0, len(ids_), BATCH):
            raw = await self._call(f"{PREFIX}/order/batch/cancel",
                                   {"orderIds": [_order_id(o) for o in ids_[start:start + BATCH]]},
                                   kind="write", priority=Priority.HIGH)
            cancelled += sum(1 for a in (raw or {}).get("results") or [] if a.get("result") == "ok")
        return cancelled

    async def fetch_order(self, order_id: str) -> Order:
        """One order by id. The venue has no single-order route, so the active
        orders are searched, then the most recent thousand of the history."""
        wanted = str(order_id)
        for order in await self.fetch_open_orders():
            if order.id == wanted:
                return order
        raw = await self._call(f"{PREFIX}/orders/history", {"limit": HISTORY_PAGE})
        for row in (raw or {}).get("orders") or []:
            if str(row.get("orderId")) == wanted:
                return order_of(row, account=self.account)
        raise OrderNotFound(f"{VENUE}: no order {wanted}")

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        symbol = symbol_of(market_id) if market_id else None
        orders: list[Order] = []
        offset = 0
        while True:
            raw = await self._call(f"{PREFIX}/orders/active",
                                   {k: v for k, v in {"symbol": symbol, "limit": ACTIVE_PAGE, "offset": offset}.items()
                                    if v is not None})
            rows = (raw or {}).get("orders") or []
            orders += [order_of(r, account=self.account) for r in rows]
            if len(rows) < ACTIVE_PAGE:
                return orders
            offset += len(rows)

    async def fetch_orders(
        self, *, status: str | None = None, market_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Order]:
        """Orders in the history (`status` `filled` or `cancelled`, the venue's
        words), or the active ones (`open`). `since` (ms) reads the window from
        then to now, at most `limit` (1000) orders and no cursor; otherwise
        the history pages by offset and `next_cursor` continues it."""
        if status == "open":
            return Page(await self.fetch_open_orders(market_id=market_id))
        if status not in (None, "filled", "cancelled"):
            raise BadRequest(f"{VENUE}: status is open, filled or cancelled, got {status!r}")
        wanted = max(1, min(limit or 50, HISTORY_PAGE))
        fields: dict[str, Any] = {"limit": wanted}
        if status:
            fields["status"] = status
        if market_id:
            fields["symbol"] = symbol_of(market_id)
        offset = 0
        if since is not None:
            fields["from"], fields["to"] = int(since), int(time.time() * 1000) + 1
        elif cursor:
            if not cursor.isdigit():
                raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
            offset = int(cursor)
            fields["offset"] = offset
        raw = await self._call(f"{PREFIX}/orders/history", fields)
        rows = (raw or {}).get("orders") or []
        more = since is None and len(rows) >= wanted
        return Page([order_of(r, account=self.account) for r in rows],
                    next_cursor=str(offset + len(rows)) if more else None)

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        """One aggregate fill per order that filled, at its average price (the
        venue has no fills route; `info["aggregate"]` says so). Individual
        executions, with fees and maker/taker, arrive on the account stream."""
        page = await self.fetch_orders(market_id=market_id, since=since, limit=limit, cursor=cursor)
        fills = [fill_of_order(order.info, account=self.account) for order in page]
        return Page([f for f in fills if f is not None and (order_id is None or f.order_id == str(order_id))],
                    next_cursor=page.next_cursor)

    # -- account --------------------------------------------------------------

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        target = account or self.account
        fields = {"account": target.subaccount} if target.subaccount else None
        raw = await self._call("/v1/balances", fields)
        return balance_of(raw if isinstance(raw, list) else [], account=target)

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        """Open positions, one per contract. `event_id` filters on the venue."""
        query = {"eventTicker": ids.native(VENUE, event_id) if event_id else None}
        raw = await self._call(f"{PREFIX}/positions", query=query)
        positions = positions_of((raw or {}).get("positions") or [], account=self.account)
        if market_id:
            wanted = ids.qualify(VENUE, symbol_of(market_id))
            positions = [p for p in positions if p.market_id == wanted]
        return [p for p in positions if p.contracts > 0 or p.inventory_yes or p.inventory_no]

    async def fetch_settlements(
        self, *, market_id: str | None = None, since: int | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page[Settlement]:
        """Settled positions, newest first, paged by offset."""
        wanted = max(1, min(limit or HISTORY_PAGE, HISTORY_PAGE))
        offset = int(cursor) if cursor and cursor.isdigit() else 0
        raw = await self._call(f"{PREFIX}/positions/settled", query={"limit": wanted, "offset": offset or None})
        rows = (raw or {}).get("positions") or []
        out = [settlement_of(r, account=self.account) for r in rows]
        if market_id:
            wanted_id = ids.qualify(VENUE, symbol_of(market_id))
            out = [s for s in out if s.market_id == wanted_id]
        if since is not None:
            out = [s for s in out if (s.timestamp or 0) >= since]
        return Page(out, next_cursor=str(offset + len(rows)) if len(rows) >= wanted else None)

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, quantity: Decimal) -> FeeEstimate:
        """The contract's fee schedule (from its listing date, one public event
        read) evaluated at `price` and `quantity`. The fee is symmetric in the
        YES price, so `side` does not change it."""
        symbol = symbol_of(market_id)
        contract = await self._contract(symbol)
        schedule = fee_schedule_of(contract, symbol=symbol)
        taker = schedule.estimate(float(price), float(quantity), taker=True)
        maker = schedule.estimate(float(price), float(quantity), taker=False)
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, symbol), side=side, price=D(price), quantity=D(quantity),
            taker_fee=D(str(taker)) if taker is not None else None,
            maker_fee=D(str(maker)) if maker is not None else None,
            currency="USD", info={"schedule": schedule.model_dump()},
        )

    # -- combos and RFQ ---------------------------------------------------------

    async def _combos(self, path: str, *, params: dict[str, Any] | None = None, fields: Any = None,
                      write: bool = False) -> Any:
        try:
            if write:
                return await self._call(path, fields, kind="write")
            return await self._public(path, params)
        except ExchangeError as exc:
            raise _combo_error(exc, base_url=self.base_url) from None

    async def fetch_combos(
        self, *, status: str | None = None, contract_id: str | None = None,
        registered: bool | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[dict[str, Any]]:
        """Combos (public), `Active` ones unless `status` says otherwise
        (`Settled`, `Voided`: Gemini's words), each with its legs (`combo_of`).
        `contract_id` keeps those with that contract as a leg; `registered`
        those with (or without) an instrument to trade. Pages by offset.
        Any listed contract can be a leg: there is no separate eligibility
        list. Not enabled in production yet (`CombosUnavailable`)."""
        wanted = max(1, limit or COMBO_PAGE)
        offset = int(cursor) if cursor and cursor.isdigit() else 0
        if cursor and not cursor.isdigit():
            raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
        params = {
            "status": status, "contractId": contract_id,
            "instrumentRegistered": None if registered is None else str(registered).lower(),
            "limit": wanted, "offset": offset or None,
        }
        raw = await self._combos(f"{PREFIX}/combos", params={k: v for k, v in params.items() if v is not None}) or {}
        rows = raw.get("combos") or []
        total = (raw.get("pagination") or {}).get("total")
        more = offset + len(rows) < int(total) if total is not None else len(rows) >= wanted
        return Page([combo_of(row) for row in rows], next_cursor=str(offset + len(rows)) if more and rows else None)

    async def fetch_combo(self, market_id: str) -> dict[str, Any]:
        """One combo by its instrument (`gemini:GEMI-CMB-...`), with its legs
        and how each has resolved (public)."""
        symbol = symbol_of(market_id)
        try:
            raw = await self._combos(f"{PREFIX}/combos/{symbol}")
        except MarketNotFound as exc:
            raise MarketNotFound(f"{VENUE}: no combo {symbol}", body=exc.body, status=exc.status) from None
        return combo_of(raw or {})

    async def create_combo(self, legs: list[Any]) -> dict[str, Any]:
        """The canonical combo for these legs, created if Gemini has none:
        the same set of legs in any order answers the same combo
        (`info["alreadyExisted"]`). Legs name Gemini's numeric contract ids
        (`combo_legs_of`). Signed; needs the key's `PredictionsNewOrder`
        permission. It places nothing: trade the combo with `create_order` on
        its `market_id`. Not enabled in production yet (`CombosUnavailable`)."""
        raw = await self._combos(f"{PREFIX}/combos", fields={"legs": combo_legs_of(legs)}, write=True) or {}
        combo = combo_of(raw.get("combo") or {})
        return {**combo, "info": raw}

    async def create_rfq(self, *args: Any, **kwargs: Any) -> Any:
        """Not offered by Gemini's API: see `RFQ_APP_ONLY`."""
        raise NotSupported(RFQ_APP_ONLY)

    async def accept_quote(self, *args: Any, **kwargs: Any) -> Any:
        """Not offered by Gemini's API: see `RFQ_APP_ONLY`. A requester's
        winning quote still arrives on `GeminiRfqStream.watch_deliveries()`."""
        raise NotSupported(RFQ_APP_ONLY)

    async def _contract(self, symbol: str) -> dict[str, Any]:
        """The public contract record for a symbol, found through its event."""
        for ticker in candidate_event_tickers(symbol):
            try:
                event = await self._public(f"{PREFIX}/events/{ticker}") or {}
            except MarketNotFound:
                continue
            for group in [event, *(event.get("events") or [])]:
                for contract in group.get("contracts") or []:
                    if native_symbol(str(contract.get("instrumentSymbol") or "")) == symbol:
                        return contract
        raise MarketNotFound(f"{VENUE}: no event lists {symbol}")

    async def close(self) -> None:
        await self.http.close()


def _order_id(order_id: Any) -> int:
    text = str(order_id)
    if not text.isdigit():
        raise InvalidOrder(f"{VENUE}: order ids are numbers, got {order_id!r}")
    return int(text)

