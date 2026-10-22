"""Kalshi order entry, on the V2 event-contract endpoints.

Two facts about the venue shape everything here.

**One leg on the wire.** Kalshi V2 quotes the YES leg only: `side=bid` is buy
YES, `side=ask` is sell YES, and the price is the YES price. A NO order is
the same order seen from the other side -- buying NO at *q* is selling YES at
`1 - q` -- so that is what is sent, and that is how the venue reports it
back. An order placed as "buy NO at 0.30" is therefore read back as "sell
YES at 0.70": the same order, in the venue's own words. `info` keeps the
venue's `outcome_side`, and the engine's journal keeps the request as it was
made.

**Limit orders only.** There is no market order type on V2. A market order
here is an immediate-or-cancel limit at the caller's protection price, and a
request without one is refused rather than sent at a price nobody chose.

Every request is signed: `timestamp + METHOD + path`, Ed25519 or RSA-PSS with SHA-256
over the full request path (`/trade-api/v2/...`, no query string), sent in
three headers. The private key never leaves `KalshiSigner`.

The venue's budget is metered in tokens by account tier (`GET
/account/limits`); `fetch_limits` reads it and hands it to the budget
limiter so the adapter paces to what the account actually has.
"""
from __future__ import annotations

import base64
import logging
import time
import uuid
from decimal import Decimal
from typing import Any

from ..base import AsyncHttpClient, Capability
from .. import ids
from ..errors import AuthenticationError, BadRequest, ExchangeError, MarketNotFound, NotSupported
from ..kalshi import parse_ts, to_float
from ..types import FeeSchedule, Page
from .base import TradingExchange
from .credentials import KalshiCredentials
from .errors import (
    DuplicateClientOrderId, InsufficientFunds, InvalidOrder, MarketHalted, OrderNotFound, OrderRejected,
    RfqNotFound,
)
from .limiter import BudgetLimiter, Priority
from .money import D, kalshi_count, kalshi_dollars, validate_quantity, validate_price
from .types import (
    Account, Balance, EditRequest, FeeEstimate, Fill, HeldBy, Liquidity, Order, OrderRequest,
    OrderStatus, OrderType, Position, PositionSide, Precision, Settlement, SettlementState, Side,
    TimeInForce, VENUE_ORDER_TYPES,
    AcceptQuote, Combo, ComboLeg, MakerQuote, QuoteStatus, Rfq, RfqCloseReason, RfqQuote, RfqStatus,
)

log = logging.getLogger("synpath.trading.kalshi")

VENUE = "kalshi"

BASE_URLS = {
    "demo": "https://external-api.demo.kalshi.co/trade-api/v2",
    "prod": "https://external-api.kalshi.com/trade-api/v2",
}

TIF_TO_VENUE = {
    TimeInForce.GTC: "good_till_canceled",
    TimeInForce.IOC: "immediate_or_cancel",
    TimeInForce.FOK: "fill_or_kill",
    TimeInForce.GTD: "good_till_canceled",
}
"""`gtd` is `good_till_canceled` plus `expiration_time`. `day` is not here on
purpose: the engine rewrites it to `gtd` at the session end before an
adapter sees it, and an adapter that received one would have to invent a
session."""

DEFAULT_COST = 10
"""Tokens a call costs unless the venue's endpoint-cost table says
otherwise; one order is one default call, a batch is one per order."""

KNOWN_COSTS: dict[tuple[str, str], int] = {
    ("DELETE", "/trade-api/v2/portfolio/events/orders"): 2,
    ("DELETE", "/trade-api/v2/portfolio/events/orders/:order_id"): 2,
    ("DELETE", "/trade-api/v2/portfolio/events/orders/batched"): 2,
    ("GET", "/trade-api/v2/portfolio/orders/:order_id"): 2,
    ("POST", "/trade-api/v2/communications/quotes"): 2,
    ("DELETE", "/trade-api/v2/communications/rfqs/:rfq_id/quotes/:quote_id"): 2,
    ("GET", "/trade-api/v2/communications/rfqs/:rfq_id/quotes/:quote_id"): 2,
    ("PUT", "/trade-api/v2/communications/rfqs/:rfq_id/quotes/:quote_id/confirm"): 1,
}
"""The venue's published non-default costs as of 2026-09-17, used until
`fetch_limits` replaces them with the live table. Paths are the venue's
own patterns: `:name` matches one segment, `*name` the rest."""

DEFAULT_PRECISION = Precision(
    tick=Decimal("0.01"), min_quantity=Decimal("0.01"), quantity_step=Decimal("0.01"), whole_contracts=False,
)
"""What an order is checked against when the caller gives no instrument
spec: the coarse tick every market accepts, and the venue's two-decimal
contract count. A market with a finer ladder wants its own `Precision`."""


# ---------------------------------------------------------------------------
# Signing
# ---------------------------------------------------------------------------

class KalshiSigner:
    """Holds the private key and produces the three auth headers.

    The signing payload is `timestamp_ms + METHOD + path` with `path` the full
    request path including the `/trade-api/v2` prefix and excluding any query
    string, per the venue's own examples. The key's type picks the scheme:
    an Ed25519 key (the venue's default for new keys) signs the payload
    directly; an RSA key uses RSA-PSS, SHA-256, salt length equal to the
    digest -- the combination the venue's reference client uses and that a
    `openssl dgst -sigopt rsa_pss_saltlen:digest` reproduces.
    """

    def __init__(self, key_id: str, private_key_pem: bytes):
        try:
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.primitives.asymmetric import ed25519, rsa
        except ImportError as exc:  # pragma: no cover - depends on the environment
            raise ImportError("Kalshi signing needs the `cryptography` package: pip install synpath") from exc
        self.key_id = key_id
        self._key = serialization.load_pem_private_key(private_key_pem, password=None)
        if not isinstance(self._key, (ed25519.Ed25519PrivateKey, rsa.RSAPrivateKey)):
            raise ValueError(f"Kalshi keys are Ed25519 or RSA; this one is {type(self._key).__name__}")

    def sign(self, timestamp_ms: int, method: str, path: str) -> str:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import ed25519, padding

        payload = f"{timestamp_ms}{method.upper()}{path}".encode()
        if isinstance(self._key, ed25519.Ed25519PrivateKey):
            signature = self._key.sign(payload)
        else:
            signature = self._key.sign(
                payload,
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
                hashes.SHA256(),
            )
        return base64.b64encode(signature).decode()

    def headers(self, method: str, path: str, *, timestamp_ms: int | None = None) -> dict[str, str]:
        stamp = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": self.sign(stamp, method, path),
            "KALSHI-ACCESS-TIMESTAMP": str(stamp),
        }


# ---------------------------------------------------------------------------
# Pure translation and normalizers
# ---------------------------------------------------------------------------

def ticker_of(market_id: str) -> str:
    """The ticker behind a Synpath market id (`kalshi:KXFOO-25`) or a bare
    ticker. Another venue's id is refused before anything is signed."""
    try:
        return ids.native(VENUE, market_id)
    except BadRequest as exc:
        raise InvalidOrder(str(exc)) from None


def translate_order(request: OrderRequest, *, precision: Precision | None = None) -> dict[str, Any]:
    """An `OrderRequest` as the V2 create body, on the YES leg.

    Validation happens here, before anything is signed: price on the tick and
    inside (0, 1), quantity on the venue's two-decimal grid, an order type the
    venue holds, a time in force it understands, a protection price for a
    market order.
    """
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"kalshi: {request.type.value} is held by the execution engine, not the venue; "
            f"submit it through the engine"
        )
    if request.time_in_force == TimeInForce.DAY:
        raise InvalidOrder("kalshi: 'day' is rewritten to 'gtd' by the engine; an adapter cannot pick a session end")
    if request.price is None:
        raise InvalidOrder(
            "kalshi: a price is required -- the venue has no market orders, so a market order is an "
            "immediate-or-cancel limit at the protection price you give"
        )
    ticker = ticker_of(request.market_id)
    spec = precision or DEFAULT_PRECISION
    price = validate_price(D(request.price), spec)
    quantity = validate_quantity(D(request.quantity), spec)

    # The venue quotes the YES leg and so does this library: `buy` is a YES bid
    # at the price given, `sell` is a YES ask at the same price, which is what
    # the venue shows a NO buyer as.
    book_side, yes_price = ("bid" if request.side == Side.BUY else "ask"), price

    if request.type == OrderType.MARKET:
        tif = "fill_or_kill" if request.time_in_force == TimeInForce.FOK else "immediate_or_cancel"
    else:
        tif = TIF_TO_VENUE[request.time_in_force]

    params = dict(request.params)
    body: dict[str, Any] = {
        "ticker": ticker,
        "client_order_id": request.client_order_id or str(uuid.uuid4()),
        "side": book_side,
        "count": kalshi_count(quantity),
        "price": kalshi_dollars(yes_price),
        "time_in_force": tif,
        "self_trade_prevention_type": params.pop("self_trade_prevention_type", "taker_at_cross"),
        "post_only": request.post_only,
    }
    if request.time_in_force == TimeInForce.GTD and request.expires_at is not None:
        body["expiration_time"] = int(request.expires_at // 1000)
    if request.account and request.account.subaccount is not None:
        body["subaccount"] = int(request.account.subaccount)
    for key in ("cancel_order_on_pause", "order_group_id", "exchange_index"):
        if key in params:
            body[key] = params.pop(key)
    return body


def _status_of(raw: dict[str, Any], *, filled: Decimal, remaining: Decimal) -> OrderStatus:
    native = str(raw.get("status") or "").lower()
    if native == "resting":
        return OrderStatus.OPEN
    if native == "executed":
        return OrderStatus.CLOSED
    if native == "canceled":
        return OrderStatus.CANCELED
    # The create, amend and decrease responses carry no status word, only the
    # two counts; what they mean is unambiguous: nothing left to rest is done
    # if anything matched and cancelled if nothing did (an IOC that found no
    # liquidity, a decrease to zero).
    if remaining == 0:
        return OrderStatus.CLOSED if filled > 0 else OrderStatus.CANCELED
    return OrderStatus.OPEN


def _side_of(raw: dict[str, Any]) -> Side:
    book = str(raw.get("book_side") or raw.get("side") or "").lower()
    if book in ("bid", "ask"):
        return Side.BUY if book == "bid" else Side.SELL
    # Legacy rows carry action + (yes|no) side: buy yes / sell no are bids.
    action, side = str(raw.get("action") or "").lower(), str(raw.get("side") or "").lower()
    return Side.BUY if (action == "buy") == (side == "yes") else Side.SELL


def _execution_average(raw: dict[str, Any], *, filled: Decimal, cost: Decimal) -> Decimal | None:
    if filled <= 0 or cost <= 0:
        return None
    # Read projections report cost in their priced outcome. The new API can
    # say side=yes while outcome_side=no; only the latter identifies the cost
    # leg. Create responses already supply a YES average and bypass this path.
    outcome = str(raw.get("outcome_side") or raw.get("side") or
                  ("no" if raw.get("book_side") == "ask" else "yes")).lower()
    average = cost / filled
    if outcome == "no":
        average = 1 - average
    return average.quantize(Decimal("0.0001"))


def order_of(raw: dict[str, Any], *, account: Account | None = None) -> Order:
    """A venue order row as an `Order`, on the YES leg."""
    ticker = str(raw.get("ticker") or "")
    filled = D(raw.get("fill_count_fp") or raw.get("fill_count") or "0")
    remaining = D(raw.get("remaining_count_fp") or raw.get("remaining_count") or "0")
    initial = raw.get("initial_count_fp")
    quantity = D(initial) if initial is not None else filled + remaining
    taker_cost = D(raw.get("taker_fill_cost_dollars") or "0")
    maker_cost = D(raw.get("maker_fill_cost_dollars") or "0")
    fee = D(raw.get("taker_fees_dollars") or "0") + D(raw.get("maker_fees_dollars") or "0")
    cost = taker_cost + maker_cost
    expires = parse_ts(raw.get("expiration_time"))
    return Order(
        id=str(raw.get("order_id") or ""),
        client_order_id=raw.get("client_order_id") or None,
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, ticker),
        side=_side_of(raw),
        type=OrderType.MARKET if str(raw.get("type") or "").lower() == "market" else OrderType.LIMIT,
        time_in_force=TimeInForce.GTD if expires else TimeInForce.GTC,
        status=_status_of(raw, filled=filled, remaining=remaining),
        held_by=HeldBy.VENUE,
        price=D(raw["yes_price_dollars"]) if raw.get("yes_price_dollars") is not None else None,
        quantity=quantity,
        filled=filled,
        remaining=remaining,
        average_price=_execution_average(raw, filled=filled, cost=cost),
        cost=cost if filled > 0 else None,
        fee=fee if filled > 0 else None,
        fee_currency="USD",
        expires_at=expires,
        created_at=parse_ts(raw.get("created_time")),
        updated_at=parse_ts(raw.get("last_update_time")),
        info=raw,
    )


def order_from_response(
    raw: dict[str, Any], *, body: dict[str, Any], account: Account | None = None,
    filled: Decimal | None = None,
) -> Order:
    """The create, amend or decrease response, which carries only the counts,
    joined with the request that produced it. `filled` stands in when the
    response omits `fill_count` (amend and decrease do)."""
    if raw.get("fill_count") not in (None, ""):
        filled = D(raw["fill_count"])
    elif filled is None:
        filled = Decimal("0")
    remaining = D(raw.get("remaining_count") or "0")
    average = raw.get("average_fill_price")
    ts_ms = raw.get("ts_ms")
    return Order(
        id=str(raw.get("order_id") or ""),
        client_order_id=raw.get("client_order_id") or body.get("client_order_id"),
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, body["ticker"]),
        side=Side.BUY if body.get("side") == "bid" else Side.SELL,
        type=OrderType.LIMIT,
        time_in_force=(
            TimeInForce.GTD if body.get("expiration_time") else
            {"good_till_canceled": TimeInForce.GTC, "immediate_or_cancel": TimeInForce.IOC,
             "fill_or_kill": TimeInForce.FOK}.get(str(body.get("time_in_force")), TimeInForce.GTC)
        ),
        status=_status_of(raw, filled=filled, remaining=remaining),
        price=D(body["price"]) if body.get("price") is not None else None,
        quantity=filled + remaining,
        filled=filled,
        remaining=remaining,
        average_price=D(average) if average not in (None, "") else None,
        fee=D(raw["average_fee_paid"]) * filled if raw.get("average_fee_paid") not in (None, "") and filled > 0 else None,
        fee_currency="USD",
        post_only=bool(body.get("post_only")),
        expires_at=int(body["expiration_time"]) * 1000 if body.get("expiration_time") else None,
        created_at=int(ts_ms) if ts_ms is not None else None,
        updated_at=int(ts_ms) if ts_ms is not None else None,
        info={"response": raw, "request": body},
    )


def fill_of(raw: dict[str, Any], *, account: Account | None = None) -> Fill:
    ticker = str(raw.get("ticker") or raw.get("market_ticker") or "")
    stamp = parse_ts(raw.get("created_time")) or parse_ts(raw.get("ts")) or 0
    return Fill(
        id=str(raw.get("fill_id") or raw.get("trade_id") or ""),
        order_id=str(raw.get("order_id") or ""),
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, ticker),
        side=_side_of(raw),
        price=D(raw["yes_price_dollars"]),
        quantity=D(raw.get("count_fp") or raw.get("count") or "0"),
        fee=D(raw["fee_cost"]) if raw.get("fee_cost") not in (None, "") else None,
        fee_currency="USD",
        liquidity=Liquidity.TAKER if raw.get("is_taker") else Liquidity.MAKER,
        settlement=SettlementState.CONFIRMED,
        timestamp=stamp,
        info=raw,
    )


def position_of(raw: dict[str, Any], *, account: Account | None = None) -> Position:
    """A market position row. `position_fp` is signed: positive is YES
    contracts (`long`), negative is NO (`short`). The venue nets, so there
    is no inventory to carry."""
    ticker = str(raw.get("ticker") or "")
    signed = D(raw.get("position_fp") or "0")
    contracts = abs(signed)
    exposure = D(raw.get("market_exposure_dollars") or "0")
    return Position(
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, ticker),
        side=PositionSide.FLAT if contracts == 0 else PositionSide.LONG if signed > 0 else PositionSide.SHORT,
        contracts=contracts,
        entry_price=(exposure / contracts).quantize(Decimal("0.0001")) if contracts > 0 else None,
        realized_pnl=D(raw["realized_pnl_dollars"]) if raw.get("realized_pnl_dollars") not in (None, "") else None,
        margin=exposure if contracts > 0 else None,
        timestamp=parse_ts(raw.get("last_updated_ts")),
        info=raw,
    )


def settlement_of(raw: dict[str, Any], *, account: Account | None = None) -> Settlement:
    ticker = str(raw.get("ticker") or "")
    result = str(raw.get("market_result") or "") or None
    yes_count = D(raw.get("yes_count_fp") or "0")
    no_count = D(raw.get("no_count_fp") or "0")
    cost = D(raw.get("yes_total_cost_dollars") or "0") + D(raw.get("no_total_cost_dollars") or "0")
    revenue = raw.get("revenue")
    payout = (D(revenue) / 100).quantize(Decimal("0.01")) if revenue is not None else None
    fee = D(raw["fee_cost"]) if raw.get("fee_cost") not in (None, "") else Decimal("0")
    held = "yes" if yes_count > no_count else "no" if no_count > yes_count else None
    won = (held == result) if held and result in ("yes", "no") else None
    return Settlement(
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, ticker),
        held=PositionSide.LONG if held == "yes" else PositionSide.SHORT if held == "no" else None,
        result=result,
        won=won,
        quantity=max(yes_count, no_count) if held else None,
        cost=cost,
        payout=payout,
        pnl=(payout - cost - fee) if payout is not None else None,
        timestamp=parse_ts(raw.get("settled_time")),
        info=raw,
    )


def balance_of(raw: dict[str, Any], *, account: Account) -> Balance:
    """`balance` is the venue's available cash; the portfolio's market value
    rides along in `info`. Kalshi reports no figure for cash reserved by
    resting orders, so `locked` is `None` rather than a guess."""
    available = D(raw["balance_dollars"]) if raw.get("balance_dollars") is not None else D(raw.get("balance") or 0) / 100
    return Balance(
        venue=VENUE,
        account=account,
        currency="USD",
        total=available,
        available=available,
        locked=None,
        buying_power=None,
        timestamp=parse_ts(raw.get("updated_ts")),
        info=raw,
    )


def map_error(exc: ExchangeError, *, path: str = "") -> ExchangeError:
    """The venue's error code as the typed error a caller branches on.

    `path` is the request path: the venue answers `not_found` for a missing
    order and a missing market alike, and only the path says which."""
    code = (exc.code or "").lower()
    message = str(exc)
    text = f"{code} {message}".lower()
    if isinstance(exc, AuthenticationError):
        return exc
    if "insufficient" in text and ("balance" in text or "fund" in text):
        return InsufficientFunds(message, body=exc.body, status=exc.status)
    missing = "not_found" in text or "not found" in text or "no such" in text or isinstance(exc, MarketNotFound)
    if missing and path.startswith(RFQ_PATHS):
        return RfqNotFound(message, body=exc.body, status=exc.status)
    if missing and ("/orders" in path or "order" in text):
        return OrderNotFound(message, body=exc.body, status=exc.status)
    if "client_order_id" in text and ("duplicate" in text or "already" in text or "exists" in text):
        return DuplicateClientOrderId(message)
    if any(word in text for word in ("market_closed", "market is closed", "not open", "paused", "halted", "market_not_open")):
        return MarketHalted(message, body=exc.body, status=exc.status)
    if isinstance(exc, MarketNotFound):
        return exc
    if isinstance(exc, BadRequest):
        return OrderRejected(message, reason=code or None, info=exc.body if isinstance(exc.body, dict) else {}, body=exc.body, status=exc.status)
    return exc


# ---------------------------------------------------------------------------
# RFQ and combos (docs/rfq-combo.md, P6)
# ---------------------------------------------------------------------------
#
# Source: https://docs.kalshi.com/openapi.yaml (v3.34.0) and
# https://docs.kalshi.com/getting_started/rfqs.md.
#
# **Sides.** A Kalshi quote is two bids for the RFQ's full size: `yes_bid`
# (the maker buys YES) and `no_bid` (the maker buys NO), either one "0" (the
# side declined), their sum at most $1. In YES prices the requester sells YES
# at `yes_bid` (our `bid`) and buys YES at `1 - no_bid` (our `ask`).
# `accepted_side` names the maker's bid being taken, not the requester's
# outcome, which the P0 demo run settled: to buy YES send `no` (the price is
# `1 - no_bid`), to buy NO send `yes` (the price is `1 - yes_bid`, i.e. YES
# sold at `yes_bid`). So a reported `accepted_side` of `no` is a YES buy by
# the requester and `yes` a YES sell.
#
# **Combos.** A combo is a market created in a multivariate event collection,
# with a `KXMVE...` ticker, its own book, positions and settlement: an
# ordinary binary market, so its Synpath id is `kalshi:<ticker>`. Its legs
# are the market's `mve_selected_legs`.

RFQ_PATHS = ("/communications", "/multivariate_event_collections")
"""Paths whose 404 names an RFQ, a quote or a combo collection."""

COMBO_PREFIX = "KXMVE"
"""Every multivariate market's ticker starts so; the fallback when an RFQ
carries no `mve_collection_ticker`."""

WEEKLY_COMBO_QUOTA = 5000
"""`POST /multivariate_event_collections/{t}`: creations per user per week."""

QUOTE_STATUS_FROM_WIRE = {
    "open": QuoteStatus.OPEN,
    "accepted": QuoteStatus.ACCEPTED,
    "confirmed": QuoteStatus.CONFIRMED,
    "executed": QuoteStatus.EXECUTED,
    # Withdrawn by the maker, closed with its RFQ, or lapsed in the last look:
    # the venue has no expired or failed quote status, and none of these traded.
    "cancelled": QuoteStatus.CANCELED,
    "canceled": QuoteStatus.CANCELED,
}
QUOTE_STATUS_TO_WIRE = {
    QuoteStatus.OPEN: "open", QuoteStatus.ACCEPTED: "accepted", QuoteStatus.CONFIRMED: "confirmed",
    QuoteStatus.EXECUTED: "executed", QuoteStatus.CANCELED: "cancelled",
}

ACCEPTED_SIDE_TO_WIRE = {Side.BUY: "no", Side.SELL: "yes"}
"""The requester's YES-leg side -> the maker's bid it takes (P0, settled)."""
ACCEPTED_SIDE_FROM_WIRE = {wire: side for side, wire in ACCEPTED_SIDE_TO_WIRE.items()}

MARKET_STATUS_TO_COMBO = {
    "active": "open",
    "determined": "settled",
    "finalized": "settled",
    "amended": "settled",
}
"""A combo market's lifecycle in `Combo.status`; every other state
(initialized, inactive, closed, disputed) reads `closed`."""


def _dollars(value: Any) -> Decimal | None:
    return D(str(value)) if value not in (None, "") else None


def _positive(value: Any) -> Decimal | None:
    """A price or size, `None` for "0" (a declined side) or an absent one."""
    number = _dollars(value)
    return number if number is not None and number > 0 else None


def is_combo_ticker(ticker: str) -> bool:
    return ticker.upper().startswith(COMBO_PREFIX)


def leg_of(raw: dict[str, Any]) -> ComboLeg:
    """One `MveSelectedLeg` (`event_ticker`, `market_ticker`, `side`)."""
    side = str(raw.get("side") or "yes").lower()
    return ComboLeg(
        market_id=ids.qualify(VENUE, str(raw.get("market_ticker") or "")),
        outcome="no" if side == "no" else "yes",
        event_id=raw.get("event_ticker") or None,
        info=raw,
    )


def leg_to_wire(leg: ComboLeg, event_ticker: str) -> dict[str, str]:
    """A `TickerPair` for `selected_markets`."""
    return {"market_ticker": ticker_of(leg.market_id), "event_ticker": event_ticker, "side": leg.outcome}


def with_event_tickers(collection: dict[str, Any]) -> dict[str, Any]:
    """A collection row with `associated_event_tickers` always present: the
    venue sends it beside `associated_events`, and it is filled from those
    when it is missing."""
    row = dict(collection)
    if not row.get("associated_event_tickers"):
        row["associated_event_tickers"] = [str(e.get("ticker")) for e in row.get("associated_events") or []
                                           if e.get("ticker")]
    return row


def combo_of(market: dict[str, Any]) -> Combo:
    """A multivariate market (`GET /markets/{ticker}`, or the `market` of the
    create answer) as a `Combo`; its legs are `mve_selected_legs`."""
    ticker = str(market.get("ticker") or "")
    legs = market.get("mve_selected_legs") or []
    if len(legs) < 2:
        raise RfqNotFound(f"kalshi: {ticker or 'the market'} is not a combo (no selected legs)")
    status = str(market.get("status") or "").lower()
    return Combo(
        market_id=ids.qualify(VENUE, ticker),
        venue=VENUE,
        collection_id=market.get("mve_collection_ticker") or None,
        legs=[leg_of(leg) for leg in legs],
        status=MARKET_STATUS_TO_COMBO.get(status, "closed") if status else None,
        created_at=parse_ts(market.get("created_time")),
        info=market,
    )


def _closed_reason(raw: dict[str, Any]) -> RfqCloseReason | None:
    """`cancellation_reason` is free text on the venue; read what it plainly says."""
    if str(raw.get("status") or "").lower() != "closed":
        return None
    text = str(raw.get("cancellation_reason") or "").lower()
    if "expire" in text:
        return RfqCloseReason.EXPIRED
    if "execut" in text or "fill" in text:
        return RfqCloseReason.EXECUTED
    if "replace" in text:
        return RfqCloseReason.REPLACED
    if "cancel" in text or "delete" in text or "user" in text:
        return RfqCloseReason.CANCELED
    return None


def rfq_of(raw: dict[str, Any], *, me: str | None = None, mine: bool | None = None,
           account: Account | None = None) -> Rfq:
    """A venue `RFQ`. Sized by `target_cost_dollars` when it has one (the
    venue still reports a `contracts_fp`, kept in `info`), else by
    `contracts_fp`. `mine` is whether `creator_id` is `me`, this account's
    communications id, unless the caller already knows."""
    ticker = str(raw.get("market_ticker") or "")
    instrument = ids.qualify(VENUE, ticker)
    legs = raw.get("mve_selected_legs") or []
    combo = None
    if len(legs) >= 2:
        combo = Combo(market_id=instrument, venue=VENUE, collection_id=raw.get("mve_collection_ticker") or None,
                      legs=[leg_of(leg) for leg in legs], info={"mve_selected_legs": legs})
    on_combo = combo is not None or bool(raw.get("mve_collection_ticker")) or is_combo_ticker(ticker)
    if mine is None:
        mine = me is not None and raw.get("creator_id") == me
    amount = _positive(raw.get("target_cost_dollars"))
    status = RfqStatus.CLOSED if str(raw.get("status") or "").lower() == "closed" else RfqStatus.OPEN
    return Rfq(
        id=str(raw.get("id") or ""),
        venue=VENUE,
        account=account if mine else None,
        market_id=None if on_combo else instrument,
        combo_id=instrument if on_combo else None,
        combo=combo,
        quantity=None if amount is not None else _positive(raw.get("contracts_fp")),
        amount=amount,
        rest_remainder=bool(raw.get("rest_remainder")),
        status=status,
        closed_reason=_closed_reason(raw),
        mine=mine,
        created_at=parse_ts(raw.get("created_ts")),
        updated_at=parse_ts(raw.get("updated_ts") or raw.get("cancelled_ts")),
        info=raw,
    )


def quote_of(raw: dict[str, Any], *, me: str | None = None, mine: bool | None = None,
             account: Account | None = None) -> RfqQuote:
    """A venue `Quote` on the YES leg: `bid` = `yes_bid_dollars`, `ask` =
    `1 - no_bid_dollars`, a "0" bid a declined side (`None`).

    Sizes: `yes_contracts_fp` is read as the YES contracts the requester can
    buy (the `ask` side) and `no_contracts_fp` as the NO contracts it can buy
    (the `bid` side); both fall back to `contracts_fp`. They differ only on an
    RFQ sized by target cost (UNVERIFIED which is which; P10 checks)."""
    if mine is None:
        mine = me is not None and raw.get("creator_id") == me
    yes_bid = _positive(raw.get("yes_bid_dollars"))
    no_bid = _positive(raw.get("no_bid_dollars"))
    full = _positive(raw.get("contracts_fp"))
    ticker = raw.get("market_ticker")
    quote = RfqQuote.from_venue(dict(
        id=str(raw.get("id") or ""),
        rfq_id=str(raw.get("rfq_id") or ""),
        venue=VENUE,
        account=account if mine else None,
        market_id=ids.qualify(VENUE, str(ticker)) if ticker else None,
        bid=yes_bid,
        ask=(1 - no_bid) if no_bid is not None else None,
        bid_quantity=(_positive(raw.get("no_contracts_fp")) or full) if yes_bid is not None else None,
        ask_quantity=(_positive(raw.get("yes_contracts_fp")) or full) if no_bid is not None else None,
        status=QUOTE_STATUS_FROM_WIRE.get(str(raw.get("status") or "").lower(), QuoteStatus.OPEN),
        accepted_side=ACCEPTED_SIDE_FROM_WIRE.get(str(raw.get("accepted_side") or "").lower()),
        taker_order_id=raw.get("rfq_creator_order_id") or None,
        maker_order_id=raw.get("creator_order_id") or None,
        mine=mine,
        created_at=parse_ts(raw.get("created_ts")),
        updated_at=parse_ts(raw.get("updated_ts")),
        info=raw,
    ))
    if quote.crossed:
        log.warning("kalshi: quote %s on RFQ %s is crossed (yes_bid %s above 1 - no_bid %s); kept and flagged",
                    quote.id, quote.rfq_id, quote.bid, quote.ask)
    return quote


def tolerant(rows: Any, build: Any, what: str) -> list[Any]:
    """Normalise a venue list row by row: a row the model refuses is logged
    and skipped, so one bad record does not break the whole list."""
    out = []
    for row in rows or []:
        try:
            out.append(build(row))
        except (ValueError, TypeError) as exc:
            log.warning("kalshi: skipped a %s the venue sent that does not validate (%s): %r", what, exc, row)
    return out


def translate_rfq(
    instrument_id: str, *, quantity: Decimal | None, amount: Decimal | None, rest_remainder: bool,
    target_cost_excludes_fees: bool = False, replace_existing: bool = False, subaccount: int | None = None,
) -> dict[str, Any]:
    """The `POST /communications/rfqs` body: `contracts_fp` for contracts
    (0.01 steps), `target_cost_dollars` for money. By default the target cost
    caps principal plus Kalshi's fees and the venue shrinks each quote's size
    to make room for them; `target_cost_excludes_fees=True` makes it principal
    only, with the taker fee charged on top."""
    if (quantity is None) == (amount is None):
        raise InvalidOrder("kalshi: size an RFQ by quantity (contracts) or amount (USD), exactly one")
    body: dict[str, Any] = {"market_ticker": ticker_of(instrument_id), "rest_remainder": bool(rest_remainder)}
    if quantity is not None:
        body["contracts_fp"] = kalshi_count(validate_quantity(D(quantity), DEFAULT_PRECISION))
    else:
        if D(amount) <= 0:  # type: ignore[arg-type]
            raise InvalidOrder(f"kalshi: amount must be positive, got {amount}")
        body["target_cost_dollars"] = kalshi_dollars(D(amount))  # type: ignore[arg-type]
        if target_cost_excludes_fees:
            body["target_cost_excludes_fees"] = True
    if target_cost_excludes_fees and amount is None:
        raise InvalidOrder("kalshi: target_cost_excludes_fees applies only to an RFQ sized by amount")
    if replace_existing:
        body["replace_existing"] = True
    if subaccount is not None:
        body["subaccount"] = int(subaccount)
    return body


def translate_quote(quote: MakerQuote, *, subaccount: int | None = None) -> dict[str, Any]:
    """The `POST /communications/quotes` body. Our `bid` (the YES price we
    buy at) is `yes_bid`; our `ask` (the YES price we sell at) is buying NO
    at `1 - ask`, so `no_bid = 1 - ask`. A missing side is "0". The venue
    takes no quantity, expiry or client id: a quote is for the RFQ's size."""
    if quote.quantity is not None:
        raise InvalidOrder("kalshi: a quote is for the RFQ's full size; the venue takes no quantity")
    if quote.expires_at is not None:
        raise InvalidOrder("kalshi: a quote has no expiry on this venue; cancel it instead")
    zero = kalshi_dollars(Decimal("0"))
    body: dict[str, Any] = {
        "rfq_id": quote.rfq_id,
        "yes_bid": kalshi_dollars(quote.bid) if quote.bid is not None else zero,
        "no_bid": kalshi_dollars(1 - quote.ask) if quote.ask is not None else zero,
        "rest_remainder": bool(quote.rest_remainder),
        "post_only": bool(quote.post_only),
    }
    if subaccount is not None:
        body["subaccount"] = int(subaccount)
    return body


def accept_key(quote_id: str) -> str:
    """The engine's idempotency key for an accept (docs/rfq-combo.md §5)."""
    return f"rfq-accept:{VENUE}:{quote_id}"


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class KalshiTrading(TradingExchange):
    """Kalshi order entry.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.kalshi import KalshiTrading

    creds = require("kalshi", load_credentials())
    async with KalshiTrading(creds) as kalshi:
        balance = await kalshi.fetch_balance()
    ```
    """

    id = VENUE
    name = "Kalshi"
    has: dict[str, Capability] = {
        "create_order": True,
        "create_orders": True,
        "cancel_order": True,
        "cancel_orders": True,
        "cancel_all_orders": True,
        "edit_order": True,
        "fetch_order": True,
        "fetch_open_orders": True,
        "fetch_orders": True,
        "fetch_my_trades": True,
        "fetch_positions": True,
        "fetch_balance": True,
        "fetch_settlements": True,
        "fetch_queue_position": True,
        "fetch_fee_estimate": True,
        # The older key, kept for one release: the adapter has Kalshi's RFQ
        # methods. Since P6 they are the typed shared interface below.
        "rfq": True,
        # docs/rfq-combo.md (P6): both RFQ sides and the multivariate combos.
        "rfq_taker": True,
        "rfq_quoter": True,
        "combos": True,
        # Kalshi nets YES against NO in one account; there are no token
        # inventories to split or merge.
        "split_merge": False,
        # The user WebSocket channels arrive with the WebSocket layer.
        "watch_orders": False,
        "watch_my_trades": False,
        "watch_positions": False,
        "watch_balance": False,
    }

    def __init__(
        self,
        credentials: KalshiCredentials,
        *,
        account_name: str = "default",
        base_url: str | None = None,
        limiter: BudgetLimiter | None = None,
        timeout: float = 30.0,
        client: Any = None,
    ):
        self.credentials = credentials
        self.env = credentials.env
        self.account = Account(venue=VENUE, name=account_name)
        self.base_url = (base_url or BASE_URLS[credentials.env]).rstrip("/")
        self._path_prefix = _path_of(self.base_url)
        self.signer = KalshiSigner(credentials.key_id, credentials.private_key_pem)
        # Basic tier until `fetch_limits` says otherwise: 200 read / 100 write
        # tokens per second, the smallest budget any account has.
        self.limiter = limiter or BudgetLimiter(read_per_second=200, write_per_second=100)
        self.http = AsyncHttpClient(self.base_url, limiter=None, timeout=timeout, client=client, venue=VENUE)
        self.default_cost = DEFAULT_COST
        self.endpoint_costs: dict[tuple[str, str], int] = dict(KNOWN_COSTS)
        self._communications_id: str | None = None

    # -- transport ------------------------------------------------------------

    async def _call(
        self, method: str, path: str, *, params: Any = None, json: Any = None,
        kind: str = "read", cost: float | None = None, priority: Priority = Priority.NORMAL,
    ) -> Any:
        """One signed call, paced by the budget, errors mapped. `cost` is
        looked up in the venue's endpoint-cost table unless given."""
        full_path = f"{self._path_prefix}{path}"
        if cost is None:
            cost = self.cost_of(method, full_path)
        await self.limiter.acquire(cost=cost, kind=kind, priority=priority)  # type: ignore[arg-type]
        headers = self.signer.headers(method, full_path)
        try:
            return await self.http.request(method, path, params=_clean(params), json=json, headers=headers)
        except ExchangeError as exc:
            raise map_error(exc, path=path) from None

    def cost_of(self, method: str, full_path: str) -> int:
        """Tokens the venue charges for this call, from its cost table.

        The table's paths are patterns: `:order_id` matches one segment,
        `*endpoint` everything after it. A literal entry wins over a pattern
        so `/orders/batched` is not read as `/orders/:order_id`."""
        method = method.upper()
        literal = self.endpoint_costs.get((method, full_path))
        if literal is not None:
            return literal
        segments = full_path.split("/")
        for (table_method, pattern), cost in self.endpoint_costs.items():
            if table_method != method:
                continue
            parts = pattern.split("/")
            if _matches(parts, segments):
                return cost
        return self.default_cost

    # -- orders ---------------------------------------------------------------

    async def create_order(self, request: OrderRequest, *, precision: Precision | None = None) -> Order:
        """Place one order. See the module docstring for how a NO order and a
        market order reach the wire."""
        body = translate_order(request, precision=precision)
        raw = await self._call("POST", "/portfolio/events/orders", json=body, kind="write")
        order = order_from_response(raw, body=body, account=request.account or self.account)
        return order.model_copy(update={"book": request.book, "trader": request.trader, "tags": request.tags})

    async def create_order_with_precision(self, request: OrderRequest, precision: Precision) -> Order:
        return await self.create_order(request, precision=precision)

    async def create_orders(
        self, requests: list[OrderRequest], *, precision: Precision | None = None,
    ) -> list[Order | Exception]:
        """Many orders in one request. Each costs the venue what a single
        order costs, so the budget is drawn per order, not per call."""
        bodies: list[dict[str, Any] | Exception] = []
        for request in requests:
            try:
                bodies.append(translate_order(request, precision=precision))
            except InvalidOrder as exc:
                bodies.append(exc)
        sendable = [b for b in bodies if isinstance(b, dict)]
        results: list[Order | Exception] = list(bodies)  # type: ignore[arg-type]
        if not sendable:
            return results
        raw = await self._call(
            "POST", "/portfolio/events/orders/batched", json={"orders": sendable},
            kind="write", cost=self.default_cost * len(sendable),
        )
        answers = list((raw or {}).get("orders") or [])
        cursor = 0
        for index, body in enumerate(bodies):
            if not isinstance(body, dict):
                continue
            answer = answers[cursor] if cursor < len(answers) else {}
            cursor += 1
            error = answer.get("error")
            if error:
                results[index] = OrderRejected(str(error.get("message") or error), reason=str(error.get("code") or ""), info=error)
            else:
                results[index] = order_from_response(answer, body=body, account=requests[index].account or self.account)
        return results

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        raw = await self._call(
            "DELETE", f"/portfolio/events/orders/{order_id}",
            params={"market_ticker": ticker_of(market_id) if market_id else None}, kind="write", priority=Priority.HIGH,
        )
        return await self._after_cancel(order_id, raw)

    async def _after_cancel(self, order_id: str, raw: dict[str, Any]) -> Order:
        """The cancel response carries only `reduced_by`; the order itself is
        read back so the caller sees what filled before the cancel landed.

        The venue acknowledged the cancel, so the order *is* cancelled; the
        read-back can still say `resting` for a beat, and is corrected
        rather than trusted.
        """
        reduced = D(raw.get("reduced_by") or "0")
        try:
            order = await self.fetch_order(order_id)
        except OrderNotFound:
            return Order(
                id=order_id, venue=VENUE, account=self.account, market_id="",
                side=Side.BUY, type=OrderType.LIMIT, time_in_force=TimeInForce.GTC,
                status=OrderStatus.CANCELED, quantity=reduced, info=raw,
            )
        remaining = order.remaining if order.remaining is not None else order.quantity - order.filled
        if order.status == OrderStatus.OPEN:
            remaining = max(Decimal("0"), remaining - reduced)
        status = OrderStatus.CLOSED if order.status == OrderStatus.CLOSED else OrderStatus.CANCELED
        return order.model_copy(update={
            "status": status, "remaining": remaining, "info": {**order.info, "cancel": raw},
        })

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        raw = await self._call(
            "DELETE", "/portfolio/events/orders/batched",
            json={"orders": [{"order_id": oid, "market_ticker": ticker_of(market_id)} if market_id else {"order_id": oid} for oid in order_ids]},
            kind="write", priority=Priority.HIGH,
        )
        answers = {str(a.get("order_id")): a for a in ((raw or {}).get("orders") or [])}
        results: list[Order | Exception] = []
        for order_id in order_ids:
            answer = answers.get(order_id, {})
            error = answer.get("error")
            if error:
                results.append(OrderRejected(str(error.get("message") or error), reason=str(error.get("code") or ""), info=error))
            else:
                results.append(await self._after_cancel(order_id, answer))
        return results

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int | None:
        """Every resting order in the account, or every one on a market.

        The venue's cancel-all answers 204 with no count and works
        asynchronously -- it may also cancel orders placed in the minute
        after the call -- so the account-wide form returns `None`. The
        per-market form is a batch cancel of that market's open orders and
        does return how many it cancelled.
        """
        if market_id:
            open_orders = await self.fetch_open_orders(market_id=market_id)
            if not open_orders:
                return 0
            results = await self.cancel_orders([o.id for o in open_orders], market_id=market_id)
            return sum(1 for r in results if not isinstance(r, Exception))
        await self._call("DELETE", "/portfolio/events/orders", kind="write", priority=Priority.HIGH)
        return None

    async def edit_order(self, request: EditRequest, *, current: Order | None = None) -> Order:
        """Change a resting order in place.

        Only the quantity coming down is a *decrease*, which keeps the order's
        place in the queue. Anything else -- a new price, or more contracts --
        is an *amend*, which the venue treats as a new order at the back of
        the queue. `queue_priority_preserved` says which one this was.
        """
        current = current or await self.fetch_order(request.order_id)
        if request.time_in_force is not None or request.expires_at is not None:
            raise InvalidOrder("kalshi: time in force cannot be edited; cancel and replace")
        price_changed = request.price is not None and D(request.price) != current.price
        new_quantity = D(request.quantity) if request.quantity is not None else None
        as_sent = {
            **(current.info.get("request") or {}), "ticker": ticker_of(current.market_id),
            "side": "bid" if current.side == Side.BUY else "ask",
            "price": kalshi_dollars(current.price) if current.price is not None else None,
            "client_order_id": current.client_order_id,
        }
        if not price_changed and new_quantity is not None and new_quantity < current.quantity:
            raw = await self._call(
                "POST", f"/portfolio/events/orders/{request.order_id}/decrease",
                json={"reduce_to": kalshi_count(new_quantity - current.filled), "market_ticker": ticker_of(current.market_id)},
                kind="write", priority=Priority.HIGH,
            )
            order = self._edited(raw, body=as_sent, current=current)
            return order.model_copy(update={"queue_priority_preserved": True})
        if request.price is None and new_quantity is None:
            return current
        price = D(request.price) if request.price is not None else current.price
        quantity = new_quantity if new_quantity is not None else current.quantity
        if price is None:
            raise InvalidOrder("kalshi: the order has no price to amend")
        body = {
            "ticker": ticker_of(current.market_id),
            "side": "bid" if current.side == Side.BUY else "ask",
            "price": kalshi_dollars(validate_price(price, DEFAULT_PRECISION)),
            "count": kalshi_count(validate_quantity(quantity, DEFAULT_PRECISION)),
            "client_order_id": current.client_order_id,
            "updated_client_order_id": request.client_order_id or str(uuid.uuid4()),
        }
        raw = await self._call(
            "POST", f"/portfolio/events/orders/{request.order_id}/amend", json=body,
            kind="write", priority=Priority.HIGH,
        )
        order = self._edited(raw, body={**body, "client_order_id": body["updated_client_order_id"]}, current=current)
        return order.model_copy(update={"queue_priority_preserved": False})

    def _edited(self, raw: dict[str, Any], *, body: dict[str, Any], current: Order) -> Order:
        """An amend or decrease answer as an `Order`. The venue answers either
        with the full order (`{"order": ...}`) or with the counts alone; the
        counts alone omit what has filled, which the order read before the
        edit supplies."""
        if isinstance(raw, dict) and isinstance(raw.get("order"), dict):
            return order_of(raw["order"], account=current.account)
        answer = dict(raw or {})
        if answer.get("remaining_count") in (None, "") and body.get("count") not in (None, ""):
            # The amend answer carries no counts at all; the total it accepted
            # is the one asked for, less what had already filled.
            answer["remaining_count"] = kalshi_count(D(body["count"]) - current.filled)
        order = order_from_response(answer, body=body, account=current.account, filled=current.filled)
        return order.model_copy(update={"id": order.id or current.id, "created_at": current.created_at})

    async def fetch_order(self, order_id: str, *, attempts: int = 3) -> Order:
        """One order by id.

        The venue's order store is a beat behind order entry: an order just
        placed can answer 404 for a few hundred milliseconds. A miss is
        retried briefly before it is reported, so read-after-write works.
        """
        import asyncio

        for attempt in range(attempts):
            try:
                raw = await self._call("GET", f"/portfolio/orders/{order_id}")
            except (MarketNotFound, OrderNotFound) as exc:
                if attempt + 1 < attempts:
                    await asyncio.sleep(0.25 * (attempt + 1))
                    continue
                raise OrderNotFound(f"kalshi: no order {order_id}", body=exc.body, status=exc.status) from None
            inner = raw.get("order") if isinstance(raw, dict) else None
            if inner:
                return order_of(inner, account=self.account)
        raise OrderNotFound(f"kalshi: no order {order_id}")

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        orders: list[Order] = []
        cursor: str | None = None
        while True:
            page = await self.fetch_orders(status="resting", market_id=market_id, limit=200, cursor=cursor)
            orders.extend(page)
            cursor = page.next_cursor
            if not cursor or not page:
                return orders

    async def fetch_orders(
        self, *, status: str | None = None, market_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Order]:
        """`status` is one of the venue's own words: resting, canceled, executed."""
        raw = await self._call("GET", "/portfolio/orders", params={
            "status": status, "ticker": ticker_of(market_id) if market_id else None,
            "min_ts": int(since / 1000) if since else None,
            "limit": limit, "cursor": cursor,
        })
        rows = (raw or {}).get("orders") or []
        return Page([order_of(r, account=self.account) for r in rows], next_cursor=(raw or {}).get("cursor") or None)

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        raw = await self._call("GET", "/portfolio/fills", params={
            "ticker": ticker_of(market_id) if market_id else None, "order_id": order_id,
            "min_ts": int(since / 1000) if since else None,
            "limit": limit, "cursor": cursor,
        })
        rows = (raw or {}).get("fills") or []
        return Page([fill_of(r, account=self.account) for r in rows], next_cursor=(raw or {}).get("cursor") or None)

    async def fetch_queue_position(self, order_id: str) -> Decimal:
        raw = await self._call("GET", f"/portfolio/orders/{order_id}/queue_position")
        return D((raw or {}).get("queue_position_fp") or "0")

    # -- account --------------------------------------------------------------

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        target = account or self.account
        raw = await self._call("GET", "/portfolio/balance", params={
            "subaccount": int(target.subaccount) if target.subaccount is not None else None,
        })
        return balance_of(raw, account=target)

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        positions: list[Position] = []
        cursor: str | None = None
        while True:
            raw = await self._call("GET", "/portfolio/positions", params={
                "ticker": ticker_of(market_id) if market_id else None,
                "event_ticker": ids.native(VENUE, event_id) if event_id else None, "count_filter": "position",
                "limit": 200, "cursor": cursor,
            })
            rows = (raw or {}).get("market_positions") or []
            positions.extend(position_of(r, account=self.account) for r in rows)
            cursor = (raw or {}).get("cursor") or None
            if not cursor or not rows:
                return [p for p in positions if p.contracts > 0]

    async def fetch_settlements(
        self, *, market_id: str | None = None, since: int | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page[Settlement]:
        raw = await self._call("GET", "/portfolio/settlements", params={
            "ticker": ticker_of(market_id) if market_id else None, "min_ts": int(since / 1000) if since else None,
            "limit": limit, "cursor": cursor,
        })
        rows = (raw or {}).get("settlements") or []
        return Page([settlement_of(r, account=self.account) for r in rows], next_cursor=(raw or {}).get("cursor") or None)

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, quantity: Decimal) -> FeeEstimate:
        """The venue's published fee for the market's series, evaluated at
        `price` and `quantity` -- the same formula the read API's
        `FeeSchedule.estimate` uses. The fee is symmetric in the YES price,
        so `side` does not change it."""
        ticker = ticker_of(market_id)
        series_id = ticker.split("-")[0]
        raw = await self._call("GET", f"/series/{series_id}")
        series = (raw or {}).get("series") or {}
        if not series.get("fee_type"):
            raise NotSupported(f"kalshi: series {series_id} publishes no fee schedule")
        schedule = FeeSchedule(
            venue=VENUE, scope="series", scope_id=series_id, fee_type=str(series["fee_type"]),
            multiplier=to_float(series.get("fee_multiplier")), rounding="up_to_cent", info=series,
        )
        taker = schedule.estimate(float(price), float(quantity), taker=True)
        maker = schedule.estimate(float(price), float(quantity), taker=False)
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, ticker), side=side, price=D(price), quantity=D(quantity),
            taker_fee=D(str(taker)) if taker is not None else None,
            maker_fee=D(str(maker)) if maker is not None else None,
            currency="USD",
            info={"schedule": schedule.model_dump()},
        )

    # -- budget ---------------------------------------------------------------

    async def fetch_limits(self) -> dict[str, Any]:
        """The account's live budget, adopted by the limiter.

        Reads `GET /account/limits` (refill rate and capacity per bucket and
        the usage tier) and `GET /account/endpoint_costs` (the endpoints that
        cost other than the default 10 tokens), and reconfigures the limiter
        to them. Call once at start-up; the tier changes with volume.
        """
        limits = await self._call("GET", "/account/limits")
        costs = await self._call("GET", "/account/endpoint_costs")
        read, write = (limits or {}).get("read") or {}, (limits or {}).get("write") or {}
        if read.get("refill_rate") and write.get("refill_rate"):
            self.limiter.configure(
                read_per_second=float(read["refill_rate"]), write_per_second=float(write["refill_rate"]),
            )
        rows = (costs or {}).get("endpoint_costs") or []
        if rows:
            self.endpoint_costs = {
                (str(row.get("method") or "").upper(), str(row.get("path") or "")): int(row.get("cost") or 0)
                for row in rows
            }
        if (costs or {}).get("default_cost"):
            self.default_cost = int(costs["default_cost"])
        return {"limits": limits, "endpoint_costs": costs}

    # -- order groups ---------------------------------------------------------

    async def create_order_group(self, contracts_limit: Decimal | int) -> str:
        """A venue-side kill switch: a rolling 15-second contract limit that,
        when breached, cancels every order in the group and blocks new ones
        until `reset_order_group`. Orders join a group through
        `params={"order_group_id": ...}` on the request."""
        raw = await self._call(
            "POST", "/portfolio/order_groups/create",
            json={"contracts_limit_fp": kalshi_count(D(contracts_limit))}, kind="write",
        )
        return str((raw or {}).get("order_group_id") or "")

    async def trigger_order_group(self, order_group_id: str) -> None:
        """Cancel every order in the group now and block new ones -- the kill switch, pulled."""
        await self._call("PUT", f"/portfolio/order_groups/{order_group_id}/trigger", json={}, kind="write", priority=Priority.HIGH)

    async def reset_order_group(self, order_group_id: str) -> None:
        await self._call("PUT", f"/portfolio/order_groups/{order_group_id}/reset", json={}, kind="write")

    async def update_order_group_limit(self, order_group_id: str, contracts_limit: Decimal | int) -> None:
        await self._call(
            "PUT", f"/portfolio/order_groups/{order_group_id}/limit",
            json={"contracts_limit_fp": kalshi_count(D(contracts_limit))}, kind="write",
        )

    async def fetch_order_group(self, order_group_id: str) -> dict[str, Any]:
        return await self._call("GET", f"/portfolio/order_groups/{order_group_id}")

    async def delete_order_group(self, order_group_id: str) -> None:
        await self._call("DELETE", f"/portfolio/order_groups/{order_group_id}", kind="write", priority=Priority.HIGH)

    # -- combos (multivariate event collections) -------------------------------

    async def fetch_combo_collections(
        self, *, status: str | None = "open", event_id: str | None = None, series_id: str | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page[dict[str, Any]]:
        """`GET /multivariate_event_collections`: the collections a combo can be
        built in, as the venue's own `MultivariateEventCollection` objects
        (`collection_ticker`, `associated_events` with `active_quoters`,
        `size_min`/`size_max`, `price_ranges`...). `status` is `unopened`,
        `open` or `closed`; `event_id` keeps those that take that event."""
        raw = await self._call("GET", "/multivariate_event_collections", params={
            "status": status, "associated_event_ticker": ids.native(VENUE, event_id) if event_id else None,
            "series_ticker": series_id, "limit": limit, "cursor": cursor,
        })
        rows = [with_event_tickers(row) for row in (raw or {}).get("multivariate_contracts") or []]
        return Page(rows, next_cursor=(raw or {}).get("cursor") or None)

    async def fetch_combo_collection(self, collection_id: str) -> dict[str, Any]:
        raw = await self._call("GET", f"/multivariate_event_collections/{collection_id}")
        found = (raw or {}).get("multivariate_contract")
        if not found:
            raise RfqNotFound(f"kalshi: no collection {collection_id!r}")
        return with_event_tickers(found)

    async def _event_of(self, leg: ComboLeg) -> str:
        """A leg's event ticker: `event_id` when given, else the market's own."""
        if leg.event_id:
            return ids.native(VENUE, leg.event_id)
        raw = await self._call("GET", f"/markets/{ticker_of(leg.market_id)}")
        event = ((raw or {}).get("market") or {}).get("event_ticker")
        if not event:
            raise InvalidOrder(f"kalshi: {leg.market_id} names no event; give the leg's event_id")
        return str(event)

    async def _collection_for(self, events: list[str], size: int) -> str:
        """The one open collection that takes every leg's event at this size."""
        fits: list[str] = []
        cursor: str | None = None
        while True:
            page = await self.fetch_combo_collections(event_id=events[0], limit=200, cursor=cursor)
            for row in page:
                held = {str(e.get("ticker")) for e in row.get("associated_events") or []} \
                    or set(row.get("associated_event_tickers") or [])
                low, high = row.get("size_min"), row.get("size_max")
                if set(events) <= held and (low is None or size >= int(low)) and (high is None or size <= int(high)):
                    fits.append(str(row.get("collection_ticker")))
            cursor = page.next_cursor
            if not cursor or not page:
                break
        if len(fits) == 1:
            return fits[0]
        if not fits:
            raise InvalidOrder(f"kalshi: no open collection takes events {', '.join(events)} as a {size}-leg combo")
        exc = InvalidOrder(
            f"kalshi: {len(fits)} open collections take these legs ({', '.join(fits)}); name one with "
            "collection_id"
        )
        exc.collections = fits  # type: ignore[attr-defined]  # served as `details.collections`
        raise exc

    async def create_combo(self, legs: list[ComboLeg], *, collection_id: str | None = None) -> Combo:
        """`POST /multivariate_event_collections/{collection}` with
        `selected_markets`: the combo market for these legs, created if it does
        not exist (the venue returns the same market for the same legs). It
        must be called once before a combo can be traded or read. Signed, and
        capped at 5,000 creations a user a week (`WEEKLY_COMBO_QUOTA`).

        `collection_id` is the collection's ticker (`KXMVE...`); without it the
        one open collection that takes every leg's event is used, and more
        than one is refused. A leg without `event_id` has its market read for
        it. Legs the collection refuses are `InvalidOrder`."""
        if len(legs) < 2:
            raise InvalidOrder(f"kalshi: a combo has at least two legs, got {len(legs)}")
        if len({ticker_of(leg.market_id) for leg in legs}) != len(legs):
            raise InvalidOrder("kalshi: a combo names each market once")
        events = [await self._event_of(leg) for leg in legs]
        collection = collection_id or await self._collection_for(list(dict.fromkeys(events)), len(legs))
        body = {"selected_markets": [leg_to_wire(leg, event) for leg, event in zip(legs, events)],
                "with_market_payload": True}
        try:
            raw = await self._call("POST", f"/multivariate_event_collections/{collection}", json=body, kind="write")
        except OrderRejected as exc:
            raise InvalidOrder(str(exc), body=exc.body, status=exc.status) from None
        ticker = str((raw or {}).get("market_ticker") or "")
        if not ticker:
            raise ExchangeError("kalshi: create combo returned no market", body=raw)
        market = (raw or {}).get("market") or {}
        if market.get("mve_selected_legs"):
            combo = combo_of(market)
        else:
            combo = Combo(
                market_id=ids.qualify(VENUE, ticker), venue=VENUE, collection_id=collection,
                legs=[leg.model_copy(update={"event_id": event}) for leg, event in zip(legs, events)],
                info={"response": raw},
            )
        return combo.model_copy(update={"collection_id": combo.collection_id or collection,
                                        "info": {**combo.info, "event_ticker": raw.get("event_ticker")}})

    async def fetch_combo(self, combo_id: str) -> Combo:
        """`GET /markets/{ticker}` on a combo's market ticker; legs from its
        `mve_selected_legs`. A market that is not a combo is `RfqNotFound`."""
        ticker = ticker_of(combo_id)
        try:
            raw = await self._call("GET", f"/markets/{ticker}")
        except MarketNotFound as exc:
            raise RfqNotFound(f"kalshi: no combo {ticker!r}", body=exc.body, status=exc.status) from None
        return combo_of((raw or {}).get("market") or {"ticker": ticker})

    async def lookup_combo(self, legs: list[ComboLeg]) -> Combo | None:
        raise NotSupported(
            "kalshi: no read-only lookup by legs (the lookup endpoint left the API); create_combo returns the "
            "existing combo market for the same legs"
        )

    # -- RFQ, both sides --------------------------------------------------------

    async def fetch_communications_id(self) -> str:
        """This account's public communications id (`GET /communications/id`),
        which marks its own RFQs (`creator_id`) and quotes. Read once and kept."""
        if self._communications_id is None:
            raw = await self._call("GET", "/communications/id")
            self._communications_id = str((raw or {}).get("communications_id") or "") or None
        return self._communications_id or ""

    async def _rfq_page(self, params: dict[str, Any], *, mine: bool | None) -> Page[Rfq]:
        me = None if mine is not None else await self.fetch_communications_id()
        raw = await self._call("GET", "/communications/rfqs", params=params)
        rows = tolerant((raw or {}).get("rfqs"), lambda row: rfq_of(row, me=me, mine=mine, account=self.account), "RFQ")
        return Page(rows, next_cursor=(raw or {}).get("cursor") or None)

    async def _quote_list(self, params: dict[str, Any], *, mine: bool | None = None) -> list[RfqQuote]:
        me = None if mine is not None else await self.fetch_communications_id()
        quotes: list[RfqQuote] = []
        cursor: str | None = None
        while True:
            raw = await self._call("GET", "/communications/quotes", params={**params, "cursor": cursor})
            quotes.extend(tolerant((raw or {}).get("quotes"),
                                   lambda row: quote_of(row, me=me, mine=mine, account=self.account), "quote"))
            cursor = (raw or {}).get("cursor") or None
            if not cursor:
                return quotes

    @staticmethod
    def _quote_status(status: QuoteStatus | str | None) -> str | None:
        if status is None:
            return None
        wire = QUOTE_STATUS_TO_WIRE.get(QuoteStatus(status))
        if wire is None:
            raise BadRequest(f"kalshi: quotes have no {QuoteStatus(status).value!r} status on this venue")
        return wire

    def _subaccount(self, account: Account | None = None) -> int | None:
        target = account or self.account
        return int(target.subaccount) if target.subaccount is not None else None

    # -- RFQ, requester (taker) side --------------------------------------------

    async def create_rfq(
        self, instrument_id: str, *, quantity: Decimal | None = None, amount: Decimal | None = None,
        outcome: str | None = None, action: str | None = None, rest_remainder: bool = False,
        combo: Combo | None = None, target_cost_excludes_fees: bool = False, replace_existing: bool = False,
    ) -> Rfq:
        """`POST /communications/rfqs` on a market or a combo market. Two-sided:
        the venue takes no side, so `outcome`/`action` are refused. `quantity`
        is sent as `contracts_fp`; `amount` (USD) as `target_cost_dollars`,
        which by default caps principal plus fees (quote sizes shrink to fit
        the fees); `target_cost_excludes_fees=True` makes it principal only.
        At most 100 open RFQs and one per market (409 otherwise, unless
        `replace_existing`). An unanswered RFQ expires on the venue.
        Returns the RFQ as sent, `open`; the venue answers with its id."""
        if outcome is not None or action is not None:
            raise InvalidOrder("kalshi: an RFQ is two-sided; pick the side when accepting a quote")
        body = translate_rfq(instrument_id, quantity=quantity, amount=amount, rest_remainder=rest_remainder,
                             target_cost_excludes_fees=target_cost_excludes_fees, replace_existing=replace_existing,
                             subaccount=self._subaccount())
        raw = await self._call("POST", "/communications/rfqs", json=body, kind="write")
        rfq_id = str((raw or {}).get("id") or "")
        if not rfq_id:
            raise ExchangeError("kalshi: create RFQ returned no id", body=raw)
        instrument = ids.qualify(VENUE, body["market_ticker"])
        on_combo = combo is not None or is_combo_ticker(body["market_ticker"])
        return Rfq(
            id=rfq_id, venue=VENUE, account=self.account,
            market_id=None if on_combo else instrument, combo_id=instrument if on_combo else None, combo=combo,
            quantity=D(body["contracts_fp"]) if "contracts_fp" in body else None,
            amount=D(amount) if amount is not None else None,
            rest_remainder=rest_remainder, status=RfqStatus.OPEN, mine=True, created_at=int(time.time() * 1000),
            info={"request": body, "response": raw},
        )

    async def fetch_rfq(self, rfq_id: str) -> Rfq:
        me = await self.fetch_communications_id()
        raw = await self._call("GET", f"/communications/rfqs/{rfq_id}")
        found = (raw or {}).get("rfq")
        if not found:
            raise RfqNotFound(f"kalshi: no RFQ {rfq_id}")
        return rfq_of(found, me=me, account=self.account)

    async def fetch_rfqs(
        self, *, status: RfqStatus | str | None = None, market_id: str | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page[Rfq]:
        """This account's RFQs (`user_filter=self`)."""
        return await self._rfq_page({
            "user_filter": "self", "status": RfqStatus(status).value if status else None,
            "market_ticker": ticker_of(market_id) if market_id else None,
            "subaccount": self._subaccount(), "limit": limit, "cursor": cursor,
        }, mine=True)

    async def cancel_rfq(self, rfq_id: str) -> None:
        """`DELETE /communications/rfqs/{id}`."""
        await self._call("DELETE", f"/communications/rfqs/{rfq_id}", kind="write", priority=Priority.HIGH)

    async def fetch_quotes(self, rfq_id: str, *, status: QuoteStatus | str | None = None) -> list[RfqQuote]:
        """The makers' quotes on one of our RFQs: `rfq_user_filter=self` and
        `rfq_id`. Rows for another RFQ are dropped here as well."""
        rows = await self._quote_list({
            "rfq_user_filter": "self", "rfq_id": rfq_id, "status": self._quote_status(status), "limit": 500,
        })
        return [q for q in rows if q.rfq_id == rfq_id]

    async def fetch_quote(self, quote_id: str, *, rfq_id: str | None = None) -> RfqQuote:
        """`GET /communications/rfqs/{rfq}/quotes/{q}`."""
        if not rfq_id:
            raise InvalidOrder("kalshi: a quote is read under its RFQ; give rfq_id")
        me = await self.fetch_communications_id()
        raw = await self._call("GET", f"/communications/rfqs/{rfq_id}/quotes/{quote_id}")
        found = (raw or {}).get("quote")
        if not found:
            raise RfqNotFound(f"kalshi: no quote {quote_id} on RFQ {rfq_id}")
        return quote_of(found, me=me, account=self.account)

    async def accept_quote(self, accept: AcceptQuote) -> RfqQuote:
        """`PUT /communications/rfqs/{rfq}/quotes/{q}/accept {accepted_side}`.

        `accepted_side` names the maker's bid taken (P0, settled on the demo):
        buying YES (or selling NO) sends `no`, at `1 - no_bid` (the quote's
        `ask`); buying NO (or selling YES) sends `yes`, at `1 - yes_bid` a NO
        contract, i.e. YES sold at `yes_bid` (the `bid`).

        The quote is read first. Refused before anything is sent when it is
        not open, the side was declined (its bid "0"), or the price is worse
        than `worst_price`. REST accepts the full size only (`quantity` and
        `amount` are refused; FIX takes a partial size). A retry is safe: a
        quote already accepted on the same side is returned without sending
        again. The venue takes no client id; the idempotency key defaults to
        `rfq-accept:kalshi:{quote_id}` and is kept in `info`. Returns at once
        with the quote `accepted`; the maker then confirms (30 s, 3 s on combo
        and high-volume markets) and the orders follow (`taker_order_id` is
        `rfq_creator_order_id`; its fills are in `fetch_my_trades`)."""
        if not accept.rfq_id:
            raise InvalidOrder("kalshi: a quote is accepted under its RFQ; give rfq_id")
        if accept.quantity is not None or accept.amount is not None:
            raise InvalidOrder("kalshi: a quote is accepted for its full size over REST; the venue takes no partial size")
        side = accept.side
        wire_side = ACCEPTED_SIDE_TO_WIRE[side]
        key = accept.client_order_id or accept_key(accept.quote_id)
        quote = await self.fetch_quote(accept.quote_id, rfq_id=accept.rfq_id)
        if quote.status in (QuoteStatus.ACCEPTED, QuoteStatus.CONFIRMED, QuoteStatus.EXECUTED):
            if quote.accepted_side == side:
                return quote.model_copy(update={"info": {**quote.info, "accept": {
                    "accepted_side": wire_side, "client_order_id": key, "resent": False}}})
            raise OrderRejected(
                f"kalshi: quote {quote.id} was already accepted on the {quote.accepted_side} side",
                reason="already_accepted", info=quote.info,
            )
        if quote.status != QuoteStatus.OPEN:
            raise OrderRejected(f"kalshi: quote {quote.id} is {quote.info.get('status') or quote.status.value}, not open",
                                reason="quote_not_open", info=quote.info)
        price = quote.price_for(accept.outcome, accept.action)
        if price is None:
            declined = "no_bid" if side == Side.BUY else "yes_bid"
            raise InvalidOrder(f"kalshi: quote {quote.id} declines the {side.value} side ({declined} is 0)")
        if accept.refuses(price):
            raise InvalidOrder(
                f"kalshi: quote {quote.id} {side.value}s YES at {price}, worse than the worst price {accept.worst_price}"
            )
        body = {"accepted_side": wire_side}
        raw = await self._call(
            "PUT", f"/communications/rfqs/{accept.rfq_id}/quotes/{accept.quote_id}/accept", json=body,
            kind="write", priority=Priority.HIGH,
        )
        return quote.model_copy(update={
            "status": QuoteStatus.ACCEPTED, "accepted_side": side,
            "info": {**quote.info, "accept": {**body, "client_order_id": key, "response": raw, "resent": True}},
        })

    # -- RFQ, quoter (maker) side -----------------------------------------------

    async def fetch_incoming_rfqs(
        self, *, market_id: str | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Rfq]:
        """Open RFQs from other accounts (ours are left out)."""
        page = await self._rfq_page({
            "status": "open", "market_ticker": ticker_of(market_id) if market_id else None,
            "limit": limit, "cursor": cursor,
        }, mine=None)
        return Page([r for r in page if not r.mine], next_cursor=page.next_cursor)

    async def send_quote(self, quote: MakerQuote) -> RfqQuote:
        """`POST /communications/quotes`, both sides at once: `yes_bid` = our
        `bid`, `no_bid` = `1 - ask`, a missing side "0", with `rest_remainder`
        and `post_only`. The venue holds collateral for a quote when it is
        sent (a demo quote with too little balance is a 400
        `insufficient_balance`, `InsufficientFunds`). Returns the quote as
        sent, `open`, under the venue's new id."""
        body = translate_quote(quote, subaccount=self._subaccount(quote.account))
        raw = await self._call("POST", "/communications/quotes", json=body, kind="write")
        quote_id = str((raw or {}).get("id") or "")
        if not quote_id:
            raise ExchangeError("kalshi: create quote returned no id", body=raw)
        return RfqQuote(
            id=quote_id, rfq_id=quote.rfq_id, venue=VENUE, account=quote.account or self.account,
            bid=quote.bid, ask=quote.ask, status=QuoteStatus.OPEN, mine=True, created_at=int(time.time() * 1000),
            info={"request": body, "response": raw, "client_order_id": quote.client_order_id},
        )

    async def replace_quote(self, quote_id: str, quote: MakerQuote) -> RfqQuote:
        """Kalshi has no replace: the old quote is deleted, then the new one is
        sent. Between the two the RFQ has no quote of ours (one round trip).
        An old quote already gone is not an error; if the send then fails,
        nothing of ours is left on the RFQ."""
        try:
            await self.cancel_quote(quote_id, rfq_id=quote.rfq_id)
        except RfqNotFound:
            pass
        new = await self.send_quote(quote)
        return new.model_copy(update={"info": {**new.info, "replaces": quote_id}})

    async def cancel_quote(self, quote_id: str, *, rfq_id: str | None = None) -> None:
        """`DELETE /communications/rfqs/{rfq}/quotes/{q}`: withdraw a quote, so
        it can no longer be accepted (or decline an accepted one before the
        confirm)."""
        if not rfq_id:
            raise InvalidOrder("kalshi: a quote is deleted under its RFQ; give rfq_id")
        await self._call("DELETE", f"/communications/rfqs/{rfq_id}/quotes/{quote_id}", kind="write",
                         priority=Priority.HIGH)

    async def confirm_quote(self, quote: RfqQuote) -> RfqQuote:
        """`PUT /communications/rfqs/{rfq}/quotes/{q}/confirm`, the maker's last
        look: within 30 s of the accept (3 s on combo and high-volume
        markets). The venue then starts the execution timer and the two
        orders reach the book after 15 s (1 s on high-volume markets)."""
        raw = await self._call(
            "PUT", f"/communications/rfqs/{quote.rfq_id}/quotes/{quote.id}/confirm", json={},
            kind="write", priority=Priority.HIGH,
        )
        return quote.model_copy(update={"status": QuoteStatus.CONFIRMED, "info": {**quote.info, "confirm": raw}})

    async def fetch_my_quotes(
        self, *, rfq_id: str | None = None, status: QuoteStatus | str | None = None,
    ) -> list[RfqQuote]:
        """Quotes this account made (`user_filter=self`, the spec's filter for
        "quotes created by the authenticated user"), on one RFQ or all."""
        return await self._quote_list({
            "user_filter": "self", "rfq_id": rfq_id, "status": self._quote_status(status), "limit": 500,
        }, mine=True)

    async def close(self) -> None:
        await self.http.close()


def _path_of(url: str) -> str:
    """`https://host/trade-api/v2` -> `/trade-api/v2`, the prefix the signature covers."""
    from urllib.parse import urlsplit

    return urlsplit(url).path.rstrip("/")


def _matches(pattern: list[str], segments: list[str]) -> bool:
    for index, part in enumerate(pattern):
        if part.startswith("*"):
            return index < len(segments)
        if index >= len(segments):
            return False
        if part.startswith(":"):
            continue
        if part != segments[index]:
            return False
    return len(pattern) == len(segments)


def _clean(params: Any) -> Any:
    if params is None:
        return None
    return {key: value for key, value in params.items() if value is not None}
