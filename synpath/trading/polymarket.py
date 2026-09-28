"""Polymarket (global CLOB V2) order entry.

Three facts about the venue shape this adapter.

**Every order is a signed message.** An order is an EIP-712 struct the wallet
signs; the CLOB matches it and the exchange contract settles it on Polygon.
Its id is the struct's hash, so the adapter knows the id before the order
is sent -- a journal can record it first, and a lost response is recovered
by asking for that id. Signing, amounts and wallet calls live in
`polymarket_signing`, checked byte for byte against the venue's client.

**YES and NO are separate tokens with separate books.** An order in this
library names a market and a side on the YES leg; this adapter picks the
token. `buy` buys the YES token at the price given. `sell` buys the NO token
at `1 - price`, because the venue has no netting and you cannot sell YES you
do not hold. With `reduce_only`, `sell` sells the YES tokens held instead,
and `buy` sells the NO tokens held. Everything the venue reports on the NO
token comes back converted to the YES leg, and a position is the two token
inventories netted, with both kept beside it. The Gamma catalog supplies
which token is which; the CLOB never says.

**A fill is not final when it matches.** A trade is `MATCHED`, then mined,
then `CONFIRMED` -- or `FAILED`, and then the tokens never arrived.
`Fill.settlement` carries which; code that acts on fills should act on
`confirmed` ones.

Wallets: a Deposit Wallet (signature type 3, the default for accounts
created since May 2026), a legacy proxy (1) or Safe (2), or an allowlisted
EOA (0). Orders work for all four. Approvals, split, merge and redeem go
through the gasless relayer for a Deposit Wallet and as the EOA's own
Polygon transactions for an EOA; the legacy proxy and Safe payloads are not
built here.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import Any

from ..base import AsyncHttpClient, Capability
from ..errors import (
    AuthenticationError, BadRequest, ExchangeError, ExchangeNotAvailable, MarketNotFound, NotSupported,
    RateLimitExceeded,
)
from .. import ids
from ..polymarket import parse_ts
from ..types import FeeSchedule, Page
from . import polymarket_signing as sig
from .base import TradingExchange
from .credentials import SYNPATH_BUILDER_CODE, PolymarketCredentials
from .errors import CredentialsMissing, InsufficientFunds, InvalidOrder, MarketHalted, OrderNotFound, OrderRejected
from .limiter import BudgetLimiter, Priority
from .money import D, from_poly_raw_amount, validate_amount, validate_price
from .types import (
    Account, Balance, FeeEstimate, Fill, HeldBy, Liquidity, Order, OrderRequest, OrderStatus, OrderType,
    Position, PositionSide, Precision, SettlementState, Side, TimeInForce, VENUE_ORDER_TYPES,
)

log = logging.getLogger(__name__)

VENUE = "polymarket"

CLOB_URL = "https://clob.polymarket.com"
DATA_URL = "https://data-api.polymarket.com"
GAMMA_URL = "https://gamma-api.polymarket.com"
RELAYER_URL = "https://relayer-v2.polymarket.com"

BUILDER_SIGNER_URL = "https://api.synpath.dev/v1/builder/sign"
"""Synpath's builder signer. Polymarket's relayer accepts a builder's
credentials in place of the account's own Relayer API key; the builder secret
stays on Synpath's server, which answers with the headers for one request.
It moves no money -- the relayer only runs a batch the account's own key
signed. `SYNPATH_BUILDER_SIGNER_URL` points elsewhere, `none` turns it off."""

INITIAL_CURSOR = "MA=="
END_CURSOR = "LTE="

ORDER_BATCH = 15
"""Orders per `POST /orders`, the venue's limit."""
CANCEL_BATCH = 1000
"""Ids per `DELETE /orders`, the venue's limit since 2026-06-15."""

GTD_SAFETY_S = 60
"""The venue expires a GTD order one minute before its stated expiration."""
GTD_MIN_LEAD_S = 180
"""And refuses an expiration less than three minutes away."""

SIZE_STEP = Decimal("0.01")
"""Shares carry two decimals at every tick size."""

MARKET_TTL_S = 60.0
"""How long a token's tick, minimum and neg-risk flag are trusted. Ticks
change as a price nears the ends; a rejection for a stale tick also clears
the entry."""


# ---------------------------------------------------------------------------
# Market facts an order needs
# ---------------------------------------------------------------------------

@dataclass
class TokenInfo:
    """What signing an order for one token requires."""

    token_id: str
    condition_id: str
    tick: Decimal
    min_size: Decimal
    neg_risk: bool
    read_at: float = field(default_factory=time.monotonic)

    @property
    def precision(self) -> Precision:
        return Precision(tick=self.tick, min_amount=self.min_size, amount_step=SIZE_STEP, whole_contracts=False)

    @classmethod
    def from_book(cls, token_id: str, book: dict[str, Any]) -> "TokenInfo":
        return cls(
            token_id=token_id,
            condition_id=str(book.get("market") or ""),
            tick=D(book.get("tick_size") or "0.01"),
            min_size=D(book.get("min_order_size") or "0"),
            neg_risk=bool(book.get("neg_risk")),
        )


@dataclass
class MarketTokens:
    """Which token is which side of a market, from the Gamma catalog."""

    gamma_id: str
    condition_id: str
    yes_token: str
    no_token: str
    neg_risk: bool = False

    @property
    def market_id(self) -> str:
        return ids.qualify(VENUE, self.gamma_id)

    def token(self, outcome: str) -> str:
        return self.yes_token if outcome == "yes" else self.no_token

    def outcome_of(self, token_id: str) -> str:
        if token_id == self.yes_token:
            return "yes"
        if token_id == self.no_token:
            return "no"
        raise InvalidOrder(f"polymarket: token {token_id} is not a side of market {self.gamma_id}")

    @classmethod
    def from_gamma(cls, raw: dict[str, Any]) -> "MarketTokens":
        tokens = raw.get("clobTokenIds")
        if isinstance(tokens, str):
            tokens = json.loads(tokens or "[]")
        tokens = [str(t) for t in (tokens or [])]
        if len(tokens) != 2 or not raw.get("id"):
            raise MarketNotFound(f"polymarket: market {raw.get('id')!r} publishes no token pair")
        return cls(
            gamma_id=str(raw["id"]), condition_id=str(raw.get("conditionId") or ""),
            yes_token=tokens[0], no_token=tokens[1], neg_risk=bool(raw.get("negRisk")),
        )


# ---------------------------------------------------------------------------
# Pure translation
# ---------------------------------------------------------------------------

def check_token_id(token_id: str) -> str:
    if not token_id.isdigit():
        raise InvalidOrder(f"polymarket: {token_id!r} is not a CLOB token id")
    return token_id


def wire_leg(request: OrderRequest) -> tuple[str, str]:
    """`(outcome, venue_side)`: which token the order goes to and whether it
    buys or sells that token. See the module docstring."""
    if request.side == Side.BUY:
        return ("no", "SELL") if request.reduce_only else ("yes", "BUY")
    return ("yes", "SELL") if request.reduce_only else ("no", "BUY")


def to_yes_leg(outcome: str, venue_side: str, price: Decimal | None) -> tuple[Side, Decimal | None]:
    """An order or fill the venue holds on one token, seen from the YES leg:
    buying NO at 0.30 is selling at 0.70."""
    buying = str(venue_side).upper() == "BUY"
    if outcome == "yes":
        return (Side.BUY if buying else Side.SELL), price
    return (Side.SELL if buying else Side.BUY), (Decimal("1") - price if price is not None else None)


def order_type_of(request: OrderRequest) -> str:
    """The wire `orderType`: how long the unfilled part lives."""
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"polymarket: {request.type.value} is held by the execution engine, not the venue; "
            f"submit it through the engine"
        )
    tif = request.time_in_force
    if tif == TimeInForce.DAY:
        raise InvalidOrder("polymarket: 'day' is rewritten to 'gtd' by the engine; an adapter cannot pick a session end")
    if request.type == OrderType.MARKET:
        return "FOK" if tif == TimeInForce.FOK else "FAK"
    return {TimeInForce.GTC: "GTC", TimeInForce.GTD: "GTD", TimeInForce.IOC: "FAK", TimeInForce.FOK: "FOK"}[tif]


def expiration_of(request: OrderRequest, *, now_s: int) -> int:
    """The wire `expiration`, seconds: the caller's expiry plus the minute the
    venue takes off, so the order lives until the time asked for."""
    if request.time_in_force != TimeInForce.GTD:
        return 0
    if request.expires_at is None:
        raise InvalidOrder("polymarket: a gtd order needs expires_at")
    expiration = math.ceil(request.expires_at / 1000) + GTD_SAFETY_S
    if expiration < now_s + GTD_MIN_LEAD_S:
        raise InvalidOrder(
            f"polymarket: gtd orders must live at least {GTD_MIN_LEAD_S - GTD_SAFETY_S}s; the venue refuses an "
            f"expiration under {GTD_MIN_LEAD_S}s away and expires orders {GTD_SAFETY_S}s early"
        )
    return expiration


def validate_order(request: OrderRequest, info: TokenInfo, *, outcome: str = "yes") -> tuple[Decimal, Decimal]:
    """The token price and size as the venue will accept them, or
    `InvalidOrder` saying why. `request.price` is the YES price; for an
    order that goes to the NO token it is converted first."""
    if request.price is None:
        raise InvalidOrder(
            "polymarket: a price is required -- a market order is sent as an immediate limit at the "
            "protection price you give"
        )
    yes_price = D(request.price)
    if not Decimal("0") < yes_price < Decimal("1"):
        raise InvalidOrder(f"polymarket: price {yes_price} is outside (0, 1)")
    price = validate_price(yes_price if outcome == "yes" else Decimal("1") - yes_price, info.precision)
    if price < info.tick or price > 1 - info.tick:
        raise InvalidOrder(f"polymarket: price {price} is outside [{info.tick}, {1 - info.tick}] for this tick")
    size = validate_amount(D(request.amount), info.precision)
    if request.post_only and request.time_in_force not in (TimeInForce.GTC, TimeInForce.GTD):
        raise InvalidOrder("polymarket: post-only orders must be gtc or gtd")
    if request.post_only and request.type == OrderType.MARKET:
        raise InvalidOrder("polymarket: a market order cannot be post-only")
    return price, size


def build_signed_order(
    request: OrderRequest, info: TokenInfo, *, signer: sig.WalletSigner, creds: PolymarketCredentials,
    api_key: str, now_ms: int, salt: int | None = None,
) -> tuple[dict[str, Any], str]:
    """The `POST /order` entry and the order's id, before anything is sent.
    `info` is the token the order goes to, chosen by `wire_leg`."""
    try:
        ids.native(VENUE, request.market_id)
    except BadRequest as exc:
        raise InvalidOrder(str(exc)) from None
    outcome, side = wire_leg(request)
    token_id = check_token_id(info.token_id)
    price, size = validate_order(request, info, outcome=outcome)
    order_type = order_type_of(request)
    maker_amount, taker_amount = sig.limit_amounts(side, price, size, info.tick)
    signature_type = creds.signature_type
    maker = creds.funder if signature_type != sig.EOA and creds.funder else signer.address
    order_signer = maker if signature_type == sig.DEPOSIT_WALLET else signer.address
    builder = request.params.get("builder_code") or creds.builder_code
    order = sig.build_order(
        maker=maker, signer=order_signer, token_id=token_id,
        maker_amount=maker_amount, taker_amount=taker_amount, side=side,
        signature_type=signature_type, timestamp_ms=now_ms,
        salt=salt if salt is not None else sig.new_salt(),
        expiration=expiration_of(request, now_s=now_ms // 1000),
        builder=builder,
    )
    order["signature"] = sig.sign_order(signer, order, neg_risk=info.neg_risk)
    entry: dict[str, Any] = {
        "deferExec": bool(request.params.get("defer_exec", False)),
        "order": order,
        "orderType": order_type,
        "owner": api_key,
    }
    if request.post_only:
        entry["postOnly"] = True
    return entry, sig.order_hash(order, neg_risk=info.neg_risk)


def serialize(body: Any) -> str:
    """The exact text sent, which is the exact text the HMAC covers."""
    return json.dumps(body, separators=(",", ":"), ensure_ascii=False)


# ---------------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------------

STATUS = {
    "live": OrderStatus.OPEN,
    "unmatched": OrderStatus.OPEN,
    "delayed": OrderStatus.PENDING,
    "matched": OrderStatus.CLOSED,
    "canceled": OrderStatus.CANCELED,
    "cancelled": OrderStatus.CANCELED,
    "canceled_market_resolved": OrderStatus.CANCELED,
    "invalid": OrderStatus.REJECTED,
}

TIF = {"GTC": TimeInForce.GTC, "GTD": TimeInForce.GTD, "FAK": TimeInForce.IOC, "FOK": TimeInForce.FOK}

SETTLEMENT = {
    "MATCHED": SettlementState.MATCHED,
    "MINED": SettlementState.MATCHED,
    "RETRYING": SettlementState.MATCHED,
    "CONFIRMED": SettlementState.CONFIRMED,
    "FAILED": SettlementState.FAILED,
}


def _seconds_to_ms(value: Any) -> int | None:
    if value in (None, "", 0, "0"):
        return None
    try:
        return int(Decimal(str(value)) * 1000)
    except Exception:
        return parse_ts(value)


def order_of(
    raw: dict[str, Any], *, market_id: str, outcome: str, account: Account | None = None,
) -> Order:
    """A CLOB order row (`GET /data/order`, `/data/orders`, the user stream),
    on the YES leg. `outcome` says which token `raw["asset_id"]` is."""
    amount = D(raw.get("original_size") or "0")
    filled = D(raw.get("size_matched") or "0")
    native = str(raw.get("status") or "").lower()
    status = STATUS.get(native, OrderStatus.OPEN)
    if status == OrderStatus.CLOSED and filled < amount:
        # `matched` on a partly filled order still resting is not the end of it.
        status = OrderStatus.OPEN
    expiration = int(raw.get("expiration") or 0)
    side, price = to_yes_leg(outcome, str(raw.get("side")), D(raw["price"]) if raw.get("price") not in (None, "") else None)
    return Order(
        id=str(raw.get("id") or ""),
        venue=VENUE,
        account=account,
        market_id=market_id,
        side=side,
        type=OrderType.LIMIT,
        time_in_force=TIF.get(str(raw.get("order_type") or "GTC").upper(), TimeInForce.GTC),
        status=status,
        price=price,
        amount=amount,
        filled=filled,
        remaining=max(Decimal("0"), amount - filled) if status == OrderStatus.OPEN else Decimal("0"),
        expires_at=(expiration - GTD_SAFETY_S) * 1000 if expiration else None,
        created_at=_seconds_to_ms(raw.get("created_at")),
        updated_at=_seconds_to_ms(raw.get("timestamp")) if raw.get("timestamp") else None,
        info=raw,
    )


def order_from_response(
    raw: dict[str, Any], *, entry: dict[str, Any], order_id: str, request: OrderRequest,
    info: TokenInfo, account: Account,
) -> Order:
    """The `POST /order` answer, joined with the order that produced it.

    The answer carries a status word and nothing about how much filled in
    shares, so a `live` order is reported open with nothing filled and a
    `matched` or `delayed` one is left for the caller to read back.
    """
    order = entry["order"]
    shares_raw = int(order["takerAmount"] if order["side"] == "BUY" else order["makerAmount"])
    native = str(raw.get("status") or "").lower()
    status = STATUS.get(native, OrderStatus.PENDING)
    if native == "matched":
        status = OrderStatus.PENDING  # how much matched is read back, not guessed
    expiration = int(order["expiration"])
    return Order(
        id=str(raw.get("orderID") or order_id),
        client_order_id=request.client_order_id,
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, ids.native(VENUE, request.market_id)),
        side=request.side,
        type=request.type,
        time_in_force=TIF[entry["orderType"]],
        status=status,
        price=D(request.price) if request.price is not None else None,
        amount=Decimal(shares_raw) / sig.SCALE,
        filled=Decimal("0"),
        post_only=bool(entry.get("postOnly")),
        expires_at=(expiration - GTD_SAFETY_S) * 1000 if expiration else None,
        created_at=int(order["timestamp"]),
        book=request.book,
        trader=request.trader,
        tags=request.tags,
        info={"response": raw, "request": entry, "computed_id": order_id},
    )


def fills_of(
    raw: dict[str, Any], *, api_key: str, wallets: set[str], market_id: str, outcome_of: Any,
    account: Account | None = None,
) -> list[Fill]:
    """One CLOB trade as this account's fills, on the YES leg. `outcome_of`
    maps a token id to `"yes"` / `"no"`; a trade can match a YES buy against
    a NO buy, so each leg is looked up on its own.

    As the taker, the trade row is the fill. As a maker, each of this
    account's orders in `maker_orders` is its own fill, at that order's
    price and size. The row's `status` is the settlement state of all of
    them.
    """
    status = str(raw.get("status") or "").upper().removeprefix("TRADE_STATUS_")
    settlement = SETTLEMENT.get(status, SettlementState.MATCHED)
    stamp = _seconds_to_ms(raw.get("match_time")) or 0
    trade_id = str(raw.get("id") or "")
    lowered = {w.lower() for w in wallets}
    fills: list[Fill] = []
    if str(raw.get("trader_side") or "").upper() == "TAKER":
        side, price = to_yes_leg(outcome_of(str(raw.get("asset_id") or "")), str(raw.get("side")), D(raw["price"]))
        fills.append(Fill(
            id=trade_id, order_id=str(raw.get("taker_order_id") or ""), venue=VENUE, account=account,
            market_id=market_id,
            side=side,
            price=price, amount=D(raw.get("size") or "0"),
            fee=None, fee_currency="pUSD", liquidity=Liquidity.TAKER,
            settlement=settlement, timestamp=stamp, info=raw,
        ))
        return fills
    for leg in raw.get("maker_orders") or []:
        mine = leg.get("owner") == api_key or str(leg.get("maker_address") or "").lower() in lowered
        if not mine:
            continue
        side, price = to_yes_leg(
            outcome_of(str(leg.get("asset_id") or raw.get("asset_id") or "")), str(leg.get("side")), D(leg["price"]),
        )
        fills.append(Fill(
            id=f"{trade_id}:{leg.get('order_id')}", order_id=str(leg.get("order_id") or ""), venue=VENUE,
            account=account, market_id=market_id,
            side=side,
            price=price, amount=D(leg.get("matched_amount") or "0"),
            fee=Decimal("0"), fee_currency="pUSD", liquidity=Liquidity.MAKER,
            settlement=settlement, timestamp=stamp, info={"trade": raw, "maker_order": leg},
        ))
    return fills


def position_of(
    rows: list[dict[str, Any]], *, market_id: str, outcome_of: Any, account: Account | None = None,
) -> Position:
    """A market's position from its Data API v2 rows, one per token held:
    the two inventories netted on the YES leg, and both kept."""
    yes_rows = [r for r in rows if outcome_of(str(r.get("token_id") or "")) == "yes"]
    no_rows = [r for r in rows if outcome_of(str(r.get("token_id") or "")) == "no"]
    held_yes = sum((D(r.get("current_size") or "0") for r in yes_rows), Decimal("0"))
    held_no = sum((D(r.get("current_size") or "0") for r in no_rows), Decimal("0"))
    net = held_yes - held_no
    # The side with inventory names the row the figures come from.
    raw = (yes_rows if net >= 0 and yes_rows else no_rows or yes_rows)[0]
    outcome = "yes" if raw in yes_rows else "no"
    status = str(raw.get("status") or "OPEN").upper()
    redeemable = bool(raw.get("redeemable"))
    value = raw.get("current_value")

    def yes_leg(value: Any) -> Decimal | None:
        if value in (None, 0, 0.0, ""):
            return None
        return D(value) if outcome == "yes" else Decimal("1") - D(value)

    return Position(
        venue=VENUE,
        account=account,
        market_id=market_id,
        side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
        contracts=abs(net),
        inventory_yes=held_yes,
        inventory_no=held_no,
        entry_price=yes_leg(raw.get("avg_price")),
        mark_price=yes_leg(raw.get("current_price")),
        unrealized_pnl=sum((D(r["unrealized_pnl"]) for r in rows if r.get("unrealized_pnl") is not None), Decimal("0"))
        if any(r.get("unrealized_pnl") is not None for r in rows) else None,
        realized_pnl=sum((D(r["realized_pnl"]) for r in rows if r.get("realized_pnl") is not None), Decimal("0"))
        if any(r.get("realized_pnl") is not None for r in rows) else None,
        resolved=redeemable or status == "REDEEMABLE",
        final=False,
        redeemable=D(value) if redeemable and value is not None else None,
        timestamp=max((int(r["last_event_at"]) * 1000 for r in rows if r.get("last_event_at")), default=None),
        info={"rows": rows},
    )


def error_of(message: str, *, body: Any = None, status: int | None = None) -> ExchangeError:
    """The venue's error text as a typed error. The venue sends text, not
    codes, for most refusals; the phrases matched here are the documented
    ones."""
    text = message.lower()
    if "not enough balance" in text or "allowance" in text:
        return InsufficientFunds(message, body=body, status=status)
    if "tick size" in text or "lower than the minimum" in text or "invalid expiration" in text:
        return InvalidOrder(message)
    if "cancel-only" in text or "post-only mode" in text or "trading is currently disabled" in text \
            or "not yet ready" in text or "closed only mode" in text:
        return MarketHalted(message, body=body, status=status)
    if "not found" in text or "does not exist" in text:
        return OrderNotFound(message, body=body, status=status)
    reason = (
        "post_only_would_cross" if "crosses book" in text or "crosses the book" in text
        else "fok_not_filled" if "fully filled or killed" in text
        else "fak_no_match" if "no orders found to match" in text
        else "duplicate" if "duplicated" in text
        else None
    )
    return OrderRejected(message, reason=reason, info=body if isinstance(body, dict) else {}, body=body, status=status)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class PolymarketTrading(TradingExchange):
    """Polymarket CLOB V2 order entry.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.polymarket import PolymarketTrading

    creds = require("polymarket", load_credentials())
    async with PolymarketTrading(creds) as poly:
        await poly.ensure_api_credentials()
        print(await poly.fetch_balance())
    ```
    """

    id = VENUE
    name = "Polymarket"
    has: dict[str, Capability] = {
        "create_order": True,
        "create_orders": True,
        "cancel_order": True,
        "cancel_orders": True,
        "cancel_all_orders": True,
        # No amend on the CLOB: a change is a cancel and a new signed order,
        # which is the engine's to do.
        "edit_order": False,
        "fetch_order": True,
        "fetch_open_orders": True,
        # The order endpoint lists live orders only; closed ones are reached
        # by id or through their trades.
        "fetch_orders": False,
        "fetch_my_trades": True,
        "fetch_positions": True,
        "fetch_balance": True,
        "fetch_settlements": True,
        "fetch_queue_position": False,
        "fetch_fee_estimate": True,
        "rfq": False,
        # Deposit Wallets through the relayer and EOAs on-chain; the legacy
        # proxy and Safe wallet payloads are not built.
        "split_merge": "partial",
        "watch_orders": False,
        "watch_my_trades": False,
        "watch_positions": False,
        "watch_balance": False,
    }

    def __init__(
        self,
        credentials: PolymarketCredentials,
        *,
        account_name: str = "default",
        clob_url: str = CLOB_URL,
        data_url: str = DATA_URL,
        relayer_url: str = RELAYER_URL,
        gamma_url: str = GAMMA_URL,
        limiter: BudgetLimiter | None = None,
        timeout: float = 30.0,
        client: Any = None,
        builder_signer_url: str | None = None,
    ):
        import httpx
        import os

        self.credentials = credentials
        configured = builder_signer_url or os.environ.get("SYNPATH_BUILDER_SIGNER_URL") or BUILDER_SIGNER_URL
        self.builder_signer_url = None if configured.strip().lower() == "none" else configured
        self.signer = sig.WalletSigner(credentials.private_key)
        if credentials.signature_type != sig.EOA and not credentials.funder:
            raise CredentialsMissing("polymarket: signature types 1-3 spend from a smart wallet; the funder address is required")
        self.wallet = credentials.funder if credentials.signature_type != sig.EOA and credentials.funder else self.signer.address
        self.account = Account(venue=VENUE, name=account_name)
        # The venue's order limits are 200 a second sustained; reads share
        # the same generous budget. Cancels take the fast lane.
        self.limiter = limiter or BudgetLimiter(read_per_second=50, write_per_second=50)
        shared = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        self.clob = AsyncHttpClient(clob_url, limiter=None, client=shared, venue=VENUE)
        self.data = AsyncHttpClient(data_url, limiter=None, client=shared, venue=VENUE)
        self.relayer = AsyncHttpClient(relayer_url, limiter=None, client=shared, venue=VENUE)
        self.gamma = AsyncHttpClient(gamma_url, limiter=None, client=shared, venue=VENUE)
        self._markets: dict[str, MarketTokens] = {}
        """By Gamma id, condition id and either token id: which token is which side."""
        self._http = shared
        self._api_key = credentials.api_key
        self._api_secret = credentials.api_secret
        self._api_passphrase = credentials.api_passphrase
        self._tokens: dict[str, TokenInfo] = {}
        self._heartbeat_task: asyncio.Task | None = None
        self._heartbeat_id = ""
        self.heartbeat_error: BaseException | None = None
        self.clock_offset_s = 0
        self._default_builder_refused = False
        """Set once the venue refuses Synpath's default builder code; orders
        are then signed without it for the rest of this session."""

    # -- auth -----------------------------------------------------------------

    def _now_s(self) -> int:
        return int(time.time()) + self.clock_offset_s

    async def sync_clock(self) -> int:
        """Adopt the venue's clock for signed timestamps; returns the offset in seconds."""
        server = await self.clob.get("/time")
        stamp = int(server.get("time") or server.get("timestamp")) if isinstance(server, dict) else int(server)
        self.clock_offset_s = stamp - int(time.time())
        return self.clock_offset_s

    async def ensure_api_credentials(self, *, nonce: int = 0) -> str:
        """The CLOB API key, deriving it from the wallet key when none was given.

        Derivation is deterministic for a signer and nonce, so it returns the
        same credentials every time; only when none exist yet is one created.
        The secret stays in memory and is registered with the log filter.
        """
        if self._api_key and self._api_secret and self._api_passphrase:
            return self._api_key
        from .credentials import SecretFilter

        headers = sig.l1_headers(self.signer, self._now_s(), nonce)
        try:
            raw = await self.clob.get("/auth/derive-api-key", headers=headers)
        except (BadRequest, MarketNotFound, AuthenticationError):
            raw = None
        if not raw or not raw.get("apiKey"):
            headers = sig.l1_headers(self.signer, self._now_s(), nonce)
            raw = await self.clob.post("/auth/api-key", headers=headers)
        self._api_key, self._api_secret, self._api_passphrase = raw["apiKey"], raw["secret"], raw["passphrase"]
        SecretFilter.install([self._api_secret, self._api_passphrase])
        return self._api_key

    async def _private(
        self, method: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None,
        kind: str = "read", cost: float = 1, priority: Priority = Priority.NORMAL,
    ) -> Any:
        """One L2-authenticated CLOB call: paced, signed over the exact body, errors typed."""
        await self.ensure_api_credentials()
        await self.limiter.acquire(cost=cost, kind=kind, priority=priority)  # type: ignore[arg-type]
        text = serialize(body) if body is not None else None
        headers = sig.l2_headers(
            address=self.signer.address, api_key=self._api_key or "", secret=self._api_secret or "",
            passphrase=self._api_passphrase or "", timestamp=self._now_s(), method=method, path=path, body=text,
        )
        if text is not None:
            headers["Content-Type"] = "application/json"
        try:
            return await self.clob.request(
                method, path, params={k: v for k, v in (params or {}).items() if v is not None} or None,
                content=text, headers=headers,
            )
        except ExchangeNotAvailable as exc:
            detail = _error_text(exc.body)
            if detail:
                raise error_of(detail, body=exc.body, status=exc.status) from None
            raise
        except (AuthenticationError, MarketNotFound) as exc:
            if isinstance(exc, MarketNotFound) and "/order" in path:
                raise OrderNotFound(str(exc), body=exc.body, status=exc.status) from None
            raise
        except ExchangeError as exc:
            detail = _error_text(exc.body) or str(exc)
            raise error_of(detail, body=exc.body, status=exc.status) from None

    # -- market facts ---------------------------------------------------------

    async def token_info(self, token_id: str, *, refresh: bool = False) -> TokenInfo:
        """Tick, minimum size, neg-risk flag and condition id for a token,
        read from its book and cached briefly."""
        check_token_id(token_id)
        cached = self._tokens.get(token_id)
        if cached and not refresh and time.monotonic() - cached.read_at < MARKET_TTL_S:
            return cached
        await self.limiter.acquire(cost=1, kind="read")
        try:
            book = await self.clob.get("/book", {"token_id": token_id})
        except MarketNotFound:
            raise InvalidOrder(f"polymarket: no order book for token {token_id}; the market is closed or unknown") from None
        info = TokenInfo.from_book(token_id, book or {})
        self._tokens[token_id] = info
        return info

    def remember_token(self, info: TokenInfo) -> None:
        """Seed the cache from elsewhere (the read API, a market stream), saving the lookup."""
        self._tokens[info.token_id] = info

    def remember_market(self, tokens: MarketTokens) -> MarketTokens:
        """Seed which token is which side, from a `Market` the read API returned
        (`MarketTokens(market.venue_market_id, market.info["conditionId"],
        market.yes.venue_token_id, market.no.venue_token_id)`) or a catalog row."""
        for key in (tokens.gamma_id, tokens.condition_id, tokens.yes_token, tokens.no_token):
            if key:
                self._markets[key] = tokens
        return tokens

    async def market_tokens(self, market_id: str) -> MarketTokens:
        """Which token is which side of a market, by Synpath id, Gamma id or
        condition id. One Gamma read the first time, cached after."""
        native = ids.native(VENUE, market_id)
        cached = self._markets.get(native)
        if cached:
            return cached
        await self.limiter.acquire(cost=1, kind="read")
        if native.startswith("0x"):
            rows = await self.gamma.get("/markets", {"condition_ids": native})
            raw = rows[0] if isinstance(rows, list) and rows else None
        else:
            raw = await self.gamma.get(f"/markets/{native}")
        if not isinstance(raw, dict) or not raw.get("id"):
            raise MarketNotFound(f"polymarket: no market {native}")
        return self.remember_market(MarketTokens.from_gamma(raw))

    async def locate_token(self, token_id: str) -> MarketTokens:
        """The market behind a token id the venue reported."""
        cached = self._markets.get(token_id)
        if cached:
            return cached
        await self.limiter.acquire(cost=1, kind="read")
        rows = await self.gamma.get("/markets", {"clob_token_ids": token_id})
        raw = rows[0] if isinstance(rows, list) and rows else None
        if not isinstance(raw, dict) or not raw.get("id"):
            raise MarketNotFound(f"polymarket: no market behind token {token_id}")
        return self.remember_market(MarketTokens.from_gamma(raw))

    async def _outcome_of(self, token_id: str) -> str:
        return (await self.locate_token(token_id)).outcome_of(token_id)

    async def _order_of(self, raw: dict[str, Any]) -> Order:
        token = str(raw.get("asset_id") or "")
        tokens = await self.locate_token(token)
        return order_of(raw, market_id=tokens.market_id, outcome=tokens.outcome_of(token), account=self.account)

    # -- orders ---------------------------------------------------------------

    async def create_order(self, request: OrderRequest, *, token: TokenInfo | None = None) -> Order:
        """Sign and place one order.

        `live` comes back open; `matched` and `delayed` come back pending,
        because the answer does not say how much matched -- read the order
        back, or follow its trades.
        """
        if token is None:
            tokens = await self.market_tokens(request.market_id)
            token = await self.token_info(tokens.token(wire_leg(request)[0]))
        info = token
        api_key = await self.ensure_api_credentials()
        try:
            return await self._place(request, info, api_key)
        except ExchangeError as exc:
            if not self._refuses_default_builder(request, exc):
                raise
            self._drop_default_builder(exc)
            return await self._place(request, info, api_key)

    async def _place(self, request: OrderRequest, info: TokenInfo, api_key: str) -> Order:
        entry, order_id = build_signed_order(
            request, info, signer=self.signer, creds=self._order_credentials(), api_key=api_key,
            now_ms=self._now_s() * 1000,
        )
        try:
            raw = await self._private("POST", "/order", body=entry, kind="write", cost=1)
        except InvalidOrder as exc:
            if "tick" in str(exc).lower():
                self._tokens.pop(info.token_id, None)
            raise
        if not raw or not raw.get("success", True) or raw.get("errorMsg"):
            raise error_of(str((raw or {}).get("errorMsg") or "order refused"), body=raw)
        return order_from_response(raw, entry=entry, order_id=order_id, request=request, info=info, account=request.account or self.account)

    def _order_credentials(self) -> PolymarketCredentials:
        """The credentials orders are signed with: without Synpath's default
        builder code once the venue has refused it."""
        if self._default_builder_refused and self.credentials.builder_code == SYNPATH_BUILDER_CODE:
            return replace(self.credentials, builder_code=None)
        return self.credentials

    def _refuses_default_builder(self, request: OrderRequest, exc: Exception) -> bool:
        """Whether a refusal is the venue rejecting Synpath's default builder
        code (a code it has disabled is refused outright). A code the account
        or the order chose itself is left to fail: that was its choice."""
        return (
            not self._default_builder_refused
            and self.credentials.builder_code == SYNPATH_BUILDER_CODE
            and not request.params.get("builder_code")
            and "builder" in str(exc).lower()
        )

    def _drop_default_builder(self, exc: Exception) -> None:
        self._default_builder_refused = True
        log.warning("polymarket: the venue refused Synpath's builder code (%s); sending orders without it", exc)

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        """Many orders, fifteen to a request. Each is validated and signed
        before anything is sent; one refused locally or by the venue does
        not fail the rest."""
        api_key = await self.ensure_api_credentials()
        prepared: list[tuple[dict[str, Any], str, TokenInfo] | Exception] = []
        for request in requests:
            try:
                tokens = await self.market_tokens(request.market_id)
                info = await self.token_info(tokens.token(wire_leg(request)[0]))
                entry, order_id = build_signed_order(
                    request, info, signer=self.signer, creds=self._order_credentials(), api_key=api_key,
                    now_ms=self._now_s() * 1000,
                )
                prepared.append((entry, order_id, info))
            except InvalidOrder as exc:
                prepared.append(exc)
        results: list[Order | Exception] = [p if isinstance(p, Exception) else p for p in prepared]  # type: ignore[misc]
        sendable = [i for i, p in enumerate(prepared) if not isinstance(p, Exception)]
        retry: list[int] = []
        for start in range(0, len(sendable), ORDER_BATCH):
            chunk = sendable[start:start + ORDER_BATCH]
            body = [prepared[i][0] for i in chunk]  # type: ignore[index]
            raw = await self._private("POST", "/orders", body=body, kind="write", cost=len(chunk))
            answers = raw if isinstance(raw, list) else []
            for offset, index in enumerate(chunk):
                entry, order_id, info = prepared[index]  # type: ignore[misc]
                answer = answers[offset] if offset < len(answers) else {}
                if not answer.get("success", False) or answer.get("errorMsg"):
                    refusal = error_of(str(answer.get("errorMsg") or "order refused"), body=answer)
                    if self._refuses_default_builder(requests[index], refusal) or (
                        self._default_builder_refused and "builder" in str(refusal).lower()
                        and entry["order"]["builder"] == SYNPATH_BUILDER_CODE
                    ):
                        if not self._default_builder_refused:
                            self._drop_default_builder(refusal)
                        retry.append(index)
                    results[index] = refusal
                else:
                    results[index] = order_from_response(
                        answer, entry=entry, order_id=order_id, request=requests[index], info=info,
                        account=requests[index].account or self.account,
                    )
        # Refused over Synpath's default builder code: signed again without it and sent once more.
        for index, again in zip(retry, await self.create_orders([requests[i] for i in retry]) if retry else []):
            results[index] = again
        return results

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        """Cancel one order and read it back. An order that had already
        matched comes back as it is, with the venue's reason in `info`."""
        raw = await self._private("DELETE", "/order", body={"orderID": order_id}, kind="write", priority=Priority.HIGH)
        refused = ((raw or {}).get("not_canceled") or {}).get(order_id)
        order = await self.fetch_order(order_id)
        if refused and not order.is_terminal:
            raise OrderRejected(f"polymarket: cancel of {order_id} refused: {refused}", reason="cancel_refused", info=raw or {})
        if order_id in ((raw or {}).get("canceled") or []) and order.status == OrderStatus.OPEN:
            order = order.model_copy(update={"status": OrderStatus.CANCELED, "remaining": Decimal("0")})
        return order.model_copy(update={"info": {**order.info, "cancel": raw}})

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        """Cancel many by id, a thousand to a request. Each result is the
        venue's verdict on that id and the orders are not read back, so a
        cancelled one carries its id and status only: read it with
        `fetch_order` for what filled first."""
        results: dict[str, Order | Exception] = {}
        for start in range(0, len(order_ids), CANCEL_BATCH):
            chunk = order_ids[start:start + CANCEL_BATCH]
            raw = await self._private("DELETE", "/orders", body=chunk, kind="write", priority=Priority.HIGH)
            canceled = set((raw or {}).get("canceled") or [])
            refused = (raw or {}).get("not_canceled") or {}
            for oid in chunk:
                if oid in canceled:
                    results[oid] = Order(
                        id=oid, venue=VENUE, account=self.account, market_id=market_id or "",
                        side=Side.BUY, type=OrderType.LIMIT, time_in_force=TimeInForce.GTC,
                        status=OrderStatus.CANCELED, amount=Decimal("0"), info={"cancel": "canceled"},
                    )
                else:
                    results[oid] = OrderRejected(
                        f"polymarket: cancel of {oid} refused: {refused.get(oid, 'not reported')}",
                        reason="cancel_refused", info={"reason": refused.get(oid)},
                    )
        return [results[oid] for oid in order_ids]

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int:
        """Every resting order in the account, or on one market. Returns how
        many the venue cancelled."""
        if market_id:
            body = {"market": (await self.market_tokens(market_id)).condition_id}
            raw = await self._private("DELETE", "/cancel-market-orders", body=body, kind="write", priority=Priority.HIGH)
        else:
            raw = await self._private("DELETE", "/cancel-all", kind="write", priority=Priority.HIGH)
        return len((raw or {}).get("canceled") or [])

    async def fetch_order(self, order_id: str) -> Order:
        raw = await self._private("GET", f"/data/order/{order_id}")
        if not raw or not isinstance(raw, dict) or not raw.get("id"):
            raise OrderNotFound(f"polymarket: no order {order_id}")
        return await self._order_of(raw)

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        orders: list[Order] = []
        cursor = INITIAL_CURSOR
        condition = (await self.market_tokens(market_id)).condition_id if market_id else None
        while cursor and cursor != END_CURSOR:
            raw = await self._private("GET", "/data/orders", params={
                "market": condition, "next_cursor": cursor,
            })
            for row in (raw or {}).get("data") or []:
                orders.append(await self._order_of(row))
            cursor = (raw or {}).get("next_cursor")
        return orders

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        """This account's fills, one page. `order_id` narrows the page to
        fills of that order; the venue itself filters by market."""
        condition = (await self.market_tokens(market_id)).condition_id if market_id else None
        raw = await self._private("GET", "/data/trades", params={
            "market": condition,
            "after": int(since / 1000) if since else None,
            "next_cursor": cursor or INITIAL_CURSOR,
        })
        wallets = {self.wallet, self.signer.address}
        fills: list[Fill] = []
        for row in (raw or {}).get("data") or []:
            tokens = await self.locate_token(str(row.get("asset_id") or ""))
            fills.extend(fills_of(
                row, api_key=self._api_key or "", wallets=wallets, market_id=tokens.market_id,
                outcome_of=tokens.outcome_of, account=self.account,
            ))
        if order_id:
            fills = [f for f in fills if f.order_id == order_id]
        nxt = (raw or {}).get("next_cursor")
        return Page(fills[:limit] if limit else fills, next_cursor=nxt if nxt and nxt != END_CURSOR else None)

    # -- account --------------------------------------------------------------

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        """The account wallet's pUSD, and in `info` the allowance each
        exchange has. The venue reports the wallet balance; open orders
        reserve part of it, which it does not report as a figure."""
        raw = await self._private("GET", "/balance-allowance", params={
            "asset_type": "COLLATERAL", "signature_type": self.credentials.signature_type,
        })
        balance = from_poly_raw_amount((raw or {}).get("balance") or 0)
        return Balance(
            venue=VENUE, account=account or self.account, currency="pUSD",
            total=balance, available=balance, locked=None, buying_power=None,
            timestamp=int(time.time() * 1000), info=raw or {},
        )

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        """Token inventories held by the account wallet, from the Data API.
        Resolved markets whose tokens are still held come back with
        `resolved` and, if they pay, `redeemable`."""
        rows: list[dict[str, Any]] = []
        cursor: str | None = None
        condition = (await self.market_tokens(market_id)).condition_id if market_id else None
        while True:
            await self.limiter.acquire(cost=1, kind="read")
            raw = await self.data.get("/v2/positions", {
                "user": self.wallet, "condition": condition,
                "event_id": ids.native(VENUE, event_id) if event_id else None,
                "limit": None if cursor else 500, "cursor": cursor,
            })
            rows.extend((raw or {}).get("data") or [])
            cursor = ((raw or {}).get("pagination") or {}).get("next_cursor")
            if not cursor:
                break
        by_market: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            tokens = await self.locate_token(str(row.get("token_id") or ""))
            by_market.setdefault(tokens.gamma_id, []).append(row)
        positions = [
            position_of(group, market_id=self._markets[gamma].market_id, outcome_of=self._markets[gamma].outcome_of, account=self.account)
            for gamma, group in by_market.items()
        ]
        return [p for p in positions if p.inventory_yes or p.inventory_no]

    async def fetch_settlements(
        self, *, market_id: str | None = None, since: int | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page:
        """Redemptions, one row per outcome redeemed (the Data API reports a
        two-sided redeem as two rows, the losing one paying 0)."""
        from .types import Settlement

        await self.limiter.acquire(cost=1, kind="read")
        raw = await self.data.get("/v2/activity", {
            "user": self.wallet, "type": "REDEEM",
            "condition": (await self.market_tokens(market_id)).condition_id if market_id else None,
            "start": int(since / 1000) if since else None, "limit": limit, "cursor": cursor,
        })
        rows = (raw or {}).get("data") or []
        settlements = []
        for row in rows:
            payout = D(row.get("usdc_size") or 0)
            token = str(row.get("token_id") or "")
            tokens = await self.locate_token(token) if token else None
            held = tokens.outcome_of(token) if tokens else None
            settlements.append(Settlement(
                venue=VENUE, account=self.account,
                market_id=tokens.market_id if tokens else ids.qualify(VENUE, str(row.get("condition_id") or "")),
                held=PositionSide.LONG if held == "yes" else PositionSide.SHORT if held == "no" else None,
                result=None, won=payout > 0,
                amount=D(row.get("size") or 0), cost=None, payout=payout, pnl=None,
                timestamp=int(row["timestamp"]) * 1000 if row.get("timestamp") else None, info=row,
            ))
        return Page(settlements, next_cursor=((raw or {}).get("pagination") or {}).get("next_cursor"))

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, amount: Decimal) -> FeeEstimate:
        """The market's taker fee at the YES `price` for `amount` shares, from
        the CLOB's own market parameters. Makers pay nothing. The fee is
        symmetric in the YES price, so `side` does not change it."""
        tokens = await self.market_tokens(market_id)
        await self.limiter.acquire(cost=1, kind="read")
        raw = await self.clob.get(f"/clob-markets/{tokens.condition_id}")
        fd = (raw or {}).get("fd") or {}
        rate = fd.get("r")
        schedule = FeeSchedule(
            venue=VENUE, scope="market", scope_id=tokens.condition_id, fee_type="quadratic_theta",
            taker_rate=float(rate) if rate is not None else 0.0, maker_rate=0.0,
            exponent=float(fd["e"]) if fd.get("e") is not None else None, info={"fd": fd},
        )
        taker = schedule.estimate(float(price), float(amount), taker=True)
        return FeeEstimate(
            venue=VENUE, market_id=tokens.market_id, side=side, price=D(price), amount=D(amount),
            taker_fee=D(str(taker)) if taker is not None else None, maker_fee=Decimal("0"),
            currency="pUSD", info={"schedule": schedule.model_dump()},
        )

    async def closed_only(self) -> bool:
        """Whether the account may only reduce positions right now."""
        raw = await self._private("GET", "/auth/ban-status/closed-only")
        return bool((raw or {}).get("closed_only"))

    # -- heartbeat ------------------------------------------------------------

    async def send_heartbeat(self) -> str:
        """One heartbeat. Once the first is accepted the venue cancels every
        order on these API credentials if ten seconds pass without another."""
        try:
            raw = await self._private(
                "POST", "/v1/heartbeats", body={"heartbeat_id": self._heartbeat_id},
                kind="write", priority=Priority.HIGH,
            )
        except ExchangeError as exc:
            expected = exc.body.get("heartbeat_id") if isinstance(exc.body, dict) else None
            if not expected:
                raise
            self._heartbeat_id = expected
            raw = await self._private(
                "POST", "/v1/heartbeats", body={"heartbeat_id": self._heartbeat_id},
                kind="write", priority=Priority.HIGH,
            )
        self._heartbeat_id = str((raw or {}).get("heartbeat_id") or "")
        return self._heartbeat_id

    def start_heartbeat(self, interval_s: float = 5.0) -> asyncio.Task:
        """Keep the dead man's switch fed from a task on this loop.

        If the process stalls or dies, the venue cancels its resting orders
        within fifteen seconds. A heartbeat that fails is recorded in
        `heartbeat_error` and retried on the next beat; it is not swallowed
        silently, and it is not allowed to kill the loop.
        """
        if self._heartbeat_task and not self._heartbeat_task.done():
            return self._heartbeat_task

        async def beat() -> None:
            while True:
                try:
                    await self.send_heartbeat()
                    self.heartbeat_error = None
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # recorded, surfaced, retried
                    self.heartbeat_error = exc
                await asyncio.sleep(interval_s)

        self._heartbeat_task = asyncio.get_running_loop().create_task(beat())
        return self._heartbeat_task

    async def stop_heartbeat(self) -> None:
        task, self._heartbeat_task = self._heartbeat_task, None
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    # -- wallet operations ----------------------------------------------------

    async def fetch_allowances(self, *, token_id: str | None = None) -> dict[str, Decimal]:
        """Spender -> allowance, as the CLOB's cache sees it: pUSD by default,
        a token's operators with `token_id`."""
        raw = await self._private("GET", "/balance-allowance", params={
            "asset_type": "CONDITIONAL" if token_id else "COLLATERAL", "token_id": token_id,
            "signature_type": self.credentials.signature_type,
        })
        return {spender: from_poly_raw_amount(value) for spender, value in ((raw or {}).get("allowances") or {}).items()}

    async def check_approvals(self) -> dict[str, bool]:
        """Whether each exchange may spend the wallet's pUSD. Missing ones
        are what `approve_trading` fixes."""
        allowances = {k.lower(): v for k, v in (await self.fetch_allowances()).items()}
        return {spender: allowances.get(spender.lower(), Decimal("0")) > 0 for spender in (sig.EXCHANGE, sig.NEG_RISK_EXCHANGE)}

    async def sync_allowances(self, *, token_id: str | None = None) -> None:
        """Tell the CLOB to re-read on-chain balances and allowances, after an
        approval or a split changed them."""
        await self._private("GET", "/balance-allowance/update", params={
            "asset_type": "CONDITIONAL" if token_id else "COLLATERAL", "token_id": token_id,
            "signature_type": self.credentials.signature_type,
        })

    async def approve_trading(self, *, include_adapters: bool = True) -> dict[str, Any]:
        """Grant every approval trading needs, in one wallet transaction.

        pUSD and the conditional tokens for both exchanges, and with
        `include_adapters` for both collateral adapters that split, merge and
        redeem use. Idempotent: an approval already granted is granted again.
        """
        spenders = [sig.EXCHANGE, sig.NEG_RISK_EXCHANGE]
        if include_adapters:
            spenders += [sig.CTF_COLLATERAL_ADAPTER, sig.NEG_RISK_CTF_COLLATERAL_ADAPTER]
        calls = sig.trading_approval_calls(spenders)
        result = await self.execute_wallet_calls(calls, description="Set trading approvals")
        await self.sync_allowances()
        return result

    async def split(self, market_id: str, amount: Decimal, *, neg_risk: bool | None = None) -> dict[str, Any]:
        """pUSD into a full set: `amount` YES and `amount` NO tokens."""
        tokens = await self.market_tokens(market_id)
        neg_risk = tokens.neg_risk if neg_risk is None else neg_risk
        call = {"target": sig.collateral_adapter_for(neg_risk), "value": "0",
                "data": sig.split_calldata(tokens.condition_id, sig.to_raw(D(amount)))}
        return await self.execute_wallet_calls([call], description="Split position")

    async def merge(self, market_id: str, amount: Decimal, *, neg_risk: bool | None = None) -> dict[str, Any]:
        """A full set back into pUSD: `amount` of each outcome."""
        tokens = await self.market_tokens(market_id)
        neg_risk = tokens.neg_risk if neg_risk is None else neg_risk
        call = {"target": sig.collateral_adapter_for(neg_risk), "value": "0",
                "data": sig.merge_calldata(tokens.condition_id, sig.to_raw(D(amount)))}
        return await self.execute_wallet_calls([call], description="Merge positions")

    async def redeem(self, market_id: str, *, neg_risk: bool | None = None) -> dict[str, Any]:
        """Every token of a resolved market into pUSD; losing tokens pay 0."""
        tokens = await self.market_tokens(market_id)
        neg_risk = tokens.neg_risk if neg_risk is None else neg_risk
        call = {"target": sig.collateral_adapter_for(neg_risk), "value": "0", "data": sig.redeem_calldata(tokens.condition_id)}
        return await self.execute_wallet_calls([call], description="Redeem positions")

    async def execute_wallet_calls(
        self, calls: list[dict[str, str]], *, description: str, deadline_s: int = 600,
        wait: bool = True, timeout_s: float = 120.0, poll_interval_s: float = 1.0,
    ) -> dict[str, Any]:
        """Run contract calls from the account wallet.

        A Deposit Wallet signs one `Batch` and the relayer submits it
        gaslessly (a Relayer API key is required); an EOA sends each call as
        its own Polygon transaction (an RPC endpoint and POL for gas are
        required). Legacy proxy and Safe wallets are refused: their relayer
        payloads differ and are not built here.
        """
        kind = self.credentials.signature_type
        if kind == sig.DEPOSIT_WALLET:
            return await self._relay_batch(
                calls, description=description, deadline_s=deadline_s, wait=wait, timeout_s=timeout_s, poll_s=poll_interval_s,
            )
        if kind == sig.EOA:
            return await self._send_transactions(calls, wait=wait, timeout_s=timeout_s, poll_s=poll_interval_s)
        raise NotSupported(
            "polymarket: wallet transactions for legacy proxy (1) and Safe (2) wallets are not built; "
            "use polymarket.com or the venue SDK for this wallet"
        )

    def _own_relayer_headers(self) -> dict[str, str] | None:
        """The account's own Relayer API key, when it has one."""
        creds = self.credentials
        if not creds.relayer_api_key or not creds.relayer_api_key_address:
            return None
        return {"RELAYER_API_KEY": creds.relayer_api_key, "RELAYER_API_KEY_ADDRESS": creds.relayer_api_key_address}

    async def _builder_headers(self, method: str, path: str, body: str = "") -> tuple[dict[str, str] | None, str]:
        """Builder headers for one relayer request from Synpath's signer, and
        why not when it gives none (off, unreachable, out of quota)."""
        if not self.builder_signer_url:
            return None, "Synpath's builder signer is turned off"
        try:
            response = await self._http.post(
                self.builder_signer_url, json={"method": method, "path": path, "body": body}, timeout=10.0,
            )
        except Exception as exc:  # noqa: BLE001 -- any failure here means "use the account's own key"
            return None, f"Synpath's builder signer is unreachable ({type(exc).__name__})"
        if response.status_code != 200:
            try:
                reason = str(response.json().get("detail") or response.status_code)
            except ValueError:
                reason = str(response.status_code)
            return None, f"Synpath's builder signer declined: {reason}"
        headers = (response.json() or {}).get("headers")
        if not isinstance(headers, dict) or not headers.get("POLY_BUILDER_SIGNATURE"):
            return None, "Synpath's builder signer answered without headers"
        return {str(k): str(v) for k, v in headers.items()}, ""

    def _no_relayer_access(self, reason: str) -> NotSupported:
        return NotSupported(
            f"polymarket: gasless wallet transactions go through Polymarket's relayer. {reason}, and this account "
            f"has no Relayer API key of its own: set POLYMARKET_RELAYER_API_KEY and POLYMARKET_RELAYER_API_KEY_ADDRESS "
            f"(polymarket.com -> Settings -> API Keys -> Relayer API Keys)"
        )

    async def _relay_batch(
        self, calls: list[dict[str, str]], *, description: str, deadline_s: int, wait: bool, timeout_s: float,
        poll_s: float,
    ) -> dict[str, Any]:
        own = self._own_relayer_headers()
        lookup = own
        if lookup is None:
            lookup, _ = await self._builder_headers("GET", "/v1/account/transactions/params")
        await self.limiter.acquire(cost=1, kind="read")
        params = await self.relayer.get(
            "/v1/account/transactions/params", {"address": self.signer.address, "type": "WALLET"}, headers=lookup or {},
        )
        nonce = int((params or {}).get("nonce") or 0)
        deadline = self._now_s() + deadline_s
        signature = self.signer.sign_typed_data(sig.wallet_batch_typed_data(self.wallet, nonce, deadline, calls))
        body = {
            "type": "WALLET",
            "from": self.signer.address,
            "to": sig.DEPOSIT_WALLET_FACTORY,
            "nonce": str(nonce),
            "signature": signature,
            "metadata": description,
            "depositWalletParams": {"depositWallet": self.wallet, "deadline": str(deadline), "calls": calls},
        }
        text = serialize(body)
        # Synpath's builder credentials first, so the transaction is attributed
        # to Synpath and needs no key of the account's own; the account's own
        # Relayer API key when the signer gives none or the relayer refuses it.
        headers, reason = await self._builder_headers("POST", "/submit", text)
        if headers is None and own is None:
            raise self._no_relayer_access(reason)
        await self.limiter.acquire(cost=1, kind="write")
        try:
            submitted = await self._submit(text, headers or own or {})
        except (RateLimitExceeded, AuthenticationError) as exc:
            if headers is None or own is None:
                if headers is not None:
                    raise self._no_relayer_access(f"Polymarket's relayer refused Synpath's builder access ({exc})") from None
                raise
            log.warning("polymarket: the relayer refused Synpath's builder access (%s); using the account's own key", exc)
            submitted = await self._submit(text, own)
        transaction_id = str((submitted or {}).get("transactionID") or "")
        if not wait:
            return {"transaction_id": transaction_id, "state": (submitted or {}).get("state")}
        return await self._await_relayed(transaction_id, headers=own or {}, timeout_s=timeout_s, poll_s=poll_s)

    async def _submit(self, text: str, headers: dict[str, str]) -> Any:
        """`POST /submit` with the exact text the headers were signed over."""
        return await self.relayer.request(
            "POST", "/submit", content=text, headers={**headers, "Content-Type": "application/json"},
        )

    async def _await_relayed(
        self, transaction_id: str, *, headers: dict[str, str], timeout_s: float, poll_s: float,
    ) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        stop = loop.time() + timeout_s
        delay = poll_s
        while True:
            await self.limiter.acquire(cost=1, kind="read")
            raw = await self.relayer.get(f"/v1/account/transactions/{transaction_id}", headers=headers)
            state = str((raw or {}).get("state") or "")
            if state == "STATE_CONFIRMED":
                return raw
            if state in ("STATE_FAILED", "STATE_INVALID"):
                raise OrderRejected(
                    f"polymarket: wallet transaction {transaction_id} {state}: {(raw or {}).get('error_msg')}",
                    reason=state.lower(), info=raw or {},
                )
            if loop.time() >= stop:
                raise ExchangeNotAvailable(
                    f"polymarket: wallet transaction {transaction_id} still {state or 'unknown'} after {timeout_s}s; "
                    f"it may yet confirm", body=raw,
                )
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 5.0)

    async def _rpc(self, method: str, params: list[Any]) -> Any:
        if not self.credentials.rpc_url:
            raise NotSupported("polymarket: an EOA's transactions need a Polygon RPC endpoint; set POLYMARKET_RPC_URL")
        raw = await self.clob.request(
            "POST", self.credentials.rpc_url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        )
        if isinstance(raw, dict) and raw.get("error"):
            raise ExchangeError(f"polygon rpc {method}: {raw['error']}", body=raw)
        return (raw or {}).get("result")

    async def _send_transactions(
        self, calls: list[dict[str, str]], *, wait: bool, timeout_s: float, poll_s: float,
    ) -> dict[str, Any]:
        address = self.signer.address
        nonce = int(await self._rpc("eth_getTransactionCount", [address, "pending"]), 16)
        gas_price = int(await self._rpc("eth_gasPrice", []), 16)
        priority_fee = int(await self._rpc("eth_maxPriorityFeePerGas", []), 16)
        hashes = []
        for offset, call in enumerate(calls):
            estimate = int(await self._rpc("eth_estimateGas", [{"from": address, "to": call["target"], "data": call["data"]}]), 16)
            tx = {
                "chainId": sig.CHAIN_ID, "nonce": nonce + offset, "to": call["target"], "value": int(call["value"]),
                "data": call["data"], "gas": int(estimate * 1.2),
                "maxFeePerGas": gas_price * 2 + priority_fee, "maxPriorityFeePerGas": priority_fee, "type": 2,
            }
            hashes.append(await self._rpc("eth_sendRawTransaction", [self.signer.sign_transaction(tx)]))
        if not wait:
            return {"transaction_hashes": hashes}
        loop = asyncio.get_running_loop()
        stop = loop.time() + timeout_s
        receipts = []
        for tx_hash in hashes:
            while True:
                receipt = await self._rpc("eth_getTransactionReceipt", [tx_hash])
                if receipt:
                    if int(receipt.get("status", "0x0"), 16) != 1:
                        raise OrderRejected(f"polymarket: transaction {tx_hash} reverted", reason="reverted", info=receipt)
                    receipts.append(receipt)
                    break
                if loop.time() >= stop:
                    raise ExchangeNotAvailable(f"polymarket: transaction {tx_hash} unmined after {timeout_s}s")
                await asyncio.sleep(poll_s)
        return {"transaction_hashes": hashes, "receipts": receipts}

    async def close(self) -> None:
        await self.stop_heartbeat()
        await self._http.aclose()


def _error_text(body: Any) -> str | None:
    if isinstance(body, dict):
        for key in ("error", "errorMsg", "error_msg", "message"):
            if body.get(key):
                return str(body[key])
    return None
