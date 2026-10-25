"""Myriad order entry on its order book, and the venue's order and fill records on the YES leg.

Built to Myriad's API reference: order hashing and signatures are checked
against an independent EIP-712 encoder, request signing against the
reference's HMAC recipe.

**Two credentials.** Every order and every cancel is an EIP-712 message the
wallet signs. Requests to the account's endpoints prove whose they are with
an API key and its secret (HMAC over the timestamp, method, path and body),
or, without a key, with a session token the wallet signs in for (SIWE; the
first sign-in creates the Myriad account and ties it to the wallet). The
venue refuses a bare key. Orders must be the connected wallet's own.

**YES is outcome 0, NO outcome 1.** `buy` buys outcome 0 at the price given;
`sell` buys outcome 1 at `1 - price`; with `reduce_only`, `sell` sells outcome
0 held and `buy` sells outcome 1 held. The matcher fills a YES buy against a
YES sell, or against a NO buy at the complement by minting, so both sides
share one book. Everything the venue reports on outcome 1 comes back on the
YES leg.

**Orders.** A limit is `GTC`, `GTD` with `expires_at`, post-only (`PO`) on
request; an IOC limit or a market order is fill-and-kill (`FAK`) at the price
given as the worst accepted, `fok` is fill-or-kill. Prices are whole cents;
sizes are shares to 18 decimals. Up to 200 orders a batch. An edit is the
venue's atomic cancel-and-place, so the queue place is lost.

**Cancels repeat the order.** The venue cancels an order only with the order
and its signature in the body. Orders placed here are remembered; any other
is read back from the venue and signed again (the same order, the same key,
the same signature).

**Fees** are taker-only: a share of the notional by price, from the market's
table (`bps_by_price`). Makers pay nothing.

**AMM markets** (`myriad:56-amm-120473`, one outcome of many
`myriad:2741-amm-412-3`) trade against the pool, one contract call per trade
that the wallet sends and pays gas for. They are taker-only: a `market`,
`ioc` or `fok` order at a worst price, which bounds the average price paid
(fees in) or received. The order is sized from the pool read on chain: `ioc`
and `market` trade the most shares up to the quantity whose average is
within the worst price, `fok` the whole quantity or nothing. The bound goes
on chain as the contract's minimum shares out (a buy) or maximum shares in
(a sell), so the transaction reverts rather than fill worse. The fill (shares,
value, fee) is read from the receipt's `MarketActionTx`. A buy needs the
market's token approved to the network's `PredictionMarket` once
(`approve_amm()`); `redeem_amm()` claims a resolved market's winnings.

Order-book approvals are `approval_calls()`, for the wallet to send once;
split, merge and redeem calldata come from `position_calldata()`.
"""
from __future__ import annotations

import asyncio
import json
import math
import re
import secrets
import time
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any
from urllib.parse import urlencode

from .. import ids
from ..base import AsyncHttpClient, Capability
from ..errors import AuthenticationError, BadRequest, ExchangeError, MarketNotFound
from ..myriad import ORDER_BOOK_NETWORK, Ref, _after_buy, _after_sell, amm_fees, is_amm, iso_ms, native_id, parse_id
from ..types import Page
from . import myriad_signing as sig
from .base import TradingExchange
from .credentials import MyriadCredentials
from .errors import (
    CredentialsMissing, DuplicateClientOrderId, InsufficientFunds, InvalidOrder, OrderNotFound, OrderRejected,
)
from .limiter import BudgetLimiter, Priority
from .polymarket import to_yes_leg, wire_leg
from .polymarket_signing import MAX_UINT256, WalletSigner
from .types import (
    Account, Balance, EditRequest, FeeEstimate, Fill, Liquidity, Order, OrderRequest, OrderStatus, OrderType,
    Position, PositionSide, SettlementState, Side, TimeInForce, VENUE_ORDER_TYPES,
)

AMM_MARGIN = 1e-6
"""How far inside the worst price an AMM order is sized, so that rounding
never puts the expected trade on the bound itself."""

VENUE = "myriad"
API_URL = "https://api-v2.myriadprotocol.com"
RPC_URL = "https://bsc-dataseed.bnbchain.org"
SCALE = Decimal(10) ** 18
BATCH = 200
"""Orders per batch place, cancel or modify: the venue's maximum."""

SESSION_S = 12 * 60
"""A session token is renewed after this long; the venue's last 15 minutes."""

CLIENT_ORDER_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

STATUS = {"open": OrderStatus.OPEN, "filled": OrderStatus.CLOSED, "cancelled": OrderStatus.CANCELED,
          "canceled": OrderStatus.CANCELED, "expired": OrderStatus.EXPIRED}

TIF = {
    "GTC": (OrderType.LIMIT, TimeInForce.GTC),
    "GTD": (OrderType.LIMIT, TimeInForce.GTD),
    "PO": (OrderType.LIMIT, TimeInForce.GTC),
    "FAK": (OrderType.LIMIT, TimeInForce.IOC),
    "FOK": (OrderType.LIMIT, TimeInForce.FOK),
}
"""The venue's time in force as this library's order type and time in force."""

UPDATE_STATUS = {"PLACED": OrderStatus.OPEN, "PARTIALLY_FILLED": OrderStatus.OPEN, "FILLED": OrderStatus.CLOSED,
                 "CANCELLED": OrderStatus.CANCELED, "EXPIRED": OrderStatus.EXPIRED}


def D(value: Any) -> Decimal:
    return Decimal(str(value)) if value not in (None, "") else Decimal("0")


def wei(value: Any) -> Decimal:
    """An 18-decimal integer as tokens."""
    return D(int(str(value))) / SCALE if value not in (None, "") else Decimal("0")


def _outcome(value: Any) -> str:
    return "no" if int(value or 0) == 1 else "yes"


def _venue_side(value: Any) -> str:
    """`0`/`"buy"` -> `BUY`, `1`/`"sell"` -> `SELL`; a mint is a buy, a merge a sell."""
    text = str(value).lower()
    return "SELL" if text in ("1", "sell", "merge") else "BUY"


def market_id_of(network: Any, market: Any, *, amm: bool = False, outcome: int | None = None) -> str:
    return ids.qualify(VENUE, native_id(network or ORDER_BOOK_NETWORK, market, amm=amm, outcome=outcome))


# ---------------------------------------------------------------------------
# Order bodies
# ---------------------------------------------------------------------------

def immediate(request: OrderRequest) -> bool:
    return request.type == OrderType.MARKET or request.time_in_force in (TimeInForce.IOC, TimeInForce.FOK)


def time_in_force_of(request: OrderRequest) -> tuple[str, int]:
    """`(time_in_force, expiration)`: the venue's word and the signed expiry, unix seconds."""
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(f"{VENUE}: {request.type.value} is held by the execution engine, not the venue; "
                           f"submit it through the engine")
    if request.price is None:
        raise InvalidOrder(f"{VENUE}: a price is required -- a market order is sent at the worst price you give")
    tif = request.time_in_force
    if tif == TimeInForce.DAY:
        raise InvalidOrder(f"{VENUE}: 'day' is rewritten to 'gtd' by the engine; an adapter cannot pick a session end")
    expiration = 0
    if tif == TimeInForce.GTD:
        if request.expires_at is None:
            raise InvalidOrder(f"{VENUE}: a gtd order needs expires_at")
        expiration = int(request.expires_at // 1000)
    if request.post_only:
        if immediate(request):
            raise InvalidOrder(f"{VENUE}: a post-only order cannot also take at once")
        return "PO", expiration
    if request.type == OrderType.MARKET:
        return ("FOK" if tif == TimeInForce.FOK else "FAK"), 0
    return {TimeInForce.GTC: "GTC", TimeInForce.GTD: "GTD", TimeInForce.IOC: "FAK", TimeInForce.FOK: "FOK"}[tif], expiration


def build_signed_order(
    request: OrderRequest, *, signer: WalletSigner, market: int, nonce: int | None = None,
) -> tuple[dict[str, Any], str, Decimal]:
    """`(entry, order hash, contracts)`: the signed `{order, signature,
    signatureType, time_in_force}` entry `POST /orders` and the batch routes
    take, before anything is sent."""
    tif, expiration = time_in_force_of(request)
    outcome, venue_side = wire_leg(request)
    yes_price = D(request.price)
    price = yes_price if outcome == "yes" else Decimal("1") - yes_price
    amount = sig.shares_wei(D(request.quantity))
    order = sig.build_order(
        trader=signer.address, market_id=market, outcome=sig.YES if outcome == "yes" else sig.NO,
        side=sig.BUY if venue_side == "BUY" else sig.SELL, amount=amount, price=sig.price_wei(price),
        nonce=nonce if nonce is not None else sig.new_nonce(), expiration=expiration,
    )
    signature, digest = sig.sign_order(signer, order)
    entry = {"order": sig.wire_order(order), "signature": signature, "signatureType": sig.EOA, "time_in_force": tif}
    return entry, digest, Decimal(amount) / SCALE


# ---------------------------------------------------------------------------
# REST records
# ---------------------------------------------------------------------------

def order_of_record(row: dict[str, Any], *, account: Account | None = None) -> Order:
    """An order record (`GET /orders`, `GET /orders/:hash`) on the YES leg."""
    struct = row.get("order") or {}
    outcome, venue_side = _outcome(struct.get("outcomeId")), _venue_side(struct.get("side"))
    side, price = to_yes_leg(outcome, venue_side, wei(struct.get("price")) if struct.get("price") not in (None, "") else None)
    quantity, filled = wei(struct.get("amount")), wei(row.get("filledAmount"))
    status = STATUS.get(str(row.get("status") or "").lower(), OrderStatus.OPEN)
    kind = str(row.get("timeInForce") or "GTC").upper()
    order_type, tif = TIF.get(kind, (OrderType.LIMIT, TimeInForce.GTC))
    expiration = int(str(struct.get("expiration") or 0))
    return Order(
        id=str(row.get("orderHash") or ""),
        client_order_id=row.get("clientOrderId") or None,
        venue=VENUE,
        account=account,
        market_id=market_id_of(row.get("networkId"), struct.get("marketId")),
        side=side,
        type=order_type,
        time_in_force=tif,
        status=status,
        price=price,
        quantity=quantity,
        filled=filled,
        remaining=max(Decimal("0"), quantity - filled) if status == OrderStatus.OPEN else Decimal("0"),
        post_only=kind == "PO",
        reduce_only=venue_side == "SELL",
        expires_at=expiration * 1000 if expiration else None,
        created_at=iso_ms(row.get("createdAt")),
        updated_at=iso_ms(row.get("updatedAt")),
        info={**row, "outcome": outcome, "venue_side": venue_side},
    )


def order_of_update(data: dict[str, Any], *, account: Account | None = None) -> Order:
    """An `orders:` channel update (placed, partly filled, filled, cancelled,
    expired) as the order it describes, on the YES leg."""
    outcome, venue_side = _outcome(data.get("outcome")), _venue_side(data.get("side"))
    side, price = to_yes_leg(outcome, venue_side, wei(data.get("price")) if data.get("price") not in (None, "") else None)
    quantity, filled = wei(data.get("amount")), wei(data.get("filledAmount"))
    status = STATUS.get(str(data.get("status") or "").lower()) or UPDATE_STATUS.get(str(data.get("type") or ""),
                                                                                  OrderStatus.OPEN)
    kind = str(data.get("timeInForce") or "GTC").upper()
    order_type, tif = TIF.get(kind, (OrderType.LIMIT, TimeInForce.GTC))
    return Order(
        id=str(data.get("orderHash") or ""),
        venue=VENUE,
        account=account,
        market_id=market_id_of(data.get("networkId"), data.get("marketId")),
        side=side,
        type=order_type,
        time_in_force=tif,
        status=status,
        price=price,
        quantity=quantity,
        filled=filled,
        remaining=max(Decimal("0"), quantity - filled) if status == OrderStatus.OPEN else Decimal("0"),
        post_only=kind == "PO",
        reduce_only=venue_side == "SELL",
        created_at=iso_ms(data.get("createdAt")),
        updated_at=iso_ms(data.get("updatedAt")),
        info={**data, "outcome": outcome, "venue_side": venue_side},
    )


def fill_of_action(row: dict[str, Any], *, account: Account | None = None, amm: bool = False,
                   many: bool = False) -> Fill | None:
    """One trade in the account's history (`/users/:address/events`) as a fill
    on the YES leg, or `None` for anything else (claims, liquidity). The
    history names neither the order nor the fee. A pool trade (`amm`) is on
    the pool's market id; on a pool of more than two outcomes (`many`) each
    outcome is its own market, YES that outcome. A pool trade's price is its
    value over its shares, fees in."""
    action = str(row.get("action") or "")
    if action not in ("buy", "sell"):
        return None
    shares, value = D(row.get("shares")), D(row.get("value"))
    if shares <= 0:
        return None
    outcome = int(row.get("outcomeId") or 0)
    side, price = to_yes_leg("yes" if many else _outcome(outcome), action.upper(), value / shares)
    stamp = iso_ms(row.get("timestamp")) or 0
    return Fill(
        id=f"{row.get('txId') or stamp}:{outcome}:{action}",
        order_id="",
        venue=VENUE,
        account=account,
        market_id=market_id_of(row.get("networkId"), row.get("marketId"), amm=amm, outcome=outcome if many else None),
        side=side,
        price=price or Decimal("0"),
        quantity=shares,
        fee=None,
        fee_currency=None,
        liquidity=Liquidity.UNKNOWN,
        settlement=SettlementState.CONFIRMED,
        timestamp=stamp,
        info={k: v for k, v in row.items() if k not in ("imageUrl", "marketTitle")},
    )


def fills_of_trade(data: dict[str, Any], *, address: str, account: Account | None = None) -> list[Fill]:
    """This account's side of a `trades:` match: one fill per outcome and
    side it traded in the transaction, summed over its legs. The id is the
    transaction's, the outcome and the side, the same key the account's
    history gives the trade."""
    mine = address.lower()
    legs: list[tuple[dict[str, Any], Liquidity, Decimal, Decimal, Decimal]] = []
    taker = data.get("taker") or {}
    if str(taker.get("trader") or "").lower() == mine:
        legs.append((taker, Liquidity.TAKER, wei(taker.get("totalAmount")), wei(taker.get("averagePrice")),
                     wei((taker.get("totalFees") or {}).get("total"))))
    for maker in data.get("makers") or []:
        if str(maker.get("trader") or "").lower() == mine:
            legs.append((maker, Liquidity.MAKER, wei(maker.get("amount")), wei(maker.get("price")),
                         wei((maker.get("fees") or {}).get("total"))))
    groups: dict[tuple[int, str], list[tuple[dict[str, Any], Liquidity, Decimal, Decimal, Decimal]]] = {}
    for leg in legs:
        groups.setdefault((int(leg[0].get("outcome") or 0), _venue_side(leg[0].get("side")).lower()), []).append(leg)
    fills = []
    for (outcome, action), group in groups.items():
        quantity = sum((leg[2] for leg in group), Decimal("0"))
        if quantity <= 0:
            continue
        average = sum((leg[2] * leg[3] for leg in group), Decimal("0")) / quantity
        side, price = to_yes_leg(_outcome(outcome), action.upper(), average)
        liquidity = {leg[1] for leg in group}
        fills.append(Fill(
            id=f"{data.get('txHash')}:{outcome}:{action}",
            order_id=str(group[0][0].get("orderHash") or ""),
            venue=VENUE,
            account=account,
            market_id=market_id_of(data.get("networkId"), data.get("marketId")),
            side=side,
            price=price or Decimal("0"),
            quantity=quantity,
            fee=sum((leg[4] for leg in group), Decimal("0")),
            fee_currency="USD1",
            liquidity=liquidity.pop() if len(liquidity) == 1 else Liquidity.UNKNOWN,
            settlement=SettlementState.CONFIRMED,
            timestamp=int(data.get("ts") or 0),
            info={"tx_hash": data.get("txHash"), "orders": [str(leg[0].get("orderHash")) for leg in group]},
        ))
    return fills


RESOLVED = ("won", "lost", "claimed", "voided")


def positions_of(rows: list[dict[str, Any]], *, account: Account | None = None,
                 many: set[tuple[int, int]] | None = None) -> list[Position]:
    """The account's portfolio rows (one per market and outcome) as positions:
    a binary market's two outcomes netted on the YES leg with both kept, and
    an outcome of a many-outcome AMM market as its own market. `many` names
    the pools (`(network, market)`) known to have more than two outcomes; a
    row on an outcome above 1 shows it too."""
    by_market: dict[tuple[int, int, bool], list[dict[str, Any]]] = {}
    for row in rows:
        if row.get("marketId") is None:
            continue
        key = (int(row.get("networkId") or ORDER_BOOK_NETWORK), int(row["marketId"]), row.get("executionMode") == 0)
        by_market.setdefault(key, []).append(row)
    out: list[Position] = []
    for (network, market, amm), group in by_market.items():
        several = amm and ((network, market) in (many or set()) or any(int(r.get("outcomeId") or 0) > 1 for r in group))
        if several:
            for row in group:
                out.append(_position(network, market, amm, {int(row.get("outcomeId") or 0): row}, account=account,
                                     outcome=int(row.get("outcomeId") or 0)))
        else:
            out.append(_position(network, market, amm, {int(r.get("outcomeId") or 0): r for r in group}, account=account))
    return out


def _position(network: int, market: int, amm: bool, legs: dict[int, dict[str, Any]], *,
              account: Account | None, outcome: int | None = None) -> Position:
    yes_row = legs.get(outcome if outcome is not None else 0) or {}
    no_row = {} if outcome is not None else (legs.get(1) or {})
    held_yes, held_no = D(yes_row.get("shares")), D(no_row.get("shares"))
    net = held_yes - held_no
    lead, lead_row = ("yes", yes_row) if net >= 0 else ("no", no_row)
    entry = D(lead_row.get("price")) if lead_row.get("price") not in (None, "") else None
    if entry is not None and lead == "no":
        entry = Decimal("1") - entry
    mark: Decimal | None = None
    if yes_row.get("value") not in (None, "") and held_yes > 0:
        mark = D(yes_row["value"]) / held_yes
    elif no_row.get("value") not in (None, "") and held_no > 0:
        mark = Decimal("1") - D(no_row["value"]) / held_no
    profits = [r.get("profit") for r in (yes_row, no_row) if r.get("profit") not in (None, "")]
    statuses = {str(r.get("status") or "") for r in (yes_row, no_row) if r}
    resolved = bool(statuses) and statuses <= set(RESOLVED)
    won = any(s in ("won", "claimed") for s in statuses) if resolved else None
    to_claim = [r for r in (yes_row, no_row) if r.get("winningsToClaim") or r.get("voidedWinningsToClaim")]
    return Position(
        venue=VENUE,
        account=account,
        market_id=market_id_of(network, market, amm=amm, outcome=outcome),
        side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
        contracts=abs(net),
        inventory_yes=held_yes,
        inventory_no=held_no if outcome is None else None,
        entry_price=entry,
        mark_price=mark,
        unrealized_pnl=sum((D(p) for p in profits), Decimal("0")) if profits else None,
        resolved=resolved,
        final=resolved,
        won=won,
        redeemable=sum((D(r.get("shares")) for r in to_claim), Decimal("0")) if to_claim else None,
        info={"rows": [r for r in (yes_row, no_row) if r]},
    )


def error_of(exc: ExchangeError, *, client_order_id: str | None = None) -> ExchangeError:
    """The venue's refusal of an order as a typed error, by its words."""
    body = exc.body if isinstance(exc.body, dict) else {}
    text = str(body.get("error") or body.get("message") or exc)
    code = str(body.get("code") or "")
    lower = f"{text} {code}".lower()
    if code == "NO_WALLET_CONNECTED" or "wallet_mismatch" in lower or "trader_forbidden" in lower:
        return CredentialsMissing(f"{VENUE}: {text} -- orders must be the wallet the account signed in with")
    if exc.status == 409:
        if client_order_id and "client" in lower:
            return DuplicateClientOrderId(f"{VENUE}: {text}")
        return OrderRejected(f"{VENUE}: {text}", reason="duplicate", body=exc.body, status=exc.status)
    if "balance" in lower or "allowance" in lower or "insufficient" in lower:
        return InsufficientFunds(f"{VENUE}: {text}", body=exc.body)
    if any(word in lower for word in ("price", "amount", "tick", "invalid", "expiration", "payload")):
        return InvalidOrder(f"{VENUE}: {text}")
    return OrderRejected(f"{VENUE}: {text}", reason=code or None, info=body, body=exc.body, status=exc.status)


# ---------------------------------------------------------------------------
# AMM orders
# ---------------------------------------------------------------------------

def _bisect(rising: Any, target: float, low: float, high: float) -> tuple[float, float]:
    """`(low, high)` bracketing where the increasing `rising` reaches `target`."""
    for _ in range(200):
        middle = (low + high) / 2
        if middle in (low, high):
            break
        if rising(middle) < target:
            low = middle
        else:
            high = middle
    return low, high


def amm_buy_shares(pools: list[float], index: int, value: float, fee: float) -> float:
    """The shares of outcome `index` that `value` buys, fees taken out of it."""
    if value <= 0:
        return 0.0
    return _after_buy(pools, index, value * (1 - fee))[1]


def amm_buy_cost(pools: list[float], index: int, shares: float, fee: float) -> float:
    """The value, fees in, that buys `shares` of outcome `index`."""
    if shares <= 0:
        return 0.0
    high = 1.0
    for _ in range(200):
        if amm_buy_shares(pools, index, high, fee) >= shares:
            break
        high *= 2
    return _bisect(lambda v: amm_buy_shares(pools, index, v, fee), shares, 0.0, high)[1]


def _sell_ceiling(pools: list[float], index: int) -> float:
    return min(n for j, n in enumerate(pools) if j != index)


def amm_sell_shares(pools: list[float], index: int, value: float, fee: float) -> float:
    """The shares of outcome `index` a payout of `value`, after fees, takes:
    the pool pays out `value / (1 - fee)` and keeps the fee."""
    if value <= 0:
        return 0.0
    gross = value / (1 - fee)
    if gross >= _sell_ceiling(pools, index):
        return math.inf
    return _after_sell(pools, index, gross)[1]


def amm_sell_value(pools: list[float], index: int, shares: float, fee: float) -> float:
    """What selling `shares` of outcome `index` pays out, after fees."""
    if shares <= 0:
        return 0.0
    ceiling = _sell_ceiling(pools, index) * (1 - fee) * (1 - 1e-12)
    return _bisect(lambda v: amm_sell_shares(pools, index, v, fee), shares, 0.0, ceiling)[0]


def _units(value: float | Decimal, decimals: int, rounding: str) -> int:
    return int((Decimal(str(value)) * (Decimal(10) ** decimals)).to_integral_value(rounding=rounding))


@dataclass
class AmmPlan:
    """One pool trade, sized and bounded before it is sent. Amounts in tokens
    unless named `_units` (the token's smallest unit); prices are the
    outcome's own, fees in."""

    network: int
    market: int
    outcome: int
    """The outcome traded."""
    action: str
    """`buy` or `sell`."""
    worst: Decimal
    """The worst average price accepted, on the outcome traded."""
    requested: Decimal
    """The shares asked for."""
    shares: Decimal = Decimal("0")
    """The shares expected, from the pool as read."""
    value: Decimal = Decimal("0")
    """Spent, fees in (a buy), or paid out, after fees (a sell)."""
    average: Decimal | None = None
    fee: Decimal = Decimal("0")
    value_units: int = 0
    bound_units: int = 0
    """The on-chain bound: the minimum shares out (a buy), the maximum shares in (a sell)."""
    decimals: int = 18
    contract: str = ""
    token: str = ""
    symbol: str = ""
    calldata: str = ""
    reason: str | None = None
    """Why nothing is traded, when the pool fills nothing within the worst price."""
    info: dict[str, Any] = field(default_factory=dict)

    @property
    def fills(self) -> bool:
        return self.reason is None

    @property
    def bound(self) -> Decimal:
        return Decimal(self.bound_units) / (Decimal(10) ** self.decimals)


def plan_amm_trade(
    pools: list[float], *, index: int, action: str, shares: Decimal, worst: Decimal, buy_fee: float, sell_fee: float,
    decimals: int, partial: bool, held: Decimal | None = None,
) -> dict[str, Any]:
    """Size a pool trade and its on-chain bound. `worst` bounds the average
    price, fees in: a buy spends `value` for at least `value / worst` shares,
    a sell is paid `value` for at most `value / worst`. `partial` (`ioc`)
    trades the most shares up to `shares` whose average is within `worst`;
    otherwise all of them or nothing. Returns the plan's numbers, or a
    `reason` when nothing trades."""
    want, bound = float(shares), float(worst)
    if action == "buy":
        def average(q: float) -> float:
            return amm_buy_cost(pools, index, q, buy_fee) / q
        within = lambda q: average(q) <= bound * (1 - AMM_MARGIN)  # noqa: E731
    else:
        def average(q: float) -> float:
            return amm_sell_value(pools, index, q, sell_fee) / q
        within = lambda q: average(q) >= bound * (1 + AMM_MARGIN)  # noqa: E731
    size = want
    if not within(size):
        if not partial:
            return {"reason": f"the pool fills {shares} at an average of {average(want):.6f}, worse than {worst}"}
        low, high = 0.0, want
        for _ in range(200):
            middle = (low + high) / 2
            if middle in (low, high):
                break
            low, high = (middle, high) if within(middle) else (low, middle)
        size = low
        if size <= 0 or not within(size) or size * 10**decimals < 1:
            return {"reason": f"the pool fills nothing at {worst} or better"}
    if action == "buy":
        value_units = _units(amm_buy_cost(pools, index, size, buy_fee), decimals, ROUND_FLOOR)
        value = Decimal(value_units) / (Decimal(10) ** decimals)
        expected = amm_buy_shares(pools, index, float(value), buy_fee)
        bound_units = int((Decimal(value_units) / worst).to_integral_value(rounding=ROUND_CEILING))
        fee = value * Decimal(str(buy_fee))
    else:
        value_units = _units(amm_sell_value(pools, index, size, sell_fee), decimals, ROUND_FLOOR)
        value = Decimal(value_units) / (Decimal(10) ** decimals)
        expected = amm_sell_shares(pools, index, float(value), sell_fee)
        bound_units = int((Decimal(value_units) / worst).to_integral_value(rounding=ROUND_FLOOR))
        if held is not None:
            bound_units = min(bound_units, _units(held, decimals, ROUND_FLOOR))
        fee = value / (1 - Decimal(str(sell_fee))) - value if sell_fee else Decimal("0")
    if value_units <= 0 or expected <= 0:
        return {"reason": f"the pool fills nothing at {worst} or better"}
    shares_out = Decimal(str(expected))
    return {"value_units": value_units, "bound_units": bound_units, "value": value, "shares": shares_out,
            "average": value / shares_out, "fee": fee}


def amm_time_in_force(request: OrderRequest) -> bool:
    """Whether an AMM order may fill in part (`market`, `ioc`) or must fill
    whole (`fok`). A pool trade fills at once or not at all, so a resting
    order is refused."""
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(f"{VENUE}: {request.type.value} is held by the execution engine, not the venue; "
                           f"submit it through the engine")
    tif = request.time_in_force
    resting = request.post_only or (request.type == OrderType.LIMIT and tif not in (TimeInForce.IOC, TimeInForce.FOK))
    if resting:
        what = "post-only" if request.post_only else f"a {tif.value} limit"
        raise InvalidOrder(f"{VENUE}: AMM markets are taker-only -- a pool trade fills at once or not at all, so "
                           f"{what} cannot rest; send a market, ioc or fok order with the worst price")
    if request.price is None:
        raise InvalidOrder(f"{VENUE}: a price is required -- an AMM order is sent with the worst average price "
                           f"you give")
    if not Decimal("0") < D(request.price) < Decimal("1"):
        raise InvalidOrder(f"{VENUE}: price {request.price} is outside (0, 1)")
    return tif != TimeInForce.FOK


def amm_leg(request: OrderRequest, ref: Ref, outcomes: int) -> tuple[int, str, Decimal]:
    """`(outcome, action, worst)`: the pool outcome an order trades, `buy`
    or `sell`, and its worst price on that outcome. On a two-outcome pool YES
    is outcome 0 and NO outcome 1, as on the order book; on one outcome of
    many, YES is that outcome and NO (the rest of the pool) is not one trade."""
    side, venue_side = wire_leg(request)
    action = "buy" if venue_side == "BUY" else "sell"
    if ref.outcome is not None:
        if ref.outcome >= outcomes:
            raise InvalidOrder(f"{VENUE}: {ref.native} names outcome {ref.outcome}; the pool has {outcomes}")
        if side == "no":
            hint = "pass reduce_only to sell the outcome held" if action == "buy" else "buy the other outcomes' markets"
            raise InvalidOrder(f"{VENUE}: NO on {ref.native} is the rest of the pool, not one trade; {hint}")
        return ref.outcome, action, D(request.price)
    if outcomes != 2:
        raise InvalidOrder(f"{VENUE}: {ref.native} has {outcomes} outcomes; trade one of them, "
                           f"{ref.native}-<outcome>")
    return (0 if side == "yes" else 1), action, (D(request.price) if side == "yes" else Decimal("1") - D(request.price))


def amm_fill(action: dict[str, Any], *, tx_hash: str, plan: AmmPlan, many: bool, order_id: str,
             client_order_id: str | None = None, account: Account | None = None) -> Fill:
    """A decoded `MarketActionTx` as this account's fill, on the YES leg and
    with the same id the account's history gives the trade. The price is the
    value over the shares, fees in, as in the history; `fee` is that fee."""
    scale = Decimal(10) ** plan.decimals
    shares, value = Decimal(int(action["shares"])) / scale, Decimal(int(action["value"])) / scale
    outcome = int(action["outcome"])
    buying = int(action["action"]) == sig.AMM_BUY
    rate = Decimal(str(plan.info.get("buy_fee" if buying else "sell_fee") or 0))
    fee = value * rate if buying else (value / (1 - rate) - value if rate else Decimal("0"))
    side, price = to_yes_leg("yes" if many or outcome == 0 else "no", "BUY" if buying else "SELL",
                             value / shares if shares > 0 else None)
    return Fill(
        id=f"{tx_hash}:{outcome}:{'buy' if buying else 'sell'}",
        order_id=order_id,
        client_order_id=client_order_id,
        venue=VENUE,
        account=account,
        market_id=market_id_of(plan.network, plan.market, amm=True, outcome=outcome if many else None),
        side=side,
        price=price or Decimal("0"),
        quantity=shares,
        fee=fee,
        fee_currency=plan.symbol or None,
        liquidity=Liquidity.TAKER,
        settlement=SettlementState.CONFIRMED,
        timestamp=int(action.get("timestamp") or 0) * 1000,
        info={"tx_hash": tx_hash, "outcome": outcome, "value": str(value), "fee_in_price": True},
    )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class MyriadTrading(TradingExchange):
    """Myriad order entry: signed orders on the order book, and market
    orders against an AMM market's pool, sent as the wallet's transactions.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.myriad import MyriadTrading

    async with MyriadTrading(require("myriad", load_credentials())) as myriad:
        print(await myriad.fetch_balance())
    ```
    """

    id = VENUE
    name = "Myriad"
    has: dict[str, Capability] = {
        # Order books: signed orders. AMM markets: market, ioc or fok only,
        # one on-chain transaction each.
        "create_order": True,
        # Up to 200 a request; the venue checks collateral at match time.
        # AMM orders go one transaction at a time.
        "create_orders": True,
        "cancel_order": True,
        # Up to 200 a request.
        "cancel_orders": True,
        "cancel_all_orders": True,
        # The venue's atomic cancel-and-place: the queue place is lost.
        "edit_order": True,
        "fetch_order": True,
        "fetch_open_orders": True,
        "fetch_orders": True,
        # The account's trade history names neither the order nor the fee.
        "fetch_my_trades": "partial",
        "fetch_positions": True,
        "fetch_balance": True,
        "fetch_settlements": False,
        "fetch_queue_position": False,
        "fetch_fee_estimate": True,
        "rfq": False,
        "split_merge": False,
        "watch_orders": False,
        "watch_my_trades": False,
        "watch_positions": False,
        "watch_balance": False,
    }

    def __init__(
        self,
        credentials: MyriadCredentials,
        *,
        account_name: str = "default",
        url: str = API_URL,
        rpc_url: str | None = None,
        catalog: Any = None,
        limiter: BudgetLimiter | None = None,
        timeout: float = 30.0,
        client: Any = None,
        rpc_client: Any = None,
        rpc_urls: dict[int, str] | None = None,
    ):
        import httpx

        self.credentials = credentials
        self.signer = WalletSigner(credentials.private_key) if credentials.private_key else None
        self.address = credentials.address
        """The trading wallet: the signer's, or `MYRIAD_ACCOUNT_ADDRESS`."""
        self.account = Account(venue=VENUE, name=account_name)
        keyed = bool(credentials.api_key and credentials.api_secret)
        # 200 requests a second with a key, 30 per 10 seconds without; orders
        # 200 per 10 seconds per wallet either way.
        self.limiter = limiter or BudgetLimiter(read_per_second=50 if keyed else 2.5,
                                                write_per_second=15 if keyed else 2.5)
        shared = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        self.http = AsyncHttpClient(url, limiter=None, client=shared, venue=VENUE)
        self.rpc_url = rpc_url or credentials.rpc_url or RPC_URL
        self.rpc_urls = {**sig.AMM_RPC_URLS, **(rpc_urls or {}), ORDER_BOOK_NETWORK: self.rpc_url}
        """A node per network (BNB Chain 56, Abstract 2741, Linea 59144) for
        AMM trades; public ones unless given."""
        self.rpc = rpc_client or httpx.AsyncClient(timeout=20, follow_redirects=True)
        self._own_catalog = catalog is None
        if catalog is None:
            from ..myriad import KEYED_LIMITER, LIMITER, Myriad

            catalog = Myriad(api_key=credentials.api_key, api_secret=credentials.api_secret,
                             limiter=KEYED_LIMITER if keyed else LIMITER)
        self.catalog = catalog
        """The read adapter: market ids, fee tables."""
        self._signed: dict[str, dict[str, Any]] = {}
        """Order hash -> the signed `{order, signature}` a cancel repeats."""
        self._session: dict[str, Any] | None = None
        self._amm_orders: dict[str, tuple[Order, AmmPlan, bool]] = {}
        """Transaction hash -> an AMM order sent here, its plan, and whether its pool has many outcomes."""
        self._outcome_counts: dict[tuple[int, int], int] = {}
        self._tx_lock = asyncio.Lock()

    @property
    def keyed(self) -> bool:
        return bool(self.credentials.api_key and self.credentials.api_secret)

    # -- plumbing -------------------------------------------------------------

    def _need_signer(self, what: str) -> WalletSigner:
        if self.signer is None:
            raise CredentialsMissing(f"{VENUE}: {what} needs the wallet's signature; set MYRIAD_PRIVATE_KEY")
        return self.signer

    def _need_address(self) -> str:
        if not self.address:
            raise CredentialsMissing(f"{VENUE}: the account's wallet is unknown; set MYRIAD_PRIVATE_KEY or "
                                     f"MYRIAD_ACCOUNT_ADDRESS")
        return self.address

    async def _call(
        self, method: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None,
        kind: str = "read", priority: Priority = Priority.NORMAL, signed: bool = True, retry: bool = True,
    ) -> Any:
        """One call, paced and proven: HMAC over exactly what is sent with a
        key, the session token without one. The query string and the body
        are built here exactly as sent."""
        await self.limiter.acquire(cost=1, kind=kind, priority=priority)  # type: ignore[arg-type]
        query = urlencode([(k, v) for k, v in (params or {}).items() if v is not None], doseq=True)
        target = f"{path}?{query}" if query else path
        text = json.dumps(body, separators=(",", ":"), ensure_ascii=False) if body is not None else ""
        headers: dict[str, str] = {"Content-Type": "application/json"} if text else {}
        if signed:
            if self.keyed:
                headers.update(sig.request_headers(self.credentials.api_key or "", self.credentials.api_secret or "",
                                                   method, target, text))
            else:
                headers["Authorization"] = f"Bearer {await self._access_token()}"
        try:
            return await self.http.request(method, target, content=text.encode() if text else None, headers=headers)
        except AuthenticationError as exc:
            if signed and retry and not self.keyed and exc.status == 401:
                self._session = None
                return await self._call(method, path, params=params, body=body, kind=kind, priority=priority,
                                        retry=False)
            raise
        except MarketNotFound as exc:
            if path.startswith("/orders"):
                raise OrderNotFound(str(exc), body=exc.body, status=exc.status) from None
            raise

    async def _access_token(self) -> str:
        """The session token: kept, refreshed once it ages, else a fresh sign-in."""
        session = self._session
        if session and time.time() - session["at"] < SESSION_S:
            return str(session["access"])
        if session and session.get("refresh"):
            try:
                raw = await self._call("POST", "/auth/refresh", body={"refreshToken": session["refresh"]},
                                       signed=False) or {}
                if raw.get("accessToken"):
                    self._keep_session(raw)
                    return str(raw["accessToken"])
            except (AuthenticationError, BadRequest):
                pass
        return await self.login()

    def _keep_session(self, raw: dict[str, Any]) -> None:
        refresh = raw.get("refreshToken") or (self._session or {}).get("refresh")
        self._session = {"access": raw["accessToken"], "refresh": refresh, "at": time.time()}

    async def login(self) -> str:
        """Sign in with the wallet (SIWE) and keep the session token. The
        venue's first sign-in for a wallet creates its Myriad account."""
        signer = self._need_signer("signing in")
        nonce = await self._call("GET", "/auth/nonce", params={"address": signer.address}, signed=False) or {}
        message = nonce.get("message")
        if not message:
            raise AuthenticationError(f"{VENUE}: no sign-in message in the venue's answer")
        raw = await self._call("POST", "/auth/login", body={"message": message, "signature": sig.sign_login(signer, message)},
                               signed=False) or {}
        if not raw.get("accessToken"):
            raise AuthenticationError(f"{VENUE}: the sign-in returned no session token")
        self._keep_session(raw)
        return str(raw["accessToken"])

    async def create_api_key(self, label: str = "synpath") -> dict[str, Any]:
        """Mint an API key for the signed-in wallet: `{key, secret, ...}`, the
        secret shown this once. Set them as MYRIAD_API_KEY / MYRIAD_API_SECRET."""
        if self.keyed:
            raise BadRequest(f"{VENUE}: an API key is minted from a wallet session; this adapter signs with a key already")
        return await self._call("POST", "/auth/api-keys", body={"label": label}, kind="write") or {}

    async def subscription_token(self, channel: str) -> str:
        """A short-lived token for one of this wallet's own realtime channels
        (`orders:56:0x...`, `positions:56:0x...`)."""
        raw = await self._call("POST", "/auth/centrifugo/subscription-token", body={"channel": channel}) or {}
        if not raw.get("token"):
            raise AuthenticationError(f"{VENUE}: no subscription token for {channel}")
        return str(raw["token"])

    async def _ref(self, market_id: str) -> Ref:
        """A market's parts, either kind; a slug is looked up."""
        native = ids.native(VENUE, market_id)
        ref = parse_id(native)
        if ref is None:
            ref, _ = await asyncio.to_thread(self.catalog._resolve, native)
        return ref

    async def _ob_ref(self, market_id: str) -> Ref:
        """An order-book market's parts; an AMM market is refused."""
        ref = await self._ref(market_id)
        if ref.amm:
            raise InvalidOrder(f"{VENUE}: {ref.native} is an AMM market: it has no orders, cancels or calldata "
                               f"routes; trade it with a market, ioc or fok order")
        if ref.network != ORDER_BOOK_NETWORK:
            raise InvalidOrder(f"{VENUE}: order books run on BNB Chain (56) only, not {ref.network}")
        return ref

    def _remember(self, digest: str, entry: dict[str, Any]) -> None:
        self._signed[digest.lower()] = {"order": entry["order"], "signature": entry["signature"],
                                        "signatureType": entry.get("signatureType", sig.EOA)}

    async def _signed_entry(self, order_id: str) -> dict[str, Any]:
        """The `{order, signature}` a cancel repeats: remembered, or read back
        and signed again."""
        cached = self._signed.get(order_id.lower())
        if cached is not None:
            return cached
        signer = self._need_signer("cancelling an order")
        row = await self._call("GET", f"/orders/{order_id}") or {}
        struct = row.get("order") or {}
        if not struct:
            raise OrderNotFound(f"{VENUE}: no order {order_id}")
        order = sig.order_of_wire(struct)
        if order["trader"].lower() != signer.address.lower():
            raise CredentialsMissing(f"{VENUE}: order {order_id} belongs to {order['trader']}, not this wallet")
        signature, digest = sig.sign_order(signer, order)
        entry = {"order": sig.wire_order(order), "signature": signature, "signatureType": sig.EOA}
        self._signed[digest.lower()] = entry
        return entry

    # -- orders ---------------------------------------------------------------

    def _order(self, request: OrderRequest, *, ref: Ref, order_id: str, contracts: Decimal, status: str | None,
               info: dict[str, Any]) -> Order:
        return Order(
            id=order_id,
            client_order_id=request.client_order_id,
            venue=VENUE,
            account=request.account or self.account,
            market_id=market_id_of(ref.network, ref.market),
            side=request.side,
            type=request.type,
            time_in_force=request.time_in_force,
            status=STATUS.get(str(status or "open").lower(), OrderStatus.OPEN),
            price=D(request.price),
            quantity=contracts,
            filled=Decimal("0"),
            reduce_only=request.reduce_only,
            post_only=request.post_only,
            expires_at=request.expires_at,
            created_at=int(time.time() * 1000),
            book=request.book,
            trader=request.trader,
            tags=request.tags,
            info=info,
        )

    async def create_order(self, request: OrderRequest) -> Order:
        """Sign and place one order. On an order book the venue answers once
        the order is on the book, and fills follow from the matcher. On an
        AMM market the trade is a transaction from the wallet; see
        `create_amm_order`."""
        ref = await self._ref(request.market_id)
        if ref.amm:
            return await self.create_amm_order(request)
        time_in_force_of(request)
        signer = self._need_signer("placing an order")
        ref = await self._ob_ref(request.market_id)
        entry, digest, contracts = build_signed_order(request, signer=signer, market=ref.market)
        body: dict[str, Any] = {**entry, "network_id": ref.network}
        if request.client_order_id and self.keyed and CLIENT_ORDER_ID.match(request.client_order_id):
            body["client_order_id"] = request.client_order_id
        try:
            raw = await self._call("POST", "/orders", body=body, kind="write") or {}
        except (BadRequest, OrderNotFound) as exc:
            raise error_of(exc, client_order_id=body.get("client_order_id")) from None
        except ExchangeError as exc:
            if exc.status == 409:
                raise error_of(exc, client_order_id=body.get("client_order_id")) from None
            raise
        order_id = str(raw.get("orderHash") or digest)
        self._remember(order_id, entry)
        return self._order(request, ref=ref, order_id=order_id, contracts=contracts, status=raw.get("status"), info={
            "venue": raw, "request": {**body, "signature": None}, "hash": digest,
        })

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        """Many orders, 200 a request, each signed; one result per request in
        the order given, a refused one as its exception."""
        results: list[Order | Exception | None] = [None] * len(requests)
        prepared: list[tuple[int, OrderRequest, dict[str, Any], str, Decimal, Ref]] = []
        for i, request in enumerate(requests):
            try:
                if (await self._ref(request.market_id)).amm:
                    results[i] = await self.create_amm_order(request)
                    continue
                time_in_force_of(request)
                signer = self._need_signer("placing an order")
                ref = await self._ob_ref(request.market_id)
                entry, digest, contracts = build_signed_order(request, signer=signer, market=ref.market)
                prepared.append((i, request, entry, digest, contracts, ref))
            except Exception as exc:
                results[i] = exc
        for start in range(0, len(prepared), BATCH):
            chunk = prepared[start:start + BATCH]
            body = {"orders": [p[2] for p in chunk], "network_id": ORDER_BOOK_NETWORK, "allow_partial": True}
            try:
                raw = await self._call("POST", "/orders/batch", body=body, kind="write") or {}
            except (BadRequest, ExchangeError) as exc:
                for i, *_ in chunk:
                    results[i] = error_of(exc)
                continue
            placed = {str(h).lower() for h in raw.get("placed") or []}
            errors = {int(e.get("index")): e for e in raw.get("errors") or [] if isinstance(e, dict) and e.get("index") is not None}
            for position, (i, request, entry, digest, contracts, ref) in enumerate(chunk):
                if position in errors or digest.lower() not in placed:
                    reason = str((errors.get(position) or {}).get("reason") or "not placed")
                    results[i] = error_of(OrderRejected(reason, body={"error": reason}))
                    continue
                self._remember(digest, entry)
                results[i] = self._order(request, ref=ref, order_id=digest, contracts=contracts, status="open", info={
                    "request": {**entry, "signature": None}, "hash": digest,
                })
        return results  # type: ignore[return-value]

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        """Cancel one order (repeating it and its signature) and read it back."""
        if order_id.lower() in self._amm_orders:
            raise InvalidOrder(f"{VENUE}: {order_id} is an AMM trade, one transaction filled or reverted at once; "
                               f"there is nothing to cancel")
        entry = await self._signed_entry(order_id)
        await self._call("DELETE", f"/orders/{order_id}", body={**entry, "network_id": ORDER_BOOK_NETWORK},
                         kind="write", priority=Priority.HIGH)
        return await self.fetch_order(order_id)

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        """Cancel up to 200 orders a request, then read each back."""
        failed: dict[str, str] = {}
        entries: list[tuple[str, dict[str, Any]]] = []
        out: dict[str, Exception] = {}
        for order_id in order_ids:
            try:
                entries.append((order_id, await self._signed_entry(order_id)))
            except Exception as exc:
                out[order_id] = exc
        for start in range(0, len(entries), BATCH):
            chunk = entries[start:start + BATCH]
            raw = await self._call("POST", "/orders/cancel-batch", body={
                "orders": [{k: v for k, v in e.items()} for _, e in chunk], "network_id": ORDER_BOOK_NETWORK,
                "allow_partial": True,
            }, kind="write", priority=Priority.HIGH) or {}
            for item in raw.get("errors") or []:
                if isinstance(item, dict) and item.get("index") is not None and int(item["index"]) < len(chunk):
                    failed[chunk[int(item["index"])][0]] = str(item.get("reason") or "refused")
        results: list[Order | Exception] = []
        for order_id in order_ids:
            if order_id in out:
                results.append(out[order_id])
                continue
            try:
                order = await self.fetch_order(order_id)
            except OrderNotFound as exc:
                results.append(exc)
                continue
            if order_id in failed and order.status == OrderStatus.OPEN:
                results.append(OrderRejected(f"{VENUE}: cancel of {order_id} refused: {failed[order_id]}",
                                             reason="cancel_refused"))
                continue
            results.append(order)
        return results

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int:
        """Every open order, or every one on a market, with one signed
        `CancelAll`. Returns how many the venue cancelled."""
        signer = self._need_signer("cancelling orders")
        market = (await self._ob_ref(market_id)).market if market_id else 0
        raw = await self._call("POST", "/orders/cancel-all", body=sig.sign_cancel_all(signer, market),
                               kind="write", priority=Priority.HIGH) or {}
        return int(raw.get("cancelled_count") or 0)

    async def edit_order(self, request: EditRequest, *, current: Order | None = None) -> Order:
        """Replace a resting order with the venue's atomic cancel-and-place:
        the old order is cancelled and the new one placed in one request, so
        the matcher never sees both. The queue place is lost."""
        current = current or await self.fetch_order(request.order_id)
        if current.status != OrderStatus.OPEN:
            raise InvalidOrder(f"{VENUE}: order {current.id} is {current.status.value}; only an open order can be edited")
        tif = request.time_in_force or current.time_in_force
        replacement = OrderRequest(
            market_id=current.market_id, side=current.side, quantity=request.quantity or current.remaining or current.quantity,
            price=request.price if request.price is not None else current.price, time_in_force=tif,
            expires_at=request.expires_at or current.expires_at, post_only=current.post_only,
            reduce_only=current.reduce_only, client_order_id=request.client_order_id,
        )
        signer = self._need_signer("editing an order")
        ref = await self._ob_ref(current.market_id)
        cancel = await self._signed_entry(current.id)
        entry, digest, contracts = build_signed_order(replacement, signer=signer, market=ref.market)
        try:
            raw = await self._call("POST", "/orders/batch-modify", body={
                "cancel": [cancel], "place": [entry], "network_id": ref.network, "allow_partial": False,
            }, kind="write", priority=Priority.HIGH) or {}
        except (BadRequest, ExchangeError) as exc:
            raise error_of(exc) from None
        placed = [str(h) for h in raw.get("placed") or []]
        order_id = placed[0] if placed else digest
        self._remember(order_id, entry)
        order = self._order(replacement, ref=ref, order_id=order_id, contracts=contracts, status="open", info={
            "venue": raw, "replaces": current.id, "request": {**entry, "signature": None}, "hash": digest,
        })
        return order.model_copy(update={"queue_priority_preserved": False})

    async def fetch_order(self, order_id: str) -> Order:
        """An order by its hash; an AMM order sent here by its transaction
        hash, its receipt read again while it is pending."""
        if order_id.lower() in self._amm_orders:
            order, plan, many = self._amm_orders[order_id.lower()]
            if order.status == OrderStatus.PENDING:
                receipt = await self._rpc(plan.network, "eth_getTransactionReceipt", [order.id])
                if receipt:
                    order = self._settle_amm(order, plan, many, receipt)
            return order
        row = await self._call("GET", f"/orders/{order_id}") or {}
        if not row.get("orderHash"):
            raise OrderNotFound(f"{VENUE}: no order {order_id}")
        return order_of_record(row, account=self.account)

    async def fetch_orders(
        self, *, status: str | None = None, market_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Order]:
        """This account's orders, newest first. `status` is `open`, `filled`,
        `cancelled` or `expired` (also `closed`, `canceled`)."""
        word = {"closed": "filled", "canceled": "cancelled"}.get(status or "", status)
        if word and word not in ("open", "filled", "cancelled", "expired"):
            raise BadRequest(f"{VENUE}: status is open, filled, cancelled or expired, not {status!r}")
        ref = await self._ref(market_id) if market_id else None
        if ref is not None and ref.amm:
            # A pool has no orders at the venue: only the trades sent here.
            wanted = ids.qualify(VENUE, ref.native)
            mine = [o for o, _, _ in self._amm_orders.values() if o.market_id == wanted and (
                word is None or (word == "open" and o.status == OrderStatus.PENDING)
                or (word == "filled" and o.status == OrderStatus.CLOSED)
                or (word == "cancelled" and o.status == OrderStatus.CANCELED))]
            return Page(sorted(mine, key=lambda o: o.created_at or 0, reverse=True), next_cursor=None)
        market = (await self._ob_ref(market_id)).market if market_id else None
        page = int(cursor) if cursor and cursor.isdigit() else 1
        raw = await self._call("GET", "/orders", params={
            "status": word, "network_id": ORDER_BOOK_NETWORK, "market_id": market, "page": page,
            "limit": min(limit or 500, 5000),
        }) or {}
        rows = raw.get("data") or [] if isinstance(raw, dict) else raw or []
        orders = [order_of_record(row, account=self.account) for row in rows]
        if since:
            orders = [o for o in orders if (o.created_at or since) >= since]
        more = bool((raw.get("pagination") or {}).get("hasNext")) if isinstance(raw, dict) else False
        return Page(orders, next_cursor=str(page + 1) if more else None)

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        orders: list[Order] = []
        cursor: str | None = None
        while True:
            page = await self.fetch_orders(status="open", market_id=market_id, limit=5000, cursor=cursor)
            orders += page
            cursor = page.next_cursor
            if not cursor:
                return orders

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        """One page of this account's trades, newest first, from its history:
        order book and AMM alike, or one market's. The history names neither
        the order behind a trade nor its fee, so `order_id` cannot filter it.
        Without a market, page `n` is the `n`th page of each kind, merged."""
        if order_id:
            raise BadRequest(f"{VENUE}: the account's history does not name orders; read the order's filled "
                             f"amount with fetch_order")
        address = self._need_address()
        ref = await self._ref(market_id) if market_id else None
        page = int(cursor) if cursor and cursor.isdigit() else 1
        size = min(limit or 100, 100)
        kinds = [ref.amm] if ref is not None else [False, True]
        fills: list[Fill] = []
        more = False
        for amm in kinds:
            raw = await self._call("GET", f"/users/{address}/events", params={
                "trading_model": "amm" if amm else "ob",
                "network_id": ref.network if ref is not None else None if amm else ORDER_BOOK_NETWORK,
                "market_id": ref.market if ref is not None else None,
                "since": since // 1000 if since else None, "page": page, "limit": size,
            }) or {}
            rows = raw.get("data") or [] if isinstance(raw, dict) else []
            for row in rows:
                many = False
                if amm and row.get("marketId") is not None:
                    many = await self._many(int(row.get("networkId") or ORDER_BOOK_NETWORK), int(row["marketId"]))
                    if ref is not None and ref.outcome is not None and int(row.get("outcomeId") or 0) != ref.outcome:
                        continue
                fill = fill_of_action(row, account=self.account, amm=amm, many=many)
                if fill is not None:
                    fills.append(fill)
            more = more or (bool((raw.get("pagination") or {}).get("hasNext")) if isinstance(raw, dict) else False)
        if since:
            fills = [f for f in fills if f.timestamp >= since]
        fills.sort(key=lambda f: f.timestamp, reverse=True)
        return Page(fills, next_cursor=str(page + 1) if more else None)

    # -- account --------------------------------------------------------------

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        """Outcome shares held, order book and AMM alike, netted per binary
        market on the YES leg."""
        address = self._need_address()
        rows: list[dict[str, Any]] = []
        page = 1
        while True:
            raw = await self._call("GET", f"/users/{address}/portfolio", params={
                "trading_model": "all", "page": page, "limit": 100,
            }) or {}
            rows += raw.get("data") or [] if isinstance(raw, dict) else []
            if not (isinstance(raw, dict) and (raw.get("pagination") or {}).get("hasNext")) or page >= 50:
                break
            page += 1
        many: set[tuple[int, int]] = set()
        for row in rows:
            if row.get("executionMode") == 0 and row.get("marketId") is not None:
                key = (int(row.get("networkId") or ORDER_BOOK_NETWORK), int(row["marketId"]))
                if key not in many and int(row.get("outcomeId") or 0) <= 1 and await self._many(*key):
                    many.add(key)
        positions = positions_of(rows, account=self.account, many=many)
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            positions = [p for p in positions if p.market_id == wanted]
        return [p for p in positions if p.inventory_yes or p.inventory_no]

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        """The wallet's USD1 on BNB Chain; what open buy orders reserve is `locked`."""
        address = self._need_address()
        data = "0x70a08231" + bytes.fromhex(address[2:]).rjust(32, b"\0").hex()
        answer = (await self.rpc.post(self.rpc_url, json={
            "jsonrpc": "2.0", "id": 1, "method": "eth_call", "params": [{"to": sig.USD1, "data": data}, "latest"],
        })).json()
        if not isinstance(answer, dict) or "result" not in answer:
            raise ExchangeError(f"{VENUE}: balance read failed: {answer.get('error') if isinstance(answer, dict) else answer}")
        total = Decimal(int(answer["result"], 16)) / SCALE
        locked = Decimal("0")
        for order in await self.fetch_open_orders():
            venue_side, outcome = order.info.get("venue_side"), order.info.get("outcome")
            if venue_side == "BUY" and order.price is not None:
                price = order.price if outcome == "yes" else Decimal("1") - order.price
                locked += (order.remaining or Decimal("0")) * price
        available = max(Decimal("0"), total - locked)
        return Balance(
            venue=VENUE, account=account or self.account, currency="USD1",
            total=total, available=available, locked=locked, buying_power=None,
            timestamp=int(time.time() * 1000), info={"address": address, "token": sig.USD1},
        )

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, quantity: Decimal) -> FeeEstimate:
        """The taker fee for `quantity` contracts at the YES `price`, on the
        outcome actually bought (`1 - price` for a `sell`). Makers pay nothing."""
        schedule = await asyncio.to_thread(self.catalog.fetch_fee_schedule, market_id)
        token_price = D(price) if side == Side.BUY else Decimal("1") - D(price)
        taker = schedule.estimate(float(token_price), float(quantity), taker=True)
        maker = schedule.estimate(float(token_price), float(quantity), taker=False)
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, ids.native(VENUE, market_id)), side=side,
            price=D(price), quantity=D(quantity),
            taker_fee=D(taker) if taker is not None else None, maker_fee=D(maker) if maker is not None else None,
            currency="USD1", info={"schedule": schedule.model_dump()},
        )

    # -- AMM markets ----------------------------------------------------------

    async def _many(self, network: int, market: int) -> bool:
        """Whether a pool has more than two outcomes (read once, kept)."""
        key = (network, market)
        if key not in self._outcome_counts:
            try:
                raw = await asyncio.to_thread(self.catalog.raw_market, native_id(network, market, amm=True))
                self._outcome_counts[key] = len(raw.get("outcomes") or [])
            except (MarketNotFound, BadRequest):
                return False
        return self._outcome_counts[key] > 2

    async def _rpc(self, network: int, method: str, params: list[Any]) -> Any:
        url = self.rpc_urls.get(network)
        if not url:
            raise BadRequest(f"{VENUE}: no node for network {network}; pass rpc_urls={{{network}: ...}}")
        answer = (await self.rpc.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})).json()
        if not isinstance(answer, dict) or answer.get("error") or "result" not in answer:
            error = answer.get("error") if isinstance(answer, dict) else answer
            raise ExchangeError(f"{VENUE}: network {network} rpc {method}: {error}", body=answer)
        return answer["result"]

    async def _eth_call(self, network: int, to: str, data: str) -> str:
        return str(await self._rpc(network, "eth_call", [{"to": to, "data": data}, "latest"]))

    async def _amm_market(self, market_id: str) -> tuple[Ref, dict[str, Any]]:
        """An AMM market's parts and the venue's record of it."""
        ref = await self._ref(market_id)
        if not ref.amm:
            raise InvalidOrder(f"{VENUE}: {ref.native} is an order-book market, not an AMM one")
        if ref.network not in sig.AMM_CONTRACTS:
            raise InvalidOrder(f"{VENUE}: AMM markets on network {ref.network} are not traded here")
        raw = await asyncio.to_thread(self.catalog.raw_market, ref.market_native)
        if not is_amm(raw):
            raise InvalidOrder(f"{VENUE}: {ref.native} is not an AMM market")
        self._outcome_counts[(ref.network, ref.market)] = len(raw.get("outcomes") or [])
        return ref, raw

    async def plan_amm_order(self, request: OrderRequest) -> AmmPlan:
        """Size an AMM order from the pool as it stands on chain, and build
        its call, without sending anything. `plan.fills` is false (with a
        `reason`) when the pool fills nothing within the worst price."""
        partial = amm_time_in_force(request)
        ref, raw = await self._amm_market(request.market_id)
        if str(raw.get("state") or "") != "open":
            raise InvalidOrder(f"{VENUE}: {ref.native} is {raw.get('state')}; a pool trades only while open")
        outcomes = raw.get("outcomes") or []
        index, action, worst = amm_leg(request, ref, len(outcomes))
        token = raw.get("token") or {}
        decimals = int(token.get("decimals") or 18)
        contract = sig.AMM_CONTRACTS[ref.network]
        scale = 10 ** decimals
        pools_units = sig.decode_uint_array(await self._eth_call(ref.network, contract, sig.amm_pools_calldata(ref.market)))
        if len(pools_units) != len(outcomes) or any(n <= 0 for n in pools_units):
            raise ExchangeError(f"{VENUE}: the pool of {ref.native} read back {pools_units}")
        pools = [n / scale for n in pools_units]
        buy_fee, sell_fee = amm_fees(raw)
        held: Decimal | None = None
        if action == "sell":
            address = self._need_address()
            shares = sig.decode_holdings(await self._eth_call(
                ref.network, contract, sig.amm_holdings_calldata(ref.market, address)))
            held = Decimal(shares[index] if index < len(shares) else 0) / scale
            if held < D(request.quantity):
                raise InsufficientFunds(f"{VENUE}: selling {request.quantity} of outcome {index} on {ref.native}, "
                                        f"but the wallet holds {held}")
        numbers = plan_amm_trade(pools, index=index, action=action, shares=D(request.quantity), worst=worst,
                                 buy_fee=buy_fee, sell_fee=sell_fee, decimals=decimals, partial=partial, held=held)
        plan = AmmPlan(
            network=ref.network, market=ref.market, outcome=index, action=action, worst=worst,
            requested=D(request.quantity), decimals=decimals, contract=contract, token=str(token.get("address") or ""),
            symbol=str(token.get("symbol") or ""), reason=numbers.get("reason"),
            info={"pools": pools, "buy_fee": buy_fee, "sell_fee": sell_fee, "partial": partial,
                  "outcomes": len(outcomes), "held": str(held) if held is not None else None},
        )
        if plan.reason is None:
            plan.shares, plan.value, plan.average, plan.fee = (numbers["shares"], numbers["value"], numbers["average"],
                                                               numbers["fee"])
            plan.value_units, plan.bound_units = numbers["value_units"], numbers["bound_units"]
            plan.calldata = (sig.amm_buy_calldata(ref.market, index, plan.bound_units, plan.value_units) if action == "buy"
                             else sig.amm_sell_calldata(ref.market, index, plan.value_units, plan.bound_units))
        return plan

    def _amm_order(self, request: OrderRequest, plan: AmmPlan, *, order_id: str, status: OrderStatus,
                   many: bool, info: dict[str, Any]) -> Order:
        return Order(
            id=order_id,
            client_order_id=request.client_order_id,
            venue=VENUE,
            account=request.account or self.account,
            market_id=market_id_of(plan.network, plan.market, amm=True, outcome=plan.outcome if many else None),
            side=request.side,
            type=request.type,
            time_in_force=request.time_in_force,
            status=status,
            price=D(request.price),
            quantity=D(request.quantity),
            filled=Decimal("0"),
            remaining=Decimal("0") if status != OrderStatus.PENDING else D(request.quantity),
            reduce_only=request.reduce_only,
            created_at=int(time.time() * 1000),
            book=request.book,
            trader=request.trader,
            tags=request.tags,
            info=info,
        )

    async def create_amm_order(self, request: OrderRequest) -> Order:
        """Trade against an AMM market's pool: one transaction from the
        wallet, sized from the pool on chain and bounded by the worst price
        (see `plan_amm_order`), then its receipt read for the fill. A buy
        checks the token's balance and its approval to the pool's contract
        first (`approve_amm`); a sell checks the shares held.

        `request.params`: `wait` (default true) waits for the receipt, up to
        `timeout_s` (default 120). An order not yet mined comes back
        `pending`, its id the transaction hash; `fetch_order` reads it again.
        When the pool fills nothing within the worst price, nothing is sent
        and the order comes back `canceled`, its id `unsent-...`."""
        signer = self._need_signer("trading on an AMM market")
        plan = await self.plan_amm_order(request)
        many = plan.info["outcomes"] > 2
        if not plan.fills:
            unsent = self._amm_order(request, plan, order_id=f"unsent-{secrets.token_hex(16)}",
                                     status=OrderStatus.CANCELED, many=many,
                                     info={"amm": True, "reason": plan.reason, "plan": self._plan_info(plan)})
            self._amm_orders[unsent.id] = (unsent, plan, many)
            return unsent
        if plan.action == "buy":
            balance = int(await self._eth_call(plan.network, plan.token, sig.balance_calldata(signer.address)), 16)
            if balance < plan.value_units:
                raise InsufficientFunds(f"{VENUE}: the trade spends {plan.value} {plan.symbol}; the wallet holds "
                                        f"{Decimal(balance) / (Decimal(10) ** plan.decimals)}")
            allowance = int(await self._eth_call(plan.network, plan.token, sig.allowance_calldata(
                signer.address, plan.contract)), 16)
            if allowance < plan.value_units:
                raise InsufficientFunds(f"{VENUE}: {plan.symbol} is not approved to the pool's contract for "
                                        f"{plan.value}; call approve_amm({request.market_id!r}) once")
        tx_hash = await self._send(plan.network, plan.contract, plan.calldata, what=f"the {plan.action}")
        order = self._amm_order(request, plan, order_id=tx_hash, status=OrderStatus.PENDING, many=many,
                                info={"amm": True, "tx_hash": tx_hash, "plan": self._plan_info(plan)})
        self._amm_orders[tx_hash.lower()] = (order, plan, many)
        if request.params.get("wait", True) is False:
            return order
        receipt = await self._receipt(plan.network, tx_hash, timeout_s=float(request.params.get("timeout_s", 120)))
        return self._settle_amm(order, plan, many, receipt) if receipt else order

    @staticmethod
    def _plan_info(plan: AmmPlan) -> dict[str, Any]:
        return {"outcome": plan.outcome, "action": plan.action, "worst": str(plan.worst), "shares": str(plan.shares),
                "value": str(plan.value), "average": str(plan.average) if plan.average is not None else None,
                "fee": str(plan.fee), "bound": str(plan.bound), "contract": plan.contract, "network": plan.network}

    def _settle_amm(self, order: Order, plan: AmmPlan, many: bool, receipt: dict[str, Any]) -> Order:
        """The order once its transaction is mined: filled from the receipt's
        `MarketActionTx`, or rejected when it reverted."""
        if int(str(receipt.get("status") or "0x0"), 16) != 1:
            settled = order.model_copy(update={"status": OrderStatus.REJECTED, "remaining": Decimal("0"), "info": {
                **order.info, "reason": "reverted: the pool moved past the worst price, or the funds changed",
                "receipt": receipt}})
            self._amm_orders[order.id.lower()] = (settled, plan, many)
            return settled
        wanted = sig.AMM_BUY if plan.action == "buy" else sig.AMM_SELL
        actions = [a for a in sig.market_actions(receipt, contract=plan.contract, user=self.address)
                   if a["market_id"] == plan.market and a["outcome"] == plan.outcome and a["action"] == wanted]
        fills = [amm_fill(a, tx_hash=order.id, plan=plan, many=many, order_id=order.id,
                          client_order_id=order.client_order_id, account=order.account) for a in actions]
        filled = sum((f.quantity for f in fills), Decimal("0"))
        value = sum((Decimal(f.info["value"]) for f in fills), Decimal("0"))
        fee = sum((f.fee or Decimal("0") for f in fills), Decimal("0"))
        average = None
        if filled > 0:
            average = value / filled if many or plan.outcome == 0 else Decimal("1") - value / filled
        whole = filled >= order.quantity * (1 - Decimal(str(AMM_MARGIN)))
        settled = order.model_copy(update={
            "status": OrderStatus.CLOSED if whole else OrderStatus.CANCELED, "filled": filled,
            "remaining": Decimal("0"), "average_price": average, "cost": value if plan.action == "buy" else None,
            "fee": fee, "fee_currency": plan.symbol or None,
            "last_fill_price": fills[-1].price if fills else None, "last_fill_quantity": fills[-1].quantity if fills else None,
            "updated_at": int(time.time() * 1000),
            "info": {**order.info, "fills": fills, "block": receipt.get("blockNumber"),
                     "gas_used": receipt.get("gasUsed")},
        })
        self._amm_orders[order.id.lower()] = (settled, plan, many)
        return settled

    async def _send(self, network: int, to: str, data: str, *, what: str) -> str:
        """Sign and send one transaction from the wallet; its hash. A call the
        node's simulation says would revert is refused before it is sent."""
        signer = self._need_signer("sending a transaction")
        async with self._tx_lock:
            nonce = int(await self._rpc(network, "eth_getTransactionCount", [signer.address, "pending"]), 16)
            gas_price = int(await self._rpc(network, "eth_gasPrice", []), 16)
            priority_fee = int(await self._rpc(network, "eth_maxPriorityFeePerGas", []), 16)
            try:
                estimate = int(await self._rpc(network, "eth_estimateGas", [{"from": signer.address, "to": to,
                                                                            "data": data}]), 16)
            except ExchangeError as exc:
                raise OrderRejected(f"{VENUE}: {what} would revert: {exc}", reason="simulation", body=exc.body) from None
            tx = {
                "chainId": network, "nonce": nonce, "to": to, "value": 0, "data": data, "gas": int(estimate * 1.2),
                "maxFeePerGas": gas_price * 2 + priority_fee, "maxPriorityFeePerGas": priority_fee, "type": 2,
            }
            return str(await self._rpc(network, "eth_sendRawTransaction", [signer.sign_transaction(tx)]))

    async def _receipt(self, network: int, tx_hash: str, *, timeout_s: float = 120.0,
                       poll_s: float = 1.0) -> dict[str, Any] | None:
        """The transaction's receipt once mined, or `None` after `timeout_s`."""
        loop = asyncio.get_running_loop()
        stop = loop.time() + timeout_s
        while True:
            receipt = await self._rpc(network, "eth_getTransactionReceipt", [tx_hash])
            if receipt:
                return receipt
            if loop.time() >= stop:
                return None
            await asyncio.sleep(poll_s)

    async def amm_allowance(self, market_id: str) -> Decimal:
        """What the network's pool contract may spend of an AMM market's
        token from this wallet, in tokens."""
        ref, raw = await self._amm_market(market_id)
        token = raw.get("token") or {}
        address = self._need_address()
        result = await self._eth_call(ref.network, str(token.get("address")), sig.allowance_calldata(
            address, sig.AMM_CONTRACTS[ref.network]))
        return Decimal(int(result, 16)) / (Decimal(10) ** int(token.get("decimals") or 18))

    async def approve_amm(self, market_id: str, amount: Decimal | None = None, *, wait: bool = True,
                          timeout_s: float = 120.0) -> dict[str, Any]:
        """Approve an AMM market's token to its network's pool contract, once
        per token and network: unlimited by default, or `amount` tokens. One
        transaction from the wallet, which pays its gas."""
        ref, raw = await self._amm_market(market_id)
        token = raw.get("token") or {}
        value = MAX_UINT256 if amount is None else _units(D(amount), int(token.get("decimals") or 18), ROUND_CEILING)
        contract = sig.AMM_CONTRACTS[ref.network]
        tx_hash = await self._send(ref.network, str(token.get("address")), sig.approve_calldata(contract, value),
                                   what="the approval")
        if not wait:
            return {"transaction_hash": tx_hash}
        receipt = await self._receipt(ref.network, tx_hash, timeout_s=timeout_s)
        if receipt is None:
            raise ExchangeError(f"{VENUE}: approval {tx_hash} unmined after {timeout_s}s; it may yet confirm")
        if int(str(receipt.get("status") or "0x0"), 16) != 1:
            raise OrderRejected(f"{VENUE}: approval {tx_hash} reverted", reason="reverted", info=receipt)
        return {"transaction_hash": tx_hash, "receipt": receipt}

    async def redeem_amm(self, market_id: str, *, wait: bool = True, timeout_s: float = 120.0) -> dict[str, Any]:
        """Claim a resolved AMM market's payout: the winning outcome's shares
        (`claimWinnings`), or, on a voided market, each outcome held
        (`claimVoidedOutcomeShares`). One transaction each from the wallet."""
        ref, raw = await self._amm_market(market_id)
        if str(raw.get("state") or "") != "resolved":
            raise InvalidOrder(f"{VENUE}: {ref.native} is {raw.get('state')}; only a resolved market pays out")
        contract = sig.AMM_CONTRACTS[ref.network]
        held = sig.decode_holdings(await self._eth_call(ref.network, contract, sig.amm_holdings_calldata(
            ref.market, self._need_address())))
        if raw.get("voided"):
            calls = [sig.amm_claim_voided_calldata(ref.market, k) for k, n in enumerate(held) if n > 0]
        else:
            winner = int(raw.get("resolvedOutcomeId") if raw.get("resolvedOutcomeId") is not None else -1)
            calls = [sig.amm_claim_calldata(ref.market)] if 0 <= winner < len(held) and held[winner] > 0 else []
        if not calls:
            raise InvalidOrder(f"{VENUE}: the wallet holds nothing on {ref.native} to claim")
        hashes = [await self._send(ref.network, contract, data, what="the claim") for data in calls]
        if not wait:
            return {"transaction_hashes": hashes}
        receipts = []
        for tx_hash in hashes:
            receipt = await self._receipt(ref.network, tx_hash, timeout_s=timeout_s)
            if receipt is None:
                raise ExchangeError(f"{VENUE}: claim {tx_hash} unmined after {timeout_s}s; it may yet confirm")
            if int(str(receipt.get("status") or "0x0"), 16) != 1:
                raise OrderRejected(f"{VENUE}: claim {tx_hash} reverted", reason="reverted", info=receipt)
            receipts.append(receipt)
        scale = Decimal(10) ** int((raw.get("token") or {}).get("decimals") or 18)
        paid = sum((Decimal(int(a["value"])) / scale for r in receipts
                    for a in sig.market_actions(r, contract=contract, user=self.address)), Decimal("0"))
        return {"transaction_hashes": hashes, "receipts": receipts, "paid": paid}

    # -- on-chain helpers -----------------------------------------------------

    def approval_calls(self) -> list[dict[str, str]]:
        """The two one-time transactions the wallet sends before trading: USD1
        approved to the exchange (buys) and the outcome shares (sells)."""
        return sig.approval_calls()

    async def position_calldata(self, action: str, market_id: str, amount: Decimal | None = None) -> dict[str, Any]:
        """The venue's `{to, calldata, value}` for `split` (collateral into
        YES and NO), `merge` (back), `redeem` or `redeem-voided` on a market,
        for the wallet to send. `amount` is collateral or shares, in tokens."""
        if action not in ("split", "merge", "redeem", "redeem-voided"):
            raise BadRequest(f"{VENUE}: action is split, merge, redeem or redeem-voided, not {action!r}")
        ref = await self._ob_ref(market_id)
        body: dict[str, Any] = {"market_id": ref.market, "network_id": ref.network}
        if action in ("split", "merge"):
            if amount is None:
                raise BadRequest(f"{VENUE}: {action} needs an amount")
            body["amount"] = str(sig.to_wei(D(amount)))
        return await self._call("POST", f"/positions/{action}", body=body) or {}

    async def close(self) -> None:
        await self.http.close()
        await self.rpc.aclose()
        if self._own_catalog:
            self.catalog.close()
