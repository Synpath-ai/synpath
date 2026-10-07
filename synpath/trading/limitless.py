"""Limitless order entry, and the venue's order and trade records on the YES leg.

Built to Limitless's documentation and its official SDK (`limitless-sdk`
1.1.1): order hashing and signatures are checked against independent EIP-712
encoders and the SDK's own amount rules; requests follow the venue's OpenAPI.

**Two credentials.** Every request is signed with a scoped API token (an id
and a base64 secret, derived on limitless.exchange); every order is also an
EIP-712 message signed by the wallet itself. The profile must be in EOA
trading mode -- an account that once enabled the web app's one-click smart
wallet rejects self-signed orders until it is switched back
(`LimitlessTrading.use_eoa_trading_mode()`).

**YES and NO are separate tokens.** `buy` buys the YES token at the price
given; `sell` buys the NO token at `1 - price`; with `reduce_only`, `sell`
sells YES held and `buy` sells NO held. Everything the venue reports on the
NO token comes back on the YES leg.

**Limits rest; market orders take.** A limit is `GTC`, post-only on request.
A market order, or an IOC limit, is the venue's fill-and-kill (`FAK`) at the
price given as the worst accepted. The venue's fill-or-kill spends a USDC
amount with no price limit, so `fok` is refused, as are `gtd` and `day`
(orders cannot expire). Prices take at most three decimals, between 0.01 and
0.99; sizes are counted in thousandths of a share.

**Fees** are taker-only, a share of what the taker receives that falls with
the price; the order carries the profile's fee rate, which the venue checks.

Two sources report on an order, in no fixed order between them:

* **The matching engine (`OME`)**: the order resting (`PLACEMENT`), its
  remaining size changing (`UPDATE`), its removal (`CANCELLATION`), and the
  one terminal result of an immediate order (`EXECUTION`: filled, partly
  filled with the rest cancelled, or killed).
* **Settlement**: a fill arrives first as `MATCHED`, the moment the engine
  fills it and before the chain does (`SettlementState.MATCHED`, its fee an
  estimate), then as `MINED` (`CONFIRMED`) or `FAILED`, under the same id.

Only the taker pays a fee; a maker's frame carries its order's configured rate
for information, and is reported here as no fee.

Approvals (the one-time "enable trading": USDC and the outcome tokens to the
market's exchange), split, merge and redeem are not built here.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

from .. import ids
from ..base import AsyncHttpClient, Capability
from ..errors import AuthenticationError, BadRequest, ExchangeError, MarketNotFound
from ..limitless import iso_ms
from ..types import Page
from . import limitless_signing as sig
from .base import TradingExchange
from .credentials import LimitlessCredentials
from .errors import CredentialsMissing, InsufficientFunds, InvalidOrder, OrderNotFound, OrderRejected
from .limiter import BudgetLimiter, Priority
from .polymarket import to_yes_leg, wire_leg
from .polymarket_signing import WalletSigner
from .types import (
    Account, Balance, FeeEstimate, Fill, Liquidity, Order, OrderRequest, OrderStatus, OrderType, Position,
    PositionSide, SettlementState, Side, TimeInForce, VENUE_ORDER_TYPES,
)

VENUE = "limitless"
SCALE = Decimal(1_000_000)
API_URL = "https://api.limitless.exchange"
RPC_URL = "https://mainnet.base.org"

ROW_STATUS = {"LIVE": OrderStatus.OPEN, "MATCHED": OrderStatus.CLOSED, "CANCELED": OrderStatus.CANCELED,
              "UNMATCHED": OrderStatus.CANCELED}
"""An order row's status. `UNMATCHED` is an immediate order that matched nothing."""

BATCH = 50
"""Order ids per batch cancel or status lookup: the venue's maximum."""

FILL_SETTLEMENT = {
    "MATCHED": SettlementState.MATCHED,
    "MINED": SettlementState.CONFIRMED,
    "FAILED": SettlementState.FAILED,
}

EXECUTION_STATUS = {
    "FILLED": OrderStatus.CLOSED,
    "PARTIALLY_FILLED": OrderStatus.CANCELED,
    "KILLED": OrderStatus.CANCELED,
}
"""An immediate order's terminal result: a partial fill's rest is cancelled."""


def D(value: Any) -> Decimal:
    return Decimal(str(value)) if value not in (None, "") else Decimal("0")


def _stamp(event: dict[str, Any]) -> int | None:
    return iso_ms(event.get("occurredAt") or event.get("timestamp"))


def order_of_event(
    event: dict[str, Any], *, market_id: str, outcome: str, account: Account | None = None,
) -> Order:
    """A matching-engine event as the order it describes, on the YES leg.

    `outcome` is which token the order is on ("yes" or "no"); the venue names
    the token by its id, which the caller resolves. A lifecycle frame carries
    what is left of the order, not what it started as, so `amount` is that
    remainder; the terminal frame of an immediate order carries what was left
    unfilled, in 6-decimal units."""
    kind = str(event.get("type") or "")
    price = D(event.get("price")) if event.get("price") not in (None, "") else None
    side, yes_price = to_yes_leg(outcome, str(event.get("side") or "BUY"), price)
    if kind == "EXECUTION":
        remaining = D(event.get("remainingSize")) / SCALE
        status = EXECUTION_STATUS.get(str(event.get("status") or ""), OrderStatus.CANCELED)
        order_type, tif = OrderType.MARKET, TimeInForce.IOC
    else:
        remaining = D(event.get("remainingSize"))
        status = OrderStatus.CANCELED if kind == "CANCELLATION" else OrderStatus.OPEN
        order_type, tif = OrderType.LIMIT, TimeInForce.GTC
    stamp = _stamp(event)
    return Order(
        id=str(event.get("orderId") or ""),
        client_order_id=event.get("clientOrderId") or None,
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, ids.native(VENUE, market_id)),
        side=side,
        type=order_type,
        time_in_force=tif,
        status=status,
        price=yes_price,
        amount=remaining,
        filled=Decimal("0"),
        remaining=remaining if status == OrderStatus.OPEN else Decimal("0"),
        updated_at=stamp,
        info={**event, "native": f"{event.get('source')}:{kind}"},
    )


def fill_of_event(
    event: dict[str, Any], *, market_id: str, outcome: str, account: Account | None = None,
) -> Fill | None:
    """A settlement event as a fill on the YES leg, or `None` for any other
    event. Its id is the trade's and the order's together, the same across
    `MATCHED` and the terminal event; the fill carries how far it has
    settled."""
    settlement = FILL_SETTLEMENT.get(str(event.get("type") or ""))
    if event.get("source") != "SETTLEMENT" or settlement is None:
        return None
    side, price = to_yes_leg(outcome, str(event.get("side") or "BUY"), D(event.get("price")))
    taker = bool(event.get("orderId")) and event.get("orderId") == event.get("takerOrderId")
    fee: Decimal | None = None
    currency: str | None = None
    if taker:
        if event.get("feeAmountCollateral") not in (None, ""):
            fee, currency = D(event["feeAmountCollateral"]), "USDC"
        elif event.get("feeAmountContracts") not in (None, ""):
            fee, currency = D(event["feeAmountContracts"]), "shares"
    else:
        fee, currency = Decimal("0"), "USDC"
    trade = str(event.get("tradeEventId") or event.get("txHash") or "")
    return Fill(
        id=f"{trade}:{event.get('orderId') or ''}",
        order_id=str(event.get("orderId") or ""),
        client_order_id=event.get("clientOrderId") or None,
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, ids.native(VENUE, market_id)),
        side=side,
        price=price or Decimal("0"),
        amount=D(event.get("amountContracts")),
        fee=fee,
        fee_currency=currency,
        liquidity=Liquidity.TAKER if taker else Liquidity.MAKER,
        settlement=settlement,
        timestamp=iso_ms(event.get("matchedAt") or event.get("timestamp")) or 0,
        info=event,
    )


# ---------------------------------------------------------------------------
# REST records
# ---------------------------------------------------------------------------

@dataclass
class MarketInfo:
    """What signing an order for one market needs."""

    slug: str
    yes_token: str
    no_token: str
    exchange: str
    fee: bool
    """Whether the market charges fees; a free market's orders carry a zero rate."""

    @property
    def market_id(self) -> str:
        return ids.qualify(VENUE, self.slug)

    def token(self, outcome: str) -> str:
        return self.yes_token if outcome == "yes" else self.no_token

    def outcome_of(self, token_id: Any) -> str:
        return "no" if str(token_id) == self.no_token else "yes"


def market_info_of(raw: dict[str, Any]) -> MarketInfo:
    """A venue market as what its orders are signed against."""
    tokens = raw.get("tokens") or {}
    venue = raw.get("venue") or {}
    slug = str(raw.get("slug") or "")
    if raw.get("tradeType") not in (None, "clob") or not tokens.get("yes") or not tokens.get("no") or not venue.get("exchange"):
        raise InvalidOrder(f"{VENUE}: market {slug} has no order book to trade on")
    return MarketInfo(
        slug=slug, yes_token=str(tokens["yes"]), no_token=str(tokens["no"]), exchange=str(venue["exchange"]),
        fee=(raw.get("metadata") or {}).get("fee", True) is not False,
    )


def order_of_row(row: dict[str, Any], info: MarketInfo, *, account: Account | None = None) -> Order:
    """An order row (`/markets/{slug}/user-orders`) as an `Order` on the YES leg."""
    outcome = info.outcome_of(row.get("token"))
    price = D(row.get("price")) if row.get("price") not in (None, "") else None
    side, yes_price = to_yes_leg(outcome, str(row.get("side") or "BUY"), price)
    status = ROW_STATUS.get(str(row.get("status") or "").upper(), OrderStatus.OPEN)
    amount, remaining = D(row.get("originalSize")) / SCALE, D(row.get("remainingSize")) / SCALE
    kind = str(row.get("type") or "GTC").upper()
    return Order(
        id=str(row.get("id") or ""),
        client_order_id=row.get("clientOrderId") or None,
        venue=VENUE,
        account=account,
        market_id=info.market_id,
        side=side,
        type=OrderType.LIMIT if kind == "GTC" else OrderType.MARKET,
        time_in_force=TimeInForce.GTC if kind == "GTC" else TimeInForce.IOC,
        status=status,
        price=yes_price,
        amount=amount,
        filled=max(Decimal("0"), amount - remaining),
        remaining=remaining if status == OrderStatus.OPEN else Decimal("0"),
        created_at=iso_ms(row.get("createdAt")),
        info=row,
    )


def fill_of_history(row: dict[str, Any], *, account: Account | None = None) -> Fill | None:
    """A trade in the account's history as a settled fill on the YES leg, or
    `None` for anything else in it (splits, merges, conversions, redemptions)."""
    strategy = str(row.get("strategy") or "")
    if not row.get("orderId") or not row.get("tradeEventId") or not ("Buy" in strategy or "Sell" in strategy):
        return None
    outcome = "no" if row.get("outcomeIndex") == 1 else "yes"
    side, price = to_yes_leg(outcome, "BUY" if "Buy" in strategy else "SELL", D(row.get("outcomeTokenPrice")))
    market = row.get("market") or {}
    stamp = int(row.get("blockTimestamp") or 0) * 1000
    return Fill(
        id=f"{row['tradeEventId']}:{row['orderId']}",
        order_id=str(row["orderId"]),
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, str(market.get("slug") or "")),
        side=side,
        price=price or Decimal("0"),
        amount=D(row.get("outcomeTokenAmount")),
        fee=None,
        fee_currency=None,
        liquidity=Liquidity.UNKNOWN,
        settlement=SettlementState.CONFIRMED,
        timestamp=stamp,
        info={k: v for k, v in row.items() if k != "market"},
    )


def position_of(entry: dict[str, Any], *, account: Account | None = None) -> Position:
    """One market's holding (`/portfolio/positions`, `clob[]`): the two token
    balances netted on the YES leg, and both kept."""
    market = entry.get("market") or {}
    balances = entry.get("tokensBalance") or {}
    held_yes, held_no = D(balances.get("yes")) / SCALE, D(balances.get("no")) / SCALE
    net = held_yes - held_no
    lead = "yes" if net >= 0 else "no"
    legs = entry.get("positions") or {}
    entry_raw = (legs.get(lead) or {}).get("fillPrice")
    entry_price = D(entry_raw) / SCALE if entry_raw not in (None, "", "0") else None
    if entry_price is not None and lead == "no":
        entry_price = Decimal("1") - entry_price
    latest = entry.get("latestTrade") or {}
    mark = D(latest["latestYesPrice"]) if latest.get("latestYesPrice") not in (None, "") else None
    pnl = [legs[k].get("unrealizedPnl") for k in ("yes", "no") if isinstance(legs.get(k), dict) and legs[k].get("unrealizedPnl") not in (None, "")]
    realised = [legs[k].get("realisedPnl") for k in ("yes", "no") if isinstance(legs.get(k), dict) and legs[k].get("realisedPnl") not in (None, "")]
    resolved = str(market.get("status") or "").upper() == "RESOLVED"
    winner = market.get("winningOutcomeIndex")
    won_held = (held_yes if winner == 0 else held_no if winner == 1 else Decimal("0"))
    return Position(
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, str(market.get("slug") or "")),
        side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
        contracts=abs(net),
        inventory_yes=held_yes,
        inventory_no=held_no,
        entry_price=entry_price,
        mark_price=mark,
        unrealized_pnl=sum((D(v) for v in pnl), Decimal("0")) / SCALE if pnl else None,
        realized_pnl=sum((D(v) for v in realised), Decimal("0")) / SCALE if realised else None,
        resolved=resolved,
        final=resolved and winner is not None,
        won=(won_held > 0) if resolved and winner is not None else None,
        redeemable=won_held if resolved and winner is not None and won_held > 0 else None,
        info={"orders": entry.get("orders"), "market": {k: market.get(k) for k in ("slug", "title", "status", "id")}},
    )


def reason_of(body: Any, fallback: str) -> str:
    if isinstance(body, dict):
        message = body.get("message")
        if isinstance(message, list):
            return "; ".join(str(m.get("message") if isinstance(m, dict) else m) for m in message)
        if message:
            return str(message)
    return fallback


def error_of(exc: ExchangeError) -> ExchangeError:
    """The venue's refusal of an order as a typed error, by its words."""
    text = reason_of(exc.body, str(exc))
    lower = text.lower()
    if "balance" in lower or "allowance" in lower or "insufficient" in lower:
        return InsufficientFunds(f"{VENUE}: {text}", body=exc.body)
    if "signer does not match" in lower or "maker does not match" in lower:
        return CredentialsMissing(
            f"{VENUE}: {text} -- the profile must be in EOA trading mode; call use_eoa_trading_mode() once")
    if "fee_rate_mismatch" in lower or "fee rate" in lower:
        return OrderRejected(f"{VENUE}: {text}", reason="fee_rate_mismatch", body=exc.body)
    if any(word in lower for word in ("price", "amount", "size", "tick", "invalid")):
        return InvalidOrder(f"{VENUE}: {text}")
    return OrderRejected(f"{VENUE}: {text}", reason=None, info=exc.body if isinstance(exc.body, dict) else {}, body=exc.body)


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

def immediate(request: OrderRequest) -> bool:
    """A market order or an IOC limit: the venue's fill-and-kill."""
    return request.type == OrderType.MARKET or request.time_in_force == TimeInForce.IOC


def check_request(request: OrderRequest) -> None:
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"{VENUE}: {request.type.value} is held by the execution engine, not the venue; submit it through the engine"
        )
    if request.time_in_force == TimeInForce.FOK:
        raise InvalidOrder(f"{VENUE}: the venue's fill-or-kill spends a USDC amount with no price limit; use ioc")
    if request.time_in_force in (TimeInForce.GTD, TimeInForce.DAY):
        raise InvalidOrder(f"{VENUE}: orders cannot expire on this venue; gtc, or ioc")
    if request.price is None:
        raise InvalidOrder(f"{VENUE}: a price is required -- a market order is sent at the worst price you give")
    if request.post_only and immediate(request):
        raise InvalidOrder(f"{VENUE}: a post-only order cannot also take at once")
    if request.client_order_id and len(request.client_order_id) > 128:
        raise InvalidOrder(f"{VENUE}: client_order_id is at most 128 characters")


def build_signed_order(
    request: OrderRequest, info: MarketInfo, *, signer: WalletSigner, owner_id: int, fee_rate_bps: int,
    salt: int | None = None,
) -> tuple[dict[str, Any], Decimal]:
    """The `POST /orders` body and the contracts it is for, before anything is
    sent. Sizes are cut to thousandths of a share."""
    check_request(request)
    outcome, venue_side = wire_leg(request)
    yes_price = D(request.price)
    price = yes_price if outcome == "yes" else Decimal("1") - yes_price
    maker_amount, taker_amount, units = sig.limit_amounts(venue_side, price, D(request.amount))  # type: ignore[arg-type]
    order = sig.build_order(
        maker=signer.address, token_id=info.token(outcome), maker_amount=maker_amount, taker_amount=taker_amount,
        side=venue_side, fee_rate_bps=fee_rate_bps if info.fee else 0,  # type: ignore[arg-type]
        salt=salt if salt is not None else sig.new_salt(),
    )
    signature = sig.sign_order(signer, order, exchange=info.exchange)
    body: dict[str, Any] = {
        "order": sig.wire_order(order, signature=signature, price=price),
        "ownerId": int(owner_id),
        "orderType": "FAK" if immediate(request) else "GTC",
        "marketSlug": info.slug,
    }
    if request.post_only:
        body["postOnly"] = True
    if request.client_order_id:
        body["clientOrderId"] = request.client_order_id
    return body, Decimal(units) / SCALE


def status_after_placing(settlement: str, *, taking: bool, filled: Decimal, amount: Decimal) -> OrderStatus:
    """An order's state from the placement's settlement status."""
    if settlement in ("CANCELED", "FAILED"):
        return OrderStatus.REJECTED if settlement == "FAILED" and filled == 0 else OrderStatus.CANCELED
    if amount > 0 and filled >= amount:
        return OrderStatus.CLOSED
    if taking and settlement not in ("DELAYED",):
        return OrderStatus.CANCELED if filled < amount else OrderStatus.CLOSED
    return OrderStatus.OPEN


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class LimitlessTrading(TradingExchange):
    """Limitless order entry.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.limitless import LimitlessTrading

    async with LimitlessTrading(require("limitless", load_credentials())) as lmts:
        print(await lmts.fetch_balance())
    ```
    """

    id = VENUE
    name = "Limitless"
    has: dict[str, Capability] = {
        "create_order": True,
        # No batch endpoint: one request per order.
        "create_orders": False,
        "cancel_order": True,
        # Up to 50 a request.
        "cancel_orders": True,
        "cancel_all_orders": True,
        "edit_order": False,
        "fetch_order": True,
        "fetch_open_orders": True,
        # Per market: the venue lists orders market by market.
        "fetch_orders": "partial",
        "fetch_my_trades": True,
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
        credentials: LimitlessCredentials,
        *,
        account_name: str = "default",
        url: str = API_URL,
        rpc_url: str | None = None,
        catalog: Any = None,
        limiter: BudgetLimiter | None = None,
        timeout: float = 30.0,
        client: Any = None,
        rpc_client: Any = None,
    ):
        import httpx

        self.credentials = credentials
        self.signer = WalletSigner(credentials.private_key)
        self.account = Account(venue=VENUE, name=account_name)
        self.limiter = limiter or BudgetLimiter(read_per_second=4, write_per_second=4)
        shared = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        self.http = AsyncHttpClient(url, limiter=None, client=shared, venue=VENUE)
        self.rpc_url = rpc_url or credentials.rpc_url or RPC_URL
        self.rpc = rpc_client or httpx.AsyncClient(timeout=20, follow_redirects=True)
        self._own_catalog = catalog is None
        if catalog is None:
            from ..limitless import Limitless

            catalog = Limitless()
        self.catalog = catalog
        """The read adapter: tokens, exchange and fee flag for a market."""
        self._markets: dict[str, MarketInfo] = {}
        self._profile: dict[str, Any] | None = None

    # -- plumbing -------------------------------------------------------------

    async def _call(
        self, method: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None,
        kind: str = "read", priority: Priority = Priority.NORMAL,
    ) -> Any:
        """One signed call, paced. The query string and the body are part of
        what is signed, so both are built here exactly as sent."""
        await self.limiter.acquire(cost=1, kind=kind, priority=priority)  # type: ignore[arg-type]
        query = urlencode([(k, v) for k, v in (params or {}).items() if v is not None], doseq=True)
        target = f"{path}?{query}" if query else path
        text = json.dumps(body, separators=(",", ":"), ensure_ascii=False) if body is not None else ""
        headers = sig.request_headers(self.credentials.token_id, self.credentials.secret, method, target, text)
        if text:
            headers["Content-Type"] = "application/json"
        try:
            return await self.http.request(method, target, content=text.encode() if text else None, headers=headers)
        except MarketNotFound as exc:
            if path.startswith("/orders"):
                raise OrderNotFound(str(exc), body=exc.body, status=exc.status) from None
            raise

    async def profile(self, *, fresh: bool = False) -> dict[str, Any]:
        """The token's profile: its id (the orders' owner), wallet, trading
        mode and fee rate. Read once and kept."""
        if self._profile is None or fresh:
            self._profile = await self._call("GET", "/profiles/me") or {}
        return self._profile

    async def _owner(self) -> tuple[int, int]:
        """`(owner id, fee rate)`, after checking the profile can take this
        wallet's own signatures."""
        profile = await self.profile()
        if str(profile.get("account") or "").lower() != self.signer.address.lower():
            raise CredentialsMissing(
                f"{VENUE}: the API token belongs to {profile.get('account')}, not this wallet ({self.signer.address})")
        if profile.get("tradeWalletOption") not in (None, "eoa"):
            raise CredentialsMissing(
                f"{VENUE}: the profile trades through the web app's smart wallet, which rejects orders this wallet "
                f"signs; switch it with use_eoa_trading_mode() (reversible from the web app)")
        rank = profile.get("rank") or {}
        return int(profile["id"]), int(rank.get("feeRateBps") or 0)

    async def use_eoa_trading_mode(self) -> None:
        """Switch the profile to EOA trading mode, so orders this wallet signs
        are accepted. The web app's one-click trading stops until it is
        switched back there."""
        await self._call("PUT", "/profiles", body={"tradeWalletOption": "eoa"}, kind="write")
        await self.profile(fresh=True)

    async def market_info(self, market_id: str) -> MarketInfo:
        """Tokens, exchange and fee flag for a market: one catalog read, kept."""
        slug = ids.native(VENUE, market_id)
        cached = self._markets.get(slug)
        if cached is None:
            cached = market_info_of(await asyncio.to_thread(self.catalog._raw, slug))
            self._markets[slug] = cached
        return cached

    # -- orders ---------------------------------------------------------------

    async def create_order(self, request: OrderRequest) -> Order:
        """Sign and place one order. The venue answers with what matched at
        once; an immediate order's rest is cancelled by the venue."""
        check_request(request)
        info = await self.market_info(request.market_id)
        owner, fee_rate = await self._owner()
        body, contracts = build_signed_order(request, info, signer=self.signer, owner_id=owner, fee_rate_bps=fee_rate)
        try:
            raw = await self._call("POST", "/orders", body=body, kind="write") or {}
        except (BadRequest, OrderNotFound, AuthenticationError) as exc:
            if isinstance(exc, AuthenticationError):
                raise
            raise error_of(exc) from None
        placed = raw.get("order") or {}
        execution = raw.get("execution") or {}
        totals = execution.get("totalsRaw") or {}
        filled = D(totals.get("contractsGross")) / SCALE
        taking = immediate(request)
        settlement = str(execution.get("settlementStatus") or "")
        status = status_after_placing(settlement, taking=taking, filled=filled, amount=contracts)
        return Order(
            id=str(placed.get("id") or ""),
            client_order_id=request.client_order_id,
            venue=VENUE,
            account=request.account or self.account,
            market_id=info.market_id,
            side=request.side,
            type=request.type,
            time_in_force=request.time_in_force,
            status=status,
            price=D(request.price),
            amount=contracts,
            filled=filled,
            remaining=max(Decimal("0"), contracts - filled) if status == OrderStatus.OPEN else Decimal("0"),
            reduce_only=request.reduce_only,
            post_only=request.post_only,
            created_at=int(time.time() * 1000),
            book=request.book,
            trader=request.trader,
            tags=request.tags,
            info={"execution": execution, "venue_order": {k: v for k, v in placed.items() if k != "signature"},
                  "request": {**body, "order": {k: v for k, v in body["order"].items() if k != "signature"}}},
        )

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        """Cancel one order and read it back."""
        await self._call("POST", "/orders/cancel", body={"orderId": order_id}, kind="write", priority=Priority.HIGH)
        return await self.fetch_order(order_id)

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        """Cancel up to 50 orders a request, then read each back."""
        failed: dict[str, str] = {}
        for start in range(0, len(order_ids), BATCH):
            raw = await self._call("POST", "/orders/batch-cancel", body={"orderIds": order_ids[start:start + BATCH]},
                                   kind="write", priority=Priority.HIGH) or {}
            for item in raw.get("failed") or []:
                if isinstance(item, dict) and item.get("orderId"):
                    failed[str(item["orderId"])] = str(item.get("message") or item.get("reason") or "refused")
        out: list[Order | Exception] = []
        for order_id in order_ids:
            try:
                order = await self.fetch_order(order_id)
            except OrderNotFound as exc:
                out.append(exc)
                continue
            if order_id in failed and order.status == OrderStatus.OPEN:
                out.append(OrderRejected(f"{VENUE}: cancel of {order_id} refused: {failed[order_id]}", reason="cancel_refused"))
                continue
            out.append(order)
        return out

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int:
        """Every open order on a market, or on every market the account has
        orders on. Returns how many the venue cancelled."""
        slugs = [ids.native(VENUE, market_id)] if market_id else await self._order_markets()
        cancelled = 0
        for slug in slugs:
            raw = await self._call("DELETE", f"/orders/all/{slug}", kind="write", priority=Priority.HIGH) or {}
            cancelled += len(raw.get("canceled") or [])
        return cancelled

    async def _rows(self, slug: str, *, statuses: list[str] | None = None) -> list[dict[str, Any]]:
        raw = await self._call("GET", f"/markets/{slug}/user-orders", params={"statuses": statuses, "limit": 200})
        return list(raw or []) if isinstance(raw, list) else []

    async def fetch_order(self, order_id: str) -> Order:
        """One order by id: the venue's status lookup names its market, whose
        order list gives its state."""
        raw = await self._call("POST", "/orders/status/batch", body={"items": [{"orderId": order_id}]}) or {}
        result = next(iter(raw.get("results") or []), {}) if isinstance(raw, dict) else {}
        data = result.get("data") or {}
        placed = data.get("order") or {}
        slug = (placed.get("market") or {}).get("slug")
        if result.get("status") != "found" or not slug:
            raise OrderNotFound(f"{VENUE}: no order {order_id}")
        info = await self.market_info(ids.qualify(VENUE, slug))
        for row in await self._rows(slug):
            if str(row.get("id")) == order_id:
                return order_of_row(row, info, account=self.account)
        raise OrderNotFound(f"{VENUE}: order {order_id} is not in its market's order list")

    async def fetch_orders(
        self, *, status: str | None = None, market_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Order]:
        """This account's orders on one market, newest first (the venue lists
        orders market by market). `status` is `open` or `closed`."""
        if not market_id:
            raise BadRequest(f"{VENUE}: the venue lists orders per market; pass market_id (fetch_open_orders spans every market)")
        statuses = {"open": ["LIVE"], "closed": ["MATCHED", "CANCELED", "UNMATCHED"]}.get(status or "", None)
        if status and statuses is None:
            raise BadRequest(f"{VENUE}: status is open or closed, not {status!r}")
        info = await self.market_info(market_id)
        orders = [order_of_row(row, info, account=self.account) for row in await self._rows(info.slug, statuses=statuses)]
        if since:
            orders = [o for o in orders if (o.created_at or since) >= since]
        return Page(orders[:limit] if limit else orders, next_cursor=None)

    async def _order_markets(self) -> list[str]:
        """The markets the account has open orders on, from its positions."""
        raw = await self._call("GET", "/portfolio/positions") or {}
        slugs = []
        for entry in raw.get("clob") or []:
            orders = entry.get("orders") or {}
            if orders.get("liveOrders") or D(orders.get("totalCollateralLocked")) > 0:
                slug = (entry.get("market") or {}).get("slug")
                if slug:
                    slugs.append(str(slug))
        return slugs

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        slugs = [ids.native(VENUE, market_id)] if market_id else await self._order_markets()
        orders: list[Order] = []
        for slug in slugs:
            info = await self.market_info(ids.qualify(VENUE, slug))
            orders += [order_of_row(row, info, account=self.account) for row in await self._rows(slug, statuses=["LIVE"])]
        return orders

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        """One page of this account's settled fills, newest first, from its
        history; splits, merges and redemptions are left out."""
        raw = await self._call("GET", "/portfolio/history", params={
            "limit": min(limit or 50, 100), "cursor": cursor,
            "market": ids.native(VENUE, market_id) if market_id else None,
        }) or {}
        fills = [f for f in (fill_of_history(row, account=self.account) for row in raw.get("data") or []) if f is not None]
        if order_id:
            fills = [f for f in fills if f.order_id == order_id]
        if since:
            fills = [f for f in fills if f.timestamp >= since]
        return Page(fills, next_cursor=raw.get("nextCursor") or None)

    # -- account --------------------------------------------------------------

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        """The wallet's USDC on Base; what open orders hold is `locked`."""
        address = self.signer.address
        data = "0x70a08231" + bytes.fromhex(address[2:]).rjust(32, b"\0").hex()
        answer = (await self.rpc.post(self.rpc_url, json={
            "jsonrpc": "2.0", "id": 1, "method": "eth_call", "params": [{"to": sig.USDC, "data": data}, "latest"],
        })).json()
        if not isinstance(answer, dict) or "result" not in answer:
            raise ExchangeError(f"{VENUE}: balance read failed: {answer.get('error') if isinstance(answer, dict) else answer}")
        total = Decimal(int(answer["result"], 16)) / SCALE
        raw = await self._call("GET", "/portfolio/positions") or {}
        locked = sum((D((e.get("orders") or {}).get("totalCollateralLocked")) for e in raw.get("clob") or []), Decimal("0")) / SCALE
        available = max(Decimal("0"), total - locked)
        return Balance(
            venue=VENUE, account=account or self.account, currency="USDC",
            total=total, available=available, locked=locked, buying_power=available,
            timestamp=int(time.time() * 1000), info={"address": address},
        )

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        """Token inventories, netted per market on the YES leg."""
        raw = await self._call("GET", "/portfolio/positions") or {}
        positions = [position_of(entry, account=self.account) for entry in raw.get("clob") or []]
        if market_id:
            positions = [p for p in positions if p.market_id == ids.qualify(VENUE, ids.native(VENUE, market_id))]
        return [p for p in positions if p.inventory_yes or p.inventory_no]

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, amount: Decimal) -> FeeEstimate:
        """The taker fee for `amount` contracts at the YES `price`, on the
        token actually bought (`1 - price` for a `sell`). Makers pay nothing."""
        schedule = await asyncio.to_thread(self.catalog.fetch_fee_schedule, market_id)
        token_price = D(price) if side == Side.BUY else Decimal("1") - D(price)
        taker = schedule.estimate(float(token_price), float(amount), taker=True)
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, ids.native(VENUE, market_id)), side=side,
            price=D(price), amount=D(amount),
            taker_fee=D(taker) if taker is not None else None, maker_fee=Decimal("0"),
            currency="USDC", info={"schedule": schedule.model_dump()},
        )

    async def close(self) -> None:
        await self.http.close()
        await self.rpc.aclose()
        if self._own_catalog:
            self.catalog.close()
