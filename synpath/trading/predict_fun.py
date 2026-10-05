"""predict.fun order entry, and the venue's order and trade records on the YES leg.

**Not yet tested against the live venue with funds.** Built to predict.fun's
documentation and its own SDK (`predict-sdk` 0.0.22): hashing and both kinds
of signature are checked against independent EIP-712 encoders and the SDK's
algorithm; requests follow the venue's OpenAPI. The first account to trade
through it should start small.

**Every order is a signed message.** The order is the CTF exchange's EIP-712
`Order`, signed by a plain wallet or, for a Predict account (the smart
wallet the web app makes), by its owner key. See `predict_fun_signing`.
Orders, cancels and account reads need a login token (a JWT), which the
adapter gets by signing the venue's login message and renews when refused.

**YES and NO are separate tokens.** `buy` buys the YES token at the price
given; `sell` buys the NO token at `1 - price`; with `reduce_only`, `sell`
sells YES held and `buy` sells NO held. Everything the venue reports on the
NO token comes back on the YES leg.

**Limits rest; market orders take.** A limit is `LIMIT` (good till
cancelled, or till `expires_at`), post-only on request. A market order, or
an IOC limit, is the venue's `MARKET` strategy at the price given as the
worst accepted: whatever does not match at once is dropped; FOK sets the
venue's fill-or-kill flag.

**A cancel removes the order from the book, not from the chain.** The
venue's operator alone matches orders, so a removed order cannot fill
through it; the signed order itself stays valid on chain until cancelled
there, which this adapter does not do. A market may lock an order against
removal for a while after it is placed; such a cancel is refused.

A match settles on chain after the venue matches it, so a fill arrives twice
on the wallet stream: `orderTransactionSubmitted` (matched,
`SettlementState.MATCHED`) and then `orderTransactionSuccess` (`CONFIRMED`)
or `orderTransactionFailed` (`FAILED`), under the same settlement id, as
Polymarket's do.

Approvals (the one-time "enable trading"), split, merge and redeem are not
built here: a Predict account has them from the web app; a plain wallet
sets them with the venue's SDK.
"""
from __future__ import annotations

import asyncio
import base64
import json
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .. import ids
from ..base import AsyncHttpClient, Capability
from ..predict_fun import iso_ms
from ..errors import AuthenticationError, BadRequest, ExchangeError, MarketNotFound
from ..types import Page
from . import predict_fun_signing as sig
from .base import TradingExchange
from .credentials import PredictFunCredentials
from .errors import InsufficientFunds, InvalidOrder, OrderNotFound, OrderRejected
from .limiter import BudgetLimiter, Priority
from .polymarket import to_yes_leg, wire_leg
from .polymarket_signing import WalletSigner
from .types import (
    Account, Balance, FeeEstimate, Fill, Liquidity, Order, OrderRequest, OrderStatus, OrderType, Position,
    PositionSide, SettlementState, Side, TimeInForce, VENUE_ORDER_TYPES,
)

VENUE = "predict_fun"
WEI = Decimal(10) ** 18
API_URL = "https://api.predict.fun/v1"
TESTNET_API_URL = "https://api-testnet.predict.fun/v1"
RPC_URL = "https://bsc-dataseed.bnbchain.org"
TESTNET_RPC_URL = "https://data-seed-prebsc-1-s1.bnbchain.org:8545"

PAGE = 100
"""Rows per order, trade or position page."""

MAX_PAGES = 20
"""Most pages one listing call walks."""

MARKET_EXPIRY_S = 300
"""How long a market order's signature lives, as the SDK signs it."""

JWT_MARGIN_S = 60
"""A login token this close to expiry is renewed before use."""

ROW_STATUS = {
    "OPEN": OrderStatus.OPEN,
    "FILLED": OrderStatus.CLOSED,
    "EXPIRED": OrderStatus.EXPIRED,
    "CANCELLED": OrderStatus.CANCELED,
    "INVALIDATED": OrderStatus.CANCELED,
}
"""An order row's status. `INVALIDATED` is an order the chain no longer
honours (cancelled there, or its funds gone)."""

LIST_STATUS = {"open": "OPEN", "closed": "FILLED"}
"""The statuses the venue's order listing filters by."""

ORDER_EVENT_STATUS = {
    "orderAccepted": OrderStatus.OPEN,
    "orderNotAccepted": OrderStatus.REJECTED,
    "orderExpired": OrderStatus.EXPIRED,
    "orderCancelled": OrderStatus.CANCELED,
}
"""Order events whose status the event itself says. A transaction event's
status follows from how much is filled."""

FILL_SETTLEMENT = {
    "orderTransactionSubmitted": SettlementState.MATCHED,
    "orderTransactionSuccess": SettlementState.CONFIRMED,
    "orderTransactionFailed": SettlementState.FAILED,
}


def D(value: Any) -> Decimal:
    return Decimal(str(value)) if value not in (None, "") else Decimal("0")


def shares(value: Any) -> Decimal:
    """A share or USDT amount: an 18-decimal integer, or already a decimal."""
    text = str(value) if value not in (None, "") else "0"
    if text.isdigit() and len(text) > 12:
        return Decimal(text) / WEI
    return Decimal(text)


def _leg(details: dict[str, Any], price: Decimal | None):
    outcome = "no" if str(details.get("outcome") or "").upper() == "NO" else "yes"
    venue_side = "BUY" if str(details.get("quoteType") or "").upper() == "BID" else "SELL"
    return to_yes_leg(outcome, venue_side, price)


def order_of_event(event: dict[str, Any], *, account: Account | None = None) -> Order:
    """A wallet event as the order it describes, on the YES leg. The order's
    id is its hash, which the venue's order lookups and cancels take."""
    details = event.get("details") or {}
    side, price = _leg(details, D(details.get("price")) if details.get("price") not in (None, "") else None)
    amount, filled = D(details.get("quantity")), D(details.get("quantityFilled"))
    kind = str(event.get("type") or "")
    status = ORDER_EVENT_STATUS.get(kind)
    if status is None:
        status = OrderStatus.CLOSED if amount > 0 and filled >= amount else OrderStatus.OPEN
    market = OrderType.MARKET if str(details.get("strategyType") or "").upper() == "MARKET" else OrderType.LIMIT
    return Order(
        id=str(event.get("orderHash") or event.get("orderId") or ""),
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, str(details.get("marketId") or "")),
        side=side,
        type=market,
        time_in_force=TimeInForce.IOC if market == OrderType.MARKET else TimeInForce.GTC,
        status=status,
        price=price,
        amount=amount,
        filled=filled,
        remaining=max(Decimal("0"), amount - filled) if status == OrderStatus.OPEN else Decimal("0"),
        updated_at=int(event["timestamp"]) if event.get("timestamp") else None,
        info={**event, "native": kind},
    )


def fill_of_event(event: dict[str, Any], *, account: Account | None = None) -> Fill | None:
    """A transaction event as a fill on the YES leg, or `None` for any other
    event. The fill carries the settlement's progress; its id is the
    settlement id, the same across the submitted and final events."""
    kind = str(event.get("type") or "")
    settlement = FILL_SETTLEMENT.get(kind)
    fill = event.get("fill") or {}
    if settlement is None or not fill:
        return None
    details = event.get("details") or {}
    size = D(fill.get("executedSizeWei")) / WEI
    price = D(fill.get("executedPriceWei")) / WEI if fill.get("executedPriceWei") else D(details.get("price"))
    side, yes_price = _leg(details, price)
    fee = event.get("fee") or {}
    is_maker = event.get("isMaker")
    return Fill(
        id=str(event.get("settlementId") or event.get("orderHash") or ""),
        order_id=str(event.get("orderHash") or event.get("orderId") or ""),
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, str(details.get("marketId") or "")),
        side=side,
        price=yes_price or Decimal("0"),
        amount=size,
        fee=D(fee.get("amountWei")) / WEI if fee.get("amountWei") not in (None, "") else None,
        fee_currency="USDT" if fee.get("type") == "COLLATERAL" else ("shares" if fee else None),
        liquidity=Liquidity.MAKER if is_maker is True else Liquidity.TAKER if is_maker is False else Liquidity.UNKNOWN,
        settlement=settlement,
        timestamp=int(event.get("timestamp") or 0),
        info=event,
    )


# ---------------------------------------------------------------------------
# REST records
# ---------------------------------------------------------------------------

@dataclass
class MarketInfo:
    """What signing an order for one market needs."""

    native: str
    yes_token: str
    no_token: str
    neg_risk: bool
    yield_bearing: bool
    fee_rate_bps: int

    @property
    def market_id(self) -> str:
        return ids.qualify(VENUE, self.native)

    def token(self, outcome: str) -> str:
        return self.yes_token if outcome == "yes" else self.no_token

    def outcome_of(self, token_id: str) -> str:
        return "no" if str(token_id) == self.no_token else "yes"


def market_info_of(market: dict[str, Any]) -> MarketInfo:
    """A venue market as what its orders are signed against."""
    outcomes = {o.get("indexSet"): str(o.get("onChainId") or "") for o in market.get("outcomes") or []}
    native = str(market.get("id") or "")
    if not outcomes.get(1) or not outcomes.get(2):
        raise InvalidOrder(f"{VENUE}: market {native} publishes no outcome tokens")
    return MarketInfo(
        native=native, yes_token=outcomes[1], no_token=outcomes[2],
        neg_risk=bool(market.get("isNegRisk")), yield_bearing=bool(market.get("isYieldBearing")),
        fee_rate_bps=int(market.get("feeRateBps") or 0),
    )


def order_of_row(row: dict[str, Any], info: MarketInfo | None, *, account: Account | None = None) -> Order:
    """An order row (`GET /orders`) as an `Order` on the YES leg. Its id is
    the order's hash; the price is what its signed amounts state."""
    signed = row.get("order") or {}
    venue_side = "BUY" if int(signed.get("side") or 0) == sig.BUY else "SELL"
    maker, taker = D(signed.get("makerAmount")), D(signed.get("takerAmount"))
    price = None
    if maker > 0 and taker > 0:
        price = (maker / taker if venue_side == "BUY" else taker / maker).quantize(Decimal("0.000001"))
    outcome = info.outcome_of(str(signed.get("tokenId") or "")) if info else "yes"
    side, yes_price = to_yes_leg(outcome, venue_side, price)
    status = ROW_STATUS.get(str(row.get("status") or "").upper(), OrderStatus.OPEN)
    amount, filled = shares(row.get("amount")), shares(row.get("amountFilled"))
    market = str(row.get("strategy") or "").upper() == "MARKET"
    expiration = int(signed.get("expiration") or 0)
    return Order(
        id=str(signed.get("hash") or row.get("id") or ""),
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, str(row.get("marketId") or "")),
        side=side,
        type=OrderType.MARKET if market else OrderType.LIMIT,
        time_in_force=TimeInForce.IOC if market else (
            TimeInForce.GTD if expiration and expiration < sig.NO_EXPIRY else TimeInForce.GTC),
        status=status,
        price=yes_price,
        amount=amount,
        filled=filled,
        remaining=max(Decimal("0"), amount - filled) if status == OrderStatus.OPEN else Decimal("0"),
        expires_at=expiration * 1000 if expiration and expiration < sig.NO_EXPIRY else None,
        info={**row, "venue_order_id": row.get("id")},
    )


def fills_of_match(match: dict[str, Any], address: str, *, account: Account | None = None) -> list[Fill]:
    """This account's side of one match (`GET /orders/matches`), as fills on
    the YES leg: the taker leg if it was the taker, each maker leg it made.
    A listed match has settled on chain. The id is the settlement's, as on
    the wallet stream."""
    me = address.lower()
    market = match.get("market") or {}
    market_id = ids.qualify(VENUE, str(market.get("id") or match.get("marketId") or ""))
    timestamp = iso_ms(match.get("executedAt")) or 0
    legs: list[tuple[dict[str, Any], Liquidity]] = []
    taker = match.get("taker") or {}
    if str(taker.get("signer") or "").lower() == me:
        legs.append((taker, Liquidity.TAKER))
    legs += [(m, Liquidity.MAKER) for m in match.get("makers") or [] if str(m.get("signer") or "").lower() == me]
    fills: list[Fill] = []
    settlement = str(match.get("settlementId") or match.get("transactionHash") or "")
    for n, (leg, liquidity) in enumerate(legs):
        outcome = "no" if (leg.get("outcome") or {}).get("indexSet") == 2 else "yes"
        venue_side = "BUY" if str(leg.get("quoteType") or "").lower() == "bid" else "SELL"
        side, price = to_yes_leg(outcome, venue_side, shares(leg.get("price")))
        amount = shares(match.get("amountFilled") if liquidity == Liquidity.TAKER else leg.get("amount"))
        fee = leg.get("fee") or {}
        fills.append(Fill(
            id=settlement if n == 0 else f"{settlement}:{leg.get('hash')}",
            order_id=str(leg.get("hash") or ""),
            venue=VENUE,
            account=account,
            market_id=market_id,
            side=side,
            price=price or Decimal("0"),
            amount=amount,
            fee=shares(fee.get("amount")) if fee.get("amount") not in (None, "") else None,
            fee_currency="USDT" if fee.get("type") == "COLLATERAL" else ("shares" if fee else None),
            liquidity=liquidity,
            settlement=SettlementState.CONFIRMED,
            timestamp=timestamp,
            info={k: v for k, v in match.items() if k != "market"},
        ))
    return fills


def position_of(rows: list[dict[str, Any]], *, market_id: str, account: Account | None = None) -> Position:
    """A market's position from its rows, one per token held: the two
    inventories netted on the YES leg, and both kept."""
    def held(row: dict[str, Any]) -> Decimal:
        return shares(row.get("amount"))

    def on_no(row: dict[str, Any]) -> bool:
        return (row.get("outcome") or {}).get("indexSet") == 2

    yes_rows = [r for r in rows if not on_no(r)]
    no_rows = [r for r in rows if on_no(r)]
    held_yes = sum((held(r) for r in yes_rows), Decimal("0"))
    held_no = sum((held(r) for r in no_rows), Decimal("0"))
    net = held_yes - held_no
    lead = (yes_rows if net >= 0 and yes_rows else no_rows or yes_rows)[0]
    lead_on_yes = lead in yes_rows

    def yes_leg(value: Decimal | None) -> Decimal | None:
        if value is None:
            return None
        return value if lead_on_yes else Decimal("1") - value

    lead_held = held(lead)
    value = lead.get("valueUsd")
    mark = D(value) / lead_held if value not in (None, "") and lead_held > 0 else None
    entry = lead.get("averageBuyPriceUsd")
    pnl = [r.get("pnlUsd") for r in rows if r.get("pnlUsd") not in (None, "")]
    market = lead.get("market") or {}
    resolved = str(market.get("status") or "").upper() == "RESOLVED"
    won = [r for r in rows if (r.get("outcome") or {}).get("status") == "WON"]
    return Position(
        venue=VENUE,
        account=account,
        market_id=market_id,
        side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
        contracts=abs(net),
        inventory_yes=held_yes,
        inventory_no=held_no,
        entry_price=yes_leg(D(entry)) if entry not in (None, "") else None,
        mark_price=yes_leg(mark),
        unrealized_pnl=sum((D(v) for v in pnl), Decimal("0")) if pnl else None,
        resolved=resolved,
        final=resolved,
        redeemable=sum((held(r) for r in won), Decimal("0")) if resolved and won else None,
        info={"rows": [{k: v for k, v in r.items() if k != "market"} for r in rows]},
    )


def error_of(exc: ExchangeError) -> ExchangeError:
    """The venue's refusal of an order as a typed error, by its words."""
    text = str(exc).lower()
    if "balance" in text or "insufficient" in text or "allowance" in text or "collateral" in text:
        return InsufficientFunds(str(exc), body=exc.body)
    if "price" in text or "amount" in text or "minimum" in text or "precision" in text or "invalid" in text:
        return InvalidOrder(str(exc))
    return OrderRejected(str(exc), reason=None, info=exc.body if isinstance(exc.body, dict) else {}, body=exc.body)


def jwt_expiry(token: str) -> int | None:
    """A JWT's `exp`, read without verifying it; `None` when it has none."""
    try:
        payload = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        return int(claims["exp"]) if "exp" in claims else None
    except (IndexError, ValueError, KeyError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

def immediate(request: OrderRequest) -> bool:
    """Whether the order takes at once and drops the rest: a market order, or
    an IOC or FOK limit. These go as the venue's `MARKET` strategy."""
    return request.type == OrderType.MARKET or request.time_in_force in (TimeInForce.IOC, TimeInForce.FOK)


def check_request(request: OrderRequest) -> None:
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"{VENUE}: {request.type.value} is held by the execution engine, not the venue; submit it through the engine"
        )
    if request.time_in_force == TimeInForce.DAY:
        raise InvalidOrder(f"{VENUE}: 'day' is rewritten to 'gtd' by the engine; an adapter cannot pick a session end")
    if request.price is None:
        raise InvalidOrder(f"{VENUE}: a price is required -- a market order is sent at the worst price you give")
    if request.post_only and immediate(request):
        raise InvalidOrder(f"{VENUE}: a post-only order cannot also take at once")


def build_signed_order(
    request: OrderRequest, info: MarketInfo, *, signer: WalletSigner, maker: str, network: sig.Network,
    account: str | None, now_s: int, salt: int | None = None,
) -> tuple[dict[str, Any], Decimal]:
    """The `POST /orders` body and the contracts it is for, before anything
    is sent. The size is kept to five significant digits and the token price
    to three, as the venue's SDK keeps them, so the contracts can differ
    slightly from `request.amount`."""
    check_request(request)
    outcome, venue_side = wire_leg(request)
    yes_price = D(request.price)
    price = yes_price if outcome == "yes" else Decimal("1") - yes_price
    maker_amount, taker_amount, price_wei, shares_wei = sig.limit_amounts(
        venue_side, price, D(request.amount))  # type: ignore[arg-type]
    taking = immediate(request)
    if taking:
        expiration = now_s + MARKET_EXPIRY_S
    elif request.time_in_force == TimeInForce.GTD and request.expires_at:
        expiration = int(request.expires_at) // 1000
        if expiration <= now_s:
            raise InvalidOrder(f"{VENUE}: expires_at is in the past")
    else:
        expiration = sig.NO_EXPIRY
    order = sig.build_order(
        maker=maker, token_id=info.token(outcome), maker_amount=maker_amount, taker_amount=taker_amount,
        side=venue_side, fee_rate_bps=info.fee_rate_bps,  # type: ignore[arg-type]
        salt=salt if salt is not None else sig.new_salt(), expiration=expiration,
    )
    exchange = network.exchange(neg_risk=info.neg_risk, yield_bearing=info.yield_bearing)
    signature, order_hash = sig.sign_order(signer, order, exchange=exchange, network=network, account=account)
    wire = {k: str(v) for k, v in order.items()}
    wire.update({"expiration": int(order["expiration"]), "side": int(order["side"]),
                 "signatureType": int(order["signatureType"]), "hash": order_hash, "signature": signature})
    data: dict[str, Any] = {"order": wire, "pricePerShare": str(price_wei), "strategy": "MARKET" if taking else "LIMIT"}
    if taking:
        data["slippageBps"] = "0"
        if request.time_in_force == TimeInForce.FOK:
            data["isFillOrKill"] = True
    if request.post_only:
        data["isPostOnly"] = True
    return {"data": data}, sig.from_wei(shares_wei)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class PredictFunTrading(TradingExchange):
    """predict.fun order entry. Not yet tested against the live venue with
    funds; see the module docstring.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.predict_fun import PredictFunTrading

    async with PredictFunTrading(require("predict_fun", load_credentials())) as pf:
        print(await pf.fetch_balance())
    ```
    """

    id = VENUE
    name = "predict.fun"
    has: dict[str, Capability] = {
        "create_order": True,
        # No batch endpoint: one request per order.
        "create_orders": False,
        "cancel_order": True,
        # Up to 100 hashes a request.
        "cancel_orders": True,
        "cancel_all_orders": True,
        "edit_order": False,
        "fetch_order": True,
        "fetch_open_orders": True,
        # The venue filters by open or filled only; other states come unfiltered.
        "fetch_orders": True,
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
        credentials: PredictFunCredentials,
        *,
        account_name: str = "default",
        url: str | None = None,
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
        self.network = sig.TESTNET if credentials.testnet else sig.MAINNET
        self.predict_account = credentials.account_address
        """The Predict account the key signs for, or `None` for a plain wallet."""
        self.address = credentials.account_address or self.signer.address
        """Maker of every order, and holder of the funds."""
        # 240 requests a minute per API key, every kind together.
        self.limiter = limiter or BudgetLimiter(read_per_second=2, write_per_second=2)
        headers = {"x-api-key": credentials.api_key} if credentials.api_key else {}
        shared = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers)
        base = url or (TESTNET_API_URL if credentials.testnet else API_URL)
        self.http = AsyncHttpClient(base, limiter=None, client=shared, venue=VENUE)
        self.rpc_url = rpc_url or credentials.rpc_url or (TESTNET_RPC_URL if credentials.testnet else RPC_URL)
        self.rpc = rpc_client or httpx.AsyncClient(timeout=20, follow_redirects=True)
        """A BNB Chain node, for the balance read."""
        self._own_catalog = catalog is None
        if catalog is None:
            from ..predict_fun import PredictFun

            catalog = PredictFun(api_key=credentials.api_key, testnet=credentials.testnet)
        self.catalog = catalog
        """The read adapter: tokens, exchange and fee rate for a market."""
        self._markets: dict[str, MarketInfo] = {}
        self._jwt: str | None = None
        self._jwt_expires: int | None = None
        self._jwt_lock = asyncio.Lock()

    # -- login ----------------------------------------------------------------

    async def jwt(self, *, fresh: bool = False) -> str:
        """The login token: the venue's message signed by the key (for the
        Predict account, when there is one). Kept until it nears expiry, or
        until `fresh` asks for a new one. The wallet stream takes this as
        its token source."""
        async with self._jwt_lock:
            now = int(time.time())
            if not fresh and self._jwt and (self._jwt_expires is None or self._jwt_expires - JWT_MARGIN_S > now):
                return self._jwt
            await self.limiter.acquire(cost=2, kind="read", priority=Priority.HIGH)  # type: ignore[arg-type]
            asked = await self.http.get("/auth/message")
            message = str(((asked or {}).get("data") or {}).get("message") or "")
            if not message:
                raise AuthenticationError(f"{VENUE}: the venue sent no login message")
            signature = sig.sign_login(self.signer, message, network=self.network, account=self.predict_account)
            answer = await self.http.post("/auth", json={"signer": self.address, "message": message, "signature": signature})
            token = str(((answer or {}).get("data") or {}).get("token") or "")
            if not token:
                raise AuthenticationError(f"{VENUE}: the venue answered the login without a token")
            self._jwt, self._jwt_expires = token, jwt_expiry(token)
            return token

    # -- plumbing -------------------------------------------------------------

    async def _call(
        self, method: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None,
        kind: str = "read", priority: Priority = Priority.NORMAL,
    ) -> Any:
        """One logged-in call, paced. A refused token is renewed once."""
        for attempt in (0, 1):
            token = await self.jwt(fresh=attempt == 1)
            await self.limiter.acquire(cost=1, kind=kind, priority=priority)  # type: ignore[arg-type]
            kwargs: dict[str, Any] = {
                "params": {k: v for k, v in (params or {}).items() if v is not None} or None,
                "headers": {"Authorization": f"Bearer {token}"},
            }
            if body is not None:
                kwargs["json"] = body
            try:
                return await self.http.request(method, path, **kwargs)
            except AuthenticationError:
                if attempt == 1:
                    raise
            except MarketNotFound as exc:
                if path.startswith("/orders"):
                    raise OrderNotFound(str(exc), body=exc.body, status=exc.status) from None
                raise
        raise AssertionError("unreachable")  # pragma: no cover

    async def market_info(self, market_id: str) -> MarketInfo:
        """Tokens, exchange and fee rate for a market: one catalog read, kept."""
        native = ids.native(VENUE, market_id)
        cached = self._markets.get(native)
        if cached is not None:
            return cached
        market = await asyncio.to_thread(self.catalog.fetch_market, native)
        info = market_info_of(market.info.get("market") or {})
        self._markets[native] = info
        return info

    async def _infos(self, rows: list[dict[str, Any]]) -> dict[str, MarketInfo]:
        natives = {str(r.get("marketId")) for r in rows if r.get("marketId") is not None}
        found: dict[str, MarketInfo] = {}
        for native in natives:
            try:
                found[native] = await self.market_info(native)
            except (MarketNotFound, InvalidOrder):
                continue
        return found

    async def _orders_of(self, rows: list[dict[str, Any]]) -> list[Order]:
        infos = await self._infos(rows)
        return [order_of_row(r, infos.get(str(r.get("marketId"))), account=self.account) for r in rows]

    # -- orders ---------------------------------------------------------------

    async def create_order(self, request: OrderRequest) -> Order:
        """Sign and place one order. A market, IOC or FOK order is read back
        once after it is placed, to report what it took."""
        check_request(request)
        info = await self.market_info(request.market_id)
        body, contracts = build_signed_order(
            request, info, signer=self.signer, maker=self.address, network=self.network,
            account=self.predict_account, now_s=int(time.time()),
        )
        try:
            raw = await self._call("POST", "/orders", body=body, kind="write")
        except (BadRequest, OrderNotFound) as exc:
            raise error_of(exc) from None
        data = (raw or {}).get("data") or {}
        order_hash = str(data.get("orderHash") or body["data"]["order"]["hash"])
        order = Order(
            id=order_hash,
            client_order_id=request.client_order_id,
            venue=VENUE,
            account=request.account or self.account,
            market_id=info.market_id,
            side=request.side,
            type=request.type,
            time_in_force=request.time_in_force,
            status=OrderStatus.OPEN,
            price=D(request.price),
            amount=contracts,
            filled=Decimal("0"),
            remaining=contracts,
            reduce_only=request.reduce_only,
            post_only=request.post_only,
            expires_at=request.expires_at,
            created_at=int(time.time() * 1000),
            book=request.book,
            trader=request.trader,
            tags=request.tags,
            info={"response": raw, "venue_order_id": data.get("orderId"),
                  "request": {**body["data"], "order": {k: v for k, v in body["data"]["order"].items() if k != "signature"}}},
        )
        if not immediate(request):
            return order
        try:
            after = await self.fetch_order(order_hash)
        except OrderNotFound:
            return order
        return after.model_copy(update={
            "client_order_id": order.client_order_id, "type": order.type, "time_in_force": order.time_in_force,
            "book": order.book, "trader": order.trader, "tags": order.tags, "reduce_only": order.reduce_only,
        })

    async def _remove(self, hashes: list[str]) -> dict[str, Any]:
        out: dict[str, list[str]] = {"removed": [], "noop": [], "rejected": []}
        for start in range(0, len(hashes), 100):
            raw = await self._call("POST", "/orders/remove-by-hash", body={"data": {"hashes": hashes[start:start + 100]}},
                                   kind="write", priority=Priority.HIGH) or {}
            for key in out:
                out[key] += [str(h).lower() for h in raw.get(key) or []]
        return out

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        """Take one order off the book and read it back. A market's removal
        lock refuses the cancel for a while after placing."""
        [result] = await self.cancel_orders([order_id])
        if isinstance(result, Exception):
            raise result
        return result

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        result = await self._remove(list(order_ids))
        out: list[Order | Exception] = []
        for order_id in order_ids:
            if order_id.lower() in result["rejected"]:
                out.append(OrderRejected(
                    f"{VENUE}: cancel of {order_id} refused -- the market locks orders against removal for a while "
                    f"after they are placed; retry shortly", reason="removal_locked", info={"cancel": result},
                ))
                continue
            try:
                order = await self.fetch_order(order_id)
            except OrderNotFound as exc:
                out.append(exc)
                continue
            if order_id.lower() in result["removed"] and order.status == OrderStatus.OPEN:
                order = order.model_copy(update={"status": OrderStatus.CANCELED, "remaining": Decimal("0")})
            out.append(order.model_copy(update={"info": {**order.info, "cancel": result}}))
        return out

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int:
        """Every open order, or every one on a market, in batches of 100.
        Returns how many came off the book."""
        hashes = [o.id for o in await self.fetch_open_orders(market_id=market_id)]
        if not hashes:
            return 0
        return len((await self._remove(hashes))["removed"])

    async def fetch_order(self, order_id: str) -> Order:
        raw = await self._call("GET", f"/orders/{order_id}") or {}
        row = raw.get("data") if isinstance(raw, dict) else None
        if not isinstance(row, dict) or not row.get("order"):
            raise OrderNotFound(f"{VENUE}: no order {order_id}")
        [order] = await self._orders_of([row])
        return order

    async def fetch_orders(
        self, *, status: str | None = None, market_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Order]:
        """One page of this account's orders, newest first. `status` is
        `open` or `closed` (filled), the two the venue filters by."""
        if status is not None and status not in LIST_STATUS:
            raise BadRequest(f"{VENUE}: the venue lists orders by {', '.join(LIST_STATUS)} only, not {status!r}")
        raw = await self._call("GET", "/orders", params={
            "first": PAGE, "after": cursor, "status": LIST_STATUS[status] if status else None,
        }) or {}
        rows = list(raw.get("data") or [])
        if market_id:
            native = ids.native(VENUE, market_id)
            rows = [r for r in rows if str(r.get("marketId")) == native]
        orders = await self._orders_of(rows)
        if since:
            orders = [o for o in orders if (o.created_at or since) >= since]
        return Page(orders[:limit] if limit else orders, next_cursor=raw.get("cursor") or None)

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        orders: list[Order] = []
        cursor: str | None = None
        for _ in range(MAX_PAGES):
            page = await self.fetch_orders(status="open", market_id=market_id, cursor=cursor)
            orders.extend(page)
            cursor = page.next_cursor
            if not cursor:
                break
        return orders

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        """One page of this account's settled fills, newest first, from the
        venue's public match record filtered to its address."""
        await self.limiter.acquire(cost=1, kind="read")  # type: ignore[arg-type]
        raw = await self.http.get("/orders/matches", params={
            "first": PAGE, "after": cursor, "signerAddress": self.address,
            "marketId": int(ids.native(VENUE, market_id)) if market_id else None,
            "orderHashes": [order_id] if order_id else None,
            "executedAfter": since // 1000 if since else None,
        }) or {}
        fills = [f for row in raw.get("data") or [] for f in fills_of_match(row, self.address, account=self.account)]
        if order_id:
            fills = [f for f in fills if f.order_id.lower() == order_id.lower()]
        return Page(fills[:limit] if limit else fills, next_cursor=raw.get("cursor") or None)

    # -- account --------------------------------------------------------------

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        """The address's USDT on chain; what open orders hold, as the venue
        counts it, is `locked`."""
        data = "0x70a08231" + bytes.fromhex(self.address[2:]).rjust(32, b"\0").hex()
        answer = (await self.rpc.post(self.rpc_url, json={
            "jsonrpc": "2.0", "id": 1, "method": "eth_call",
            "params": [{"to": self.network.usdt, "data": data}, "latest"],
        })).json()
        if not isinstance(answer, dict) or "result" not in answer:
            raise ExchangeError(f"{VENUE}: balance read failed: {answer.get('error') if isinstance(answer, dict) else answer}")
        total = Decimal(int(answer["result"], 16)) / WEI
        reserved = await self._call("POST", "/account/reserved-balances/query", body={"assets": [{"type": "USDT"}]}) or {}
        rows = reserved.get("data") or []
        locked = shares(rows[0].get("amount")) if rows else Decimal("0")
        available = max(Decimal("0"), total - locked)
        return Balance(
            venue=VENUE, account=account or self.account, currency="USDT",
            total=total, available=available, locked=locked, buying_power=available,
            timestamp=int(time.time() * 1000), info={"address": self.address, "reserved": reserved},
        )

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        """Token inventories, netted per market on the YES leg. `event_id`
        keeps the markets of one category."""
        rows: list[dict[str, Any]] = []
        cursor: str | None = None
        for _ in range(MAX_PAGES):
            raw = await self._call("GET", "/positions", params={
                "first": PAGE, "after": cursor,
                "marketId": int(ids.native(VENUE, market_id)) if market_id else None,
            }) or {}
            rows.extend(raw.get("data") or [])
            cursor = raw.get("cursor") or None
            if not cursor:
                break
        if event_id:
            slug = ids.native(VENUE, event_id)
            rows = [r for r in rows if str((r.get("market") or {}).get("categorySlug")) == slug]
        by_market: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            by_market.setdefault(str((row.get("market") or {}).get("id") or ""), []).append(row)
        positions = [
            position_of(group, market_id=ids.qualify(VENUE, native), account=self.account)
            for native, group in by_market.items() if native
        ]
        return [p for p in positions if p.inventory_yes or p.inventory_no]

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, amount: Decimal) -> FeeEstimate:
        """The taker fee for `amount` contracts at the YES `price`: the
        market's rate on `min(p, 1 - p)` a share, the same for YES and NO.
        Makers pay nothing."""
        schedule = await asyncio.to_thread(self.catalog.fetch_fee_schedule, market_id)
        taker = schedule.estimate(float(price), float(amount), taker=True)
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, ids.native(VENUE, market_id)), side=side,
            price=D(price), amount=D(amount),
            taker_fee=D(taker) if taker is not None else None, maker_fee=Decimal("0"),
            currency="USDT", info={"schedule": schedule.model_dump()},
        )

    async def close(self) -> None:
        await self.http.close()
        await self.rpc.aclose()
        if self._own_catalog:
            self.catalog.close()
