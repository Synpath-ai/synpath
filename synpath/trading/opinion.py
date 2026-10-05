"""Opinion order entry, and the venue's order and trade records on the YES leg.

Built to Opinion's documentation and its own SDK (`opinion_clob_sdk` 0.7,
`opinion_api` 0.4): signing and amounts are checked byte for byte against the
SDK's code, every request against the SDK's wire shapes.

Four facts about the venue shape this adapter.

**Every order is a signed message.** The order is the CTF exchange's EIP-712
`Order`, signed by the wallet's key for the account's Safe on BNB Chain,
which holds the USDT and tokens. See `opinion_signing`.

**YES and NO are separate tokens**, as on Polymarket. `buy` buys the YES
token at the price given; `sell` buys the NO token at `1 - price`; with
`reduce_only`, `sell` sells YES tokens held and `buy` sells NO tokens held.
Everything the venue reports on the NO token comes back on the YES leg.

**Only resting limits exist.** The venue has no time in force, no post-only
and no limit-protected market order (its market buy spends a USDT amount,
which can buy more contracts than asked). So `market` and `ioc` are sent as
a limit at the price given -- for a market order, the worst price accepted
-- and whatever has not matched straight away is cancelled.

**A match is not a fill until the chain confirms it.** An order's filled
figures move, and a trade record arrives, only once the match settles on
chain; a match the chain refuses is a failed trade. The minimum order is
5 USDT, and every taker order pays at least 0.25 USDT in fees.

The records:

* An **order update** (`trade.order.update` on the WebSocket, an order row
  over REST) when an order is placed, matched, cancelled or confirmed.
* A **trade record** (`trade.record.new`, a trade row) once a match is
  confirmed, one per fill, with the fee actually charged. Splits and merges
  come on the same channel and are not fills.

Approvals (the one-time "enable trading") are made on opinion.trade; split,
merge and redeem are not built here.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .. import ids
from ..base import AsyncHttpClient, Capability
from ..errors import BadRequest, ExchangeError, MarketNotFound
from ..types import Page
from . import opinion_signing as sig
from .base import TradingExchange
from .credentials import OpinionCredentials
from .errors import CredentialsMissing, InsufficientFunds, InvalidOrder, OrderNotFound, OrderRejected
from .limiter import BudgetLimiter, Priority
from .polymarket import to_yes_leg, wire_leg
from .polymarket_signing import WalletSigner
from .types import (
    Account, Balance, FeeEstimate, Fill, Liquidity, Order, OrderRequest, OrderStatus, OrderType, Position,
    PositionSide, SettlementState, Side, TimeInForce, VENUE_ORDER_TYPES,
)

VENUE = "opinion"
TRADING_URL = "https://proxy.opinion.trade:8443/openapi"
"""The host the venue's SDK sends orders to."""

PAGE = 20
"""Rows per order, trade or position page: the venue's maximum."""

MAX_PAGES = 50
"""Most pages one listing call walks: 1000 rows."""

MIN_ORDER_USDT = Decimal("5")
"""The venue's documented minimum order value."""

ORDER_STATUS = {
    1: OrderStatus.OPEN,        # pending: resting, or matched and awaiting the chain
    2: OrderStatus.CLOSED,      # finished
    3: OrderStatus.CANCELED,
    4: OrderStatus.EXPIRED,
    5: OrderStatus.REJECTED,    # failed
}
"""The venue's numeric order status."""

STATUS_CODES = {"open": 1, "closed": 2, "canceled": 3, "expired": 4, "rejected": 5}
"""Order statuses as the venue's filter codes."""

TRADE_SETTLEMENT = {
    2: SettlementState.CONFIRMED,   # finished
    3: SettlementState.FAILED,      # canceled
    5: SettlementState.FAILED,      # failed
    6: SettlementState.FAILED,      # failed on chain
}
"""The venue's numeric trade status, as a settlement state."""

FILL_SIDES = ("buy", "sell")
"""Trade record sides that are fills. `Split` and `Merge` are conversions
between collateral and a YES/NO pair, not trades."""

RESOLVED = 4
"""A market's status once its outcome is final."""


def D(value: Any) -> Decimal:
    return Decimal(str(value)) if value not in (None, "") else Decimal("0")


def outcome_of(raw: dict[str, Any]) -> str:
    """`outcomeSide` 1 is YES, 2 is NO."""
    return "no" if raw.get("outcomeSide") == 2 else "yes"


def _ms(seconds: Any) -> int | None:
    value = int(seconds or 0)
    return value * 1000 if value else None


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

def order_of(raw: dict[str, Any], *, account: Account | None = None) -> Order:
    """An order update or order row as an `Order` on the YES leg.

    The size is `shares` on the WebSocket and `orderShares` over REST, and
    `filledShares` is how much of it the chain has confirmed. An order the
    venue reports finished is closed with nothing left; one still pending
    stays open with the unconfirmed rest.
    """
    venue_side = "BUY" if raw.get("side") == 1 else "SELL"
    price = Decimal(str(raw["price"])) if raw.get("price") not in (None, "") else None
    side, yes_price = to_yes_leg(outcome_of(raw), venue_side, price)
    status = ORDER_STATUS.get(raw.get("status"), OrderStatus.OPEN)
    amount = D(raw.get("shares") or raw.get("orderShares"))
    filled = D(raw.get("filledShares"))
    expires = _ms(raw.get("expiresAt"))
    return Order(
        id=str(raw.get("orderId") or ""),
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, str(raw.get("marketId") or "")),
        side=side,
        type=OrderType.MARKET if raw.get("tradingMethod") == 1 else OrderType.LIMIT,
        time_in_force=TimeInForce.GTD if expires else TimeInForce.GTC,
        status=status,
        price=yes_price,
        amount=amount,
        filled=filled,
        remaining=max(Decimal("0"), amount - filled) if status == OrderStatus.OPEN else Decimal("0"),
        cost=D(raw.get("filledAmount")) if raw.get("filledAmount") not in (None, "") else None,
        expires_at=expires,
        created_at=_ms(raw.get("createdAt")),
        info=raw,
    )


def fill_of(raw: dict[str, Any], *, account: Account | None = None) -> Fill | None:
    """A trade record as a `Fill` on the YES leg, or `None` for a split or a
    merge. Records arrive after the chain confirms, so a finished one is
    settled; a failed one is reported failed rather than dropped, so a fill
    already counted from an order update can be taken back. The fee is
    `fee` on the WebSocket and `feeFormatted` over REST."""
    venue_side = str(raw.get("side") or "").lower()
    if venue_side not in FILL_SIDES:
        return None
    side, price = to_yes_leg(outcome_of(raw), venue_side, D(raw.get("price")))
    fee = raw.get("fee", raw.get("feeFormatted"))
    return Fill(
        id=str(raw.get("tradeNo") or raw.get("txHash") or ""),
        order_id=str(raw.get("orderId") or raw.get("orderNo") or ""),
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, str(raw.get("marketId") or "")),
        side=side,
        price=price or Decimal("0"),
        amount=D(raw.get("shares")),
        fee=D(fee) if fee not in (None, "") else None,
        fee_currency="USDT",
        liquidity=Liquidity.UNKNOWN,
        settlement=TRADE_SETTLEMENT.get(raw.get("status"), SettlementState.MATCHED),
        timestamp=_ms(raw.get("createdAt")) or 0,
        info=raw,
    )


def position_of(rows: list[dict[str, Any]], *, market_id: str, account: Account | None = None) -> Position:
    """A market's position from its rows, one per token held: the two
    inventories netted on the YES leg, and both kept."""
    yes_rows = [r for r in rows if outcome_of(r) == "yes"]
    no_rows = [r for r in rows if outcome_of(r) == "no"]
    held_yes = sum((D(r.get("sharesOwned")) for r in yes_rows), Decimal("0"))
    held_no = sum((D(r.get("sharesOwned")) for r in no_rows), Decimal("0"))
    net = held_yes - held_no
    lead = (yes_rows if net >= 0 and yes_rows else no_rows or yes_rows)[0]
    on_yes = lead in yes_rows

    def yes_leg(value: Any) -> Decimal | None:
        if value in (None, "", "0", 0):
            return None
        return D(value) if on_yes else Decimal("1") - D(value)

    shares = D(lead.get("sharesOwned"))
    value = lead.get("currentValueInQuoteToken")
    mark = D(value) / shares if value not in (None, "") and shares > 0 else None
    claimable = [r for r in rows if str(r.get("claimStatusEnum") or "") == "WaitClaim" or r.get("claimStatus") == 1]
    unrealized = [r.get("unrealizedPnl") for r in rows if r.get("unrealizedPnl") not in (None, "")]
    return Position(
        venue=VENUE,
        account=account,
        market_id=market_id,
        side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
        contracts=abs(net),
        inventory_yes=held_yes,
        inventory_no=held_no,
        entry_price=yes_leg(lead.get("avgEntryPrice")),
        mark_price=yes_leg(mark) if mark is not None else None,
        unrealized_pnl=sum((D(v) for v in unrealized), Decimal("0")) if unrealized else None,
        resolved=any(r.get("marketStatus") == RESOLVED for r in rows),
        final=False,
        redeemable=sum((D(r.get("currentValueInQuoteToken")) for r in claimable), Decimal("0")) if claimable else None,
        info={"rows": rows},
    )


def error_of(message: str, *, body: Any = None) -> ExchangeError:
    """The venue's refusal as a typed error. It answers in words inside its
    envelope; the phrases matched are the ones its docs and SDK use."""
    text = message.lower()
    if "balance" in text or "insufficient" in text or "allowance" in text:
        return InsufficientFunds(f"{VENUE}: {message}", body=body)
    if "not found" in text or "not exist" in text:
        return OrderNotFound(f"{VENUE}: {message}", body=body)
    if "price" in text or "amount" in text or "minimum" in text or "invalid" in text:
        return InvalidOrder(f"{VENUE}: {message}")
    return OrderRejected(f"{VENUE}: {message}", reason=None, info=body if isinstance(body, dict) else {}, body=body)


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

@dataclass
class MarketInfo:
    """What signing an order for one market needs."""

    native: str
    yes_token: str
    no_token: str
    quote_token: str
    exchange: str
    """The CTF exchange for the market's collateral: the verifying contract."""

    @property
    def market_id(self) -> str:
        return ids.qualify(VENUE, self.native)

    def token(self, outcome: str) -> str:
        return self.yes_token if outcome == "yes" else self.no_token


def immediate(request: OrderRequest) -> bool:
    """Whether the unmatched rest is cancelled at once: a market order (sent
    as a limit at its worst price) or an IOC."""
    return request.type == OrderType.MARKET or request.time_in_force == TimeInForce.IOC


def check_request(request: OrderRequest) -> None:
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"{VENUE}: {request.type.value} is held by the execution engine, not the venue; submit it through the engine"
        )
    if request.time_in_force not in (TimeInForce.GTC, TimeInForce.IOC):
        raise InvalidOrder(
            f"{VENUE}: {request.time_in_force.value} is not available -- the venue rests limits until cancelled; "
            f"gtc, or ioc (sent as a limit and the rest cancelled)"
        )
    if request.post_only:
        raise InvalidOrder(f"{VENUE}: the venue has no post-only orders")
    if request.price is None:
        raise InvalidOrder(f"{VENUE}: a price is required -- a market order is sent as a limit at the worst price you give")


def build_signed_order(
    request: OrderRequest, info: MarketInfo, *, signer: WalletSigner, maker: str, now_s: int,
    salt: int | None = None,
) -> tuple[dict[str, Any], Decimal]:
    """The `POST /order` body and the contracts it is for, before anything is
    sent. The contracts can differ slightly from `request.amount`: the venue
    needs the amounts to state the price exactly, which trims the size to
    four significant digits of its USDT value."""
    try:
        native = ids.native(VENUE, request.market_id)
    except BadRequest as exc:
        raise InvalidOrder(str(exc)) from None
    check_request(request)
    outcome, venue_side = wire_leg(request)
    yes_price = D(request.price)
    price = sig.check_price(yes_price if outcome == "yes" else Decimal("1") - yes_price)
    amount = D(request.amount)
    if amount * price < MIN_ORDER_USDT:
        raise InvalidOrder(
            f"{VENUE}: {amount} at {price} is {amount * price} USDT; the venue's minimum order is {MIN_ORDER_USDT} USDT"
        )
    maker_wei = sig.to_wei(amount * price if venue_side == "BUY" else amount)
    maker_amount, taker_amount = sig.limit_amounts(venue_side, price, maker_wei)  # type: ignore[arg-type]
    order = sig.build_order(
        maker=maker, signer=signer.address, token_id=info.token(outcome),
        maker_amount=maker_amount, taker_amount=taker_amount, side=venue_side,  # type: ignore[arg-type]
        salt=salt if salt is not None else sig.new_salt(),
    )
    signature = sig.sign_order(signer, order, exchange=info.exchange)
    body = {
        "salt": str(order["salt"]),
        "topicId": int(native),
        "maker": order["maker"],
        "signer": order["signer"],
        "taker": order["taker"],
        "tokenId": str(order["tokenId"]),
        "makerAmount": str(order["makerAmount"]),
        "takerAmount": str(order["takerAmount"]),
        "expiration": "0",
        "nonce": "0",
        "feeRateBps": "0",
        "side": str(order["side"]),
        "signatureType": str(order["signatureType"]),
        "signature": signature,
        "sign": signature,
        "contractAddress": "",
        "currencyAddress": info.quote_token,
        "price": str(price),
        "tradingMethod": sig.LIMIT_ORDER,
        "timestamp": now_s,
        "safeRate": "0",
        "orderExpTime": "0",
    }
    contracts = sig.from_wei(taker_amount if venue_side == "BUY" else maker_amount)
    return body, contracts


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class OpinionTrading(TradingExchange):
    """Opinion order entry; see the module docstring.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.opinion import OpinionTrading

    async with OpinionTrading(require("opinion", load_credentials())) as opinion:
        print(await opinion.fetch_balance())
    ```
    """

    id = VENUE
    name = "Opinion"
    has: dict[str, Capability] = {
        "create_order": True,
        # No batch endpoint: one request per order.
        "create_orders": False,
        "cancel_order": True,
        "cancel_orders": False,
        # Every open order listed, then cancelled one by one.
        "cancel_all_orders": True,
        "edit_order": False,
        "fetch_order": True,
        "fetch_open_orders": True,
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
        credentials: OpinionCredentials,
        *,
        account_name: str = "default",
        url: str = TRADING_URL,
        catalog: Any = None,
        limiter: BudgetLimiter | None = None,
        timeout: float = 30.0,
        client: Any = None,
    ):
        import httpx

        self.credentials = credentials
        self.signer = WalletSigner(credentials.private_key)
        self.account = Account(venue=VENUE, name=account_name)
        # The venue allows 15 requests a second per API key, all kinds together.
        self.limiter = limiter or BudgetLimiter(read_per_second=10, write_per_second=5)
        shared = client or httpx.AsyncClient(
            timeout=timeout, follow_redirects=True, headers={"apikey": credentials.api_key},
        )
        self.http = AsyncHttpClient(url, limiter=None, client=shared, venue=VENUE)
        self._own_catalog = catalog is None
        if catalog is None:
            from ..opinion import Opinion

            catalog = Opinion(api_key=credentials.api_key)
        self.catalog = catalog
        """The read adapter: tokens, collateral and fees for a market."""
        self.wallet: str | None = credentials.multisig_address
        """The account's Safe, which makes every order and holds the funds."""
        self._markets: dict[str, MarketInfo] = {}
        self._exchanges: dict[str, str] = {}
        """Collateral token address (lowercase) -> its CTF exchange."""

    # -- plumbing -------------------------------------------------------------

    async def _call(
        self, method: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None,
        kind: str = "read", priority: Priority = Priority.NORMAL,
    ) -> Any:
        """One API-key call, paced, its envelope opened and its refusal typed."""
        await self.limiter.acquire(cost=1, kind=kind, priority=priority)  # type: ignore[arg-type]
        kwargs: dict[str, Any] = {"params": {k: v for k, v in (params or {}).items() if v is not None} or None}
        if body is not None:
            kwargs["json"] = body
        try:
            payload = await self.http.request(method, path, **kwargs)
        except MarketNotFound as exc:
            if "/order" in path:
                raise OrderNotFound(str(exc), body=exc.body, status=exc.status) from None
            raise
        if not isinstance(payload, dict):
            return payload
        errno = payload.get("errno", payload.get("code", 0))
        if errno:
            raise error_of(str(payload.get("errmsg") or payload.get("msg") or f"error {errno}"), body=payload)
        return payload.get("result")

    async def maker(self) -> str:
        """The account's Safe: configured, or read from the venue once."""
        if self.wallet:
            return self.wallet
        raw = await self._call("GET", "/user/balance", params={"chain_id": sig.CHAIN_ID}) or {}
        safe = raw.get("multiSignAddress")
        if not safe:
            raise CredentialsMissing(
                f"{VENUE}: this account has no trading wallet yet -- enable trading on opinion.trade first"
            )
        self.wallet = safe
        return safe

    async def market_info(self, market_id: str) -> MarketInfo:
        """Tokens, collateral and exchange for a market. One catalog read and,
        the first time a collateral is seen, one read of the venue's list."""
        native = ids.native(VENUE, market_id)
        cached = self._markets.get(native)
        if cached is not None:
            return cached
        market = await asyncio.to_thread(self.catalog.fetch_market, native)
        quote = str(market.info.get("quoteToken") or "")
        if quote.lower() not in self._exchanges:
            rows = (await self._call("GET", "/quoteToken") or {}).get("list") or []
            for row in rows:
                self._exchanges[str(row.get("quoteTokenAddress") or "").lower()] = str(row.get("ctfExchangeAddress") or "")
        exchange = self._exchanges.get(quote.lower())
        if not exchange or not market.yes.venue_token_id or not market.no.venue_token_id:
            raise InvalidOrder(f"{VENUE}: market {native} is not tradable (no tokens or exchange published)")
        info = MarketInfo(
            native=native, yes_token=market.yes.venue_token_id, no_token=market.no.venue_token_id,
            quote_token=quote, exchange=exchange,
        )
        self._markets[native] = info
        return info

    # -- orders ---------------------------------------------------------------

    async def create_order(self, request: OrderRequest) -> Order:
        """Sign and place one order. A market or IOC order is placed as a
        limit and its unmatched rest cancelled at once; it comes back as the
        venue reports it after the cancel."""
        check_request(request)
        info = await self.market_info(request.market_id)
        body, contracts = build_signed_order(
            request, info, signer=self.signer, maker=await self.maker(), now_s=int(time.time()),
        )
        raw = await self._call("POST", "/order", body=body, kind="write") or {}
        data = raw.get("orderData") or raw
        order_id = str(data.get("orderId") or "")
        if not order_id:
            raise error_of("order accepted without an order id", body=raw)
        order = Order(
            id=order_id,
            client_order_id=request.client_order_id,
            venue=VENUE,
            account=request.account or self.account,
            market_id=info.market_id,
            side=request.side,
            type=request.type,
            time_in_force=request.time_in_force,
            status=ORDER_STATUS.get(data.get("status"), OrderStatus.OPEN),
            price=D(request.price),
            amount=contracts,
            filled=Decimal("0"),
            reduce_only=request.reduce_only,
            created_at=int(time.time() * 1000),
            book=request.book,
            trader=request.trader,
            tags=request.tags,
            info={"response": raw, "request": {k: v for k, v in body.items() if k not in ("signature", "sign")}},
        )
        if immediate(request):
            return await self._cancel_rest(order)
        return order

    async def _cancel_rest(self, order: Order) -> Order:
        """Cancel whatever of an immediate order did not match, and report it
        as the venue then does. A cancel the venue refuses because nothing is
        left is not an error."""
        try:
            await self._call("POST", "/order/cancel", body={"orderId": order.id}, kind="write", priority=Priority.HIGH)
        except (OrderNotFound, OrderRejected, InvalidOrder):
            pass
        try:
            after = await self.fetch_order(order.id)
        except OrderNotFound:
            return order
        return after.model_copy(update={
            "client_order_id": order.client_order_id, "type": order.type, "time_in_force": order.time_in_force,
            "book": order.book, "trader": order.trader, "tags": order.tags,
        })

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        """Cancel one order and read it back, with whatever matched first."""
        raw = await self._call("POST", "/order/cancel", body={"orderId": order_id}, kind="write", priority=Priority.HIGH)
        order = await self.fetch_order(order_id)
        accepted = raw.get("result") if isinstance(raw, dict) else raw
        if accepted is False and not order.is_terminal:
            raise OrderRejected(f"{VENUE}: cancel of {order_id} refused", reason="cancel_refused", info={"cancel": raw})
        return order.model_copy(update={"info": {**order.info, "cancel": raw}})

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int:
        """Every open order, or every one on a market, one cancel each.
        Returns how many the venue accepted."""
        cancelled = 0
        for order in await self.fetch_open_orders(market_id=market_id):
            try:
                raw = await self._call("POST", "/order/cancel", body={"orderId": order.id}, kind="write", priority=Priority.HIGH)
            except (OrderNotFound, OrderRejected):
                continue
            if not (isinstance(raw, dict) and raw.get("result") is False):
                cancelled += 1
        return cancelled

    async def fetch_order(self, order_id: str) -> Order:
        raw = await self._call("GET", f"/order/{order_id}") or {}
        data = raw.get("orderData") or raw
        if not isinstance(data, dict) or not data.get("orderId"):
            raise OrderNotFound(f"{VENUE}: no order {order_id}")
        return order_of(data, account=self.account)

    async def fetch_orders(
        self, *, status: str | None = None, market_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Order]:
        """One page of this account's orders, newest first. `status` is
        `open`, `closed`, `canceled`, `expired` or `rejected`."""
        if status is not None and status not in STATUS_CODES:
            raise BadRequest(f"{VENUE}: unknown status {status!r}; expected one of {', '.join(STATUS_CODES)}")
        page = _page(cursor)
        raw = await self._call("GET", "/order", params={
            "marketId": int(ids.native(VENUE, market_id)) if market_id else None,
            "status": STATUS_CODES[status] if status else None,
            "page": page, "limit": PAGE,
        }) or {}
        rows = raw.get("list") or []
        orders = [order_of(row, account=self.account) for row in rows]
        if since:
            orders = [o for o in orders if (o.created_at or 0) >= since]
        more = len(rows) == PAGE and (raw.get("total") is None or page * PAGE < int(raw["total"]))
        return Page(orders[:limit] if limit else orders, next_cursor=str(page + 1) if more else None)

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
        """One page of this account's confirmed fills, newest first. Splits
        and merges are left out; `order_id` narrows the page."""
        page = _page(cursor)
        raw = await self._call("GET", "/trade", params={
            "marketId": int(ids.native(VENUE, market_id)) if market_id else None,
            "page": page, "limit": PAGE,
        }) or {}
        rows = raw.get("list") or []
        fills = [f for f in (fill_of(row, account=self.account) for row in rows) if f is not None]
        if order_id:
            fills = [f for f in fills if f.order_id == order_id]
        if since:
            fills = [f for f in fills if f.timestamp >= since]
        more = len(rows) == PAGE and (raw.get("total") is None or page * PAGE < int(raw["total"]))
        return Page(fills[:limit] if limit else fills, next_cursor=str(page + 1) if more else None)

    # -- account --------------------------------------------------------------

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        """The Safe's USDT: what open orders hold is `locked`."""
        raw = await self._call("GET", "/user/balance", params={"chain_id": sig.CHAIN_ID}) or {}
        if raw.get("multiSignAddress") and not self.wallet:
            self.wallet = raw["multiSignAddress"]
        rows = raw.get("balances") or []
        total = sum((D(r.get("totalBalance")) for r in rows), Decimal("0"))
        available = sum((D(r.get("availableBalance")) for r in rows), Decimal("0"))
        frozen = sum((D(r.get("frozenBalance")) for r in rows), Decimal("0"))
        return Balance(
            venue=VENUE, account=account or self.account, currency="USDT",
            total=total, available=available, locked=frozen, buying_power=available,
            timestamp=int(time.time() * 1000), info=raw,
        )

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        """Token inventories, netted per market on the YES leg. `event_id`
        keeps the options of one categorical topic."""
        rows: list[dict[str, Any]] = []
        for page in range(1, MAX_PAGES + 1):
            raw = await self._call("GET", "/positions", params={
                "marketId": int(ids.native(VENUE, market_id)) if market_id else None,
                "page": page, "limit": PAGE,
            }) or {}
            batch = raw.get("list") or []
            rows.extend(batch)
            if len(batch) < PAGE:
                break
        if event_id:
            topic = ids.native(VENUE, event_id)
            rows = [r for r in rows if str(r.get("rootMarketId") or r.get("marketId")) == topic]
        by_market: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            by_market.setdefault(str(row.get("marketId") or ""), []).append(row)
        positions = [
            position_of(group, market_id=ids.qualify(VENUE, native), account=self.account)
            for native, group in by_market.items() if native
        ]
        return [p for p in positions if p.inventory_yes or p.inventory_no]

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, amount: Decimal) -> FeeEstimate:
        """The taker fee for `amount` contracts at the YES `price`, from the
        market's rate on chain, with the venue's 0.25 USDT minimum. The fee
        is charged on the token actually bought, so a `sell` (buying NO) is
        priced at `1 - price`. Makers pay nothing."""
        schedule = await asyncio.to_thread(self.catalog.fetch_fee_schedule, market_id)
        token_price = D(price) if side == Side.BUY else Decimal("1") - D(price)
        taker = schedule.estimate(float(token_price), float(amount), taker=True)
        floor = schedule.min_fee if (schedule.taker_rate or 0) > 0 else None
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, ids.native(VENUE, market_id)), side=side,
            price=D(price), amount=D(amount),
            taker_fee=D(taker) if taker is not None else None, maker_fee=Decimal("0"),
            min_fee=D(floor) if floor is not None else None,
            currency="USDT", info={"schedule": schedule.model_dump()},
        )

    async def close(self) -> None:
        await self.http.close()
        if self._own_catalog:
            self.catalog.close()


# ---------------------------------------------------------------------------
# API key
# ---------------------------------------------------------------------------

AUTH_URL = "https://openapi.opinion.trade/openapi/auth/api-key"

AUTH_ERRORS = {
    11004: "the venue has paused self-service keys; apply with its form instead",
    11005: "this wallet is not a registered Opinion account; connect it on opinion.trade and finish onboarding",
    11011: "the venue rejected the signature",
    11012: "the signature expired; check the computer's clock",
}


def api_key_typed_data(address: str, action: str, timestamp: str) -> dict[str, Any]:
    """The `OpinionApiKeyAuth` message that proves control of the wallet."""
    return {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
            ],
            "OpinionApiKeyAuth": [
                {"name": "walletAddress", "type": "address"},
                {"name": "action", "type": "string"},
                {"name": "timestamp", "type": "string"},
            ],
        },
        "primaryType": "OpinionApiKeyAuth",
        "domain": {"name": "Opinion OpenAPI", "version": "1", "chainId": sig.CHAIN_ID},
        "message": {"walletAddress": address, "action": action, "timestamp": timestamp},
    }


def api_key_headers(signer: WalletSigner, action: str, *, now_s: int | None = None) -> dict[str, str]:
    timestamp = str(now_s if now_s is not None else int(time.time()))
    return {
        "OPINION_ADDRESS": signer.address,
        "OPINION_SIGNATURE": signer.sign_typed_data(api_key_typed_data(signer.address, action, timestamp)),
        "OPINION_TIMESTAMP": timestamp,
    }


def create_api_key(private_key: str, *, url: str = AUTH_URL, client: Any = None) -> str:
    """The wallet's Open API key: created by signing with it, or, when it
    already has one, read back the same way. The private key signs two
    messages at most and goes nowhere."""
    import httpx

    signer = WalletSigner(private_key)
    http = client or httpx.Client(timeout=20)
    try:
        answer = http.post(url, headers=api_key_headers(signer, "create")).json()
        if answer.get("errno") == 11009:   # one key per wallet, and this one has it
            answer = http.get(url, headers=api_key_headers(signer, "get")).json()
    finally:
        if client is None:
            http.close()
    errno = answer.get("errno")
    if errno:
        reason = AUTH_ERRORS.get(errno, str(answer.get("errmsg") or f"error {errno}"))
        raise CredentialsMissing(f"{VENUE}: cannot get an API key: {reason}")
    key = (answer.get("result") or {}).get("apiKey")
    if not key:
        raise CredentialsMissing(f"{VENUE}: the venue answered without an API key")
    return str(key)


def _page(cursor: str | None) -> int:
    if not cursor:
        return 1
    if not cursor.isdigit() or int(cursor) < 1:
        raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
    return int(cursor)
