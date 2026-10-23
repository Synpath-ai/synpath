"""Azuro bets, and the venue's bet records on the YES leg.

Built to `@azuro-org/toolkit` 7.2.0 and the Azuro backend API: bets are
signed off chain as EIP-712 messages and placed on chain by Azuro's relayer
(`azuro_signing`). Signing is checked against digests and signatures that
toolkit and viem produce.

**One credential: the wallet.** Its private key signs every bet and cash-out,
and its bet token pays for them (USDT on Polygon, WETH on Base). Before the
first bet the wallet must allow the relayer to spend the token (`approve()`,
an on-chain transaction the wallet pays gas for; `fetch_allowance()` reads it).
The relayer takes its fee, a flat amount per bet read from `/bet/gas-info`,
from the same allowance.

**A bet is a market order with a worst price.** The pool takes it whole at
the odds it quotes when the bet reaches the chain, or not at all, so every
order here is `market` (or a `limit` that is `ioc` or `fok`, which is the
same thing) and nothing rests. The price given is the worst accepted:
`buy` backs the market's YES outcome with minimum odds `1 / price`; `sell`
backs the other outcome of a two-outcome market with minimum odds
`1 / (1 - price)`. One outcome of a larger condition has no NO to sell. An
order for `n` contracts stakes `n` times the price paid, so at the quoted odds
or better it returns at least `n` (a contract pays one token).

**Placing is asynchronous.** The API answers at once with the bet order's id
in state `Created`; the relayer sends it to the chain and the order becomes
`Accepted` (filled) or `Rejected`. `create_order` returns the order pending;
`fetch_order` and `wait_for_order` follow it. A placed bet cannot be cancelled,
but where the venue offers it a bet can be cashed out before it settles
(`fetch_cashout_quote`, `cash_out`).

**Positions** net a market's accepted, unsettled single bets on the YES leg:
the contracts on the YES outcome less those on the other. A winning bet's
payout is claimed on chain from the pool; this library does not send that
transaction.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime
from decimal import Decimal
from typing import Any

from .. import ids
from ..azuro import Azuro, Chain, chain_of, game_of_condition, parse_market_id
from ..base import AsyncHttpClient, Capability
from ..errors import BadRequest, ExchangeError, MarketNotFound, NotSupported
from ..types import Page
from . import azuro_signing as sig
from .base import TradingExchange
from .credentials import AzuroCredentials
from .errors import InsufficientFunds, InvalidOrder, OrderNotFound, OrderRejected
from .limiter import BudgetLimiter, Priority
from .polymarket_signing import MAX_UINT256, WalletSigner
from .types import (
    Account, Balance, FeeEstimate, Fill, Liquidity, Order, OrderRequest, OrderStatus, OrderType, Position,
    PositionSide, Settlement, Side, TimeInForce, VENUE_ORDER_TYPES,
)

VENUE = "azuro"

DEADLINE_S = 300
"""How long a signed bet stays valid: the venue's SDK default, five minutes."""

ORDER_STATUS = {
    "Created": OrderStatus.PENDING,
    "Placed": OrderStatus.PENDING,
    "Sent": OrderStatus.PENDING,
    "Accepted": OrderStatus.CLOSED,
    "Rejected": OrderStatus.REJECTED,
    "PendingCancel": OrderStatus.PENDING_CANCEL,
    "CancelFailed": OrderStatus.CLOSED,
    "Canceled": OrderStatus.CANCELED,
    "Settled": OrderStatus.CLOSED,
}
"""A bet order's state: `Created` -> `Placed` -> `Sent` -> `Accepted` or
`Rejected`, then `Settled`. `Canceled` is the venue cancelling it (a
cancelled game, a failed relay); `CancelFailed` leaves the bet standing."""

PENDING_STATES = ("Created", "Placed", "Sent", "PendingCancel")
FILLED_STATES = ("Accepted", "CancelFailed", "Settled")

ORDERS_PAGE = 100
MAX_ORDER_PAGES = 50


def D(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def iso_ms(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

def market_of(condition_id: str, outcome_id: str, outcomes: list[str] | None) -> tuple[str, Side]:
    """The Synpath market a bet on this outcome is on, and its side there:
    on a two-outcome condition the lower outcome is YES (`buy`), the other NO
    (`sell`); on a larger one every outcome is its own market's YES. A
    condition whose outcomes are unknown is read as a larger one."""
    if outcomes and len(outcomes) == 2:
        return ids.qualify(VENUE, condition_id), Side.BUY if str(outcome_id) == outcomes[0] else Side.SELL
    return ids.qualify(VENUE, f"{condition_id}-{outcome_id}"), Side.BUY


def yes_price(odds: Decimal, side: Side) -> Decimal | None:
    """The YES price of a bet at these odds, on its side."""
    if odds <= 1:
        return None
    paid = Decimal(1) / odds
    return paid if side == Side.BUY else Decimal(1) - paid


def _leg(row: dict[str, Any]) -> dict[str, Any]:
    legs = row.get("conditions") or []
    return legs[0] if legs and isinstance(legs[0], dict) else {}


def contracts_of_row(row: dict[str, Any]) -> Decimal:
    """What the bet pays if it wins: its stake times its odds."""
    leg = _leg(row)
    if leg.get("potentialAmount") not in (None, "") and row.get("betType") != "COMBO":
        return D(leg["potentialAmount"])
    return D(row.get("amount")) * D(row.get("odds"))


def order_of_row(row: dict[str, Any], *, outcomes: list[str] | None, account: Account | None = None) -> Order:
    """One bet order (`GET /bet/orders/{id}`, `GET /bet/orders/bettor/...`),
    as an order on the YES leg. A combo is reported on its first leg's market,
    marked `info["combo"]`."""
    leg = _leg(row)
    market_id, side = market_of(str(leg.get("conditionId") or ""), str(leg.get("outcomeId") or ""), outcomes)
    state = str(row.get("state") or "")
    status = ORDER_STATUS.get(state, OrderStatus.PENDING)
    odds = D(row.get("odds") or leg.get("price"))
    quantity = contracts_of_row(row)
    filled = quantity if state in FILLED_STATES else Decimal("0")
    price = yes_price(odds, side)
    return Order(
        id=str(row.get("id") or ""),
        venue=VENUE,
        account=account,
        market_id=market_id,
        side=side,
        type=OrderType.MARKET,
        time_in_force=TimeInForce.FOK,
        status=status,
        price=price,
        quantity=quantity,
        filled=filled,
        remaining=quantity - filled if status in (OrderStatus.PENDING, OrderStatus.PENDING_CANCEL) else Decimal("0"),
        average_price=price if filled else None,
        cost=D(row.get("amount")),
        created_at=iso_ms(row.get("createdAt")),
        updated_at=iso_ms(row.get("updatedAt")),
        info={**row, "combo": row.get("betType") == "COMBO"},
    )


def fill_of_row(row: dict[str, Any], *, outcomes: list[str] | None, account: Account | None = None) -> Fill | None:
    """An accepted single bet as one fill: the pool took it whole."""
    if row.get("betType") == "COMBO" or str(row.get("state") or "") not in FILLED_STATES:
        return None
    order = order_of_row(row, outcomes=outcomes, account=account)
    return Fill(
        id=str(row.get("txHash") or row.get("betId") or row.get("id") or ""),
        order_id=order.id,
        venue=VENUE,
        account=account,
        market_id=order.market_id,
        side=order.side,
        price=order.price or Decimal("0"),
        quantity=order.quantity,
        fee=None,
        liquidity=Liquidity.TAKER,
        timestamp=iso_ms(row.get("createdAt")) or 0,
        info=row,
    )


def settlement_of_row(row: dict[str, Any], *, outcomes: list[str] | None, account: Account | None = None) -> Settlement | None:
    """A settled single bet: won, lost, or cancelled (the stake returned)."""
    result = row.get("result")
    if row.get("betType") == "COMBO" or result in (None, ""):
        return None
    order = order_of_row(row, outcomes=outcomes, account=account)
    cost = D(row.get("amount"))
    payout = D(row.get("payout")) if row.get("payout") is not None else (order.quantity if result == "Won" else
                                                                         cost if result == "Canceled" else Decimal("0"))
    return Settlement(
        venue=VENUE, account=account, market_id=order.market_id,
        held=PositionSide.LONG if order.side == Side.BUY else PositionSide.SHORT,
        result=str(result).lower(), won=result == "Won" if result != "Canceled" else None,
        quantity=order.quantity, cost=cost, payout=payout, pnl=payout - cost,
        timestamp=iso_ms(row.get("settledAt")) or iso_ms(row.get("updatedAt")),
        info=row,
    )


def positions_of(rows: list[dict[str, Any]], *, outcomes: dict[str, list[str]], account: Account | None = None) -> list[Position]:
    """Accepted, unsettled single bets netted per market on the YES leg."""
    nets: dict[str, dict[str, Decimal]] = {}
    for row in rows:
        if row.get("betType") == "COMBO" or str(row.get("state") or "") not in ("Accepted", "CancelFailed") \
                or row.get("result") not in (None, ""):
            continue
        leg = _leg(row)
        condition_id = str(leg.get("conditionId") or "")
        market_id, side = market_of(condition_id, str(leg.get("outcomeId") or ""), outcomes.get(condition_id))
        entry = nets.setdefault(market_id, {"yes": Decimal("0"), "no": Decimal("0"), "cost": Decimal("0")})
        entry["yes" if side == Side.BUY else "no"] += contracts_of_row(row)
        entry["cost"] += D(row.get("amount"))
    out = []
    for market_id, entry in nets.items():
        net = entry["yes"] - entry["no"]
        out.append(Position(
            venue=VENUE, account=account, market_id=market_id,
            side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
            contracts=abs(net), inventory_yes=entry["yes"], inventory_no=entry["no"], margin=entry["cost"],
            info={"bets": "accepted, unsettled single bets"},
        ))
    return out


def error_of(exc: ExchangeError) -> ExchangeError:
    body = exc.body if isinstance(exc.body, dict) else {}
    text = str(body.get("errorMessage") or body.get("message") or body.get("error") or exc)
    lower = text.lower()
    if "balance" in lower or "allowance" in lower or "insufficient" in lower:
        return InsufficientFunds(f"{VENUE}: {text} -- the wallet needs the bet token, and an allowance to the "
                                 "relayer for the stake and its fee (approve())", body=exc.body)
    if any(word in lower for word in ("odds", "amount", "limit", "signature", "expired", "nonce")):
        return InvalidOrder(f"{VENUE}: {text}")
    return OrderRejected(f"{VENUE}: {text}", reason=lower or None, info=body, body=exc.body)


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

def check_request(request: OrderRequest) -> None:
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"{VENUE}: {request.type.value} is held by the execution engine, not the venue; submit it through the engine"
        )
    if request.type == OrderType.LIMIT and request.time_in_force not in (TimeInForce.IOC, TimeInForce.FOK):
        raise InvalidOrder(f"{VENUE}: the pool holds no resting orders; send a market order with a worst price, "
                           "or a limit that is ioc or fok")
    if request.time_in_force in (TimeInForce.GTD, TimeInForce.DAY):
        raise InvalidOrder(f"{VENUE}: a bet is taken at once or not at all; there is no {request.time_in_force.value}")
    if request.price is None:
        raise InvalidOrder(f"{VENUE}: a price is required -- a bet is sent with the worst price you give as its minimum odds")
    if request.post_only:
        raise InvalidOrder(f"{VENUE}: the pool only takes; there is no post-only bet")
    if request.reduce_only:
        raise InvalidOrder(f"{VENUE}: bets are not netted at the venue; there is no reduce-only bet")


def selection_of(request: OrderRequest, outcomes: list[str]) -> tuple[str, str, Decimal]:
    """(condition id, outcome id, price paid for the outcome) for a request."""
    condition_id, outcome_id = parse_market_id(ids.native(VENUE, request.market_id))
    price = D(request.price)
    if outcome_id is not None:
        if len(outcomes) == 2:
            raise InvalidOrder(f"{VENUE}: {condition_id} has two outcomes and is one market, {VENUE}:{condition_id}")
        if outcomes and outcome_id not in outcomes:
            raise InvalidOrder(f"{VENUE}: condition {condition_id} has no outcome {outcome_id}")
        if request.side == Side.SELL:
            raise InvalidOrder(f"{VENUE}: one outcome of a larger condition has only YES to buy; the pool sells no NO")
        return condition_id, outcome_id, price
    if len(outcomes) != 2:
        raise InvalidOrder(f"{VENUE}: condition {condition_id} has {len(outcomes)} outcomes; name one: "
                           f"{', '.join(f'{VENUE}:{condition_id}-{o}' for o in outcomes)}")
    if request.side == Side.BUY:
        return condition_id, outcomes[0], price
    return condition_id, outcomes[1], Decimal(1) - price


def build_bet(
    request: OrderRequest, *, chain: Chain, signer: WalletSigner, outcomes: list[str], relayer_fee: int,
    affiliate: str | None = None, nonce: int, now: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """`(body, bet)` for one request, signed, before anything is sent."""
    check_request(request)
    condition_id, outcome_id, paid = selection_of(request, outcomes)
    data = sig.client_data(chain, affiliate=affiliate or signer.address,
                           expires_at=int(time.time() if now is None else now) + DEADLINE_S, relayer_fee=relayer_fee)
    bet = sig.sub_bet(condition_id=condition_id, outcome_id=outcome_id, min_odds=sig.min_odds(paid),
                      amount=sig.stake(D(request.quantity), paid, chain.decimals), nonce=nonce)
    body = sig.wire_bet(chain, bettor=signer.address, data=data, bet=bet, signature=sig.sign_bet(signer, data, bet))
    return body, bet


def relayer_fee_of(info: Any) -> int:
    """The relayer's fee in base units from `/bet/gas-info`: an object in the
    toolkit, a one-element list in the API guide."""
    row = info[0] if isinstance(info, list) and info else info
    if not isinstance(row, dict) or row.get("relayerFeeAmount") in (None, ""):
        raise ExchangeError(f"{VENUE}: no relayer fee in /bet/gas-info: {info}", body=info)
    return int(D(row["relayerFeeAmount"]))


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class AzuroTrading(TradingExchange):
    """Azuro bets.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.azuro import AzuroTrading

    async with AzuroTrading(require("azuro", load_credentials())) as azuro:
        print(await azuro.fetch_balance(), await azuro.fetch_allowance())
    ```
    """

    id = VENUE
    name = "Azuro"
    has: dict[str, Capability] = {
        "create_order": True,
        # One request a bet; not atomic, one result each.
        "create_orders": True,
        # A bet on chain cannot be cancelled; it can be cashed out.
        "cancel_order": False,
        "cancel_orders": False,
        "cancel_all_orders": False,
        "edit_order": False,
        "fetch_order": True,
        # Bets the relayer has not yet put on chain.
        "fetch_open_orders": True,
        "fetch_orders": True,
        "fetch_my_trades": True,
        "fetch_positions": True,
        # The bet token in the wallet, read on chain.
        "fetch_balance": True,
        "fetch_settlements": True,
        "fetch_queue_position": False,
        # The relayer's flat fee per bet.
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
        credentials: AzuroCredentials,
        *,
        account_name: str = "default",
        catalog: Any = None,
        limiter: BudgetLimiter | None = None,
        timeout: float = 30.0,
        client: Any = None,
    ):
        import httpx

        self.credentials = credentials
        self.chain = chain_of(credentials.chain)
        if self.chain.discontinued:
            raise BadRequest(f"{VENUE}: the {self.chain.name} deployment is discontinued and takes no bets")
        self.signer = WalletSigner(credentials.private_key)
        self.account = Account(venue=VENUE, name=account_name)
        self.limiter = limiter or BudgetLimiter(read_per_second=4, write_per_second=4)
        self.rpc_url = credentials.rpc_url or self.chain.rpc_url
        shared = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        self.http = AsyncHttpClient(self.chain.api_url, limiter=None, client=shared, venue=VENUE)
        self._own_catalog = catalog is None
        if catalog is None:
            catalog = Azuro(chain=self.chain)
        self.catalog = catalog
        """The read adapter: conditions' outcomes, which decide market ids and sides."""
        self._outcomes: dict[str, list[str]] = {}
        self._nonce = 0

    @property
    def address(self) -> str:
        return self.signer.address

    # -- plumbing -------------------------------------------------------------

    async def _call(
        self, method: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None,
        kind: str = "read", priority: Priority = Priority.NORMAL,
    ) -> Any:
        await self.limiter.acquire(cost=1, kind=kind, priority=priority)  # type: ignore[arg-type]
        query = {k: v for k, v in (params or {}).items() if v is not None}
        return await self.http.request(method, path, params=query or None, json=body)

    async def _rpc(self, method: str, params: list[Any]) -> Any:
        raw = await self.http.request("POST", self.rpc_url, json={"jsonrpc": "2.0", "id": 1, "method": method,
                                                                  "params": params})
        if not isinstance(raw, dict) or raw.get("error") or "result" not in raw:
            raise ExchangeError(f"{VENUE}: {self.chain.name} rpc {method}: {raw}", body=raw)
        return raw["result"]

    async def outcomes_of(self, condition_ids: list[str]) -> dict[str, list[str]]:
        """Each condition's outcome ids, lowest first, read once and kept."""
        wanted = [c for c in dict.fromkeys(condition_ids) if c and c not in self._outcomes]
        if wanted:
            self._outcomes.update(await asyncio.to_thread(self.catalog.condition_outcomes, wanted))
        return {c: self._outcomes.get(c, []) for c in condition_ids}

    async def _orders_from(self, rows: list[dict[str, Any]]) -> dict[str, list[str]]:
        return await self.outcomes_of([str(_leg(r).get("conditionId") or "") for r in rows])

    def _next_nonce(self) -> int:
        """Milliseconds, as the venue's SDK uses, never repeated in this process."""
        self._nonce = max(self._nonce + 1, int(time.time() * 1000))
        return self._nonce

    async def relayer_fee(self) -> int:
        """The relayer's fee for one bet now, in the bet token's base units."""
        return relayer_fee_of(await self._call("GET", "/bet/gas-info", params={"environment": self.chain.environment}))

    # -- orders ---------------------------------------------------------------

    async def create_order(self, request: OrderRequest) -> Order:
        """Sign one bet and hand it to the relayer. Returns it pending; it is
        on chain once `fetch_order` says `closed` (accepted) or `rejected`."""
        condition_id, _ = parse_market_id(ids.native(VENUE, request.market_id))
        check_request(request)
        outcomes = (await self.outcomes_of([condition_id]))[condition_id]
        if not outcomes:
            raise MarketNotFound(f"{VENUE}: no condition {condition_id}")
        body, bet = build_bet(request, chain=self.chain, signer=self.signer, outcomes=outcomes,
                              relayer_fee=await self.relayer_fee(), affiliate=self.credentials.affiliate,
                              nonce=self._next_nonce())
        try:
            answer = await self._call("POST", "/bet/orders/ordinar", body=body, kind="write") or {}
        except (BadRequest, MarketNotFound) as exc:
            raise error_of(exc) from None
        state = str(answer.get("state") or "")
        if state != "Created" or not answer.get("id"):
            raise OrderRejected(f"{VENUE}: bet not created: {answer.get('errorMessage') or state or answer}",
                                reason=str(answer.get("error") or state or "rejected").lower(), info=answer)
        market_id, side = market_of(condition_id, bet["outcomeId"], outcomes)
        paid = Decimal(sig.ODDS_SCALE) / Decimal(int(bet["minOdds"]))
        quantity = sig.token_amount(bet["amount"], self.chain.decimals) / paid
        return Order(
            id=str(answer["id"]), client_order_id=request.client_order_id, venue=VENUE,
            account=request.account or self.account, market_id=market_id, side=side,
            type=request.type, time_in_force=request.time_in_force, status=OrderStatus.PENDING,
            price=paid if side == Side.BUY else Decimal(1) - paid,
            quantity=quantity.quantize(Decimal("0.000001")), filled=Decimal("0"),
            cost=sig.token_amount(bet["amount"], self.chain.decimals),
            created_at=int(time.time() * 1000), book=request.book, trader=request.trader, tags=request.tags,
            info={"answer": answer, "bet": bet},
        )

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        """Many bets, one request each, one result each in the order asked for."""
        out: list[Order | Exception] = []
        for request in requests:
            try:
                out.append(await self.create_order(request))
            except (InvalidOrder, OrderRejected, InsufficientFunds, MarketNotFound, BadRequest) as exc:
                out.append(exc)
        return out

    async def wait_for_order(self, order_id: str, *, timeout_s: float = 30.0, poll_s: float = 1.0) -> Order:
        """Read the order until the relayer has put it on chain or it was
        refused, as the venue's SDK does; the last read when time runs out."""
        loop = asyncio.get_running_loop()
        stop = loop.time() + timeout_s
        while True:
            order = await self.fetch_order(order_id)
            if order.status != OrderStatus.PENDING or loop.time() >= stop:
                return order
            await asyncio.sleep(poll_s)

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        raise NotSupported(f"{VENUE}: a bet cannot be cancelled once signed; cash it out with cash_out() where offered")

    async def fetch_order(self, order_id: str) -> Order:
        """One bet order by its id (`<bettor>_<nonce>`), in any state."""
        try:
            row = await self._call("GET", f"/bet/orders/{order_id}")
        except MarketNotFound:
            raise OrderNotFound(f"{VENUE}: no bet order {order_id}") from None
        if not isinstance(row, dict) or not row.get("id"):
            raise OrderNotFound(f"{VENUE}: no bet order {order_id}")
        outcomes = await self._orders_from([row])
        return order_of_row(row, outcomes=outcomes.get(str(_leg(row).get("conditionId") or "")), account=self.account)

    async def _bettor_rows(self, *, states: list[str] | None = None, result: list[str] | None = None,
                           offset: int = 0, limit: int | None = None) -> list[dict[str, Any]]:
        """This wallet's bet orders, newest first, page by page."""
        rows: list[dict[str, Any]] = []
        for _ in range(MAX_ORDER_PAGES):
            size = min(ORDERS_PAGE, (limit - len(rows)) if limit else ORDERS_PAGE)
            params: list[tuple[str, Any]] = [("environment", self.chain.environment), ("offset", offset), ("limit", size)]
            params += [("states", s) for s in states or []] + [("result", r) for r in result or []]
            try:
                raw = await self._bettor_page(params)
            except MarketNotFound:
                return rows
            page = [r for r in (raw or {}).get("orders") or [] if isinstance(r, dict)]
            rows += page
            offset += len(page)
            if len(page) < size or (limit and len(rows) >= limit):
                break
        return rows

    async def _bettor_page(self, params: list[tuple[str, Any]]) -> Any:
        await self.limiter.acquire(cost=1, kind="read", priority=Priority.NORMAL)  # type: ignore[arg-type]
        return await self.http.request("GET", f"/bet/orders/bettor/{self.address}", params=params)

    async def fetch_orders(
        self, *, status: str | None = None, market_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Order]:
        """One page of this wallet's bets, newest first. `status` is `open`
        (not yet on chain) or `closed` (accepted, settled or ended); the cursor
        is the venue's offset into the list."""
        if cursor is not None and not cursor.isdigit():
            raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
        states = list(PENDING_STATES) if status == "open" else None
        offset, size = int(cursor or 0), min(limit or 50, ORDERS_PAGE)
        rows = await self._bettor_rows(states=states, offset=offset, limit=size)
        outcomes = await self._orders_from(rows)
        orders = [order_of_row(r, outcomes=outcomes.get(str(_leg(r).get("conditionId") or "")), account=self.account)
                  for r in rows]
        if status == "closed":
            orders = [o for o in orders if o.is_terminal]
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            orders = [o for o in orders if o.market_id == wanted]
        if since:
            orders = [o for o in orders if (o.created_at or 0) >= since]
        return Page(orders, next_cursor=str(offset + len(rows)) if len(rows) >= size else None)

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        """Bets signed and not yet on chain: nothing rests in a pool."""
        rows = await self._bettor_rows(states=list(PENDING_STATES))
        outcomes = await self._orders_from(rows)
        orders = [order_of_row(r, outcomes=outcomes.get(str(_leg(r).get("conditionId") or "")), account=self.account)
                  for r in rows]
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            orders = [o for o in orders if o.market_id == wanted]
        return orders

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        """This wallet's accepted single bets, one fill each, newest first."""
        if cursor is not None and not cursor.isdigit():
            raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
        offset, size = int(cursor or 0), min(limit or 50, ORDERS_PAGE)
        rows = await self._bettor_rows(states=list(FILLED_STATES), offset=offset, limit=size)
        outcomes = await self._orders_from(rows)
        fills = [f for f in (fill_of_row(r, outcomes=outcomes.get(str(_leg(r).get("conditionId") or "")),
                                         account=self.account) for r in rows) if f is not None]
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            fills = [f for f in fills if f.market_id == wanted]
        if order_id:
            fills = [f for f in fills if f.order_id == order_id]
        if since:
            fills = [f for f in fills if f.timestamp >= since]
        return Page(fills, next_cursor=str(offset + len(rows)) if len(rows) >= size else None)

    async def fetch_settlements(
        self, *, market_id: str | None = None, since: int | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page[Settlement]:
        """This wallet's settled single bets: won, lost or cancelled."""
        if cursor is not None and not cursor.isdigit():
            raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
        offset, size = int(cursor or 0), min(limit or 50, ORDERS_PAGE)
        rows = await self._bettor_rows(result=["Won", "Lost", "Canceled"], offset=offset, limit=size)
        outcomes = await self._orders_from(rows)
        out = [s for s in (settlement_of_row(r, outcomes=outcomes.get(str(_leg(r).get("conditionId") or "")),
                                             account=self.account) for r in rows) if s is not None]
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            out = [s for s in out if s.market_id == wanted]
        if since:
            out = [s for s in out if (s.timestamp or 0) >= since]
        return Page(out, next_cursor=str(offset + len(rows)) if len(rows) >= size else None)

    # -- account --------------------------------------------------------------

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        """The wallet's bet token, read on chain. Accepted bets are held by
        the pool, not the wallet, so nothing here is `locked`."""
        raw = await self._rpc("eth_call", [{"to": self.chain.token, "data": sig.balance_calldata(self.address)}, "latest"])
        total = sig.token_amount(int(raw, 16), self.chain.decimals)
        return Balance(
            venue=VENUE, account=account or self.account, currency=self.chain.symbol,
            total=total, available=total, locked=None, buying_power=None,
            timestamp=int(time.time() * 1000), info={"address": self.address, "token": self.chain.token,
                                                     "chain": self.chain.name},
        )

    async def fetch_allowance(self) -> Decimal:
        """What the relayer may spend of the wallet's bet token, read on chain."""
        raw = await self._rpc("eth_call", [{"to": self.chain.token, "data": sig.allowance_calldata(
            self.address, self.chain.relayer)}, "latest"])
        return sig.token_amount(int(raw, 16), self.chain.decimals)

    async def approve(self, amount: Decimal | None = None, *, wait: bool = True, timeout_s: float = 120.0) -> dict[str, Any]:
        """Allow the relayer to spend the wallet's bet token: unlimited by
        default, as the venue's SDK does, or `amount` tokens. An on-chain
        transaction from the wallet, which pays its gas in the chain's coin."""
        value = MAX_UINT256 if amount is None else int(D(amount) * (Decimal(10) ** self.chain.decimals))
        data = sig.approve_calldata(self.chain.relayer, value)
        nonce = int(await self._rpc("eth_getTransactionCount", [self.address, "pending"]), 16)
        gas_price = int(await self._rpc("eth_gasPrice", []), 16)
        priority_fee = int(await self._rpc("eth_maxPriorityFeePerGas", []), 16)
        estimate = int(await self._rpc("eth_estimateGas", [{"from": self.address, "to": self.chain.token, "data": data}]), 16)
        tx = {
            "chainId": self.chain.chain_id, "nonce": nonce, "to": self.chain.token, "value": 0, "data": data,
            "gas": int(estimate * 1.2), "maxFeePerGas": gas_price * 2 + priority_fee,
            "maxPriorityFeePerGas": priority_fee, "type": 2,
        }
        tx_hash = await self._rpc("eth_sendRawTransaction", [self.signer.sign_transaction(tx)])
        if not wait:
            return {"transaction_hash": tx_hash}
        loop = asyncio.get_running_loop()
        stop = loop.time() + timeout_s
        while True:
            receipt = await self._rpc("eth_getTransactionReceipt", [tx_hash])
            if receipt:
                if int(receipt.get("status", "0x0"), 16) != 1:
                    raise OrderRejected(f"{VENUE}: approval {tx_hash} reverted", reason="reverted", info=receipt)
                return {"transaction_hash": tx_hash, "receipt": receipt}
            if loop.time() >= stop:
                raise ExchangeError(f"{VENUE}: approval {tx_hash} unmined after {timeout_s}s; it may yet confirm")
            await asyncio.sleep(2.0)

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        """Accepted, unsettled single bets netted per market on the YES leg."""
        rows = await self._bettor_rows(states=["Accepted", "CancelFailed"])
        rows = [r for r in rows if r.get("result") in (None, "")]
        outcomes = await self._orders_from(rows)
        positions = positions_of(rows, outcomes=outcomes, account=self.account)
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            positions = [p for p in positions if p.market_id == wanted]
        if event_id:
            game = ids.native(VENUE, event_id)
            positions = [p for p in positions
                         if game_of_condition(parse_market_id(ids.split(p.market_id)[1])[0]) == game]
        return positions

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, quantity: Decimal) -> FeeEstimate:
        """The relayer's flat fee for one bet, in the bet token: there is no
        fee on top of the pool's odds, whose margin is already in them."""
        fee = sig.token_amount(await self.relayer_fee(), self.chain.decimals)
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, ids.native(VENUE, market_id)), side=side,
            price=D(price), quantity=D(quantity), taker_fee=fee, maker_fee=None,
            currency=self.chain.symbol, info={"charged": "per bet, to the relayer, for the transaction"},
        )

    # -- cash-out -------------------------------------------------------------

    async def fetch_cashout_quote(self, bet_id: str | int) -> dict[str, Any] | None:
        """What the venue offers now to close one accepted bet early
        (`/cashout/get-calculation`), by the bet's on-chain id (`Order.info["betId"]`);
        `None` when it offers nothing."""
        try:
            raw = await self._call("POST", "/cashout/get-calculation", body={
                "environment": self.chain.environment, "owner": self.address, "betId": str(bet_id),
            })
        except MarketNotFound:
            return None
        return raw if isinstance(raw, dict) and raw.get("calculationId") else None

    async def cash_out(self, bet_id: str | int, *, quote: dict[str, Any] | None = None) -> dict[str, Any]:
        """Close one accepted bet at the venue's cash-out offer: the offer is
        read (or given), signed by the wallet with its odds as the minimum,
        and sent (`/cashout/create`). Returns the cash-out order; read it on
        with `fetch_cashout`."""
        quote = quote or await self.fetch_cashout_quote(bet_id)
        if not quote:
            raise OrderRejected(f"{VENUE}: no cash-out offered on bet {bet_id}", reason="no_cashout")
        message = sig.cashout_message(self.chain, bet_id=bet_id, cashout_odds=str(quote["cashoutOdds"]),
                                      expires_at=int(quote["expiredAt"]))
        body = sig.wire_cashout(self.chain, calculation_id=str(quote["calculationId"]), attention=message["attention"],
                                signature=sig.sign_cashout(self.signer, self.chain, message))
        try:
            answer = await self._call("POST", "/cashout/create", body=body, kind="write") or {}
        except (BadRequest, MarketNotFound) as exc:
            raise error_of(exc) from None
        if str(answer.get("state") or "").upper() == "REJECTED":
            raise OrderRejected(f"{VENUE}: cash-out refused: {answer.get('errorMessage') or answer}",
                                reason="cashout_rejected", info=answer)
        return answer

    async def fetch_cashout(self, cashout_id: str) -> dict[str, Any]:
        """One cash-out order: its state and, once done, its transaction."""
        try:
            return await self._call("GET", f"/cashout/{cashout_id}") or {}
        except MarketNotFound:
            raise OrderNotFound(f"{VENUE}: no cash-out {cashout_id}") from None

    async def close(self) -> None:
        await self.http.close()
        if self._own_catalog:
            self.catalog.close()


__all__ = ["AzuroTrading", "order_of_row", "fill_of_row", "settlement_of_row", "positions_of", "build_bet"]
