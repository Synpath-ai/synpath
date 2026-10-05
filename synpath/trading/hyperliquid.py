"""Hyperliquid outcome trading (HIP-4): order entry, and the venue's order
and fill records as this library's types.

An outcome trades as two coins, `#<10 * outcome>` for YES and
`#<10 * outcome + 1>` for NO, each with its own book, orders and fills, and
the asset id `100_000_000 + <coin number>` on the wire. Every record here is
turned onto the market's YES leg: buying NO at 0.30 is selling at 0.70, as on
Polymarket. An order goes to the token it buys -- `buy` buys YES, `sell`
buys NO -- and with `reduce_only` it sells the other token held instead.

Fills carry a direction (`dir`) besides their side. `Buy` and `Sell` are
trades. `Split Outcome` and `Merge Outcome` turn quote into a YES and NO
pair and back, and `Settlement` is the payout when an outcome resolves;
none of the three is a trade, and `fill_of` returns `None` for them.

Orders are signed as L1 actions (`hyperliquid_signing`). The key may be the
account's own wallet or an API wallet the account approved; with an API
wallet, `HYPERLIQUID_ACCOUNT_ADDRESS` names the account, since that is whose
orders and balances are read.

Fees are charged only when a fill closes a position (and at settlement),
in the quote token, USDC. Prices take at most five significant figures and
sizes are whole contracts, at least 10 USDC an order.
"""
from __future__ import annotations

import asyncio
import hashlib
import time
from decimal import Decimal
from typing import Any

from .. import ids
from ..base import AsyncHttpClient, Capability
from ..errors import BadRequest, ExchangeError
from ..types import Page
from . import hyperliquid_signing as sig
from .base import TradingExchange
from .credentials import HyperliquidCredentials
from .errors import InsufficientFunds, InvalidOrder, OrderNotFound, OrderRejected, PermissionDenied
from .limiter import BudgetLimiter, Priority
from .polymarket import to_yes_leg, wire_leg
from .polymarket_signing import WalletSigner
from .types import (
    Account, Balance, FeeEstimate, Fill, Liquidity, Order, OrderRequest, OrderStatus, OrderType, Position,
    PositionSide, SettlementState, Side, TimeInForce, VENUE_ORDER_TYPES,
)

VENUE = "hyperliquid"
MAINNET_URL = "https://api.hyperliquid.xyz"
TESTNET_URL = "https://api.hyperliquid-testnet.xyz"

OUTCOME_ASSET_OFFSET = 100_000_000
"""An outcome coin's asset id is this plus its number (`#75440` -> 100_075_440)."""

MIN_NOTIONAL = Decimal("10")
"""The venue's minimum order value, in the quote token, counted on the token bought."""

MAX_SIGNIFICANT = 5
"""Significant figures a price may have."""

QUOTE = "USDC"

TRADE_DIRECTIONS = frozenset({"Buy", "Sell"})
"""Fill directions that are trades. The others (`Split Outcome`, `Merge
Outcome`, `Settlement`) move quote and outcome tokens without one."""

TIME_IN_FORCE = {"Gtc": TimeInForce.GTC, "Ioc": TimeInForce.IOC, "Alo": TimeInForce.GTC, "FrontendMarket": TimeInForce.IOC}
"""The venue's `tif`. `Alo` (add liquidity only) is a post-only GTC."""


def D(value: Any) -> Decimal:
    return Decimal(str(value)) if value not in (None, "") else Decimal("0")


def outcome_coin(coin: str) -> tuple[str, str] | None:
    """`#75441` -> (`"7544"`, `"no"`): the outcome a coin belongs to and which
    side it is, or `None` for anything that is not an outcome coin."""
    if not isinstance(coin, str) or not coin.startswith("#") or not coin[1:].isdigit():
        return None
    encoded = int(coin[1:])
    return str(encoded // 10), ("no" if encoded % 10 else "yes")


def status_of(word: str) -> OrderStatus:
    """The venue's order status word. Rejections and cancellations come in
    many named kinds (`insufficientSpotBalanceRejected`,
    `reduceOnlyCanceled`, ...); the suffix says which family."""
    if word in ("open", "triggered"):
        return OrderStatus.OPEN
    if word == "filled":
        return OrderStatus.CLOSED
    if word.endswith("Rejected") or word == "rejected":
        return OrderStatus.REJECTED
    if word.endswith("Canceled") or word == "canceled":
        return OrderStatus.CANCELED
    return OrderStatus.OPEN


def order_of(raw: dict[str, Any], *, account: Account | None = None) -> Order | None:
    """An order update (`{"order": ..., "status": ..., "statusTimestamp": ...}`)
    or a bare order as an `Order` on the YES leg; `None` when it is not on an
    outcome coin. `sz` is what is left, `origSz` what was asked."""
    order = raw.get("order", raw)
    coin = outcome_coin(str(order.get("coin") or ""))
    if coin is None:
        return None
    native, outcome = coin
    venue_side = "BUY" if order.get("side") == "B" else "SELL"
    price = D(order.get("limitPx")) if order.get("limitPx") not in (None, "") else None
    side, yes_price = to_yes_leg(outcome, venue_side, price)
    word = str(raw.get("status") or "open")
    status = status_of(word)
    amount = D(order.get("origSz") or order.get("sz"))
    left = D(order.get("sz"))
    filled = amount if status == OrderStatus.CLOSED else max(Decimal("0"), amount - left)
    tif = TIME_IN_FORCE.get(str(order.get("tif") or ""), TimeInForce.GTC)
    return Order(
        id=str(order.get("oid") or ""),
        client_order_id=order.get("cloid") or None,
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, native),
        side=side,
        type=OrderType.MARKET if str(order.get("orderType") or "").lower() == "market" else OrderType.LIMIT,
        time_in_force=tif,
        status=status,
        price=yes_price,
        amount=amount,
        filled=filled,
        remaining=left if status == OrderStatus.OPEN else Decimal("0"),
        post_only=order.get("tif") == "Alo",
        reduce_only=bool(order.get("reduceOnly")),
        created_at=int(order["timestamp"]) if order.get("timestamp") else None,
        updated_at=int(raw["statusTimestamp"]) if raw.get("statusTimestamp") else None,
        info={**raw, "outcome_side": outcome, "native_status": word},
    )


def fill_of(raw: dict[str, Any], *, account: Account | None = None) -> Fill | None:
    """A fill as a `Fill` on the YES leg, or `None` for a split, a merge, a
    settlement or a coin that is not an outcome. `crossed` is whether the
    fill took liquidity. The fee is in `feeToken`, charged only when the
    fill closes a position."""
    coin = outcome_coin(str(raw.get("coin") or ""))
    if coin is None or raw.get("dir") not in TRADE_DIRECTIONS:
        return None
    native, outcome = coin
    venue_side = "BUY" if raw.get("side") == "B" else "SELL"
    side, price = to_yes_leg(outcome, venue_side, D(raw.get("px")))
    crossed = raw.get("crossed")
    return Fill(
        id=str(raw.get("tid") or raw.get("hash") or ""),
        order_id=str(raw.get("oid") or ""),
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, native),
        side=side,
        price=price or Decimal("0"),
        amount=D(raw.get("sz")),
        fee=D(raw.get("fee")) if raw.get("fee") not in (None, "") else None,
        fee_currency=raw.get("feeToken") or None,
        liquidity=Liquidity.TAKER if crossed is True else Liquidity.MAKER if crossed is False else Liquidity.UNKNOWN,
        settlement=SettlementState.CONFIRMED,
        timestamp=int(raw.get("time") or 0),
        info={**raw, "outcome_side": outcome},
    )


def error_of(message: str, *, body: Any = None) -> ExchangeError:
    """The venue's refusal, in its words, as a typed error."""
    text = message.lower()
    if "user or api wallet" in text and "does not exist" in text:
        return PermissionDenied(
            f"{VENUE}: {message} -- the signing wallet is not a Hyperliquid account, or an API wallet this "
            f"account has not approved (set HYPERLIQUID_ACCOUNT_ADDRESS to the account it trades for)", body=body,
        )
    if "insufficient" in text or "balance" in text:
        return InsufficientFunds(f"{VENUE}: {message}", body=body)
    if "never placed" in text or "already canceled" in text or "unknownoid" in text:
        return OrderNotFound(f"{VENUE}: {message}", body=body)
    if "minimum" in text or "tick" in text or "price" in text or "size" in text or "invalid" in text:
        return InvalidOrder(f"{VENUE}: {message}")
    return OrderRejected(f"{VENUE}: {message}", reason=None, info=body if isinstance(body, dict) else {}, body=body)


def asset_of(native: str, outcome: str) -> int:
    """The wire asset id of one side of an outcome."""
    return OUTCOME_ASSET_OFFSET + 10 * int(native) + (1 if outcome == "no" else 0)


def coin_of(native: str, outcome: str) -> str:
    return f"#{10 * int(native) + (1 if outcome == 'no' else 0)}"


def cloid_of(client_order_id: str | None) -> str | None:
    """The venue's client order id: 16 bytes as 0x-hex. One already in that
    form is sent as it is; any other string is hashed into one, so the same
    id always maps to the same cloid."""
    if not client_order_id:
        return None
    text = client_order_id.lower()
    if text.startswith("0x") and len(text) == 34 and all(c in "0123456789abcdef" for c in text[2:]):
        return text
    return "0x" + hashlib.sha256(client_order_id.encode()).hexdigest()[:32]


def check_request(request: OrderRequest) -> None:
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"{VENUE}: {request.type.value} is held by the execution engine, not the venue; submit it through the engine"
        )
    if request.time_in_force not in (TimeInForce.GTC, TimeInForce.IOC):
        raise InvalidOrder(
            f"{VENUE}: {request.time_in_force.value} is not available -- the venue rests limits until cancelled; "
            f"gtc, or ioc"
        )
    if request.price is None:
        raise InvalidOrder(f"{VENUE}: a price is required -- a market order is sent as an IOC limit at the worst price you give")
    if request.post_only and (request.type == OrderType.MARKET or request.time_in_force == TimeInForce.IOC):
        raise InvalidOrder(f"{VENUE}: a post-only order rests; it cannot be a market or IOC order")


def tif_of(request: OrderRequest) -> str:
    if request.post_only:
        return "Alo"
    if request.type == OrderType.MARKET or request.time_in_force == TimeInForce.IOC:
        return "Ioc"
    return "Gtc"


def build_order_wire(request: OrderRequest) -> tuple[dict[str, Any], str, str, Decimal]:
    """The order's wire form, before signing: `(wire, native, outcome, token
    price)`. Refuses what the venue would refuse, with the reason, rather
    than rounding a price or a size into a different order."""
    try:
        native = ids.native(VENUE, request.market_id)
    except BadRequest as exc:
        raise InvalidOrder(str(exc)) from None
    if not native.isdigit():
        raise InvalidOrder(f"{VENUE}: {native!r} is not an outcome id; a question's outcomes are the markets")
    check_request(request)
    outcome, venue_side = wire_leg(request)
    yes_price = Decimal(str(request.price))
    price = yes_price if outcome == "yes" else Decimal("1") - yes_price
    if not Decimal("0") < price < Decimal("1"):
        raise InvalidOrder(f"{VENUE}: price {yes_price} must be strictly between 0 and 1")
    if sig.significant_figures(price) > MAX_SIGNIFICANT:
        raise InvalidOrder(f"{VENUE}: {price} has more than {MAX_SIGNIFICANT} significant figures")
    amount = Decimal(str(request.amount))
    if amount <= 0 or amount != amount.to_integral_value():
        raise InvalidOrder(f"{VENUE}: {amount} contracts -- the venue trades whole contracts")
    if price * amount < MIN_NOTIONAL:
        raise InvalidOrder(
            f"{VENUE}: {amount} at {price} is {price * amount} {QUOTE}; the venue's minimum order is {MIN_NOTIONAL} {QUOTE}"
        )
    try:
        wire: dict[str, Any] = {
            "a": asset_of(native, outcome),
            "b": venue_side == "BUY",
            "p": sig.to_wire(price),
            "s": sig.to_wire(amount),
            "r": False,
            "t": {"limit": {"tif": tif_of(request)}},
        }
    except ValueError as exc:
        raise InvalidOrder(str(exc)) from None
    cloid = cloid_of(request.client_order_id)
    if cloid:
        wire["c"] = cloid
    return wire, native, outcome, price


def position_of(rows: list[dict[str, Any]], *, market_id: str, account: Account | None = None) -> Position:
    """One outcome's position from its two token balances (`+<coin>`), netted
    on the YES leg, with both inventories kept. `entryNtl` is what the held
    tokens cost; tokens an open order holds count as held."""
    by_side = {"yes": Decimal("0"), "no": Decimal("0")}
    cost = {"yes": Decimal("0"), "no": Decimal("0")}
    for row in rows:
        coin = outcome_coin("#" + str(row.get("coin") or "").removeprefix("+"))
        if coin is None:
            continue
        by_side[coin[1]] += D(row.get("total"))
        cost[coin[1]] += D(row.get("entryNtl"))
    net = by_side["yes"] - by_side["no"]
    lead = "yes" if net >= 0 else "no"
    held = by_side[lead]
    entry = (cost[lead] / held).quantize(Decimal("1e-8")) if held > 0 and cost[lead] > 0 else None
    return Position(
        venue=VENUE,
        account=account,
        market_id=market_id,
        side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
        contracts=abs(net),
        inventory_yes=by_side["yes"],
        inventory_no=by_side["no"],
        entry_price=(entry if lead == "yes" else Decimal("1") - entry) if entry is not None else None,
        info={"rows": rows},
    )


class HyperliquidTrading(TradingExchange):
    """Hyperliquid outcome order entry.

    Signing is checked byte for byte against the venue's own SDK.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.hyperliquid import HyperliquidTrading

    async with HyperliquidTrading(require("hyperliquid", load_credentials())) as hl:
        print(await hl.fetch_balance())
    ```
    """

    id = VENUE
    name = "Hyperliquid"
    has: dict[str, Capability] = {
        "create_order": True,
        # One signed action holds them all.
        "create_orders": True,
        "cancel_order": True,
        "cancel_orders": True,
        "cancel_all_orders": True,
        "edit_order": False,
        "fetch_order": True,
        "fetch_open_orders": True,
        # The venue's recent history (up to 2000 orders), no paging.
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
        credentials: HyperliquidCredentials,
        *,
        account_name: str = "default",
        url: str | None = None,
        catalog: Any = None,
        limiter: BudgetLimiter | None = None,
        timeout: float = 30.0,
        client: Any = None,
    ):
        import httpx

        self.credentials = credentials
        self.signer = WalletSigner(credentials.private_key)
        self.address = credentials.address
        """The account whose orders, fills and balances these are."""
        self.mainnet = not credentials.testnet
        self.account = Account(venue=VENUE, name=account_name)
        # 1200 request weight a minute per IP; an action weighs 1, most info reads 20.
        self.limiter = limiter or BudgetLimiter(read_per_second=1, write_per_second=10)
        base = url or (MAINNET_URL if self.mainnet else TESTNET_URL)
        self.http = AsyncHttpClient(base, limiter=None, client=client or httpx.AsyncClient(timeout=timeout), venue=VENUE)
        self._own_catalog = catalog is None
        if catalog is None:
            from ..hyperliquid import Hyperliquid

            catalog = Hyperliquid(testnet=not self.mainnet)
        self.catalog = catalog
        """The read adapter: fee scales for estimates."""
        self._last_nonce = 0
        self._nonce_lock = asyncio.Lock()

    # -- plumbing -------------------------------------------------------------

    async def _info(self, body: dict[str, Any]) -> Any:
        await self.limiter.acquire(cost=1, kind="read")  # type: ignore[arg-type]
        return await self.http.post("/info", json=body)

    async def _nonce(self) -> int:
        """Milliseconds, strictly increasing: the venue refuses a nonce it has seen."""
        async with self._nonce_lock:
            nonce = max(int(time.time() * 1000), self._last_nonce + 1)
            self._last_nonce = nonce
            return nonce

    async def _exchange(self, action: dict[str, Any], *, priority: Priority = Priority.NORMAL) -> Any:
        """Sign and send one action; the venue's `response` on success."""
        await self.limiter.acquire(cost=1, kind="write", priority=priority)  # type: ignore[arg-type]
        nonce = await self._nonce()
        body = {
            "action": action,
            "nonce": nonce,
            "signature": sig.sign_l1_action(self.signer, action, nonce=nonce, mainnet=self.mainnet),
            "vaultAddress": None,
        }
        payload = await self.http.post("/exchange", json=body)
        if not isinstance(payload, dict) or payload.get("status") != "ok":
            message = payload.get("response") if isinstance(payload, dict) else payload
            raise error_of(str(message), body=payload)
        return payload.get("response") or {}

    # -- orders ---------------------------------------------------------------

    def _placed(self, request: OrderRequest, native: str, status: Any, wire: dict[str, Any]) -> Order | Exception:
        """One entry of an order action's `statuses` as an `Order`, or the
        refusal as an exception."""
        if not isinstance(status, dict):
            return error_of(str(status), body=status)
        if "error" in status:
            return error_of(str(status["error"]), body=status)
        resting, filled = status.get("resting"), status.get("filled")
        raw = resting or filled or {}
        oid = str(raw.get("oid") or "")
        amount = Decimal(str(request.amount))
        done = D(filled.get("totalSz")) if filled else Decimal("0")
        immediate = wire["t"]["limit"]["tif"] == "Ioc"
        if filled and done >= amount:
            state = OrderStatus.CLOSED
        elif resting:
            state = OrderStatus.OPEN
        elif immediate:
            state = OrderStatus.CANCELED  # the unmatched rest of an IOC
        else:
            state = OrderStatus.OPEN
        avg = D(filled.get("avgPx")) if filled and filled.get("avgPx") else None
        outcome = "yes" if wire["a"] % 10 == 0 else "no"
        return Order(
            id=oid,
            client_order_id=request.client_order_id,
            venue=VENUE,
            account=request.account or self.account,
            market_id=ids.qualify(VENUE, native),
            side=request.side,
            type=request.type,
            time_in_force=request.time_in_force,
            status=state,
            price=Decimal(str(request.price)),
            amount=amount,
            filled=done,
            remaining=amount - done if state == OrderStatus.OPEN else Decimal("0"),
            average_price=(avg if outcome == "yes" else Decimal("1") - avg) if avg is not None else None,
            post_only=request.post_only,
            reduce_only=request.reduce_only,
            created_at=int(time.time() * 1000),
            book=request.book,
            trader=request.trader,
            tags=request.tags,
            info={"status": status, "wire": wire, "cloid": wire.get("c")},
        )

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        """Many orders in one signed action. Each comes back placed or with
        the venue's reason; one refused order does not stop the others."""
        built: list[tuple[OrderRequest, dict[str, Any], str] | Exception] = []
        for request in requests:
            try:
                wire, native, _, _ = build_order_wire(request)
                built.append((request, wire, native))
            except InvalidOrder as exc:
                built.append(exc)
        good = [item for item in built if not isinstance(item, Exception)]
        statuses: list[Any] = []
        if good:
            response = await self._exchange({"type": "order", "orders": [w for _, w, _ in good], "grouping": "na"})
            statuses = list(((response.get("data") or {}).get("statuses")) or [])
        results: list[Order | Exception] = []
        index = 0
        for item in built:
            if isinstance(item, Exception):
                results.append(item)
                continue
            request, wire, native = item
            status = statuses[index] if index < len(statuses) else {"error": "no status returned"}
            index += 1
            results.append(self._placed(request, native, status, wire))
        return results

    async def create_order(self, request: OrderRequest) -> Order:
        """Sign and place one order. A market order is an IOC limit at the
        worst price given; whatever does not match at once is cancelled by
        the venue."""
        result = (await self.create_orders([request]))[0]
        if isinstance(result, Exception):
            raise result
        return result

    async def _cancel_many(self, targets: list[tuple[int, int]]) -> list[Any]:
        if not targets:
            return []
        response = await self._exchange(
            {"type": "cancel", "cancels": [{"a": asset, "o": oid} for asset, oid in targets]}, priority=Priority.HIGH,
        )
        return list(((response.get("data") or {}).get("statuses")) or [])

    async def _asset_of_order(self, order_id: str) -> int:
        order = await self._order_record(order_id)
        coin = outcome_coin(str((order.get("order") or {}).get("coin") or ""))
        if coin is None:
            raise OrderNotFound(f"{VENUE}: order {order_id} is not on an outcome")
        return asset_of(*coin)

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        """Cancel one order and read it back. The venue keys a cancel on the
        coin as well as the id; it is read from the order when not known."""
        asset = await self._asset_of_order(order_id)
        statuses = await self._cancel_many([(asset, int(order_id))])
        status = statuses[0] if statuses else None
        order = await self.fetch_order(order_id)
        if isinstance(status, dict) and "error" in status and not order.is_terminal:
            raise error_of(str(status["error"]), body=status)
        return order.model_copy(update={"info": {**order.info, "cancel": status}})

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        results: list[Order | Exception] = []
        for order_id in order_ids:
            try:
                results.append(await self.cancel_order(order_id, market_id=market_id))
            except Exception as exc:  # noqa: BLE001 - one refusal must not hide the others
                results.append(exc)
        return results

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int:
        """Every open outcome order, or every one on a market, in one action.
        Returns how many the venue cancelled."""
        targets = []
        for order in await self.fetch_open_orders(market_id=market_id):
            coin = order.info.get("order", order.info).get("coin") if isinstance(order.info, dict) else None
            found = outcome_coin(str(coin or ""))
            if found is not None:
                targets.append((asset_of(*found), int(order.id)))
        statuses = await self._cancel_many(targets)
        return sum(1 for status in statuses if status == "success")

    async def _order_record(self, order_id: str) -> dict[str, Any]:
        try:
            oid: int | str = int(order_id)
        except ValueError:
            oid = order_id  # a cloid
        raw = await self._info({"type": "orderStatus", "user": self.address, "oid": oid}) or {}
        if raw.get("status") != "order" or not isinstance(raw.get("order"), dict):
            raise OrderNotFound(f"{VENUE}: no order {order_id} ({raw.get('status')})")
        return raw["order"]

    async def fetch_order(self, order_id: str) -> Order:
        """One order by id (or by its cloid), in its latest state."""
        order = order_of(await self._order_record(order_id), account=self.account)
        if order is None:
            raise OrderNotFound(f"{VENUE}: order {order_id} is not on an outcome")
        return order

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        rows = await self._info({"type": "frontendOpenOrders", "user": self.address}) or []
        orders = [o for o in (order_of({"order": row, "status": "open"}, account=self.account) for row in rows) if o]
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            orders = [o for o in orders if o.market_id == wanted]
        return orders

    async def fetch_orders(
        self, *, status: str | None = None, market_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Order]:
        """This account's recent orders on outcomes, newest first, each in its
        latest state: the venue's history holds up to 2000 and has no paging."""
        if cursor:
            raise BadRequest(f"{VENUE}: the venue's order history has no further pages")
        rows = await self._info({"type": "historicalOrders", "user": self.address}) or []
        latest: dict[str, Order] = {}
        for row in rows:
            order = order_of(row, account=self.account)
            if order is None:
                continue
            held = latest.get(order.id)
            if held is None or (order.updated_at or 0) >= (held.updated_at or 0):
                latest[order.id] = order
        orders = sorted(latest.values(), key=lambda o: o.updated_at or o.created_at or 0, reverse=True)
        if status:
            orders = [o for o in orders if o.status.value == status]
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            orders = [o for o in orders if o.market_id == wanted]
        if since:
            orders = [o for o in orders if (o.created_at or 0) >= since]
        return Page(orders[:limit] if limit else orders, next_cursor=None)

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        """This account's fills on outcomes, newest first. Splits, merges and
        settlements are left out. With `since` the venue is asked from that
        time; without, its most recent fills."""
        if cursor:
            raise BadRequest(f"{VENUE}: fills have no cursor; use `since`")
        if since is not None:
            rows = await self._info({"type": "userFillsByTime", "user": self.address, "startTime": int(since)}) or []
        else:
            rows = await self._info({"type": "userFills", "user": self.address}) or []
        fills = [f for f in (fill_of(row, account=self.account) for row in rows) if f is not None]
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            fills = [f for f in fills if f.market_id == wanted]
        if order_id:
            fills = [f for f in fills if f.order_id == order_id]
        fills.sort(key=lambda f: f.timestamp, reverse=True)
        return Page(fills[:limit] if limit else fills, next_cursor=None)

    # -- account --------------------------------------------------------------

    async def _balances(self) -> list[dict[str, Any]]:
        raw = await self._info({"type": "spotClearinghouseState", "user": self.address}) or {}
        return list(raw.get("balances") or [])

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        """The account's USDC on the spot side, where outcomes trade: what
        open orders hold is `locked`."""
        rows = await self._balances()
        usdc = next((row for row in rows if row.get("coin") == QUOTE), {})
        total, hold = D(usdc.get("total")), D(usdc.get("hold"))
        return Balance(
            venue=VENUE, account=account or self.account, currency=QUOTE,
            total=total, available=total - hold, locked=hold, buying_power=total - hold,
            timestamp=int(time.time() * 1000), info={"balances": rows},
        )

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        """Outcome token balances, netted per market on the YES leg.
        `event_id` keeps the outcomes of one question."""
        by_market: dict[str, list[dict[str, Any]]] = {}
        for row in await self._balances():
            coin = outcome_coin("#" + str(row.get("coin") or "").removeprefix("+")) if str(row.get("coin", "")).startswith("+") else None
            if coin is None or D(row.get("total")) == 0:
                continue
            by_market.setdefault(coin[0], []).append(row)
        positions = [
            position_of(rows, market_id=ids.qualify(VENUE, native), account=self.account)
            for native, rows in by_market.items()
        ]
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            positions = [p for p in positions if p.market_id == wanted]
        if event_id:
            event = await asyncio.to_thread(self.catalog.catalog)
            members = {m.id for m in (event.events.get(ids.native(VENUE, event_id)) or _NoEvent).markets}
            positions = [p for p in positions if p.market_id in members]
        return positions

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, amount: Decimal) -> FeeEstimate:
        """What a fill of `amount` at the YES `price` is charged *if it closes
        a position*: the market's rate on the notional of the token traded,
        at the lowest volume tier. A fill that opens a position pays nothing,
        which `info["opening_fee"]` says."""
        schedule = await asyncio.to_thread(self.catalog.fetch_fee_schedule, market_id)
        token_price = Decimal(str(price)) if side == Side.BUY else Decimal("1") - Decimal(str(price))
        notional = token_price * Decimal(str(amount))
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, ids.native(VENUE, market_id)), side=side,
            price=Decimal(str(price)), amount=Decimal(str(amount)),
            taker_fee=(notional * Decimal(str(schedule.taker_rate or 0))).quantize(Decimal("1e-6")),
            maker_fee=(notional * Decimal(str(schedule.maker_rate or 0))).quantize(Decimal("1e-6")),
            currency=QUOTE,
            info={"schedule": schedule.model_dump(), "opening_fee": "0", "charged_on": "closing fills only"},
        )

    async def close(self) -> None:
        await self.http.close()
        if self._own_catalog:
            self.catalog.close()


class _NoEvent:
    markets: list = []
