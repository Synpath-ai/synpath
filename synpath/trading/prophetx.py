"""ProphetX order entry, and the venue's order and fill records on the YES leg.

Built to ProphetX's Trading API reference (docs.prophetx.co, the v4 `/mm`
routes).

**One key pair, one session.** The access key and secret key (self-service
for any KYC-approved account, under API Integration) are exchanged at `POST
/auth/login` for a bearer token that lasts ten minutes; `ProphetXSession`
renews it with the refresh token. ProphetX counts live sessions per access
key, and a second login adds one rather than replacing the first, so the
adapter, its catalog and the streams share one session.

**Every order is a buy of one side.** ProphetX has no sell: each side of a
line is its own instrument (`strike_id`). `buy` buys the line's first side
(this library's YES) at the price given; `sell` buys the second side at one
minus it. The price goes to ProphetX as American odds on its price ladder
(`GET /v4/mm/get_price_ladder`), snapped to the ladder on the cheaper side so
the limit is never worse than the one asked for. The quantity ProphetX takes
is the stake in USD, to the cent, at least $1: an order for `n` contracts
stakes `n * P`, where `P` is the implied probability of the ladder price,
rounded down to the cent. A fill's contracts are its stake over its price's
probability, which is its stake plus its profit.

**Time in force.** A limit rests until filled or cancelled (`gtc`); `post_only`
is ProphetX's `ALO`, rejected after the fact (`wiped`) if it would cross. A
market order, or an `ioc` limit, is ProphetX's `fillOrKill`: it takes what is
there at the price given and the rest is cancelled about two seconds after its
last fill. ProphetX has no all-or-none (`fok`) and no expiry (`gtd`).

**Answers are early, reads are late.** `submit_order` answers before matching
runs (status `inactive`, so `pending` here); `get_order` and the history read
a replica that can lag by seconds. The account stream is the fast signal, and
`cancel_order` is final the moment it answers.

**Positions** are built from the account's orders, since ProphetX has no
positions route: each live order's matched stake, in contracts at its limit
price, netted per line on the YES leg. A fill at a better price than the limit
holds a little more than that.

**Fees** are charged per order and reported on orders and fills (`fee`);
ProphetX publishes no rate in its API documentation, so there is no estimate.

The default rate tier allows one request a second per endpoint and turns the
batch endpoints off, so many orders or cancels are sent one at a time.
"""
from __future__ import annotations

import asyncio
import re
import time
import uuid
from decimal import ROUND_DOWN, Decimal
from fractions import Fraction
from typing import Any

from .. import ids
from ..base import AsyncHttpClient, Capability
from ..errors import AuthenticationError, BadRequest, ExchangeError, MarketNotFound
from ..prophetx import (
    API_URL, SANDBOX_URL, ProphetXSession, implied_probability, iso_ms, line_native_id, parse_line_id,
    unwrap,
)
from ..types import Page
from .base import TradingExchange
from .credentials import ProphetXCredentials
from .errors import (
    CredentialsMissing, DuplicateClientOrderId, InsufficientFunds, InvalidOrder, MarketHalted, OrderNotFound,
    OrderRejected,
)
from .limiter import BudgetLimiter, Priority
from .types import (
    Account, Balance, Fill, Liquidity, Order, OrderRequest, OrderStatus, OrderType, Position, PositionSide, Side,
    TimeInForce, VENUE_ORDER_TYPES,
)

VENUE = "prophetx"
CENT = Decimal("0.01")
CONTRACT_STEP = Decimal("0.000001")
MIN_STAKE = Decimal("1")
"""ProphetX's smallest stake, in USD."""

EXTERNAL_ID = re.compile(r"^[A-Za-z0-9_-]{1,100}$")
"""What ProphetX accepts as an `external_id`: up to 100 letters, digits, `-` and `_`."""

HISTORY_WINDOW_S = 60 * 86400
"""The widest creation window one order-history read takes."""

HISTORY_PAGE = 1000
MAX_PAGES = 50

CANCEL_RETRIES = 4
"""How many times a cancel is retried while ProphetX says the order is still
being placed or matched (`409`, `425`)."""

RETRY_CODES = ("order_is_placing", "order_is_matching", "order_opened_retry")
CANCEL_DONE_CODES = ("order_already_filled", "order_already_cancelled")


def D(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def exact(probability: Fraction) -> Decimal:
    return Decimal(probability.numerator) / Decimal(probability.denominator)


def side_probability(american: Any) -> Fraction:
    value = implied_probability(american)
    if value is None:
        raise InvalidOrder(f"{VENUE}: {american!r} is not an American price")
    return value


def contracts_of(stake: Any, american: Any) -> Decimal:
    """A stake at an American price as contracts paying 1 each: the stake
    over the price's implied probability, which is the stake plus its profit.
    Rounded down to a millionth."""
    value = D(stake)
    if value <= 0:
        return Decimal("0")
    return (value / exact(side_probability(american))).quantize(CONTRACT_STEP, rounding=ROUND_DOWN)


def yes_price(american: Any, is_yes: bool) -> Decimal:
    """A side's American price as the YES price: its probability on side
    one, one minus it on side two."""
    probability = exact(side_probability(american))
    return probability if is_yes else Decimal("1") - probability


def side_of(is_yes: bool) -> Side:
    return Side.BUY if is_yes else Side.SELL


def ladder_price(probability: Decimal, ladder: list[int]) -> int:
    """The ladder price a buyer pays at most `probability` for: the dearest
    one whose implied probability is not above it."""
    limit = Fraction(str(probability))
    best: tuple[Fraction, int] | None = None
    for american in ladder:
        implied = implied_probability(american)
        if implied is None or implied > limit:
            continue
        if best is None or implied > best[0]:
            best = (implied, int(american))
    if best is None:
        raise InvalidOrder(f"{VENUE}: no price on ProphetX's ladder at or below {probability}")
    return best[1]


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

def status_of(row: dict[str, Any]) -> OrderStatus:
    """An order's state from ProphetX's `status` and `matching_status`.

    `inactive` is still being validated; `invalid` failed validation (most
    often the balance). A cancel of an order with no fill ends it `canceled`;
    a cancel or a `fillOrKill` sweep of a partly filled one keeps it `open`
    and `fully_matched` with `quantity` shrunk to what filled, which is a
    cancel here. `wiped` (an event going live, a self-match, an `ALO` that
    would have crossed) and `void` end it too. `settled`, `closed` and
    `manually_settled` come only after the event, on what filled."""
    state = str(row.get("status") or "").lower()
    matching = str(row.get("matching_status") or "").lower()
    if state == "inactive":
        return OrderStatus.PENDING
    if state == "invalid":
        return OrderStatus.REJECTED
    if state in ("canceled", "cancelled", "wiped", "void"):
        return OrderStatus.CANCELED
    if state == "open":
        if matching != "fully_matched":
            return OrderStatus.OPEN
        original = row.get("original_quantity")
        if original not in (None, "") and D(original) > D(row.get("quantity")):
            return OrderStatus.CANCELED
        return OrderStatus.CLOSED
    if state in ("settled", "closed", "manually_settled"):
        filled = D(row.get("filled_quantity") if row.get("filled_quantity") is not None else row.get("total_filled_quantity"))
        return OrderStatus.CLOSED if filled > 0 else OrderStatus.CANCELED
    return OrderStatus.PENDING


def order_of_row(
    row: dict[str, Any], *, market_id: str, is_yes: bool, account: Account | None = None, stream: bool = False,
) -> Order:
    """One order as ProphetX reports it (REST, or the account stream with
    `stream=True`, whose `filled_quantity` is one fill and whose running total
    is `total_filled_quantity`). Stakes become contracts at the order's limit."""
    american = row.get("price")
    status = status_of(row)
    original = D(row.get("original_quantity") if row.get("original_quantity") not in (None, "") else row.get("quantity"))
    filled_stake = D(row.get("total_filled_quantity") if stream else row.get("filled_quantity"))
    quantity = contracts_of(original, american)
    filled = contracts_of(filled_stake, american)
    if status == OrderStatus.CLOSED:
        filled = max(filled, contracts_of(row.get("quantity"), american))
    remaining = contracts_of(row.get("open_quantity"), american) if status in (OrderStatus.OPEN, OrderStatus.PENDING) else Decimal("0")
    if status == OrderStatus.PENDING and row.get("open_quantity") in (None, ""):
        remaining = quantity - filled
    fee = row.get("fee")
    return Order(
        id=str(row.get("order_id") or row.get("id") or ""),
        client_order_id=row.get("external_id") or None,
        venue=VENUE,
        account=account,
        market_id=market_id,
        side=side_of(is_yes),
        type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        status=status,
        price=yes_price(american, is_yes),
        quantity=quantity,
        filled=filled,
        remaining=max(Decimal("0"), remaining),
        last_fill_price=yes_price(row["fill_price"], is_yes) if stream and row.get("fill_price") else None,
        last_fill_quantity=contracts_of(row.get("filled_quantity"), row["fill_price"])
        if stream and row.get("fill_price") and row.get("filled_quantity") else None,
        fee=D(fee) if fee not in (None, "") else None,
        fee_currency="USD" if fee not in (None, "") else None,
        created_at=iso_ms(row.get("created_at")),
        updated_at=iso_ms(row.get("updated_at")),
        info={**row, "american_price": american, "stake": str(original)},
    )


def fill_of_row(
    row: dict[str, Any], *, market_id: str, is_yes: bool, order_id: str | None = None, account: Account | None = None,
) -> Fill:
    """One fill: a matched-detail or trade row (`price` is the fill's), or an
    account-stream order update for a fill (`fill_price`, `matching_id`)."""
    american = row.get("fill_price") if row.get("fill_price") is not None else row.get("price")
    aggressive = row.get("aggressive")
    fee = row.get("fee")
    return Fill(
        id=str(row.get("matching_id") or row.get("id") or ""),
        order_id=str(order_id or row.get("order_id") or row.get("id") or ""),
        client_order_id=row.get("external_id") or None,
        venue=VENUE,
        account=account,
        market_id=market_id,
        side=side_of(is_yes),
        price=yes_price(american, is_yes),
        quantity=contracts_of(row.get("filled_quantity") if row.get("fill_price") is not None else row.get("quantity"),
                              american),
        fee=D(fee) if fee not in (None, "") else None,
        fee_currency="USD",
        liquidity=Liquidity.TAKER if aggressive is True else Liquidity.MAKER if aggressive is False else Liquidity.UNKNOWN,
        timestamp=iso_ms(row.get("matched_at")) or iso_ms(row.get("updated_at")) or int(time.time() * 1000),
        info=row,
    )


def code_of(exc: ExchangeError) -> str:
    """ProphetX's error code: `{"error": "order_already_filled", "message": ...}`."""
    body = exc.body
    if isinstance(body, dict) and isinstance(body.get("error"), str):
        return body["error"]
    return ""


def error_of(exc: ExchangeError) -> ExchangeError:
    """ProphetX's refusal of an order as a typed error, by its code."""
    code = code_of(exc)
    message = (exc.body or {}).get("message") if isinstance(exc.body, dict) else None
    text = f"{VENUE}: {code or exc}" + (f" ({message})" if message and message != code else "")
    if code == "order_external_id_existed":
        return DuplicateClientOrderId(text)
    if "balance" in code or "insufficient" in code:
        return InsufficientFunds(text, body=exc.body)
    if code in ("invalid_price", "invalid_quantity", "order_invalid_profit", "invalid_strike_id", "strike_id_invalid",
                "invalid_external_id", "invalid_aggregator_id", "order_limit_exceeded"):
        return InvalidOrder(text)
    if code in ("event_not_available", "market_is_only_for_prematch", "supplemental_market_pre_match_only",
                "sport_event_is_not_booked", "err_order_placement_disabled"):
        return MarketHalted(text, body=exc.body, status=exc.status)
    return OrderRejected(text, reason=code or None, info=exc.body if isinstance(exc.body, dict) else {},
                         body=exc.body, status=exc.status)


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

def check_request(request: OrderRequest) -> None:
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"{VENUE}: {request.type.value} is held by the execution engine, not the venue; submit it through the engine"
        )
    if request.time_in_force in (TimeInForce.FOK, TimeInForce.GTD, TimeInForce.DAY):
        raise InvalidOrder(f"{VENUE}: ProphetX has no {request.time_in_force.value} orders; gtc or ioc")
    if request.price is None:
        raise InvalidOrder(f"{VENUE}: a price is required -- a market order is sent at the worst price you give")
    if not Decimal("0") < request.price < Decimal("1"):
        raise InvalidOrder(f"{VENUE}: the YES price must be strictly between 0 and 1")
    if request.reduce_only:
        raise InvalidOrder(f"{VENUE}: every ProphetX order buys a side; there is no reduce-only order")
    if request.post_only and strategy_of(request) == "fillOrKill":
        raise InvalidOrder(f"{VENUE}: a post-only order cannot also take at once")
    if request.client_order_id is not None and not EXTERNAL_ID.match(request.client_order_id):
        raise InvalidOrder(f"{VENUE}: client_order_id must be 1-100 letters, digits, '-' or '_'")


def strategy_of(request: OrderRequest) -> str | None:
    """ProphetX's `order_strategy`: `fillOrKill` for an order that takes at
    once, `ALO` for post-only, none for a resting limit."""
    if request.type == OrderType.MARKET or request.time_in_force == TimeInForce.IOC:
        return "fillOrKill"
    if request.post_only:
        return "ALO"
    return None


def build_order(request: OrderRequest, *, strike_id: str, ladder: list[int]) -> dict[str, Any]:
    """The `submit_order` body for one request, before anything is sent:
    the side's strike, its price on the ladder, and the stake for the
    contracts asked for at that price."""
    check_request(request)
    is_yes = request.side == Side.BUY
    paid = D(request.price) if is_yes else Decimal("1") - D(request.price)
    american = ladder_price(paid, ladder)
    stake = (D(request.quantity) * exact(side_probability(american))).quantize(CENT, rounding=ROUND_DOWN)
    if stake < MIN_STAKE:
        raise InvalidOrder(f"{VENUE}: {request.quantity} contracts at {american} stake ${stake}; the minimum is $1")
    body: dict[str, Any] = {
        "external_id": request.client_order_id or uuid.uuid4().hex,
        "strike_id": strike_id,
        "price": american,
        "quantity": float(stake),
    }
    strategy = strategy_of(request)
    if strategy:
        body["order_strategy"] = strategy
    return body


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class ProphetXTrading(TradingExchange):
    """ProphetX order entry.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.prophetx import ProphetXTrading

    async with ProphetXTrading(require("prophetx", load_credentials())) as px:
        print(await px.fetch_balance())
    ```
    """

    id = VENUE
    name = "ProphetX"
    has: dict[str, Capability] = {
        "create_order": True,
        # One at a time: the batch route is off on the default rate tier.
        "create_orders": True,
        "cancel_order": True,
        "cancel_orders": True,
        "cancel_all_orders": True,
        "edit_order": False,
        "fetch_order": True,
        "fetch_open_orders": True,
        # Sixty days of history a read, newest windows first.
        "fetch_orders": True,
        "fetch_my_trades": True,
        # Built from the account's orders; ProphetX has no positions route.
        "fetch_positions": "partial",
        "fetch_balance": True,
        "fetch_settlements": False,
        "fetch_queue_position": False,
        "fetch_fee_estimate": False,
        "rfq": False,
        "split_merge": False,
        "watch_orders": False,
        "watch_my_trades": False,
        "watch_positions": False,
        "watch_balance": False,
    }

    def __init__(
        self,
        credentials: ProphetXCredentials,
        *,
        account_name: str = "default",
        url: str | None = None,
        catalog: Any = None,
        session: ProphetXSession | None = None,
        limiter: BudgetLimiter | None = None,
        timeout: float = 30.0,
        client: Any = None,
    ):
        import httpx

        if not (credentials.access_key and credentials.secret_key):
            raise CredentialsMissing(
                f"{VENUE}: trading needs the Trading API key pair; set PROPHETX_ACCESS_KEY and PROPHETX_SECRET_KEY "
                "(prophetx.co, API Integration). PROPHETX_AFFILIATE_KEY reads market data only"
            )
        self.credentials = credentials
        self.sandbox = bool(credentials.sandbox)
        self.account = Account(venue=VENUE, name=account_name)
        self.limiter = limiter or BudgetLimiter(read_per_second=1, write_per_second=1, max_wait_s=10.0)
        self.url = (url or (SANDBOX_URL if self.sandbox else API_URL)).rstrip("/")
        self._client = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        self.http = AsyncHttpClient(self.url, limiter=None, client=self._client, venue=VENUE)
        self._own_session = session is None
        self.session = session or ProphetXSession(
            access_key=credentials.access_key, secret_key=credentials.secret_key, base_url=self.url, timeout=timeout,
        )
        self._own_catalog = catalog is None
        if catalog is None:
            from ..prophetx import ProphetX

            catalog = ProphetX(session=self.session, base_url=self.url, timeout=timeout)
        self.catalog = catalog
        """The read adapter: an event's lines, to know which side a strike is."""
        self._ladder: list[int] | None = None
        self._sides: dict[str, tuple[str, bool]] = {}
        """strike_id -> (Synpath market id, whether it is the YES side)."""
        self._events: dict[str, str] = {}
        """strike_id -> its event id, from `get_strikes`."""
        self._external: dict[str, str] = {}
        """order_id -> external_id, for the orders placed here; a cancel needs both."""

    # -- plumbing -------------------------------------------------------------

    async def _call(
        self, method: str, path: str, *, params: Any = None, body: Any = None,
        kind: str = "read", priority: Priority = Priority.NORMAL,
    ) -> Any:
        """One call with the session's bearer token, paced; ProphetX's `data`
        unwrapped. A 401 renews the token once."""
        await self.limiter.acquire(cost=1, kind=kind, priority=priority)  # type: ignore[arg-type]
        query = [(k, v) for k, v in params if v is not None] if isinstance(params, list) else \
            {k: v for k, v in (params or {}).items() if v is not None}
        for attempt in range(2):
            token = await self.session.token_async(self._client)
            try:
                payload = await self.http.request(method, path, params=query or None, json=body,
                                                  headers={"Authorization": f"Bearer {token}"})
            except AuthenticationError as exc:
                if exc.status == 401 and attempt == 0:
                    self.session.invalidate()
                    continue
                raise
            return unwrap(payload)
        raise AuthenticationError(f"{VENUE}: the session token was refused")  # pragma: no cover

    async def ladder(self, *, fresh: bool = False) -> list[int]:
        """The American prices ProphetX accepts. Read once and kept."""
        if self._ladder is None or fresh:
            rows = await self._call("GET", "/v4/mm/get_price_ladder") or []
            self._ladder = sorted(int(p) for p in rows if isinstance(p, (int, float)) and not isinstance(p, bool))
        return self._ladder

    async def _learn_event(self, event_id: str) -> None:
        """Which side of which line every strike of an event is."""
        lines = await asyncio.to_thread(self.catalog.lines, event_id)
        for line in lines:
            market_id = ids.qualify(VENUE, line_native_id(event_id, line["_yes"]))
            self._sides[line["_yes"]] = (market_id, True)
            self._sides[line["_no"]] = (market_id, False)

    async def side(self, strike_id: str, *, event_id: Any = None) -> tuple[str, bool]:
        """(Synpath market id, is the YES side) for a strike. A strike whose
        event ProphetX no longer lists cannot be paired with its other side:
        it is named as its own line's YES (`prophetx:<event>-<strike>`)."""
        strike_id = str(strike_id)
        if strike_id not in self._sides:
            event = str(event_id) if event_id not in (None, "") else await self._event_of(strike_id)
            if event:
                try:
                    await self._learn_event(event)
                except MarketNotFound:
                    pass
            if strike_id not in self._sides:
                return ids.qualify(VENUE, line_native_id(event or "0", strike_id)), True
        return self._sides[strike_id]

    async def _event_of(self, strike_id: str) -> str | None:
        if strike_id not in self._events:
            rows = await self._call("GET", "/v4/mm/get_strikes", params={"strike_ids": strike_id}) or []
            for row in rows if isinstance(rows, list) else []:
                if isinstance(row, dict) and row.get("strike_id") and row.get("event_id") is not None:
                    self._events[str(row["strike_id"])] = str(row["event_id"])
        return self._events.get(strike_id)

    async def strike_for(self, market_id: str, is_yes: bool) -> str:
        """The strike an order on this line's side buys."""
        event_id, yes_strike = parse_line_id(ids.native(VENUE, market_id))
        if is_yes:
            return yes_strike
        if yes_strike not in self._sides:
            await self._learn_event(event_id)
        synpath_id = ids.qualify(VENUE, line_native_id(event_id, yes_strike))
        no = next((s for s, (m, yes) in self._sides.items() if m == synpath_id and not yes), None)
        if no is None:
            raise MarketNotFound(f"{VENUE}: no open line {ids.native(VENUE, market_id)}")
        return no

    async def _order(self, row: dict[str, Any]) -> Order:
        market_id, is_yes = await self.side(str(row.get("strike_id") or ""), event_id=row.get("sport_event_id"))
        order = order_of_row(row, market_id=market_id, is_yes=is_yes, account=self.account)
        if order.client_order_id:
            self._external[order.id] = order.client_order_id
        return order

    # -- orders ---------------------------------------------------------------

    async def create_order(self, request: OrderRequest) -> Order:
        """Place one order. ProphetX answers before matching runs, so the
        order comes back `pending`; its fills arrive on the account stream or
        a later `fetch_order`."""
        check_request(request)
        strike = await self.strike_for(request.market_id, request.side == Side.BUY)
        body = build_order(request, strike_id=strike, ladder=await self.ladder())
        try:
            data = await self._call("POST", "/v4/mm/submit_order", body=body, kind="write") or {}
        except (BadRequest, AuthenticationError) as exc:
            raise error_of(exc) from None
        except ExchangeError as exc:
            if exc.status is not None and exc.status < 500:
                raise error_of(exc) from None
            raise
        row = data.get("order") if isinstance(data, dict) else None
        if not isinstance(row, dict):
            raise OrderRejected(f"{VENUE}: submit_order answered without an order: {data}", reason="no_order")
        market_id = ids.qualify(VENUE, ids.native(VENUE, request.market_id))
        is_yes = request.side == Side.BUY
        order = order_of_row({"external_id": body["external_id"], **row}, market_id=market_id, is_yes=is_yes,
                             account=request.account or self.account)
        self._external[order.id] = body["external_id"]
        self._sides.setdefault(strike, (market_id, is_yes))
        return order.model_copy(update={
            "type": request.type, "time_in_force": request.time_in_force, "post_only": request.post_only,
            "book": request.book, "trader": request.trader, "tags": request.tags,
            "created_at": order.created_at or int(time.time() * 1000),
        })

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        """Many orders, one request each (the batch route is off on the
        default tier), one result each in the order asked for."""
        out: list[Order | Exception] = []
        for request in requests:
            try:
                out.append(await self.create_order(request))
            except (ExchangeError, InvalidOrder) as exc:
                out.append(exc)
        return out

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        """Cancel one order and read it back. ProphetX's cancel is final when
        it answers; the read may lag, so a cancelled order still read as open
        is reported cancelled. A cancel of an order already filled or already
        cancelled reads it back as it stands."""
        external = self._external.get(order_id)
        current: Order | None = None
        if external is None:
            current = await self.fetch_order(order_id)
            external = current.client_order_id
        if not external:
            raise OrderRejected(f"{VENUE}: order {order_id} has no external_id to cancel it by", reason="no_external_id")
        cancelled = False
        for attempt in range(CANCEL_RETRIES):
            try:
                await self._call("POST", "/v4/mm/cancel_order", body={"external_id": external, "order_id": order_id},
                                 kind="write", priority=Priority.HIGH)
                cancelled = True
                break
            except MarketNotFound as exc:
                raise OrderNotFound(f"{VENUE}: no order {order_id}", body=exc.body, status=exc.status) from None
            except ExchangeError as exc:
                code = code_of(exc)
                if code in CANCEL_DONE_CODES:
                    break
                if code in RETRY_CODES and attempt < CANCEL_RETRIES - 1:
                    await asyncio.sleep(0.3 * (attempt + 1))
                    continue
                raise error_of(exc) from None
        order = await self.fetch_order(order_id)
        if cancelled and order.status in (OrderStatus.OPEN, OrderStatus.PENDING):
            order = order.model_copy(update={
                "status": OrderStatus.CANCELED if order.filled < order.quantity else OrderStatus.CLOSED,
                "remaining": Decimal("0"),
            })
        return order

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        """Cancel orders one at a time (the batch route is off on the
        default tier), one result each."""
        out: list[Order | Exception] = []
        for order_id in order_ids:
            try:
                out.append(await self.cancel_order(order_id))
            except ExchangeError as exc:
                out.append(exc)
        return out

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int | None:
        """Every resting order on a line (both its sides), or on the account.
        ProphetX cancels in the background and gives no count: `None`, or 0
        when it says there was nothing to cancel."""
        if market_id is None:
            try:
                await self._call("POST", "/v4/mm/cancel_all_orders", kind="write", priority=Priority.HIGH)
            except MarketNotFound:
                return 0
            return None
        event_id, yes_strike = parse_line_id(ids.native(VENUE, market_id))
        no_strike = await self.strike_for(market_id, False)
        lines = await asyncio.to_thread(self.catalog.lines, event_id)
        line = next((ln for ln in lines if ln["_yes"] == yes_strike), None)
        if line is None or line.get("id") is None:
            raise MarketNotFound(f"{VENUE}: no open line {ids.native(VENUE, market_id)}")
        found = False
        for strike in (yes_strike, no_strike):
            try:
                await self._call("POST", "/v4/mm/cancel_orders_by_market", body={
                    "event_id": int(event_id), "market_id": int(line["id"]), "strike_id": strike,
                }, kind="write", priority=Priority.HIGH)
                found = True
            except MarketNotFound:
                continue
        return None if found else 0

    async def fetch_order(self, order_id: str) -> Order:
        """One order by id, in any state. ProphetX's read lags its matching
        by up to seconds, so an order placed a moment ago can be not found yet."""
        try:
            row = await self._call("GET", f"/v4/mm/get_order/{order_id}")
        except MarketNotFound as exc:
            raise OrderNotFound(f"{VENUE}: no order {order_id} (yet: reads lag placing by seconds)",
                                body=exc.body, status=exc.status) from None
        if not isinstance(row, dict) or not row:
            raise OrderNotFound(f"{VENUE}: no order {order_id}")
        return await self._order(row)

    async def _history(
        self, *, status: str | None = None, since_s: int | None = None, cursor: str | None = None,
        limit: int = HISTORY_PAGE, market: str | None = None, event: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        data = await self._call("GET", "/v4/mm/get_order_history", params={
            "from": since_s if since_s is not None else int(time.time()) - HISTORY_WINDOW_S + 60,
            "status": status, "limit": limit, "next_cursor": cursor, "market_id": market, "event_id": event,
        }) or {}
        return [r for r in data.get("orders") or [] if isinstance(r, dict)], data.get("next_cursor") or None

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        """Every resting order of the last sixty days, or every one on a line."""
        event = parse_line_id(ids.native(VENUE, market_id))[0] if market_id else None
        orders: list[Order] = []
        cursor = None
        for _ in range(MAX_PAGES):
            rows, cursor = await self._history(status="open", cursor=cursor, event=event)
            for row in rows:
                if str(row.get("matching_status") or "") == "fully_matched":
                    continue
                orders.append(await self._order(row))
            if not cursor:
                break
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            orders = [o for o in orders if o.market_id == wanted]
        return orders

    async def fetch_orders(
        self, *, status: str | None = None, market_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Order]:
        """One page of the account's orders, created since `since` (at most
        sixty days back; the last sixty days by default). `status` is
        ProphetX's own word (`open`, `canceled`, `settled`, ...)."""
        event = parse_line_id(ids.native(VENUE, market_id))[0] if market_id else None
        rows, following = await self._history(
            status=status, since_s=int(since // 1000) if since else None, cursor=cursor,
            limit=min(limit or 100, HISTORY_PAGE), event=event,
        )
        orders = [await self._order(row) for row in rows]
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            orders = [o for o in orders if o.market_id == wanted]
        return Page(orders, next_cursor=following)

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        """One page of this account's fills. With `order_id`, that order's
        fills (`get_order_matched_detail`); otherwise every fill
        (`get_trades`), filtered here to `market_id`."""
        params = {"from": int(since // 1000) if since else None, "limit": min(limit or 100, 100), "next_cursor": cursor}
        if order_id:
            order = await self.fetch_order(order_id)
            data = await self._call("GET", "/v4/mm/get_order_matched_detail", params={**params, "order_id": order_id}) or {}
            is_yes = order.side == Side.BUY
            fills = [fill_of_row(row, market_id=order.market_id, is_yes=is_yes, order_id=order_id, account=self.account)
                     for row in data.get("matching_details") or [] if isinstance(row, dict)]
        else:
            data = await self._call("GET", "/v4/mm/get_trades", params=params) or {}
            fills = []
            for row in data.get("trades") or []:
                if not isinstance(row, dict):
                    continue
                market, is_yes = await self.side(str(row.get("strike_id") or ""))
                fills.append(fill_of_row(row, market_id=market, is_yes=is_yes, account=self.account))
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            fills = [f for f in fills if f.market_id == wanted]
        return Page(fills, next_cursor=data.get("next_cursor") or None)

    # -- account --------------------------------------------------------------

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        """The wallet in USD: cash free to stake (`available`), and what
        resting orders reserve plus what matched orders hold (`locked`). An
        exposure credit line, where ProphetX grants one, is in `info`."""
        raw = await self._call("GET", "/v4/mm/get_balance") or {}
        available = D(raw.get("balance"))
        locked = D(raw.get("unmatched_order_balance")) + D(raw.get("matched_order_balance"))
        return Balance(
            venue=VENUE, account=account or self.account, currency="USD",
            total=available + locked, available=available, locked=locked, buying_power=None,
            timestamp=int(time.time() * 1000), info=raw,
        )

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        """Matched stakes of the last sixty days' live orders, netted per line
        on the YES leg: contracts bought on side one less those on side two,
        each order's at its limit price. Settled orders are left out."""
        event = ids.native(VENUE, event_id) if event_id else (
            parse_line_id(ids.native(VENUE, market_id))[0] if market_id else None)
        net: dict[str, dict[str, Decimal]] = {}
        cursor = None
        for _ in range(MAX_PAGES):
            rows, cursor = await self._history(status="open", cursor=cursor, event=event)
            for row in rows:
                if D(row.get("filled_quantity")) <= 0:
                    continue
                order = await self._order(row)
                entry = net.setdefault(order.market_id, {"yes": Decimal("0"), "no": Decimal("0"), "stake": Decimal("0")})
                entry["yes" if order.side == Side.BUY else "no"] += order.filled
                entry["stake"] += D(row.get("filled_quantity"))
            if not cursor:
                break
        out = []
        for market, entry in net.items():
            balance = entry["yes"] - entry["no"]
            out.append(Position(
                venue=VENUE, account=self.account, market_id=market,
                side=PositionSide.LONG if balance > 0 else PositionSide.SHORT if balance < 0 else PositionSide.FLAT,
                contracts=abs(balance), inventory_yes=entry["yes"], inventory_no=entry["no"], margin=entry["stake"],
                timestamp=int(time.time() * 1000), info={"source": "order history"},
            ))
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            out = [p for p in out if p.market_id == wanted]
        return out

    async def close(self) -> None:
        await self.http.close()
        if self._own_catalog:
            self.catalog.close()
        if self._own_session:
            self.session.close()


__all__ = ["ProphetXTrading", "order_of_row", "fill_of_row", "build_order", "ladder_price", "status_of"]
