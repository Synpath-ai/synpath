"""Matchbook order entry: offers placed, edited and cancelled, and the
account's offers, bets, positions and balance on the YES leg.

Built to Matchbook's API reference (developers.matchbook.com); its records
follow the reference's examples. Every call carries the session token of an
account login (`MATCHBOOK_USERNAME`, `MATCHBOOK_PASSWORD`); see
`synpath.matchbook.MatchbookSession`.

**An order is an offer to back or lay one runner.** `buy` backs the runner
(takes its YES) at decimal odds 1/price; `sell` lays it (takes its NO) at the
same odds. Matchbook counts both in the backer's stake: an order for `n`
contracts stakes `n / odds`, rounded down to the penny, and a lay's liability
is that stake times `odds - 1`. Prices are snapped onto Matchbook's odds
ladder (1.01 to 1000) on the cheaper side: a buy at higher odds (a lower YES
price), a sell at lower odds (a higher YES price).

**Limits rest; market orders take what is there.** Matchbook has no
immediate-or-cancel offer, so a market order, or an IOC limit, is an offer at
the price given as the worst accepted whose unmatched rest is cancelled at
once (after the in-play delay when the market is in play). A limit is `GTC`
and rests until matched or cancelled; by default its unmatched part is
cancelled when the event goes in play (`params={"keep_in_play": True}` keeps
it). FOK, GTD and post-only orders are not offered.

**No client order id.** Matchbook assigns the offer id and takes none from the
client; `client_order_id` is kept on the returned order only. After a timeout,
read the open offers on the runner to find what was placed.

**Positions** net per runner on the YES leg from the account's current
matched bets: backs are YES contracts, lays NO contracts, a stake S at odds O
being S·O contracts either way.

**Fees** are commission on the account's net winnings per market at
settlement (2% in the UK and Ireland, 4% elsewhere by Matchbook's terms);
API and commercial accounts can be on volume commission instead, which each
offer names (`commission-type`, `originator-commission-rate`).

The *heartbeat* (`heartbeat(seconds)`) is a dead man's switch: unless renewed
in time, Matchbook cancels every open offer on the account.
"""
from __future__ import annotations

import asyncio
import bisect
import time
from decimal import ROUND_DOWN, Decimal
from typing import Any

from .. import ids
from ..base import AsyncHttpClient, Capability, RateLimiter
from ..errors import AuthenticationError, BadRequest, ExchangeError, MarketNotFound
from ..matchbook import (
    API_URL, EDGE, ODDS_LADDER, MatchbookSession, error_messages, iso_ms, market_native_id, parse_market_id,
    parse_runner_id, runner_native_id,
)
from ..smarkets import user_agent
from ..types import Page
from .base import TradingExchange
from .credentials import MatchbookCredentials
from .errors import InsufficientFunds, InvalidOrder, MarketHalted, OrderNotFound, OrderRejected
from .limiter import BudgetLimiter, Priority
from .types import (
    Account, Balance, EditRequest, FeeEstimate, Fill, Liquidity, Order, OrderRequest, OrderStatus, OrderType,
    Position, PositionSide, Settlement, Side, TimeInForce, VENUE_ORDER_TYPES,
)

VENUE = "matchbook"
ONE = Decimal("1")
PRICE_PLACES = Decimal("0.0000000001")
PENNY = Decimal("0.01")

OFFERS_PER_REQUEST = 50
"""Offers one submit or cancel request carries. Matchbook documents no cap;
kept modest."""

PER_PAGE = 500
"""Offers or bets read per request when listing."""

MAX_PAGES = 50
"""Most pages one listing call reads."""

DELAY_GRACE_S = 0.5
"""Seconds past an in-play delay before an immediate order's rest is cancelled."""

REPORTS_LIMITER = RateLimiter(40 / 60, burst=2)
"""The reports group (current and settled bets) allows 40 requests a minute."""

STATUS: dict[str, OrderStatus] = {
    "open": OrderStatus.OPEN,
    "edited": OrderStatus.OPEN,
    "matched": OrderStatus.CLOSED,
    "cancelled": OrderStatus.CANCELED,
    "flushed": OrderStatus.CANCELED,
    "failed": OrderStatus.REJECTED,
    "delayed": OrderStatus.PENDING,
}
"""An offer's state by Matchbook's status. An edited offer is still resting
with its new values; a flushed one was taken off the market by Matchbook (a
withdrawn runner, a goal); a delayed one waits out an in-play delay before it
enters the market."""


def D(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def yes_price(odds: Any) -> Decimal:
    """Decimal odds as the YES price 1/O, to ten places."""
    return (ONE / D(odds)).quantize(PRICE_PLACES)


def side_of(native: Any) -> Side:
    """`back` (or `win`) takes YES; `lay` (or `lose`) takes NO."""
    return Side.BUY if str(native or "").lower() in ("back", "win") else Side.SELL


def odds_for(side: Side, price: Any) -> Decimal:
    """The ladder odds for an order at this YES price, on the cheaper side: a
    buy at the first odds at or above 1/price, a sell at the last at or below."""
    p = D(price)
    if not 0 < p < 1:
        raise InvalidOrder(f"{VENUE}: a price is a YES probability strictly between 0 and 1, not {price}")
    # To a millionth of a point of odds, so a price such as 0.3333333333
    # meant as odds 3 is not pushed to the next tick by its last digit.
    raw = (ONE / p).quantize(Decimal("0.000001"))
    if side == Side.BUY:
        index = bisect.bisect_left(ODDS_LADDER, raw)
        if index >= len(ODDS_LADDER):
            raise InvalidOrder(f"{VENUE}: {price} is below the ladder's lowest YES price (odds 1000)")
        return ODDS_LADDER[index]
    index = bisect.bisect_right(ODDS_LADDER, raw) - 1
    if index < 0:
        raise InvalidOrder(f"{VENUE}: {price} is above the ladder's highest YES price (odds 1.01)")
    return ODDS_LADDER[index]


def stake_for(quantity: Any, odds: Decimal) -> Decimal:
    """The backer's stake for `quantity` contracts at `odds`, down to the penny."""
    stake = (D(quantity) / odds).quantize(PENNY, rounding=ROUND_DOWN)
    if stake <= 0:
        raise InvalidOrder(f"{VENUE}: {quantity} contracts at odds {odds} is less than a penny of stake")
    return stake


def _number(value: Decimal) -> float | int:
    """A Decimal as the JSON number Matchbook reads."""
    return int(value) if value == value.to_integral_value() else float(value)


def _offer_market(row: dict[str, Any]) -> str:
    return ids.qualify(VENUE, runner_native_id(row.get("event-id"), row.get("market-id"), row.get("runner-id")))


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

def order_of_offer(row: dict[str, Any], *, account: Account | None = None) -> Order:
    """One offer as Matchbook reports it (submit, get, cancel, edit). Stakes
    become contracts at the offer's own odds."""
    odds = D(row.get("decimal-odds") or row.get("odds"))
    stake, remaining = D(row.get("stake")), D(row.get("remaining"))
    quantity, left = stake * odds, remaining * odds
    native = str(row.get("status") or "")
    status = STATUS.get(native, OrderStatus.PENDING)
    if status == OrderStatus.OPEN and remaining <= 0 and stake > 0:
        status = OrderStatus.CLOSED
    filled = quantity if status == OrderStatus.CLOSED else max(Decimal("0"), quantity - left)
    if status in (OrderStatus.PENDING, OrderStatus.REJECTED):
        filled = Decimal("0")
    bets = [b for b in row.get("matched-bets") or [] if isinstance(b, dict)]
    bet_stake = sum((D(b.get("stake")) for b in bets), Decimal("0"))
    bet_contracts = sum((D(b.get("stake")) * D(b.get("decimal-odds") or b.get("odds")) for b in bets), Decimal("0"))
    average = (bet_stake / bet_contracts).quantize(PRICE_PLACES) if bet_contracts else None
    fee = sum((D(b.get("commission")) for b in bets), Decimal("0")) if bets else None
    return Order(
        id=str(row.get("id") or ""),
        venue=VENUE,
        account=account,
        market_id=_offer_market(row),
        side=side_of(row.get("side")),
        type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        status=status,
        price=yes_price(odds) if odds else None,
        quantity=quantity,
        filled=filled,
        remaining=left if status in (OrderStatus.OPEN, OrderStatus.PENDING) else Decimal("0"),
        average_price=average,
        fee=fee,
        fee_currency=row.get("currency"),
        created_at=iso_ms(row.get("created-at")),
        info=row,
    )


def fills_of_offer(row: dict[str, Any], *, account: Account | None = None) -> list[Fill]:
    """The matched bets on one offer, each a fill: contracts are its stake
    times its odds, the price 1/odds on the YES leg."""
    out = []
    for bet in row.get("matched-bets") or []:
        if not isinstance(bet, dict):
            continue
        odds = D(bet.get("decimal-odds") or bet.get("odds"))
        if not odds:
            continue
        out.append(Fill(
            id=str(bet.get("id") or ""),
            order_id=str(bet.get("offer-id") or row.get("id") or ""),
            venue=VENUE,
            account=account,
            market_id=_offer_market(row),
            side=side_of(row.get("side")),
            price=yes_price(odds),
            quantity=D(bet.get("stake")) * odds,
            fee=D(bet["commission"]) if bet.get("commission") is not None else None,
            fee_currency=bet.get("currency") or row.get("currency"),
            liquidity=Liquidity.UNKNOWN,
            timestamp=iso_ms(bet.get("created-at")) or 0,
            info=bet,
        ))
    return out


def positions_of_bets(body: dict[str, Any], *, account: Account | None = None) -> list[Position]:
    """Current matched bets (`/reports/v2/bets/current`) netted per runner on
    the YES leg: backs are YES contracts, lays NO contracts."""
    rows: dict[str, dict[str, Any]] = {}
    for market in body.get("markets") or []:
        for selection in market.get("selections") or []:
            synpath_id = ids.qualify(VENUE, runner_native_id(market.get("event-id"), market.get("id"),
                                                             selection.get("runner-id")))
            row = rows.setdefault(synpath_id, {"yes": Decimal("0"), "no": Decimal("0"), "stake": Decimal("0"),
                                               "margin": Decimal("0"), "selections": [], "market": market})
            back = str(selection.get("side") or "").lower() in ("back", "win")
            for bet in selection.get("bets") or []:
                odds, stake = D(bet.get("odds")), D(bet.get("stake"))
                row["yes" if back else "no"] += stake * odds
                row["stake"] += stake
                row["margin"] += D(bet.get("liability")) if bet.get("liability") is not None else (
                    stake if back else stake * (odds - 1))
            row["selections"].append(selection)
    out = []
    for synpath_id, row in rows.items():
        net = row["yes"] - row["no"]
        traded = row["yes"] + row["no"]
        out.append(Position(
            venue=VENUE,
            account=account,
            market_id=synpath_id,
            side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
            contracts=abs(net),
            inventory_yes=row["yes"],
            inventory_no=row["no"],
            # The average YES price of every bet: stakes over contracts.
            entry_price=(row["stake"] / traded).quantize(PRICE_PLACES) if traded else None,
            margin=row["margin"],
            info={"selections": row["selections"],
                  "market": {k: v for k, v in row["market"].items() if k != "selections"}},
        ))
    return out


RESULTS = {("back", "win"): "yes", ("back", "lose"): "no", ("lay", "win"): "no", ("lay", "lose"): "yes"}
"""A settled bet's result, which is the bet's own (a lay that wins means the
runner lost), as the runner's YES result."""


def settlements_of(body: dict[str, Any], *, account: Account | None = None) -> list[Settlement]:
    """Settled bets (`/reports/v2/bets/settled`), one settlement per runner and
    side the account bet."""
    out = []
    for market in body.get("markets") or []:
        for selection in market.get("selections") or []:
            side = str(selection.get("side") or "").lower()
            bets = [b for b in selection.get("bets") or [] if isinstance(b, dict)]
            results = {RESULTS.get((side, str(b.get("result") or "").lower())) for b in bets} - {None}
            result = results.pop() if len(results) == 1 else None
            quantity = sum((D(b.get("stake")) * D(b.get("odds")) for b in bets), Decimal("0"))
            pnl = selection.get("net-profit-and-loss")
            out.append(Settlement(
                venue=VENUE,
                account=account,
                market_id=ids.qualify(VENUE, runner_native_id(market.get("event-id"), market.get("id"),
                                                              selection.get("runner-id"))),
                held=PositionSide.LONG if side in ("back", "win") else PositionSide.SHORT,
                result=result,
                won=(D(pnl) > 0) if pnl is not None else None,
                quantity=quantity,
                cost=D(selection.get("stake")) if side in ("back", "win") else None,
                pnl=D(pnl) if pnl is not None else None,
                timestamp=iso_ms(market.get("settled-time")),
                info={"selection": selection, "market": {k: v for k, v in market.items() if k != "selections"}},
            ))
    return out


def reason_of(body: Any, fallback: str) -> str:
    return error_messages(body) or fallback


def error_of(exc: ExchangeError) -> ExchangeError:
    """Matchbook's refusal of an offer as a typed error, by its message."""
    text = reason_of(exc.body, str(exc))
    lower = text.lower()
    if "fund" in lower or "balance" in lower or "exposure" in lower:
        return InsufficientFunds(f"{VENUE}: {text}", body=exc.body)
    if "suspended" in lower or "closed" in lower:
        return MarketHalted(f"{VENUE}: {text}", body=exc.body, status=exc.status)
    if "odds" in lower or "stake" in lower or "runner" in lower:
        return InvalidOrder(f"{VENUE}: {text}")
    return OrderRejected(f"{VENUE}: {text}", reason=lower or None,
                         info=exc.body if isinstance(exc.body, dict) else {}, body=exc.body)


def _rejected(row: dict[str, Any]) -> OrderRejected:
    text = reason_of(row, "the offer failed")
    return OrderRejected(f"{VENUE}: offer refused: {text}", reason="failed", info=row)


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

def immediate(request: OrderRequest) -> bool:
    """Whether the order takes what is there and leaves nothing resting."""
    return request.type == OrderType.MARKET or request.time_in_force == TimeInForce.IOC


def check_request(request: OrderRequest) -> None:
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"{VENUE}: {request.type.value} is held by the execution engine, not the venue; submit it through the engine"
        )
    if request.time_in_force in (TimeInForce.FOK, TimeInForce.GTD, TimeInForce.DAY):
        raise InvalidOrder(f"{VENUE}: Matchbook offers rest until matched or cancelled; gtc or ioc only")
    if request.price is None:
        raise InvalidOrder(f"{VENUE}: a price is required -- a market order is sent at the worst price you give")
    if request.post_only:
        raise InvalidOrder(f"{VENUE}: Matchbook has no post-only offer")
    if request.reduce_only:
        raise InvalidOrder(f"{VENUE}: Matchbook nets every bet on a runner; there is no reduce-only offer")


def build_offer(request: OrderRequest) -> dict[str, Any]:
    """One request as a Matchbook offer, before anything is sent."""
    check_request(request)
    _, _, runner_id = parse_runner_id(ids.native(VENUE, request.market_id))
    odds = odds_for(request.side, request.price)
    stake = stake_for(request.quantity, odds)
    return {
        "runner-id": int(runner_id),
        "side": "back" if request.side == Side.BUY else "lay",
        "odds": _number(odds),
        "stake": _number(stake),
        "keep-in-play": bool(request.params.get("keep_in_play", False)),
    }


def order_after_placing(request: OrderRequest, row: dict[str, Any], *, account: Account) -> Order:
    """The order from Matchbook's answer to a submit, with what the request
    carried that Matchbook does not keep."""
    order = order_of_offer(row, account=request.account or account)
    return order.model_copy(update={
        "client_order_id": request.client_order_id, "type": request.type,
        "time_in_force": request.time_in_force, "book": request.book, "trader": request.trader,
        "tags": request.tags,
    })


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class MatchbookTrading(TradingExchange):
    """Matchbook order entry.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.matchbook import MatchbookTrading

    async with MatchbookTrading(require("matchbook", load_credentials())) as mb:
        print(await mb.fetch_balance())
    ```
    """

    id = VENUE
    name = "Matchbook"
    has: dict[str, Capability] = {
        "create_order": True,
        # One request, one answer per offer; not atomic.
        "create_orders": True,
        "cancel_order": True,
        "cancel_orders": True,
        "cancel_all_orders": True,
        # Odds and remaining stake, in place.
        "edit_order": True,
        "fetch_order": True,
        "fetch_open_orders": True,
        # Offers on markets not yet settled only.
        "fetch_orders": "partial",
        "fetch_my_trades": "partial",
        "fetch_positions": True,
        "fetch_balance": True,
        "fetch_settlements": True,
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
        credentials: MatchbookCredentials | None = None,
        *,
        session: MatchbookSession | None = None,
        session_token: str | None = None,
        account_name: str = "default",
        url: str = API_URL,
        catalog: Any = None,
        limiter: BudgetLimiter | None = None,
        reports_limiter: RateLimiter | None | str = "shared",
        timeout: float = 30.0,
        client: Any = None,
        session_client: Any = None,
    ):
        """`credentials` log in on the first call; `session_token` reuses a
        login made elsewhere (an account with two-factor login needs that)."""
        import httpx

        if session is None:
            if credentials is None and not session_token:
                from .errors import CredentialsMissing

                raise CredentialsMissing(f"{VENUE}: set MATCHBOOK_USERNAME and MATCHBOOK_PASSWORD, or pass session_token=")
            session = MatchbookSession(
                username=credentials.username if credentials else None,
                password=credentials.password if credentials else None,
                session_token=session_token, client=session_client, timeout=timeout, base_url=url,
            )
        self.credentials = credentials
        self.session = session
        self.account = Account(venue=VENUE, name=account_name)
        # Matchbook's betting writes allow 3,000 a minute and its account reads 300.
        self.limiter = limiter or BudgetLimiter(read_per_second=4, write_per_second=20)
        self.reports_limiter = REPORTS_LIMITER if reports_limiter == "shared" else reports_limiter
        self.url = url
        shared = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        shared.headers["User-Agent"] = user_agent()
        shared.headers["Accept"] = "application/json"
        self.http = AsyncHttpClient(url, limiter=None, client=shared, venue=VENUE)
        self._own_catalog = catalog is None
        if catalog is None:
            from ..matchbook import Matchbook

            catalog = Matchbook(base_url=url)
        self.catalog = catalog
        """The read adapter: fee schedules."""
        self._currency: str | None = None

    # -- plumbing -------------------------------------------------------------

    async def _call(
        self, method: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None,
        kind: str = "read", priority: Priority = Priority.NORMAL, reports: bool = False,
    ) -> Any:
        """One call with the session token, paced; a 401 logs in again once."""
        await self.limiter.acquire(cost=1, kind=kind, priority=priority)  # type: ignore[arg-type]
        if reports and self.reports_limiter is not None:
            await self.reports_limiter.acquire_async()
        query = {k: v for k, v in (params or {}).items() if v is not None}
        for attempt in range(2):
            token = await asyncio.to_thread(self.session.token)
            try:
                return await self.http.request(method, f"{EDGE}{path}", params=query or None, json=body,
                                               headers={"session-token": token})
            except AuthenticationError as exc:
                if exc.status == 401 and attempt == 0:
                    self.session.invalidate()
                    continue
                raise
            except MarketNotFound as exc:
                if "/offers" in path:
                    raise OrderNotFound(f"{VENUE}: {reason_of(exc.body, 'no such offer')}", body=exc.body,
                                        status=exc.status) from None
                raise
        raise AssertionError("unreachable")  # pragma: no cover

    async def currency(self) -> str:
        """The account's currency, read once: every stake and balance is in it."""
        if self._currency is None:
            account = self.session.account
            if not account or not account.get("currency"):
                account = await self._call("GET", "/account") or {}
            self._currency = str(account.get("currency") or "")
        return self._currency

    async def heartbeat(self, timeout_s: int) -> dict[str, Any]:
        """Arm or renew the dead man's switch: unless this is called again
        within `timeout_s` seconds, Matchbook cancels every open offer on the
        account. Matchbook may set a longer timeout than asked (its answer's
        `actual-timeout`). `0` turns it off."""
        if int(timeout_s) < 0:
            raise BadRequest(f"{VENUE}: the heartbeat timeout is a number of seconds")
        if int(timeout_s) == 0:
            return await self._call("DELETE", "/v1/heartbeat", kind="write", priority=Priority.HIGH) or {}
        return await self._call("POST", "/v1/heartbeat", body={"timeout": int(timeout_s)},
                                kind="write", priority=Priority.HIGH) or {}

    # -- orders ---------------------------------------------------------------

    async def _submit(self, offers: list[dict[str, Any]]) -> list[dict[str, Any]]:
        body = {"odds-type": "DECIMAL", "exchange-type": "back-lay", "offers": offers}
        try:
            raw = await self._call("POST", "/v2/offers", body=body, kind="write") or {}
        except BadRequest as exc:
            # A refused batch still answers offer by offer where it can.
            rows = (exc.body or {}).get("offers") if isinstance(exc.body, dict) else None
            if isinstance(rows, list) and len(rows) == len(offers):
                return [dict(row, status=row.get("status") or "failed") for row in rows]
            raise error_of(exc) from None
        return list(raw.get("offers") or [])

    async def create_order(self, request: OrderRequest) -> Order:
        """Place one offer. Matchbook answers with what matched on arrival; an
        immediate order's unmatched rest is cancelled before this returns."""
        result = await self.create_orders([request])
        if isinstance(result[0], Exception):
            raise result[0]
        return result[0]

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        """Many offers, fifty a request, one result each in the order asked
        for. A refused offer comes back as its exception."""
        out: list[Order | Exception] = []
        for start in range(0, len(requests), OFFERS_PER_REQUEST):
            chunk = requests[start:start + OFFERS_PER_REQUEST]
            built: list[tuple[OrderRequest, dict[str, Any]] | Exception] = []
            for request in chunk:
                try:
                    built.append((request, build_offer(request)))
                except (InvalidOrder, BadRequest) as exc:
                    built.append(exc)
            ready = [b for b in built if not isinstance(b, Exception)]
            rows = iter(await self._submit([offer for _, offer in ready]) if ready else [])
            for entry in built:
                if isinstance(entry, Exception):
                    out.append(entry)
                    continue
                request, _ = entry
                row = next(rows, None)
                if not isinstance(row, dict) or row.get("status") == "failed" or not row.get("id"):
                    out.append(_rejected(row if isinstance(row, dict) else {}))
                    continue
                out.append(order_after_placing(request, row, account=self.account))
        return await self._finish_immediate(requests, out)

    async def _finish_immediate(self, requests: list[OrderRequest], placed: list[Order | Exception]) -> list[Order | Exception]:
        """Cancel the unmatched rest of every market or IOC order, waiting out
        an in-play delay first, so nothing of them is left resting."""
        to_cancel = [i for i, (request, order) in enumerate(zip(requests, placed))
                     if immediate(request) and isinstance(order, Order)
                     and order.status in (OrderStatus.OPEN, OrderStatus.PENDING)]
        if not to_cancel:
            return placed
        delay = max((float(placed[i].info.get("delay") or 0) for i in to_cancel  # type: ignore[union-attr]
                     if placed[i].status == OrderStatus.PENDING), default=0.0)  # type: ignore[union-attr]
        if delay:
            await asyncio.sleep(delay + DELAY_GRACE_S)
        cancelled = await self.cancel_orders([placed[i].id for i in to_cancel])  # type: ignore[union-attr]
        out = list(placed)
        for i, result in zip(to_cancel, cancelled):
            original = placed[i]
            if isinstance(result, Order):
                request = requests[i]
                out[i] = result.model_copy(update={
                    "client_order_id": request.client_order_id, "type": request.type,
                    "time_in_force": request.time_in_force, "book": request.book, "trader": request.trader,
                    "tags": request.tags,
                })
            else:
                out[i] = result if not isinstance(original, Order) else original
        return out

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        """Cancel one offer. One that can no longer be cancelled (matched,
        already cancelled) is read back as it stands."""
        try:
            row = await self._call("DELETE", f"/v2/offers/{order_id}", kind="write", priority=Priority.HIGH)
        except BadRequest as exc:
            order = await self.fetch_order(order_id)
            if order.is_terminal:
                return order
            raise OrderRejected(f"{VENUE}: cancel of {order_id} refused: {reason_of(exc.body, str(exc))}",
                                reason="cancel_refused", body=exc.body) from None
        return order_of_offer(row or {}, account=self.account)

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        """Cancel up to fifty offers a request. An offer not in Matchbook's
        answer (no longer open) is read back as it stands."""
        cancelled: dict[str, dict[str, Any]] = {}
        for start in range(0, len(order_ids), OFFERS_PER_REQUEST):
            chunk = order_ids[start:start + OFFERS_PER_REQUEST]
            raw = await self._call("DELETE", "/v2/offers", params={"offer-ids": ",".join(chunk)},
                                   kind="write", priority=Priority.HIGH) or {}
            for row in raw.get("offers") or []:
                if isinstance(row, dict) and row.get("id") is not None:
                    cancelled[str(row["id"])] = row
        out: list[Order | Exception] = []
        for order_id in order_ids:
            if str(order_id) in cancelled:
                out.append(order_of_offer(cancelled[str(order_id)], account=self.account))
                continue
            try:
                out.append(await self.fetch_order(order_id))
            except OrderNotFound as exc:
                out.append(exc)
        return out

    def _scope(self, market_id: str | None) -> dict[str, Any]:
        """Matchbook's filter for a runner, a market pair or an event id."""
        if not market_id:
            return {}
        parts = ids.native(VENUE, market_id).split("-")
        if not all(part.isdigit() for part in parts) or len(parts) > 3:
            raise BadRequest(f"{VENUE}: {market_id!r} is not a Matchbook runner, market or event id")
        if len(parts) == 3:
            return {"runner-ids": parts[2]}
        if len(parts) == 2:
            return {"market-ids": parts[1]}
        return {"event-ids": parts[0]}

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int:
        """Every open offer on a runner, a market (`<event>-<market>`) or the
        account. Returns how many Matchbook cancelled."""
        raw = await self._call("DELETE", "/v2/offers", params=self._scope(market_id) or None,
                               kind="write", priority=Priority.HIGH) or {}
        return len(raw.get("offers") or [])

    async def edit_order(self, request: EditRequest, *, current: Order | None = None) -> Order:
        """New odds or a new remaining stake on an unmatched offer, in place.
        `quantity` is the order's new total, filled contracts included."""
        if request.time_in_force is not None or request.expires_at is not None:
            raise InvalidOrder(f"{VENUE}: an offer's time in force cannot be edited; cancel and replace")
        current = current or await self.fetch_order(request.order_id)
        if request.price is None and request.quantity is None:
            return current
        info = current.info
        current_odds = D(info.get("decimal-odds") or info.get("odds"))
        current_stake = D(info.get("remaining"))
        odds = odds_for(current.side, request.price) if request.price is not None else current_odds
        left = (D(request.quantity) - current.filled) if request.quantity is not None else current_stake * current_odds
        if left <= 0:
            raise InvalidOrder(f"{VENUE}: the new quantity leaves nothing unmatched; cancel the offer instead")
        body = {
            "current-odds": _number(current_odds), "new-odds": _number(odds),
            "current-stake": _number(current_stake), "new-stake": _number(stake_for(left, odds)),
        }
        try:
            row = await self._call("PUT", f"/v2/offers/{request.order_id}", body=body, kind="write")
        except BadRequest as exc:
            edit = (exc.body or {}).get("offer-edit") if isinstance(exc.body, dict) else None
            if isinstance(edit, dict) and edit.get("errors"):
                raise InvalidOrder(f"{VENUE}: edit refused: {reason_of(edit, 'refused')}") from None
            raise error_of(exc) from None
        edit = (row or {}).get("offer-edit") or {}
        if edit.get("errors"):
            raise InvalidOrder(f"{VENUE}: edit refused: {reason_of(edit, 'refused')}")
        order = order_of_offer(row or {}, account=self.account)
        return order.model_copy(update={"client_order_id": current.client_order_id})

    async def fetch_order(self, order_id: str) -> Order:
        """One offer by id, in any state, while its market is unsettled."""
        row = await self._call("GET", f"/v2/offers/{order_id}")
        if not isinstance(row, dict) or not row.get("id"):
            raise OrderNotFound(f"{VENUE}: no offer {order_id}")
        return order_of_offer(row, account=self.account)

    async def _offers(self, params: dict[str, Any], *, offset: int = 0, pages: int = MAX_PAGES) -> tuple[list[dict[str, Any]], int | None]:
        """Offer rows from `offset`, page by page; and the next offset, when
        more are left."""
        rows: list[dict[str, Any]] = []
        for _ in range(pages):
            raw = await self._call("GET", "/v2/offers", params={**params, "offset": offset, "per-page": PER_PAGE}) or {}
            page = [r for r in raw.get("offers") or [] if isinstance(r, dict)]
            rows += page
            offset += len(page)
            if not page or offset >= int(raw.get("total") or 0):
                return rows, None
        return rows, offset

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        """Every resting offer, or every one on a runner or market."""
        rows, _ = await self._offers({"status": "open,edited", **self._scope(market_id)})
        return [order_of_offer(row, account=self.account) for row in rows]

    async def fetch_orders(
        self, *, status: str | None = None, market_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Order]:
        """One page of offers on unsettled markets, in any state: `open`,
        `closed` (fully matched) or `canceled` (cancelled or flushed).
        `since` keeps offers changed since then."""
        native = {"open": "open,edited", "closed": "matched", "canceled": "cancelled,flushed"}
        if status is not None and status not in native:
            raise BadRequest(f"{VENUE}: status is open, closed or canceled")
        params: dict[str, Any] = {"status": native.get(status or ""), **self._scope(market_id)}
        if since is not None:
            params["interval"] = max(1, int(time.time() - since / 1000))
        offset = _offset(cursor)
        raw = await self._call("GET", "/v2/offers", params={**params, "offset": offset,
                                                             "per-page": min(limit or PER_PAGE, PER_PAGE)}) or {}
        rows = [r for r in raw.get("offers") or [] if isinstance(r, dict)]
        orders = [order_of_offer(row, account=self.account) for row in rows]
        if status == "closed":
            orders = [o for o in orders if o.status == OrderStatus.CLOSED]
        following = offset + len(rows)
        return Page(orders, next_cursor=str(following) if rows and following < int(raw.get("total") or 0) else None)

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        """The matched bets on this account's offers in unsettled markets, one
        fill each; settled ones are in `fetch_settlements`."""
        if order_id:
            fills = fills_of_offer((await self.fetch_order(order_id)).info, account=self.account)
            next_cursor = None
        else:
            offset = _offset(cursor)
            rows, following = await self._offers({"status": "matched", **self._scope(market_id)}, offset=offset, pages=1)
            fills = [f for row in rows for f in fills_of_offer(row, account=self.account)]
            next_cursor = str(following) if following is not None else None
        if since is not None:
            fills = [f for f in fills if f.timestamp >= since]
        fills.sort(key=lambda f: f.timestamp, reverse=True)
        return Page(fills[:limit] if limit else fills, next_cursor=next_cursor)

    # -- account --------------------------------------------------------------

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        """Cash (`balance`), what bets hold (`exposure` and the commission
        reserve, `locked`), and what is free to bet."""
        raw = await self._call("GET", "/account/balance") or {}
        total = D(raw.get("balance"))
        free = D(raw.get("free-funds"))
        locked = abs(D(raw.get("exposure"))) + abs(D(raw.get("commission-reserve")))
        return Balance(
            venue=VENUE, account=account or self.account, currency=await self.currency(),
            total=total, available=free, locked=locked, buying_power=free,
            timestamp=int(time.time() * 1000), info=raw,
        )

    async def _reports(self, path: str, params: dict[str, Any], *, offset: int = 0, pages: int = MAX_PAGES) -> tuple[dict[str, Any], int | None]:
        """A bets report, its markets merged across pages."""
        merged: dict[str, Any] = {"markets": []}
        for _ in range(pages):
            raw = await self._call("GET", path, params={**params, "offset": offset, "per-page": PER_PAGE,
                                                        "odds-type": "DECIMAL"}, reports=True) or {}
            page = [m for m in raw.get("markets") or [] if isinstance(m, dict)]
            merged = {**raw, "markets": merged["markets"] + page}
            offset += len(page)
            if not page or offset >= int(raw.get("total") or 0):
                return merged, None
        return merged, offset

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        """The account's current matched bets netted per runner on the YES leg.
        `market_id` is a runner, `event_id` a Matchbook market (`<event>-<market>`)."""
        params: dict[str, Any] = {}
        wanted = None
        if market_id:
            _, market, _ = parse_runner_id(ids.native(VENUE, market_id))
            params["market-ids"] = market
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
        elif event_id:
            _, market = parse_market_id(ids.native(VENUE, event_id))
            params["market-ids"] = market
        body, _ = await self._reports("/reports/v2/bets/current", params)
        positions = positions_of_bets(body, account=self.account)
        return [p for p in positions if p.market_id == wanted] if wanted else positions

    async def fetch_settlements(
        self, *, market_id: str | None = None, since: int | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page[Settlement]:
        """One page of settled bets, one settlement per runner and side, newest
        markets first as Matchbook lists them."""
        params: dict[str, Any] = {}
        if market_id:
            params["market-ids"] = parse_market_id(ids.native(VENUE, market_id))[1]
        if since is not None:
            from ..types import iso

            params["after"] = iso(since)
        body, following = await self._reports("/reports/v2/bets/settled", params, offset=_offset(cursor), pages=1)
        settlements = settlements_of(body, account=self.account)
        if market_id and len(ids.native(VENUE, market_id).split("-")) == 3:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            settlements = [s for s in settlements if s.market_id == wanted]
        return Page(settlements[:limit] if limit else settlements,
                    next_cursor=str(following) if following is not None else None)

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, quantity: Decimal) -> FeeEstimate:
        """The commission on `quantity` contracts at the YES `price` if the bet
        wins: a share of its profit. A `sell` lays the runner, which wins
        `price` a contract when the runner loses. Makers pay the same."""
        schedule = await asyncio.to_thread(self.catalog.fetch_fee_schedule, market_id)
        paid = D(price) if side == Side.BUY else ONE - D(price)
        fee = schedule.estimate(float(paid), float(quantity), taker=True)
        event, market = parse_market_id(ids.native(VENUE, market_id))
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, ids.native(VENUE, market_id)), side=side,
            price=D(price), quantity=D(quantity),
            taker_fee=D(fee) if fee is not None else None, maker_fee=D(fee) if fee is not None else None,
            currency=await self.currency(),
            info={"schedule": schedule.model_dump(), "market": market_native_id(event, market),
                  "charged": "at settlement, on net winnings in the market"},
        )

    async def close(self) -> None:
        await self.http.close()
        self.session.close()
        if self._own_catalog:
            self.catalog.close()


def _offset(cursor: str | None) -> int:
    if not cursor:
        return 0
    if not cursor.isdigit():
        raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
    return int(cursor)


__all__ = ["MatchbookTrading", "order_of_offer", "fills_of_offer", "positions_of_bets", "settlements_of", "build_offer"]
