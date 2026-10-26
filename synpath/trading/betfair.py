"""Betfair order entry: bets placed, replaced, reduced and cancelled, and the
account's orders, matched bets, settled bets and funds on the YES leg.

Built to Betfair's Betting API (`placeOrders`, `cancelOrders`,
`replaceOrders`, `updateOrders`, `listCurrentOrders`, `listClearedOrders`,
`listMarketProfitAndLoss`) and Accounts API (`getAccountFunds`,
`getAccountDetails`) documentation; its records follow the documentation's
examples. Every call carries the application key and the session of the
account's login, interactive or by certificate, exactly as the read adapter
makes it; see `synpath.betfair.BetfairSession`. **Betting needs a live
application key**: Betfair refuses every bet from a delayed key
(`PERMISSION_DENIED`), which still reads the account.

**An order is a bet to back or lay one runner.** `buy` backs the runner
(takes its YES) at decimal odds 1/price; `sell` lays it (takes its NO) at the
same odds; Sell YES and Buy NO are the same lay. Betfair counts both in the
backer's stake (a lay's `size` is the backer's stake it accepts): an order for `n` contracts stakes `n / odds`,
rounded down to the penny, and a lay's liability is that stake times
`odds - 1`. Prices are snapped onto Betfair's CLASSIC odds ladder (1.01 to
1000) on the cheaper side: a buy at higher odds (a lower YES price), a sell
at lower odds (a higher YES price).

**Minimum stakes.** A bet stakes at least the currency's minimum bet size
(£1, €1, $1) unless what it pays out, stake times odds (its contracts), is at
least the currency's minimum bet payout (£10, €20, $20): 10p at 100 is a bet.
The payout rule is not offered on the Italian and Spanish sites. Orders below
both are refused before anything is sent.

**Persistence and time in force.** A limit (`gtc`) is Betfair's `LIMIT` with
persistence `LAPSE` by default: its unmatched part is cancelled when the
market turns in play. `params={"persistence": "PERSIST"}` (or
`keep_in_play=True`) keeps it in play; `"MARKET_ON_CLOSE"` moves it into the
starting-price auction at the off. `fok` is Betfair's `FILL_OR_KILL`, matched
whole or not at all (`params={"min_fill": contracts}` accepts a part of at
least that much), and Betfair reads its price as the worst *average* price.
Betfair has no immediate-or-cancel bet, so a market order, or an `ioc`
limit, is a `LAPSE` bet at the price given as the worst accepted whose
unmatched rest is cancelled as soon as Betfair answers. GTD, day and
post-only orders are not offered.

**In-play delay.** On an in-play market Betfair holds a bet for the market's
`betDelay` (1 to 10 seconds) before matching it, and `placeOrders` answers
only after that, so an order returns already matched or resting. A request
Betfair could not finish in time answers `TIMEOUT`; the order is then looked
up by its client order id, which Betfair keeps.

**Client order ids.** `client_order_id` is sent as the bet's
`customerOrderRef` (up to 32 characters), which every Betfair record and the
order stream carry back, and a single order's id also de-duplicates a
resubmission within 60 seconds (`customerRef`).

**Edits.** A new price is `replaceOrders`: Betfair cancels the unmatched part
and places it again at the new odds as a new bet with a new id, losing its
place in the queue. A smaller quantity is a partial cancel, which keeps the
place. Betfair cannot raise a bet's stake. `update_persistence` changes what
happens at the off in place (`updateOrders`).

**Positions** net per runner on the YES leg from the account's matched bets
on unsettled markets: backs are YES contracts, lays NO contracts, a matched
stake S at average odds O being S·O contracts either way.

**Fees** are commission on the account's net winnings per Betfair market at
settlement, at the market's base rate (5% on most .com markets).
"""
from __future__ import annotations

import asyncio
import bisect
import re
import time
from decimal import ROUND_DOWN, Decimal
from typing import Any

from .. import ids
from ..base import AsyncHttpClient, Capability
from ..betfair import (
    AUTH_ERRORS, CLASSIC_LADDER, JURISDICTIONS, RATE_ERRORS, SESSION_ERRORS, BetfairSession, api_error_code,
    iso_ms, parse_runner_id, runner_native_id,
)
from ..errors import AuthenticationError, BadRequest, ExchangeError, RateLimitExceeded, RequestTimeout
from ..types import Page, iso
from .base import TradingExchange
from .credentials import BetfairCredentials
from .errors import (
    DuplicateClientOrderId, InsufficientFunds, InvalidOrder, MarketHalted, OrderNotFound, OrderRejected,
    PermissionDenied,
)
from .limiter import BudgetLimiter, Priority
from .types import (
    Account, Balance, EditRequest, FeeEstimate, Fill, Liquidity, Order, OrderRequest, OrderStatus, OrderType,
    Position, PositionSide, Settlement, Side, TimeInForce, VENUE_ORDER_TYPES,
)

VENUE = "betfair"
ONE = Decimal("1")
ZERO = Decimal("0")
PRICE_PLACES = Decimal("0.0000000001")
PENNY = Decimal("0.01")

BETTING_PATH = "/exchange/betting/rest/v1.0"
ACCOUNT_PATH = "/exchange/account/rest/v1.0"

PLACE_PER_REQUEST = {"it": 50}
"""Place instructions one `placeOrders` request carries: 200 on the global
exchange, 50 on the Italian one."""
DEFAULT_PLACE_PER_REQUEST = 200
CANCEL_PER_REQUEST = 60
"""Cancel, replace and update instructions per request."""

PAGE_SIZE = 1000
"""Records one `listCurrentOrders` or `listClearedOrders` request returns at most."""

MAX_PAGES = 50
"""Most pages one listing call reads."""

MAX_IDS = 250
"""Bet ids and market ids together one `listCurrentOrders` request may name."""

REF_LIMIT = 32
"""Longest `customerOrderRef` and `customerRef` Betfair takes."""
CUSTOMER_REF = re.compile(r"^[A-Za-z0-9:\-._+*;~]{1,32}$")
"""What a `customerRef` (the request's de-duplication key) may contain."""

PERSISTENCE = ("LAPSE", "PERSIST", "MARKET_ON_CLOSE")

MINIMUMS: dict[str, tuple[Decimal, Decimal]] = {
    "GBP": (Decimal("1"), Decimal("10")), "EUR": (Decimal("1"), Decimal("20")),
    "USD": (Decimal("1"), Decimal("20")), "AUD": (Decimal("1"), Decimal("30")),
    "CAD": (Decimal("2"), Decimal("30")), "HKD": (Decimal("10"), Decimal("125")),
    "DKK": (Decimal("15"), Decimal("150")), "NOK": (Decimal("10"), Decimal("150")),
    "SEK": (Decimal("15"), Decimal("150")), "SGD": (Decimal("2"), Decimal("30")),
    "NZD": (Decimal("2"), Decimal("10")), "RON": (Decimal("5"), Decimal("50")),
    "BRL": (Decimal("5"), Decimal("50")), "MXN": (Decimal("30"), Decimal("300")),
}
"""(Min Bet Size, Min Bet Payout) by account currency, from Betfair's
Currency Parameters. A bet is valid at or above the first, or below it when
its payout reaches the second."""
NO_PAYOUT_RULE = frozenset({"it", "es"})
"""Sites where the minimum-payout rule is not offered."""

CLEARED_STATUSES = ("CANCELLED", "LAPSED", "SETTLED", "VOIDED")
"""The `betStatus` values `fetch_order` searches, in order, for a bet that has
left the current orders."""

SIDES = {"BACK": Side.BUY, "LAY": Side.SELL, "B": Side.BUY, "L": Side.SELL}


def D(value: Any) -> Decimal:
    if value in (None, ""):
        return ZERO
    return Decimal(str(value))


def yes_price(odds: Any) -> Decimal:
    """Decimal odds as the YES price 1/O, to ten places."""
    return (ONE / D(odds)).quantize(PRICE_PLACES)


def side_of(native: Any) -> Side:
    """`BACK` takes YES; `LAY` takes NO."""
    return SIDES.get(str(native or "").upper(), Side.SELL)


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
        index = bisect.bisect_left(CLASSIC_LADDER, raw)
        if index >= len(CLASSIC_LADDER):
            raise InvalidOrder(f"{VENUE}: {price} is below the ladder's lowest YES price (odds 1000)")
        return CLASSIC_LADDER[index]
    index = bisect.bisect_right(CLASSIC_LADDER, raw) - 1
    if index < 0:
        raise InvalidOrder(f"{VENUE}: {price} is above the ladder's highest YES price (odds 1.01)")
    return CLASSIC_LADDER[index]


def stake_for(quantity: Any, odds: Decimal) -> Decimal:
    """The backer's stake for `quantity` contracts at `odds`, down to the penny."""
    stake = (D(quantity) / odds).quantize(PENNY, rounding=ROUND_DOWN)
    if stake <= 0:
        raise InvalidOrder(f"{VENUE}: {quantity} contracts at odds {odds} is less than a penny of stake")
    return stake


def check_minimum(stake: Decimal, odds: Decimal, *, currency: str | None, jurisdiction: str = "com") -> None:
    """Refuse a bet below both of the currency's minimums. A currency this
    module has no figures for is left for Betfair to judge."""
    minimums = MINIMUMS.get((currency or "").upper())
    if minimums is None:
        return
    size, payout = minimums
    if stake >= size:
        return
    if jurisdiction not in NO_PAYOUT_RULE and stake * odds >= payout:
        return
    rule = f"{size} {currency}" + ("" if jurisdiction in NO_PAYOUT_RULE else f", or a payout of {payout} {currency}")
    raise InvalidOrder(f"{VENUE}: a stake of {stake} at odds {odds} is below Betfair's minimum bet ({rule})")


def _number(value: Decimal) -> float | int:
    """A Decimal as the JSON number Betfair reads."""
    return int(value) if value == value.to_integral_value() else float(value)


def _runner_id(market_id: Any, selection_id: Any, handicap: Any) -> str:
    return ids.qualify(VENUE, runner_native_id(str(market_id), selection_id, handicap or 0))


def _parse_runner(market_id: str) -> tuple[str, int, float]:
    native = ids.native(VENUE, market_id)
    return parse_runner_id(native)


def _market_of(market_id: str) -> str:
    """The Betfair market id of a runner id or of a Betfair market (event) id."""
    native = ids.native(VENUE, market_id)
    return parse_runner_id(native)[0] if "-" in native else native


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

def _price_size(row: dict[str, Any]) -> tuple[Decimal, Decimal]:
    """(odds, stake) of a current order or of a place instruction."""
    price_size = row.get("priceSize") or row.get("limitOrder") or (row.get("instruction") or {}).get("limitOrder") or {}
    return D(price_size.get("price")), D(price_size.get("size"))


def status_of(row: dict[str, Any], *, size: Decimal) -> OrderStatus:
    """An order's state from Betfair's status and the amounts on it. A bet no
    longer executable ended matched in full (`closed`), killed by its fill or
    kill (`expired`), or cancelled or lapsed for the rest (`canceled`)."""
    native = str(row.get("status") or row.get("orderStatus") or "").upper()
    matched = D(row.get("sizeMatched"))
    if native == "PENDING":
        return OrderStatus.PENDING
    if native == "EXECUTABLE":
        return OrderStatus.OPEN
    if native == "EXPIRED":
        return OrderStatus.EXPIRED
    if native == "EXECUTION_COMPLETE":
        ended = D(row.get("sizeCancelled")) + D(row.get("sizeLapsed")) + D(row.get("sizeVoided"))
        if ended > 0 or (size > 0 and matched < size):
            return OrderStatus.CANCELED
        return OrderStatus.CLOSED
    return OrderStatus.OPEN


TIME_IN_FORCE = {"FILL_OR_KILL": TimeInForce.FOK}


def order_of_current(row: dict[str, Any], *, account: Account | None = None, currency: str | None = None) -> Order:
    """One current order (`listCurrentOrders`, or a stream order in the same
    shape). Stakes become contracts at the bet's own odds; what matched is
    averaged at its matched odds."""
    odds, size = _price_size(row)
    matched, remaining = D(row.get("sizeMatched")), D(row.get("sizeRemaining"))
    status = status_of(row, size=size)
    average = D(row.get("averagePriceMatched"))
    return Order(
        id=str(row.get("betId") or ""),
        client_order_id=row.get("customerOrderRef") or None,
        venue=VENUE,
        account=account,
        market_id=_runner_id(row.get("marketId"), row.get("selectionId"), row.get("handicap")),
        side=side_of(row.get("side")),
        type=OrderType.LIMIT,
        time_in_force=TIME_IN_FORCE.get(str(row.get("timeInForce") or ""), TimeInForce.GTC),
        status=status,
        price=yes_price(odds) if odds else None,
        quantity=size * odds,
        filled=matched * odds,
        remaining=remaining * odds if status in (OrderStatus.OPEN, OrderStatus.PENDING) else ZERO,
        average_price=yes_price(average) if matched and average else None,
        cost=matched if side_of(row.get("side")) == Side.BUY else matched * (average - 1) if average else None,
        fee_currency=currency,
        created_at=iso_ms(row.get("placedDate")),
        updated_at=iso_ms(row.get("matchedDate")),
        info=row,
    )


def order_of_report(
    report: dict[str, Any], *, market_id: str, request: OrderRequest | None = None, account: Account | None = None,
    currency: str | None = None,
) -> Order:
    """The order from a successful place instruction report. Betfair answers
    with the bet id and what matched on arrival; the order's state follows
    from those when the report carries none."""
    instruction = report.get("instruction") or {}
    limit = instruction.get("limitOrder") or {}
    odds, size = D(limit.get("price")), D(limit.get("size"))
    matched = D(report.get("sizeMatched"))
    native = report.get("orderStatus")
    fok = limit.get("timeInForce") == "FILL_OR_KILL"
    if native is None:
        if matched >= size > 0:
            native = "EXECUTION_COMPLETE"
        elif fok:
            native = "EXPIRED"
        else:
            native = "EXECUTABLE"
    elif fok and native == "EXECUTION_COMPLETE" and matched < size:
        native = "EXPIRED"
    row = {
        "betId": report.get("betId"), "marketId": market_id, "selectionId": instruction.get("selectionId"),
        "handicap": instruction.get("handicap"), "priceSize": {"price": limit.get("price"), "size": limit.get("size")},
        "side": instruction.get("side"), "status": native, "persistenceType": limit.get("persistenceType"),
        "orderType": instruction.get("orderType"), "placedDate": report.get("placedDate"),
        "averagePriceMatched": report.get("averagePriceMatched"), "sizeMatched": report.get("sizeMatched"),
        "sizeRemaining": _number(size - matched) if native == "EXECUTABLE" else 0,
        "customerOrderRef": instruction.get("customerOrderRef"), "timeInForce": limit.get("timeInForce"),
    }
    order = order_of_current(row, account=account, currency=currency)
    update: dict[str, Any] = {"info": {**report, "marketId": market_id}}
    if request is not None:
        update.update({
            "client_order_id": request.client_order_id, "type": request.type, "time_in_force": request.time_in_force,
            "book": request.book, "trader": request.trader, "tags": request.tags,
        })
    if odds and size and matched >= size:
        update["remaining"] = ZERO
    return order.model_copy(update=update)


def order_of_cleared(row: dict[str, Any], *, account: Account | None = None, currency: str | None = None) -> Order:
    """A bet that has left the current orders, from `listClearedOrders` at bet
    level: cancelled or lapsed (nothing matched), voided, or settled."""
    bet_status = str(row.get("betStatus") or "").upper()
    odds = D(row.get("priceRequested"))
    settled = D(row.get("sizeSettled"))
    left = D(row.get("sizeCancelled"))
    matched_odds = D(row.get("priceMatched")) or odds
    size = settled + left
    status = OrderStatus.CLOSED if bet_status == "SETTLED" and not left else OrderStatus.CANCELED
    return Order(
        id=str(row.get("betId") or ""),
        client_order_id=row.get("customerOrderRef") or None,
        venue=VENUE,
        account=account,
        market_id=_runner_id(row.get("marketId"), row.get("selectionId"), row.get("handicap")),
        side=side_of(row.get("side")),
        type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        status=status,
        price=yes_price(odds) if odds else None,
        quantity=size * odds,
        filled=settled * odds if bet_status == "SETTLED" else ZERO,
        remaining=ZERO,
        average_price=yes_price(matched_odds) if settled and matched_odds else None,
        fee_currency=currency,
        created_at=iso_ms(row.get("placedDate")),
        updated_at=iso_ms(row.get("settledDate")),
        info=row,
    )


def fill_of_current(row: dict[str, Any], *, account: Account | None = None, currency: str | None = None) -> Fill | None:
    """What has matched on one bet, as one fill at its average matched odds.
    Betfair lists bets, not their matched fragments, so the fill carries the
    bet's id and its matched stake times those odds in contracts."""
    matched, average = D(row.get("sizeMatched")), D(row.get("averagePriceMatched"))
    if not matched or not average:
        return None
    return Fill(
        id=str(row.get("betId") or ""),
        order_id=str(row.get("betId") or ""),
        client_order_id=row.get("customerOrderRef") or None,
        venue=VENUE,
        account=account,
        market_id=_runner_id(row.get("marketId"), row.get("selectionId"), row.get("handicap")),
        side=side_of(row.get("side")),
        price=yes_price(average),
        quantity=matched * average,
        fee=None,
        fee_currency=currency,
        liquidity=Liquidity.UNKNOWN,
        timestamp=iso_ms(row.get("matchedDate")) or iso_ms(row.get("placedDate")) or 0,
        info={**row, "aggregated": True},
    )


def positions_of_orders(rows: list[dict[str, Any]], *, account: Account | None = None) -> list[Position]:
    """Current orders' matched stakes netted per runner on the YES leg: backs
    are YES contracts, lays NO contracts, at each bet's average matched odds."""
    held: dict[str, dict[str, Any]] = {}
    for row in rows:
        matched, average = D(row.get("sizeMatched")), D(row.get("averagePriceMatched"))
        if not matched or not average:
            continue
        synpath_id = _runner_id(row.get("marketId"), row.get("selectionId"), row.get("handicap"))
        entry = held.setdefault(synpath_id, {"yes": ZERO, "no": ZERO, "stake": ZERO, "margin": ZERO, "bets": []})
        back = side_of(row.get("side")) == Side.BUY
        entry["yes" if back else "no"] += matched * average
        entry["stake"] += matched
        entry["margin"] += matched if back else matched * (average - 1)
        entry["bets"].append(row.get("betId"))
    out = []
    for synpath_id, entry in held.items():
        net = entry["yes"] - entry["no"]
        traded = entry["yes"] + entry["no"]
        out.append(Position(
            venue=VENUE,
            account=account,
            market_id=synpath_id,
            side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
            contracts=abs(net),
            inventory_yes=entry["yes"],
            inventory_no=entry["no"],
            # The average YES price of every matched bet: stakes over contracts.
            entry_price=(entry["stake"] / traded).quantize(PRICE_PLACES) if traded else None,
            margin=entry["margin"],
            info={"bets": entry["bets"]},
        ))
    return out


RESULTS = {("BACK", "WON"): "yes", ("BACK", "LOST"): "no", ("LAY", "WON"): "no", ("LAY", "LOST"): "yes",
           ("BACK", "PLACE"): "yes", ("LAY", "PLACE"): "yes"}
"""A settled bet's outcome, which is the bet's own (a lay that won means the
runner lost), as the runner's YES result."""


def settlement_of(row: dict[str, Any], *, account: Account | None = None) -> Settlement:
    """One settled bet (`listClearedOrders`, bet level)."""
    side = str(row.get("side") or "").upper()
    odds = D(row.get("priceMatched")) or D(row.get("priceRequested"))
    stake = D(row.get("sizeSettled"))
    profit = row.get("profit")
    return Settlement(
        venue=VENUE,
        account=account,
        market_id=_runner_id(row.get("marketId"), row.get("selectionId"), row.get("handicap")),
        held=PositionSide.LONG if side == "BACK" else PositionSide.SHORT,
        result=RESULTS.get((side, str(row.get("betOutcome") or "").upper())),
        won=(D(profit) > 0) if profit is not None else None,
        quantity=stake * odds,
        cost=stake if side == "BACK" else None,
        pnl=D(profit) if profit is not None else None,
        timestamp=iso_ms(row.get("settledDate")),
        info=row,
    )


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

INVALID = frozenset({
    "INVALID_BET_SIZE", "INVALID_RUNNER", "INVALID_ODDS", "INVALID_PERSISTENCE_TYPE", "INVALID_BACK_LAY_COMBINATION",
    "INVALID_BID_TYPE", "INVALID_PRICE_EDIT", "INVALID_MIN_FILL_SIZE", "INVALID_CUSTOMER_ORDER_REF",
    "INVALID_ORDER", "INVALID_MARKET_ID", "INVALID_INPUT_DATA", "TOO_MANY_INSTRUCTIONS", "INVALID_MARKET_VERSION",
    "INVALID_PROFIT_RATIO", "INVALID_ORDER_TYPE", "INVALID_PERSISTENCE",
})
HALTED = frozenset({
    "MARKET_SUSPENDED", "MARKET_NOT_OPEN_FOR_BETTING", "RUNNER_REMOVED", "MARKET_NOT_OPEN_FOR_BSP_BETTING",
})


def error_of(code: str | None, *, what: str, body: Any = None) -> ExchangeError:
    """Betfair's refusal of an instruction or a whole request as a typed
    error, by its error code."""
    code = str(code or "")
    text = f"{VENUE}: {what} refused: {code or 'no reason given'}"
    if code == "INSUFFICIENT_FUNDS":
        return InsufficientFunds(text, body=body)
    if code in HALTED:
        return MarketHalted(text, body=body)
    if code == "PERMISSION_DENIED":
        return PermissionDenied(
            f"{text}: Betfair takes bets only on a live application key, and not from restricted countries",
            body=body,
        )
    if code == "DUPLICATE_TRANSACTION":
        return DuplicateClientOrderId(f"{text}: this client order id was sent in the last 60 seconds")
    if code == "INVALID_BET_ID":
        return OrderNotFound(text, body=body)
    if code in INVALID:
        return InvalidOrder(text)
    return OrderRejected(text, reason=code.lower() or None, info=body if isinstance(body, dict) else {}, body=body)


def _failure(report: dict[str, Any], execution: dict[str, Any], *, what: str) -> ExchangeError:
    """The error for one failed instruction: its own code, else the request's."""
    code = report.get("errorCode")
    if not code or code in ("ERROR_IN_ORDER", "RELATED_ACTION_FAILED"):
        code = execution.get("errorCode") if execution.get("errorCode") != "BET_ACTION_ERROR" else code
    return error_of(code, what=what, body=report)


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

def immediate(request: OrderRequest) -> bool:
    """Whether the order takes what is there and leaves nothing resting
    (other than a fill or kill, which Betfair holds to that itself)."""
    return request.type == OrderType.MARKET or request.time_in_force == TimeInForce.IOC


def persistence_of(params: dict[str, Any]) -> str:
    """The `persistenceType` a request asks for: `LAPSE` unless it says
    otherwise (`persistence=`, or `keep_in_play=True` for `PERSIST`)."""
    wanted = params.get("persistence")
    if wanted is None:
        return "PERSIST" if params.get("keep_in_play") else "LAPSE"
    wanted = str(wanted).upper()
    if wanted not in PERSISTENCE:
        raise InvalidOrder(f"{VENUE}: persistence is LAPSE, PERSIST or MARKET_ON_CLOSE, not {params['persistence']!r}")
    return wanted


def check_request(request: OrderRequest) -> None:
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"{VENUE}: {request.type.value} is held by the execution engine, not the venue; submit it through the engine"
        )
    if request.time_in_force in (TimeInForce.GTD, TimeInForce.DAY):
        raise InvalidOrder(f"{VENUE}: Betfair bets rest until matched, cancelled or lapsed; gtc, ioc or fok only")
    if request.price is None:
        raise InvalidOrder(f"{VENUE}: a price is required -- a market order is sent at the worst price you give")
    if request.post_only:
        raise InvalidOrder(f"{VENUE}: Betfair has no post-only bet")
    if request.client_order_id is not None and len(request.client_order_id) > REF_LIMIT:
        raise InvalidOrder(f"{VENUE}: a client order id is at most {REF_LIMIT} characters (Betfair's customerOrderRef)")


def build_instruction(request: OrderRequest) -> dict[str, Any]:
    """One request as a Betfair place instruction, before anything is sent."""
    check_request(request)
    _, selection, handicap = _parse_runner(request.market_id)
    odds = odds_for(request.side, request.price)
    stake = stake_for(request.quantity, odds)
    limit: dict[str, Any] = {"size": _number(stake), "price": _number(odds)}
    if request.time_in_force == TimeInForce.FOK:
        limit["timeInForce"] = "FILL_OR_KILL"
        if request.params.get("min_fill") is not None:
            limit["minFillSize"] = _number(stake_for(request.params["min_fill"], odds))
    else:
        persistence = persistence_of(request.params)
        if immediate(request) and persistence != "LAPSE":
            raise InvalidOrder(f"{VENUE}: a market or ioc order leaves nothing resting, so it cannot persist")
        limit["persistenceType"] = persistence
    instruction: dict[str, Any] = {
        "selectionId": selection, "handicap": handicap, "side": "BACK" if request.side == Side.BUY else "LAY",
        "orderType": "LIMIT", "limitOrder": limit,
    }
    if request.client_order_id:
        instruction["customerOrderRef"] = request.client_order_id
    return instruction


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class BetfairTrading(TradingExchange):
    """Betfair Exchange order entry, on the account's live application key.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.betfair import BetfairTrading

    async with BetfairTrading(require("betfair", load_credentials())) as bf:
        print(await bf.fetch_balance())
    ```
    """

    id = VENUE
    name = "Betfair Exchange"
    has: dict[str, Capability] = {
        "create_order": True,
        # One placeOrders per Betfair market, 200 bets a request; atomic per market.
        "create_orders": True,
        "cancel_order": True,
        "cancel_orders": True,
        "cancel_all_orders": True,
        # A new price is a new bet (replaceOrders); a smaller quantity a partial cancel.
        "edit_order": True,
        "fetch_order": True,
        "fetch_open_orders": True,
        # Bets on markets not yet settled only.
        "fetch_orders": "partial",
        # One aggregate fill per bet once it has stopped matching, on unsettled markets.
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
        credentials: BetfairCredentials | None = None,
        *,
        session: BetfairSession | None = None,
        session_token: str | None = None,
        account_name: str = "default",
        catalog: Any = None,
        limiter: BudgetLimiter | None = None,
        timeout: float = 60.0,
        client: Any = None,
        identity_client: Any = None,
    ):
        """`credentials` log in on the first call (by certificate when the
        certificate and key are set); `session_token` reuses a login made
        elsewhere. `timeout` allows for the in-play delay a placement waits."""
        import httpx

        if session is None:
            if credentials is None:
                from .errors import CredentialsMissing

                raise CredentialsMissing(
                    f"{VENUE}: set BETFAIR_APP_KEY, BETFAIR_USERNAME and BETFAIR_PASSWORD (a live application key bets)"
                )
            session = BetfairSession(
                app_key=credentials.app_key, username=credentials.username, password=credentials.password,
                cert_path=credentials.cert_path, key_path=credentials.key_path,
                jurisdiction=credentials.jurisdiction, session_token=session_token, client=identity_client,
                timeout=timeout,
            )
        self.credentials = credentials
        self.session = session
        self.jurisdiction = session.jurisdiction
        self.account = Account(venue=VENUE, name=account_name)
        # Betfair publishes no request rate for betting, only a charge on
        # accounts that send very many transactions; kept modest.
        self.limiter = limiter or BudgetLimiter(read_per_second=5, write_per_second=20)
        shared = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        shared.headers["Accept"] = "application/json"
        self.host = f"https://{JURISDICTIONS[self.jurisdiction]['api']}"
        self.http = AsyncHttpClient(self.host, limiter=None, client=shared, venue=VENUE, attempts=2)
        self._own_catalog = catalog is None
        if catalog is None:
            from ..betfair import Betfair

            catalog = Betfair(session=session)
        self.catalog = catalog
        """The read adapter, on the same session: fee schedules."""
        self._currency: str | None = None
        self._markets: dict[str, str] = {}
        """Bet id -> Betfair market id, for the cancels and edits Betfair wants a market for."""

    # -- plumbing -------------------------------------------------------------

    @property
    def place_per_request(self) -> int:
        return PLACE_PER_REQUEST.get(self.jurisdiction, DEFAULT_PLACE_PER_REQUEST)

    async def _call(
        self, operation: str, body: dict[str, Any], *, api: str = BETTING_PATH, kind: str = "read",
        priority: Priority = Priority.NORMAL,
    ) -> Any:
        """One operation with the session's headers, paced; a refused session
        logs in again once. Betfair's own error code names the error."""
        await self.limiter.acquire(cost=1, kind=kind, priority=priority)  # type: ignore[arg-type]
        for attempt in range(2):
            token = await asyncio.to_thread(self.session.token)
            headers = {"X-Application": self.session.app_key, "X-Authentication": token,
                       "Content-Type": "application/json"}
            try:
                return await self.http.post(f"{api}/{operation}/", json=body, headers=headers)
            except ExchangeError as exc:
                code = api_error_code(exc.body)
                if code in SESSION_ERRORS and attempt == 0:
                    self.session.invalidate()
                    continue
                if code is None:
                    raise
                if code in AUTH_ERRORS:
                    raise AuthenticationError(f"{VENUE}: {operation} refused: {code}", body=exc.body,
                                              status=exc.status) from None
                if code in RATE_ERRORS:
                    raise RateLimitExceeded(f"{VENUE}: {operation} refused: {code}") from None
                if code in ("PERMISSION_DENIED", "INSUFFICIENT_FUNDS", "DUPLICATE_TRANSACTION") or code in HALTED:
                    raise error_of(code, what=operation, body=exc.body) from None
                raise BadRequest(f"{VENUE}: {operation} refused: {code}", body=exc.body, status=exc.status) from None
        raise AssertionError("unreachable")  # pragma: no cover

    async def currency(self) -> str:
        """The account's currency, read once: every stake and balance is in it."""
        if self._currency is None:
            details = await self._call("getAccountDetails", {}, api=ACCOUNT_PATH) or {}
            self._currency = str(details.get("currencyCode") or "")
        return self._currency

    async def _market_for(self, order_id: str, market_id: str | None) -> str:
        """The Betfair market a bet is on: from the caller, from this adapter's
        own placements, or read back from the account's orders."""
        if market_id:
            return _market_of(market_id)
        known = self._markets.get(str(order_id))
        if known:
            return known
        order = await self.fetch_order(order_id)
        return _market_of(order.market_id)

    # -- orders ---------------------------------------------------------------

    async def create_order(self, request: OrderRequest) -> Order:
        """Place one bet. Betfair answers once it has been matched or put on the
        book (after the in-play delay on an in-play market); an immediate
        order's unmatched rest is cancelled before this returns."""
        result = await self.create_orders([request])
        if isinstance(result[0], Exception):
            raise result[0]
        return result[0]

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        """Many bets, one `placeOrders` per Betfair market (200 a request, 50
        on the Italian exchange), one result each in the order asked for. A
        refused bet comes back as its exception. Betfair places a request's
        bets all or none, so one refused bet fails the others on its market."""
        out: list[Order | Exception | None] = [None] * len(requests)
        groups: dict[str, list[tuple[int, dict[str, Any]]]] = {}
        currency: str | None = None
        for i, request in enumerate(requests):
            try:
                instruction = build_instruction(request)
                limit = instruction["limitOrder"]
                if currency is None:
                    currency = await self.currency()
                check_minimum(D(limit["size"]), D(limit["price"]), currency=currency, jurisdiction=self.jurisdiction)
                groups.setdefault(_market_of(request.market_id), []).append((i, instruction))
            except (InvalidOrder, BadRequest) as exc:
                out[i] = exc
        for market, entries in groups.items():
            for start in range(0, len(entries), self.place_per_request):
                chunk = entries[start:start + self.place_per_request]
                for i, result in zip([i for i, _ in chunk], await self._place(market, chunk, requests)):
                    out[i] = result
        return await self._finish_immediate(requests, out)  # type: ignore[arg-type]

    async def _place(
        self, market: str, chunk: list[tuple[int, dict[str, Any]]], requests: list[OrderRequest],
    ) -> list[Order | Exception]:
        body: dict[str, Any] = {"marketId": market, "instructions": [instruction for _, instruction in chunk]}
        first = requests[chunk[0][0]]
        if len(chunk) == 1 and first.client_order_id and CUSTOMER_REF.match(first.client_order_id):
            # One order's id also keys Betfair's 60-second de-duplication.
            body["customerRef"] = first.client_order_id
        version = first.params.get("market_version")
        if version is not None:
            # Lapse the bets if the market has changed materially since then
            # (a runner removed, turned in play).
            body["marketVersion"] = {"version": int(version)}
        try:
            execution = await self._call("placeOrders", body, kind="write") or {}
        except ExchangeError as exc:
            return [exc] * len(chunk)
        reports = list(execution.get("instructionReports") or [])
        currency = self._currency
        out: list[Order | Exception] = []
        for n, (i, instruction) in enumerate(chunk):
            report = reports[n] if n < len(reports) and isinstance(reports[n], dict) else {}
            request = requests[i]
            status = report.get("status") or execution.get("status")
            if status == "TIMEOUT":
                out.append(await self._after_timeout(request))
            elif status == "SUCCESS" and report.get("betId"):
                order = order_of_report(report, market_id=market, request=request,
                                        account=request.account or self.account, currency=currency)
                self._markets[order.id] = market
                out.append(order)
            else:
                out.append(_failure(report, execution, what="placeOrders"))
        return out

    async def _after_timeout(self, request: OrderRequest) -> Order | Exception:
        """Betfair did not finish the bet in time: it may yet be placed, or
        lapse within fifteen seconds. Look for it by its client order id."""
        if request.client_order_id:
            found = await self._current({"customerOrderRefs": [request.client_order_id], "orderProjection": "ALL"})
            if found:
                return found[0]
        return RequestTimeout(
            f"{VENUE}: Betfair timed out placing the bet; its state is unknown for up to 15 seconds -- read the "
            f"open orders" + (f" for client order id {request.client_order_id}" if request.client_order_id else "")
        )

    async def _finish_immediate(
        self, requests: list[OrderRequest], placed: list[Order | Exception],
    ) -> list[Order | Exception]:
        """Cancel the unmatched rest of every market or IOC order, so nothing
        of them is left resting. Betfair answered after any in-play delay,
        so the rest is already on the book."""
        to_cancel = [i for i, (request, order) in enumerate(zip(requests, placed))
                     if immediate(request) and isinstance(order, Order) and order.status == OrderStatus.OPEN]
        if not to_cancel:
            return placed
        out = list(placed)
        cancelled = await self.cancel_orders([placed[i].id for i in to_cancel])  # type: ignore[union-attr]
        for i, result in zip(to_cancel, cancelled):
            if isinstance(result, Order):
                request = requests[i]
                out[i] = result.model_copy(update={
                    "client_order_id": request.client_order_id, "type": request.type,
                    "time_in_force": request.time_in_force, "book": request.book, "trader": request.trader,
                    "tags": request.tags,
                })
        return out

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        """Cancel one bet's unmatched part. One that can no longer be cancelled
        (matched, lapsed, already cancelled) is read back as it stands."""
        [result] = await self.cancel_orders([order_id], market_id=market_id)
        if isinstance(result, Exception):
            raise result
        return result

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        """Cancel bets, sixty a request per Betfair market, each read back after
        so the answer carries what matched first."""
        by_market: dict[str, list[str]] = {}
        failed: dict[str, Exception] = {}
        for order_id in dict.fromkeys(str(o) for o in order_ids):
            try:
                by_market.setdefault(await self._market_for(order_id, market_id), []).append(order_id)
            except OrderNotFound as exc:
                failed[order_id] = exc
        for market, bets in by_market.items():
            for start in range(0, len(bets), CANCEL_PER_REQUEST):
                chunk = bets[start:start + CANCEL_PER_REQUEST]
                body = {"marketId": market, "instructions": [{"betId": bet} for bet in chunk]}
                execution = await self._call("cancelOrders", body, kind="write", priority=Priority.HIGH) or {}
                for report in execution.get("instructionReports") or []:
                    bet = str((report.get("instruction") or {}).get("betId") or "")
                    code = report.get("errorCode")
                    if report.get("status") != "SUCCESS" and code not in (None, "BET_TAKEN_OR_LAPSED"):
                        failed[bet] = _failure(report, execution, what="cancelOrders")
        wanted = [str(o) for o in order_ids if str(o) not in failed]
        orders = {o.id: o for o in await self._orders_by_id(wanted)}
        out: list[Order | Exception] = []
        for order_id in (str(o) for o in order_ids):
            if order_id in failed:
                out.append(failed[order_id])
            elif order_id in orders:
                out.append(orders[order_id])
            else:
                out.append(OrderNotFound(f"{VENUE}: no bet {order_id}"))
        return out

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int | None:
        """Every unmatched bet on a runner, a Betfair market (`betfair:1.234`)
        or the whole account. Returns how many Betfair reports cancelling."""
        if market_id and "-" in ids.native(VENUE, market_id):
            bets = [o.id for o in await self.fetch_open_orders(market_id=market_id)]
            if not bets:
                return 0
            results = await self.cancel_orders(bets, market_id=market_id)
            return sum(1 for r in results if isinstance(r, Order))
        body = {"marketId": _market_of(market_id)} if market_id else {}
        execution = await self._call("cancelOrders", body, kind="write", priority=Priority.HIGH) or {}
        if execution.get("status") not in (None, "SUCCESS", "PROCESSED_WITH_ERRORS"):
            raise error_of(execution.get("errorCode"), what="cancelOrders", body=execution)
        reports = execution.get("instructionReports")
        if reports is None:
            return None
        return sum(1 for r in reports if isinstance(r, dict) and r.get("status") == "SUCCESS")

    async def edit_order(self, request: EditRequest, *, current: Order | None = None) -> Order:
        """A smaller quantity (a partial cancel, which keeps the bet's place in
        the queue) and a new price (`replaceOrders`: the unmatched part placed
        again at the new odds, as a new bet with a new id). `quantity` is the
        order's new total, filled contracts included; it can only go down.
        `update_persistence` changes what happens to it at the off."""
        if request.time_in_force is not None or request.expires_at is not None:
            raise InvalidOrder(f"{VENUE}: a bet's time in force cannot be edited; cancel and place again")
        current = current or await self.fetch_order(request.order_id)
        if current.status not in (OrderStatus.OPEN, OrderStatus.PENDING):
            raise InvalidOrder(f"{VENUE}: bet {request.order_id} has nothing unmatched to edit")
        market = _market_of(current.market_id)
        info = current.info
        odds, _ = _price_size(info)
        odds = odds or (ONE / current.price if current.price else ZERO)
        remaining_stake = D(info.get("sizeRemaining")) if info.get("sizeRemaining") is not None \
            else (current.remaining or ZERO) / odds
        order = current
        if request.quantity is not None:
            left = D(request.quantity) - current.filled
            if left <= 0:
                raise InvalidOrder(f"{VENUE}: the new quantity leaves nothing unmatched; cancel the bet instead")
            new_stake = (left / odds).quantize(PENNY, rounding=ROUND_DOWN)
            if new_stake > remaining_stake:
                raise InvalidOrder(f"{VENUE}: Betfair cannot raise a bet's stake; place another bet for the rest")
            if new_stake < remaining_stake:
                reduction = remaining_stake - new_stake
                body = {"marketId": market, "instructions": [{"betId": current.id, "sizeReduction": _number(reduction)}]}
                await self._edit("cancelOrders", body)
                order = (await self.fetch_order(current.id)).model_copy(update={"queue_priority_preserved": True})
        if request.price is not None:
            new_odds = odds_for(current.side, request.price)
            if new_odds != odds:
                body = {"marketId": market, "instructions": [{"betId": current.id, "newPrice": _number(new_odds)}]}
                execution = await self._edit("replaceOrders", body)
                report = (execution.get("instructionReports") or [{}])[0]
                placed = report.get("placeInstructionReport") or {}
                if placed.get("status") != "SUCCESS" or not placed.get("betId"):
                    raise _failure(placed or report, execution, what="replaceOrders")
                order = order_of_report(placed, market_id=market, account=self.account, currency=self._currency)
                self._markets[order.id] = market
                order = order.model_copy(update={
                    "queue_priority_preserved": False,
                    "info": {**order.info, "replaced": current.id},
                })
        return order.model_copy(update={"client_order_id": current.client_order_id})

    async def update_persistence(self, order_id: str, persistence: str, *, market_id: str | None = None) -> Order:
        """What happens to a bet's unmatched part when the market turns in play
        (`updateOrders`): `LAPSE`, `PERSIST` or `MARKET_ON_CLOSE`. Changes no
        exposure, and keeps the bet's place in the queue."""
        wanted = persistence_of({"persistence": persistence})
        market = await self._market_for(order_id, market_id)
        body = {"marketId": market, "instructions": [{"betId": str(order_id), "newPersistenceType": wanted}]}
        await self._edit("updateOrders", body)
        return await self.fetch_order(order_id)

    async def _edit(self, operation: str, body: dict[str, Any]) -> dict[str, Any]:
        execution = await self._call(operation, body, kind="write") or {}
        if execution.get("status") == "SUCCESS":
            return execution
        report = (execution.get("instructionReports") or [{}])[0]
        raise _failure(report, execution, what=operation)

    async def fetch_order(self, order_id: str) -> Order:
        """One bet by id: among the current orders, then, once it has left
        them, among the cancelled, lapsed, settled and voided bets."""
        found = await self._orders_by_id([str(order_id)])
        if found:
            return found[0]
        raise OrderNotFound(f"{VENUE}: no bet {order_id}")

    async def fetch_order_by_client_id(self, client_order_id: str) -> Order:
        """The current bet placed with this client order id (its `customerOrderRef`)."""
        found = await self._current({"customerOrderRefs": [client_order_id], "orderProjection": "ALL"})
        if not found:
            raise OrderNotFound(f"{VENUE}: no current bet with client order id {client_order_id}")
        return found[0]

    async def _orders_by_id(self, order_ids: list[str]) -> list[Order]:
        out: list[Order] = []
        # No bet ids would ask for every bet on the account.
        order_ids = [o for o in order_ids if o]
        for start in range(0, len(order_ids), MAX_IDS):
            chunk = order_ids[start:start + MAX_IDS]
            current = await self._current({"betIds": chunk, "orderProjection": "ALL"})
            out += current
            missing = [o for o in chunk if o not in {order.id for order in current}]
            for bet_status in CLEARED_STATUSES:
                if not missing:
                    break
                rows, _ = await self._cleared({"betStatus": bet_status, "betIds": missing}, pages=1)
                cleared = [order_of_cleared({**row, "betStatus": bet_status}, account=self.account,
                                            currency=self._currency) for row in rows]
                out += cleared
                missing = [o for o in missing if o not in {order.id for order in cleared}]
        for order in out:
            self._markets.setdefault(order.id, _market_of(order.market_id))
        return out

    async def _current_rows(
        self, params: dict[str, Any], *, offset: int = 0, pages: int = MAX_PAGES,
    ) -> tuple[list[dict[str, Any]], int | None]:
        """Current order rows from `offset`, page by page; and the next offset
        when more are left."""
        rows: list[dict[str, Any]] = []
        for _ in range(pages):
            raw = await self._call("listCurrentOrders", {**params, "fromRecord": offset, "recordCount": PAGE_SIZE}) or {}
            page = [r for r in raw.get("currentOrders") or [] if isinstance(r, dict)]
            rows += page
            offset += len(page)
            if not page or not raw.get("moreAvailable"):
                return rows, None
        return rows, offset

    async def _current(self, params: dict[str, Any]) -> list[Order]:
        rows, _ = await self._current_rows(params)
        return [order_of_current(row, account=self.account, currency=self._currency) for row in rows]

    async def _cleared(
        self, params: dict[str, Any], *, offset: int = 0, pages: int = MAX_PAGES,
    ) -> tuple[list[dict[str, Any]], int | None]:
        rows: list[dict[str, Any]] = []
        for _ in range(pages):
            raw = await self._call("listClearedOrders", {**params, "fromRecord": offset, "recordCount": PAGE_SIZE}) or {}
            page = [r for r in raw.get("clearedOrders") or [] if isinstance(r, dict)]
            rows += page
            offset += len(page)
            if not page or not raw.get("moreAvailable"):
                return rows, None
        return rows, offset

    def _scope(self, market_id: str | None) -> tuple[dict[str, Any], str | None]:
        """Betfair's market filter for a runner or a Betfair market, and the
        runner to keep when it is a runner."""
        if not market_id:
            return {}, None
        native = ids.native(VENUE, market_id)
        if "-" in native:
            market, selection, handicap = parse_runner_id(native)
            return {"marketIds": [market]}, _runner_id(market, selection, handicap)
        return {"marketIds": [native]}, None

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        """Every bet with an unmatched part, or every one on a runner or a Betfair market."""
        scope, runner = self._scope(market_id)
        orders = await self._current({**scope, "orderProjection": "EXECUTABLE"})
        return [o for o in orders if runner is None or o.market_id == runner]

    async def fetch_orders(
        self, *, status: str | None = None, market_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Order]:
        """One page of bets on unsettled markets, in any state: `open`,
        `closed` (matched in full) or `canceled` (the rest cancelled, lapsed
        or killed). `since` keeps bets placed since then."""
        projection = {None: "ALL", "open": "EXECUTABLE", "closed": "EXECUTION_COMPLETE", "canceled": "EXECUTION_COMPLETE"}
        if status not in projection:
            raise BadRequest(f"{VENUE}: status is open, closed or canceled")
        scope, runner = self._scope(market_id)
        params: dict[str, Any] = {**scope, "orderProjection": projection[status], "orderBy": "BY_PLACE_TIME",
                                  "sortDir": "LATEST_TO_EARLIEST"}
        if since is not None:
            params["dateRange"] = {"from": iso(since)}
        offset = _offset(cursor)
        size = min(limit or PAGE_SIZE, PAGE_SIZE)
        raw = await self._call("listCurrentOrders", {**params, "fromRecord": offset, "recordCount": size}) or {}
        rows = [r for r in raw.get("currentOrders") or [] if isinstance(r, dict)]
        orders = [order_of_current(row, account=self.account, currency=self._currency) for row in rows]
        if status == "closed":
            orders = [o for o in orders if o.status == OrderStatus.CLOSED]
        elif status == "canceled":
            orders = [o for o in orders if o.status in (OrderStatus.CANCELED, OrderStatus.EXPIRED)]
        if runner is not None:
            orders = [o for o in orders if o.market_id == runner]
        following = offset + len(rows)
        return Page(orders, next_cursor=str(following) if rows and raw.get("moreAvailable") else None)

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        """What has matched on this account's bets in unsettled markets, one
        fill per bet at its average matched odds, newest match first. A bet
        appears once it has stopped matching (filled, or its rest cancelled
        or lapsed), so its fill does not change after it is read; one still
        matching is on `BetfairUserStream` and in `fetch_open_orders`."""
        scope, runner = self._scope(market_id)
        params: dict[str, Any] = {**scope, "orderProjection": "EXECUTION_COMPLETE", "orderBy": "BY_MATCH_TIME",
                                  "sortDir": "LATEST_TO_EARLIEST"}
        if order_id:
            params = {"betIds": [str(order_id)], "orderProjection": "ALL"}
        if since is not None:
            params["dateRange"] = {"from": iso(since)}
        rows, following = await self._current_rows(params, offset=_offset(cursor), pages=1)
        fills = [f for row in rows if str(row.get("status")) == "EXECUTION_COMPLETE" or order_id
                 for f in [fill_of_current(row, account=self.account, currency=self._currency)] if f is not None]
        if runner is not None:
            fills = [f for f in fills if f.market_id == runner]
        if since is not None:
            fills = [f for f in fills if f.timestamp >= since]
        fills.sort(key=lambda f: f.timestamp, reverse=True)
        return Page(fills[:limit] if limit else fills, next_cursor=str(following) if following is not None else None)

    # -- account --------------------------------------------------------------

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        """What is free to bet (`availableToBetBalance`), what open bets hold
        (`exposure`, `locked`), and the two together (`total`)."""
        raw = await self._call("getAccountFunds", {}, api=ACCOUNT_PATH) or {}
        free = D(raw.get("availableToBetBalance"))
        locked = abs(D(raw.get("exposure")))
        return Balance(
            venue=VENUE, account=account or self.account, currency=await self.currency(),
            total=free + locked, available=free, locked=locked, buying_power=free,
            timestamp=int(time.time() * 1000), info=raw,
        )

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        """The account's matched bets on unsettled markets netted per runner on
        the YES leg. `market_id` is a runner, `event_id` a Betfair market."""
        scope, runner = self._scope(market_id or event_id)
        rows, _ = await self._current_rows({**scope, "orderProjection": "ALL"})
        positions = positions_of_orders(rows, account=self.account)
        return [p for p in positions if p.market_id == runner] if market_id and runner else positions

    async def fetch_profit_and_loss(
        self, market_ids: list[str], *, net_of_commission: bool = False,
    ) -> list[dict[str, Any]]:
        """Betfair's own profit and loss on open markets, if each runner wins
        (`ifWin`) or loses (`ifLose`, on markets with several winners), as
        `listMarketProfitAndLoss` returns it. Runner ids or Betfair market ids."""
        markets = list(dict.fromkeys(_market_of(m) for m in market_ids))
        return list(await self._call("listMarketProfitAndLoss", {
            "marketIds": markets, "includeSettledBets": False, "includeBspBets": True,
            "netOfCommission": net_of_commission,
        }) or [])

    async def fetch_settlements(
        self, *, market_id: str | None = None, since: int | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page[Settlement]:
        """One page of settled bets, one settlement each, as Betfair orders them
        by settled date. `since` keeps bets settled since then."""
        scope, runner = self._scope(market_id)
        params: dict[str, Any] = {"betStatus": "SETTLED", **scope}
        if since is not None:
            params["settledDateRange"] = {"from": iso(since)}
        rows, following = await self._cleared(params, offset=_offset(cursor), pages=1)
        # Bet-level rows name their runner; a roll-up Betfair hoists above it does not.
        settlements = [settlement_of(row, account=self.account) for row in rows if row.get("selectionId") is not None]
        if runner is not None:
            settlements = [s for s in settlements if s.market_id == runner]
        return Page(settlements[:limit] if limit else settlements,
                    next_cursor=str(following) if following is not None else None)

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, quantity: Decimal) -> FeeEstimate:
        """The commission on `quantity` contracts at the YES `price` if the bet
        wins: the market's base rate on its profit. A `sell` lays the runner,
        which wins `price` a contract when the runner loses. Makers pay the
        same. Betfair charges on net winnings per market, so this is an
        upper bound for one bet among others."""
        schedule = await asyncio.to_thread(self.catalog.fetch_fee_schedule, market_id)
        paid = D(price) if side == Side.BUY else ONE - D(price)
        fee = schedule.estimate(float(paid), float(quantity), taker=True)
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, ids.native(VENUE, market_id)), side=side,
            price=D(price), quantity=D(quantity),
            taker_fee=D(fee) if fee is not None else None, maker_fee=D(fee) if fee is not None else None,
            currency=await self.currency(),
            info={"schedule": schedule.model_dump(), "market": _market_of(market_id),
                  "charged": "at settlement, on net winnings in the market, before the account's discount"},
        )

    async def close(self) -> None:
        await self.http.close()
        if self._own_catalog:
            self.catalog.close()
        else:
            self.session.close()


def _offset(cursor: str | None) -> int:
    if not cursor:
        return 0
    if not cursor.isdigit():
        raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
    return int(cursor)


__all__ = [
    "BetfairTrading", "build_instruction", "order_of_current", "order_of_report", "order_of_cleared",
    "fill_of_current", "positions_of_orders", "settlement_of", "odds_for", "stake_for", "check_minimum",
]
