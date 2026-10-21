"""SX Bet order entry, and the venue's order and fill records on the YES leg.

Built to SX Bet's documentation and OpenAPI (OBv3, the exchange's third order
book): order hashing and signatures are checked against an independent
EIP-712 encoder.

**Two credentials.** Every request carries an SX Bet API key
(`x-sx-api-key`, self-service at sx.bet under Account), and the key alone
reads the account's orders, fills, positions and balance and makes every
cancel and heartbeat; the venue scopes each to the address that owns it. Only
placing needs more: every new order is an EIP-712 message signed by the
wallet itself, so the private key is optional until then. The wallet is the order's maker;
the funds sit in a *proxy wallet* the venue derives from it, which must be
deployed (`deploy_proxy()`, once) and funded (sx.bet's deposit) before the
first order.

**An order bets one outcome.** `buy` bets outcome one (this library's YES) at
the price given; `sell` bets outcome two at one minus it. The venue counts the
stake in USDC and the price as the implied probability paid, so an order for
`n` contracts stakes `n * price` (or `n * (1 - price)`), rounded down to the
cent's millionth; prices are snapped onto the venue's ladder (0.125% today)
on the cheaper side. The smallest stake is 1 USDC.

**Limits rest; market orders take.** A limit is `GTC` (post-only on
request), or expires at `expires_at` with `gtd`. A market order, or an IOC
limit, is the venue's `IOC` at the price given as the worst accepted; `fok` is
the venue's `FOK`. Every order is placed with the venue's synchronous mode, so
what matched on arrival comes back in the answer. A resting order the venue
does not see filled before kickoff is swept when the game starts.

**Fees** are charged at settlement on winnings: 1% of a winning taker bet's
profit, nothing for a maker (the venue can set other rates per account).

**Positions** net per market on the YES leg: what the account wins if outcome
one comes in, less what it wins if outcome two does.

The *heartbeat* (`heartbeat(seconds)`) is a dead man's switch: unless renewed
in time, the venue cancels every open order on the account.

**Parlays** (the quoter side of docs/rfq-combo.md). A bettor's parlay request
on sx.bet makes a real market (the parent hash) that takes makers' orders for
60 seconds; the request is published once on `parlay_markets:global` and
nowhere else. Each one is an incoming `Rfq` (`mine=False`) on that market,
with its legs as a `Combo`. A quote is ordinary signed orders on the parlay
market: the bid on outcome one (the parlay wins), the ask on outcome two,
both resting until the parlay ends; the bettor takes one with FOK or IOC, so
there is no last look and no taker side over the API.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import time
import uuid
from decimal import ROUND_DOWN, Decimal
from typing import Any, Callable

from .. import ids
from ..base import AsyncHttpClient, Capability
from ..errors import BadRequest, ExchangeError, MarketNotFound, NotSupported
from ..sx_bet import iso_ms
from ..types import Page
from . import sx_bet_signing as sig
from .base import TradingExchange
from .credentials import SxBetCredentials
from .errors import CredentialsMissing, InsufficientFunds, InvalidOrder, OrderNotFound, OrderRejected, RfqNotFound
from .limiter import BudgetLimiter, Priority
from .polymarket_signing import WalletSigner
from .types import (
    Account, Balance, FeeEstimate, Fill, Liquidity, Order, OrderRequest, OrderStatus, OrderType, Position,
    PositionSide, SettlementState, Side, TimeInForce, VENUE_ORDER_TYPES,
    Combo, ComboLeg, MakerQuote, QuoteStatus, Rfq, RfqCloseReason, RfqQuote, RfqStatus,
)

log = logging.getLogger("synpath.trading.sx_bet")

VENUE = "sx_bet"
API_URL = "https://api.sx.bet"
TESTNET_API_URL = "https://api.toronto.sx.bet"
USDC = Decimal(1_000_000)
ODDS = Decimal(10) ** 20

DEFAULT_EXPIRY_S = 30 * 86400
"""How long a GTC order is signed to live: the venue needs an expiry, and a
game's orders are swept at kickoff long before this."""

WAIT_MS = 10_000
"""How long `create_order` lets the venue wait for the matching result."""

OUTCOME_STATUS = {
    "RESTED": OrderStatus.OPEN,
    "PARTIAL_FILL_RESTED": OrderStatus.OPEN,
    "FULLY_FILLED": OrderStatus.CLOSED,
    "PARTIAL_FILL_DONE": OrderStatus.CANCELED,
    "CANCELLED": OrderStatus.CANCELED,
    "TIMEOUT": OrderStatus.PENDING,
}
"""What the venue's synchronous answer says became of an order on arrival."""

INACTIVE_STATUS = {
    "FILLED": OrderStatus.CLOSED,
    "EXPIRED": OrderStatus.EXPIRED,
}
"""An inactive order's state by its `inactiveReason`; every other reason
(cancelled by the account, the venue, a heartbeat, kickoff) is a cancel."""


def D(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def yes_price(odds: Any, outcome_one: bool) -> Decimal:
    """An outcome's 1e20-scaled odds as the YES price."""
    price = D(odds) / ODDS
    return price if outcome_one else Decimal("1") - price


def side_of(outcome_one: bool) -> Side:
    return Side.BUY if outcome_one else Side.SELL


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

def order_of_row(row: dict[str, Any], *, account: Account | None = None) -> Order:
    """One order as the venue lists it (`GET /orders-v3`, the orders channel).
    Stakes become contracts at the order's own odds."""
    outcome_one = bool(row.get("isBettingOutcomeOne"))
    odds = int(D(row.get("percentageOdds")))
    total, remaining = int(D(row.get("totalBetSize"))), int(D(row.get("remainingSize")))
    quantity = sig.contracts_of(total, odds)
    left = sig.contracts_of(remaining, odds)
    native = str(row.get("status") or "")
    reason = str(row.get("inactiveReason") or "")
    if native == "ACTIVE":
        status = OrderStatus.OPEN
    elif native == "PENDING":
        status = OrderStatus.PENDING
    else:
        status = INACTIVE_STATUS.get(reason, OrderStatus.CANCELED)
    filled = max(Decimal("0"), quantity - left) if status != OrderStatus.CLOSED else quantity
    return Order(
        id=str(row.get("id") or row.get("orderId") or ""),
        client_order_id=row.get("clientOrderId") or None,
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, str(row.get("marketHash") or "").lower()),
        side=side_of(outcome_one),
        type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        status=status,
        price=yes_price(odds, outcome_one),
        quantity=quantity,
        filled=filled,
        remaining=left if status in (OrderStatus.OPEN, OrderStatus.PENDING) else Decimal("0"),
        expires_at=iso_ms(row.get("expiry")),
        created_at=iso_ms(row.get("createdAt")),
        updated_at=iso_ms(row.get("updatedAt")),
        info={k: v for k, v in row.items() if k != "orderSignature"},
    )


def fill_of_row(row: dict[str, Any], *, account: Account | None = None) -> Fill | None:
    """One fill (`GET /fills-v3`, the fills channel): the contracts are its
    return, one dollar each; the price its odds, on the YES leg. A parlay's
    fill is on the parlay market itself (its `marketHash` is the parent), so
    it books on the combo's own id like any other market's."""
    if not row.get("marketHash"):
        return None
    outcome_one = bool(row.get("isBettingOutcomeOne"))
    state = str(row.get("status") or "")
    return Fill(
        id=str(row.get("id") or row.get("matchId") or ""),
        order_id=str(row.get("orderId") or ""),
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, str(row.get("marketHash") or "").lower()),
        side=side_of(outcome_one),
        price=yes_price(row.get("fillOdds"), outcome_one),
        quantity=D(row.get("returnAmount")) / USDC,
        fee=None,
        fee_currency="USDC",
        liquidity=Liquidity.MAKER if row.get("isMaker") else Liquidity.TAKER if row.get("isMaker") is False else Liquidity.UNKNOWN,
        settlement=SettlementState.FAILED if state == "FAILED" else
        SettlementState.MATCHED if state == "MATCHED" else SettlementState.CONFIRMED,
        timestamp=iso_ms(row.get("createdAt")) or 0,
        info=row,
    )


def position_of(row: dict[str, Any], *, account: Account | None = None) -> Position:
    """One market's bets netted on the YES leg: what the account makes if
    outcome one comes in, less what it makes if outcome two does."""
    best, worst = D(row.get("maxWin")) / USDC, D(row.get("maxLoss")) / USDC
    spread = best - worst
    net = spread if row.get("isOutcomeOneMaxWin") else -spread
    market = row.get("market") or {}
    settled = market.get("outcome") is not None or market.get("reportedDate") is not None
    return Position(
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, str(row.get("marketHash") or "").lower()),
        side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
        contracts=abs(net),
        margin=D(row.get("totalStake")) / USDC,
        realized_pnl=D(row["pnl"]) / USDC if row.get("pnl") not in (None, "") else None,
        resolved=settled,
        final=settled,
        timestamp=iso_ms(row.get("updatedAt")),
        info=row,
    )


def reason_of(body: Any, fallback: str) -> str:
    if isinstance(body, dict):
        message = body.get("message")
        if isinstance(message, list):
            return "; ".join(str(m) for m in message)
        if message:
            return str(message)
    return fallback


def error_of(exc: ExchangeError) -> ExchangeError:
    """The venue's refusal of an order as a typed error, by its code."""
    text = reason_of(exc.body, str(exc))
    upper = text.upper()
    if "PROXY_NOT_DEPLOYED" in upper or "INVALID_PROXY" in upper:
        return CredentialsMissing(f"{VENUE}: {text} -- deploy the proxy wallet once with deploy_proxy(), then fund it on sx.bet")
    if "BALANCE" in upper or "INSUFFICIENT" in upper:
        return InsufficientFunds(f"{VENUE}: {text}", body=exc.body)
    if any(word in upper for word in ("ODDS", "SIZE", "EXPIRY", "LADDER", "INVALID")):
        return InvalidOrder(f"{VENUE}: {text}")
    return OrderRejected(f"{VENUE}: {text}", reason=text.lower() or None,
                         info=exc.body if isinstance(exc.body, dict) else {}, body=exc.body)


# ---------------------------------------------------------------------------
# Parlays (docs/rfq-combo.md, P8)
# ---------------------------------------------------------------------------

PARLAY_LIFETIME_S = 60
"""A parlay market takes orders for 60 seconds after the bettor's request."""

PARLAY_EXPIRY_MARGIN_S = 3
"""How much earlier than the estimated end of the parlay a quote's orders are
signed to expire. The request carries no expiry, only the 60 s rule, and it
reaches us after it was made; an order signed past the parlay's end is
refused (`PARLAY_ORDER_EXPIRY_BEYOND_PARLAY_EXPIRY`, which names the exact
end, and the quote is re-signed to it once)."""

PARLAY_KEEP_S = 600
"""How long a parlay request is remembered after it closes, so a quote on it
can still be read back by its RFQ."""

VOID_LEG_RULE = (
    "SX Bet's documents disagree on a VOID leg: the developer docs (developers/parlays) say one VOID leg voids the "
    "parlay and every stake is returned; the user guide (user-guides/trading/parlays) says a VOID or push leg loses "
    "a parlay WIN bet, and a parlay LOSE bet needs no VOID leg to win. Synpath assumes the worse reading for the side "
    "held (parlay_result's `worst`) and books nothing from a leg itself: the venue's settlement is the record."
)

LEG_RESULTS = frozenset({"won", "lost", "void", "open"})


def parlay_result(legs: list[str], *, outcome_one: bool = True) -> dict[str, str]:
    """What a parlay bet is worth from its legs' results, each `won`, `lost`,
    `void` (or push) or `open`, from the parlay's side (`won` = the leg went
    the way the parlay needs). `outcome_one` is the parlay WIN side (the
    bettor's request, a maker's bid); outcome two bets against it.

    Returns each document's reading (`developer_docs`, `user_guide`) and the
    `worst` of the two for the side held, as one of `won`, `lost`, `refund`
    or `open`. See `VOID_LEG_RULE`."""
    unknown = set(legs) - LEG_RESULTS
    if unknown:
        raise ValueError(f"leg results are won, lost, void or open, not {sorted(unknown)}")

    void, lost, still_open = "void" in legs, "lost" in legs, "open" in legs
    if outcome_one:     # parlay WIN
        dev = "refund" if void else "lost" if lost else "open" if still_open else "won"
        guide = "lost" if void or lost else "open" if still_open else "won"
    else:               # parlay LOSE
        dev = "refund" if void else "won" if lost else "open" if still_open else "lost"
        guide = "lost" if void else "won" if lost else "open" if still_open else "lost"
    readings = (dev, guide)
    if "lost" in readings:
        worst = "lost"
    elif "open" in readings:
        worst = "open"
    else:
        worst = "refund" if "refund" in readings else "won"
    return {"developer_docs": dev, "user_guide": guide, "worst": worst}


def combo_of_parlay(row: dict[str, Any], *, created_at: int | None = None) -> Combo:
    """The parlay's legs as a `Combo` on the parlay market's own id. A leg's
    `bettingOutcomeOne` is the bettor's side of it: `yes` is outcome one."""
    parent = str(row.get("marketHash") or "").lower()
    legs = [
        ComboLeg(
            market_id=ids.qualify(VENUE, str(leg.get("marketHash") or "").lower()),
            outcome="yes" if leg.get("bettingOutcomeOne") else "no",
            info={"bettingOutcomeOne": bool(leg.get("bettingOutcomeOne"))},
        )
        for leg in row.get("legs") or [] if isinstance(leg, dict) and leg.get("marketHash")
    ]
    return Combo(
        market_id=ids.qualify(VENUE, parent), venue=VENUE, legs=legs, status="open", created_at=created_at,
        info={"parlay": True, "void_leg_rule": VOID_LEG_RULE, "chainVersion": row.get("chainVersion")},
    )


def rfq_of_parlay(row: dict[str, Any], *, received_at: int) -> Rfq:
    """A parlay request (`parlay_markets:global`) as an incoming RFQ on the
    parlay market. Two-sided: the request does not say which side of the
    parlay the bettor will take. `amount` is `requestSize`, the stake asked
    for (USDC, six decimals), which does not cap what a maker may offer.
    `expires_at` is an estimate (`info["expiry_exact"]` False): the request
    names no end, only the 60 s rule, counted here from its arrival."""
    parent = str(row.get("marketHash") or "").lower()
    if not parent:
        raise ValueError("a parlay request names its market")
    size = row.get("requestSize")
    return Rfq(
        id=parent, venue=VENUE, combo_id=ids.qualify(VENUE, parent),
        combo=combo_of_parlay(row, created_at=received_at),
        amount=D(size) / USDC if size not in (None, "", "0", 0) else None,
        mine=False, created_at=received_at, expires_at=received_at + PARLAY_LIFETIME_S * 1000,
        info={**{k: v for k, v in row.items() if k != "legs"}, "expiry_exact": False},
    )


QUOTE_SEPARATOR = "~"


def quote_id_of(bid_order: str | None, ask_order: str | None) -> str:
    """A quote's id: its two orders' ids, bid first (`0xbid~0xask`, `0xbid~`,
    `~0xask`), so a quote is read and cancelled from its id alone."""
    return f"{(bid_order or '').lower()}{QUOTE_SEPARATOR}{(ask_order or '').lower()}"


def order_ids_of(quote_id: str) -> tuple[str | None, str | None]:
    if QUOTE_SEPARATOR not in quote_id:
        return quote_id.lower() or None, None
    bid, _, ask = quote_id.partition(QUOTE_SEPARATOR)
    return bid.lower() or None, ask.lower() or None


def quote_of_orders(bid: Order | None, ask: Order | None, *, rfq_id: str | None = None,
                    account: Account | None = None) -> RfqQuote:
    """Our quote from the orders that make it: the bid is a YES buy (outcome
    one, the parlay wins), the ask a YES sell (outcome two at one minus it).
    Open while either order rests; executed once neither rests and one
    filled; else expired (both expired) or canceled."""
    orders = [o for o in (bid, ask) if o is not None]
    if not orders:
        raise ValueError("a quote has at least one order")
    resting = any(o.status in (OrderStatus.OPEN, OrderStatus.PENDING) for o in orders)
    filled = [o for o in orders if o.filled > 0]
    if resting:
        status = QuoteStatus.OPEN
    elif filled:
        status = QuoteStatus.EXECUTED
    elif all(o.status == OrderStatus.EXPIRED for o in orders):
        status = QuoteStatus.EXPIRED
    else:
        status = QuoteStatus.CANCELED
    market_id = orders[0].market_id
    expiries = [o.expires_at for o in orders if o.expires_at]
    created = [o.created_at for o in orders if o.created_at]
    return RfqQuote.from_venue({
        "id": quote_id_of(bid.id if bid else None, ask.id if ask else None),
        "rfq_id": rfq_id or ids.native(VENUE, market_id),
        "venue": VENUE, "account": account, "market_id": market_id,
        "bid": bid.price if bid else None, "ask": ask.price if ask else None,
        "bid_quantity": bid.quantity if bid else None, "ask_quantity": ask.quantity if ask else None,
        "status": status, "accepted_side": None,
        "maker_order_id": filled[0].id if filled else None,
        "expires_at": min(expiries) if expiries else None, "mine": True,
        "created_at": min(created) if created else None,
        "info": {
            "orders": {"bid": bid.id if bid else None, "ask": ask.id if ask else None},
            "filled": {"bid": str(bid.filled) if bid else None, "ask": str(ask.filled) if ask else None},
            "statuses": {"bid": bid.status.value if bid else None, "ask": ask.status.value if ask else None},
        },
    })


def quote_error_of(exc: ExchangeError) -> ExchangeError:
    """A refusal of a quote's orders, with the parlay codes read first."""
    body = exc.body if isinstance(exc.body, dict) else {}
    text = reason_of(exc.body, str(exc))
    upper = text.upper()
    if "PARLAY_ORDER_EXPIRY_BEYOND_PARLAY_EXPIRY" in upper:
        return InvalidOrder(f"{VENUE}: the quote's orders expire after the parlay does "
                            f"(parlay ends {body.get('parlayExpiry')}): {text}")
    if "PARLAY_LEG_INACTIVE" in upper:
        return OrderRejected(f"{VENUE}: leg {body.get('legMarketHash')} of parlay {body.get('parentMarketHash')} is "
                             f"{body.get('legStatus') or 'inactive'}; the parlay takes no orders", reason="parlay_leg_inactive",
                             info=body, body=exc.body)
    if upper.startswith("INVALID_MARKET") or upper == "INVALID_MARKET":
        return OrderRejected(f"{VENUE}: the parlay market is closed (its 60 s are over) or unknown: {text}",
                             reason="parlay_expired", info=body, body=exc.body)
    return error_of(exc)


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

def venue_time_in_force(request: OrderRequest) -> str:
    if request.type == OrderType.MARKET or request.time_in_force == TimeInForce.IOC:
        return "IOC"
    if request.time_in_force == TimeInForce.FOK:
        return "FOK"
    return "GTC"


def check_request(request: OrderRequest) -> None:
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"{VENUE}: {request.type.value} is held by the execution engine, not the venue; submit it through the engine"
        )
    if request.time_in_force == TimeInForce.DAY:
        raise InvalidOrder(f"{VENUE}: day orders are the engine's; gtc, gtd, ioc or fok")
    if request.price is None:
        raise InvalidOrder(f"{VENUE}: a price is required -- a market order is sent at the worst price you give")
    if request.post_only and venue_time_in_force(request) != "GTC":
        raise InvalidOrder(f"{VENUE}: a post-only order cannot also take at once")


def build_signed_order(
    request: OrderRequest, *, signer: WalletSigner, meta: dict[str, Any], salt: int | None = None,
    now: float | None = None,
) -> tuple[dict[str, Any], str, int, int]:
    """`(body item, order id, odds, stake)` for one request, before anything is sent."""
    check_request(request)
    outcome_one = request.side == Side.BUY
    limits = meta.get("limits") or {}
    asset = meta.get("activeAsset") or {}
    odds, stake = sig.bet(
        outcome_one, D(request.price), D(request.quantity),
        step_size=int(meta.get("oddsLadderStepSize") or 125),
        min_stake=int(limits.get("orderSizeMinimumBaseUnits") or 1_000_000),
    )
    clock = time.time() if now is None else now
    if request.time_in_force == TimeInForce.GTD and request.expires_at:
        expiry = int(request.expires_at // 1000)
        if expiry <= clock:
            raise InvalidOrder(f"{VENUE}: expires_at is in the past")
    else:
        expiry = int(clock) + DEFAULT_EXPIRY_S
    order = sig.build_order(
        market_hash=ids.native(VENUE, request.market_id).lower(), base_token=str(asset["baseToken"]),
        stake=stake, odds=odds, expiry=expiry, maker=signer.address, outcome_one=outcome_one,
        salt=salt if salt is not None else sig.new_salt(),
    )
    domain = meta["domain"]
    item = sig.wire_order(order, signature=sig.sign_order(signer, order, domain=domain),
                          time_in_force=venue_time_in_force(request))
    if request.post_only:
        item["postOnly"] = True
    if request.client_order_id:
        item["clientOrderId"] = request.client_order_id
    return item, sig.order_id(order, domain=domain), odds, stake


def order_after_placing(
    request: OrderRequest, result: dict[str, Any], *, order_id: str, odds: int, stake: int,
    account: Account,
) -> Order:
    """The order from the venue's synchronous answer: what matched on arrival
    and whether the rest is resting."""
    outcome = result.get("outcome") or {}
    state = str(outcome.get("state") or "TIMEOUT")
    status = OUTCOME_STATUS.get(state, OrderStatus.PENDING)
    quantity = sig.contracts_of(stake, odds)
    blended = int(D(outcome.get("blendedOdds")))
    fill_stake = int(D(outcome.get("fillAmount")))
    filled = sig.contracts_of(fill_stake, blended) if blended else Decimal("0")
    outcome_one = request.side == Side.BUY
    if status == OrderStatus.CLOSED:
        filled = max(filled, quantity) if filled else quantity
    remaining = Decimal("0")
    if status == OrderStatus.OPEN:
        remaining = sig.contracts_of(int(D(outcome.get("remainingAmount"))), odds) if outcome.get("remainingAmount") else quantity - filled
    elif status == OrderStatus.PENDING:
        remaining = quantity
    return Order(
        id=order_id,
        client_order_id=request.client_order_id,
        venue=VENUE,
        account=request.account or account,
        market_id=ids.qualify(VENUE, ids.native(VENUE, request.market_id).lower()),
        side=request.side,
        type=request.type,
        time_in_force=request.time_in_force,
        status=status,
        price=yes_price(odds, outcome_one),
        quantity=quantity,
        filled=filled,
        remaining=max(Decimal("0"), remaining),
        average_price=yes_price(blended, outcome_one) if blended else None,
        post_only=request.post_only,
        created_at=int(time.time() * 1000),
        book=request.book,
        trader=request.trader,
        tags=request.tags,
        info={"result": result, "stake": stake, "odds": odds},
    )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class SxBetTrading(TradingExchange):
    """SX Bet order entry.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.sx_bet import SxBetTrading

    async with SxBetTrading(require("sx_bet", load_credentials())) as sx:
        print(await sx.fetch_balance())
    ```
    """

    id = VENUE
    name = "SX Bet"
    has: dict[str, Capability] = {
        "create_order": True,
        # Up to ten a request, one maker; not atomic, one result each.
        "create_orders": True,
        "cancel_order": True,
        # Up to 100 a request.
        "cancel_orders": True,
        "cancel_all_orders": True,
        "edit_order": False,
        "fetch_order": True,
        "fetch_open_orders": True,
        # The venue lists only active orders; a closed one is read by id.
        "fetch_orders": False,
        "fetch_my_trades": True,
        "fetch_positions": True,
        "fetch_balance": True,
        "fetch_settlements": False,
        "fetch_queue_position": False,
        "fetch_fee_estimate": True,
        "rfq": False,
        # Parlays (docs/rfq-combo.md, P8). No API asks for a parlay: a bettor
        # requests one on sx.bet (docs.sx.bet/developers/parlays), so there is
        # no taker side here.
        "rfq_taker": False,
        # A parlay request from `parlay_markets:global` is an incoming RFQ; a
        # quote is one or two signed orders on the parlay market, expiring
        # with it. No last look: `confirm_quote` is not supported.
        "rfq_quoter": True,
        # A parlay market is a combo with its own id, read from the requests
        # seen; none is created through the API.
        "combos": True,
        "split_merge": False,
        "watch_orders": False,
        "watch_my_trades": False,
        "watch_positions": False,
        "watch_balance": False,
    }

    def __init__(
        self,
        credentials: SxBetCredentials,
        *,
        account_name: str = "default",
        url: str | None = None,
        catalog: Any = None,
        limiter: BudgetLimiter | None = None,
        timeout: float = 30.0,
        client: Any = None,
        parlay_stream: Any = None,
    ):
        import httpx

        self.credentials = credentials
        self.testnet = bool(credentials.testnet)
        self.signer = WalletSigner(credentials.private_key) if credentials.private_key else None
        """Signs new orders; `None` with the API key alone, which does everything else."""
        self.account = Account(venue=VENUE, name=account_name)
        self.limiter = limiter or BudgetLimiter(read_per_second=4, write_per_second=10)
        self.url = url or (TESTNET_API_URL if self.testnet else API_URL)
        shared = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        self.http = AsyncHttpClient(self.url, limiter=None, client=shared, venue=VENUE)
        self._own_catalog = catalog is None
        if catalog is None:
            from ..sx_bet import SxBet

            catalog = SxBet(api_key=credentials.api_key, api_url=self.url)
        self.catalog = catalog
        """The read adapter: fee schedules."""
        self._meta: dict[str, Any] | None = None
        self.parlay_stream = parlay_stream
        """The `parlay_markets:global` stream parlay requests come from; an
        `SxBetParlayStream` is made on the first `watch_parlays()` if none is given."""
        self._own_parlay_stream = parlay_stream is None
        self._parlay_task: asyncio.Task | None = None
        self.parlays: dict[str, Rfq] = {}
        """Parlay requests seen on the stream, by parlay market hash."""
        self._rfq_listeners: list[Callable[[Rfq], Any]] = []
        self._quotes: dict[str, str] = {}
        """Quote id -> parlay hash, for the quotes sent through this adapter."""
        self.clock: Callable[[], float] = time.time

    @property
    def address(self) -> str:
        """The wallet: the orders' maker, and the account the API key belongs to."""
        address = self.credentials.address
        if not address:
            raise CredentialsMissing(
                f"{VENUE}: the account's address is unknown with the API key alone; set SX_BET_ACCOUNT_ADDRESS "
                "to the wallet's 0x address (not its private key), or SX_BET_PRIVATE_KEY"
            )
        return address

    # -- plumbing -------------------------------------------------------------

    async def _call(
        self, method: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None,
        kind: str = "read", priority: Priority = Priority.NORMAL,
    ) -> Any:
        """One call with the API key, paced; the venue's `data` unwrapped."""
        await self.limiter.acquire(cost=1, kind=kind, priority=priority)  # type: ignore[arg-type]
        headers = {"x-sx-api-key": self.credentials.api_key}
        query = {k: v for k, v in (params or {}).items() if v is not None}
        try:
            payload = await self.http.request(method, path, params=query or None, json=body, headers=headers)
        except MarketNotFound as exc:
            if path.startswith("/orders"):
                raise OrderNotFound(str(exc), body=exc.body, status=exc.status) from None
            raise
        if isinstance(payload, dict) and payload.get("status") == "failure":
            raise ExchangeError(f"{VENUE}: {path} failed: {reason_of(payload, 'failure')}", body=payload)
        return payload.get("data") if isinstance(payload, dict) and "data" in payload else payload

    async def metadata(self, *, fresh: bool = False) -> dict[str, Any]:
        """The exchange's signing domain, token, odds ladder and limits. Read
        once and kept."""
        if self._meta is None or fresh:
            self._meta = await self._call("GET", "/metadata/obv3") or {}
        return self._meta

    async def proxy(self) -> dict[str, Any]:
        """The proxy wallet that holds this account's funds, and whether it is
        deployed yet."""
        return await self._call("GET", "/user/proxy") or {}

    async def deploy_proxy(self) -> dict[str, Any]:
        """Deploy the proxy wallet, once, before the first order. The venue
        does it on chain in a few seconds; `proxy()` says when it is done."""
        return await self._call("POST", "/user/deploy-proxy", kind="write") or {}

    async def heartbeat(self, timeout_s: int) -> dict[str, Any]:
        """Arm or renew the dead man's switch: unless this is called again
        within `timeout_s` seconds (at most 3600), the venue cancels every open
        order on the account. `0` disarms it."""
        if not 0 <= int(timeout_s) <= 3600:
            raise BadRequest(f"{VENUE}: the heartbeat timeout is 0 to 3600 seconds")
        return await self._call("POST", "/heartbeat/v3", body={"timeoutSeconds": int(timeout_s)},
                                kind="write", priority=Priority.HIGH) or {}

    # -- orders ---------------------------------------------------------------

    async def _post_orders(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """`POST /orders-v3`, synchronous; the venue's refusal raised as it came."""
        raw = await self._call("POST", "/orders-v3", body={
            "orders": items, "waitForOutcome": True, "maxWaitTime": WAIT_MS,
        }, kind="write") or {}
        return list(raw.get("orders") or [])

    async def _place(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        try:
            return await self._post_orders(items)
        except (BadRequest, OrderNotFound) as exc:
            raise error_of(exc) from None

    async def create_order(self, request: OrderRequest) -> Order:
        """Sign and place one order. The venue answers with what matched on
        arrival; an immediate order's rest is cancelled by the venue."""
        result = await self.create_orders([request])
        if isinstance(result[0], Exception):
            raise result[0]
        return result[0]

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        """Many orders, ten a request, one result each in the order asked for.
        A refused order comes back as its exception."""
        signer = self._order_signer()
        meta = await self.metadata()
        batch = int((meta.get("limits") or {}).get("maxCreateOrders") or 10)
        out: list[Order | Exception] = []
        for start in range(0, len(requests), batch):
            chunk = requests[start:start + batch]
            built: list[tuple[OrderRequest, dict[str, Any], str, int, int] | Exception] = []
            for request in chunk:
                try:
                    item, order_id, odds, stake = build_signed_order(request, signer=signer, meta=meta)
                    built.append((request, item, order_id, odds, stake))
                except InvalidOrder as exc:
                    built.append(exc)
            ready = [b for b in built if not isinstance(b, Exception)]
            results = await self._place([b[1] for b in ready]) if ready else []
            answers = iter(results)
            for entry in built:
                if isinstance(entry, Exception):
                    out.append(entry)
                    continue
                request, _, order_id, odds, stake = entry
                result = next(answers, {})
                if str(result.get("status") or "") != "SUBMITTED":
                    out.append(OrderRejected(f"{VENUE}: order {order_id} was not submitted: {result}",
                                             reason="failed", info=result))
                    continue
                out.append(order_after_placing(request, result, order_id=str(result.get("orderId") or order_id).lower(),
                                               odds=odds, stake=stake, account=self.account))
        return out

    def _order_signer(self) -> WalletSigner:
        """The wallet that signs new orders, checked before anything is sent."""
        if self.signer is None:
            raise CredentialsMissing(
                f"{VENUE}: SX Bet signs every order with the wallet's private key (EIP-712); set SX_BET_PRIVATE_KEY "
                "to place orders. Reading the account and cancelling work with SX_BET_API_KEY alone."
            )
        return self.signer

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        """Cancel one order and read it back."""
        result = (await self.cancel_orders([order_id]))[0]
        if isinstance(result, Exception):
            raise result
        return result

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        """Cancel up to 100 orders a request, then read each back. A cancel
        the venue could not confirm in time is read back as it stands."""
        meta = await self.metadata()
        batch = int((meta.get("limits") or {}).get("maxCancelOrders") or 100)
        refused: dict[str, str] = {}
        for start in range(0, len(order_ids), batch):
            raw = await self._call("DELETE", "/orders-v3", body={
                "orders": [{"orderId": o} for o in order_ids[start:start + batch]],
            }, kind="write", priority=Priority.HIGH) or {}
            for item in raw.get("notCancelled") or []:
                if isinstance(item, dict) and item.get("orderId"):
                    refused[str(item["orderId"]).lower()] = str(item.get("reason") or "refused")
        out: list[Order | Exception] = []
        for order_id in order_ids:
            try:
                order = await self.fetch_order(order_id)
            except OrderNotFound as exc:
                out.append(exc)
                continue
            if order_id.lower() in refused and order.status == OrderStatus.OPEN:
                out.append(OrderRejected(f"{VENUE}: cancel of {order_id} refused: {refused[order_id.lower()]}",
                                         reason="cancel_refused"))
                continue
            out.append(order)
        return out

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int:
        """Every open order on a market, or on the account. The venue submits
        the cancels and returns at once; returns how many it submitted."""
        path, params = ("/orders-v3/market", {"marketHash": ids.native(VENUE, market_id).lower()}) if market_id \
            else ("/orders-v3/all", None)
        submitted = 0
        # The venue cancels one batch a call and says `hasMore` until none are left.
        for _ in range(MAX_CANCEL_ALL_ROUNDS):
            raw = await self._call("DELETE", path, params=params, kind="write", priority=Priority.HIGH) or {}
            submitted += len(raw.get("cancelledSubmitted") or [])
            if not raw.get("hasMore"):
                break
        return submitted

    async def fetch_order(self, order_id: str) -> Order:
        """One order by id, in any state."""
        raw = await self._call("GET", f"/orders-v3/{order_id.lower()}")
        row = raw.get("order") if isinstance(raw, dict) and isinstance(raw.get("order"), dict) else raw
        if not isinstance(row, dict) or not row:
            raise OrderNotFound(f"{VENUE}: no order {order_id}")
        return order_of_row(row, account=self.account)

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        """Every resting order, or every one on a market, page by page."""
        orders: list[Order] = []
        key = None
        for _ in range(50):
            raw = await self._call("GET", "/orders-v3", params={
                "marketHash": ids.native(VENUE, market_id).lower() if market_id else None,
                "perPage": 100, "nextKey": key,
            }) or {}
            orders += [order_of_row(row, account=self.account) for row in raw.get("orders") or []]
            key = raw.get("nextKey")
            if not key:
                break
        return orders

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        """One page of this account's fills, newest first; a parlay's are on
        the parlay market's own id. The venue filters by order but not by
        market, so a market is filtered here."""
        raw = await self._call("GET", "/fills-v3", params={
            "orderId": order_id.lower() if order_id else None,
            "startDate": _iso(since) if since else None,
            "perPage": min(limit or 50, 100), "nextKey": cursor,
        }) or {}
        fills = [f for f in (fill_of_row(row, account=self.account) for row in raw.get("fills") or []) if f]
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id).lower())
            fills = [f for f in fills if f.market_id == wanted]
        return Page(fills, next_cursor=raw.get("nextKey") or None)

    # -- account --------------------------------------------------------------

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        """The proxy wallet's USDC: what is free to bet, and what matched bets
        hold in escrow (`locked`). Resting orders lock nothing."""
        raw = await self._call("GET", "/user/balance-v3") or {}
        rows = raw.get("balances") if isinstance(raw, dict) else raw
        meta = await self.metadata()
        token = str((meta.get("activeAsset") or {}).get("baseToken") or "").lower()
        row = next((r for r in rows or [] if str(r.get("tokenAddress") or "").lower() == token), None) \
            or next(iter(rows or []), {})
        available = (D(row.get("availableAmount")) + D(row.get("pendingAvailableAmount"))) / USDC
        locked = (D(row.get("escrowedAmount")) + D(row.get("pendingEscrowAmount"))) / USDC
        return Balance(
            venue=VENUE, account=account or self.account, currency="USDC",
            total=available + locked, available=available, locked=locked, buying_power=available,
            timestamp=int(time.time() * 1000), info=row,
        )

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        """Open bets (matched or locked), netted per market on the YES leg."""
        positions: list[Position] = []
        key = None
        for _ in range(50):
            raw = await self._call("GET", "/positions-v3", params={
                "status": "MATCHED,LOCKED", "eventId": event_id, "perPage": 100, "nextKey": key,
            }) or {}
            positions += [position_of(row, account=self.account) for row in raw.get("positions") or []]
            key = raw.get("nextKey")
            if not key:
                break
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id).lower())
            positions = [p for p in positions if p.market_id == wanted]
        return positions

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, quantity: Decimal) -> FeeEstimate:
        """The taker fee on `quantity` contracts at the YES `price` if the bet
        wins: a share of its profit. A `sell` bets outcome two at `1 - price`.
        Makers pay nothing."""
        schedule = await asyncio.to_thread(self.catalog.fetch_fee_schedule, market_id)
        paid = D(price) if side == Side.BUY else Decimal("1") - D(price)
        taker = schedule.estimate(float(paid), float(quantity), taker=True)
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, ids.native(VENUE, market_id).lower()), side=side,
            price=D(price), quantity=D(quantity),
            taker_fee=D(taker) if taker is not None else None, maker_fee=Decimal("0"),
            currency="USDC", info={"schedule": schedule.model_dump(), "charged": "at settlement, if the bet wins"},
        )

    # -- parlays: incoming requests (docs/rfq-combo.md, P8) -------------------
    #
    # A bettor's parlay request makes a real market (the parent hash) that
    # takes makers' orders for 60 s. The venue publishes each request once on
    # `parlay_markets:global` and has no REST listing of open requests, so
    # the requests this adapter knows are the ones its stream has seen.

    def _now_ms(self) -> int:
        return int(self.clock() * 1000)

    def _parlay_state(self, rfq: Rfq) -> Rfq:
        """The request as it stands now: closed (`expired`) once its 60 s are up."""
        if rfq.status == RfqStatus.OPEN and rfq.expires_at is not None and rfq.expires_at <= self._now_ms():
            return rfq.model_copy(update={"status": RfqStatus.CLOSED, "closed_reason": RfqCloseReason.EXPIRED})
        return rfq

    def _prune_parlays(self) -> None:
        cutoff = self._now_ms() - PARLAY_KEEP_S * 1000
        for parent in [h for h, r in self.parlays.items() if (r.expires_at or 0) < cutoff]:
            self.parlays.pop(parent, None)

    def on_parlay(self, request: dict[str, Any] | Rfq, *, received_at: int | None = None) -> Rfq | None:
        """Take one parlay request (the channel's row, or the `Rfq` the
        stream made of it). A request seen before is kept as first seen.
        Every listener of `subscribe_incoming_rfqs` hears a new one at once."""
        try:
            rfq = request if isinstance(request, Rfq) else rfq_of_parlay(
                request, received_at=received_at if received_at is not None else self._now_ms())
        except ValueError as exc:
            log.warning("synpath.trading.sx_bet: unreadable parlay request: %s", exc)
            return None
        self._prune_parlays()
        if rfq.id in self.parlays:
            return self.parlays[rfq.id]
        self.parlays[rfq.id] = rfq
        for listener in list(self._rfq_listeners):
            try:
                result = listener(rfq)
                if inspect.isawaitable(result):
                    asyncio.ensure_future(result)
            except Exception:
                log.exception("synpath.trading.sx_bet: an incoming-RFQ listener failed")
        return rfq

    def subscribe_incoming_rfqs(self, listener: Callable[[Rfq], Any]) -> Callable[[], None]:
        """Call `listener(rfq)` (sync or async) for every new parlay request
        as it arrives: the stream hook for a quoter that must answer inside
        60 s. The engine subscribes here at start (`engine.rfq.on_incoming`),
        so its quoting callbacks hear a request at once and SX Bet is not
        polled. Starts nothing: call `watch_parlays()` (or
        `start_incoming_rfqs()`, or let the first `fetch_incoming_rfqs` do
        it). Returns the unsubscribe."""
        self._rfq_listeners.append(listener)
        return lambda: self._rfq_listeners.remove(listener) if listener in self._rfq_listeners else None

    async def start_incoming_rfqs(self) -> None:
        """Start the parlay stream (the engine calls this when its incoming
        feed starts): `watch_parlays()`."""
        await self.watch_parlays()

    async def watch_parlays(self) -> Any:
        """Start listening to `parlay_markets:global`, once; returns the stream.
        Needs the API key, as every SX Bet channel does."""
        if self._parlay_task is not None and not self._parlay_task.done():
            return self.parlay_stream
        if self.parlay_stream is None:
            from ..ws.sx_bet import SxBetParlayStream

            self.parlay_stream = SxBetParlayStream(api_key=self.credentials.api_key, testnet=self.testnet,
                                                   api_url=self.url)
        await self.parlay_stream.watch_parlays()
        self._parlay_task = asyncio.get_running_loop().create_task(
            self._consume_parlays(self.parlay_stream), name="synpath-sx_bet-parlays")
        return self.parlay_stream

    async def _consume_parlays(self, stream: Any) -> None:
        async for event in stream:
            if getattr(event, "name", None) != "parlay_request":
                continue
            payload = getattr(event, "payload", None) or {}
            rfq = payload.get("rfq")
            if rfq is not None:
                self.on_parlay(rfq)
            elif isinstance(payload.get("row"), dict):
                self.on_parlay(payload["row"], received_at=getattr(event, "timestamp", None))

    def _parlay(self, rfq_id: str) -> Rfq:
        parent = ids.native(VENUE, rfq_id).lower()
        rfq = self.parlays.get(parent)
        if rfq is None:
            raise RfqNotFound(f"{VENUE}: no parlay request {parent} seen on parlay_markets:global; the venue lists "
                              "no open requests over REST, so only those heard on the stream are known")
        return self._parlay_state(rfq)

    async def fetch_incoming_rfqs(
        self, *, market_id: str | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Rfq]:
        """The parlay requests still open (inside their 60 s), newest first,
        as the stream heard them. The first call starts the stream, so it
        returns nothing until a request arrives. `market_id` matches the
        parlay market or any of its legs."""
        await self.watch_parlays()
        self._prune_parlays()
        rfqs = [self._parlay_state(r) for r in self.parlays.values()]
        rfqs = [r for r in rfqs if r.status == RfqStatus.OPEN]
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id).lower())
            rfqs = [r for r in rfqs if r.combo_id == wanted or any(l.market_id == wanted for l in r.combo.legs)]
        rfqs.sort(key=lambda r: r.created_at or 0, reverse=True)
        return Page(rfqs[:limit] if limit else rfqs, next_cursor=None)

    async def fetch_rfq(self, rfq_id: str) -> Rfq:
        """One parlay request the stream heard, open or (after its 60 s) closed."""
        return self._parlay(rfq_id)

    async def fetch_combo(self, combo_id: str) -> Combo:
        """A parlay market's legs, from its request."""
        combo = self._parlay(combo_id).combo
        assert combo is not None
        return combo

    async def lookup_combo(self, legs: list[ComboLeg]) -> Combo | None:
        """The newest parlay seen with exactly these legs and sides, or `None`."""
        wanted = {(l.market_id, l.outcome) for l in legs}
        for rfq in sorted(self.parlays.values(), key=lambda r: r.created_at or 0, reverse=True):
            if rfq.combo is not None and {(l.market_id, l.outcome) for l in rfq.combo.legs} == wanted:
                return rfq.combo
        return None

    async def create_combo(self, legs: list[ComboLeg]) -> Combo:
        raise NotSupported(f"{VENUE}: a parlay market is made only by a bettor's request on sx.bet; the API has no "
                           "way to request one (docs.sx.bet/developers/parlays), so combos are read, not created")

    # -- parlays: quoting -----------------------------------------------------

    def _parlay_expiry_s(self, rfq: Rfq) -> int:
        """The latest a quote's orders may be signed to live, in seconds."""
        end = int((rfq.expires_at or self._now_ms() + PARLAY_LIFETIME_S * 1000) // 1000)
        return end if rfq.info.get("expiry_exact") else end - PARLAY_EXPIRY_MARGIN_S

    def _quote_sides(self, quote: MakerQuote, rfq: Rfq, expiry_s: int, key: str) -> list[tuple[str, OrderRequest]]:
        """The orders a quote is: a bid buys YES (outcome one, the parlay
        wins), an ask sells YES (outcome two at one minus it), each `gtd` to
        the parlay's end. Without a quantity each side covers the bettor's
        requested stake at the price the bettor would pay."""
        sides: list[tuple[str, OrderRequest]] = []
        for label, price, side in (("bid", quote.bid, Side.BUY), ("ask", quote.ask, Side.SELL)):
            if price is None:
                continue
            quantity = quote.quantity
            if quantity is None:
                if rfq.amount is None:
                    raise BadRequest(f"{VENUE}: parlay {rfq.id} gives no requested size; quote a quantity")
                paid = price if side == Side.SELL else Decimal("1") - price
                quantity = (rfq.amount / paid).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
            sides.append((label, OrderRequest(
                market_id=rfq.combo_id, side=side, type=OrderType.LIMIT, price=price, quantity=quantity,
                time_in_force=TimeInForce.GTD, expires_at=expiry_s * 1000, post_only=quote.post_only,
                client_order_id=f"{key}:{label}", account=quote.account, book=quote.book, trader=quote.trader,
                tags=quote.tags,
            )))
        return sides

    async def send_quote(self, quote: MakerQuote) -> RfqQuote:
        """Answer a parlay request with signed orders on the parlay market:
        the bid as an outcome-one order, the ask as an outcome-two order,
        both `GTC` on the venue with the expiry the parlay allows (its 60 s
        less `PARLAY_EXPIRY_MARGIN_S`, or `quote.expires_at` if sooner). The
        bettor takes one with a FOK or IOC order; there is no last look.

        The quote's id names both orders (`quote_id_of`). A side the venue
        refuses while the other is placed leaves a one-sided quote, with the
        refusal in `info["refused"]`; both refused raises the first."""
        rfq = self._parlay(quote.rfq_id)
        if rfq.status != RfqStatus.OPEN:
            raise OrderRejected(f"{VENUE}: parlay {rfq.id} has closed (its 60 s are over)", reason="parlay_expired")
        signer = self._order_signer()
        meta = await self.metadata()
        key = quote.client_order_id or f"sx-quote-{uuid.uuid4().hex[:16]}"
        now = self.clock()
        for attempt in range(2):
            expiry_s = self._parlay_expiry_s(rfq)
            if quote.expires_at is not None:
                expiry_s = min(expiry_s, int(quote.expires_at // 1000))
            if expiry_s <= now:
                raise OrderRejected(f"{VENUE}: parlay {rfq.id} ends before a quote could live", reason="parlay_expired")
            sides = self._quote_sides(quote, rfq, expiry_s, key)
            built = [(label, req, *build_signed_order(req, signer=signer, meta=meta, now=now)) for label, req in sides]
            try:
                results = await self._post_orders([b[2] for b in built])
                break
            except (BadRequest, OrderNotFound) as exc:
                body = exc.body if isinstance(exc.body, dict) else {}
                exact = body.get("parlayExpiry")
                if attempt == 0 and "PARLAY_ORDER_EXPIRY_BEYOND_PARLAY_EXPIRY" in reason_of(exc.body, "") and exact:
                    # The venue names the parlay's real end: sign to it once.
                    rfq = rfq.model_copy(update={"expires_at": int(exact) * 1000,
                                                 "info": {**rfq.info, "expiry_exact": True}})
                    self.parlays[rfq.id] = rfq
                    continue
                raise quote_error_of(exc) from None
        placed: dict[str, Order] = {}
        refused: dict[str, Exception] = {}
        answers = iter(results)
        for label, req, _, order_id, odds, stake in built:
            result = next(answers, {})
            if str(result.get("status") or "") != "SUBMITTED":
                refused[label] = OrderRejected(f"{VENUE}: the {label} of the quote on parlay {rfq.id} was not "
                                               f"submitted: {result}", reason="failed", info=result)
                continue
            order = order_after_placing(req, result, order_id=str(result.get("orderId") or order_id).lower(),
                                        odds=odds, stake=stake, account=self.account)
            placed[label] = order.model_copy(update={"expires_at": expiry_s * 1000})
        if not placed:
            raise next(iter(refused.values()))
        out = quote_of_orders(placed.get("bid"), placed.get("ask"), rfq_id=rfq.id, account=quote.account or self.account)
        out = out.model_copy(update={"info": {
            **out.info, "client_order_id": key, "expiry_s": expiry_s, "time_in_force": "GTC",
            "refused": {k: str(v) for k, v in refused.items()},
        }})
        self._quotes[out.id] = rfq.id
        return out

    async def cancel_quote(self, quote_id: str, *, rfq_id: str | None = None) -> None:
        """Cancel the quote's orders (`DELETE /orders-v3`). An order already
        filled or expired stays as it is; none found at all is `RfqNotFound`."""
        wanted = [o for o in order_ids_of(quote_id) if o]
        if not wanted:
            raise RfqNotFound(f"{VENUE}: {quote_id!r} names no order")
        results = await self.cancel_orders(wanted)
        missing = [r for r in results if isinstance(r, OrderNotFound)]
        for result in results:
            if isinstance(result, Exception) and not isinstance(result, OrderNotFound):
                raise result
        self._quotes.pop(quote_id, None)
        if len(missing) == len(wanted):
            raise RfqNotFound(f"{VENUE}: no order of quote {quote_id} at the venue")

    async def replace_quote(self, quote_id: str, quote: MakerQuote) -> RfqQuote:
        """Cancel, then send: SX Bet has no order edit. A new id."""
        try:
            await self.cancel_quote(quote_id, rfq_id=quote.rfq_id)
        except RfqNotFound:
            pass
        return await self.send_quote(quote)

    async def confirm_quote(self, quote: RfqQuote) -> RfqQuote:
        raise NotSupported(f"{VENUE}: no last look on parlays; a quote is a resting order the bettor takes with FOK "
                           "or IOC, so it trades without a maker's confirm (docs.sx.bet/api-reference/parlay-markets)")

    async def fetch_quote(self, quote_id: str, *, rfq_id: str | None = None) -> RfqQuote:
        """One of our quotes, read back from its orders."""
        found: list[Order | None] = []
        for order_id in order_ids_of(quote_id):
            if not order_id:
                found.append(None)
                continue
            try:
                found.append(await self.fetch_order(order_id))
            except OrderNotFound:
                found.append(None)
        bid, ask = found
        if bid is None and ask is None:
            raise RfqNotFound(f"{VENUE}: no order of quote {quote_id} at the venue")
        quote = quote_of_orders(bid, ask, rfq_id=rfq_id or self._quotes.get(quote_id), account=self.account)
        return quote if quote.id == quote_id else quote.model_copy(update={"id": quote_id})

    async def fetch_my_quotes(
        self, *, rfq_id: str | None = None, status: QuoteStatus | str | None = None,
    ) -> list[RfqQuote]:
        """Our quotes: those sent through this adapter, read back; with
        `rfq_id`, also the resting orders on that parlay market, paired into
        quotes by the client order id they were sent with (so a quote sent
        in doubt is found again)."""
        wanted = QuoteStatus(status) if status is not None else None
        parent = ids.native(VENUE, rfq_id).lower() if rfq_id else None
        quotes: dict[str, RfqQuote] = {}
        for quote_id, held in list(self._quotes.items()):
            if parent and held != parent:
                continue
            try:
                quotes[quote_id] = await self.fetch_quote(quote_id, rfq_id=held)
            except RfqNotFound:
                self._quotes.pop(quote_id, None)
        if parent:
            pairs: dict[str, dict[str, Order]] = {}
            for order in await self.fetch_open_orders(market_id=ids.qualify(VENUE, parent)):
                coid = order.client_order_id or order.id
                base, _, label = coid.rpartition(":")
                if label not in ("bid", "ask"):
                    base, label = coid, "bid" if order.side == Side.BUY else "ask"
                pairs.setdefault(base, {})[label] = order
            for pair in pairs.values():
                quote = quote_of_orders(pair.get("bid"), pair.get("ask"), rfq_id=parent, account=self.account)
                quotes.setdefault(quote.id, quote)
        out = list(quotes.values())
        return [q for q in out if wanted is None or q.status == wanted]

    async def close(self) -> None:
        if self._parlay_task is not None:
            self._parlay_task.cancel()
            try:
                await self._parlay_task
            except (asyncio.CancelledError, Exception):
                pass
            self._parlay_task = None
        if self._own_parlay_stream and self.parlay_stream is not None:
            await self.parlay_stream.close()
        await self.http.close()
        if self._own_catalog:
            self.catalog.close()


MAX_CANCEL_ALL_ROUNDS = 50
"""Most cancel-all calls one `cancel_all_orders` makes while the venue says more are left."""


def _iso(ms: int) -> str:
    from ..types import iso

    return iso(ms) or ""


__all__ = [
    "SxBetTrading", "order_of_row", "fill_of_row", "position_of", "build_signed_order",
    "rfq_of_parlay", "combo_of_parlay", "parlay_result", "quote_of_orders", "quote_id_of", "order_ids_of",
]
