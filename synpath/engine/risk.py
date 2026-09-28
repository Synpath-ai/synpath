"""Pre-trade risk: the rules that refuse an order before it is signed.

Every rule here answers one question and names itself when it says no, so a
rejection is a sentence an operator can act on rather than a generic error.
The configuration is versioned in the journal: a rejection records which
version refused it, and a later argument about why is settled by reading
that version instead of guessing what the file said at the time.

Rules fall into three groups:

**About this order.** Price collar against the mark, order size and notional,
whole-contract and tick rules the venue would reject anyway, a restricted
list, and a guard against sending anything into a market that is about to
close. Also the duplicate window: the same order twice within a few seconds
is almost always a retry loop, not a decision.

**About the book it joins.** Open-order count, position caps per instrument,
per event and per venue, the exchange's own position limit where the venue
publishes one, and the daily loss limit, which reads the ledger rather than
a number someone maintains by hand.

**About the firm.** Self-trade prevention: an order that would cross this
firm's own resting order is refused (or the resting one is cancelled first,
where the caller asks for that), because a wash trade is a compliance
problem, not a fill. It looks at every book in the account, not just the one
placing the order, which is why it lives here and not in a strategy.

The kill switch sits alongside the rules. It can be engaged globally or for
one venue, and it decides what happens to orders already resting: `cancel`
pulls them, `hold` leaves them and blocks new ones, `rearm` blocks new
orders and lifts itself after a timer. The engine maps it onto the venues'
own mechanisms where they exist, which is what makes it fast: a Kalshi order
group cancels server-side in one round trip, and a Polymarket heartbeat that
stops beating cancels everything within ten seconds even if this process is
gone.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable, Literal

from pydantic import BaseModel, ConfigDict, Field

from .. import ids
from ..trading.types import Account, Order, OrderRequest, Side
from .ledger import Ledger

ZERO = Decimal("0")
ONE = Decimal("1")

HaltPolicy = Literal["cancel", "hold", "rearm"]
SelfTradeScope = Literal["off", "account", "firm"]


class RiskConfig(BaseModel):
    """What the engine will and will not do. Stored as a version in the journal."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True

    # -- this order --
    price_collar: Decimal | None = Decimal("0.10")
    """How far from the mark a limit may sit, in price units (contracts are
    priced 0 to 1, so 0.10 is ten cents). `None` disables the check."""
    max_order_contracts: Decimal | None = None
    max_order_notional: Decimal | None = None
    """Price times contracts, in account currency."""
    min_price: Decimal = Decimal("0")
    max_price: Decimal = Decimal("1")
    duplicate_window_ms: int = 2_000
    """The same instrument, side, price and size within this window is a retry."""
    closing_soon_s: int | None = 60
    """Refuse opening orders this close to a market's resolution time."""
    restricted: list[str] = Field(default_factory=list)
    """Instrument or market ids, or prefixes ending in `*`, nobody may trade."""

    # -- the book --
    max_open_orders: int | None = 200
    max_position_contracts: Decimal | None = None
    """Per instrument, per account, netted across books."""
    max_event_contracts: Decimal | None = None
    max_venue_notional: Decimal | None = None
    daily_loss_limit: Decimal | None = None
    """Realized loss since the day's start, as a positive number."""
    max_orders_per_minute: int | None = 120

    # -- the firm --
    self_trade_prevention: SelfTradeScope = "firm"
    exchange_limits: dict[str, Decimal] = Field(default_factory=dict)
    """Venue-published position limits, per market id."""
    paused_books: list[str] = Field(default_factory=list)
    """Strategies that may cancel but not open."""

    def paused(self, book: str | None) -> bool:
        return bool(book and book in self.paused_books)

    def restricts(self, market_id: str, _unused: str | None = None) -> str | None:
        for pattern in self.restricted:
            for candidate in (market_id, ids.split(market_id)[1]):
                if not candidate:
                    continue
                if pattern.endswith("*") and candidate.startswith(pattern[:-1]):
                    return pattern
                if candidate == pattern:
                    return pattern
        return None


@dataclass(frozen=True, slots=True)
class Decision:
    """The answer, and which rule gave it."""

    ok: bool
    rule: str = ""
    message: str = ""
    config_version: int | None = None
    detail: dict[str, Any] = field(default_factory=dict)

    def __bool__(self) -> bool:
        return self.ok


ALLOWED = Decision(ok=True)


@dataclass(slots=True)
class KillSwitch:
    """Stop trading now, and say what happens to what is already resting."""

    engaged: bool = False
    scope: str = "*"
    """`*` for everything, or a venue id."""
    policy: HaltPolicy = "cancel"
    reason: str = ""
    engaged_ts: int | None = None
    rearm_after_s: float | None = None
    venues: set[str] = field(default_factory=set)

    def engage(self, *, reason: str, scope: str = "*", policy: HaltPolicy = "cancel", rearm_after_s: float | None = None) -> None:
        self.engaged = True
        self.scope = scope
        self.policy = policy
        self.reason = reason
        self.engaged_ts = int(time.time() * 1000)
        self.rearm_after_s = rearm_after_s
        if scope != "*":
            self.venues.add(scope)

    def release(self, *, scope: str | None = None) -> None:
        if scope and scope != "*" and scope in self.venues:
            self.venues.discard(scope)
            if not self.venues and self.scope == scope:
                self.engaged = False
            return
        self.engaged = False
        self.venues.clear()
        self.reason = ""
        self.rearm_after_s = None

    def blocks(self, venue: str, *, now: float | None = None) -> bool:
        if not self.engaged:
            return False
        if self.policy == "rearm" and self.rearm_after_s is not None and self.engaged_ts is not None:
            if ((now or time.time()) * 1000 - self.engaged_ts) / 1000 >= self.rearm_after_s:
                self.release()
                return False
        return self.scope == "*" or venue in self.venues or venue == self.scope


class RiskEngine:
    """Runs the rules. Holds no venue state: everything it needs is passed in."""

    def __init__(
        self,
        config: RiskConfig | None = None,
        *,
        ledger: Ledger | None = None,
        config_version: int | None = None,
        clock: Any = time.time,
    ):
        self.config = config or RiskConfig()
        self.config_version = config_version
        self.ledger = ledger
        self.clock = clock
        self.kill = KillSwitch()
        self._recent: deque[tuple[float, tuple[str, str, str, str]]] = deque(maxlen=512)
        self._sent: deque[float] = deque(maxlen=4096)
        self.day_start_ms: int = _day_start_ms(self.clock())
        self.realized_at_day_start: Decimal = ZERO

    # -- configuration --------------------------------------------------------

    def configure(self, config: RiskConfig, *, version: int | None = None) -> None:
        self.config, self.config_version = config, version

    def roll_day(self, *, realized_now: Decimal | None = None) -> None:
        """Start a new trading day: the loss limit counts from here."""
        self.day_start_ms = _day_start_ms(self.clock())
        if realized_now is not None:
            self.realized_at_day_start = realized_now
        elif self.ledger is not None:
            self.realized_at_day_start = self.ledger.total().realized

    def realized_today(self) -> Decimal:
        if self.ledger is None:
            return ZERO
        return self.ledger.total().realized - self.realized_at_day_start

    # -- the check ------------------------------------------------------------

    def check(
        self,
        request: OrderRequest,
        *,
        venue: str,
        account: Account,
        open_orders: Iterable[Order] = (),
        mark: Decimal | None = None,
        market_close_ts: int | None = None,
        event_id: str | None = None,
        venue_notional: Decimal | None = None,
        deliberate: bool = False,
    ) -> Decision:
        """Everything that must be true before this order is signed.

        `deliberate` is true when the caller named the order's
        `client_order_id` itself: the duplicate window is for an accidental
        second click, and a caller that names each order is not clicking."""
        config = self.config
        if not config.enabled:
            return self._ok()
        if self.kill.blocks(venue, now=self.clock()):
            return self._no("kill_switch", f"trading is halted: {self.kill.reason or 'kill switch engaged'}")
        if config.paused(request.book):
            return self._no("book_paused", f"strategy {request.book!r} is paused; it may cancel but not open")

        market_id = request.market_id
        if (pattern := config.restricts(market_id)):
            return self._no("restricted", f"{market_id} matches the restricted entry {pattern!r}")

        price = request.price
        if price is not None:
            if not (config.min_price <= price <= config.max_price):
                return self._no("price_bounds", f"price {price} is outside {config.min_price}-{config.max_price}")
            if config.price_collar is not None and mark is not None:
                away = abs(price - mark)
                if away > config.price_collar:
                    return self._no(
                        "price_collar", f"price {price} is {away} from the mark {mark}, over the collar {config.price_collar}",
                        detail={"mark": str(mark), "away": str(away)},
                    )
        if config.max_order_contracts is not None and request.amount > config.max_order_contracts:
            return self._no("max_order_contracts", f"{request.amount} contracts is over the {config.max_order_contracts} limit")
        notional = (price or ONE) * request.amount
        if config.max_order_notional is not None and notional > config.max_order_notional:
            return self._no("max_order_notional", f"{notional} is over the {config.max_order_notional} order limit")

        now = self.clock()
        # An engine-held order's children are sliced on purpose and often look
        # alike (an iceberg's reloads, a TWAP's slices, two parents on one
        # market), so the window applies to orders sent from outside only.
        if config.duplicate_window_ms and not deliberate and not request.tags.get("parent"):
            signature = (market_id, request.side.value, str(price), str(request.amount))
            cutoff = now - config.duplicate_window_ms / 1000
            for stamp, seen in reversed(self._recent):
                if stamp < cutoff:
                    break
                if seen == signature:
                    return self._no(
                        "duplicate", f"the same order was sent {round((now - stamp) * 1000)}ms ago; "
                                     f"pass a different client_order_id if this is deliberate",
                    )
        if config.max_orders_per_minute is not None:
            while self._sent and self._sent[0] < now - 60:
                self._sent.popleft()
            if len(self._sent) >= config.max_orders_per_minute:
                return self._no("order_rate", f"{len(self._sent)} orders in the last minute, at the {config.max_orders_per_minute} limit")

        if config.closing_soon_s is not None and market_close_ts is not None and not request.reduce_only:
            left = (market_close_ts - now * 1000) / 1000
            if left <= config.closing_soon_s:
                return self._no(
                    "closing_soon", f"{market_id} resolves in {round(left)}s, inside the {config.closing_soon_s}s guard",
                    detail={"seconds_left": round(left)},
                )

        resting = [o for o in open_orders if not o.is_terminal]
        if config.max_open_orders is not None and len(resting) >= config.max_open_orders:
            return self._no("max_open_orders", f"{len(resting)} orders already resting, at the {config.max_open_orders} limit")

        if config.self_trade_prevention != "off" and price is not None:
            crossing = self._self_cross(request, resting, account, price)
            if crossing is not None:
                return self._no(
                    "self_trade", f"this would trade against the firm's own order {crossing.id} "
                                  f"({crossing.side.value} {crossing.remaining} at {crossing.price} on {crossing.market_id})",
                    detail={"order_id": crossing.id, "book": crossing.book or ""},
                )

        signed = request.amount if request.side == Side.BUY else -request.amount
        if self.ledger is not None:
            current = self._net_contracts(account.key, venue, market_id)
            after = abs(current + signed)
            if config.max_position_contracts is not None and after > config.max_position_contracts:
                return self._no(
                    "max_position", f"{market_id} would reach {after} contracts, over the {config.max_position_contracts} limit",
                    detail={"current": str(current), "after": str(after)},
                )
            limit = config.exchange_limits.get(market_id)
            if limit is not None and after > limit:
                return self._no(
                    "exchange_limit", f"{market_id} would reach {after} contracts, over the exchange limit {limit}",
                    detail={"limit": str(limit)},
                )
            if config.max_event_contracts is not None and event_id:
                total = self._event_contracts(account.key, venue, event_id) + abs(signed)
                if total > config.max_event_contracts:
                    return self._no("max_event", f"event {event_id} would reach {total} contracts, over {config.max_event_contracts}")
            if config.daily_loss_limit is not None:
                loss = -self.realized_today()
                if loss >= config.daily_loss_limit:
                    return self._no(
                        "daily_loss", f"today's realized loss {loss} has reached the {config.daily_loss_limit} limit",
                        detail={"loss": str(loss)},
                    )
        if config.max_venue_notional is not None and venue_notional is not None:
            if venue_notional + notional > config.max_venue_notional:
                return self._no(
                    "max_venue_notional",
                    f"{venue} would hold {venue_notional + notional} of notional, over {config.max_venue_notional}",
                )
        return self._ok()

    # -- bookkeeping ----------------------------------------------------------

    def record_sent(self, request: OrderRequest) -> None:
        """Count an order that actually went out, for the rate and duplicate windows."""
        now = self.clock()
        self._sent.append(now)
        self._recent.append((now, (request.market_id, request.side.value, str(request.price), str(request.amount))))

    def _self_cross(self, request: OrderRequest, resting: list[Order], account: Account, price: Decimal) -> Order | None:
        scope = self.config.self_trade_prevention
        side = request.side
        limit = price
        for order in resting:
            if order.price is None:
                continue
            if order.market_id != request.market_id:
                continue
            if scope == "account":
                order_account = order.account.key if order.account else None
                if order_account != account.key:
                    continue
            other_side = order.side
            other_price = order.price
            if other_side == side:
                continue
            if side == Side.BUY and limit >= other_price:
                return order
            if side == Side.SELL and limit <= other_price:
                return order
        return None

    def _net_contracts(self, account_key: str, venue: str, market_id: str) -> Decimal:
        total = ZERO
        for state in self.ledger.positions.values() if self.ledger else ():
            if state.account_key == account_key and state.venue == venue and state.market_id == market_id:
                total += state.contracts
        return total

    def _event_contracts(self, account_key: str, venue: str, event_id: str) -> Decimal:
        total = ZERO
        for state in self.ledger.positions.values() if self.ledger else ():
            if state.account_key == account_key and state.venue == venue and state.market_id.startswith(event_id):
                total += abs(state.contracts)
        return total

    def _ok(self) -> Decision:
        return Decision(ok=True, config_version=self.config_version)

    def _no(self, rule: str, message: str, *, detail: dict[str, Any] | None = None) -> Decision:
        return Decision(ok=False, rule=rule, message=message, config_version=self.config_version, detail=detail or {})


def _day_start_ms(now: float) -> int:
    """Midnight UTC before `now`, in milliseconds."""
    day = int(now) // 86_400 * 86_400
    return day * 1000


def market_of(market_id: str) -> str:
    """Kept for callers written against the instrument era; the id is the market."""
    return market_id


__all__ = [
    "RiskConfig", "RiskEngine", "Decision", "KillSwitch", "HaltPolicy", "SelfTradeScope", "ALLOWED", "market_of",
]
