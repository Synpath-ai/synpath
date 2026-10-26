"""Novig's parlay RFQ, from the liquidity provider's side: a pricer.

Built to Novig's RFQ documentation (docs.novig.com, API reference, RFQ).

**A round.** A user on Novig builds a parlay of 2 to 20 legs (outcome ids)
and Novig sends an RFQ to every registered pricer. The auction stays open 3
seconds; a pricer answers with a `price` (the taker's stake per $1 of
payout, so the parlay's probability as the pricer sees it) and a
`max_wager` (the largest stake it takes), or declines. The lowest price
wins, a tie going to the larger `max_wager`. When the user accepts, the
winner gets a 1-second last look at the user's actual stake and confirms or
rejects; silence rejects. The executed parlay settles as one position.

**Two transports, one pricer.** `NovigPricer` holds the pricing callback,
the last-look policy and the confirm-rate tracker, and answers rounds from
either transport:

* the maker WebSocket (`wss://api.novig.com/rfq/ws`), one outbound
  connection: `NovigPricer.stream()` (see `synpath.ws.novig_rfq`);
* webhooks Novig calls (`POST {base}/quote`, `/confirm`, `/ping`), each
  signed with `X-Novig-Signature`: `NovigPricer.webhook_app()` is a FastAPI
  app serving the three, and `handle_webhook` the same logic for any other
  framework.

A round that reaches both is priced once: the second transport gets the
same answer, and Novig keeps the first quote it receives.

**Confirm rate.** A pricer must confirm at least 60% of its last looks over
the past 30 minutes, judged once 20 count; below that Novig locks it out of
quoting for 5 minutes and starts counting again from zero. `ConfirmRate`
keeps the same count locally, so a pricer can see a lock coming, and the
pricer stops quoting while one is known.

**Auth.** The RFQ routes and sockets take the OAuth 2.0 client-credentials
bearer token Novig issues to market makers (`NovigRFQCredentials`), not the
NOVIG-V3 order-entry signature. The webhook signature is a BLAKE3 keyed
hash with the pricer's shared secret (`novig_rfq_signing`).

**Reads.** `NovigRFQ` reads the registration and confirm standing
(`GET /rfq/pricer`), the RFQs won (`GET /rfq/executions`, paged, and
`reconcile_executions` to catch up after a gap) and the collateral held in
open parlays (`GET /rfq/collateral`). Registration (`register_pricer`) and
the webhook test (`ping_webhook`) are there for the operator to run once.

Amounts are Novig Cash in `Decimal`; Novig sends and takes them as strings.
"""
from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import re
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import ROUND_CEILING, ROUND_DOWN, ROUND_HALF_UP, Decimal
from typing import Any, AsyncIterator, Awaitable, Callable, Iterable, Literal, Sequence

import httpx

from ..base import AsyncHttpClient
from ..errors import AuthenticationError, MarketNotFound
from . import novig_rfq_signing as signing
from .credentials import NovigRFQCredentials
from .errors import CredentialsMissing, InvalidOrder, PermissionDenied

log = logging.getLogger("synpath.trading.novig_rfq")

VENUE = "novig"

API_URLS = {"production": "https://api.novig.com", "qa": "https://api-qa.novig.us"}
WS_URLS = {"production": "wss://api.novig.com/rfq/ws", "qa": "wss://api-qa.novig.us/rfq/ws"}
FEED_URLS = {"production": "wss://api.novig.com/rfq/executions/feed", "qa": "wss://api-qa.novig.us/rfq/executions/feed"}
TOKEN_PATH = "/nbx/v1/auth/emm-token"
"""Where the client credentials are exchanged for a bearer token."""

TOKEN_LIFETIME = 30 * 60.0
"""Seconds a token lasts when the answer does not say."""
TOKEN_REFRESH_EARLY = 120.0

AUCTION_SECONDS = 3.0
LAST_LOOK_SECONDS = 1.0
QUOTE_LIFETIME_PREGAME = 30.0
QUOTE_LIFETIME_LIVE = 10.0
"""Seconds a winning quote stays acceptable after the auction closes."""

MIN_WAGER = Decimal("10.00")
"""The platform minimum: a quote whose `max_wager` is below it, or below the
taker's `min_wager`, is passed over."""
MIN_LEGS, MAX_LEGS = 2, 20
PRICE_STEP = Decimal("0.001")
PRICE_MIN, PRICE_MAX = Decimal("0.001"), Decimal("0.999")
QUOTE_ID_MAX_BYTES = 256

CONFIRM_THRESHOLD = Decimal("0.60")
CONFIRM_LOOKBACK = 30 * 60.0
CONFIRM_MIN_SAMPLE = 20
LOCK_SECONDS = 5 * 60.0

EXECUTIONS_PAGE = 500
CENT = Decimal("0.01")
ZERO, ONE = Decimal("0"), Decimal("1")

Transport = Literal["websocket", "webhook"]


def D(value: Any) -> Decimal:
    if value in (None, ""):
        return ZERO
    return Decimal(str(value))


def maybe_D(value: Any) -> Decimal | None:
    return None if value in (None, "") else Decimal(str(value))


_FRACTION = re.compile(r"\.(\d+)")


def ms_of(value: Any) -> int | None:
    """An RFC 3339 time as Unix milliseconds; `None` when absent or unreadable."""
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip().replace("Z", "+00:00").replace("z", "+00:00")
    # Python 3.10 reads three or six fractional digits only.
    text = _FRACTION.sub(lambda m: "." + (m.group(1) + "000000")[:6], text, count=1)
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return int(moment.timestamp() * 1000)


def now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Rounds, quotes and last looks
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True, kw_only=True)
class ParlayRFQ:
    """One round: the legs a taker wants combined, and when it closes."""

    rfq_id: str
    outcome_ids: tuple[str, ...]
    """One outcome id per leg, in the order the taker asked for them."""
    min_wager: Decimal = MIN_WAGER
    """The smallest stake a quote must cover: the taker's minimum or the
    platform's $10, whichever is higher."""
    expires_at: int | None = None
    """When the auction closes, Unix ms (the venue's clock)."""
    created_at: int | None = None
    received_at: int = field(default_factory=now_ms)
    transport: Transport = "websocket"
    info: dict[str, Any] = field(default_factory=dict)

    @property
    def legs(self) -> int:
        return len(self.outcome_ids)

    def deadline(self) -> int:
        """When an answer must be in, Unix ms on this machine's clock: the
        auction's close, or 3 s after this round arrived when that is sooner."""
        local = self.received_at + int(AUCTION_SECONDS * 1000)
        return min(local, self.expires_at) if self.expires_at else local

    @classmethod
    def from_ws(cls, data: dict[str, Any], *, received_at: int | None = None) -> "ParlayRFQ":
        """From the socket's `rfq_created`."""
        return cls(
            rfq_id=str(data["rfq_id"]), outcome_ids=tuple(str(o) for o in data.get("outcome_ids") or ()),
            min_wager=max(MIN_WAGER, D(data.get("min_wager"))), expires_at=ms_of(data.get("expires_at")),
            received_at=received_at or now_ms(), transport="websocket", info=dict(data),
        )

    @classmethod
    def from_webhook(cls, body: dict[str, Any], *, received_at: int | None = None) -> "ParlayRFQ":
        """From a `/quote` request. `min_wager` is left out when the taker set
        none; the platform minimum still applies. The auction closes 3 s
        after `timestamp`."""
        created = ms_of(body.get("timestamp"))
        return cls(
            rfq_id=str(body["rfq_id"]), outcome_ids=tuple(str(o) for o in body.get("outcome_ids") or ()),
            min_wager=max(MIN_WAGER, D(body.get("min_wager"))), created_at=created,
            expires_at=created + int(AUCTION_SECONDS * 1000) if created else None,
            received_at=received_at or now_ms(), transport="webhook", info=dict(body),
        )


def round_price(price: Any) -> Decimal:
    """A price on Novig's RFQ grid of 0.001, rounded up (toward the pricer),
    so the venue's own rounding to the nearest 0.001 never moves it."""
    value = D(price).quantize(PRICE_STEP, rounding=ROUND_CEILING)
    return value


@dataclass(frozen=True, slots=True)
class ParlayQuote:
    """A pricer's answer to a round.

    `price` is the taker's stake per $1 of payout: `0.45` means the taker
    stakes $0.45 to win $1.00 back, so the pricer puts up $0.55. Lower wins.
    `max_wager` is the largest taker stake taken at that price. `quote_id`
    is the pricer's own reference (up to 256 bytes), handed back on the last
    look as `external_flag`."""

    price: Decimal
    max_wager: Decimal
    quote_id: str | None = None

    def checked(self) -> "ParlayQuote":
        """The quote as it will be sent: the price on the 0.001 grid, and
        refused (`InvalidOrder`) where Novig would drop it."""
        price = round_price(self.price)
        if not PRICE_MIN <= price <= PRICE_MAX:
            raise InvalidOrder(f"{VENUE}: RFQ price {self.price} is outside 0.001 to 0.999 after rounding")
        wager = D(self.max_wager)
        if wager <= 0:
            raise InvalidOrder(f"{VENUE}: RFQ max_wager must be above zero, got {self.max_wager}")
        if self.quote_id is not None and len(self.quote_id.encode("utf-8")) > QUOTE_ID_MAX_BYTES:
            raise InvalidOrder(f"{VENUE}: RFQ quote_id is longer than {QUOTE_ID_MAX_BYTES} bytes")
        return ParlayQuote(price=price, max_wager=wager, quote_id=self.quote_id)

    def ws_data(self, rfq_id: str) -> dict[str, Any]:
        """`create_quote`'s data: every amount a decimal string."""
        data: dict[str, Any] = {"rfq_id": rfq_id, "price": str(self.price), "max_wager": str(self.max_wager)}
        if self.quote_id is not None:
            data["quote_id"] = self.quote_id
        return data

    def webhook_body(self) -> dict[str, Any]:
        """`/quote`'s answer: `price` a JSON number, `max_wager` a string."""
        body: dict[str, Any] = {"price": float(self.price), "max_wager": str(self.max_wager)}
        if self.quote_id is not None:
            body["quote_id"] = self.quote_id
        return body

    def collateral(self, wager: Decimal | None = None) -> Decimal:
        """What the pricer puts up against a stake (the whole `max_wager`
        by default): the pot less the stake."""
        return collateral(self.max_wager if wager is None else D(wager), self.price)


@dataclass(frozen=True, slots=True, kw_only=True)
class LastLook:
    """A won quote the taker accepted: confirm or reject within 1 second."""

    rfq_id: str
    wager: Decimal
    """The taker's actual stake; never above the quote's `max_wager`."""
    price: Decimal
    """The price selected: the pricer's own."""
    quote_id: str | None = None
    """Novig's id for the quote (the socket's `quote_accepted`); what the
    socket's `confirm` names. Webhooks carry none."""
    external_flag: str | None = None
    """The pricer's own `quote_id`, or `None` when it sent none."""
    transport: Transport = "websocket"
    received_at: int = field(default_factory=now_ms)
    rfq: ParlayRFQ | None = None
    """The round, when this pricer saw it."""
    quote: ParlayQuote | None = None
    """What this pricer quoted on the round, when it did."""
    info: dict[str, Any] = field(default_factory=dict)

    def deadline(self) -> int:
        return self.received_at + int(LAST_LOOK_SECONDS * 1000)

    @property
    def collateral(self) -> Decimal:
        return collateral(self.wager, self.price)

    @classmethod
    def from_ws(cls, data: dict[str, Any], *, received_at: int | None = None) -> "LastLook":
        """From the socket's `quote_accepted`."""
        return cls(
            rfq_id=str(data["rfq_id"]), wager=D(data.get("wager")), price=D(data.get("price")),
            quote_id=str(data["quote_id"]) if data.get("quote_id") else None,
            external_flag=data.get("external_flag"), transport="websocket",
            received_at=received_at or now_ms(), info=dict(data),
        )

    @classmethod
    def from_webhook(cls, body: dict[str, Any], *, received_at: int | None = None) -> "LastLook":
        """From a `/confirm` request."""
        return cls(
            rfq_id=str(body["rfq_id"]), wager=D(body.get("wager")), price=D(body.get("price")),
            external_flag=body.get("external_flag"), transport="webhook",
            received_at=received_at or now_ms(), info=dict(body),
        )


def matches_quote(look: LastLook) -> bool:
    """The default last-look policy: confirm a trade on a quote this pricer
    sent, at its price, for no more than its `max_wager`, under its own
    `quote_id`. Anything else is rejected."""
    quote = look.quote
    if quote is None:
        return False
    if look.wager <= 0 or look.wager > quote.max_wager:
        return False
    if round_price(look.price) != quote.price:
        return False
    if look.external_flag is not None and quote.quote_id is not None and look.external_flag != quote.quote_id:
        return False
    return True


# ---------------------------------------------------------------------------
# Money: the pot, collateral and settlement
# ---------------------------------------------------------------------------

def pot(wager: Any, price: Any) -> Decimal:
    """The parlay's pot, the stake plus the pricer's collateral: the stake
    over the quoted price, rounded down to the cent."""
    return (D(wager) / D(price)).quantize(CENT, rounding=ROUND_DOWN)


def collateral(wager: Any, price: Any) -> Decimal:
    """What the pricer puts up: the pot less the stake. The same figure as
    an execution's `liability`."""
    return pot(wager, price) - D(wager)


LegValue = Decimal | float | int | str | None


def settlement_scalar(leg_values: Iterable[LegValue]) -> Decimal | None:
    """What share of the pot goes to the taker, from each leg's graded value
    (`1` a win, `0` a loss, a voided leg's fair market value) or `None` for a
    leg not yet graded.

    Any loss settles the parlay at once, at `0`, whatever is still open;
    otherwise an open leg leaves it open (`None`); otherwise the product of
    the values, rounded to three decimals and never below `0.001`."""
    values = [None if v is None else D(v) for v in leg_values]
    if any(v is not None and v == 0 for v in values):
        return ZERO
    if any(v is None for v in values):
        return None
    product = ONE
    for value in values:
        if not ZERO <= value <= ONE:  # type: ignore[operator]
            raise ValueError(f"a leg's value is between 0 and 1, got {value}")
        product *= value  # type: ignore[operator]
    return max(Decimal("0.001"), product.quantize(PRICE_STEP, rounding=ROUND_HALF_UP))


@dataclass(frozen=True, slots=True)
class ParlaySettlement:
    result: Literal["open", "loss", "win", "fmv"]
    """`loss`: the pricer takes the pot; `win`: the taker does; `fmv`: they
    split it by `scalar` (a voided leg)."""
    scalar: Decimal | None
    pot: Decimal
    taker_payout: Decimal | None
    pricer_payout: Decimal | None
    pricer_pnl: Decimal | None
    """The pricer's payout less its collateral."""


def settle(wager: Any, price: Any, leg_values: Iterable[LegValue]) -> ParlaySettlement:
    """How an executed parlay pays, by Novig's rules from August 20, 2026:
    the taker gets the pot times the scalar, the pricer the rest. The
    taker's share is rounded down to the cent here; Novig does not publish
    its rounding of a split pot."""
    whole = pot(wager, price)
    scalar = settlement_scalar(leg_values)
    if scalar is None:
        return ParlaySettlement("open", None, whole, None, None, None)
    taker = (whole * scalar).quantize(CENT, rounding=ROUND_DOWN)
    pricer = whole - taker
    result: Literal["loss", "win", "fmv"] = "loss" if scalar == 0 else "win" if scalar == 1 else "fmv"
    return ParlaySettlement(result, scalar, whole, taker, pricer, pricer - (whole - D(wager)))


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------

LINE_KINDS = {"moneyline": "side", "spread": "side", "total": "total"}
"""Lines of the same kind on one game do not combine: a spread with a
moneyline, two spreads, two totals."""

POWER_PLAY_CONFLICTS = frozenset({"points", "assists", "goals", "shots_on_goal"})


@dataclass(frozen=True, slots=True, kw_only=True)
class ParlayLeg:
    """What the eligibility rules look at in one leg. The RFQ itself carries
    only outcome ids; a pricer fills these from its own catalog."""

    outcome_id: str
    event_id: str | None = None
    """The game (or futures question) the leg is on."""
    league: str | None = None
    market_type: str | None = None
    """`moneyline`, `spread`, `total`, or a prop's type, like `player_points`
    or `first_touchdown_scorer`."""
    period: str = "game"
    """`game` or `first_half`: the same line on each half counts apart."""
    team_ids: tuple[str, ...] = ()
    """The teams the leg's event involves."""
    player_id: str | None = None
    stats: frozenset[str] = frozenset()
    """A player prop's stats: `{"points", "rebounds", "assists"}` for PRA."""
    future: bool = False
    special: bool = False
    """A special or promo market."""
    yes_no: bool = False
    """A yes-or-no market (a future, a first scorer, a draw, a three-way
    moneyline leg)."""
    no_side: bool = False
    """Whether the leg takes that market's NO."""
    open: bool = True
    """Whether the leg's event is open, before the game or in play."""


def eligibility_problems(legs: Sequence[ParlayLeg]) -> list[str]:
    """Why Novig would refuse to send an RFQ for these legs, by its published
    rules; empty when none apply. The venue's blocked pairs change over time,
    so this is a pre-check, not the venue's answer, and a pricer can decline
    any RFQ it receives."""
    problems: list[str] = []
    if not MIN_LEGS <= len(legs) <= MAX_LEGS:
        problems.append(f"a parlay has {MIN_LEGS} to {MAX_LEGS} legs, not {len(legs)}")
    seen: set[str] = set()
    for leg in legs:
        if leg.outcome_id in seen:
            problems.append(f"outcome {leg.outcome_id} appears twice")
        seen.add(leg.outcome_id)
        if not leg.open:
            problems.append(f"leg {leg.outcome_id}: its event is not open")
        if leg.special:
            problems.append(f"leg {leg.outcome_id}: special and promo markets cannot be legs")
        if leg.yes_no and leg.no_side:
            problems.append(f"leg {leg.outcome_id}: the NO side of a yes-or-no market cannot be a leg")
    for i, a in enumerate(legs):
        for b in legs[i + 1:]:
            problems.extend(_pair_problems(a, b))
    return problems


def is_eligible(legs: Sequence[ParlayLeg]) -> bool:
    return not eligibility_problems(legs)


def _pair_problems(a: ParlayLeg, b: ParlayLeg) -> list[str]:
    pair = f"legs {a.outcome_id} and {b.outcome_id}"
    out: list[str] = []
    same_game = a.event_id is not None and a.event_id == b.event_id
    if same_game:
        kind_a, kind_b = LINE_KINDS.get(a.market_type or ""), LINE_KINDS.get(b.market_type or "")
        if kind_a and kind_a == kind_b and a.period == b.period:
            out.append(f"{pair}: two {kind_a} lines on the same game")
        if (a.market_type and a.market_type == b.market_type and "first" in a.market_type
                and "scorer" in a.market_type):
            out.append(f"{pair}: two first-scorer legs of the same type")
        pp_a, pp_b = "power_play_points" in a.stats, "power_play_points" in b.stats
        if (pp_a and b.stats & POWER_PLAY_CONFLICTS) or (pp_b and a.stats & POWER_PLAY_CONFLICTS):
            out.append(f"{pair}: power play points with points, assists, goals or shots on goal")
    if a.player_id is not None and a.player_id == b.player_id:
        if a.market_type and a.market_type == b.market_type:
            out.append(f"{pair}: two legs of the same market type on one player")
        elif a.stats and b.stats and a.stats != b.stats and (a.stats <= b.stats or b.stats <= a.stats):
            out.append(f"{pair}: one stat contains the other on one player")
    if not same_game and a.event_id is not None and b.event_id is not None:
        if set(a.team_ids) & set(b.team_ids):
            out.append(f"{pair}: legs from different games share a team")
        if (a.future or b.future) and a.league is not None and a.league == b.league:
            out.append(f"{pair}: a future combines only with legs from other leagues")
    return out


# ---------------------------------------------------------------------------
# Confirm rate
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ConfirmStanding:
    """Where a pricer stands against the confirm rate (`GET /rfq/pricer`)."""

    lookback_minutes: int = int(CONFIRM_LOOKBACK // 60)
    counted: int = 0
    confirmed: int = 0
    threshold: Decimal = CONFIRM_THRESHOLD
    min_sample: int = CONFIRM_MIN_SAMPLE

    @property
    def rate(self) -> Decimal | None:
        return Decimal(self.confirmed) / Decimal(self.counted) if self.counted else None

    @property
    def judged(self) -> bool:
        return self.counted >= self.min_sample

    @property
    def below(self) -> bool:
        rate = self.rate
        return self.judged and rate is not None and rate < self.threshold

    def rejects_left(self) -> int:
        """How many rejections (or misses) in a row this window takes before
        the next one locks the pricer out; `0` when the next one would."""
        if self.below:
            return 0
        rejects = 0
        while True:
            counted = self.counted + rejects + 1
            if counted >= self.min_sample and Decimal(self.confirmed) / Decimal(counted) < self.threshold:
                return rejects
            rejects += 1

    @classmethod
    def from_row(cls, row: dict[str, Any] | None) -> "ConfirmStanding":
        row = row or {}
        return cls(
            lookback_minutes=int(row.get("lookbackMinutes") or CONFIRM_LOOKBACK // 60),
            counted=int(row.get("counted") or 0), confirmed=int(row.get("confirmed") or 0),
            threshold=D(row.get("threshold") or CONFIRM_THRESHOLD), min_sample=int(row.get("minSample") or CONFIRM_MIN_SAMPLE),
        )


@dataclass(frozen=True, slots=True)
class PricerLock:
    until: int | None
    """Unix ms, or `None` for a lock with no set end."""
    source: str
    """`confirm_rate`, or `admin` for one Novig set by hand."""


class ConfirmRate:
    """The confirm rate as Novig counts it, kept locally: every last look in
    the past 30 minutes counts, a confirm for, a reject or a miss against.
    Once 20 count, a rate below 60% locks the pricer out for 5 minutes, and
    counting starts again from zero when the lock ends.

    Novig also counts last looks this process never saw (a confirm that
    arrived late, a webhook that errored), so `sync` takes the venue's own
    standing from `GET /rfq/pricer` when it is read."""

    def __init__(self, *, threshold: Decimal = CONFIRM_THRESHOLD, lookback: float = CONFIRM_LOOKBACK,
                 min_sample: int = CONFIRM_MIN_SAMPLE, lock_seconds: float = LOCK_SECONDS,
                 clock: Callable[[], float] = time.time):
        self.threshold = Decimal(str(threshold))
        self.lookback = lookback
        self.min_sample = min_sample
        self.lock_seconds = lock_seconds
        self.clock = clock
        self._looks: dict[str, tuple[float, bool]] = {}
        self._base_counted = 0
        self._base_confirmed = 0
        self.lock: PricerLock | None = None

    def _now(self, at: float | None) -> float:
        return self.clock() if at is None else at

    def _prune(self, now: float) -> None:
        horizon = now - self.lookback
        for key in [k for k, (t, _) in self._looks.items() if t < horizon]:
            del self._looks[key]

    def record(self, rfq_id: str, confirmed: bool, *, at: float | None = None) -> None:
        """One last look answered (`confirmed`), or rejected or missed."""
        now = self._now(at)
        if self.locked(now):
            return
        self._prune(now)
        self._looks[rfq_id] = (now, bool(confirmed))
        standing = self.standing(now)
        if standing.below:
            self.locked_out(until=int((now + self.lock_seconds) * 1000), source="confirm_rate", at=now)

    def amend(self, rfq_id: str, confirmed: bool) -> None:
        """Correct an answer Novig counted otherwise: a confirm that reached
        it after the window closed counts as a miss."""
        if rfq_id in self._looks:
            stamp, _ = self._looks[rfq_id]
            self._looks[rfq_id] = (stamp, bool(confirmed))

    def standing(self, at: float | None = None) -> ConfirmStanding:
        now = self._now(at)
        self._prune(now)
        counted = self._base_counted + len(self._looks)
        confirmed = self._base_confirmed + sum(1 for _, ok in self._looks.values() if ok)
        return ConfirmStanding(int(self.lookback // 60), counted, confirmed, self.threshold, self.min_sample)

    @property
    def rate(self) -> Decimal | None:
        return self.standing().rate

    def would_lock(self, confirmed: bool, *, at: float | None = None) -> bool:
        """Whether answering the next last look this way would lock the pricer out."""
        standing = self.standing(at)
        after = ConfirmStanding(standing.lookback_minutes, standing.counted + 1,
                                standing.confirmed + (1 if confirmed else 0), standing.threshold, standing.min_sample)
        return after.below

    def locked(self, at: float | None = None) -> bool:
        lock = self.lock
        if lock is None:
            return False
        if lock.until is None:
            return True
        if self._now(at) * 1000 >= lock.until:
            self.lock = None
            return False
        return True

    def locked_out(self, *, until: int | None, source: str = "confirm_rate", at: float | None = None) -> None:
        """Novig locked the pricer out; counting starts from zero when it ends."""
        self.lock = PricerLock(until=until, source=source)
        self._looks.clear()
        self._base_counted = self._base_confirmed = 0

    def sync(self, standing: ConfirmStanding | None, lock: PricerLock | None = None) -> None:
        """Take Novig's own count as the base; last looks recorded here from
        now on are added to it."""
        if standing is not None:
            self._looks.clear()
            self._base_counted, self._base_confirmed = standing.counted, standing.confirmed
            self.threshold, self.min_sample = standing.threshold, standing.min_sample
        self.lock = lock


_UNTIL = re.compile(r"until\s+(\d{4}-\d{2}-\d{2}T[0-9:.]+(?:Z|[+-]\d{2}:\d{2})?)")


def lock_of_message(message: str) -> PricerLock | None:
    """A `FORBIDDEN` error's lock: `"... locked out of RFQ quoting until
    2026-10-06T18:28:00Z for a low confirm rate."`"""
    if "locked out" not in (message or "").lower():
        return None
    match = _UNTIL.search(message)
    source = "confirm_rate" if "confirm rate" in message.lower() else "admin"
    return PricerLock(until=ms_of(match.group(1)) if match else None, source=source)


# ---------------------------------------------------------------------------
# REST: registration, executions, collateral
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class PricerRegistration:
    pricer_id: str
    webhook_url: str | None
    shared_secret: str | None = field(default=None, repr=False)
    """Only `register_pricer` returns it, once; store it."""
    standing: ConfirmStanding | None = None
    lock: PricerLock | None = None
    info: dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "PricerRegistration":
        lock = row.get("lock")
        return cls(
            pricer_id=str(row.get("pricerId") or ""), webhook_url=row.get("webhookUrl"),
            shared_secret=row.get("sharedSecret"),
            standing=ConfirmStanding.from_row(row["confirmStanding"]) if row.get("confirmStanding") else None,
            lock=PricerLock(until=ms_of(lock.get("until")), source=str(lock.get("source") or "")) if lock else None,
            info={k: v for k, v in row.items() if k != "sharedSecret"},
        )


@dataclass(frozen=True, slots=True)
class RFQExecution:
    """An RFQ this pricer won (`GET /rfq/executions`)."""

    rfq_id: str
    parlay_id: str | None
    wager: Decimal
    price: Decimal
    liability: Decimal
    """The pricer's collateral on the parlay."""
    status: str
    """`open` until the parlay settles, then `settled`."""
    result: str | None
    """`win`, `loss`, `push` (before August 20, 2026) or `fmv`, once settled:
    the taker's result."""
    fmv_value: Decimal | None
    outcome_ids: tuple[str, ...]
    """The legs, sorted by outcome id (not in the taker's order)."""
    executed_at: int | None
    settled_at: int | None
    info: dict[str, Any] = field(default_factory=dict, repr=False)

    @property
    def pot(self) -> Decimal:
        return self.wager + self.liability

    @property
    def pricer_payout(self) -> Decimal | None:
        """What the pricer was paid from the pot, once settled. A `push`
        refunded the stake, so the pricer got its collateral back."""
        if self.status != "settled" or self.result is None:
            return None
        if self.result == "loss":
            return self.pot
        if self.result == "win":
            return ZERO
        if self.result == "push":
            return self.liability
        if self.result == "fmv" and self.fmv_value is not None:
            return self.pot - (self.pot * self.fmv_value).quantize(CENT, rounding=ROUND_DOWN)
        return None

    @property
    def pricer_pnl(self) -> Decimal | None:
        paid = self.pricer_payout
        return None if paid is None else paid - self.liability

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "RFQExecution":
        return cls(
            rfq_id=str(row.get("rfq_id") or ""), parlay_id=row.get("parlay_id"), wager=D(row.get("wager")),
            price=D(row.get("price")), liability=D(row.get("liability")), status=str(row.get("status") or ""),
            result=row.get("result"), fmv_value=maybe_D(row.get("fmv_value")),
            outcome_ids=tuple(str(o) for o in row.get("outcome_ids") or ()),
            executed_at=ms_of(row.get("executed_at")), settled_at=ms_of(row.get("settled_at")), info=dict(row),
        )


@dataclass(frozen=True, slots=True)
class RFQCollateral:
    """The collateral in RFQ parlays that filled and have not settled."""

    collateral: Decimal
    rfqs_filled: int
    timestamp: int | None
    info: dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "RFQCollateral":
        return cls(collateral=D(row.get("rfq_collateral")), rfqs_filled=int(row.get("rfqs_filled") or 0),
                   timestamp=ms_of(row.get("timestamp")), info=dict(row))


class BearerToken:
    """The market-maker bearer token: a fixed one, or one exchanged for the
    client credentials and renewed before it expires."""

    def __init__(self, credentials: NovigRFQCredentials, *, base_url: str, client: httpx.AsyncClient | None = None,
                 clock: Callable[[], float] = time.time):
        self.credentials = credentials
        self.url = credentials.token_url or f"{base_url.rstrip('/')}{TOKEN_PATH}"
        self._client = client
        self._token: str | None = credentials.access_token
        self._expires = math.inf if credentials.access_token else 0.0
        self._lock = asyncio.Lock()
        self.clock = clock

    @property
    def fixed(self) -> bool:
        return bool(self.credentials.access_token)

    def invalidate(self) -> None:
        if not self.fixed:
            self._token, self._expires = None, 0.0

    async def get(self) -> str:
        if self._token and self.clock() < self._expires - TOKEN_REFRESH_EARLY:
            return self._token
        async with self._lock:
            if self._token and self.clock() < self._expires - TOKEN_REFRESH_EARLY:
                return self._token
            creds = self.credentials
            if not (creds.client_id and creds.client_secret):
                raise CredentialsMissing(
                    f"{VENUE}: the RFQ routes need a market-maker bearer token; set NOVIG_RFQ_CLIENT_ID and "
                    "NOVIG_RFQ_CLIENT_SECRET (or NOVIG_RFQ_ACCESS_TOKEN)"
                )
            client = self._client or httpx.AsyncClient(timeout=30.0)
            try:
                response = await client.post(self.url, json={
                    "grant_type": "client_credentials", "client_id": creds.client_id,
                    "client_secret": creds.client_secret,
                })
            finally:
                if self._client is None:
                    await client.aclose()
            if response.status_code >= 400:
                raise AuthenticationError(f"{VENUE}: the token endpoint refused the client credentials "
                                          f"({response.status_code})", status=response.status_code)
            body = response.json()
            token = body.get("access_token") if isinstance(body, dict) else None
            if not token:
                raise AuthenticationError(f"{VENUE}: the token endpoint answered without an access_token")
            lifetime = float(body.get("expires_in") or TOKEN_LIFETIME)
            self._token, self._expires = str(token), self.clock() + lifetime
            return self._token


class BearerAuth(httpx.Auth):
    """Puts the bearer token on each request, and on a 401 renews it and
    tries once more."""

    def __init__(self, token: BearerToken):
        self.token = token

    async def async_auth_flow(self, request: httpx.Request) -> AsyncIterator[httpx.Request]:  # type: ignore[override]
        request.headers["Authorization"] = f"Bearer {await self.token.get()}"
        response = yield request
        if response.status_code == 401 and not self.token.fixed:
            self.token.invalidate()
            request.headers["Authorization"] = f"Bearer {await self.token.get()}"
            yield request


class NovigRFQ:
    """Novig's RFQ REST routes for a pricer's account.

    ```python
    from synpath.trading.credentials import load_novig_rfq
    from synpath.trading.novig_rfq import NovigRFQ

    async with NovigRFQ(load_novig_rfq()) as rfq:
        print(await rfq.fetch_pricer(), await rfq.fetch_collateral())
    ```
    """

    def __init__(self, credentials: NovigRFQCredentials | None = None, *, env: str | None = None,
                 url: str | None = None, client: httpx.AsyncClient | None = None,
                 token_client: httpx.AsyncClient | None = None, timeout: float = 30.0):
        credentials = credentials or NovigRFQCredentials()
        self.credentials = credentials
        self.env = env or credentials.env
        if self.env not in API_URLS:
            raise ValueError(f"{VENUE}: the RFQ env is production or qa, got {self.env!r}")
        self.url = (url or API_URLS[self.env]).rstrip("/")
        self.token = BearerToken(credentials, base_url=self.url, client=token_client)
        shared = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True,
                                             headers={"Accept": "application/json"})
        shared.auth = BearerAuth(self.token)
        self._own_client = client is None
        self.http = AsyncHttpClient(self.url, limiter=None, client=shared, venue=VENUE)

    async def __aenter__(self) -> "NovigRFQ":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        if self._own_client:
            await self.http._client.aclose()

    async def _call(self, method: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None) -> Any:
        query = {k: v for k, v in (params or {}).items() if v is not None}
        try:
            return await self.http.request(method, path, params=query or None, json=body)
        except AuthenticationError as exc:
            if exc.status == 403:
                raise PermissionDenied(
                    f"{VENUE}: {path} refused (403): the account needs a registered pricer and an LP account "
                    "(developers@novig.com)", body=exc.body, status=exc.status) from None
            raise

    # -- registration ---------------------------------------------------------

    async def fetch_pricer(self) -> PricerRegistration | None:
        """The registration and confirm standing, without the secret; `None`
        with no pricer registered."""
        try:
            row = await self._call("GET", "/rfq/pricer")
        except MarketNotFound:
            return None
        return PricerRegistration.from_row(row or {})

    async def register_pricer(self, webhook_url: str | None = None) -> PricerRegistration:
        """Register the account's pricer, or change its webhook URL. The
        first call with a `webhook_url` returns the shared secret, once; a
        first call without one registers a WebSocket-only pricer. A webhook
        cannot be removed once registered."""
        body: dict[str, Any] = {} if webhook_url is None else {"webhookUrl": webhook_url}
        return PricerRegistration.from_row(await self._call("POST", "/rfq/pricer", body=body) or {})

    async def ping_webhook(self) -> dict[str, Any]:
        """Have Novig send a signed request to `{webhook_url}/ping` and report
        how it answered (`url`, `ok`, `status`, `error`). A successful ping
        also restarts a paused webhook."""
        return dict(await self._call("POST", "/rfq/pricer/ping") or {})

    # -- executions -----------------------------------------------------------

    async def fetch_executions(self, *, status: Literal["open", "settled"] | None = None,
                               limit: int = EXECUTIONS_PAGE, after: str | None = None) -> list[RFQExecution]:
        """One page of the RFQs this pricer won, newest first. `after` is the
        last `rfq_id` of the previous page."""
        if not 1 <= limit <= EXECUTIONS_PAGE:
            raise ValueError(f"{VENUE}: limit is 1 to {EXECUTIONS_PAGE}")
        rows = await self._call("GET", "/rfq/executions", params={"status": status, "limit": limit, "after": after})
        return [RFQExecution.from_row(r) for r in rows or []]

    async def iter_executions(self, *, status: Literal["open", "settled"] | None = None,
                              page: int = EXECUTIONS_PAGE) -> AsyncIterator[RFQExecution]:
        """Every execution, newest first, page by page."""
        after: str | None = None
        while True:
            rows = await self.fetch_executions(status=status, limit=page, after=after)
            for row in rows:
                yield row
            if len(rows) < page:
                return
            after = rows[-1].rfq_id

    async def reconcile_executions(self, known: Iterable[str], *, page: int = 100,
                                   max_pages: int = 50) -> list[RFQExecution]:
        """The executions missing from `known` (rfq ids already held), newest
        first: pages from the newest until one already held is reached, as
        Novig's documentation recommends after a gap in the feed."""
        held = set(known)
        missing: list[RFQExecution] = []
        after: str | None = None
        for _ in range(max_pages):
            rows = await self.fetch_executions(limit=page, after=after)
            for row in rows:
                if row.rfq_id in held:
                    return missing
                missing.append(row)
            if len(rows) < page:
                return missing
            after = rows[-1].rfq_id
        return missing

    async def fetch_collateral(self) -> RFQCollateral:
        return RFQCollateral.from_row(await self._call("GET", "/rfq/collateral") or {})


# ---------------------------------------------------------------------------
# The pricer
# ---------------------------------------------------------------------------

PriceFn = Callable[[ParlayRFQ], "ParlayQuote | None | Awaitable[ParlayQuote | None]"]
ConfirmFn = Callable[[LastLook], "bool | Awaitable[bool]"]


@dataclass
class _Round:
    rfq: ParlayRFQ
    answer: "asyncio.Future[ParlayQuote | None]"
    at: float


@dataclass
class PricerStats:
    rounds: int = 0
    quoted: int = 0
    declined: int = 0
    late: int = 0
    """Rounds the pricing callback did not answer in time."""
    errors: int = 0
    last_looks: int = 0
    confirmed: int = 0
    rejected: int = 0


async def _maybe(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


class NovigPricer:
    """Prices Novig parlay RFQs over the WebSocket, webhooks, or both.

    ```python
    from decimal import Decimal
    from synpath.trading.credentials import load_novig_rfq
    from synpath.trading.novig_rfq import NovigPricer, ParlayQuote

    async def price(rfq):
        if rfq.legs > 4:
            return None                       # decline
        return ParlayQuote(price=Decimal("0.30"), max_wager=Decimal("200"))

    pricer = NovigPricer(price, credentials=load_novig_rfq())
    async with pricer.stream() as stream:      # the maker WebSocket
        async for event in stream:
            print(event)

    app = pricer.webhook_app()                 # or: uvicorn serves /quote, /confirm, /ping
    ```

    `price` gets each round as a `ParlayRFQ` and returns a `ParlayQuote`, or
    `None` to decline; it may be a coroutine. It has until the auction
    closes, less `margin` seconds for the answer's trip; a slower answer
    declines. `confirm` gets each last look as a `LastLook`, with the
    pricer's own quote on it, and returns whether to take the trade, within
    `confirm_timeout` seconds or the trade is rejected. Without one, a trade
    is confirmed when it matches the quote this pricer sent (`matches_quote`).
    """

    def __init__(
        self,
        price: PriceFn,
        *,
        confirm: ConfirmFn | None = None,
        credentials: NovigRFQCredentials | None = None,
        shared_secret: str | None = None,
        env: str | None = None,
        rfq: NovigRFQ | None = None,
        margin: float = 0.25,
        confirm_timeout: float = 0.75,
        confirm_rate: ConfirmRate | None = None,
        decline_below_min_wager: bool = True,
        remember: float = 300.0,
    ):
        self.price = price
        self.confirm = confirm
        self.credentials = credentials or NovigRFQCredentials()
        self.env = env or self.credentials.env
        self.shared_secret = shared_secret or self.credentials.shared_secret
        self._rfq = rfq
        self.margin = margin
        self.confirm_timeout = min(confirm_timeout, LAST_LOOK_SECONDS)
        self.confirm_rate = confirm_rate or ConfirmRate()
        self.decline_below_min_wager = decline_below_min_wager
        self.remember = remember
        self.stats = PricerStats()
        self._rounds: dict[str, _Round] = {}

    @property
    def rfq(self) -> NovigRFQ:
        """The REST client, made on first use."""
        if self._rfq is None:
            self._rfq = NovigRFQ(self.credentials, env=self.env)
        return self._rfq

    async def close(self) -> None:
        if self._rfq is not None:
            await self._rfq.close()

    # -- rounds ---------------------------------------------------------------

    def _prune(self) -> None:
        horizon = time.monotonic() - self.remember
        for key in [k for k, r in self._rounds.items() if r.at < horizon]:
            del self._rounds[key]

    def round_of(self, rfq_id: str) -> tuple[ParlayRFQ, ParlayQuote | None] | None:
        """The round as this pricer saw it, and its quote (`None` declined)."""
        held = self._rounds.get(rfq_id)
        if held is None or not held.answer.done() or held.answer.cancelled():
            return None
        return held.rfq, held.answer.result()

    async def quote(self, rfq: ParlayRFQ) -> ParlayQuote | None:
        """The answer to a round: the pricing callback's quote, checked, or
        `None` to decline. Priced once per `rfq_id`; a second transport
        delivering the same round gets the same answer."""
        held = self._rounds.get(rfq.rfq_id)
        if held is not None:
            return await asyncio.shield(held.answer)
        self._prune()
        answer: asyncio.Future = asyncio.get_running_loop().create_future()
        self._rounds[rfq.rfq_id] = _Round(rfq=rfq, answer=answer, at=time.monotonic())
        self.stats.rounds += 1
        try:
            result = await self._price(rfq)
        except BaseException:
            answer.set_result(None)
            raise
        answer.set_result(result)
        if result is None:
            self.stats.declined += 1
        else:
            self.stats.quoted += 1
        return result

    async def _price(self, rfq: ParlayRFQ) -> ParlayQuote | None:
        if self.confirm_rate.locked():
            return None
        budget = (rfq.deadline() - now_ms()) / 1000 - self.margin
        if budget <= 0:
            self.stats.late += 1
            return None
        try:
            quote = await asyncio.wait_for(_maybe(self.price(rfq)), timeout=budget)
        except asyncio.TimeoutError:
            self.stats.late += 1
            log.warning("novig rfq %s: the pricing callback took longer than %.2fs; declined", rfq.rfq_id, budget)
            return None
        except Exception:
            self.stats.errors += 1
            log.exception("novig rfq %s: the pricing callback raised; declined", rfq.rfq_id)
            return None
        if quote is None:
            return None
        try:
            quote = quote.checked()
        except InvalidOrder as exc:
            self.stats.errors += 1
            log.warning("novig rfq %s: %s; declined", rfq.rfq_id, exc)
            return None
        if self.decline_below_min_wager and quote.max_wager < rfq.min_wager:
            log.info("novig rfq %s: max_wager %s is below the round's minimum %s; declined",
                     rfq.rfq_id, quote.max_wager, rfq.min_wager)
            return None
        return quote

    # -- last looks -----------------------------------------------------------

    def with_round(self, look: LastLook) -> LastLook:
        """The last look with this pricer's round and quote on it."""
        seen = self.round_of(look.rfq_id)
        if seen is None:
            return look
        rfq, quote = seen
        return replace(look, rfq=rfq, quote=quote)

    async def last_look(self, look: LastLook) -> bool:
        """Whether to take a won trade: the confirm callback's answer within
        `confirm_timeout`, or the default policy. A slow, failing or absent
        answer rejects. Counted toward the confirm rate."""
        look = self.with_round(look)
        self.stats.last_looks += 1
        budget = min(self.confirm_timeout, (look.deadline() - now_ms()) / 1000 - 0.05)
        decision = False
        if budget > 0:
            try:
                if self.confirm is None:
                    decision = matches_quote(look)
                else:
                    decision = bool(await asyncio.wait_for(_maybe(self.confirm(look)), timeout=budget))
            except asyncio.TimeoutError:
                log.warning("novig rfq %s: the confirm callback took longer than %.2fs; rejected", look.rfq_id, budget)
            except Exception:
                self.stats.errors += 1
                log.exception("novig rfq %s: the confirm callback raised; rejected", look.rfq_id)
        self.confirm_rate.record(look.rfq_id, decision)
        if decision:
            self.stats.confirmed += 1
        else:
            self.stats.rejected += 1
        return decision

    async def sync_standing(self) -> PricerRegistration | None:
        """Read the registration and take Novig's confirm count and lock."""
        registration = await self.rfq.fetch_pricer()
        if registration is not None:
            self.confirm_rate.sync(registration.standing, registration.lock)
        return registration

    # -- webhooks -------------------------------------------------------------

    async def handle_webhook(self, route: str, raw_body: bytes,
                             signature: str | None) -> tuple[int, dict[str, Any] | None]:
        """One webhook request as `(status, JSON body or None)`, for any web
        framework. `route` is `quote`, `confirm` or `ping`; `raw_body` the
        bytes exactly as received; `signature` the `X-Novig-Signature`
        header. A bad signature is `401` and the body is never read."""
        if not self.shared_secret:
            raise CredentialsMissing(f"{VENUE}: webhooks need the shared secret POST /rfq/pricer returned; "
                                     "set NOVIG_RFQ_SHARED_SECRET")
        if not signing.verify_webhook(self.shared_secret, raw_body, signature):
            return 401, {"error": "invalid signature"}
        received = now_ms()
        name = route.strip("/").rsplit("/", 1)[-1]
        if name == "ping":
            return 200, {"ok": True}
        try:
            body = json.loads(raw_body)
        except ValueError:
            return 400, {"error": "invalid JSON"}
        if not isinstance(body, dict) or "rfq_id" not in body:
            return 400, {"error": "missing rfq_id"}
        if name == "quote":
            quote = await self.quote(ParlayRFQ.from_webhook(body, received_at=received))
            return (204, None) if quote is None else (200, quote.webhook_body())
        if name == "confirm":
            return 200, {"confirmed": await self.last_look(LastLook.from_webhook(body, received_at=received))}
        return 404, {"error": f"no route {route}"}

    def webhook_router(self) -> Any:
        """A FastAPI router with `POST /quote`, `/confirm` and `/ping`, to
        include under the path the webhook URL was registered with."""
        from fastapi import APIRouter
        from fastapi.responses import JSONResponse, Response

        router = APIRouter()

        def endpoint_for(route: str) -> Callable[[Any], Awaitable[Any]]:
            # A plain Starlette endpoint: the raw body is read, never parsed first.
            async def endpoint(request: Any) -> Any:
                status, body = await self.handle_webhook(
                    route, await request.body(), request.headers.get(signing.SIGNATURE_HEADER),
                )
                return Response(status_code=status) if body is None else JSONResponse(body, status_code=status)
            return endpoint

        for route in ("quote", "confirm", "ping"):
            router.add_route(f"/{route}", endpoint_for(route), methods=["POST"], include_in_schema=False)
        return router

    def webhook_app(self, *, prefix: str = "") -> Any:
        """A FastAPI app serving the pricer's webhooks under `prefix` (the
        path of the registered webhook URL, like `/novig`). Run it with
        uvicorn behind HTTPS."""
        from fastapi import FastAPI

        app = FastAPI(title="Novig RFQ pricer", docs_url=None, redoc_url=None, openapi_url=None)
        app.include_router(self.webhook_router(), prefix=prefix.rstrip("/"))
        return app

    # -- the socket -----------------------------------------------------------

    def stream(self, **kwargs: Any) -> Any:
        """The maker WebSocket, answering rounds with this pricer: a
        `synpath.ws.novig_rfq.NovigPricerStream`. Iterate it for every frame
        as an event."""
        from ..ws.novig_rfq import NovigPricerStream

        return NovigPricerStream(self, **kwargs)
