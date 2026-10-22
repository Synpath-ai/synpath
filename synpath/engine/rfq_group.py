"""Cross-venue RFQ: one request, every venue that lists it, one ranked list.

`docs/rfq-combo.md`, "Cross-venue RFQ", is the design; this is the code
behind `engine.rfq_groups` and the `Engine` methods that delegate to it. It
sits on the RFQ desk (`synpath.engine.rfq`): every member is an ordinary
RFQ of the desk's, created, watched, cancelled, accepted and recovered the
way any RFQ is. What a group adds is which venues to ask, one ranking of
everything they answer, and the rule that only one of them trades.

**Instrument resolution.** A group is on one of three things:

* a bucket (`bucket:<id>`): one member per enabled bucket member, on that
  member's market, with its `flip`;
* a combo: legs (market id and outcome, each on any venue or a bucket),
  or a combo's own market id the engine holds the legs of. Each venue that
  has `combos` and `rfq_taker` gets each leg mapped onto its own market (the
  leg itself on its own venue, a bucket's member there, else the hosted
  matching service, `synpath.matching.match_market`, with its `side_map`),
  then its eligibility checked (Kalshi: an open collection whose
  `associated_event_tickers` take every leg's event; Polymarket and
  Polymarket US: the venue's combo-eligible list), then its combo created
  (Kalshi with that `collection_id`). A venue where a leg has no market or is
  not eligible is skipped, with the reason;
* a plain market: the market's own venue, and with `cross_venue=True` its
  matched market on every other RFQ venue, with the match's `side_map`.

**Prices in the group's terms.** A member whose YES is the group's NO (a
flipped bucket member, a match whose `side_map` swaps the sides) is asked
and accepted on the other side: buying the group's YES is buying that
member's NO, so its quote's bid `b` is the group's ask `1 - b`. Every quote
is ranked in the group's YES terms after the venue's taker fee for the size
the accept would trade (`fetch_fee_estimate`, the venue's own schedule, as
the router prices a bucket), and an amount-sized group is ranked by the
contracts the amount buys after that fee.

**One accept per group, in the journal.** An accept claims the group with
an intent keyed `rfq-group-accept:{group}:{attempt}`. A claim is refused
while any earlier attempt is not `failed`, `rejected` or `lost`, and the
key is re-read after it is written, so two accepts cannot both hold it.
Only then are the other members cancelled (a venue with no requester
cancel, Polymarket global, is left to expire and recorded so), and the
chosen quote is accepted through `RfqDesk.accept_quote`: risk, the journal,
following and recovery are the desk's. An accept that fails releases the
claim; the group can be accepted again on a quote still open, and what was
cancelled stays cancelled. A quote on a member RFQ cannot be accepted
around the group (`RfqDesk.accept_quote` refuses it).

**What a restart finds.** The groups not yet final are in the cursors
table (`rfq:groups`), saved at every step. `restore()` reads them back and
re-seeds any member RFQ the desk's working set lacks (`restore_rfqs`). A
member whose create was in doubt is matched to its create intent once the
desk has resolved it. A claim whose accept was never sent (the process died
between the cancels and the accept) finishes the cancels it planned and
is released, nothing traded; one whose accept was sent is followed by the
desk and the group follows its intent.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Literal, Mapping

from pydantic import Field

from .. import ids
from ..bucket import Bucket, bucket_id_of, is_bucket_id
from ..errors import BadRequest, MarketNotFound
from ..trading.errors import InvalidOrder, RfqNotFound, RiskRejected
from ..trading.money import FALLBACK_QUANTITY_STEP, contracts_for_amount, outcome_price
from ..trading.types import (
    AcceptQuote, Action, Combo, ComboLeg, ComboMarket, OrderType, Outcome, QuoteStatus, Rfq, RfqQuote, Side, yes_side,
)
from ..types import MarketLink, _Base
from .storage_types import Intent, IntentState
from .submissions import SubmissionInDoubt

if TYPE_CHECKING:
    from .engine import Engine

log = logging.getLogger("synpath.engine.rfq_group")

ZERO = Decimal("0")
ONE = Decimal("1")

GROUPS_CURSOR = "rfq:groups"
ELIGIBLE_PAGES = 20
"""Pages of a venue's combo-eligible list (or of its collections) read
looking for a group's legs before a leg not yet seen counts as ineligible."""
ELIGIBLE_TTL_S = 60.0
"""How long a venue's combo-eligible list is trusted between groups."""
MATCH_TTL_S = 60.0
"""How long a matching answer (a listing, or none) is reused: a preview and
the group it leads to ask the same legs within seconds."""

Matcher = Callable[[str, str], Awaitable[MarketLink | None]]
"""`matcher(anchor, venue)` -> the other venue's listing of the same
proposition, or `None` when it has none. Raises `MarketNotFound` when the
anchor is not a listing the service knows."""

GroupStatus = Literal["creating", "open", "accepting", "accepted", "executed", "canceled", "expired", "failed"]
MemberStatus = Literal["creating", "open", "failed", "canceled", "lapsing", "closed", "accepting", "accepted",
                       "executed"]
FINAL = frozenset({"executed", "canceled", "expired", "failed"})
RELEASED = (IntentState.FAILED, IntentState.REJECTED, IntentState.LOST)
"""A claim in one of these states no longer holds its group."""


def _now_ms() -> int:
    return int(time.time() * 1000)


def _opposite(side: Side) -> Side:
    return Side.SELL if side == Side.BUY else Side.BUY


def _flip_outcome(outcome: str) -> Outcome:
    return "no" if outcome == "yes" else "yes"  # type: ignore[return-value]


def lists_combo_markets(adapter: Any) -> bool:
    """The adapter answers `fetch_combo_eligible_markets` itself (the base
    class's default is `NotSupported`)."""
    from ..trading.base import TradingExchange

    method = getattr(type(adapter), "fetch_combo_eligible_markets", None)
    return method is not None and method is not TradingExchange.fetch_combo_eligible_markets


def hosted_matcher(*, base_url: str | None = None, client: Any = None, api_key: str | None = None) -> Matcher:
    """The hosted matching service (`synpath.matching.match_market`) as a
    group's matcher: one blocking HTTP read per leg and venue, in a thread."""
    from ..matching import match_market

    async def match(anchor: str, venue: str) -> MarketLink | None:
        found = await asyncio.to_thread(match_market, anchor, venue=venue, base_url=base_url, client=client,
                                        api_key=api_key)
        return found.matched

    return match


# ---------------------------------------------------------------------------
# The model
# ---------------------------------------------------------------------------

class GroupInstrument(_Base):
    """What a group asks about."""

    kind: Literal["market", "bucket", "combo"]
    market_id: str | None = Field(default=None, description="The market, `bucket:<id>`, or the combo's own id "
                                                            "the legs came from")
    legs: list[ComboLeg] | None = Field(default=None, description="A combo's legs, as given (any venue, or a bucket)")


class GroupMember(_Base):
    """One venue's RFQ in a group."""

    venue: str
    instrument_id: str | None = Field(default=None, description="The market or combo asked about on this venue")
    flip: bool = Field(default=False, description="This member's YES is the group's NO")
    via: Literal["anchor", "bucket", "matching", "combo"] = "anchor"
    legs: list[ComboLeg] | None = Field(default=None, description="A combo's legs on this venue, after mapping")
    combo: Combo | None = None
    collection_id: str | None = None
    eligibility: str | None = Field(default=None, description="How the legs were found eligible")
    create_key: str | None = Field(default=None, description="The member RFQ's create intent")
    rfq_id: str | None = None
    rfq: dict[str, Any] | None = None
    status: MemberStatus = "creating"
    detail: str | None = None
    accept_key: str | None = Field(default=None, description="The accept intent, once this member's quote is taken")

    @property
    def key(self) -> str:
        return f"{self.venue}:{self.rfq_id}"


class SkippedVenue(_Base):
    """A venue the group does not ask, and why."""

    venue: str
    code: str = Field(description="`excluded`, `forbidden`, `not_configured`, `no_rfq`, `no_combos`, `no_match`, "
                                  "`matching_error`, `missing_leg`, `ineligible_leg`, `duplicate_leg`, "
                                  "`one_sided_size`, `member_disabled`")
    reason: str
    leg: str | None = Field(default=None, description="The leg (as given) that could not be placed here")


class GroupQuote(_Base):
    """One quote, in the group's terms. Prices are YES prices of the group's
    instrument (a flipped member's are turned over); `fee` is the venue's
    taker fee for `quantity` contracts."""

    quote_id: str
    venue: str
    rfq_id: str
    market_id: str | None = None
    flip: bool = False
    status: QuoteStatus
    maker: str | None = None
    raw_bid: Decimal | None = None
    raw_ask: Decimal | None = None
    raw_price: Decimal | None = Field(default=None, description="The member's own YES price for the requested side: "
                                                                "what the venue trades at")
    price: Decimal | None = Field(default=None, description="The same, as the group's YES price")
    outcome_price: Decimal | None = Field(default=None, description="What one contract of the requested outcome "
                                                                    "costs (buy) or fetches (sell), before fees")
    available: Decimal | None = Field(default=None, description="Contracts the quote offers on that side")
    quantity: Decimal | None = Field(default=None, description="Contracts an accept would trade")
    fee: Decimal | None = Field(default=None, description="Taker fee for `quantity`; null when the venue would "
                                                          "not price it (counted as zero)")
    fee_per_contract: Decimal | None = None
    effective_price: Decimal | None = Field(default=None, description="`price` plus the fee per contract on a YES "
                                                                      "buy, minus it on a YES sell")
    effective_outcome_price: Decimal | None = Field(default=None, description="`outcome_price` with the fee per "
                                                                              "contract: the ranking key")
    partial: bool = Field(default=False, description="The quote offers fewer contracts than asked")
    rank: int | None = Field(default=None, description="1 is best; null for a quote that cannot be accepted now")
    note: str | None = None
    expires_at: int | None = Field(default=None, description="When the quote stops being acceptable, ms, as the "
                                                             "venue says: the quote's own expiry (Polymarket "
                                                             "`expires_at`), else its RFQ's. Null where the venue "
                                                             "gives none (Kalshi, Polymarket US): live until the "
                                                             "venue closes it")
    expires_from: Literal["quote", "rfq"] | None = Field(default=None, description="Which expiry `expires_at` is")
    confirm_by: int | None = Field(default=None, description="After an accept: the maker's last look ends, ms")
    execute_by: int | None = Field(default=None, description="After a confirm: the venue executes by, ms")


class RfqGroup(_Base):
    """A cross-venue RFQ: one `Rfq` per venue, one ranked list of quotes."""

    id: str
    instrument: GroupInstrument
    quantity: Decimal | None = None
    amount: Decimal | None = None
    outcome: Outcome = "yes"
    action: Action = "buy"
    cross_venue: bool = False
    book: str | None = None
    trader: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)
    members: list[GroupMember] = Field(default_factory=list)
    skipped: list[SkippedVenue] = Field(default_factory=list)
    quotes: list[GroupQuote] = Field(default_factory=list, description="Best first; unrankable quotes last")
    status: GroupStatus = "creating"
    accept_attempts: int = 0
    claim_key: str | None = Field(default=None, description="The live or last accept claim")
    accepted_quote_id: str | None = None
    accepted_venue: str | None = None
    order_id: str | None = None
    failures: list[dict[str, Any]] = Field(default_factory=list)
    expires_at: int | None = Field(default=None, description="The soonest venue expiry among the quotes that can "
                                                             "be accepted now, ms; null when none gives one")
    created_at: int = Field(default_factory=_now_ms)
    updated_at: int = Field(default_factory=_now_ms)

    @property
    def side(self) -> Side:
        """The YES-side the requester takes, in the group's terms."""
        return yes_side(self.outcome, self.action)

    @property
    def final(self) -> bool:
        return self.status in FINAL

    @property
    def market_id(self) -> str:
        return self.instrument.market_id or f"rfq-group:{self.id}"

    def venues(self) -> set[str]:
        return {m.venue for m in self.members}

    def member_of(self, venue: str, rfq_id: str | None) -> GroupMember | None:
        for member in self.members:
            if member.venue == venue and member.rfq_id == rfq_id:
                return member
        return None

    def best(self) -> GroupQuote | None:
        ranked = [q for q in self.quotes if q.rank is not None]
        return ranked[0] if ranked else None


class PreviewVenue(_Base):
    """One venue a group would ask, as `preview` resolves it: nothing created."""

    venue: str
    via: Literal["anchor", "bucket", "matching", "combo"]
    instrument_id: str | None = Field(default=None, description="The market asked (a plain market or bucket "
                                                                "member); a combo has none until it is built")
    flip: bool = False
    legs: list[ComboLeg] | None = Field(default=None, description="A combo's legs on this venue, after mapping")
    collection_id: str | None = None
    eligibility: str | None = None
    leg_prices: list[Decimal | None] | None = Field(
        default=None, description="Each mapped leg's price in its outcome's terms (YES ask, or 1 - YES bid), "
                                  "from the server's catalogue; filled in by the HTTP route")
    combined_price: Decimal | None = Field(default=None, description="The leg prices multiplied: an estimate "
                                                                     "of the combo's price, not a quote")


class GroupPreview(_Base):
    """Which venues a group on this instrument would ask, and why the others
    would be skipped: `create_group`'s planning without creating anything."""

    instrument: GroupInstrument
    offered: list[PreviewVenue] = Field(default_factory=list)
    skipped: list[SkippedVenue] = Field(default_factory=list)


class GroupAccept(_Base):
    """What an accept on a group did, as far as it is known yet."""

    group: RfqGroup
    quote: RfqQuote
    venue: str
    client_order_id: str = Field(description="The member accept's intent key: follow its events by it")
    claim_key: str = Field(description="The group's claim intent")


# ---------------------------------------------------------------------------
# The desk
# ---------------------------------------------------------------------------

class RfqGroupDesk:
    """Cross-venue RFQ groups, attached as `engine.rfq_groups`."""

    def __init__(self, engine: "Engine", *, matcher: Matcher | None = None):
        self.engine = engine
        self.matcher: Matcher = matcher or hosted_matcher()
        self.groups: dict[str, RfqGroup] = {}
        self._by_rfq: dict[str, str] = {}
        """`venue:rfq_id` -> the group it is a member of."""
        self._busy: set[str] = set()
        """Groups an accept or a cancel is working on in this process."""
        self._fees: dict[tuple[str, str, str, Decimal, Decimal], Decimal | None] = {}
        self._matches: dict[tuple[str, str], tuple[float, MarketLink | None]] = {}
        self._eligible_lists: dict[str, tuple[float, dict[str, ComboMarket], bool]] = {}

    @property
    def desk(self) -> Any:
        return self.engine.rfq

    # -- lookups --------------------------------------------------------------

    def group_of(self, venue: str, rfq_id: str | None) -> str | None:
        """The group an RFQ of ours is a member of, if any."""
        return self._by_rfq.get(f"{venue}:{rfq_id}") if rfq_id else None

    async def get(self, group_id: str) -> RfqGroup:
        """A group the engine holds, else the last state the journal recorded."""
        if group_id in self.groups:
            return self.groups[group_id]
        for event in await self.engine.journal.events_page(prefixes=["rfq_group."], keys=[f"rfq_group:{group_id}"],
                                                           limit=50):
            body = event.payload.get("group")
            if isinstance(body, dict):
                return RfqGroup.model_validate(body)
        raise RfqNotFound(f"no RFQ group {group_id!r}")

    def list(self, *, status: str | None = None) -> list[RfqGroup]:
        rows = [g for g in self.groups.values() if status is None or g.status == status]
        return sorted(rows, key=lambda g: -g.created_at)

    # -- persistence and events -----------------------------------------------

    async def _save(self, group: RfqGroup) -> None:
        group.updated_at = _now_ms()
        self.groups[group.id] = group
        for member in group.members:
            if member.rfq_id:
                self._by_rfq[member.key] = group.id
        live = [g.model_dump(mode="json") for g in self.groups.values() if not g.final]
        await self.engine.journal.set_cursor(GROUPS_CURSOR, json.dumps(live, default=str))

    async def _publish(self, kind: str, group: RfqGroup, **extra: Any) -> None:
        await self.engine.bus.publish(kind, {"group_id": group.id, "status": group.status, **extra,
                                             "group": group.model_dump(mode="json")}, key=f"rfq_group:{group.id}")

    # -- create ---------------------------------------------------------------

    async def create_group(
        self, instrument_id: str | None = None, *, legs: list[ComboLeg] | None = None,
        quantity: Decimal | None = None, amount: Decimal | None = None, outcome: Outcome = "yes",
        action: Action = "buy", cross_venue: bool = False, venues: list[str] | None = None,
        exclude_venues: list[str] | None = None, refused_venues: Mapping[str, str] | None = None,
        book: str | None = None, trader: str | None = None, tags: dict[str, str] | None = None,
    ) -> RfqGroup:
        """Ask every venue that lists the instrument (module doc). Returns the
        group with its members (one RFQ per venue), the venues skipped and
        why, and the quotes read so far. A venue whose create fails is a
        `failed` member; the group stays open with the rest. `BadRequest`
        (with `skipped` on the exception) when no venue can be asked."""
        await self.desk._lease()
        if (quantity is None) == (amount is None):
            raise BadRequest("size an RFQ group by quantity (contracts) or amount (money), exactly one")
        if (instrument_id is None) == (not legs):
            raise BadRequest("an RFQ group is on an instrument_id (a market, a bucket, a combo) or on legs, exactly one")
        instrument = await self._instrument(instrument_id, legs)
        group = RfqGroup(id=uuid.uuid4().hex, instrument=instrument, quantity=quantity, amount=amount,
                         outcome=outcome, action=action, cross_venue=cross_venue, book=book, trader=trader,
                         tags=dict(tags or {}))
        group.members, group.skipped = await self._plan(group, include=venues, exclude=exclude_venues or [],
                                                        refused=dict(refused_venues or {}))
        if not group.members:
            exc = BadRequest("no venue can quote this: " + "; ".join(f"{s.venue}: {s.reason}" for s in group.skipped)
                             if group.skipped else "no venue with RFQ is configured")
            exc.skipped = [s.model_dump(mode="json") for s in group.skipped]  # type: ignore[attr-defined]
            raise exc
        key = f"rfq-group:{group.id}"
        first = group.members[0].venue
        request = self.desk._record(market_id=group.market_id, key=key, account=self.desk._account(first),
                                    type=OrderType.RFQ_TAKER, quantity=quantity, book=book, trader=trader, tags=tags,
                                    params={"group_id": group.id, "operation": "rfq_group_create",
                                            "venues": [m.venue for m in group.members]})
        await self._save(group)
        await self.desk._begin(request, key=key, venue=first, account=self.desk._account(first),
                               operation="rfq_group_create", target=group.id)
        self._busy.add(group.id)
        refusals: list[Exception] = []
        try:
            for index, member in enumerate(group.members):
                refusal = await self._create_member(group, member, index)
                if refusal is not None:
                    refusals.append(refusal)
            await self._settle_create(group, key)
        finally:
            self._busy.discard(group.id)
        if group.status == "failed" and refusals and len(refusals) == len(group.members):
            # Every venue refused it before sending (the halt, a paused book,
            # a size limit): the caller gets the refusal, the journal the group.
            raise refusals[0]
        await self.refresh(group.id)
        return group

    async def _create_member(self, group: RfqGroup, member: GroupMember, index: int) -> Exception | None:
        """Create one member's combo (where it is one) and RFQ. Returns the
        risk refusal when the engine's own rules refused it."""
        venue = member.venue
        adapter = self.engine.adapters[venue]
        try:
            if member.via == "combo":
                combo = await self.desk.create_combo(member.legs or [], venue=venue, collection_id=member.collection_id)
                member.combo, member.instrument_id = combo, combo.market_id
            member.create_key = f"rfq-create:{venue}:{group.id}-{index}"
            await self._save(group)
            outcome, action = self._ask_side(adapter, group, member)
            rfq = await self.desk.create_rfq(
                member.instrument_id, quantity=group.quantity, amount=group.amount, outcome=outcome, action=action,
                combo=member.combo, venue=venue, book=group.book, trader=group.trader,
                tags={**group.tags, "rfq_group": group.id}, key=member.create_key)
        except SubmissionInDoubt as exc:
            member.detail = f"in doubt: {exc}"
            await self._save(group)
            return None
        except Exception as exc:  # one venue failing must not stop the others
            member.status, member.detail = "failed", f"{type(exc).__name__}: {exc}"
            group.failures.append({"venue": venue, "stage": "create", "error": member.detail, "ts": _now_ms()})
            await self._save(group)
            return exc if isinstance(exc, RiskRejected) else None
        self._member_open(group, member, rfq)
        await self._save(group)
        return None

    def _member_open(self, group: RfqGroup, member: GroupMember, rfq: Rfq) -> None:
        member.rfq_id, member.rfq, member.status = rfq.id, rfq.model_dump(mode="json"), "open"
        member.detail = None
        self._by_rfq[member.key] = group.id

    async def _settle_create(self, group: RfqGroup, key: str) -> None:
        """The group's create intent once every member is created, failed or
        in doubt: `sent` while any member stands, `rejected` when none does."""
        standing = [m for m in group.members if m.status != "failed"]
        if group.status == "creating":
            group.status = "open" if standing else "failed"
        await self.engine.journal.mark_intent(
            key, IntentState.SENT if standing else IntentState.REJECTED, order_id=group.id,
            detail=f"{len([m for m in group.members if m.status == 'open'])} open, "
                   f"{len([m for m in group.members if m.status == 'failed'])} failed, "
                   f"{len([m for m in group.members if m.status == 'creating'])} in doubt")
        await self._save(group)
        await self._publish("rfq_group.created", group)

    def _ask_side(self, adapter: Any, group: RfqGroup, member: GroupMember) -> tuple[str | None, str | None]:
        """What the member RFQ asks: nothing on a two-sided venue; on one that
        asks a side (Polymarket global), the YES side the group takes there."""
        if not getattr(adapter, "rfq_one_sided", False):
            return None, None
        side = group.side if not member.flip else _opposite(group.side)
        return "yes", "buy" if side == Side.BUY else "sell"

    async def preview(
        self, instrument_id: str | None = None, *, legs: list[ComboLeg] | None = None,
        quantity: Decimal | None = None, amount: Decimal | None = None, outcome: Outcome = "yes",
        action: Action = "buy", cross_venue: bool = False, venues: list[str] | None = None,
        exclude_venues: list[str] | None = None, refused_venues: Mapping[str, str] | None = None,
    ) -> GroupPreview:
        """What `create_group` would ask, without creating anything: each
        venue's mapped market or legs (matching, eligibility, Kalshi's
        collection) or why it would be skipped. Reads only (the matching
        service, the venues' eligibility lists); nothing is journaled or sent.
        Unsized, it plans a buy by amount, as a combo slip asks."""
        if (instrument_id is None) == (not legs):
            raise BadRequest("a preview is on an instrument_id (a market, a bucket, a combo) or on legs, exactly one")
        if quantity is None and amount is None:
            amount = ONE
        instrument = await self._instrument(instrument_id, legs)
        group = RfqGroup(id="preview", instrument=instrument, quantity=quantity, amount=amount,
                         outcome=outcome, action=action, cross_venue=cross_venue)
        members, skipped = await self._plan(group, include=venues, exclude=exclude_venues or [],
                                            refused=dict(refused_venues or {}))
        offered = [PreviewVenue(venue=m.venue, via=m.via, instrument_id=m.instrument_id, flip=m.flip, legs=m.legs,
                                collection_id=m.collection_id, eligibility=m.eligibility) for m in members]
        return GroupPreview(instrument=instrument, offered=offered, skipped=skipped)

    async def _instrument(self, instrument_id: str | None, legs: list[ComboLeg] | None) -> GroupInstrument:
        if legs:
            if len(legs) < 2:
                raise BadRequest("a combo needs at least two legs")
            return GroupInstrument(kind="combo", legs=list(legs))
        assert instrument_id is not None
        if is_bucket_id(instrument_id):
            return GroupInstrument(kind="bucket", market_id=instrument_id)
        combo = await self.desk.combo(instrument_id)
        if combo is not None:
            return GroupInstrument(kind="combo", market_id=instrument_id, legs=list(combo.legs))
        ids.venue_of(instrument_id)   # a Synpath id, or BadRequest
        return GroupInstrument(kind="market", market_id=instrument_id)

    # -- planning: which venues, on which markets ------------------------------

    async def _plan(self, group: RfqGroup, *, include: list[str] | None, exclude: list[str],
                    refused: dict[str, str]) -> tuple[list[GroupMember], list[SkippedVenue]]:
        adapters = self.engine.adapters
        for name in [*(include or []), *exclude]:
            if name not in adapters:
                raise BadRequest(f"this engine does not trade {name}")
        skipped: list[SkippedVenue] = []
        candidates: list[str] = []
        for venue, adapter in adapters.items():
            if not adapter.has.get("rfq_taker"):
                continue
            if venue in exclude or (include is not None and venue not in include):
                skipped.append(SkippedVenue(venue=venue, code="excluded", reason="excluded by the request"))
            elif venue in refused:
                skipped.append(SkippedVenue(venue=venue, code="forbidden", reason=refused[venue]))
            else:
                candidates.append(venue)
        kind = group.instrument.kind
        if kind == "bucket":
            members = await self._plan_bucket(group, candidates, skipped)
        elif kind == "combo":
            members = await self._plan_combo(group, candidates, skipped)
        else:
            members = await self._plan_market(group, candidates, skipped)
        out: list[GroupMember] = []
        for member in members:
            adapter = adapters[member.venue]
            if getattr(adapter, "rfq_one_sided", False):
                side = group.side if not member.flip else _opposite(group.side)
                if (side == Side.BUY) != (group.amount is not None):
                    skipped.append(SkippedVenue(
                        venue=member.venue, code="one_sided_size",
                        reason=f"{member.venue} asks a YES buy by amount and a YES sell by quantity; this group is a "
                               f"YES {side.value} there by {'amount' if group.amount is not None else 'quantity'}"))
                    continue
            out.append(member)
        return out, skipped

    def _usable(self, venue: str, candidates: list[str], skipped: list[SkippedVenue], *, what: str) -> bool:
        if venue in candidates:
            return True
        if any(s.venue == venue for s in skipped):
            return False
        adapter = self.engine.adapters.get(venue)
        if adapter is None:
            skipped.append(SkippedVenue(venue=venue, code="not_configured", reason=f"{what}: no adapter for {venue}"))
        else:
            skipped.append(SkippedVenue(venue=venue, code="no_rfq", reason=f"{what}: {venue} has no RFQ"))
        return False

    async def _plan_market(self, group: RfqGroup, candidates: list[str],
                           skipped: list[SkippedVenue]) -> list[GroupMember]:
        anchor = group.instrument.market_id or ""
        home = ids.venue_of(anchor)
        members = []
        if self._usable(home, candidates, skipped, what=anchor):
            members.append(GroupMember(venue=home, instrument_id=anchor, via="anchor"))
        if group.cross_venue:
            for venue in candidates:
                if venue == home:
                    continue
                link, reason, code = await self._match(anchor, venue)
                if link is None:
                    skipped.append(SkippedVenue(venue=venue, code=code, reason=reason, leg=anchor))
                    continue
                members.append(GroupMember(venue=venue, instrument_id=link.id, via="matching",
                                           flip=link.side_map.get("yes") == "no"))
        return members

    async def _bucket(self, market_id: str) -> Bucket:
        row = await self.engine.journal.bucket(bucket_id_of(market_id))
        if row is None:
            raise BadRequest(f"{market_id}: no such bucket in this journal")
        bucket = Bucket.model_validate(row)
        if bucket.status != "active":
            raise BadRequest(f"{market_id}: the bucket is {bucket.status}")
        return bucket

    async def _plan_bucket(self, group: RfqGroup, candidates: list[str],
                           skipped: list[SkippedVenue]) -> list[GroupMember]:
        bucket = await self._bucket(group.instrument.market_id or "")
        members = []
        for member in bucket.members:
            venue = ids.venue_of(member.market_id)
            if not member.enabled:
                skipped.append(SkippedVenue(venue=venue, code="member_disabled", leg=member.market_id,
                                            reason=f"{member.market_id} is switched off in the bucket"))
                continue
            if self._usable(venue, candidates, skipped, what=member.market_id):
                members.append(GroupMember(venue=venue, instrument_id=member.market_id, flip=member.flip,
                                           via="bucket"))
        return members

    async def _match(self, anchor: str, venue: str) -> tuple[MarketLink | None, str, str]:
        held = self._matches.get((anchor, venue))
        now = time.monotonic()
        try:
            if held is not None and now - held[0] <= MATCH_TTL_S:
                link = held[1]
            else:
                link = await self.matcher(anchor, venue)
                self._matches[(anchor, venue)] = (now, link)
        except MarketNotFound as exc:
            return None, f"the matching service does not know {anchor} ({exc})", "no_match"
        except Exception as exc:
            return None, f"matching {anchor} on {venue} failed: {type(exc).__name__}: {exc}", "matching_error"
        if link is None or link.venue != venue:
            return None, f"{venue} lists nothing matching {anchor}", "no_match"
        return link, "", ""

    async def _map_leg(self, leg: ComboLeg, venue: str) -> tuple[ComboLeg | None, str, str]:
        """A leg as `venue` names it, or why it has none there."""
        if is_bucket_id(leg.market_id):
            bucket = await self._bucket(leg.market_id)
            for member in bucket.enabled_members():
                if ids.venue_of(member.market_id) == venue:
                    outcome = _flip_outcome(leg.outcome) if member.flip else leg.outcome
                    return ComboLeg(market_id=member.market_id, outcome=outcome), "", ""
            return None, f"bucket {leg.market_id} has no enabled member on {venue}", "missing_leg"
        if ids.venue_of(leg.market_id) == venue:
            return leg, "", ""
        link, reason, code = await self._match(leg.market_id, venue)
        if link is None:
            return None, reason, "missing_leg" if code == "no_match" else code
        outcome = leg.outcome if link.side_map.get(leg.outcome, leg.outcome) == leg.outcome else _flip_outcome(leg.outcome)
        return ComboLeg(market_id=link.id, outcome=outcome), "", ""

    async def _plan_combo(self, group: RfqGroup, candidates: list[str],
                          skipped: list[SkippedVenue]) -> list[GroupMember]:
        legs = group.instrument.legs or []
        members = []
        for venue in candidates:
            adapter = self.engine.adapters[venue]
            if not adapter.has.get("combos"):
                skipped.append(SkippedVenue(venue=venue, code="no_combos", reason=f"{venue} has no combos"))
                continue
            mapped: list[ComboLeg] = []
            missing: SkippedVenue | None = None
            for leg in legs:
                found, reason, code = await self._map_leg(leg, venue)
                if found is None:
                    missing = SkippedVenue(venue=venue, code=code, reason=reason, leg=leg.market_id)
                    break
                mapped.append(found)
            if missing is not None:
                skipped.append(missing)
                continue
            if len({leg.market_id for leg in mapped}) != len(mapped):
                skipped.append(SkippedVenue(venue=venue, code="duplicate_leg",
                                            reason=f"two legs are the same market on {venue}"))
                continue
            try:
                mapped, collection, how = await self._eligible(venue, adapter, mapped, legs)
            except _Ineligible as exc:
                skipped.append(SkippedVenue(venue=venue, code="ineligible_leg", reason=str(exc), leg=exc.leg))
                continue
            members.append(GroupMember(venue=venue, via="combo", legs=mapped, collection_id=collection,
                                       eligibility=how))
        return members

    # -- eligibility -----------------------------------------------------------

    async def _eligible(self, venue: str, adapter: Any, legs: list[ComboLeg],
                        given: list[ComboLeg]) -> tuple[list[ComboLeg], str | None, str]:
        """The legs as the venue's combo takes them, the collection to build
        it in (Kalshi), and how eligibility was decided. `_Ineligible` names
        the first leg the venue will not take."""
        if callable(getattr(adapter, "fetch_combo_collections", None)):
            return await self._eligible_collections(venue, adapter, legs, given)
        if lists_combo_markets(adapter):
            return await self._eligible_listed(venue, adapter, legs, given)
        return legs, None, "unchecked: the venue lists no eligibility"

    async def _leg_event(self, adapter: Any, leg: ComboLeg) -> str:
        if leg.event_id:
            return ids.split(leg.event_id)[1]
        reader = getattr(adapter, "_event_of", None)
        if callable(reader):
            return str(await reader(leg))
        ticker = ids.split(leg.market_id)[1]
        return ticker.rsplit("-", 1)[0]

    async def _eligible_collections(self, venue: str, adapter: Any, legs: list[ComboLeg],
                                    given: list[ComboLeg]) -> tuple[list[ComboLeg], str | None, str]:
        events = [await self._leg_event(adapter, leg) for leg in legs]
        legs = [leg.model_copy(update={"event_id": event}) for leg, event in zip(legs, events)]
        wanted = list(dict.fromkeys(events))
        rows: list[dict[str, Any]] = []
        cursor: str | None = None
        for _ in range(ELIGIBLE_PAGES):
            page = await adapter.fetch_combo_collections(status="open", event_id=wanted[0], limit=200, cursor=cursor)
            rows.extend(page)
            cursor = page.next_cursor
            if not cursor or not page:
                break
        fits: list[str] = []
        seen: set[str] = set()
        for row in rows:
            held = set(row.get("associated_event_tickers") or [])
            seen |= held
            low, high = row.get("size_min"), row.get("size_max")
            # Kalshi lists `size_max` 0 (or none) for a collection with no maximum.
            if set(wanted) <= held and (low is None or len(legs) >= int(low)) and \
                    (high is None or int(high) <= 0 or len(legs) <= int(high)):
                fits.append(str(row.get("collection_ticker")))
        if not fits:
            if not rows:
                raise _Ineligible(f"no open collection on {venue} takes event {wanted[0]}", leg=given[0].market_id)
            for leg, mine, event in zip(given, legs, events):
                if event not in seen:
                    raise _Ineligible(f"event {event} ({mine.market_id}) is in no open collection on {venue} with "
                                      f"event {wanted[0]}", leg=leg.market_id)
            raise _Ineligible(f"no open collection on {venue} takes these {len(legs)} legs together "
                              f"(events {', '.join(wanted)})", leg=given[-1].market_id)
        fits = sorted(set(fits))
        how = f"collection {fits[0]}" + (f" (of {len(fits)}: {', '.join(fits)})" if len(fits) > 1 else "")
        return legs, fits[0], how

    async def _eligible_listed(self, venue: str, adapter: Any, legs: list[ComboLeg],
                               given: list[ComboLeg]) -> tuple[list[ComboLeg], str | None, str]:
        held = self._eligible_lists.get(venue)
        now = time.monotonic()
        if held is None or now - held[0] > ELIGIBLE_TTL_S:
            held = (now, {}, False)
        known, complete = dict(held[1]), held[2]
        if not complete and any(leg.market_id not in known for leg in legs):
            cursor: str | None = None
            pages = 0
            while pages < ELIGIBLE_PAGES:
                page = await adapter.fetch_combo_eligible_markets(cursor=cursor)
                for row in page:
                    known[row.market_id] = row
                pages += 1
                cursor = page.next_cursor
                if not cursor:
                    complete = True
                    break
                if all(leg.market_id in known for leg in legs):
                    break
            self._eligible_lists[venue] = (now, known, complete)
        out = []
        for leg, original in zip(legs, given):
            row = known.get(leg.market_id)
            if row is None:
                where = "is not on" if complete else f"is not in the first {ELIGIBLE_PAGES} pages of"
                raise _Ineligible(f"{leg.market_id} {where} {venue}'s combo-eligible list", leg=original.market_id)
            position = (row.position_ids or {}).get(leg.outcome)
            if position and not leg.info.get("position_id"):
                leg = leg.model_copy(update={"info": {**leg.info, "position_id": position}})
            if row.event_id and not leg.event_id:
                leg = leg.model_copy(update={"event_id": row.event_id})
            out.append(leg)
        return out, None, f"{venue}'s combo-eligible list"

    # -- quotes and ranking ----------------------------------------------------

    async def refresh(self, group_id: str) -> RfqGroup:
        """Read every open member's quotes from its venue now (the follower
        does the same on its timer) and rank them."""
        group = await self.get(group_id)
        for member in group.members:
            if member.status != "open" or not member.rfq_id:
                continue
            adapter = self.engine.adapters.get(member.venue)
            if adapter is None:
                continue
            try:
                quotes = await adapter.fetch_quotes(member.rfq_id)
            except Exception as exc:
                log.info("synpath.engine: reading quotes of group %s at %s failed: %s", group.id, member.venue, exc)
                continue
            for quote in quotes:
                await self.desk._observe(member.venue, quote, extra={"book": group.book})
        return group

    async def on_quote(self, venue: str, quote: RfqQuote) -> None:
        """A quote on a member RFQ, new or changed (from the desk's
        `_observe`): normalized, ranked, published."""
        group_id = self.group_of(venue, quote.rfq_id)
        if group_id is None or group_id not in self.groups:
            return
        group = self.groups[group_id]
        member = group.member_of(venue, quote.rfq_id)
        if member is None:
            return
        entry = await self._normalize(group, member, quote)
        group.quotes = [q for q in group.quotes if not (q.quote_id == entry.quote_id and q.venue == venue)] + [entry]
        self._rank(group)
        await self._save(group)
        ranked = next(q for q in group.quotes if q.quote_id == entry.quote_id and q.venue == venue)
        await self.engine.bus.publish("rfq_group.quote", {"group_id": group.id, **ranked.model_dump(mode="json")},
                                      key=f"rfq_group:{group.id}")
        await self._publish("rfq_group.updated", group, reason="quote", quote_id=quote.id)

    async def _fee(self, venue: str, market_id: str, side: Side, price: Decimal, quantity: Decimal) -> Decimal | None:
        """The venue's taker fee for one order of `quantity` at `price` (the
        member's own YES side and price), from its fee schedule; `None` where
        it will not say. Cached per price and size."""
        key = (venue, market_id, side.value, price, quantity)
        if key in self._fees:
            return self._fees[key]
        adapter = self.engine.adapters.get(venue)
        fee: Decimal | None = None
        if adapter is not None and adapter.has.get("fetch_fee_estimate") and quantity > 0:
            try:
                estimate = await adapter.fetch_fee_estimate(market_id, side, price, quantity)
                if estimate.taker_fee is not None:
                    fee = Decimal(str(estimate.taker_fee))
            except Exception as exc:   # a fee the venue would not quote is not a reason to drop the quote
                log.info("synpath.engine: no fee estimate for %s on %s: %s", market_id, venue, exc)
        self._fees[key] = fee
        return fee

    async def _normalize(self, group: RfqGroup, member: GroupMember, quote: RfqQuote) -> GroupQuote:
        side_m = group.side if not member.flip else _opposite(group.side)
        raw = quote.ask if side_m == Side.BUY else quote.bid
        base = dict(quote_id=quote.id, venue=member.venue, rfq_id=quote.rfq_id,
                    market_id=quote.market_id or member.instrument_id, flip=member.flip, status=quote.status,
                    maker=(quote.info or {}).get("maker"), raw_bid=quote.bid, raw_ask=quote.ask, raw_price=raw,
                    **quote_expiry(quote, member))
        if raw is None:
            return GroupQuote(**base, note="the maker declined the side asked")
        price = raw if not member.flip else ONE - raw
        token = outcome_price(price, group.outcome)
        available = quote.ask_quantity if side_m == Side.BUY else quote.bid_quantity
        market_id = base["market_id"] or ""
        note = None
        if group.quantity is not None:
            quantity = group.quantity if available is None else min(group.quantity, available)
            fee = await self._fee(member.venue, market_id, side_m, raw, quantity)
        else:
            quantity, fee, note = await self._size_amount(group, member, market_id, side_m, raw, token, available)
        per = (fee or ZERO) / quantity if quantity else ZERO
        effective = price + per if group.side == Side.BUY else price - per
        effective_outcome = token + per if group.action == "buy" else token - per
        partial = group.quantity is not None and available is not None and available < group.quantity
        return GroupQuote(**base, price=price, outcome_price=token, available=available, quantity=quantity,
                          fee=fee, fee_per_contract=per if quantity else None, effective_price=effective,
                          effective_outcome_price=effective_outcome, partial=partial, note=note)

    async def _size_amount(self, group: RfqGroup, member: GroupMember, market_id: str, side_m: Side, raw: Decimal,
                           token: Decimal, available: Decimal | None) -> tuple[Decimal | None, Decimal | None, str | None]:
        """The contracts `amount` buys from this quote after its taker fee
        (a buy), or sells for (a sell, fees not deducted from the size)."""
        amount = group.amount or ZERO
        precision = self.desk._precision(member.venue, market_id)
        step = ONE if precision.whole_contracts else (precision.quantity_step or FALLBACK_QUANTITY_STEP)
        try:
            quantity = contracts_for_amount(amount, token, precision, action=group.action)
        except InvalidOrder as exc:
            return None, None, str(exc)
        if available is not None:
            quantity = min(quantity, available)
        fee = await self._fee(member.venue, market_id, side_m, raw, quantity)
        if group.action == "buy":
            for _ in range(6):
                if quantity * token + (fee or ZERO) <= amount:
                    break
                quantity = ((amount - (fee or ZERO)) / token / step).to_integral_value(rounding=ROUND_DOWN) * step
                if quantity <= 0 or quantity < precision.min_quantity:
                    return None, None, f"{amount} does not cover the fee and the minimum size here"
                fee = await self._fee(member.venue, market_id, side_m, raw, quantity)
        return quantity, fee, None

    def _rank(self, group: RfqGroup) -> None:
        """Best first: by contracts for the amount (an amount-sized buy), then
        by `effective_outcome_price` (lowest for a buy, highest for a sell).
        Quotes that cannot be accepted now (not open, their member gone, no
        price for the side asked) come last, unranked."""
        open_members = {(m.venue, m.rfq_id) for m in group.members if m.status == "open"}

        def acceptable(q: GroupQuote) -> bool:
            return (q.status == QuoteStatus.OPEN and q.effective_outcome_price is not None and q.quantity
                    and (q.venue, q.rfq_id) in open_members) or False

        buy = group.action == "buy"

        def order(q: GroupQuote) -> tuple:
            price = q.effective_outcome_price or ZERO
            by_price = price if buy else -price
            if group.amount is not None and buy:
                return (-(q.quantity or ZERO), by_price, q.venue, q.quote_id)
            return (by_price, q.venue, q.quote_id)

        ranked = sorted((q for q in group.quotes if acceptable(q)), key=order)
        rest = [q for q in group.quotes if not acceptable(q)]
        for index, quote in enumerate(ranked, 1):
            quote.rank = index
        for quote in rest:
            quote.rank = None
        group.quotes = ranked + rest
        group.expires_at = min((q.expires_at for q in ranked if q.expires_at is not None), default=None)

    # -- members closing --------------------------------------------------------

    async def on_rfq_closed(self, venue: str, rfq_id: str, *, reason: str) -> None:
        """A member RFQ closed at the venue (cancelled, expired or executed)."""
        group_id = self.group_of(venue, rfq_id)
        group = self.groups.get(group_id) if group_id else None
        if group is None:
            return
        member = group.member_of(venue, rfq_id)
        if member is None:
            return
        if reason == "executed":
            member.status = "executed" if member.accept_key else "closed"
        elif member.status in ("open", "creating"):
            member.status = "canceled" if reason == "canceled" else "closed"
            member.detail = member.detail or reason
        if group.status == "open" and group.id not in self._busy \
                and not any(m.status in ("open", "creating") for m in group.members):
            group.status = "expired"
        self._rank(group)
        await self._save(group)
        await self._publish("rfq_group.updated", group, reason=f"member_{reason}", venue=venue)

    async def _cancel_member(self, group: RfqGroup, member: GroupMember, *, reason: str) -> bool:
        """Close one member RFQ. A venue with no requester cancel
        (`rfq_requester_cancel = False`, Polymarket global) is dropped here
        and left to lapse at its expiry: nothing trades there unless accepted."""
        if member.status not in ("open", "creating") or not member.rfq_id:
            return member.status not in ("open", "creating")
        adapter = self.engine.adapters.get(member.venue)
        lapses = adapter is not None and not getattr(adapter, "rfq_requester_cancel", True)
        try:
            await self.desk.cancel_rfq(member.rfq_id, venue=member.venue, reason=reason)
        except Exception as exc:
            member.detail = f"cancel failed: {type(exc).__name__}: {exc}"
            group.failures.append({"venue": member.venue, "stage": "cancel", "error": member.detail, "ts": _now_ms()})
            await self._save(group)
            return False
        if lapses:
            member.status = "lapsing"
            member.detail = (f"{reason}: {member.venue} has no requester cancel; the RFQ lapses at its expiry and "
                             "nothing trades there unless accepted")
        else:
            member.status, member.detail = "canceled", reason
        await self._save(group)
        return True

    async def cancel_group(self, group_id: str, *, reason: str = "canceled by the caller") -> RfqGroup:
        """Cancel every open member. A group already final is returned as it
        is; one with an accept in flight is refused (its others are cancelled
        already)."""
        async with self.engine._lock(f"rfq-group-accept:{group_id}"):
            group = await self.get(group_id)
            if group.final:
                return group
            await self._reconcile(group)
            if group.status in ("accepting", "accepted"):
                raise RiskRejected(f"RFQ group {group_id} has an accept in flight on {group.accepted_venue}; "
                                   "the other venues are cancelled already", rule="rfq_group_accept_in_flight")
            self._busy.add(group.id)
            try:
                for member in group.members:
                    await self._cancel_member(group, member, reason=reason)
                group.status = "canceled"
                self._rank(group)
                await self._save(group)
                await self._publish("rfq_group.canceled", group, reason=reason)
            finally:
                self._busy.discard(group.id)
            return group

    async def on_halt(self, scope: str = "*") -> dict[str, Any]:
        """The kill switch: every open group's members in scope cancelled. A
        group whose accept is in flight keeps it (the halt refuses any new
        one); a group left with no open member is `canceled`."""
        out: dict[str, Any] = {}
        for group in list(self.groups.values()):
            if group.final:
                continue
            done = []
            self._busy.add(group.id)
            try:
                for member in group.members:
                    if scope in ("*", member.venue) and member.status in ("open", "creating"):
                        if await self._cancel_member(group, member, reason="halt"):
                            done.append(member.venue)
                if group.status in ("open", "creating") and not any(
                        m.status in ("open", "creating") for m in group.members):
                    group.status = "canceled"
                self._rank(group)
                await self._save(group)
                await self._publish("rfq_group.canceled" if group.status == "canceled" else "rfq_group.updated",
                                    group, reason="halt")
            finally:
                self._busy.discard(group.id)
            out[group.id] = {"status": group.status, "canceled": done}
        return out

    # -- accept ---------------------------------------------------------------

    async def accept_group_quote(self, group_id: str, quote_id: str, *, venue: str | None = None,
                                 worst_price: Decimal | None = None, wait: bool = False, timeout_s: float = 30.0,
                                 tags: dict[str, str] | None = None) -> GroupAccept:
        """Take one quote of the group (module doc): the claim, the other
        members cancelled, then the accept through `RfqDesk.accept_quote`.
        A quote is named by `(venue, quote_id)`: quote ids are the venues'
        own, so two venues may use the same one. `venue` is required when the
        group spans more than one venue (`BadRequest`, reason
        `venue_required`). `worst_price` is in the group's YES terms.
        Refused (`RiskRejected`, rule `rfq_group_accept_in_flight` or
        `rfq_group_accepted`) while an accept on the group is in flight or
        done."""
        await self.desk._lease()
        async with self.engine._lock(f"rfq-group-accept:{group_id}"):
            group = await self.get(group_id)
            await self._reconcile(group)
            self._refuse_taken(group)
            if group.final:
                raise BadRequest(f"RFQ group {group_id} is {group.status}")
            venue = self._accept_venue(group, quote_id, venue)
            entry = find_quote(group, venue, quote_id)
            if entry is None:
                await self.refresh(group_id)
                entry = find_quote(group, venue, quote_id)
            if entry is None:
                raise RfqNotFound(f"no quote {quote_id!r} on {venue} in RFQ group {group_id}")
            member = group.member_of(entry.venue, entry.rfq_id)
            if member is None or member.status != "open":
                raise BadRequest(f"quote {quote_id} is on {entry.venue}, whose RFQ in this group is "
                                 f"{member.status if member else 'gone'}")
            if entry.status != QuoteStatus.OPEN or not entry.quantity:
                raise BadRequest(f"quote {quote_id} is {entry.status.value}"
                                 + ("" if entry.quantity else f", with no size for this group ({entry.note})"))
            accept = self._member_accept(group, member, entry, worst_price=worst_price, tags=tags)
            # Everything the accept checks, before anything is written: a risk
            # refusal here leaves every venue's RFQ standing.
            await self.desk.check_accept(accept, venue=member.venue)
            self._busy.add(group.id)
            try:
                claim = await self._claim(group, member, entry, accept)
                return await self._accept_claimed(group, member, accept, claim, wait=wait, timeout_s=timeout_s)
            finally:
                self._busy.discard(group.id)

    @staticmethod
    def _accept_venue(group: RfqGroup, quote_id: str, venue: str | None) -> str:
        """The venue an accept names, or the group's only venue. A group on
        several venues needs it: quote ids are the venues' own."""
        venues = sorted(group.venues())
        if venue is not None:
            if venue not in venues:
                exc = BadRequest(f"RFQ group {group.id} has no member on {venue} (its venues: {', '.join(venues)})")
                exc.reason = "venue_not_in_group"
                raise exc
            return venue
        if len(venues) == 1:
            return venues[0]
        holders = sorted({q.venue for q in group.quotes if q.quote_id == quote_id})
        what = (f"quote id {quote_id!r} is on {len(holders)} venues ({', '.join(holders)})" if len(holders) > 1
                else f"the group spans {', '.join(venues)}")
        exc = BadRequest(f"name the quote's venue: {what}; accept takes {{quote_id, venue}}")
        exc.reason = "venue_required"
        raise exc

    def _refuse_taken(self, group: RfqGroup) -> None:
        if group.status in ("accepting", "accepted"):
            raise RiskRejected(f"RFQ group {group.id} has an accept in flight on {group.accepted_venue} "
                               f"(quote {group.accepted_quote_id}); a group trades once",
                               rule="rfq_group_accept_in_flight")
        if group.status == "executed":
            raise RiskRejected(f"RFQ group {group.id} was accepted on {group.accepted_venue} and executed; a group "
                               "trades once", rule="rfq_group_accepted")

    def _member_accept(self, group: RfqGroup, member: GroupMember, entry: GroupQuote, *,
                       worst_price: Decimal | None, tags: dict[str, str] | None) -> AcceptQuote:
        adapter = self.engine.adapters[member.venue]
        side_m = group.side if not member.flip else _opposite(group.side)
        if getattr(adapter, "rfq_one_sided", False):
            outcome, action = "yes", "buy" if side_m == Side.BUY else "sell"
        else:
            outcome = _flip_outcome(group.outcome) if member.flip else group.outcome
            action = group.action
        worst = None
        if worst_price is not None:
            worst = worst_price if not member.flip else ONE - worst_price
        return AcceptQuote(
            quote_id=entry.quote_id, rfq_id=entry.rfq_id, outcome=outcome, action=action, worst_price=worst,
            quantity=entry.quantity, client_order_id=f"rfq-accept:{member.venue}:{entry.quote_id}",
            book=group.book, trader=group.trader, tags={**group.tags, **(tags or {}), "rfq_group": group.id})

    async def _claim(self, group: RfqGroup, member: GroupMember, entry: GroupQuote, accept: AcceptQuote) -> str:
        """Hold the group for this quote, in the journal (module doc)."""
        journal = self.engine.journal
        attempt = 1
        while True:
            key = f"rfq-group-accept:{group.id}:{attempt}"
            prior = await journal.intent(key)
            if prior is None:
                break
            if prior.state not in RELEASED:
                raise RiskRejected(f"RFQ group {group.id} is held by accept {key} ({prior.state}); a group trades once",
                                   rule="rfq_group_accepted" if prior.state in (IntentState.SENT, IntentState.SETTLED)
                                   else "rfq_group_accept_in_flight")
            attempt += 1
        others = [m.key for m in group.members if m is not member and m.status in ("open", "creating") and m.rfq_id]
        account = self.desk._account(member.venue)
        request = self.desk._record(
            market_id=group.market_id, key=key, account=account, type=OrderType.RFQ_TAKER, side=group.side,
            quantity=entry.quantity, price=entry.price, book=group.book, trader=group.trader, tags=group.tags,
            params={"group_id": group.id, "operation": "rfq_group_accept", "quote_id": entry.quote_id,
                    "venue": member.venue, "rfq_id": entry.rfq_id, "member_accept_key": accept.client_order_id,
                    "cancel": others, "attempt": attempt})
        await journal.record_intent(request, client_order_id=key, venue=member.venue, account=account,
                                    operation="rfq_group_accept", target_order_id=entry.quote_id)
        stored = await journal.intent(key)
        if stored is None or stored.target_order_id != entry.quote_id \
                or _params(stored).get("group_id") != group.id or _params(stored).get("venue") != member.venue:
            raise RiskRejected(f"RFQ group {group.id}: another accept claimed it first", rule="rfq_group_accept_in_flight")
        group.status, group.claim_key, group.accept_attempts = "accepting", key, attempt
        group.accepted_quote_id, group.accepted_venue = entry.quote_id, member.venue
        member.accept_key = accept.client_order_id
        await self._save(group)
        await self._publish("rfq_group.accepting", group, quote_id=entry.quote_id, venue=member.venue, claim_key=key)
        return key

    async def _accept_claimed(self, group: RfqGroup, member: GroupMember, accept: AcceptQuote, claim: str, *,
                              wait: bool, timeout_s: float) -> GroupAccept:
        journal = self.engine.journal
        for other in group.members:
            if other is not member:
                await self._cancel_member(group, other, reason=f"accepted {accept.quote_id} on {member.venue}")
        await journal.mark_intent(claim, IntentState.SENDING, detail="the others cancelled; accepting")
        member.status = "accepting"
        await self._save(group)
        try:
            quote = await self.desk.accept_quote(accept, venue=member.venue, _group=group.id)
        except SubmissionInDoubt:
            # The desk follows it; the group follows the desk (`_reconcile`).
            await self._save(group)
            raise
        except Exception as exc:
            await self._release(group, member, claim, detail=f"{type(exc).__name__}: {exc}")
            raise
        group.status, member.status = "accepted", "accepted"
        await self._save(group)
        await self._publish("rfq_group.accepted", group, quote_id=accept.quote_id, venue=member.venue)
        if wait:
            quote = await self.desk.wait(accept.client_order_id or "", venue=member.venue, timeout_s=timeout_s)
        await self._reconcile(group)
        return GroupAccept(group=group, quote=quote, venue=member.venue, client_order_id=accept.client_order_id or "",
                           claim_key=claim)

    async def _release(self, group: RfqGroup, member: GroupMember, claim: str, *, detail: str,
                       sent: bool = True) -> None:
        """The accept did not trade: the claim is released and the group is
        open again on what is still open. What was cancelled stays so."""
        await self.engine.journal.mark_intent(claim, IntentState.FAILED, detail=detail)
        still = member.rfq_id is not None and f"{member.venue}:{member.rfq_id}" in self.desk.rfqs
        member.status = "open" if still else "closed"
        member.detail = f"accept failed: {detail}"
        group.failures.append({"venue": member.venue, "stage": "accept", "quote_id": group.accepted_quote_id,
                               "claim_key": claim, "error": detail, "ts": _now_ms()})
        failed_quote = group.accepted_quote_id
        for quote in group.quotes:
            if sent and quote.quote_id == failed_quote and quote.venue == member.venue \
                    and quote.status == QuoteStatus.OPEN:
                quote.status = QuoteStatus.FAILED
        group.accepted_quote_id = group.accepted_venue = None
        group.status = "open" if any(m.status == "open" for m in group.members) else "expired"
        self._rank(group)
        await self._save(group)
        await self._publish("rfq_group.accept_failed", group, claim_key=claim, venue=member.venue,
                            quote_id=failed_quote, error=detail)

    async def _reconcile(self, group: RfqGroup) -> str:
        """Bring the group in line with the journal: members whose create was
        in doubt, and the claim against its member accept's intent. Saved
        only when something changed."""
        before = group.model_dump_json(exclude={"updated_at"})
        result = await self._reconcile_once(group)
        if group.model_dump_json(exclude={"updated_at"}) != before:
            await self._save(group)
        return result

    async def _reconcile_once(self, group: RfqGroup) -> str:
        journal = self.engine.journal
        # While this process is creating the group's members, their state is
        # its own: only a restarted engine reads them back from the journal.
        for member in group.members if group.id not in self._busy or group.status != "creating" else []:
            if member.status == "creating" and not member.create_key:
                # The fan-out never reached it (the process stopped first).
                member.status, member.detail = "failed", "not created: the process stopped before its turn"
                continue
            if member.status == "creating" and member.create_key:
                intent = await journal.intent(member.create_key)
                if intent is None:
                    member.status, member.detail = "failed", "the create was never written: nothing was sent"
                elif intent.state in (IntentState.SENT, IntentState.SETTLED) and intent.order_id:
                    held = self.desk.rfqs.get(f"{member.venue}:{intent.order_id}")
                    rfq = Rfq.model_validate(held["rfq"]) if held else None
                    if rfq is not None:
                        self._member_open(group, member, rfq)
                        if group.status not in ("open", "creating"):
                            await self._cancel_member(group, member, reason=f"the group is {group.status}")
                    else:
                        member.rfq_id, member.status = intent.order_id, "closed"
                elif intent.state in RELEASED:
                    member.status, member.detail = "failed", intent.detail or intent.state
        if group.status == "creating":
            group.status = "open" if any(m.status in ("open", "creating") for m in group.members) else "failed"
        if group.status not in ("accepting", "accepted") or not group.claim_key:
            if group.status == "open" and not any(m.status in ("open", "creating") for m in group.members):
                group.status = "expired"
                await self._publish("rfq_group.updated", group, reason="expired")
            return "unchanged"
        claim = await journal.intent(group.claim_key)
        if claim is None:
            return "unchanged"
        params = _params(claim)
        member = group.member_of(str(params.get("venue")), str(params.get("rfq_id")))
        if member is None:
            return "unchanged"
        accept_key = params.get("member_accept_key") or member.accept_key
        accepted = await journal.intent(accept_key) if accept_key else None
        if claim.state in RELEASED:
            if group.status in ("accepting", "accepted"):
                await self._release(group, member, claim.client_order_id, detail=claim.detail or claim.state)
            return "released"
        if accepted is None or accepted.state == IntentState.PLANNED:
            # The process stopped between the claim and the accept: finish the
            # cancels it planned, release it. Nothing was sent.
            for key in params.get("cancel") or []:
                venue, _, rfq_id = str(key).partition(":")
                other = group.member_of(venue, rfq_id)
                if other is not None:
                    await self._cancel_member(group, other, reason=f"accepted {params.get('quote_id')} on "
                                                                   f"{params.get('venue')} (finished after a restart)")
            if accepted is not None:
                await journal.mark_intent(accept_key, IntentState.REJECTED, detail="never sent: the process stopped")
            await self._release(group, member, claim.client_order_id,
                                detail="the process stopped before the accept was sent; nothing traded", sent=False)
            return "released"
        if accepted.state in (IntentState.SENT, IntentState.SETTLED):
            group.status, member.status, group.order_id = "executed", "executed", accepted.order_id
            await journal.mark_intent(claim.client_order_id, IntentState.SENT, order_id=accepted.order_id,
                                      detail="executed")
            self._rank(group)
            await self._save(group)
            await self._publish("rfq_group.executed", group, order_id=accepted.order_id, venue=member.venue)
            return "executed"
        if accepted.state in RELEASED:
            await self._release(group, member, claim.client_order_id,
                                detail=f"the accept ended without a trade: {accepted.detail or accepted.state}")
            return "released"
        if claim.state == IntentState.PLANNED:
            await journal.mark_intent(claim.client_order_id, IntentState.SENDING, detail="accept sent; following")
        if group.status == "accepting" and accepted.detail == self._accepted_mark():
            group.status, member.status = "accepted", "accepted"
        return "pending"

    @staticmethod
    def _accepted_mark() -> str:
        from .rfq import ACCEPTED_MARK
        return ACCEPTED_MARK

    # -- the follower and recovery ----------------------------------------------

    async def tick(self) -> None:
        """One pass: every group not final reconciled with the journal."""
        for group in list(self.groups.values()):
            if group.final or group.id in self._busy:
                continue
            try:
                await self._reconcile(group)
            except Exception:
                log.exception("synpath.engine: following RFQ group %s failed", group.id)

    async def restore(self) -> None:
        """At start, after the desk's own restore: the groups not final, and
        any member RFQ the desk's working set lacks re-seeded at its venue."""
        raw = await self.engine.journal.cursor(GROUPS_CURSOR)
        reseed: dict[str, list[Rfq]] = {}
        for row in json.loads(raw) if raw else []:
            try:
                group = RfqGroup.model_validate(row)
            except Exception as exc:
                log.warning("synpath.engine: an RFQ group in the journal does not validate: %s", exc)
                continue
            self.groups[group.id] = group
            for member in group.members:
                if not member.rfq_id:
                    continue
                self._by_rfq[member.key] = group.id
                if member.status in ("open", "accepting", "accepted") and member.rfq \
                        and member.key not in self.desk.rfqs:
                    self.desk.rfqs[member.key] = {"rfq": member.rfq, "book": group.book, "trader": group.trader}
                    try:
                        reseed.setdefault(member.venue, []).append(Rfq.model_validate(member.rfq))
                    except Exception as exc:
                        log.warning("synpath.engine: member RFQ %s does not validate: %s", member.key, exc)
        for venue, rfqs in reseed.items():
            adapter = self.engine.adapters.get(venue)
            restore = getattr(adapter, "restore_rfqs", None)
            if callable(restore):
                try:
                    restore(rfqs, [])
                except Exception as exc:
                    log.warning("synpath.engine: re-seeding group RFQs at %s failed: %s", venue, exc)
            await self.desk._save_working_set(venue)

    async def reconcile_all(self) -> list[dict[str, Any]]:
        """After recovery's in-doubt pass: every group reconciled."""
        out = []
        for group in list(self.groups.values()):
            if group.final:
                continue
            out.append({"group_id": group.id, "result": await self._reconcile(group), "status": group.status})
        return out

    async def resolve(self, intent: Intent) -> dict[str, Any]:
        """A group intent in doubt (`Engine._resolve`): the create once its
        members are known, the claim against its member accept."""
        base = {"client_order_id": intent.client_order_id, "venue": intent.venue, "operation": intent.operation}
        group_id = _params(intent).get("group_id")
        group = self.groups.get(str(group_id)) if group_id else None
        if group is None:
            await self.engine.journal.mark_intent(intent.client_order_id, IntentState.LOST,
                                                  detail="no such group in the journal's working set")
            return base | {"result": "swept"}
        if intent.operation == "rfq_group_create":
            await self._reconcile(group)
            standing = [m for m in group.members if m.status != "failed"]
            await self.engine.journal.mark_intent(
                intent.client_order_id, IntentState.SENT if standing else IntentState.REJECTED, order_id=group.id,
                detail=f"recovered with {len(standing)} of {len(group.members)} members")
            if not standing:
                group.status = "failed"
            await self._save(group)
            return base | {"result": "adopted" if standing else "swept", "group_id": group.id}
        result = await self._reconcile(group)
        return base | {"result": {"executed": "adopted", "released": "swept"}.get(result, "pending"),
                       "group_id": group.id}


def quote_expiry(quote: RfqQuote, member: GroupMember) -> dict[str, Any]:
    """A quote's deadlines as its venue gives them: its own expiry, else its
    RFQ's; none is made up where the venue gives none."""
    rfq_expiry = (member.rfq or {}).get("expires_at")
    expires = quote.expires_at if quote.expires_at is not None else rfq_expiry
    source = "quote" if quote.expires_at is not None else "rfq" if rfq_expiry is not None else None
    return {"expires_at": int(expires) if expires is not None else None, "expires_from": source,
            "confirm_by": quote.confirm_by, "execute_by": quote.execute_by}


def find_quote(group: RfqGroup, venue: str, quote_id: str) -> GroupQuote | None:
    """A group's quote by `(venue, quote_id)`."""
    return next((q for q in group.quotes if q.venue == venue and q.quote_id == quote_id), None)


class _Ineligible(Exception):
    def __init__(self, message: str, *, leg: str | None = None):
        super().__init__(message)
        self.leg = leg


def _params(intent: Intent) -> dict[str, Any]:
    """An intent's request parameters, as stored."""
    request = intent.request or {}
    return dict(request.get("params") or {})


__all__ = ["GroupAccept", "GroupInstrument", "GroupMember", "GroupQuote", "Matcher", "RfqGroup", "RfqGroupDesk",
           "SkippedVenue", "hosted_matcher", "lists_combo_markets"]
