"""RFQ and combos in the engine: both sides, journaled, recoverable.

`docs/rfq-combo.md` §5 is the design; this is the code behind
`engine.rfq` and the `Engine` methods that delegate to it.

**An accept is an order.** Taking a side of a quote commits capital the way
an order does, so it takes the same road: the quote is re-read, the price
for the caller's outcome and action is checked against `worst_price`, an
`amount` is sized into contracts (`synpath.trading.money`), and the
resulting `rfq_taker` `OrderRequest` goes through `Engine.evaluate_order_risk`, every
rule an order meets. Only then is the intent written, keyed
`rfq-accept:{venue}:{quote_id}`, marked `sending`, and the venue called.
A confirm on the quoter side is the same with `operation="confirm_quote"`
and the side that would trade (an `rfq_maker` order).

**The intent stays `sending` until the quote is final.** The venue answers
an accept with a quote in `accepted`; the maker's last look and the
execution come later. The engine does not invent a state for that: the
intent is in doubt until the quote is `executed` (the order is adopted and
the intent becomes `sent`) or ends without trading (`failed`). That one
rule makes following and recovering the same code: the follower loop, a
venue stream, `wait=True`, the sweep and `recover()` all call `resolve`,
which reads the quote back and acts on what the venue says.

**What a restart finds.** An accept intent written but never sent (the
process died before the call) reads back as a quote still `open`: after
`lost_after_s` it is marked `rejected`, nothing traded. One sent whose
answer was lost reads back `accepted`, `confirmed` or `executed`: it is
followed, and adopted when it executes. A venue that cannot be reached
leaves it unresolved.

**Book what the venue reports.** The order and its fills are booked on the
side the venue reports. Where that differs from the side the intent asked
for, `rfq.side_mismatch` is published (an alert) and the order's outcome
and action are left unset rather than contradict its side.

**A combo is an instrument.** Its own market id carries the order, the
fills, the position, the P&L and the reconciliation. Its legs are kept in
the journal as `combo.saved` events and shown on the position
(`Position.info["combo"]`). No schema change.

**Offers commit nothing.** Creating an RFQ and sending a maker quote are
journaled and checked against the halt, a paused book, the restricted list
and the order size limits (`RiskEngine.check_offer`), not the position
rules, which apply when the offer trades. A maker quote's `expires_at` is
held by the engine, which cancels the quote when it passes. A halt with the
`cancel` policy cancels every open RFQ we asked and every quote we made, on
every venue, before it cancels orders.
"""
from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable

from .. import ids
from ..errors import BadRequest, ExchangeError, NetworkError
from ..trading.base import TradingExchange
from ..trading.errors import OrderNotFound, OrderRejected, RfqNotFound, RiskRejected
from ..trading.instruments import venue_precision
from ..trading.money import contracts_for_amount, outcome_price
from ..trading.types import (
    AcceptQuote, Account, Combo, ComboLeg, MakerQuote, Order, OrderRequest, OrderStatus, OrderType, Precision,
    QuoteStatus, Rfq, RfqQuote, RfqStatus, Side, default_leg, yes_side,
)
from .storage_types import Intent, IntentState
from .submissions import SubmissionInDoubt

if TYPE_CHECKING:
    from .engine import Engine

log = logging.getLogger("synpath.engine.rfq")

ZERO = Decimal("0")
ONE = Decimal("1")

GROUP_OPERATIONS = frozenset({"rfq_group_create", "rfq_group_accept"})
"""The cross-venue group's own intents (`synpath.engine.rfq_group`)."""
RFQ_OPERATIONS = frozenset({
    "create_rfq", "cancel_rfq", "accept_quote", "send_quote", "replace_quote", "cancel_quote", "confirm_quote",
}) | GROUP_OPERATIONS
"""Intent operations this module owns; `Engine._resolve` hands them here."""
FOLLOWED = frozenset({"accept_quote", "confirm_quote"})
"""Operations whose intent stays `sending` until the quote is final."""

ACCEPTED_MARK = "accepted at the venue"
CONFIRMED_MARK = "confirmed at the venue"

HAPPY_PATH = (QuoteStatus.OPEN, QuoteStatus.ACCEPTED, QuoteStatus.CONFIRMED, QuoteStatus.EXECUTED)
STATUS_EVENT = {
    QuoteStatus.OPEN: "quote.received", QuoteStatus.ACCEPTED: "quote.accepted",
    QuoteStatus.CONFIRMED: "quote.confirmed", QuoteStatus.EXECUTED: "quote.executed",
    QuoteStatus.CANCELED: "quote.canceled", QuoteStatus.EXPIRED: "quote.expired",
    QuoteStatus.FAILED: "quote.failed",
}
NO_TRADE = (QuoteStatus.CANCELED, QuoteStatus.EXPIRED, QuoteStatus.FAILED)
RECENT_MS = 5_000
RFQ_STATUS_INTERVAL_S = 10.0
"""How often the follower re-reads one of our open RFQs from its venue, to
see it close there (a venue's own expiry, often unstated, as Kalshi's); at
once when the venue's `expires_at` for it has passed."""
"""How far before an in-doubt create an RFQ or quote may be stamped and still be its result."""


def _opposite(side: Side) -> Side:
    return Side.SELL if side == Side.BUY else Side.BUY


def _now_ms() -> int:
    return int(time.time() * 1000)


class RfqDesk:
    """The engine's RFQ and combo desk, attached as `engine.rfq`."""

    def __init__(self, engine: "Engine"):
        self.engine = engine
        self.rfqs: dict[str, dict[str, Any]] = {}
        """Our open RFQs, `venue:id` -> {rfq, book, trader}: watched for quotes."""
        self.maker_quotes: dict[str, dict[str, Any]] = {}
        """Our live maker quotes, `venue:id` -> what the engine holds about them (expiry, book, size)."""
        self.incoming: dict[str, Rfq] = {}
        """Open RFQs from others, as the incoming feed last saw them."""
        self.combos: dict[str, Combo] = {}
        self.following: dict[str, str] = {}
        """`venue:quote_id` -> the key of the accept or confirm intent following it."""
        self._status: dict[str, QuoteStatus] = {}
        self._last_quote: dict[str, RfqQuote] = {}
        self._subscribed: set[str] = set()
        self._incoming_ts = 0.0
        self._rfq_read: dict[str, float] = {}
        self.pushed: set[str] = set()
        """Venues whose incoming RFQs arrive on a stream (`subscribe_incoming_rfqs`):
        not polled, and their quoting callbacks run as each RFQ lands."""
        self._incoming_subscribed: set[str] = set()
        self._streams_started: set[str] = set()
        self._incoming_callbacks: list[Callable[[Rfq], Any]] = []

    # -- plumbing -------------------------------------------------------------

    @property
    def config(self) -> Any:
        return self.engine.config

    def _venue_for(self, venue: str | None, *, account: Account | None = None, market_id: str | None = None) -> str:
        if venue:
            return venue
        if account is not None:
            return account.venue
        if market_id:
            found, _ = ids.split(market_id)
            if found is not None and found in self.engine.adapters:
                return found
        if len(self.engine.adapters) == 1:
            return next(iter(self.engine.adapters))
        raise BadRequest("name the venue: pass venue=, name an account, or use a Synpath id (venue:native)")

    def _adapter(self, venue: str, capability: str) -> TradingExchange:
        adapter = self.engine._adapter(venue)
        if not adapter.has.get(capability):
            raise BadRequest(f"{venue}: no {capability} on this venue (has[{capability!r}] is False)")
        return adapter

    def _account(self, venue: str, account: Account | None = None) -> Account:
        return account or self.engine.accounts.get(venue) or Account(venue=venue)

    async def _lease(self) -> None:
        if self.engine.config.require_lease and not await self.engine.journal.holds_lease():
            from .journal import LeaseLost
            raise LeaseLost("this execution worker no longer holds the journal lease")

    async def _publish(self, kind: str, payload: dict[str, Any], *, key: str, persist: bool = True) -> None:
        await self.engine.bus.publish(kind, payload, key=key, persist=persist)

    async def _refused(self, decision: Any, *, operation: str, market_id: str, book: str | None,
                       extra: dict[str, Any] | None = None) -> RiskRejected:
        await self.engine.bus.publish("risk.rejected", {
            "rule": decision.rule, "message": decision.message, "operation": operation, "market_id": market_id,
            "book": book, "config_version": decision.config_version, **decision.detail, **(extra or {}),
        })
        return RiskRejected(decision.message, rule=decision.rule)

    def _precision(self, venue: str, market_id: str) -> Precision:
        spec = self.engine.market_master.find(market_id)
        if spec is not None:
            return spec.precision
        try:
            return venue_precision(venue)
        except KeyError:
            return Precision(tick=Decimal("0.01"))

    def _record(self, *, market_id: str, key: str, account: Account, type: OrderType, side: Side = Side.BUY,
                quantity: Decimal | None = None, price: Decimal | None = None, book: str | None = None,
                trader: str | None = None, tags: dict[str, str] | None = None, outcome: str | None = None,
                action: str | None = None, params: dict[str, Any] | None = None) -> OrderRequest:
        """The intent's request: what the journal keeps about the operation."""
        body: dict[str, Any] = dict(
            user_id=self.engine.config.user_id, market_id=market_id, side=side, quantity=quantity or ZERO,
            type=type, price=price, client_order_id=key, account=account, book=book, trader=trader,
            tags=dict(tags or {}), params=params or {},
        )
        if outcome is not None:
            body.update(outcome=outcome, action=action)
        return OrderRequest(**body)

    async def _begin(self, request: OrderRequest, *, key: str, venue: str, account: Account, operation: str,
                     target: str | None) -> None:
        """Write the intent (or reopen a failed one for a retry) and mark it `sending`."""
        journal = self.engine.journal
        prior = await journal.intent(key)
        if prior is None:
            await journal.record_intent(request, client_order_id=key, venue=venue, account=account,
                                        operation=operation, target_order_id=target)
        elif prior.state in (IntentState.REJECTED, IntentState.FAILED, IntentState.LOST):
            await journal.mark_intent(key, IntentState.PLANNED, detail="retried")
        await journal.mark_intent(key, IntentState.SENDING, bump_attempt=True)

    async def _save_working_set(self, venue: str) -> None:
        """The open RFQs and live maker quotes, per venue, in the cursors
        table: what the engine watches after a restart."""
        rfqs = [v for k, v in self.rfqs.items() if k.startswith(f"{venue}:")]
        quotes = [v for k, v in self.maker_quotes.items() if k.startswith(f"{venue}:")]
        await self.engine.journal.set_cursor(f"rfq:open:{venue}", json.dumps(rfqs, default=str))
        await self.engine.journal.set_cursor(f"rfq:maker_quotes:{venue}", json.dumps(quotes, default=str))

    # -- events ---------------------------------------------------------------

    def _quote_key(self, venue: str, quote_id: str) -> str:
        return f"{venue}:quote:{quote_id}"

    async def _last_status(self, key: str) -> QuoteStatus | None:
        if key in self._status:
            return self._status[key]
        for event in await self.engine.journal.events_page(prefixes=["quote."], keys=[key], limit=20):
            status = event.payload.get("status")
            if status in QuoteStatus._value2member_map_:
                self._status[key] = QuoteStatus(status)
                return self._status[key]
        return None

    async def _observe(self, venue: str, quote: RfqQuote, *, extra: dict[str, Any] | None = None) -> None:
        """Publish what changed about a quote since the engine last saw it.
        A jump along the happy path (open to executed between two reads)
        publishes the steps in between too, marked `inferred`."""
        key = self._quote_key(venue, quote.id)
        self._last_quote[key] = quote
        last = await self._last_status(key)
        new = quote.status
        if last == new:
            return
        steps: list[tuple[QuoteStatus, bool]] = []
        if new in HAPPY_PATH:
            start = HAPPY_PATH.index(last) + 1 if last in HAPPY_PATH else (1 if last is None and new != QuoteStatus.OPEN else 0)
            for step in HAPPY_PATH[start:HAPPY_PATH.index(new)]:
                if step != QuoteStatus.OPEN:
                    steps.append((step, True))
        steps.append((new, False))
        body = quote.model_dump(mode="json")
        for status, inferred in steps:
            self._status[key] = status
            payload = {**body, "status": status.value, **(extra or {})}
            if inferred:
                payload["inferred"] = True
            await self._publish(STATUS_EVENT[status], payload, key=key)
        await self._groups_hear(venue, quote)

    async def _groups_hear(self, venue: str, quote: RfqQuote) -> None:
        """Hand a quote on one of our RFQs to the cross-venue groups, which
        rank it if the RFQ is a group member. Never fails the caller."""
        groups = getattr(self.engine, "rfq_groups", None)
        if groups is None or quote.mine:
            return
        try:
            await groups.on_quote(venue, quote)
        except Exception:
            log.exception("synpath.engine: ranking quote %s at %s for its group failed", quote.id, venue)

    async def _guard_side(self, key: str, venue: str, *, expected: Side, reported: Side | None,
                          quote_id: str, where: str) -> bool:
        """`True` when the venue reports the side asked for (or none)."""
        if reported is None or reported == expected:
            return True
        found = await self.engine.journal.events_page(kinds=["rfq.side_mismatch"], keys=[key], limit=1)
        if not found:
            log.error("synpath.engine: %s reports %s on quote %s, the intent %s asked for %s; booking the venue's side",
                      venue, reported.value, quote_id, key, expected.value)
            await self._publish("rfq.side_mismatch", {
                "client_order_id": key, "venue": venue, "quote_id": quote_id, "expected": expected.value,
                "reported": reported.value, "where": where,
            }, key=key)
        return False

    # -- combos ---------------------------------------------------------------

    async def create_combo(self, legs: list[ComboLeg], *, venue: str | None = None,
                           collection_id: str | None = None) -> Combo:
        """The venue's combo for these legs (`create_combo` on its adapter),
        kept with the engine. `collection_id` is passed only when given, so
        an adapter without the keyword still works without it."""
        venue = self._venue_for(venue, market_id=legs[0].market_id if legs else None)
        adapter = self._adapter(venue, "combos")
        combo = await (adapter.create_combo(legs, collection_id=collection_id) if collection_id is not None
                       else adapter.create_combo(legs))
        await self.save_combo(combo)
        return combo

    async def save_combo(self, combo: Combo) -> None:
        """Keep a combo's legs with the engine (`combo.saved`), for display
        on its position and fills. Written once per change."""
        known = self.combos.get(combo.market_id)
        if known is not None and [l.model_dump() for l in known.legs] == [l.model_dump() for l in combo.legs]:
            return
        self.combos[combo.market_id] = combo
        await self._publish("combo.saved", combo.model_dump(mode="json"), key=f"combo:{combo.market_id}")

    async def combo(self, market_id: str) -> Combo | None:
        if market_id in self.combos:
            return self.combos[market_id]
        found = await self.engine.journal.events_page(kinds=["combo.saved"], keys=[f"combo:{market_id}"], limit=1)
        if found:
            self.combos[market_id] = Combo.model_validate(found[0].payload)
            return self.combos[market_id]
        return None

    # -- taker: RFQs ----------------------------------------------------------

    async def create_rfq(
        self, instrument_id: str, *, quantity: Decimal | None = None, amount: Decimal | None = None,
        outcome: str | None = None, action: str | None = None, rest_remainder: bool = False,
        combo: Combo | None = None, venue: str | None = None, account: Account | None = None,
        book: str | None = None, trader: str | None = None, tags: dict[str, str] | None = None,
        key: str | None = None,
    ) -> Rfq:
        """Ask for quotes on a market or a combo. Journaled; refused while
        halted, on a paused book, a restricted market or over the size limits.
        `key` names the intent (a cross-venue group names its members' so a
        restart can find them); by default one is made."""
        await self._lease()
        if (quantity is None) == (amount is None):
            raise BadRequest("size an RFQ by quantity (contracts) or amount (money), exactly one")
        venue = self._venue_for(venue, account=account, market_id=instrument_id)
        adapter = self._adapter(venue, "rfq_taker")
        account = self._account(venue, account)
        decision = self.engine.order_risk_checks.check_offer(instrument_id, venue=venue, book=book, quantity=quantity, notional=amount)
        if not decision.ok:
            raise await self._refused(decision, operation="create_rfq", market_id=instrument_id, book=book)
        if combo is not None:
            await self.save_combo(combo)
        key = key or f"rfq-create:{venue}:{uuid.uuid4().hex[:16]}"
        request = self._record(market_id=instrument_id, key=key, account=account, type=OrderType.RFQ_TAKER,
                               quantity=quantity, book=book, trader=trader, tags=tags,
                               params={"amount": str(amount) if amount is not None else None,
                                       "outcome": outcome, "action": action, "rest_remainder": rest_remainder,
                                       "combo": combo.model_dump(mode="json") if combo else None})
        await self._begin(request, key=key, venue=venue, account=account, operation="create_rfq", target=None)
        try:
            rfq = await adapter.create_rfq(instrument_id, quantity=quantity, amount=amount, outcome=outcome,
                                           action=action, rest_remainder=rest_remainder, combo=combo)
        except (NetworkError, asyncio.TimeoutError) as exc:
            await self._publish("rfq.in_doubt", {"client_order_id": key, "venue": venue, "error": str(exc)}, key=key)
            raise SubmissionInDoubt(f"{venue}: no answer to the RFQ create ({type(exc).__name__}: {exc}); "
                                    "the engine resolves it against the venue", client_order_id=key) from exc
        except ExchangeError as exc:
            await self.engine.journal.mark_intent(key, IntentState.REJECTED, detail=str(exc))
            raise
        await self._track_rfq(venue, rfq, book=book, trader=trader, key=key)
        return rfq

    async def _track_rfq(self, venue: str, rfq: Rfq, *, book: str | None, trader: str | None, key: str) -> None:
        if rfq.account is None:
            rfq = rfq.model_copy(update={"account": self._account(venue)})
        if rfq.combo is not None:
            await self.save_combo(rfq.combo)
        await self.engine.journal.mark_intent(key, IntentState.SENT, order_id=rfq.id)
        self.rfqs[f"{venue}:{rfq.id}"] = {"rfq": rfq.model_dump(mode="json"), "book": book, "trader": trader}
        await self._save_working_set(venue)
        await self._publish("rfq.created", {**rfq.model_dump(mode="json"), "book": book, "trader": trader},
                            key=f"{venue}:rfq:{rfq.id}")

    async def cancel_rfq(self, rfq_id: str, *, venue: str | None = None, reason: str = "canceled by the caller") -> None:
        """Close one of our RFQs. Journaled with its own key; never risk-checked."""
        venue = self._venue_for(venue, market_id=self._rfq_market(rfq_id))
        adapter = self._adapter(venue, "rfq_taker")
        key = f"rfq-cancel:{venue}:{rfq_id}"
        async with self.engine._lock(key):
            prior = await self.engine.journal.intent(key)
            if prior is not None and prior.state == IntentState.SETTLED:
                return
            held = self.rfqs.get(f"{venue}:{rfq_id}")
            market_id = held["rfq"].get("market_id") or held["rfq"].get("combo_id") if held else (self._rfq_market(rfq_id) or rfq_id)
            account = self._account(venue)
            request = self._record(market_id=market_id, key=key, account=account, type=OrderType.RFQ_TAKER,
                                   book=held.get("book") if held else None, params={"rfq_id": rfq_id, "reason": reason})
            await self._begin(request, key=key, venue=venue, account=account, operation="cancel_rfq", target=rfq_id)
            try:
                await adapter.cancel_rfq(rfq_id)
            except RfqNotFound:
                pass
            except (NetworkError, asyncio.TimeoutError) as exc:
                raise SubmissionInDoubt(f"{venue}: no answer to the RFQ cancel ({exc})", client_order_id=key) from exc
            await self._rfq_closed(venue, rfq_id, key=key, reason="canceled", detail=reason)

    async def _rfq_closed(self, venue: str, rfq_id: str, *, key: str | None, reason: str, detail: str = "") -> None:
        if key is not None:
            await self.engine.journal.mark_intent(key, IntentState.SETTLED, order_id=rfq_id, detail=detail or None)
        held = self.rfqs.pop(f"{venue}:{rfq_id}", None)
        self._rfq_read.pop(f"{venue}:{rfq_id}", None)
        await self._save_working_set(venue)
        if reason in ("canceled", "expired"):
            status = QuoteStatus.CANCELED if reason == "canceled" else QuoteStatus.EXPIRED
            for qkey, quote in list(self._last_quote.items()):
                if qkey.startswith(f"{venue}:quote:") and quote.rfq_id == rfq_id and not quote.mine \
                        and quote.status == QuoteStatus.OPEN:
                    await self._observe(venue, quote.model_copy(update={"status": status}),
                                        extra={"reason": "rfq_closed"})
        await self._publish("rfq.closed", {"rfq_id": rfq_id, "venue": venue, "reason": reason, "detail": detail,
                                           "book": held.get("book") if held else None}, key=f"{venue}:rfq:{rfq_id}")
        groups = getattr(self.engine, "rfq_groups", None)
        if groups is not None:
            try:
                await groups.on_rfq_closed(venue, rfq_id, reason=reason)
            except Exception:
                log.exception("synpath.engine: closing RFQ %s at %s in its group failed", rfq_id, venue)

    def _rfq_market(self, rfq_id: str) -> str | None:
        for key, held in self.rfqs.items():
            if key.endswith(f":{rfq_id}"):
                return held["rfq"].get("market_id") or held["rfq"].get("combo_id")
        return None

    def _rfq_held(self, venue: str, rfq_id: str | None) -> dict[str, Any] | None:
        return self.rfqs.get(f"{venue}:{rfq_id}") if rfq_id else None

    # -- taker: accept --------------------------------------------------------

    async def accept_quote(self, accept: AcceptQuote, *, venue: str | None = None, wait: bool = False,
                           timeout_s: float = 30.0, _group: str | None = None) -> RfqQuote:
        """Take one side of a quote. Returns at once with the quote as the
        venue answered (`accepted`); the confirm, the execution, the order and
        its fills arrive as events. `wait=True` follows the quote until it is
        final or `timeout_s` passes and returns it as it then stands.

        A quote on an RFQ that belongs to a cross-venue group is refused
        here: it is accepted through the group (`accept_group_quote`), which
        is what keeps a group to one accept."""
        await self._lease()
        held = None
        if venue is None and accept.account is None:
            for key_, value in self.rfqs.items():
                if accept.rfq_id and key_.endswith(f":{accept.rfq_id}"):
                    held, venue = value, key_.split(":", 1)[0]
        venue = self._venue_for(venue, account=accept.account)
        adapter = self._adapter(venue, "rfq_taker")
        account = self._account(venue, accept.account)
        held = held or self._rfq_held(venue, accept.rfq_id)
        key = accept.client_order_id or f"rfq-accept:{venue}:{accept.quote_id}"
        async with self.engine._lock(key):
            prior = await self.engine.journal.intent(key)
            if prior is not None and prior.state in (IntentState.SENDING, IntentState.SENT, IntentState.SETTLED):
                # A retry: the venue is asked, never sent to twice.
                if prior.state == IntentState.SENDING:
                    await self._resolve_followed(prior)
                quote = self._last_quote.get(self._quote_key(venue, accept.quote_id)) \
                    or await adapter.fetch_quote(accept.quote_id, rfq_id=accept.rfq_id)
                result = quote
            else:
                result = await self._accept_once(accept, venue=venue, adapter=adapter, account=account, key=key, held=held,
                                                 group=_group)
        if wait:
            return await self.wait(key, venue=venue, timeout_s=timeout_s)
        return result

    async def check_accept(self, accept: AcceptQuote, *, venue: str) -> OrderRequest:
        """Everything an accept checks before anything is written or sent
        (the quote re-read, its price against `worst_price`, the sizing,
        every order risk rule), without writing or sending: the order it
        would be. Raises as the accept itself would."""
        adapter = self._adapter(venue, "rfq_taker")
        account = self._account(venue, accept.account)
        held = self._rfq_held(venue, accept.rfq_id)
        key = accept.client_order_id or f"rfq-accept:{venue}:{accept.quote_id}"
        groups = getattr(self.engine, "rfq_groups", None)
        group = groups.group_of(venue, accept.rfq_id) if groups is not None else None
        _, request = await self._prepare_accept(accept, venue=venue, adapter=adapter, account=account, key=key,
                                                held=held, group=group)
        return request

    async def _prepare_accept(self, accept: AcceptQuote, *, venue: str, adapter: TradingExchange, account: Account,
                              key: str, held: dict[str, Any] | None,
                              group: str | None = None) -> tuple[tuple[RfqQuote, Decimal | None], OrderRequest]:
        quote = await adapter.fetch_quote(accept.quote_id, rfq_id=accept.rfq_id)
        groups = getattr(self.engine, "rfq_groups", None)
        owner = groups.group_of(venue, quote.rfq_id) if groups is not None else None
        if owner is not None and owner != group:
            raise BadRequest(f"{venue}: quote {quote.id} is on RFQ {quote.rfq_id} of cross-venue group {owner}; "
                             "accept it through the group (accept_group_quote), which cancels the other venues")
        if quote.status != QuoteStatus.OPEN:
            raise OrderRejected(f"{venue}: quote {quote.id} is {quote.status.value}, not open", reason="quote_not_open")
        price = quote.price_for(accept.outcome, accept.action)
        if price is None:
            raise BadRequest(f"{venue}: quote {quote.id} declines the side that {accept.action}s {accept.outcome}")
        rfq = Rfq.model_validate(held["rfq"]) if held else None
        market_id = quote.market_id or (rfq.instrument_id if rfq else None)
        if not market_id:
            raise BadRequest(f"{venue}: quote {quote.id} names no instrument and the engine does not hold its RFQ")
        book = accept.book or (held.get("book") if held else None)
        trader = accept.trader or (held.get("trader") if held else None)
        if accept.refuses(price):
            message = (f"quote {quote.id} {accept.side.value}s YES at {price}, worse than the worst price "
                       f"{accept.worst_price}")
            await self.engine.bus.publish("risk.rejected", {
                "rule": "worst_price", "message": message, "operation": "rfq_taker", "market_id": market_id,
                "book": book, "quote_id": quote.id, "price": str(price), "worst_price": str(accept.worst_price),
                "config_version": self.engine.order_risk_checks.config_version,
            })
            raise RiskRejected(message, rule="worst_price")
        quantity, send_size = self._accept_size(accept, quote, price, venue, market_id, rfq)
        request = self._record(
            market_id=market_id, key=key, account=account, type=OrderType.RFQ_TAKER, side=accept.side,
            quantity=quantity, price=price, book=book, trader=trader, tags=accept.tags,
            outcome=accept.outcome, action=accept.action,
            params={"rfq_id": accept.rfq_id or quote.rfq_id, "quote_id": quote.id,
                    "worst_price": str(accept.worst_price) if accept.worst_price is not None else None,
                    "amount": str(accept.amount) if accept.amount is not None else None},
        )
        decision = self.engine.evaluate_order_risk(request, venue=venue, account=account, caller_supplied_order_id=True)
        if not decision.ok:
            raise await self._refused(decision, operation="rfq_taker", market_id=market_id, book=book,
                                      extra={"quote_id": quote.id, "client_order_id": key})
        return (quote, send_size), request

    async def _accept_once(self, accept: AcceptQuote, *, venue: str, adapter: TradingExchange, account: Account,
                           key: str, held: dict[str, Any] | None, group: str | None = None) -> RfqQuote:
        (quote, send_size), request = await self._prepare_accept(accept, venue=venue, adapter=adapter,
                                                                 account=account, key=key, held=held, group=group)
        book = request.book
        await self._begin(request, key=key, venue=venue, account=account, operation="accept_quote", target=quote.id)
        self.engine.order_risk_checks.record_sent(request)
        self.following[self._quote_key(venue, quote.id)] = key
        sent = accept.model_copy(update={"client_order_id": key, "account": account,
                                         "rfq_id": accept.rfq_id or quote.rfq_id, "quantity": send_size,
                                         "amount": None})
        try:
            result = await adapter.accept_quote(sent)
        except (NetworkError, asyncio.TimeoutError) as exc:
            await self._publish("quote.in_doubt", {"client_order_id": key, "venue": venue, "quote_id": quote.id,
                                                   "error": f"{type(exc).__name__}: {exc}"}, key=key)
            raise SubmissionInDoubt(
                f"{venue}: no answer to the accept of quote {quote.id} ({type(exc).__name__}: {exc}); it may have "
                f"been taken. Retry with client_order_id {key!r}: the venue is read first and nothing is sent twice",
                client_order_id=key) from exc
        except ExchangeError as exc:
            state = IntentState.REJECTED if isinstance(exc, (OrderRejected, BadRequest)) else IntentState.FAILED
            await self.engine.journal.mark_intent(key, state, detail=str(exc))
            self.following.pop(self._quote_key(venue, quote.id), None)
            await self._publish("quote.failed", {**quote.model_dump(mode="json"), "client_order_id": key,
                                                 "reason": "accept_refused", "error": str(exc)},
                                key=self._quote_key(venue, quote.id))
            raise
        await self.engine.journal.mark_intent(key, IntentState.SENDING, detail=ACCEPTED_MARK)
        await self._guard_side(key, venue, expected=accept.side, reported=result.accepted_side,
                               quote_id=quote.id, where="accept")
        await self._observe(venue, result, extra={"client_order_id": key, "book": book})
        return result

    def _accept_size(self, accept: AcceptQuote, quote: RfqQuote, price: Decimal, venue: str, market_id: str,
                     rfq: Rfq | None) -> tuple[Decimal, Decimal | None]:
        """The contracts the accept trades (for risk and the record), and the
        size to send: `None` when it is the quote's full size."""
        full = quote.quantity_for(accept.outcome, accept.action)
        token = outcome_price(price, accept.outcome)
        worst = outcome_price(accept.worst_price, accept.outcome) if accept.worst_price is not None else None
        precision = self._precision(venue, market_id)
        if accept.quantity is not None:
            quantity = accept.quantity
            if full is not None and quantity > full:
                raise BadRequest(f"{venue}: {quantity} contracts is more than quote {quote.id} offers ({full})")
        elif accept.amount is not None:
            if full is not None and accept.amount >= full * token:
                quantity = full
            else:
                quantity = contracts_for_amount(accept.amount, token, precision, action=accept.action, worst_price=worst)
        elif full is not None:
            quantity = full
        elif rfq is not None and rfq.quantity is not None:
            quantity = rfq.quantity
        elif rfq is not None and rfq.amount is not None:
            quantity = contracts_for_amount(rfq.amount, token, precision, action=accept.action, worst_price=worst)
        else:
            raise BadRequest(f"{venue}: quote {quote.id} gives no size and the engine does not hold its RFQ; "
                             "give quantity or amount")
        return quantity, (None if full is not None and quantity == full else quantity)

    # -- following ------------------------------------------------------------

    async def resolve(self, intent: Intent) -> dict[str, Any]:
        """Resolve an RFQ intent against the venue: `Engine._resolve` (the
        sweep and `recover()`) hands every RFQ operation here."""
        base = {"client_order_id": intent.client_order_id, "venue": intent.venue, "operation": intent.operation}
        adapter = self.engine.adapters.get(intent.venue)
        if adapter is None:
            await self.engine.bus.publish("intent.unresolved", base | {"reason": "no adapter for this venue"})
            return base | {"result": "unresolved", "reason": "no adapter"}
        if intent.operation in FOLLOWED:
            return await self._resolve_followed(intent)
        if intent.operation in GROUP_OPERATIONS:
            return await self.engine.rfq_groups.resolve(intent)
        try:
            return await self._resolve_offer(adapter, intent, base)
        except Exception as exc:
            await self.engine.bus.publish("intent.unresolved", base | {"reason": f"{type(exc).__name__}: {exc}"})
            return base | {"result": "unresolved", "reason": str(exc)}

    async def _resolve_followed(self, intent: Intent) -> dict[str, Any]:
        base = {"client_order_id": intent.client_order_id, "venue": intent.venue, "operation": intent.operation}
        async with self.engine._lock(f"rfq-follow:{intent.client_order_id}"):
            current = await self.engine.journal.intent(intent.client_order_id)
            if current is None or current.state != IntentState.SENDING:
                return base | {"result": "adopted" if current and current.state == IntentState.SENT else "swept"}
            adapter = self.engine.adapters.get(intent.venue)
            request = OrderRequest.stored(current.request)
            self.following[self._quote_key(current.venue, current.target_order_id or "")] = current.client_order_id
            try:
                quote = await adapter.fetch_quote(current.target_order_id, rfq_id=request.params.get("rfq_id"))
            except RfqNotFound:
                if current.age_ms < self.config.lost_after_s * 1000:
                    return base | {"result": "pending", "age_ms": current.age_ms}
                await self._no_trade(current, request, reason="not_found", detail="the venue has no such quote")
                return base | {"result": "swept"}
            except Exception as exc:
                await self.engine.bus.publish("intent.unresolved", base | {"reason": f"{type(exc).__name__}: {exc}"})
                return base | {"result": "unresolved", "reason": str(exc)}
            return await self._advance(current, request, quote, base)

    async def _advance(self, intent: Intent, request: OrderRequest, quote: RfqQuote,
                       base: dict[str, Any]) -> dict[str, Any]:
        venue, key = intent.venue, intent.client_order_id
        taker = intent.operation == "accept_quote"
        extra = {"client_order_id": key, "book": request.book}
        if taker:
            await self._guard_side(key, venue, expected=request.side, reported=quote.accepted_side,
                                   quote_id=quote.id, where="quote")
        await self._observe(venue, quote, extra=extra)
        status = quote.status
        if status == QuoteStatus.EXECUTED:
            order_id = quote.taker_order_id if taker else quote.maker_order_id
            if not order_id:
                return base | {"result": "pending", "reason": "executed, no order id yet"}
            adapter = self.engine.adapters[venue]
            try:
                order = await adapter.fetch_order(order_id)
            except OrderNotFound:
                return base | {"result": "pending", "reason": f"order {order_id} not listed yet"}
            await self._adopt(intent, request, order)
            await self.engine.journal.mark_intent(key, IntentState.SENT, order_id=order.id, detail="executed")
            self.following.pop(self._quote_key(venue, quote.id), None)
            if taker:
                rfq_id = request.params.get("rfq_id")
                if rfq_id and f"{venue}:{rfq_id}" in self.rfqs:
                    await self._rfq_closed(venue, rfq_id, key=None, reason="executed", detail=quote.id)
            else:
                self.maker_quotes.pop(f"{venue}:{quote.id}", None)
                await self._save_working_set(venue)
            await self.engine.bus.publish("intent.adopted", base | {"order_id": order.id, "status": order.status.value})
            return base | {"result": "adopted", "order_id": order.id}
        if status in NO_TRADE:
            await self._no_trade(intent, request, reason=status.value, detail=f"quote {status.value}: nothing traded",
                                 quote=quote)
            return base | {"result": "swept"}
        lapsed = intent.age_ms >= self.config.lost_after_s * 1000
        if taker and status == QuoteStatus.OPEN and intent.detail != ACCEPTED_MARK and lapsed:
            await self._no_trade(intent, request, reason="never_accepted", state=IntentState.REJECTED,
                                 detail="the venue never took the accept; nothing traded", quote=quote)
            return base | {"result": "swept"}
        if not taker and status == QuoteStatus.ACCEPTED and intent.detail != CONFIRMED_MARK and lapsed:
            await self._no_trade(intent, request, reason="never_confirmed", state=IntentState.REJECTED,
                                 detail="the confirm never reached the venue; the last look lapses", quote=quote)
            return base | {"result": "swept"}
        return base | {"result": "pending", "status": status.value}

    async def _no_trade(self, intent: Intent, request: OrderRequest, *, reason: str, detail: str,
                        state: str = IntentState.FAILED, quote: RfqQuote | None = None) -> None:
        await self.engine.journal.mark_intent(intent.client_order_id, state, detail=detail)
        quote_id = intent.target_order_id or ""
        self.following.pop(self._quote_key(intent.venue, quote_id), None)
        payload = (quote.model_dump(mode="json") if quote else {"id": quote_id, "venue": intent.venue})
        await self._publish("quote.failed", {**payload, "client_order_id": intent.client_order_id,
                                             "operation": intent.operation, "reason": reason, "detail": detail,
                                             "book": request.book},
                            key=self._quote_key(intent.venue, quote_id))
        if state == IntentState.REJECTED:
            await self.engine.bus.publish("intent.swept", {"client_order_id": intent.client_order_id,
                                                           "venue": intent.venue, "operation": intent.operation,
                                                           "reason": detail})

    async def _adopt(self, intent: Intent, request: OrderRequest, order: Order) -> None:
        """Record the order an executed quote made through the normal order
        path, on the side the venue reports, then book its fills."""
        engine = self.engine
        stamp = request
        if order.side != request.side:
            await self._guard_side(intent.client_order_id, intent.venue, expected=request.side, reported=order.side,
                                   quote_id=intent.target_order_id or "", where="order")
            stamp = request.model_copy(update={"side": order.side, "outcome": None, "action": None})
        stamped = engine._stamp(order, stamp)
        if stamped.outcome is not None and yes_side(stamped.outcome, stamped.action or "buy") != stamped.side:
            # The venue's own outcome contradicts its side: keep the side.
            stamped = stamped.model_copy(update={"outcome": None, "action": None})
        if stamped.client_order_id is None:
            stamped = stamped.model_copy(update={"client_order_id": intent.client_order_id})
        stored = await engine.journal.order(order.venue, order.id)
        if stored is not None and stored.filled > stamped.filled:
            stamped = engine._stamp(stored, stamp)
        stamped = stamped.model_copy(update={"tags": {**stamped.tags, "rfq_id": str(request.params.get("rfq_id") or ""),
                                                      "quote_id": intent.target_order_id or ""}})
        await engine.journal.upsert_order(stamped, event="order.accepted")
        engine._remember(stamped)
        await engine.bus.publish("order.accepted", stamped.model_dump(mode="json"),
                                 key=f"{order.venue}:{order.id}", persist=False)
        if stamped.status not in (OrderStatus.OPEN, OrderStatus.PENDING):
            await engine.bus.publish(f"order.{stamped.status.value}", stamped.model_dump(mode="json"),
                                     key=f"{order.venue}:{order.id}", persist=False)
        await engine.managed_order_controller.on_order(stamped)
        adapter = engine.adapters[order.venue]
        if adapter.has.get("fetch_my_trades"):
            try:
                fills = await adapter.fetch_my_trades(market_id=order.market_id)
            except Exception as exc:  # the poll loop books them later
                log.warning("synpath.engine: reading the fills of %s on %s failed: %s", order.id, order.venue, exc)
                fills = []
            for fill in fills:
                if fill.order_id == order.id:
                    await engine.on_fill(fill)

    async def claim_fill(self, fill: Any) -> Order | None:
        """A fill for an order the journal does not know: if an accept or a
        confirm is being followed on that venue, resolve it first, so the
        fill is booked against its order (book, outcome and action) rather
        than unattributed."""
        pending = [k for q, k in self.following.items() if q.startswith(f"{fill.venue}:")]
        for key in pending:
            intent = await self.engine.journal.intent(key)
            if intent is not None and intent.state == IntentState.SENDING:
                await self._resolve_followed(intent)
        return await self.engine.journal.order(fill.venue, fill.order_id)

    async def wait(self, key: str, *, venue: str, timeout_s: float = 30.0) -> RfqQuote:
        """Follow one accept or confirm until its quote is final, or until
        `timeout_s`; return the quote as it then stands."""
        deadline = time.monotonic() + timeout_s
        while True:
            intent = await self.engine.journal.intent(key)
            if intent is None:
                raise BadRequest(f"no RFQ intent {key!r}")
            if intent.state == IntentState.SENDING:
                await self._resolve_followed(intent)
                intent = await self.engine.journal.intent(key)
            quote = self._last_quote.get(self._quote_key(venue, intent.target_order_id or ""))
            if intent.state != IntentState.SENDING or time.monotonic() >= deadline:
                if quote is None:
                    adapter = self.engine.adapters[venue]
                    quote = await adapter.fetch_quote(intent.target_order_id,
                                                      rfq_id=OrderRequest.stored(intent.request).params.get("rfq_id"))
                return quote
            await asyncio.sleep(self.config.rfq_poll_interval_s)

    async def on_quote(self, quote: RfqQuote, *, venue: str | None = None) -> None:
        """A quote update from a venue stream (or anything else): publish what
        changed and move the accept or confirm following it."""
        venue = venue or quote.venue
        qkey = self._quote_key(venue, quote.id)
        key = self.following.get(qkey)
        if key is not None:
            intent = await self.engine.journal.intent(key)
            if intent is not None and intent.state == IntentState.SENDING:
                async with self.engine._lock(f"rfq-follow:{key}"):
                    intent = await self.engine.journal.intent(key)
                    if intent is not None and intent.state == IntentState.SENDING:
                        base = {"client_order_id": key, "venue": venue, "operation": intent.operation}
                        await self._advance(intent, OrderRequest.stored(intent.request), quote, base)
                        return
        if f"{venue}:{quote.rfq_id}" in self.rfqs or f"{venue}:{quote.id}" in self.maker_quotes:
            await self._observe(venue, quote)
            if quote.status == QuoteStatus.EXECUTED and f"{venue}:{quote.rfq_id}" in self.rfqs:
                await self._rfq_closed(venue, quote.rfq_id, key=None, reason="executed", detail=quote.id)
            if quote.mine and quote.is_terminal:
                self.maker_quotes.pop(f"{venue}:{quote.id}", None)
                await self._save_working_set(venue)

    # -- quoter ---------------------------------------------------------------

    async def _incoming_rfq(self, venue: str, adapter: TradingExchange, rfq_id: str) -> Rfq:
        known = self.incoming.get(f"{venue}:{rfq_id}")
        if known is not None:
            return known
        return await adapter.fetch_rfq(rfq_id)

    def _maker_size(self, quote: MakerQuote, rfq: Rfq) -> tuple[Decimal | None, Decimal | None]:
        """The most contracts and the most money the quote could commit us
        to, on its worse side: buying YES at the bid costs the bid, selling
        YES at the ask costs (holding NO) one minus the ask."""
        sides = []
        if quote.bid is not None:
            sides.append((quote.bid, quote.bid, ONE - quote.bid))      # requester sells YES: pays 1 - bid per contract
        if quote.ask is not None:
            sides.append((quote.ask, ONE - quote.ask, quote.ask))      # requester buys YES: pays ask
        quantity = quote.quantity or rfq.quantity
        worst_q: Decimal | None = None
        worst_n: Decimal | None = None
        for _, ours, theirs in sides:
            q = quantity if quantity is not None else (rfq.amount / theirs if rfq.amount is not None and theirs > 0 else None)
            if q is None:
                continue
            worst_q = q if worst_q is None else max(worst_q, q)
            n = q * ours
            worst_n = n if worst_n is None else max(worst_n, n)
        return worst_q, worst_n

    async def send_quote(self, quote: MakerQuote, *, venue: str | None = None) -> RfqQuote:
        """Answer an incoming RFQ. Journaled; checked against the halt, a
        paused book, the restricted list and the size limits. The engine
        holds `expires_at` and cancels the quote when it passes."""
        await self._lease()
        venue = self._venue_for(venue, account=quote.account)
        adapter = self._adapter(venue, "rfq_quoter")
        account = self._account(venue, quote.account)
        rfq = await self._incoming_rfq(venue, adapter, quote.rfq_id)
        return await self._quote_once(quote, venue=venue, adapter=adapter, account=account, rfq=rfq,
                                      operation="send_quote", replaces=None)

    async def replace_quote(self, quote_id: str, quote: MakerQuote, *, venue: str | None = None) -> RfqQuote:
        """Swap a live quote for new prices; the result may carry a new id."""
        await self._lease()
        held = self._maker_held(quote_id, venue)
        venue = self._venue_for(venue or (held or {}).get("venue"), account=quote.account)
        adapter = self._adapter(venue, "rfq_quoter")
        account = self._account(venue, quote.account)
        rfq = await self._incoming_rfq(venue, adapter, quote.rfq_id)
        return await self._quote_once(quote, venue=venue, adapter=adapter, account=account, rfq=rfq,
                                      operation="replace_quote", replaces=quote_id)

    def _maker_held(self, quote_id: str, venue: str | None) -> dict[str, Any] | None:
        if venue is not None:
            return self.maker_quotes.get(f"{venue}:{quote_id}")
        for key, held in self.maker_quotes.items():
            if key.endswith(f":{quote_id}"):
                return held
        return None

    async def _quote_once(self, quote: MakerQuote, *, venue: str, adapter: TradingExchange, account: Account,
                          rfq: Rfq, operation: str, replaces: str | None) -> RfqQuote:
        market_id = rfq.instrument_id
        now = _now_ms()
        if quote.expires_at is not None and quote.expires_at <= now:
            raise BadRequest("expires_at is in the past")
        quantity, notional = self._maker_size(quote, rfq)
        decision = self.engine.order_risk_checks.check_offer(market_id, venue=venue, book=quote.book, quantity=quantity,
                                                notional=notional)
        if not decision.ok:
            raise await self._refused(decision, operation="rfq_quoter", market_id=market_id, book=quote.book,
                                      extra={"rfq_id": quote.rfq_id})
        prefix = "rfq-quote" if replaces is None else f"rfq-replace:{replaces}"
        key = quote.client_order_id or f"{prefix}:{venue}:{uuid.uuid4().hex[:16]}"
        request = self._record(
            market_id=market_id, key=key, account=account, type=OrderType.RFQ_MAKER,
            side=Side.BUY if quote.bid is not None else Side.SELL, quantity=quote.quantity or rfq.quantity,
            book=quote.book, trader=quote.trader, tags=quote.tags,
            params={"rfq_id": quote.rfq_id, "maker_quote": quote.model_dump(mode="json"), "replaces": replaces},
        )
        await self._begin(request, key=key, venue=venue, account=account, operation=operation, target=replaces)
        sent = quote.model_copy(update={"expires_at": None, "client_order_id": key, "account": account})
        try:
            result = (await adapter.send_quote(sent) if replaces is None else await adapter.replace_quote(replaces, sent))
        except (NetworkError, asyncio.TimeoutError) as exc:
            await self._publish("quote.in_doubt", {"client_order_id": key, "venue": venue, "rfq_id": quote.rfq_id,
                                                   "error": f"{type(exc).__name__}: {exc}"}, key=key)
            raise SubmissionInDoubt(f"{venue}: no answer to the quote send ({type(exc).__name__}: {exc}); the "
                                    "engine resolves it against the venue", client_order_id=key) from exc
        except ExchangeError as exc:
            await self.engine.journal.mark_intent(key, IntentState.REJECTED, detail=str(exc))
            raise
        tracked = await self._track_quote(venue, result, quote=quote, key=key, market_id=market_id, replaces=replaces)
        if rfq.combo is not None:
            # After the send: the legs are for display, and a quote has a
            # window to meet (docs/rfq-combo.md, "Quoting in time").
            await self.save_combo(rfq.combo)
        return tracked

    async def _track_quote(self, venue: str, result: RfqQuote, *, quote: MakerQuote, key: str, market_id: str,
                           replaces: str | None) -> RfqQuote:
        result = result.model_copy(update={"expires_at": quote.expires_at, "mine": True,
                                           "market_id": result.market_id or market_id})
        await self.engine.journal.mark_intent(key, IntentState.SENT, order_id=result.id)
        if replaces is not None:
            old = self.maker_quotes.pop(f"{venue}:{replaces}", None)
            okey = self._quote_key(venue, replaces)
            self._status[okey] = QuoteStatus.CANCELED
            await self._publish("quote.replaced", {"replaced": replaces, "status": QuoteStatus.CANCELED.value,
                                                   "quote_id": result.id, "rfq_id": result.rfq_id,
                                                   "book": quote.book, "old": old}, key=okey)
        self.maker_quotes[f"{venue}:{result.id}"] = {
            "id": result.id, "rfq_id": result.rfq_id, "venue": venue, "market_id": result.market_id,
            "expires_at": quote.expires_at, "book": quote.book, "trader": quote.trader,
            "quantity": str(quote.quantity) if quote.quantity is not None else None,
            "bid": str(quote.bid) if quote.bid is not None else None,
            "ask": str(quote.ask) if quote.ask is not None else None, "client_order_id": key,
        }
        await self._save_working_set(venue)
        qkey = self._quote_key(venue, result.id)
        self._status[qkey] = QuoteStatus.OPEN
        self._last_quote[qkey] = result
        await self._publish("quote.sent", {**result.model_dump(mode="json"), "status": QuoteStatus.OPEN.value,
                                           "client_order_id": key, "book": quote.book, "replaces": replaces}, key=qkey)
        return result

    async def cancel_quote(self, quote_id: str, *, rfq_id: str | None = None, venue: str | None = None,
                           reason: str = "canceled by the caller") -> None:
        """Withdraw one of our quotes (or decline it in the last look).
        Journaled with its own key; never risk-checked."""
        held = self._maker_held(quote_id, venue)
        venue = self._venue_for(venue or (held or {}).get("venue"))
        adapter = self._adapter(venue, "rfq_quoter")
        rfq_id = rfq_id or (held or {}).get("rfq_id")
        key = f"rfq-quote-cancel:{venue}:{quote_id}"
        async with self.engine._lock(key):
            prior = await self.engine.journal.intent(key)
            if prior is not None and prior.state == IntentState.SETTLED:
                return
            account = self._account(venue)
            request = self._record(market_id=(held or {}).get("market_id") or rfq_id or quote_id, key=key,
                                   account=account, type=OrderType.RFQ_MAKER, book=(held or {}).get("book"),
                                   params={"rfq_id": rfq_id, "reason": reason})
            await self._begin(request, key=key, venue=venue, account=account, operation="cancel_quote", target=quote_id)
            try:
                await adapter.cancel_quote(quote_id, rfq_id=rfq_id)
            except RfqNotFound:
                pass
            except (NetworkError, asyncio.TimeoutError) as exc:
                raise SubmissionInDoubt(f"{venue}: no answer to the quote cancel ({exc})", client_order_id=key) from exc
            await self.engine.journal.mark_intent(key, IntentState.SETTLED, order_id=quote_id, detail=reason)
            await self._quote_gone(venue, quote_id, rfq_id=rfq_id, reason=reason,
                                   status=QuoteStatus.EXPIRED if reason == "expired" else QuoteStatus.CANCELED)

    async def _quote_gone(self, venue: str, quote_id: str, *, rfq_id: str | None, reason: str,
                          status: QuoteStatus) -> None:
        held = self.maker_quotes.pop(f"{venue}:{quote_id}", None)
        await self._save_working_set(venue)
        qkey = self._quote_key(venue, quote_id)
        known = self._last_quote.get(qkey)
        if known is not None:
            await self._observe(venue, known.model_copy(update={"status": status}),
                                extra={"reason": reason, "book": (held or {}).get("book")})
        else:
            self._status[qkey] = status
            await self._publish(STATUS_EVENT[status], {"id": quote_id, "rfq_id": rfq_id, "venue": venue,
                                                       "status": status.value, "reason": reason, "mine": True}, key=qkey)

    async def confirm_quote(self, quote: RfqQuote | str, *, rfq_id: str | None = None, venue: str | None = None,
                            wait: bool = False, timeout_s: float = 30.0) -> RfqQuote:
        """Confirm a quote of ours a requester accepted: the maker's last
        look. Commits capital, so it runs the order risk rules on the side and
        size that would trade (an `rfq_maker` order), is journaled
        (`rfq-confirm:{venue}:{quote_id}`), and is followed to execution like
        an accept. Refused while halted: the last look lapses."""
        await self._lease()
        quote_id = quote if isinstance(quote, str) else quote.id
        held = self._maker_held(quote_id, venue)
        venue = self._venue_for(venue or (quote.venue if isinstance(quote, RfqQuote) else None)
                                or (held or {}).get("venue"))
        adapter = self._adapter(venue, "rfq_quoter")
        rfq_id = rfq_id or (quote.rfq_id if isinstance(quote, RfqQuote) else None) or (held or {}).get("rfq_id")
        key = f"rfq-confirm:{venue}:{quote_id}"
        async with self.engine._lock(key):
            prior = await self.engine.journal.intent(key)
            if prior is not None and prior.state in (IntentState.SENDING, IntentState.SENT, IntentState.SETTLED):
                if prior.state == IntentState.SENDING:
                    await self._resolve_followed(prior)
                result = self._last_quote.get(self._quote_key(venue, quote_id)) \
                    or await adapter.fetch_quote(quote_id, rfq_id=rfq_id)
            else:
                result = await self._confirm_once(quote_id, rfq_id=rfq_id, venue=venue, adapter=adapter,
                                                  held=held, key=key)
        if wait:
            return await self.wait(key, venue=venue, timeout_s=timeout_s)
        return result

    async def _confirm_once(self, quote_id: str, *, rfq_id: str | None, venue: str, adapter: TradingExchange,
                            held: dict[str, Any] | None, key: str) -> RfqQuote:
        current = await adapter.fetch_quote(quote_id, rfq_id=rfq_id)
        if current.status != QuoteStatus.ACCEPTED:
            raise OrderRejected(f"{venue}: quote {quote_id} is {current.status.value}; only an accepted quote is "
                                "confirmed", reason="quote_not_accepted")
        if current.accepted_side is None:
            raise BadRequest(f"{venue}: quote {quote_id} does not say which side the requester took")
        requester = current.accepted_side
        ours = _opposite(requester)
        price = current.ask if requester == Side.BUY else current.bid
        if price is None:
            raise BadRequest(f"{venue}: quote {quote_id} has no price on the side the requester took")
        quantity = current.ask_quantity if requester == Side.BUY else current.bid_quantity
        if quantity is None and held and held.get("quantity"):
            quantity = Decimal(held["quantity"])
        if quantity is None:
            rfq = self.incoming.get(f"{venue}:{current.rfq_id}")
            if rfq is None:
                try:
                    rfq = await adapter.fetch_rfq(current.rfq_id)
                except Exception:
                    rfq = None
            if rfq is not None and rfq.quantity is not None:
                quantity = rfq.quantity
            elif rfq is not None and rfq.amount is not None:
                theirs = price if requester == Side.BUY else ONE - price
                quantity = rfq.amount / theirs
        if quantity is None:
            raise BadRequest(f"{venue}: quote {quote_id} gives no size to confirm")
        market_id = current.market_id or (held or {}).get("market_id")
        if not market_id:
            raise BadRequest(f"{venue}: quote {quote_id} names no instrument")
        account = self._account(venue)
        outcome, action = self._maker_leg(ours, venue, market_id, account)
        book = (held or {}).get("book")
        request = self._record(
            market_id=market_id, key=key, account=account, type=OrderType.RFQ_MAKER, side=ours, quantity=quantity,
            price=price, book=book, trader=(held or {}).get("trader"), outcome=outcome, action=action,
            params={"rfq_id": current.rfq_id, "quote_id": quote_id, "requester_side": requester.value},
        )
        decision = self.engine.evaluate_order_risk(request, venue=venue, account=account, caller_supplied_order_id=True)
        if not decision.ok:
            raise await self._refused(decision, operation="rfq_quoter", market_id=market_id, book=book,
                                      extra={"quote_id": quote_id, "client_order_id": key})
        await self._begin(request, key=key, venue=venue, account=account, operation="confirm_quote", target=quote_id)
        self.engine.order_risk_checks.record_sent(request)
        self.following[self._quote_key(venue, quote_id)] = key
        try:
            result = await adapter.confirm_quote(current)
        except (NetworkError, asyncio.TimeoutError) as exc:
            await self._publish("quote.in_doubt", {"client_order_id": key, "venue": venue, "quote_id": quote_id,
                                                   "error": f"{type(exc).__name__}: {exc}"}, key=key)
            raise SubmissionInDoubt(f"{venue}: no answer to the confirm of quote {quote_id} ({exc}); the engine "
                                    "resolves it against the venue", client_order_id=key) from exc
        except ExchangeError as exc:
            state = IntentState.REJECTED if isinstance(exc, (OrderRejected, BadRequest)) else IntentState.FAILED
            await self.engine.journal.mark_intent(key, state, detail=str(exc))
            self.following.pop(self._quote_key(venue, quote_id), None)
            raise
        await self.engine.journal.mark_intent(key, IntentState.SENDING, detail=CONFIRMED_MARK)
        await self._observe(venue, result, extra={"client_order_id": key, "book": book})
        return result

    def _maker_leg(self, side: Side, venue: str, market_id: str, account: Account) -> tuple[str, str]:
        """What our side of a confirmed quote does: it closes what we hold
        where it can (selling YES out of a long, NO out of a short), else it
        opens, as `default_leg` says."""
        held = self.engine.order_risk_checks._net_contracts(account.key, venue, market_id)
        if side == Side.SELL and held > 0:
            return "yes", "sell"
        if side == Side.BUY and held < 0:
            return "no", "sell"
        return default_leg(side)

    # -- halt -----------------------------------------------------------------

    async def on_halt(self, scope: str = "*") -> dict[str, Any]:
        """Cancel every open RFQ we asked and every live quote we made, on
        every venue in scope: the ones the engine holds and any the venue
        lists. One venue failing does not stop the others."""
        out: dict[str, Any] = {"rfqs": {}, "quotes": {}}
        groups = getattr(self.engine, "rfq_groups", None)
        if groups is not None:
            # Groups first, so each records why its members went.
            halted = await groups.on_halt(scope)
            if halted:
                out["groups"] = halted
        for venue, adapter in self.engine.adapters.items():
            if scope not in ("*", venue):
                continue
            if adapter.has.get("rfq_taker"):
                out["rfqs"][venue] = await self._halt_rfqs(venue, adapter)
            if adapter.has.get("rfq_quoter"):
                out["quotes"][venue] = await self._halt_quotes(venue, adapter)
        return out

    async def _halt_rfqs(self, venue: str, adapter: TradingExchange) -> int | str:
        wanted = {k.split(":", 1)[1] for k in self.rfqs if k.startswith(f"{venue}:")}
        try:
            wanted |= {r.id for r in await adapter.fetch_rfqs(status=RfqStatus.OPEN)}
        except Exception as exc:
            log.warning("synpath.engine: listing open RFQs on %s for the halt failed: %s", venue, exc)
        count, failed = 0, []
        for rfq_id in sorted(wanted):
            try:
                await self.cancel_rfq(rfq_id, venue=venue, reason="halt")
                count += 1
            except Exception as exc:
                failed.append(f"{rfq_id}: {type(exc).__name__}: {exc}")
        return count if not failed else f"canceled {count}, failed: {'; '.join(failed)}"

    async def _halt_quotes(self, venue: str, adapter: TradingExchange) -> int | str:
        wanted = {k.split(":", 1)[1]: v.get("rfq_id") for k, v in self.maker_quotes.items() if k.startswith(f"{venue}:")}
        try:
            for quote in await adapter.fetch_my_quotes(status=QuoteStatus.OPEN):
                wanted.setdefault(quote.id, quote.rfq_id)
        except Exception as exc:
            log.warning("synpath.engine: listing our quotes on %s for the halt failed: %s", venue, exc)
        count, failed = 0, []
        for quote_id, rfq_id in sorted(wanted.items()):
            try:
                await self.cancel_quote(quote_id, rfq_id=rfq_id, venue=venue, reason="halt")
                count += 1
            except Exception as exc:
                failed.append(f"{quote_id}: {type(exc).__name__}: {exc}")
        return count if not failed else f"canceled {count}, failed: {'; '.join(failed)}"

    # -- recovery of offers ---------------------------------------------------

    async def _resolve_offer(self, adapter: TradingExchange, intent: Intent, base: dict[str, Any]) -> dict[str, Any]:
        """A create, cancel or quote send in doubt, read back from the venue."""
        venue, key, op = intent.venue, intent.client_order_id, intent.operation
        request = OrderRequest.stored(intent.request)
        young = intent.age_ms < self.config.lost_after_s * 1000
        if op == "create_rfq":
            since = intent.created_ts - RECENT_MS
            known = {k.split(":", 1)[1] for k in self.rfqs if k.startswith(f"{venue}:")}
            for rfq in await adapter.fetch_rfqs(status=RfqStatus.OPEN, market_id=request.market_id):
                if rfq.id not in known and (rfq.created_at is None or rfq.created_at >= since):
                    await self._track_rfq(venue, rfq, book=request.book, trader=request.trader, key=key)
                    return base | {"result": "adopted", "rfq_id": rfq.id}
        elif op == "cancel_rfq":
            try:
                rfq = await adapter.fetch_rfq(intent.target_order_id)
            except RfqNotFound:
                rfq = None
            if rfq is None or rfq.status == RfqStatus.CLOSED:
                await self._rfq_closed(venue, intent.target_order_id, key=key, reason="canceled",
                                       detail=str(request.params.get("reason") or ""))
                return base | {"result": "adopted"}
            await self.engine.journal.mark_intent(key, IntentState.FAILED, detail="the venue still shows the RFQ open")
            return base | {"result": "swept"}
        elif op == "cancel_quote":
            try:
                quote = await adapter.fetch_quote(intent.target_order_id, rfq_id=request.params.get("rfq_id"))
            except RfqNotFound:
                quote = None
            if quote is None or quote.is_terminal:
                await self.engine.journal.mark_intent(key, IntentState.SETTLED, order_id=intent.target_order_id)
                await self._quote_gone(venue, intent.target_order_id, rfq_id=request.params.get("rfq_id"),
                                       reason=str(request.params.get("reason") or "canceled"),
                                       status=QuoteStatus.CANCELED)
                return base | {"result": "adopted"}
            await self.engine.journal.mark_intent(key, IntentState.FAILED, detail="the venue still shows the quote live")
            return base | {"result": "swept"}
        elif op in ("send_quote", "replace_quote"):
            rfq_id = request.params.get("rfq_id")
            known = {k.split(":", 1)[1] for k in self.maker_quotes if k.startswith(f"{venue}:")}
            since = intent.created_ts - RECENT_MS
            for quote in await adapter.fetch_my_quotes(rfq_id=rfq_id, status=QuoteStatus.OPEN):
                if quote.id not in known and (quote.created_at is None or quote.created_at >= since):
                    maker = MakerQuote.model_validate(request.params["maker_quote"])
                    await self._track_quote(venue, quote, quote=maker, key=key, market_id=request.market_id,
                                            replaces=request.params.get("replaces"))
                    return base | {"result": "adopted", "quote_id": quote.id}
        if young:
            await self.engine.bus.publish("intent.pending", base | {"age_ms": intent.age_ms})
            return base | {"result": "pending", "age_ms": intent.age_ms}
        await self.engine.journal.mark_intent(key, IntentState.LOST, detail="not found at the venue")
        await self.engine.bus.publish("intent.swept", base | {"reason": "nothing at the venue matches it"})
        return base | {"result": "swept"}

    async def restore(self) -> None:
        """At start: the working sets from the journal, the combos, the
        accepts and confirms to follow, and the venues' quote streams."""
        journal = self.engine.journal
        for venue in self.engine.adapters:
            for name, target in ((f"rfq:open:{venue}", "rfqs"), (f"rfq:maker_quotes:{venue}", "maker_quotes")):
                raw = await journal.cursor(name)
                if not raw:
                    continue
                for row in json.loads(raw):
                    if target == "rfqs":
                        self.rfqs[f"{venue}:{row['rfq']['id']}"] = row
                    else:
                        self.maker_quotes[f"{venue}:{row['id']}"] = row
        for event in await journal.events_page(kinds=["combo.saved"], limit=10_000):
            market_id = event.payload.get("market_id")
            if market_id and market_id not in self.combos:
                self.combos[market_id] = Combo.model_validate(event.payload)
        in_doubt = [i for i in await journal.intents(state=IntentState.SENDING)
                    if i.operation in FOLLOWED and i.target_order_id]
        for intent in in_doubt:
            self.following[self._quote_key(intent.venue, intent.target_order_id)] = intent.client_order_id
        await self._reseed(in_doubt)
        for venue, adapter in self.engine.adapters.items():
            subscribe = getattr(adapter, "subscribe_quotes", None)
            if callable(subscribe) and venue not in self._subscribed:
                self._subscribed.add(venue)
                subscribe(lambda quote, v=venue: self.on_quote(quote, venue=v))
            self._subscribe_incoming(venue, adapter)

    async def _reseed(self, in_doubt: list[Intent]) -> None:
        """Hand each adapter what its venue cannot list back after a restart
        (`TradingExchange.restore_rfqs`): our open RFQs from the working set,
        and the quotes whose accept or confirm is in doubt, at the status the
        intent implies. Polymarket global needs it (its gateway lists no
        RFQs); venues that list their own ignore it."""
        journal = self.engine.journal
        for venue, adapter in self.engine.adapters.items():
            restore = getattr(adapter, "restore_rfqs", None)
            if not callable(restore):
                continue
            rfqs: dict[str, Rfq] = {}
            for key, held in self.rfqs.items():
                if key.startswith(f"{venue}:"):
                    try:
                        rfqs[held["rfq"]["id"]] = Rfq.model_validate(held["rfq"])
                    except Exception as exc:
                        log.warning("synpath.engine: RFQ %s in the working set does not validate: %s", key, exc)
            quotes: list[RfqQuote] = []
            for intent in in_doubt:
                if intent.venue != venue:
                    continue
                request = OrderRequest.stored(intent.request)
                rfq_id = str(request.params.get("rfq_id") or "")
                answered = intent.detail in (ACCEPTED_MARK, CONFIRMED_MARK)
                if intent.operation == "accept_quote":
                    if rfq_id and rfq_id not in rfqs:
                        # Gone from the working set: the create's own event has it.
                        found = await journal.events_page(kinds=["rfq.created"], keys=[f"{venue}:rfq:{rfq_id}"], limit=1)
                        if found:
                            body = {k: v for k, v in found[0].payload.items() if k in Rfq.model_fields}
                            try:
                                rfqs[rfq_id] = Rfq.model_validate(body)
                            except Exception as exc:
                                log.warning("synpath.engine: RFQ %s cannot be rebuilt from its event: %s", rfq_id, exc)
                    price = request.price if request.price is not None else ONE
                    buy = request.side == Side.BUY
                    quotes.append(RfqQuote(id=intent.target_order_id, rfq_id=rfq_id or "", venue=venue,
                                           market_id=request.market_id, status=QuoteStatus.ACCEPTED,
                                           ask=price if buy else None, bid=None if buy else price,
                                           accepted_side=request.side, mine=False, updated_at=intent.created_ts,
                                           info={"client_order_id": intent.client_order_id, "answered": answered}))
                else:
                    # The latest event of our quote that carries its full
                    # body (the accepted one has the size the requester took).
                    found = [e for e in await journal.events_page(
                        kinds=["quote.sent", "quote.accepted", "quote.confirmed"],
                        keys=[self._quote_key(venue, intent.target_order_id)], limit=10)
                        if e.payload.get("rfq_id") and e.payload.get("venue")]
                    if not found:
                        continue
                    body = {k: v for k, v in found[0].payload.items() if k in RfqQuote.model_fields}
                    status = QuoteStatus.CONFIRMED if intent.detail == CONFIRMED_MARK else QuoteStatus.ACCEPTED
                    try:
                        quotes.append(RfqQuote.model_validate({**body, "status": status, "mine": True}))
                    except Exception as exc:
                        log.warning("synpath.engine: quote %s cannot be rebuilt from its event: %s",
                                    intent.target_order_id, exc)
            if not rfqs and not quotes:
                continue
            try:
                restore(list(rfqs.values()), quotes)
            except Exception as exc:
                log.warning("synpath.engine: re-seeding RFQs at %s failed: %s", venue, exc)

    # -- incoming RFQs: pushed, or polled as a fallback -----------------------

    def on_incoming_rfq(self, callback: Callable[[Rfq], Any]) -> Callable[[], None]:
        """Register a quoting callback: `callback(rfq)` (sync or async) runs
        as soon as an incoming RFQ is recorded and `rfq.incoming` is out,
        before anything else, so it can `engine.send_quote` inside the
        venue's window (Polymarket global 400 ms, SX Bet 60 s). On a venue
        that pushes, that is the moment the stream delivers it; elsewhere
        the 2 s poll. A callback that raises is logged. Returns the
        unsubscribe."""
        self._incoming_callbacks.append(callback)
        return lambda: self._incoming_callbacks.remove(callback) if callback in self._incoming_callbacks else None

    async def on_incoming(self, venue: str, rfq: Rfq) -> bool:
        """An incoming RFQ, from a venue stream or the poll. In order: it is
        recorded in `incoming`, `rfq.incoming` is published (not journaled),
        and every `on_incoming_rfq` callback runs, awaited. Its combo's legs
        are journaled after that. `False` for one already known (or ours)."""
        key = f"{venue}:{rfq.id}"
        if key in self.incoming or rfq.mine:
            return False
        self.incoming[key] = rfq
        await self._publish("rfq.incoming", rfq.model_dump(mode="json"), key=f"{venue}:rfq:{rfq.id}", persist=False)
        for callback in list(self._incoming_callbacks):
            try:
                result = callback(rfq)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                log.exception("synpath.engine: an incoming-RFQ callback failed on %s", key)
        if rfq.combo is not None:
            await self.save_combo(rfq.combo)
        return True

    def _subscribe_incoming(self, venue: str, adapter: TradingExchange) -> None:
        if venue in self._incoming_subscribed or not adapter.has.get("rfq_quoter"):
            return
        subscribe = getattr(adapter, "subscribe_incoming_rfqs", None)
        if not callable(subscribe):
            return
        self._incoming_subscribed.add(venue)
        if subscribe(lambda rfq, v=venue: self.on_incoming(v, rfq)) is not None:
            self.pushed.add(venue)

    async def start_streams(self) -> None:
        """Start each pushing venue's incoming stream, once, and take what it
        already heard. A venue whose stream will not start is polled instead."""
        if self.config.rfq_incoming_interval_s is None or not self.config.rfq_stream_incoming:
            return
        for venue in sorted(self.pushed - self._streams_started):
            adapter = self.engine.adapters.get(venue)
            if adapter is None:
                continue
            self._streams_started.add(venue)
            try:
                await adapter.start_incoming_rfqs()
                for rfq in await adapter.fetch_incoming_rfqs():
                    await self.on_incoming(venue, rfq)
            except Exception as exc:
                log.warning("synpath.engine: the incoming-RFQ stream at %s did not start (%s); polling it instead",
                            venue, exc)
                self.pushed.discard(venue)

    def _prune_pushed(self) -> None:
        """Pushed RFQs leave `incoming` a minute after their window (or after
        ten minutes when they name none); polled ones leave when the poll no
        longer lists them."""
        now = _now_ms()
        for key, rfq in list(self.incoming.items()):
            if key.split(":", 1)[0] not in self.pushed:
                continue
            end = rfq.expires_at if rfq.expires_at is not None else (rfq.created_at or now) + 540_000
            if end + 60_000 < now:
                self.incoming.pop(key, None)

    # -- the loop -------------------------------------------------------------

    async def run(self) -> None:
        """The follower: accepts and confirms, quotes on our RFQs, our maker
        quotes and their expiry, and the incoming feed (venue streams are
        started first, so pushed RFQs flow from the start)."""
        try:
            await self.start_streams()
        except Exception:
            log.exception("synpath.engine: starting the incoming-RFQ streams failed")
        while self.engine.running:
            await asyncio.sleep(self.config.rfq_poll_interval_s)
            try:
                await self.tick()
            except Exception:
                log.exception("synpath.engine: the RFQ follower failed")

    async def tick(self) -> None:
        """One pass of the follower, for a host or a test to drive."""
        await self.start_streams()
        await self.follow()
        await self.watch_rfqs()
        await self.watch_maker_quotes()
        groups = getattr(self.engine, "rfq_groups", None)
        if groups is not None:
            await groups.tick()
        self._prune_pushed()
        interval = self.config.rfq_incoming_interval_s
        if interval is not None and time.monotonic() - self._incoming_ts >= interval:
            self._incoming_ts = time.monotonic()
            await self.watch_incoming()

    async def follow(self) -> None:
        for intent in await self.engine.journal.intents(state=IntentState.SENDING):
            if intent.operation not in FOLLOWED or self.engine._lock(intent.client_order_id).locked():
                continue
            try:
                await self._resolve_followed(intent)
            except Exception:
                log.exception("synpath.engine: following %s failed", intent.client_order_id)

    async def watch_rfqs(self) -> None:
        for key, held in list(self.rfqs.items()):
            venue, rfq_id = key.split(":", 1)
            adapter = self.engine.adapters.get(venue)
            if adapter is None:
                continue
            try:
                quotes = await adapter.fetch_quotes(rfq_id)
            except RfqNotFound:
                await self._rfq_closed(venue, rfq_id, key=None, reason="expired")
                continue
            except Exception as exc:
                log.warning("synpath.engine: reading quotes on RFQ %s at %s failed: %s", rfq_id, venue, exc)
                continue
            for quote in quotes:
                if self._quote_key(venue, quote.id) in self.following:
                    continue
                await self._observe(venue, quote, extra={"book": held.get("book")})
                if quote.status == QuoteStatus.EXECUTED and key in self.rfqs:
                    await self._rfq_closed(venue, rfq_id, key=None, reason="executed", detail=quote.id)
            if key in self.rfqs:
                await self._watch_rfq_status(venue, rfq_id, key, held, adapter)

    async def _watch_rfq_status(self, venue: str, rfq_id: str, key: str, held: dict[str, Any], adapter: Any) -> None:
        """Close our RFQ here when its venue has closed it (its expiry, or
        any close the quotes do not show): read every
        `RFQ_STATUS_INTERVAL_S`, and at once past its `expires_at`."""
        expires = (held.get("rfq") or {}).get("expires_at")
        due = expires is not None and int(expires) <= _now_ms()
        now = time.monotonic()
        if not due and now - self._rfq_read.get(key, 0.0) < RFQ_STATUS_INTERVAL_S:
            return
        if not callable(getattr(adapter, "fetch_rfq", None)):
            if due:
                await self._rfq_closed(venue, rfq_id, key=None, reason="expired", detail="past its expires_at")
            return
        self._rfq_read[key] = now
        try:
            rfq = await adapter.fetch_rfq(rfq_id)
        except RfqNotFound:
            await self._rfq_closed(venue, rfq_id, key=None, reason="expired")
            return
        except Exception as exc:
            log.info("synpath.engine: reading RFQ %s at %s failed: %s", rfq_id, venue, exc)
            return
        if rfq.status == RfqStatus.CLOSED:
            reason = rfq.closed_reason.value if rfq.closed_reason is not None else "expired"
            await self._rfq_closed(venue, rfq_id, key=None, reason=reason if reason != "replaced" else "canceled",
                                   detail="closed at the venue")
        elif rfq.expires_at is not None and held.get("rfq", {}).get("expires_at") != rfq.expires_at:
            held["rfq"]["expires_at"] = rfq.expires_at

    async def watch_maker_quotes(self) -> None:
        now = _now_ms()
        by_venue: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        for key, held in list(self.maker_quotes.items()):
            by_venue.setdefault(held["venue"], []).append((key, held))
        for venue, rows in by_venue.items():
            adapter = self.engine.adapters.get(venue)
            if adapter is None:
                continue
            for _, held in rows:
                expires = held.get("expires_at")
                qkey = self._quote_key(venue, held["id"])
                if expires is not None and int(expires) <= now and qkey not in self.following:
                    status = self._status.get(qkey)
                    if status in (None, QuoteStatus.OPEN, QuoteStatus.ACCEPTED):
                        try:
                            await self.cancel_quote(held["id"], rfq_id=held.get("rfq_id"), venue=venue, reason="expired")
                        except Exception as exc:
                            log.warning("synpath.engine: expiring quote %s at %s failed: %s", held["id"], venue, exc)
            try:
                mine = {q.id: q for q in await adapter.fetch_my_quotes()}
            except Exception as exc:
                log.warning("synpath.engine: reading our quotes at %s failed: %s", venue, exc)
                continue
            for key, held in rows:
                quote = mine.get(held["id"])
                if quote is None or key not in self.maker_quotes:
                    continue
                if self._quote_key(venue, quote.id) in self.following:
                    continue
                await self._observe(venue, quote.model_copy(update={"expires_at": held.get("expires_at")}),
                                    extra={"book": held.get("book")})
                if quote.is_terminal:
                    self.maker_quotes.pop(key, None)
                    await self._save_working_set(venue)

    async def watch_incoming(self) -> None:
        """The fallback poll, for quoting venues without a stream."""
        for venue, adapter in self.engine.adapters.items():
            if not adapter.has.get("rfq_quoter") or venue in self.pushed:
                continue
            try:
                page = await adapter.fetch_incoming_rfqs()
            except Exception as exc:
                log.warning("synpath.engine: reading incoming RFQs at %s failed: %s", venue, exc)
                continue
            seen = set()
            for rfq in page:
                seen.add(f"{venue}:{rfq.id}")
                await self.on_incoming(venue, rfq)
            for key in [k for k in self.incoming if k.startswith(f"{venue}:") and k not in seen]:
                self.incoming.pop(key, None)
