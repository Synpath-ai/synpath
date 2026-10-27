"""The engine: one place that decides, records, sends, and knows what happened.

An adapter sends an order. An engine is what you need when the process can
die between deciding and sending, when two people share an account, when a
venue answers late, and when someone has to explain afterwards what was
refused and why. It owns four things: the journal, the ledger, the risk
rules, and the venue adapters.

**Submitting is four steps, in this order.** Risk decides, the intent is
written to disk with its client order id, the venue is called, the answer is
written. The write happens before the call because that is the only
ordering that survives a crash: a restarted engine finds an intent marked
`sending`, asks the venue whether an order with that client order id exists,
and either adopts it or sweeps it. The other order would lose orders
silently or send them twice.

**An idempotency key per operation.** Creating uses the request's client
order id; cancelling and editing get their own, so a retried cancel is the
same cancel and a retried edit does not apply twice. All three are journaled
before they leave.

**Recovery is a first-class path, not an error handler.** `recover()` runs
at startup: it rebuilds the ledger from the journal's fills, resolves every
in-doubt intent against the venue, and reports what it adopted, swept or
could not explain. `sweep()` runs on a timer and does the same for intents
that have been in doubt longer than the engine will wait.

**The kill switch uses the venue's own mechanism first.** `halt()` engages
the switch, then applies its policy: `cancel` pulls every resting order
through the adapter's cancel-all (a Kalshi order group, a Polymarket
heartbeat stopping, one call rather than one per order), `hold` leaves them
and refuses new ones, `rearm` refuses new ones and lifts itself after a
timer. New orders are refused by the risk rules the moment it engages, so a
halt cannot race a submit.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Iterable, Mapping

from .. import ids
from ..bucket import Bucket, bucket_id_of, is_bucket_id
from ..errors import BadRequest, ExchangeError, NetworkError
from ..trading.base import TradingExchange
from ..trading.errors import OrderNotFound, OrderRejected, RiskRejected
from ..trading.types import (
    VENUE_ORDER_TYPES, Account, EditRequest, Fill, HeldBy, Order, OrderRequest, OrderStatus, OrderType, Position,
    Settlement, Side, Precision,
)
from ..trading.instruments import MarketMaster
from .execution_scope import MANAGED_NAMESPACE, BucketNotFound, validate_execution_accounts
from .events import EventBus
from .journal import IntentState, LeaseLost, now_ms
from .storage import JournalStore, create_journal
from .submissions import CancelUnconfirmed, SubmissionInDoubt, SubmissionPending, request_identity
from .ledger import Ledger
from .orders import ManagedOrderController, as_gtd
from .orders.manager import REGISTRY
from .risk import Decision, HaltPolicy, RiskConfig, RiskEngine
from .rfq import FOLLOWED, RFQ_OPERATIONS, RfqDesk
from .rfq_group import Matcher, RfqGroupDesk

log = logging.getLogger("synpath.engine")

ZERO = Decimal("0")


@dataclass
class EngineConfig:
    """How this engine runs. The risk rules live in `RiskConfig`."""

    journal_path: str = "synpath.db"
    storage_backend: str = "sqlite"
    storage_schema: str = "public"
    user_id: str = field(default_factory=lambda: os.environ.get("SYNPATH_USER_ID", "local-user"))
    lease_name: str = "engine"
    lease_ttl_ms: int = 30_000
    lease_renew_s: float = 10.0
    in_doubt_timeout_s: float = 20.0
    """How long an unanswered send stays in doubt before the sweep resolves it."""
    lost_after_s: float = 60.0
    """How long the venue must keep saying it has no such order before the
    engine believes it. Venues do not list an order the instant they accept
    it -- Kalshi's demo takes a few hundred milliseconds -- so declaring one
    lost too early would abandon a live order."""
    sweep_interval_s: float = 10.0
    reconcile_interval_s: float = 60.0
    poll_interval_s: float = 5.0
    """How often to poll orders and fills when no stream is attached."""
    halt_policy: HaltPolicy = "cancel"
    require_lease: bool = True
    managed_tick_s: float = 1.0
    """How often engine-held orders get their timer. A TWAP slice, an
    iceberg reload and a peg's minimum stay are all measured against it."""
    session_timezone: str | None = "Europe/London"
    """Where "today" ends, for Day orders. Europe/London by default, including seasonal clock changes."""
    session_end: str = "23:59:59"
    rfq_poll_interval_s: float = 1.0
    """How often the RFQ follower reads accepted and confirmed quotes, the
    quotes on our open RFQs and our own maker quotes (and expires them)."""
    rfq_incoming_interval_s: float | None = 2.0
    """How often to poll incoming RFQs on quoting venues without a stream
    (`rfq.incoming`); venues that push (`subscribe_incoming_rfqs`) are not
    polled. `None` turns the feed off, streams included."""
    rfq_stream_incoming: bool = True
    """Start the venues' incoming-RFQ streams (Polymarket global's quoter
    session, SX Bet's parlay channel) when the RFQ loop starts. `False`
    leaves starting them to the caller; RFQs they push still reach the engine."""


@dataclass(slots=True)
class Recovery:
    """What `recover()` found. Every number here is a thing that was in doubt."""

    in_doubt: int = 0
    adopted: int = 0
    """Intents whose order was found at the venue after all."""
    swept: int = 0
    """Intents with no order at the venue: nothing was sent."""
    unresolved: int = 0
    """Still in doubt: the venue could not be reached. Trading stays blocked
    for these instruments until it can."""
    fills_replayed: int = 0
    orders_open: int = 0
    managed: int = 0
    """Engine-held orders brought back and still running."""
    details: list[dict[str, Any]] = field(default_factory=list)

    def summary(self, *, detail_limit: int = 20) -> dict[str, Any]:
        return {
            "in_doubt": self.in_doubt, "adopted": self.adopted, "swept": self.swept, "unresolved": self.unresolved,
            "fills_replayed": self.fills_replayed, "orders_open": self.orders_open, "managed": self.managed,
            "details": self.details[:detail_limit],
        }


POST_ONLY_KINDS = ("iceberg", "peg", "twap", "stop_limit")
"""Engine types whose children rest on the book, where `post_only` means
something. A TWAP only in its `limit` style."""


STOP_KINDS = frozenset({"stop_market", "stop_limit", "trailing_stop"})


def check_managed_fields(request: OrderRequest, *, now_ms: int) -> None:
    """Refuse the order fields an engine-held type cannot honour, rather than
    ignore them. The outcome and action always pass to the children; `expires_at` is
    the whole order's expiry."""
    kind = str(request.params.get("managed_kind") or request.type.value)
    if request.post_only:
        if kind in ("oco", "bracket"):
            raise BadRequest(f"post_only goes on the {kind}'s legs, not on the order itself")
        style = str(request.params.get("style") or "limit")
        if kind not in POST_ONLY_KINDS or (kind == "twap" and style != "limit"):
            raise BadRequest(f"post_only is for types that rest on the book (iceberg, peg, limit-style twap, stop_limit); "
                             f"a {kind} order takes liquidity")
    if request.expires_at is not None and request.expires_at <= now_ms:
        raise BadRequest("expires_at is in the past")
    if kind in STOP_KINDS:
        if "trigger_source" in request.params:
            raise BadRequest("trigger_source is a field of the order, not a param")
        for key in ("max_slippage", "protection"):
            if key in request.params:
                raise BadRequest(f"a {kind} takes no {key}: the engine sets the market child's worst price itself")


class Engine:
    """Order entry with a memory.

    ```python
    engine = Engine({"kalshi": kalshi_adapter}, EngineConfig(journal_path="trading.db"))
    async with engine:
        await engine.submit(OrderRequest(market_id="kalshi:KXX", side=Side.BUY,
                                         quantity=Decimal("10"), price=Decimal("0.42"), book="alpha"))
    ```
    """

    def __init__(
        self,
        adapters: Mapping[str, TradingExchange],
        config: EngineConfig | None = None,
        *,
        risk: RiskConfig | None = None,
        journal: JournalStore | None = None,
        accounts: Mapping[str, Account] | None = None,
        clock: Any = time.time,
        matcher: Matcher | None = None,
    ):
        self.config = config or EngineConfig()
        if MANAGED_NAMESPACE in adapters or MANAGED_NAMESPACE in (accounts or {}):
            raise ValueError("the managed storage namespace cannot be configured as a venue")
        self.adapters: dict[str, TradingExchange] = dict(adapters)
        if not self.config.user_id.strip():
            raise ValueError("user_id must not be empty")
        self.journal = journal or create_journal(backend=self.config.storage_backend,
                                               path=self.config.journal_path, user_id=self.config.user_id, schema=self.config.storage_schema)
        if self.journal.user_id != self.config.user_id:
            raise ValueError("execution engine and journal must use the same user_id")
        self.bus = EventBus(self.journal)
        self.ledger = Ledger()
        # Holds the order risk rules, trading halt state and recent submission counters.
        self.order_risk_checks = RiskEngine(risk or RiskConfig(), ledger=self.ledger, clock=clock)
        self.accounts: dict[str, Account] = dict(accounts or {})
        for venue in self.adapters:
            # Every configured venue trades under some account; name the
            # default one, so listing accounts shows what orders will use.
            adapter_account = getattr(self.adapters[venue], "account", None)
            self.accounts.setdefault(venue, adapter_account if isinstance(adapter_account, Account)
                                     else Account(venue=venue))
        self.clock = clock
        self.running = False
        self.started_ts: int | None = None
        self._tasks: list[asyncio.Task] = []
        self._dispatch_queue: asyncio.Queue[str] = asyncio.Queue()
        self._queued_parents: set[str] = set()
        self.dispatch_running = False
        self.dispatch_parent: str | None = None
        self._locks: dict[str, asyncio.Lock] = {}
        self.market_close: dict[str, int] = {}
        """Resolution timestamps, for the closing-soon guard; filled by the caller."""
        self.event_of: dict[str, str] = {}
        """Market to event id, for the per-event cap."""
        self._open_cache: dict[str, Order] = {}
        # Creates, restores and advances engine-held orders, and links their venue children.
        self.managed_order_controller = ManagedOrderController(self)
        self.books: dict[str, Any] = {}
        """The local books engine-held orders watch, fed by `on_book`."""
        self.last_trade: dict[str, Decimal] = {}
        self.last_trade_timestamps: dict[str, int] = {}
        self.market_master = MarketMaster()
        self.rfq = RfqDesk(self)
        """RFQs and combos, both sides (docs/rfq-combo.md); the methods below delegate to it."""
        self.rfq_groups = RfqGroupDesk(self, matcher=matcher)
        """Cross-venue RFQ groups over the desk (docs/rfq-combo.md, "Cross-venue RFQ"). `matcher`
        maps a market onto another venue; by default the hosted matching service."""

    # -- lifecycle ------------------------------------------------------------

    async def start(self) -> Recovery:
        await self.journal.open()
        if self.config.require_lease:
            await self.journal.acquire_lease(self.config.lease_name, ttl_ms=self.config.lease_ttl_ms)
        version = await self.journal.save_config("risk", self.order_risk_checks.config.model_dump(mode="json"))
        self.order_risk_checks.config_version = version
        recovery = await self.recover()
        self.running = True
        self.started_ts = int(self.clock() * 1000)
        await self.bus.publish("engine.started", {
            "owner": self.journal.owner, "risk_version": version, "recovery": recovery.summary(),
        })
        return recovery

    def background(self) -> dict[str, Any]:
        """Every loop a running engine needs, by name, as coroutines to schedule.
        One list for `run()` and for anything that hosts the engine itself (the
        daemon, `synpath serve`), so a host cannot leave one out: without the
        managed loop a TWAP never slices and nothing engine-held ever expires."""
        return {
            "lease": self._lease_loop(),
            "sweep": self._sweep_loop(),
            "poll": self._poll_loop(),
            "managed": self._managed_loop(),
            "dispatch": self._dispatch_loop(),
            "rfq": self.rfq.run(),
        }

    async def run(self) -> None:
        """Start, then keep the background work going until `stop()`."""
        if not self.running:
            await self.start()
        loop = asyncio.get_running_loop()
        self._tasks = [loop.create_task(coro, name=f"synpath-engine-{name}") for name, coro in self.background().items()]
        await asyncio.gather(*self._tasks)

    async def stop(self) -> None:
        self.running = False
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks = []
        if self.journal.is_open:
            await self.bus.publish("engine.stopped", {"owner": self.journal.owner})
        await self.journal.close()

    async def __aenter__(self) -> "Engine":
        await self.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.stop()

    # -- submitting -----------------------------------------------------------

    async def submit(self, request: OrderRequest, *, venue: str | None = None, risk_reference_price: Decimal | None = None,
                     precision: Precision | None = None,
                     authorize_execution_accounts: Callable[[dict[str, Account]], None] | None = None) -> Order:
        """Retry the same instruction, never repeat an uncertain venue send.

        `risk_reference_price` overrides the book midpoint for the limit-price risk check.
        """
        request = await self._prepare_submission_request(request)
        # Bucket parents capture and authorize their cross-venue execution scope separately.
        if is_bucket_id(request.market_id):
            if venue is not None:
                raise BadRequest("a bucket parent has no singular execution venue")
            return await self._submit_bucket(request, risk_reference_price=risk_reference_price,
                                             authorize_execution_accounts=authorize_execution_accounts)
        return await self._submit_with_retry_protection(
            request, venue=venue, risk_reference_price=risk_reference_price, precision=precision,
        )

    async def _prepare_submission_request(self, request: OrderRequest) -> OrderRequest:
        """Check execution ownership and entry-point rules, then assign the worker's user."""
        # A caller cannot submit as another user.
        if request.user_id not in (None, self.config.user_id):
            raise BadRequest("user_id is assigned by the execution worker")
        # Only the active lease holder may submit.
        if self.config.require_lease and not await self.journal.holds_lease():
            raise LeaseLost("this execution worker no longer holds the journal lease")
        # Persist the configured owner on the instruction.
        request = request.model_copy(update={"user_id": self.config.user_id})
        # RFQ orders require the quote acceptance/confirmation workflow.
        if request.type in (OrderType.RFQ_TAKER, OrderType.RFQ_MAKER):
            raise BadRequest(f"a {request.type.value} order comes from a quote: use accept_quote or confirm_quote")
        return request

    async def _submit_with_retry_protection(
        self, request: OrderRequest, *, venue: str | None,
        risk_reference_price: Decimal | None, precision: Precision | None,
    ) -> Order:
        """Reuse a durable submission or reserve its identity before executing it.

        Venue orders and stops use this retry reservation. Other managed types
        retain their own lifecycle path through _submit_once(). Bucket parents
        are dispatched separately by submit().
        """
        # Other managed types keep their existing lifecycle path.
        supported = VENUE_ORDER_TYPES | {OrderType.STOP_MARKET, OrderType.STOP_LIMIT}
        if request.type not in supported or request.params.get("walk"):
            return await self._submit_once(request, venue=venue, risk_reference_price=risk_reference_price, precision=precision)

        # Establish the account and stable order ID before looking for earlier attempts.
        venue, account, request, caller_supplied_order_id = self._prepare_submission_identity(request, venue=venue)

        # Serialize lookup, reservation and execution for this submission ID.
        async with self._lock(f"submission:{request.client_order_id}"):
            request, previous_result = await self._resume_or_reserve_submission(request, venue=venue, account=account)
            if previous_result is not None:
                return previous_result

            # Execute only after confirming this instruction can safely proceed.
            try:
                return await self._submit_once(
                    request, venue=venue, risk_reference_price=risk_reference_price,
                    caller_supplied_order_id=caller_supplied_order_id, precision=precision,
                )
            except (RiskRejected, BadRequest, OrderRejected) as error:
                # Preserve definite failures so retries observe the same rejection.
                await self._record_submission_rejection(request, error)
                raise

    def _prepare_submission_identity(
        self, request: OrderRequest, *, venue: str | None,
    ) -> tuple[str, Account, OrderRequest, bool]:
        """Resolve the destination and assign an order ID while retaining its origin."""
        venue = venue or self._venue_of(request)
        account = request.account or self.accounts.get(venue) or Account(venue=venue)
        # Caller-assigned IDs bypass the accidental-duplicate check, not same-ID retry protection.
        caller_supplied_order_id = request.client_order_id is not None
        request = request.model_copy(update={
            "account": account,
            "client_order_id": request.client_order_id or f"sp-{uuid.uuid4().hex[:20]}",
        })
        return venue, account, request, caller_supplied_order_id

    async def _resume_or_reserve_submission(
        self, request: OrderRequest, *, venue: str, account: Account,
    ) -> tuple[OrderRequest, Order | None]:
        """Return an earlier outcome or the effective instruction safe to execute.

        The caller must hold the submission lock through any subsequent execution.
        """
        previous_submission = await self.journal.submission(request.client_order_id)
        if previous_submission is None:
            return await self._reserve_new_submission(request, venue=venue, account=account)

        # Reusing an ID for another instruction or account must never return an unrelated order.
        if previous_submission["request"] != request_identity(request):
            raise BadRequest("client_order_id already identifies a different instruction or account")
        previous_result = await self._existing_submission_result(previous_submission)
        if previous_result is not None:
            return request, previous_result

        # Keep the original effective expiry; retrying a DAY order must not extend its life.
        return OrderRequest.stored(previous_submission["effective_request"]), None

    async def _reserve_new_submission(
        self, request: OrderRequest, *, venue: str, account: Account,
    ) -> tuple[OrderRequest, Order | None]:
        """Reserve a new submission, reconciling any older intent before allowing execution."""
        # Freeze session-relative DAY expiry before saving the instruction used for execution.
        effective_request = as_gtd(request, now_s=self.clock(), timezone_name=self.config.session_timezone,
                                   session_end=self.config.session_end)

        # Older durable actions may already own this ID without a submission reservation.
        prior_intent = await self.journal.intent(request.client_order_id)
        if prior_intent is not None:
            prior_request = OrderRequest.stored(prior_intent.request)
            if prior_intent.operation != "create" or request_identity(prior_request) != request_identity(effective_request):
                raise BadRequest("client_order_id already identifies a different durable action")
        elif await self.journal.order_by_client_id(request.client_order_id) is not None:
            raise BadRequest("client_order_id belongs to an older order without a verifiable instruction")

        await self.journal.record_submission(request, effective_request, venue=venue, account=account)
        if prior_intent is not None:
            # Resolve the older send before execution; an unanswered send may already be accepted.
            submission = await self.journal.submission(request.client_order_id)
            previous_result = await self._existing_submission_result(submission)
            if previous_result is not None:
                return effective_request, previous_result
        return effective_request, None

    async def _record_submission_rejection(
        self, request: OrderRequest, error: RiskRejected | BadRequest | OrderRejected,
    ) -> None:
        """Persist a definite failure without rewriting a send that may have reached the venue."""
        # Only an unsent, planned intent can be marked rejected here.
        intent = await self.journal.intent(request.client_order_id)
        if intent is not None and intent.state == IntentState.PLANNED:
            await self.journal.mark_intent(request.client_order_id, IntentState.REJECTED, detail=str(error))
        await self.journal.reject_submission(request.client_order_id,
            {"kind": type(error).__name__, "message": str(error), "rule": getattr(error, "rule", None)})

    async def _existing_submission_result(self, submission: dict[str, Any]) -> Order | None:
        if submission["result_id"]:
            parent = self.managed_order_controller.get(submission["result_id"])
            if parent is None:
                stored = await self.journal.managed_one(submission["result_id"])
                parent = self._rebuilt(*stored) if stored else None
            if parent is not None:
                if parent.live and self.managed_order_controller.get(parent.id) is None:
                    self.managed_order_controller.remember(parent)
                return parent.as_order()
            result = await self.journal.order(submission["venue"], submission["result_id"])
            if result is None:
                raise SubmissionPending("the accepted submission has no readable result; refusing to repeat it",
                                        client_order_id=submission["client_order_id"])
            return result
        if submission["state"] == "rejected":
            error = submission["error"]
            if error["kind"] == "RiskRejected":
                raise RiskRejected(error["message"], rule=error["rule"])
            if error["kind"] == "OrderRejected":
                raise OrderRejected(error["message"])
            raise BadRequest(error["message"])
        intent = await self.journal.intent(submission["client_order_id"])
        if intent is None or intent.state == IntentState.PLANNED:
            return None
        if intent.state == IntentState.SENDING:
            await self._resolve(intent)
        result = await self.journal.order_by_client_id(submission["client_order_id"])
        if result is not None:
            return result
        if intent.state in (IntentState.REJECTED, IntentState.FAILED, IntentState.SETTLED):
            raise OrderRejected(intent.detail or "the earlier submission ended without an accepted order")
        raise SubmissionPending("the earlier venue send is unresolved; refusing to send it again",
                                client_order_id=submission["client_order_id"])

    async def _submit_once(self, request: OrderRequest, *, venue: str | None = None,
                           risk_reference_price: Decimal | None = None, caller_supplied_order_id: bool | None = None,
                           precision: Precision | None = None) -> Order:
        """Risk, journal, send, record. Raises `RiskRejected` before anything
        is sent, or the venue's own error after.

        `risk_reference_price` is the reference for the limit-price risk check.
        `caller_supplied_order_id` skips the accidental-duplicate check when true;
        when omitted, infer it before generating an order ID.
        """
        if request.user_id not in (None, self.config.user_id):
            raise BadRequest("user_id is assigned by the execution worker")
        request = request.model_copy(update={"user_id": self.config.user_id})
        if is_bucket_id(request.market_id):
            return await self._submit_bucket(request, risk_reference_price=risk_reference_price)
        venue = venue or self._venue_of(request)
        adapter = self._adapter(venue)
        account = request.account or self.accounts.get(venue) or Account(venue=venue)
        # Day is not a venue concept here: the expiry must outlive this process.
        request = as_gtd(request, now_s=self.clock(), timezone_name=self.config.session_timezone,
                         session_end=self.config.session_end)
        if request.type not in VENUE_ORDER_TYPES:
            return await self.submit_managed(request, venue=venue, account=account, risk_reference_price=risk_reference_price)
        if request.type == OrderType.MARKET and request.params.get("walk"):
            # An explicit request for the engine's book walk rather than the
            # adapter's single immediate limit.
            walked = request.model_copy(update={"params": {**request.params, "managed_kind": "market_engine"}})
            return await self.submit_managed(walked, venue=venue, account=account, risk_reference_price=risk_reference_price)
        if caller_supplied_order_id is None:
            caller_supplied_order_id = request.client_order_id is not None
        client_order_id = request.client_order_id or f"sp-{uuid.uuid4().hex[:20]}"
        request = request.model_copy(update={"client_order_id": client_order_id, "account": account})

        await self._check_venue_order_risk(
            request, venue=venue, account=account, risk_reference_price=risk_reference_price,
            caller_supplied_order_id=caller_supplied_order_id,
        )
        order = await self._send_venue_order_with_journaled_intent(
            request, venue=venue, account=account, adapter=adapter, precision=precision,
        )
        return await self._record_accepted_venue_order(order, request, venue=venue)

    async def _check_venue_order_risk(
        self, request: OrderRequest, *, venue: str, account: Account,
        risk_reference_price: Decimal | None, caller_supplied_order_id: bool,
    ) -> None:
        """Reject and explain a failed risk check before any venue intent is written."""
        client_order_id = request.client_order_id
        decision = self.evaluate_order_risk(
            request, venue=venue, account=account, risk_reference_price=risk_reference_price,
            caller_supplied_order_id=caller_supplied_order_id,
        )
        if not decision.ok:
            await self.bus.publish("risk.rejected", {
                "rule": decision.rule, "message": decision.message, "client_order_id": client_order_id,
                "market_id": request.market_id, "book": request.book, "config_version": decision.config_version,
                **decision.detail,
            }, key=client_order_id)
            raise RiskRejected(decision.message, rule=decision.rule)

    async def _send_venue_order_with_journaled_intent(
        self, request: OrderRequest, *, venue: str, account: Account,
        adapter: TradingExchange, precision: Precision | None,
    ) -> Order:
        """Persist the intent before sending; leave unanswered sends for reconciliation.

        Keep the sending transition and venue call under the same dispatch lock.
        A timeout cannot prove rejection, so it must never trigger a fresh send.
        """
        client_order_id = request.client_order_id
        await self.journal.record_intent(request, client_order_id=client_order_id, venue=venue, account=account)
        async with self._lock(f"{venue}:{client_order_id}"):
            await self.journal.mark_intent(client_order_id, IntentState.SENDING, bump_attempt=True)
            self.order_risk_checks.record_sent(request)
            try:
                order = (await adapter.create_order_with_precision(request, precision)
                         if precision is not None else await adapter.create_order(request))
            except OrderRejected as exc:
                await self.journal.mark_intent(client_order_id, IntentState.REJECTED, detail=str(exc))
                await self.bus.publish("order.rejected", {"client_order_id": client_order_id, "error": str(exc)}, key=client_order_id)
                raise
            except (NetworkError, asyncio.TimeoutError) as exc:
                # In doubt on purpose: the intent stays `sending` so the sweep
                # asks the venue rather than this process guessing. The caller
                # gets the in-doubt error with the id to retry under.
                await self.bus.publish("order.in_doubt", {
                    "client_order_id": client_order_id, "venue": venue, "error": f"{type(exc).__name__}: {exc}",
                }, key=client_order_id)
                raise SubmissionInDoubt(
                    f"{venue}: no answer to the order send ({type(exc).__name__}: {exc}); it may have been "
                    f"accepted. Retry with client_order_id {client_order_id!r}: the venue is checked first "
                    "and the order is never sent twice", client_order_id=client_order_id) from exc
            except ExchangeError as exc:
                await self.journal.mark_intent(client_order_id, IntentState.FAILED, detail=str(exc))
                await self.bus.publish("order.failed", {"client_order_id": client_order_id, "error": str(exc)}, key=client_order_id)
                raise
        return order

    async def _record_accepted_venue_order(self, order: Order, request: OrderRequest, *, venue: str) -> Order:
        """Save the venue response before marking its intent sent and announcing acceptance."""
        client_order_id = request.client_order_id
        order = self._stamp(order, request)
        await self.journal.upsert_order(order, event="order.accepted")
        self._remember(order)
        await self.journal.mark_intent(client_order_id, IntentState.SENT, order_id=order.id)
        await self.bus.publish("order.accepted", order.model_dump(mode="json"), key=f"{venue}:{order.id}", persist=False)
        return order

    async def _submit_bucket(self, request: OrderRequest, *, risk_reference_price: Decimal | None,
                             authorize_execution_accounts: Callable[[dict[str, Account]], None] | None = None) -> Order:
        """Capture, authorize and persist one cross-venue instruction.

        The mutable bucket is read once. A reservation retains the captured
        bucket/account scope if acceptance is interrupted before parent creation.
        Retrying an accepted client ID returns its original parent.
        """
        if request.type != OrderType.MARKET:
            raise BadRequest("an order on a bucket is a market order: set type to market")
        if request.price is None:
            raise BadRequest("a market order on a bucket needs price: the worst price you accept")
        if request.account is not None:
            raise BadRequest("a bucket parent has no singular account; configure each venue's execution account")
        request = request.model_copy(update={"client_order_id": request.client_order_id or f"sp-{uuid.uuid4().hex[:20]}"})
        async with self._lock(f"submission:{request.client_order_id}"):
            previous = await self.journal.submission(request.client_order_id)
            if previous is not None:
                if previous["request"] != request_identity(request):
                    raise BadRequest("client_order_id already identifies a different instruction")
                effective = OrderRequest.stored(previous["effective_request"])
                accounts = validate_execution_accounts({venue: Account.model_validate(account)
                    for venue, account in effective.params["execution_accounts"].items()})
                if authorize_execution_accounts:
                    authorize_execution_accounts(accounts)
                result = await self._existing_submission_result(previous)
                if result is not None:
                    return result
            else:
                if await self.journal.order_by_client_id(request.client_order_id) is not None:
                    raise BadRequest("client_order_id belongs to an older bucket order without a verifiable instruction; look it up by parent ID")
                row = await self.journal.bucket(bucket_id_of(request.market_id))
                if row is None:
                    raise BucketNotFound(f"{request.market_id}: no such bucket in this journal")
                bucket = Bucket.model_validate(row)
                if bucket.status != "active":
                    raise BadRequest(f"{request.market_id}: the bucket is {bucket.status}")
                bucket = bucket.routable()
                missing = bucket.venues() - set(self.adapters)
                if missing:
                    raise BadRequest(f"no adapter configured for {', '.join(sorted(missing))}")
                accounts = validate_execution_accounts({venue: self.accounts.get(venue) or Account(venue=venue)
                                                        for venue in bucket.venues()})
                if authorize_execution_accounts:
                    authorize_execution_accounts(accounts)
                params = {**request.params, "managed_kind": "routed_limit", "bucket": bucket.model_dump(mode="json"),
                          "execution_accounts": {venue: account.model_dump(mode="json") for venue, account in accounts.items()}}
                effective = request.model_copy(update={"params": params})
                await self.journal.record_submission(request, effective, venue=MANAGED_NAMESPACE, account=None)
            try:
                bucket = Bucket.model_validate(effective.params["bucket"])
                self._evaluate_bucket_risk(effective, bucket, accounts)
                return await self.submit_managed(effective, venue=None, account=None,
                                                  execution_accounts=accounts, risk_reference_price=risk_reference_price)
            except (RiskRejected, BadRequest) as error:
                await self.journal.reject_submission(request.client_order_id,
                    {"kind": type(error).__name__, "message": str(error), "rule": getattr(error, "rule", None)})
                raise

    def _evaluate_bucket_risk(self, request: OrderRequest, bucket: Bucket, accounts: dict[str, Account]) -> None:
        """Check the instruction once, then full capacity in each possible destination.

        Destinations are alternatives, not summed exposure. Only actual venue
        children count as resting venue orders; parent projections are not orders
        at an exchange. No capacity is reserved by these admission checks.
        """
        decision = self.order_risk_checks.evaluate_order_risk(request, venue=None, account=None, instruction_only=True)
        if not decision.ok:
            raise RiskRejected(decision.message, rule=decision.rule)
        resting = [order for order in self.open_orders() if order.held_by == HeldBy.VENUE]
        for member in sorted(bucket.members, key=lambda item: item.market_id):
            venue = ids.venue_of(member.market_id)
            self.assert_execution_account(venue, accounts[venue])
            outcome = request.outcome
            if member.flip and outcome is not None:
                outcome = "no" if outcome == "yes" else "yes"
            projected = request.model_copy(update={"market_id": member.market_id,
                "side": (Side.SELL if request.side == Side.BUY else Side.BUY) if member.flip else request.side,
                "price": Decimal("1") - request.price if member.flip else request.price,
                "outcome": outcome, "account": accounts[venue], "tags": {**request.tags, "parent": "admission"}})
            decision = self.order_risk_checks.evaluate_order_risk(projected, venue=venue, account=accounts[venue],
                open_orders=[order for order in resting if order.venue == venue],
                market_close_ts=self.market_close.get(member.market_id), event_id=self.event_of.get(member.market_id),
                venue_notional=sum((abs(state.contracts) for (key, _, _), state in self.ledger.positions.items()
                                    if key == accounts[venue].key), ZERO))
            if not decision.ok:
                raise RiskRejected(f"{venue}/{accounts[venue].key}: {decision.message}", rule=decision.rule)

    def assert_execution_account(self, venue: str, account: Account) -> None:
        """Refuse account-bound I/O when a saved routed account no longer matches.

        The engine supports one configured execution identity per venue. It
        cannot reconcile or cancel the old account through replacement credentials.
        """
        configured = self.accounts.get(venue) or Account(venue=venue)
        adapter_account = getattr(self.adapters.get(venue), "account", None)
        if (account.venue != venue or account != configured
                or (isinstance(adapter_account, Account) and adapter_account != account)):
            raise SubmissionPending(f"{venue}: saved execution account {account.key} differs from configured "
                                    f"{configured.key}; restore the original account before recovery or execution")

    async def guard_routed_order_account(self, order: Order) -> None:
        parent_id = order.parent_id or order.tags.get("parent")
        if parent_id:
            stored = await self.journal.managed_one(parent_id)
            if stored and stored[0] == "routed_limit":
                if order.account is None:
                    raise SubmissionPending("routed child has no persisted execution account")
                self.assert_execution_account(order.venue, order.account)

    async def guard_routed_venue_accounts(self, venue: str) -> None:
        for saved_key in await self.journal.routed_account_keys(venue):
            configured = self.accounts.get(venue) or Account(venue=venue)
            if saved_key != configured.key:
                raise SubmissionPending(f"{venue}: saved routed account {saved_key} differs from configured {configured.key}")
            self.assert_execution_account(venue, configured)

    async def save_bucket(self, bucket: Bucket) -> Bucket:
        """Store or update a bucket definition in this engine's journal."""
        bucket.check()
        await self.journal.save_bucket(bucket.model_dump(mode="json"))
        return bucket

    async def submit_managed(self, request: OrderRequest, *, venue: str | None, account: Account | None,
                             execution_accounts: dict[str, Account] | None = None,
                             risk_reference_price: Decimal | None = None) -> Order:
        """Accept an engine-held order: check it, journal it, start it."""
        check_managed_fields(request, now_ms=int(self.clock() * 1000))
        decision = (Decision(ok=True) if execution_accounts else self.evaluate_order_risk(
            request, venue=venue, account=account, risk_reference_price=risk_reference_price,
        ))
        if not decision.ok:
            await self.bus.publish("risk.rejected", {
                "rule": decision.rule, "message": decision.message, "market_id": request.market_id,
                "book": request.book, "type": request.type.value, "config_version": decision.config_version,
            })
            raise RiskRejected(decision.message, rule=decision.rule)
        request = request.model_copy(update={"user_id": request.user_id or self.config.user_id,
                                             "client_order_id": request.client_order_id or f"sp-{uuid.uuid4().hex[:20]}"})
        parent = await self.managed_order_controller.create(request, venue=venue, account=account, execution_accounts=execution_accounts)
        return parent.as_order()

    async def submit_child(self, parent: Any, request: OrderRequest, **kw: Any) -> Order:
        """A child of an engine-held order. A venue type goes to the venue; an
        engine type becomes a parent of its own (a bracket's stop-loss).

        The child's venue comes from its own market id, not the parent's: a
        parent on a bucket puts legs on several venues. A child whose id names
        no venue falls back to the parent's, which is every single-venue type."""
        venue = self._child_venue(parent, request)
        account = parent.account
        if parent.execution_accounts:
            account = parent.execution_accounts.get(venue)
            if account is None:
                raise BadRequest(f"{venue}: child is outside the saved execution scope")
            self.assert_execution_account(venue, account)
            request = request.model_copy(update={"account": account})
        elif venue != parent.venue:
            # `child_request` stamps the parent's account on every child; a
            # leg on another venue must carry that venue's instead.
            account = self.accounts.get(venue) or Account(venue=venue)
            request = request.model_copy(update={"account": account})
        if request.type in VENUE_ORDER_TYPES:
            order = await self.submit(request, venue=venue, **kw)
            self.managed_order_controller.adopt_child(parent.id, order.id, order.venue)
            return order
        leg = await self.managed_order_controller.create(request, venue=venue, account=account, owner=parent)
        return leg.as_order()

    @staticmethod
    def _child_venue(parent: Any, request: OrderRequest) -> str:
        try:
            return ids.venue_of(request.market_id)
        except BadRequest:
            if parent.venue is None:
                raise BadRequest("a routed child must name its concrete venue") from None
            return parent.venue

    async def cancel_child(self, parent: Any, order_id: str, venue: str | None = None) -> Order | None:
        """Cancel a managed child through its controller, or a venue child through cancel()."""
        if order_id in self.managed_order_controller.parents:
            return (await self.managed_order_controller.cancel(order_id, reason=f"cancelled by {parent.id}")).as_order()
        child = parent.child_of(order_id, venue)
        venue = venue or (child.venue if child is not None else parent.venue)
        try:
            return await self.cancel(order_id, venue=venue)
        except CancelUnconfirmed:
            # The venue knows nothing of it: the child stays live and the
            # parent pending, retried by its timer, never assumed cancelled.
            raise
        except OrderNotFound:
            return None

    def evaluate_order_risk(
        self, request: OrderRequest, *, venue: str, account: Account,
        risk_reference_price: Decimal | None = None,
        caller_supplied_order_id: bool = False,
    ) -> Decision:
        """Return whether an order passes the configured risk rules, without submitting it."""
        market_id = request.market_id
        if risk_reference_price is None:
            risk_reference_price = self.book_mid(market_id)
        return self.order_risk_checks.evaluate_order_risk(
            request, venue=venue, account=account,
            open_orders=[o for o in self.open_orders(venue=venue) if not o.execution_accounts],
            risk_reference_price=risk_reference_price, market_close_ts=self.market_close.get(market_id),
            event_id=self.event_of.get(market_id),
            caller_supplied_order_id=caller_supplied_order_id,
        )

    async def cancel(self, order_id: str, *, venue: str | None = None, market_id: str | None = None) -> Order:
        """Cancel, journaled with its own idempotency key. An engine-held order
        pulls its children first.

        Only the venue confirms a cancellation. An order already finished is
        returned as it is, without a venue call; a venue that answers "not
        found" is asked for the order itself, so a fill that won the race is
        recorded as `closed` rather than overwritten as `canceled`."""
        if order_id in self.managed_order_controller.parents:
            parent = await self.managed_order_controller.cancel(order_id)
            return parent.as_order()
        order = await self._known(order_id, venue)
        if order.is_terminal or order.held_by == HeldBy.ENGINE:
            # Nothing at a venue to cancel: a finished venue order, or an
            # engine-held parent this process no longer runs because it
            # finished before a restart (live parents are always restored).
            return order
        await self.guard_routed_order_account(order)
        venue = venue or order.venue
        adapter = self._adapter(venue)
        key = f"cancel:{venue}:{order_id}"
        request = OrderRequest(
            market_id=order.market_id, side=order.side, quantity=order.remaining or order.quantity,
            price=order.price, book=order.book, trader=order.trader, client_order_id=key,
        )
        account = order.account or self.accounts.get(venue) or Account(venue=venue)
        request = request.model_copy(update={"account": account, "tags": dict(order.tags)})
        await self.journal.record_intent(request, client_order_id=key, venue=venue, account=account,
                                         operation="cancel", target_order_id=order_id)
        async with self._lock(key):
            await self.journal.mark_intent(key, IntentState.SENDING, bump_attempt=True)
            try:
                result = await adapter.cancel_order(order_id, market_id=market_id or order.market_id)
            except OrderNotFound:
                return await self._record_order_the_venue_would_not_cancel(adapter, order, key)
            except (NetworkError, asyncio.TimeoutError):
                raise
        result = self._stamp(result, None, template=order)
        await self.journal.upsert_order(result, event="order.canceled")
        self._remember(result)
        await self.journal.mark_intent(key, IntentState.SETTLED, order_id=order_id)
        await self.managed_order_controller.on_order(result)
        await self.bus.publish("order.canceled", result.model_dump(mode="json"), key=f"{venue}:{order_id}", persist=False)
        return result

    async def _record_order_the_venue_would_not_cancel(self, adapter: TradingExchange, order: Order,
                                                       cancel_key: str) -> Order:
        """The venue has no resting order to cancel. That is not a
        cancellation: the order may have filled, expired, or been cancelled
        elsewhere. Read the order itself and record the venue's verdict, with
        whatever filled, through the same path as any order update, so an
        engine-held parent hears about its child. A venue with no record at
        all confirms nothing: the last observed status stands."""
        try:
            observed = await adapter.fetch_order(order.id)
        except OrderNotFound:
            detail = "the venue has no record of the order; cancellation unconfirmed"
            await self.journal.mark_intent(cancel_key, IntentState.FAILED, detail=detail)
            raise CancelUnconfirmed(
                f"{order.venue} has no record of order {order.id}: cancellation unconfirmed; "
                f"its last observed status ({order.status.value}) stands") from None
        observed = self._stamp(observed, None, template=order)
        if observed.is_terminal:
            await self.journal.mark_intent(cancel_key, IntentState.SETTLED, order_id=order.id,
                                           detail=f"already {observed.status.value} at the venue")
        else:
            await self.journal.mark_intent(cancel_key, IntentState.FAILED,
                                           detail=f"the venue refused the cancel and reports the order "
                                                  f"{observed.status.value}")
        await self.on_order(observed)
        return await self.journal.order(order.venue, order.id) or observed

    async def edit(self, request: EditRequest, *, venue: str | None = None) -> Order:
        """Amend a resting order. The edit carries its own idempotency key, so
        a retry is the same edit rather than a second one."""
        order = await self._known(request.order_id, venue)
        if order.held_by == HeldBy.ENGINE:
            raise BadRequest("engine-held parents cannot be amended; cancel and submit a new instruction")
        await self.guard_routed_order_account(order)
        venue = venue or order.venue
        adapter = self._adapter(venue)
        key = request.client_order_id or f"edit:{venue}:{request.order_id}:{uuid.uuid4().hex[:8]}"
        request = request.model_copy(update={"client_order_id": key})
        intent_request = OrderRequest(
            market_id=order.market_id, side=order.side, quantity=request.quantity or order.quantity,
            price=request.price if request.price is not None else order.price, book=order.book, trader=order.trader,
            client_order_id=key,
        )
        account = order.account or self.accounts.get(venue) or Account(venue=venue)
        decision = self.evaluate_order_risk(intent_request, venue=venue, account=account)
        if not decision.ok:
            await self.bus.publish("risk.rejected", {
                "rule": decision.rule, "message": decision.message, "order_id": order.id, "operation": "edit",
            }, key=key)
            raise RiskRejected(decision.message, rule=decision.rule)
        intent_request = intent_request.model_copy(update={"account": account, "tags": dict(order.tags)})
        await self.journal.record_intent(intent_request, client_order_id=key, venue=venue, account=account,
                                         operation="edit", target_order_id=order.id)
        async with self._lock(key):
            await self.journal.mark_intent(key, IntentState.SENDING, bump_attempt=True)
            result = await adapter.edit_order(request, current=order)
        result = self._stamp(result, None, template=order)
        await self.journal.upsert_order(result, event="order.edited")
        self._remember(result)
        await self.journal.mark_intent(key, IntentState.SETTLED, order_id=result.id)
        await self.managed_order_controller.on_order(result)
        await self.bus.publish("order.edited", result.model_dump(mode="json"), key=f"{venue}:{result.id}", persist=False)
        return result

    # -- RFQ and combos (docs/rfq-combo.md; the work is in `engine.rfq`) -------

    async def create_rfq(self, instrument_id: str, **kw: Any) -> Any:
        """Ask for quotes on a market or a combo. See `RfqDesk.create_rfq`."""
        return await self.rfq.create_rfq(instrument_id, **kw)

    async def cancel_rfq(self, rfq_id: str, *, venue: str | None = None) -> None:
        await self.rfq.cancel_rfq(rfq_id, venue=venue)

    async def accept_quote(self, accept: Any, *, venue: str | None = None, wait: bool = False,
                           timeout_s: float = 30.0) -> Any:
        """Take one side of a quote: risk, intent, venue, then events. Returns
        at once with the quote state unless `wait`. See `RfqDesk.accept_quote`."""
        return await self.rfq.accept_quote(accept, venue=venue, wait=wait, timeout_s=timeout_s)

    async def send_quote(self, quote: Any, *, venue: str | None = None) -> Any:
        return await self.rfq.send_quote(quote, venue=venue)

    async def replace_quote(self, quote_id: str, quote: Any, *, venue: str | None = None) -> Any:
        return await self.rfq.replace_quote(quote_id, quote, venue=venue)

    async def cancel_quote(self, quote_id: str, *, rfq_id: str | None = None, venue: str | None = None) -> None:
        await self.rfq.cancel_quote(quote_id, rfq_id=rfq_id, venue=venue)

    async def confirm_quote(self, quote: Any, *, rfq_id: str | None = None, venue: str | None = None,
                            wait: bool = False, timeout_s: float = 30.0) -> Any:
        """The maker's last look, through risk and the journal. See `RfqDesk.confirm_quote`."""
        return await self.rfq.confirm_quote(quote, rfq_id=rfq_id, venue=venue, wait=wait, timeout_s=timeout_s)

    async def create_combo(self, legs: list[Any], *, venue: str | None = None,
                           collection_id: str | None = None) -> Any:
        """The combo for these legs; `collection_id` names the collection on a
        venue that builds combos in one (Kalshi). See `RfqDesk.create_combo`."""
        return await self.rfq.create_combo(legs, venue=venue, collection_id=collection_id)

    # -- cross-venue RFQ (docs/rfq-combo.md, "Cross-venue RFQ"; `engine.rfq_groups`) --

    async def create_rfq_group(self, instrument_id: str | None = None, **kw: Any) -> Any:
        """One RFQ on every venue that lists the instrument (a market with
        `cross_venue=True`, a bucket, or a combo's legs), ranked as one. See
        `RfqGroupDesk.create_group`."""
        return await self.rfq_groups.create_group(instrument_id, **kw)

    async def rfq_group(self, group_id: str) -> Any:
        return await self.rfq_groups.get(group_id)

    async def preview_rfq_group(self, instrument_id: str | None = None, **kw: Any) -> Any:
        """Which venues `create_rfq_group` would ask, and why it would skip
        the others, without creating anything. See `RfqGroupDesk.preview`."""
        return await self.rfq_groups.preview(instrument_id, **kw)

    async def accept_group_quote(self, group_id: str, quote_id: str, **kw: Any) -> Any:
        """Take one quote of a group: the claim, every other member cancelled,
        then the accept. See `RfqGroupDesk.accept_group_quote`."""
        return await self.rfq_groups.accept_group_quote(group_id, quote_id, **kw)

    async def cancel_rfq_group(self, group_id: str, *, reason: str = "canceled by the caller") -> Any:
        return await self.rfq_groups.cancel_group(group_id, reason=reason)

    # -- halting --------------------------------------------------------------

    async def halt(self, reason: str, *, scope: str = "*", policy: HaltPolicy | None = None, rearm_after_s: float | None = None) -> dict[str, Any]:
        """Stop trading. Returns what the policy did, per venue."""
        policy = policy or self.config.halt_policy
        self.order_risk_checks.kill.engage(reason=reason, scope=scope, policy=policy, rearm_after_s=rearm_after_s)
        result: dict[str, Any] = {"policy": policy, "scope": scope, "reason": reason, "canceled": {}, "remaining": {}}
        if policy == "cancel":
            # Offers first: an RFQ or a quote left up could be accepted while
            # the orders are being pulled (docs/rfq-combo.md §5).
            result["rfq"] = await self.rfq.on_halt(scope)
            for venue, adapter in self.adapters.items():
                if scope not in ("*", venue):
                    continue
                try:
                    await self.guard_routed_venue_accounts(venue)
                    count = await adapter.cancel_all_orders()
                    result["canceled"][venue] = count
                except Exception as exc:  # a venue that cannot be reached must not stop the others
                    result["canceled"][venue] = f"failed: {type(exc).__name__}: {exc}"
                    continue
                left = await self._finish_cancelling(venue, adapter)
                if left:
                    result["remaining"][venue] = left
            for order in self.open_orders():
                if (order.held_by == HeldBy.VENUE and scope in ("*", order.venue)
                        and not isinstance(result["canceled"].get(order.venue), str)):
                    canceled = order.model_copy(update={"status": OrderStatus.CANCELED})
                    await self.journal.upsert_order(canceled, event="order.canceled")
                    self._remember(canceled)
        result["managed"] = await self.managed_order_controller.on_halt(reason, scope=scope)
        await self.bus.publish("engine.halted", result)
        return result

    async def _finish_cancelling(self, venue: str, adapter: TradingExchange, *, rounds: int = 4, pause_s: float = 0.5) -> int:
        """Cancel-all is not always instant, and on some venues not always
        complete. Read the book back and pull whatever is still resting, one
        order at a time; return how many refused to go."""
        for attempt in range(rounds):
            try:
                left = await adapter.fetch_open_orders()
            except Exception:
                return -1
            if not left:
                return 0
            if attempt:
                for order in left:
                    try:
                        await adapter.cancel_order(order.id, market_id=order.market_id)
                    except Exception:
                        pass
            await asyncio.sleep(pause_s)
        try:
            return len(await adapter.fetch_open_orders())
        except Exception:
            return -1

    async def resume(self, *, scope: str | None = None) -> None:
        self.order_risk_checks.kill.release(scope=scope)
        await self.bus.publish("engine.resumed", {"scope": scope or "*"})

    async def pause_book(self, book: str, *, paused: bool = True) -> None:
        """Stop one strategy without stopping the engine."""
        books = set(self.order_risk_checks.config.paused_books)
        books.add(book) if paused else books.discard(book)
        self.order_risk_checks.config = self.order_risk_checks.config.model_copy(update={"paused_books": sorted(books)})
        self.order_risk_checks.config_version = await self.journal.save_config("risk", self.order_risk_checks.config.model_dump(mode="json"))
        await self.bus.publish("engine.book_paused" if paused else "engine.book_resumed", {"book": book})

    async def set_risk(self, config: RiskConfig, *, author: str | None = None) -> int:
        """Replace the rules; the new version is journaled and returned."""
        version = await self.journal.save_config("risk", config.model_dump(mode="json"), author=author)
        self.order_risk_checks.configure(config, version=version)
        await self.bus.publish("risk.configured", {"version": version, "author": author})
        return version

    # -- recovery and the sweep ----------------------------------------------

    async def recover(self) -> Recovery:
        """Rebuild the ledger and resolve everything that was in doubt."""
        recovery = Recovery()
        for fill in await self.journal.fills():
            self.ledger.apply_fill(fill)
            recovery.fills_replayed += 1
        self.order_risk_checks.roll_day()
        await self.rfq.restore()
        await self.rfq_groups.restore()
        for intent in await self.journal.in_doubt():
            recovery.in_doubt += 1
            outcome = await self._resolve(intent)
            recovery.details.append(outcome)
            if outcome["result"] == "adopted":
                recovery.adopted += 1
            elif outcome["result"] == "swept":
                recovery.swept += 1
            else:
                # Pending or unreachable: still in doubt, and reported as such.
                recovery.unresolved += 1
        # After the in-doubt pass, so a member create or accept resolved a
        # moment ago is what its group sees.
        await self.rfq_groups.reconcile_all()
        recovery.managed = await self.managed_order_controller.restore()
        # After the in-doubt intents, so an order adopted a moment ago is in
        # the cache the risk rules count against.
        recovery.orders_open = len(await self.refresh_open_orders())
        await self.managed_order_controller.resume_restored()
        return recovery

    async def sweep(self) -> list[dict[str, Any]]:
        """Resolve intents that have been in doubt longer than the timeout."""
        out = []
        for intent in await self.journal.in_doubt(older_than_ms=int(self.config.in_doubt_timeout_s * 1000)):
            out.append(await self._resolve(intent))
        return out

    async def _resolve(self, intent: Any) -> dict[str, Any]:
        """Ask the venue whether this intent's order exists."""
        if intent.operation in RFQ_OPERATIONS:
            return await self.rfq.resolve(intent)
        adapter = self.adapters.get(intent.venue)
        base = {"client_order_id": intent.client_order_id, "venue": intent.venue, "operation": intent.operation}
        if adapter is None:
            await self.bus.publish("intent.unresolved", base | {"reason": "no adapter for this venue"})
            return base | {"result": "unresolved", "reason": "no adapter"}
        try:
            persisted = OrderRequest.stored(intent.request)
            parent_id = persisted.tags.get("parent")
            stored = await self.journal.managed_one(parent_id) if parent_id else None
            if stored and stored[0] == "routed_limit":
                if persisted.account is None:
                    raise SubmissionPending("routed intent has no saved execution account")
                self.assert_execution_account(intent.venue, persisted.account)
            if intent.target_order_id:
                target = await self.journal.order(intent.venue, intent.target_order_id)
                if target is not None:
                    await self.guard_routed_order_account(target)
            found = await self._find_by_client_id(adapter, intent)
        except Exception as exc:
            await self.bus.publish("intent.unresolved", base | {"reason": f"{type(exc).__name__}: {exc}"})
            return base | {"result": "unresolved", "reason": str(exc)}
        if found is not None:
            if intent.operation == "create":
                found = self._stamp(found, OrderRequest.stored(intent.request))
            await self.journal.upsert_order(found, event="order.adopted")
            self._remember(found)
            await self.journal.mark_intent(intent.client_order_id, IntentState.SENT, order_id=found.id,
                                           detail="adopted after recovery")
            await self.bus.publish("intent.adopted", base | {"order_id": found.id, "status": found.status.value})
            return base | {"result": "adopted", "order_id": found.id}
        if intent.age_ms < self.config.lost_after_s * 1000:
            # The venue says no such order, but it may simply not list it yet.
            await self.bus.publish("intent.pending", base | {"age_ms": intent.age_ms})
            return base | {"result": "pending", "age_ms": intent.age_ms}
        await self.journal.mark_intent(intent.client_order_id, IntentState.LOST, detail="not found at the venue")
        await self.bus.publish("intent.swept", base | {"reason": "no order with this client order id"})
        return base | {"result": "swept"}

    async def _find_by_client_id(self, adapter: TradingExchange, intent: Any) -> Order | None:
        """Look for an order carrying this intent's client order id."""
        if intent.operation != "create":
            # A cancel or edit in doubt is resolved by reading the order itself.
            if intent.target_order_id:
                try:
                    return await adapter.fetch_order(intent.target_order_id)
                except OrderNotFound:
                    return None
            return None
        for order in await adapter.fetch_open_orders():
            if order.client_order_id == intent.client_order_id:
                return order
        if not adapter.has.get("fetch_orders"):
            raise SubmissionPending("the venue cannot confirm historical order absence")
        since = intent.created_ts - 60_000
        cursor = None
        seen = set()
        while True:
            page = await adapter.fetch_orders(since=since, limit=200, cursor=cursor)
            for order in page:
                if order.client_order_id == intent.client_order_id:
                    return order
            cursor = getattr(page, "next_cursor", None)
            if not cursor:
                break
            if cursor in seen:
                raise SubmissionPending("the venue repeated its order-history cursor")
            seen.add(cursor)
        return None

    # -- state ----------------------------------------------------------------

    def open_orders(self, *, venue: str | None = None, book: str | None = None) -> list[Order]:
        """Open orders as the engine believes them, from memory of the journal."""
        return [o for o in self._open_cache.values()
                if (venue is None or o.venue == venue or venue in o.execution_accounts) and (book is None or o.book == book)]

    async def refresh_open_orders(self) -> list[Order]:
        orders = await self.journal.open_orders()
        self._open_cache = {f"{o.venue}:{o.id}": o for o in orders}
        return orders

    async def on_order(self, order: Order) -> None:
        """Record an order update, from a stream or a poll."""
        stored = await self.journal.order(order.venue, order.id)
        if stored is not None:
            if order.filled < stored.filled or (stored.is_terminal and not order.is_terminal and order.filled == stored.filled):
                return
            order = self._stamp(order, None, template=stored)
        elif order.client_order_id:
            intent = await self.journal.intent(order.client_order_id)
            if intent is not None and intent.operation in ("create", *FOLLOWED):
                order = self._stamp(order, OrderRequest.stored(intent.request))
        await self.journal.upsert_order(order)
        self._remember(order)
        await self.bus.publish(f"order.{order.status.value}", order.model_dump(mode="json"),
                               key=f"{order.venue}:{order.id}", persist=False)
        await self.managed_order_controller.on_order(order)

    async def on_fill(self, fill: Fill) -> None:
        """Record a fill once, book it in the ledger, publish it. A fill of a
        known order carries that order's outcome and action."""
        order = await self.journal.order(fill.venue, fill.order_id)
        if order is None and self.rfq.following:
            # Maybe the order an executed quote made, not adopted yet.
            order = await self.rfq.claim_fill(fill)
        if order is not None and order.outcome is not None and fill.outcome is None:
            fill = fill.model_copy(update={"outcome": order.outcome, "action": order.action})
        book = order.book if order else None
        trader = order.trader if order else None
        changed = await self.journal.record_fill(fill, book=book, trader=trader)
        if not changed:
            return
        realized = self.ledger.apply_fill(fill, book=book)
        await self.bus.publish("fill.booked", fill.model_dump(mode="json") | {"book": book, "realized": str(realized)},
                               key=f"{fill.venue}:{fill.id}", persist=False)
        # Engine-held parents do not hear about fill events: they follow the
        # venue's order record, which `on_order` carries.

    async def on_book(self, event: Any) -> None:
        """A book changed, from `synpath.ws` or anywhere else. Engine-held
        orders that watch this instrument hear about it."""
        market_id = getattr(event, "market_id", None) or event["market_id"]
        book = getattr(event, "book", None)
        if book is None:
            book = self.books.get(market_id)
        if book is not None:
            self.books[market_id] = book
        await self.managed_order_controller.on_book(market_id)

    def set_book(self, market_id: str, book: Any) -> None:
        """Hand the engine a `LocalBook` (or anything with `best_bid`,
        `best_ask` and `levels`) to watch."""
        self.books[market_id] = book

    async def on_trade(self, event: Any) -> None:
        """A public print."""
        market_id = getattr(event, "market_id", None) or event["market_id"]
        price = getattr(event, "price", None) or event["price"]
        quantity = getattr(event, "quantity", None) or event.get("quantity", ZERO)
        timestamp = getattr(event, "timestamp", None)
        if isinstance(event, dict):
            timestamp = event.get("timestamp")
        if timestamp is not None:
            timestamp = int(timestamp)
            minimum = max(self.started_ts or 0, self.last_trade_timestamps.get(market_id, 0))
            if timestamp < minimum:
                return
            self.last_trade_timestamps[market_id] = timestamp
        self.last_trade[market_id] = Decimal(str(price))
        await self.managed_order_controller.on_trade(market_id, Decimal(str(price)), Decimal(str(quantity)))

    async def on_settlement(self, settlement: Settlement) -> None:
        realized = self.ledger.apply_settlement(settlement)
        await self.bus.publish("settlement.booked", settlement.model_dump(mode="json") | {"realized": str(realized)},
                               key=f"{settlement.venue}:{settlement.market_id}")

    def marks(self) -> dict[tuple[str, str, str], Decimal]:
        """What every open position is marked at, keyed `(account_key, book,
        market_id)` as `Ledger.rollup` wants: the price it could be closed at
        now (`exit_price`). Read from the live book each time, so a mark never
        outlives the book it came from."""
        marks: dict[tuple[str, str, str], Decimal] = {}
        for key, state in self.ledger.positions.items():
            mark = self.exit_price(state.market_id, state.contracts)
            if mark is not None:
                marks[key] = mark
        return marks

    def exit_price(self, market_id: str, contracts: Decimal) -> Decimal | None:
        """The YES price a position of `contracts` (signed, YES terms) could be
        closed at: on the engine's ready, uncrossed book, a long YES sells YES
        at the best bid and a short YES, which is a long NO, buys YES back at
        the best ask (sells NO at 1 − ask). With no such book, or nobody on
        that side, the last trade the engine saw. `None` when flat or with
        neither."""
        if contracts == 0:
            return None
        book = self.books.get(market_id)
        if book is not None and getattr(book, "ready", True) is not False:
            bid, ask = book.best_bid, book.best_ask
            if not (bid is not None and ask is not None and bid > ask):
                touch = bid if contracts > 0 else ask
                if touch is not None:
                    return touch
        return self.last_trade.get(market_id)

    def book_mid(self, market_id: str) -> Decimal | None:
        """The midpoint of the engine's ready, uncrossed book for a market,
        with both sides quoted; `None` otherwise. The risk collar's reference."""
        book = self.books.get(market_id)
        if book is None or getattr(book, "ready", True) is False:
            return None
        bid, ask = book.best_bid, book.best_ask
        if bid is None or ask is None or bid > ask:
            return None
        return (bid + ask) / 2

    def positions(self) -> list[Position]:
        """Every position the ledger holds. A combo is its own instrument; its
        legs ride along in `info["combo"]` for display."""
        marks = self.marks()
        out = []
        for key, state in self.ledger.positions.items():
            position = state.to_position(self.accounts.get(state.venue), mark=marks.get(key))
            combo = self.rfq.combos.get(state.market_id)
            if combo is not None:
                position.info["combo"] = {"collection_id": combo.collection_id,
                                          "legs": [leg.model_dump(mode="json", exclude={"info"}) for leg in combo.legs]}
            out.append(position)
        return out

    async def bucket_orders(self, bucket_id: str) -> list[Any]:
        """Every order placed on a bucket, live or finished, oldest first. A
        live parent is the running one; a finished one this process no longer
        holds is rebuilt from its journal snapshot, read-only."""
        out = []
        for kind, snapshot in await self.journal.managed_on(f"bucket:{bucket_id}"):
            parent = self.managed_order_controller.get(snapshot["id"]) or self._rebuilt(kind, snapshot)
            if parent is not None:
                out.append(parent)
        return out

    async def bucket_order(self, parent_id: str) -> Any | None:
        """One order on a bucket, by its id, or None if the id is not one."""
        parent = self.managed_order_controller.get(parent_id)
        if parent is None:
            found = await self.journal.managed_one(parent_id)
            parent = self._rebuilt(*found) if found else None
        return parent if parent is not None and is_bucket_id(parent.market_id) else None

    @staticmethod
    def _rebuilt(kind: str, snapshot: Mapping[str, Any]) -> Any | None:
        cls = REGISTRY.get(kind)
        return cls.from_snapshot(snapshot) if cls is not None else None

    async def bucket_position(self, bucket_id: str, *, book: str | None = None) -> dict[str, Any]:
        """The ledger's positions in a bucket's members, netted in bucket
        terms: a flipped member's long YES is a short bucket. Per member
        underneath, so the net can be traced."""
        row = await self.journal.bucket(bucket_id)
        if row is None:
            raise BadRequest(f"bucket:{bucket_id}: no such bucket in this journal")
        bucket = Bucket.model_validate(row)
        marks = self.marks()
        members: list[dict[str, Any]] = []
        net = ZERO
        cost = ZERO
        realized = ZERO
        fees = ZERO
        for member in bucket.members:
            for (account_key, name, market_id), state in self.ledger.positions.items():
                if market_id != member.market_id or (book is not None and name != book):
                    continue
                signed = -state.contracts if member.flip else state.contracts
                entry = state.average_price_in_bucket(member.flip) if state.contracts else None
                net += signed
                if entry is not None:
                    cost += signed * entry
                realized += state.realized
                fees += state.fees
                mark = marks.get((account_key, name, market_id))
                members.append({
                    "market_id": member.market_id, "venue": state.venue, "book": name, "account": account_key,
                    "flip": member.flip, "contracts": str(state.contracts), "bucket_contracts": str(signed),
                    "entry_price": str(entry) if entry is not None else None,
                    "mark": str(mark) if mark is not None else None,
                })
        return {
            "bucket_id": bucket.id, "name": bucket.name, "book": book or bucket.book,
            "contracts": str(net), "side": "long" if net > 0 else "short" if net < 0 else "flat",
            "entry_price": str(cost / net) if net else None,
            "realized": str(realized), "fees": str(fees), "members": members,
        }

    def pnl(self, level: str = "book") -> dict[str, Any]:
        marks = self.marks()
        rows = self.ledger.rollup(level, marks)  # type: ignore[arg-type]
        total = self.ledger.total(marks)
        return {"level": level, "rows": rows, "total": total}

    # -- background loops -----------------------------------------------------

    async def _lease_loop(self) -> None:
        while self.running:
            await asyncio.sleep(self.config.lease_renew_s)
            if not self.config.require_lease:
                continue
            if not await self.journal.renew_lease():
                await self.bus.publish("engine.lease_lost", {"owner": self.journal.owner})
                await self.halt("the journal lease was taken by another engine", policy="hold")
                raise LeaseLost("another engine took the journal lease; this one stopped trading")

    def queue_managed_action(self, parent_id: str) -> None:
        """Queue a hint; the durable parent snapshot remains the source of truth."""
        if parent_id not in self._queued_parents:
            self._queued_parents.add(parent_id)
            self._dispatch_queue.put_nowait(parent_id)

    async def _dispatch_loop(self) -> None:
        """One stop-action dispatcher keeps venue latency off the price consumer."""
        self.dispatch_running = True
        try:
            while self.running:
                parent_id = await self._dispatch_queue.get()
                self._queued_parents.discard(parent_id)
                parent = self.managed_order_controller.get(parent_id)
                if parent is None or not parent.live:
                    continue
                self.dispatch_parent = parent_id
                try:
                    await self.managed_order_controller.run(parent, lambda ctx: parent.on_timer(ctx))
                finally:
                    self.dispatch_parent = None
        finally:
            self.dispatch_running = False

    async def _managed_loop(self) -> None:
        """The clock engine-held orders run on."""
        while self.running:
            await asyncio.sleep(self.config.managed_tick_s)
            try:
                await self.managed_order_controller.on_timer()
            except Exception:
                log.exception("synpath.engine: the managed-order timer failed")

    async def _sweep_loop(self) -> None:
        while self.running:
            await asyncio.sleep(self.config.sweep_interval_s)
            try:
                await self.sweep()
            except Exception:
                log.exception("synpath.engine: sweep failed")

    async def _poll_loop(self) -> None:
        """Poll orders and fills where no stream feeds the engine."""
        while self.running:
            await asyncio.sleep(self.config.poll_interval_s)
            for venue, adapter in self.adapters.items():
                try:
                    await self.poll(venue, adapter)
                except Exception:
                    log.exception("synpath.engine: polling %s failed", venue)

    async def poll(self, venue: str, adapter: TradingExchange | None = None) -> None:
        await self.guard_routed_venue_accounts(venue)
        adapter = adapter or self._adapter(venue)
        for order in await adapter.fetch_open_orders():
            await self.on_order(order)
        if adapter.has.get("fetch_my_trades"):
            since = int(await self.journal.cursor(f"fills:{venue}", 0) or 0)
            fills = await adapter.fetch_my_trades(since=since or None, limit=200)
            newest = since
            for fill in fills:
                await self.on_fill(fill)
                newest = max(newest, fill.timestamp or 0)
            if newest > since:
                await self.journal.set_cursor(f"fills:{venue}", newest)

    # -- helpers --------------------------------------------------------------

    def _adapter(self, venue: str) -> TradingExchange:
        adapter = self.adapters.get(venue)
        if adapter is None:
            raise KeyError(f"no adapter configured for {venue!r}; known venues: {sorted(self.adapters)}")
        return adapter

    def _venue_of(self, request: OrderRequest) -> str:
        """The account's venue if one is named, else the venue the market id
        names (`kalshi:...` routes to Kalshi), else the only venue there is."""
        if request.account is not None:
            return request.account.venue
        venue, _ = ids.split(request.market_id)
        if venue is not None and venue in self.adapters:
            return venue
        if len(self.adapters) == 1:
            return next(iter(self.adapters))
        raise ValueError("more than one venue is configured and the market id names none of them: "
                         "use a Synpath id (venue:native), pass venue=, or name an account")

    async def _known(self, order_id: str, venue: str | None) -> Order:
        managed = await self.journal.managed_one(order_id)
        if managed is not None:
            parent = self._rebuilt(*managed)
            if parent is not None:
                return parent.as_order()
        if venue is not None:
            order = await self.journal.order(venue, order_id)
            if order is not None:
                return order
        for name in ([venue] if venue else list(self.adapters)):
            order = await self.journal.order(name, order_id)
            if order is not None:
                return order
        raise OrderNotFound(f"the engine has no order {order_id!r} in its journal")

    def _stamp(self, order: Order, request: OrderRequest | None, *, template: Order | None = None) -> Order:
        """Carry the engine's own fields onto a venue's answer."""
        source = request or template
        updates: dict[str, Any] = {}
        if source is not None:
            updates["user_id"] = source.user_id
            updates["type"] = source.type
            # Only what the caller said: an order with a side alone has no
            # outcome and action of its own (on a netting venue a sell may
            # close YES or open NO), and a token venue reports the token.
            if source.outcome is not None:
                updates["outcome"], updates["action"] = source.outcome, source.action
            updates["time_in_force"] = source.time_in_force
            updates["expires_at"] = source.expires_at
            updates["parent_id"] = getattr(source, "parent_id", None) or source.tags.get("parent")
            if order.book is None and source.book:
                updates["book"] = source.book
            if order.trader is None and source.trader:
                updates["trader"] = source.trader
            if not order.tags and getattr(source, "tags", None):
                updates["tags"] = source.tags
            if order.client_order_id is None and source.client_order_id:
                updates["client_order_id"] = source.client_order_id
        return order.model_copy(update=updates) if updates else order

    def _remember(self, order: Order) -> None:
        """Keep the open-order view in step with what was just written, so the
        rules that count resting orders count this one too."""
        key = f"{order.venue}:{order.id}"
        if order.is_terminal:
            self._open_cache.pop(key, None)
        else:
            self._open_cache[key] = order

    def _lock(self, key: str) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = self._locks[key] = asyncio.Lock()
        return lock
