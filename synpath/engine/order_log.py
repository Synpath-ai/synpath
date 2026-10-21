"""The order log: every action on an order, as the journal recorded it.

The journal's append-only `events` already hold each step an order goes
through, under several keys: the instruction saved before anything is sent
(`intent.planned`, keyed by `client_order_id`, or `cancel:<venue>:<id>` for
a cancel), the send and its outcome (`intent.sending`, `intent.sent`,
`intent.failed`, `intent.rejected`, `submission.*`), the venue's record
(`order.accepted`, `order.updated`, `order.canceled`, ..., keyed
`<venue>:<id>`), an engine-held parent's life (`managed.*`, keyed by its
id and by `<venue>:<id>`), fills (`fill.new`) and risk refusals
(`risk.rejected`). This module reads them back as one log:

* **One shape per entry** (`entry`): what happened (`kind`), when, to which
  order, and the order's fields as the event carried them.
* **Repeats folded** (`fold`): an engine-held parent re-records its state
  while it waits, and the venue's record repeats on every poll; a run of the
  same kind (and, for a venue order, status and filled quantity) for one
  order is shown once, at its first time.
* **Facts carried forward** (`fold`): an event that leaves out the order's
  market, side, outcome or action (a trigger, a cancel's instruction) takes
  them from the order's earlier events.
* **One order's family** (`order_keys`): an order's log includes its
  cancel, its fills, and for an engine-held parent its children's.

Read only: nothing here changes the journal.
"""
from __future__ import annotations

from typing import Any, Iterable

from .storage_types import JournalEvent

ORDER_EVENT_PREFIXES = ("intent.", "submission.", "order.", "managed.", "fill.")
ORDER_EVENT_KINDS = ("risk.rejected",)
NOT_ORDER_KINDS = frozenset({"managed.restored"})
"""Engine-wide, not about one order: the journal view shows them."""
RAW_BATCH = 1_000
MOST_RAW_EVENTS = 50_000
"""Raw events one page reads at most, however many fold away."""

CARRIED = ("market_id", "venue", "side", "outcome", "action")
"""An order's fields a later event of it may leave out."""
ORDER_FIELDS = ("market_id", "side", "outcome", "action", "type", "quantity", "filled", "price", "stop_price")


def entry(event: JournalEvent) -> dict[str, Any]:
    """One journal event as a log entry."""
    payload = event.payload if isinstance(event.payload, dict) else {}
    request = payload.get("request") if isinstance(payload.get("request"), dict) else {}
    row: dict[str, Any] = {"seq": event.seq, "ts": event.ts, "kind": event.kind, "order_id": None,
                           "client_order_id": None, "parent_id": None, "venue": None, "status": None,
                           "trigger_price": None, "rule": None, "detail": None,
                           **{name: None for name in ORDER_FIELDS}}
    key = event.key or ""
    if key.startswith("cancel:"):
        key = key.removeprefix("cancel:")  # a cancel's intent, keyed cancel:<venue>:<order id>
    venue, _, rest = key.partition(":")
    if "id" in payload and ("status" in payload or "state" in payload):
        # An order or an engine-held parent's snapshot.
        row.update({name: payload.get(name) for name in ORDER_FIELDS if payload.get(name) is not None})
        row.update({name: request.get(name) for name in ORDER_FIELDS if row.get(name) is None and request.get(name) is not None})
        row["order_id"], row["client_order_id"] = payload.get("id"), payload.get("client_order_id")
        row["parent_id"], row["venue"] = payload.get("parent_id"), payload.get("venue")
        row["status"] = payload.get("status") or payload.get("state")
        row["type"] = row["type"] or payload.get("kind")
        row["detail"] = (payload.get("info") or {}).get("detail") if isinstance(payload.get("info"), dict) else payload.get("detail")
    elif event.kind == "fill.new":
        row.update({name: payload.get(name) for name in ("market_id", "side", "outcome", "action", "price", "quantity")})
        row["order_id"], row["client_order_id"] = payload.get("order_id"), payload.get("client_order_id")
        row["venue"], row["detail"] = payload.get("venue"), payload.get("liquidity")
    elif request:
        # An instruction saved before sending: a create or a cancel.
        row.update({name: request.get(name) for name in ORDER_FIELDS if request.get(name) is not None})
        row["client_order_id"] = request.get("client_order_id") or event.key
        row["order_id"] = payload.get("target_order_id")
        row["venue"] = payload.get("venue")
        row["detail"] = payload.get("operation")
    else:
        row["order_id"] = payload.get("parent_id") or payload.get("order_id")
        row["client_order_id"] = payload.get("client_order_id") or payload.get("child_client_order_id")
        row["type"] = payload.get("kind") if event.kind.startswith("managed.") else None
        row["filled"] = payload.get("filled")
        row["stop_price"] = payload.get("stop_price")
        row["trigger_price"] = payload.get("trigger_price")
        row["market_id"] = payload.get("market_id")
        row["rule"] = payload.get("rule")
        row["detail"] = payload.get("detail") or payload.get("error") or payload.get("message")
        if event.kind.startswith("intent.") and not row["order_id"] and (event.key or "").startswith("cancel:"):
            row["venue"], row["order_id"] = venue, rest
        elif event.kind.startswith("intent.") or event.kind.startswith("submission.") or event.kind.endswith(".failed") \
                or event.kind.endswith(".rejected"):
            row["client_order_id"] = row["client_order_id"] or event.key
    if row["venue"] is None and rest and venue:
        row["venue"] = venue
    if row["order_id"] is None and ":" in key:
        row["order_id"] = rest or None
    return row


def fold(entries_oldest_first: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop an entry that repeats the last one kept for its order: the same
    kind, status and filled quantity. Every change is kept."""
    kept: list[dict[str, Any]] = []
    last: dict[str, tuple[Any, ...]] = {}
    known: dict[str, dict[str, Any]] = {}
    for row in entries_oldest_first:
        who = str(row.get("order_id") or row.get("client_order_id") or row["seq"])
        # What an earlier event of this order said and this one leaves out
        # (a trigger, a cancel's instruction): carried forward, never guessed.
        facts = known.setdefault(who, {})
        for name in CARRIED:
            if row.get(name) is None and facts.get(name) is not None:
                row[name] = facts[name]
            elif row.get(name) is not None:
                facts[name] = row[name]
        # An engine-held parent records each state twice (its snapshot and
        # its order view), so its kind alone says what changed.
        signature = (row["kind"],) if row["kind"].startswith("managed.") else (row["kind"], row.get("status"),
                                                                               row.get("filled"))
        if last.get(who) == signature:
            continue
        if row["kind"] != "managed.created":
            # Creation is not a state: a parent's waiting on either side of it is one wait.
            last[who] = signature
        kept.append(row)
    return kept


async def order_keys(engine: Any, order_id: str) -> list[str]:
    """Every journal key one order's log is under: the order, its client
    order id, its cancel, and an engine-held parent's children and theirs."""
    keys: list[str] = []
    family = [order_id]
    seen: set[str] = set()
    while family:
        current = family.pop()
        if current in seen:
            continue
        seen.add(current)
        keys.append(current)
        found = []
        for venue in engine.adapters:
            order = await engine.journal.order(venue, current)
            if order is not None:
                found.append(order)
        managed = await engine.journal.managed_one(current)
        if managed is not None:
            _, snapshot = managed
            venue = snapshot.get("venue")
            keys += [f"{venue}:{current}"] if venue else []
            client = snapshot.get("client_order_id") or (snapshot.get("request") or {}).get("client_order_id")
            keys += [client] if client else []
            family += [str(child.get("order_id") if isinstance(child, dict) else child)
                       for child in snapshot.get("children") or []]
        for order in found:
            keys += [f"{order.venue}:{order.id}", f"cancel:{order.venue}:{order.id}"]
            keys += [order.client_order_id] if order.client_order_id else []
            family += [str(child) for child in (order.info or {}).get("children") or []]
    return list(dict.fromkeys(key for key in keys if key))


async def order_log(engine: Any, *, order_id: str | None = None, limit: int = 200,
                    before: int | None = None) -> tuple[list[dict[str, Any]], int | None]:
    """The log newest first, for one order (and its family) or the whole
    account, with the `before` cursor for the next page (`None` at the end).
    Raw events are read in batches until the folded page is full: a waiting
    stop leaves thousands of repeats that fold to one entry."""
    keys = await order_keys(engine, order_id) if order_id else None
    events: list[JournalEvent] = []
    cursor, exhausted = before, False
    while True:
        batch = await engine.journal.events_page(kinds=ORDER_EVENT_KINDS, prefixes=ORDER_EVENT_PREFIXES, keys=keys,
                                                 before=cursor, limit=RAW_BATCH)
        events += [event for event in batch if event.kind not in NOT_ORDER_KINDS]
        exhausted = len(batch) < RAW_BATCH
        if batch:
            cursor = batch[-1].seq
        rows = fold(entry(event) for event in reversed(events))
        if exhausted or len(rows) > limit or len(events) >= MOST_RAW_EVENTS:
            break
    rows.reverse()
    page = rows[:limit]
    more = len(rows) > limit or not exhausted
    return page, (page[-1]["seq"] if page and more else None)
