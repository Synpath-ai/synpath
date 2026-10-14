"""The journal: what the engine knew, in the order it knew it.

Every decision the engine makes is written here before it leaves the
process, and every answer a venue gives is written here when it arrives. A
crashed engine restarts by reading this file, so the rules are strict:

**Append first, act second.** An order's intent, with its client order id,
is committed to disk before any network call. A process killed between the
write and the venue's answer restarts holding an intent marked `sending`
and a client order id it can ask the venue about, which is the only way to
tell "never sent" from "sent, answer lost" without risking a double order.

**One writer.** Two engines on one journal would both submit. A lease row
with an owner and an expiry makes the second one refuse to trade; it is
taken before the first write and renewed while the engine runs.

**The event log is the truth; the tables are a convenience.** `events` is
append-only and ordered by `seq`. `intents`, `orders`, `fills` and
`positions` are materialized in the same transaction as the event that
changes them, so a reader never sees a row that no event explains, and
`replay(since)` can rebuild any of it.

SQLite in WAL mode with `synchronous=FULL`, through `aiosqlite`: one engine
on one machine needs no server, and FULL is what makes "written before the
call" true across a power cut rather than only across a crash.
"""
from __future__ import annotations

import asyncio
import json
import os
import socket
import time
import uuid
from decimal import Decimal
from typing import Any, AsyncIterator, Iterable

from ..errors import BadRequest
from ..trading.types import Account, Fill, Order, OrderRequest
from .storage_types import Intent, IntentState, JournalEvent, Lease, LeaseLost
from .submissions import request_identity
from .order_storage import OrderRecords, UNATTRIBUTED_USER, schema as order_schema

SCHEMA_VERSION = 6

SCHEMA = """
CREATE TABLE IF NOT EXISTS submissions (
    client_order_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    account_key TEXT NOT NULL,
    venue TEXT NOT NULL,
    request TEXT NOT NULL,
    effective_request TEXT NOT NULL,
    result_id TEXT,
    state TEXT NOT NULL DEFAULT 'pending',
    error TEXT,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS submissions_owner ON submissions(user_id, account_key, created_ts);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    kind TEXT NOT NULL,
    key TEXT,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_kind ON events(kind, seq);
CREATE INDEX IF NOT EXISTS events_key ON events(key, seq);

CREATE TABLE IF NOT EXISTS intents (
    client_order_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    operation TEXT NOT NULL,
    venue TEXT NOT NULL,
    account_key TEXT NOT NULL,
    market_id TEXT,
    order_id TEXT,
    target_order_id TEXT,
    book TEXT,
    trader TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    request TEXT NOT NULL,
    detail TEXT,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS intents_state ON intents(state, updated_ts);
CREATE INDEX IF NOT EXISTS intents_order ON intents(venue, order_id);

CREATE TABLE IF NOT EXISTS orders (
    venue TEXT NOT NULL,
    id TEXT NOT NULL,
    client_order_id TEXT,
    account_key TEXT NOT NULL,
    market_id TEXT NOT NULL,
    side TEXT NOT NULL,
    status TEXT NOT NULL,
    terminal INTEGER NOT NULL DEFAULT 0,
    book TEXT,
    trader TEXT,
    quantity TEXT NOT NULL,
    filled TEXT NOT NULL,
    price TEXT,
    parent_id TEXT,
    payload TEXT NOT NULL,
    created_ts INTEGER,
    updated_ts INTEGER NOT NULL,
    PRIMARY KEY (venue, id)
);
CREATE INDEX IF NOT EXISTS orders_open ON orders(terminal, venue, account_key);
CREATE INDEX IF NOT EXISTS orders_client ON orders(client_order_id);

CREATE TABLE IF NOT EXISTS fills (
    venue TEXT NOT NULL,
    id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    client_order_id TEXT,
    account_key TEXT NOT NULL,
    market_id TEXT NOT NULL,
    side TEXT NOT NULL,
    price TEXT NOT NULL,
    quantity TEXT NOT NULL,
    fee TEXT,
    settlement TEXT NOT NULL,
    book TEXT,
    trader TEXT,
    ts INTEGER NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (venue, id)
);
CREATE INDEX IF NOT EXISTS fills_order ON fills(venue, order_id);
CREATE INDEX IF NOT EXISTS fills_ts ON fills(ts);

CREATE TABLE IF NOT EXISTS managed_orders (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    account_key TEXT,
    market_id TEXT NOT NULL,
    book TEXT,
    trader TEXT,
    payload TEXT NOT NULL,
    child_order_id TEXT,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS managed_state ON managed_orders(state);

CREATE TABLE IF NOT EXISTS leases (
    name TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    holder TEXT NOT NULL,
    acquired_ts INTEGER NOT NULL,
    expires_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS config_versions (
    version INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    ts INTEGER NOT NULL,
    author TEXT,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fair_values (
    account_key TEXT NOT NULL,
    market_id TEXT NOT NULL,
    value TEXT NOT NULL,
    source TEXT,
    ts INTEGER NOT NULL,
    PRIMARY KEY (account_key, market_id)
);

CREATE TABLE IF NOT EXISTS buckets (
    id TEXT PRIMARY KEY,
    book TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS bucket_members (
    bucket_id TEXT NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
    market_id TEXT NOT NULL,
    flip INTEGER NOT NULL DEFAULT 0,
    position INTEGER NOT NULL,
    PRIMARY KEY (bucket_id, market_id)
);

CREATE TABLE IF NOT EXISTS cursors (
    name TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_ts INTEGER NOT NULL
);
"""


# Stop tables own their recovery snapshots. The legacy managed table remains
# the source only for advanced types that have not migrated in this checkpoint.
MANAGED_SOURCE = """
    SELECT id, kind, state, market_id, created_ts, payload FROM managed_orders
        WHERE kind NOT IN ('stop_market', 'stop_limit')
    UNION ALL
    SELECT id, 'stop_market', state, market_id, created_ts, snapshot FROM stop_market_orders
        WHERE json_extract(snapshot, '$.kind') = 'stop_market'
    UNION ALL
    SELECT id, 'stop_limit', state, market_id, created_ts, snapshot FROM stop_limit_orders
        WHERE json_extract(snapshot, '$.kind') = 'stop_limit'
"""


def now_ms() -> int:
    return int(time.time() * 1000)


def _dump(value: Any) -> str:
    return json.dumps(value, default=str, separators=(",", ":"))


_RENAMED = {"amount": "quantity", "last_fill_amount": "last_fill_quantity"}
_VERBATIM = frozenset({"info", "raw", "tags"})


def _quantity_keys(value: Any) -> Any:
    """A payload written before 4 with this library's own `amount` keys
    named `quantity`. A venue's raw record (`info`, `raw`) is left as the
    venue sent it."""
    if isinstance(value, dict):
        return {_RENAMED.get(k, k): (v if k in _VERBATIM else _quantity_keys(v)) for k, v in value.items()}
    if isinstance(value, list):
        return [_quantity_keys(v) for v in value]
    return value


def _text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _word(value: Any) -> str:
    """An enum's value, or the string a `model_copy` left in its place."""
    return str(getattr(value, "value", value))



class _Held(Exception):
    """Internal: the lease row belongs to someone else. Becomes `LeaseLost`."""



IN_DOUBT = (IntentState.SENDING,)






class Journal:
    """The engine's write-ahead log and its materialized state."""

    def __init__(self, path: str | os.PathLike[str] = "synpath.db", *, owner: str | None = None, busy_timeout_ms: int = 10_000, user_id: str = "local-user"):
        if not user_id.strip():
            raise ValueError("user_id must not be empty")
        self.user_id = user_id
        self.path = str(path)
        self.owner = owner or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.busy_timeout_ms = busy_timeout_ms
        self._db: Any = None
        self._write = asyncio.Lock()
        self.lease: Lease | None = None

    # -- lifecycle ------------------------------------------------------------

    async def open(self) -> "Journal":
        try:
            import aiosqlite
        except ImportError as exc:  # pragma: no cover - depends on the environment
            raise ImportError("the engine's journal needs aiosqlite: pip install synpath") from exc
        connection = aiosqlite.connect(self.path, isolation_level=None)
        # aiosqlite runs each connection on its own thread. Marking it a
        # daemon means a journal somebody forgot to close cannot keep the
        # process alive after everything else has finished.
        connection.daemon = True
        self._db = await connection
        try:
            self._db.row_factory = aiosqlite.Row
            await self._db.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
            await self._db.execute("PRAGMA journal_mode=WAL")
            # An order must be on disk before it is on the wire, so the write that
            # records it cannot be one the operating system is still holding.
            await self._db.execute("PRAGMA synchronous=FULL")
            await self._db.execute("PRAGMA foreign_keys=ON")
            await self._db.executescript(SCHEMA)
            await self._upgrade()
            await self._db.executescript(order_schema())
            self._order_records = OrderRecords(self._db)
            await self._upgrade_order_records()
            await self._upgrade_submissions()
            await self._db.execute("CREATE INDEX IF NOT EXISTS orders_parent ON orders(parent_id)")
            await self._db.execute(
                "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(SCHEMA_VERSION),),
            )
        except BaseException:
            await self._db.close()
            self._db = None
            raise
        return self

    async def _upgrade_submissions(self) -> None:
        """Keep pre-checkpoint client keys reserved without inventing requests."""
        async with self._db.execute("SELECT value FROM meta WHERE key='submissions_version'") as cursor:
            if await cursor.fetchone():
                return
        from .order_storage import ORDER_TABLES
        await self._db.execute("BEGIN IMMEDIATE")
        try:
            for kind, table in ORDER_TABLES.items():
                async with self._db.execute(
                    f"SELECT * FROM {table} WHERE request IS NOT NULL AND client_order_id IS NOT NULL"
                ) as cursor:
                    rows = await cursor.fetchall()
                for row in rows:
                    request = OrderRequest.model_validate(json.loads(row["request"]))
                    snapshot = json.loads(row["snapshot"])
                    if snapshot.get("account"):
                        account = Account.model_validate(snapshot["account"])
                    else:
                        account = request.account or Account(venue=row["venue"])
                    request = request.model_copy(update={"account": account, "user_id": row["user_id"],
                                                         "client_order_id": row["client_order_id"]})
                    result_id = (row["id"] if kind.startswith("stop") and snapshot.get("kind") else row["venue_order_id"])
                    await self._db.execute(
                        "INSERT INTO submissions(client_order_id,user_id,account_key,venue,request,effective_request,result_id,state,created_ts,updated_ts) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(client_order_id) DO NOTHING",
                        (row["client_order_id"], row["user_id"], account.key, row["venue"],
                         _dump(request_identity(request)), _dump(request.model_dump(mode="json")), result_id,
                         "accepted" if result_id else "pending", row["created_ts"], row["updated_ts"]),
                    )
            await self._db.execute("INSERT INTO meta(key,value) VALUES('submissions_version','1')")
            await self._db.execute("COMMIT")
        except BaseException:
            await self._db.execute("ROLLBACK")
            raise

    @property
    def is_open(self) -> bool:
        return self._db is not None

    async def typed_order(self, kind: str, venue: str, application_id: str) -> dict[str, Any] | None:
        """A type-owned instruction and its latest durable state."""
        return await self._order_records.get(kind, venue, application_id)

    async def _upgrade_order_records(self) -> None:
        """Backfill once without inventing ownership or missing instructions.

        Old venue records contain execution results, not necessarily the
        original user request. Keep that distinction and attribute unknown
        historical owners to an explicit legacy identity.
        """
        async with self._db.execute("SELECT value FROM meta WHERE key='order_records_version'") as cursor:
            if await cursor.fetchone():
                return
        await self._db.execute("BEGIN IMMEDIATE")
        try:
            async with self._db.execute("SELECT * FROM intents WHERE operation='create'") as cursor:
                intents = await cursor.fetchall()
            for row in intents:
                try:
                    request = OrderRequest.model_validate(json.loads(row["request"]))
                except (ValueError, TypeError):
                    continue
                await self._order_records.write_request(
                    request, venue=row["venue"], account_key=row["account_key"],
                    client_order_id=row["client_order_id"], user_id=request.user_id or UNATTRIBUTED_USER,
                    stamp=row["created_ts"], state=row["state"],
                )
            async with self._db.execute("SELECT payload FROM orders") as cursor:
                rows = await cursor.fetchall()
            for row in rows:
                try:
                    order = Order.model_validate(json.loads(row["payload"]))
                except (ValueError, TypeError):
                    continue
                if order.held_by.value == "venue":
                    await self._order_records.write_order(order, stamp=now_ms())
            async with self._db.execute("SELECT payload FROM managed_orders") as cursor:
                rows = await cursor.fetchall()
            for row in rows:
                snapshot = json.loads(row["payload"])
                if snapshot.get("venue") and snapshot.get("request"):
                    snapshot = {**snapshot, "request": {**snapshot["request"],
                                "user_id": snapshot["request"].get("user_id") or UNATTRIBUTED_USER}}
                    await self._order_records.write_managed(snapshot, user_id=UNATTRIBUTED_USER, stamp=now_ms())
            await self._db.execute("INSERT INTO meta(key,value) VALUES('order_records_version','1')")
            await self._db.execute("COMMIT")
        except BaseException:
            await self._db.execute("ROLLBACK")
            raise

    async def lease_records(self) -> list[dict[str, Any]]:
        """Lease inspection without exposing a database connection to callers."""
        async with self._db.execute("SELECT name, owner, expires_ts FROM leases") as cursor:
            return [dict(row) async for row in cursor]

    async def schema_version(self) -> int:
        async with self._db.execute("SELECT value FROM meta WHERE key='schema_version'") as cursor:
            row = await cursor.fetchone()
        return int(row["value"]) if row else 0

    async def _columns(self, table: str) -> dict[str, dict[str, Any]]:
        async with self._db.execute(f"PRAGMA table_info({table})") as cursor:
            rows = await cursor.fetchall()
        return {row["name"]: {"notnull": bool(row["notnull"]), "type": row["type"]} for row in rows}

    async def _upgrade(self) -> None:
        """Bring a file written by an older version up to this one. `SCHEMA` is
        all `CREATE TABLE IF NOT EXISTS`, so it adds tables but never changes
        one; anything else lives here, keyed on what the file actually has
        rather than on the number it claims, so a half-applied upgrade is
        finished rather than skipped.

        2 -> 3: `orders.parent_id` (a child names its parent in a column, not
        only inside its payload) and `managed_orders.account_key` nullable (a
        parent across venues has no single account).

        3 -> 4: a number of contracts is `quantity`, never `amount`: the
        `orders` and `fills` columns are renamed and the stored payloads
        (orders, fills, intents, engine-held parents) rewritten to the new
        names. The append-only `events` log keeps what it said at the time."""
        orders = await self._columns("orders")
        if "parent_id" not in orders:
            await self._db.execute("ALTER TABLE orders ADD COLUMN parent_id TEXT")
        if "amount" in orders or "amount" in await self._columns("fills"):
            await self._rename_amount_to_quantity()
        managed = await self._columns("managed_orders")
        if managed.get("account_key", {}).get("notnull"):
            # SQLite cannot drop NOT NULL in place: rebuild, copy, swap.
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                await self._db.execute("""
                    CREATE TABLE managed_orders_v3 (
                        id TEXT PRIMARY KEY,
                        kind TEXT NOT NULL,
                        state TEXT NOT NULL,
                        account_key TEXT,
                        market_id TEXT NOT NULL,
                        book TEXT,
                        trader TEXT,
                        payload TEXT NOT NULL,
                        child_order_id TEXT,
                        created_ts INTEGER NOT NULL,
                        updated_ts INTEGER NOT NULL
                    )""")
                await self._db.execute("""
                    INSERT INTO managed_orders_v3
                        (id, kind, state, account_key, market_id, book, trader, payload, child_order_id, created_ts, updated_ts)
                    SELECT id, kind, state, account_key, market_id, book, trader, payload, child_order_id, created_ts, updated_ts
                    FROM managed_orders""")
                await self._db.execute("DROP TABLE managed_orders")
                await self._db.execute("ALTER TABLE managed_orders_v3 RENAME TO managed_orders")
                await self._db.execute("COMMIT")
            except Exception:
                await self._db.execute("ROLLBACK")
                raise

    async def _rename_amount_to_quantity(self) -> None:
        await self._db.execute("BEGIN IMMEDIATE")
        try:
            for table in ("orders", "fills"):
                if "amount" in await self._columns(table):
                    await self._db.execute(f"ALTER TABLE {table} RENAME COLUMN amount TO quantity")
            for table, column in (("orders", "payload"), ("fills", "payload"),
                                  ("intents", "request"), ("managed_orders", "payload")):
                async with self._db.execute(f"SELECT rowid, {column} FROM {table}") as cursor:
                    rows = await cursor.fetchall()
                for row in rows:
                    try:
                        before = json.loads(row[column])
                    except (TypeError, ValueError):
                        continue
                    after = _quantity_keys(before)
                    if after != before:
                        await self._db.execute(f"UPDATE {table} SET {column}=? WHERE rowid=?", (_dump(after), row["rowid"]))
            await self._db.execute("COMMIT")
        except Exception:
            await self._db.execute("ROLLBACK")
            raise

    async def close(self) -> None:
        if self.lease is not None and self._db is not None:
            await self.release_lease()
        self.lease = None
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def __aenter__(self) -> "Journal":
        return await self.open()

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    # -- the log --------------------------------------------------------------

    async def append(self, kind: str, payload: dict[str, Any] | None = None, *, key: str | None = None) -> int:
        """One event. Returns its sequence number."""
        async with self._write:
            cursor = await self._db.execute(
                "INSERT INTO events(ts, kind, key, payload) VALUES(?,?,?,?)",
                (now_ms(), kind, key, _dump(payload or {})),
            )
            return int(cursor.lastrowid)

    async def replay(self, since: int = 0, *, kinds: Iterable[str] | None = None, limit: int | None = None) -> AsyncIterator[JournalEvent]:
        """Every event after `since`, oldest first."""
        sql = "SELECT seq, ts, kind, key, payload FROM events WHERE seq > ?"
        args: list[Any] = [since]
        if kinds:
            names = list(kinds)
            sql += f" AND kind IN ({','.join('?' * len(names))})"
            args += names
        sql += " ORDER BY seq"
        if limit:
            sql += f" LIMIT {int(limit)}"
        async with self._db.execute(sql, args) as cursor:
            async for row in cursor:
                yield JournalEvent(row["seq"], row["ts"], row["kind"], row["key"], json.loads(row["payload"]))

    async def last_seq(self) -> int:
        async with self._db.execute("SELECT COALESCE(MAX(seq), 0) AS seq FROM events") as cursor:
            row = await cursor.fetchone()
        return int(row["seq"])

    # -- submission identity --------------------------------------------------

    async def submission(self, client_order_id: str) -> dict[str, Any] | None:
        async with self._db.execute("SELECT * FROM submissions WHERE client_order_id=?", (client_order_id,)) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        result = dict(row)
        for field in ("request", "effective_request", "error"):
            result[field] = json.loads(result[field]) if result[field] else None
        return result

    async def record_submission(self, original: OrderRequest, effective: OrderRequest,
                                *, venue: str, account: Account) -> None:
        """Reserve the instruction and its audit event in one transaction."""
        stamp = now_ms()
        identity = request_identity(original)
        async with self._write:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                cursor = await self._db.execute(
                    "INSERT INTO submissions(client_order_id,user_id,account_key,venue,request,effective_request,created_ts,updated_ts) "
                    "VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(client_order_id) DO NOTHING",
                    (original.client_order_id, original.user_id, account.key, venue,
                     _dump(identity), _dump(effective.model_dump(mode="json")), stamp, stamp),
                )
                if cursor.rowcount:
                    await self._db.execute(
                        "INSERT INTO events(ts,kind,key,payload) VALUES(?,?,?,?)",
                        (stamp, "submission.reserved", original.client_order_id,
                         _dump({"user_id": original.user_id, "account": account.key, "venue": venue,
                                "request": identity, "effective_request": effective.model_dump(mode="json")})),
                    )
                else:
                    async with self._db.execute("SELECT request FROM submissions WHERE client_order_id=?",
                                                (original.client_order_id,)) as existing_cursor:
                        existing = await existing_cursor.fetchone()
                    if json.loads(existing["request"]) != identity:
                        raise BadRequest("client_order_id already identifies a different durable instruction")
                await self._db.execute("COMMIT")
            except BaseException:
                await self._db.execute("ROLLBACK")
                raise

    async def reject_submission(self, client_order_id: str, error: dict[str, Any]) -> None:
        async with self._write:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                stamp = now_ms()
                result = await self._db.execute(
                    "UPDATE submissions SET state='rejected', error=?, updated_ts=? WHERE client_order_id=? AND result_id IS NULL",
                    (_dump(error), stamp, client_order_id),
                )
                if result.rowcount:
                    await self._db.execute(
                        "INSERT INTO events(ts,kind,key,payload) VALUES(?,?,?,?)",
                        (stamp, "submission.rejected", client_order_id, _dump(error)),
                    )
                await self._db.execute("COMMIT")
            except BaseException:
                await self._db.execute("ROLLBACK")
                raise

    # -- intents --------------------------------------------------------------

    async def record_intent(self, request: OrderRequest, *, client_order_id: str,
                            venue: str, account: Account, operation: str = "create",
                            target_order_id: str | None = None) -> Intent:
        """Commit an instruction and its pending side effect before sending."""
        async with self._write:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                await self._write_intent(request, client_order_id=client_order_id, venue=venue,
                                         account=account, operation=operation,
                                         target_order_id=target_order_id, stamp=now_ms())
                await self._db.execute("COMMIT")
            except BaseException:
                await self._db.execute("ROLLBACK")
                raise
        return await self.intent(client_order_id)

    async def _write_intent(self, request: OrderRequest, *, client_order_id: str,
                            venue: str, account: Account, operation: str = "create",
                            target_order_id: str | None = None, stamp: int) -> None:
        body = request.model_dump(mode="json")
        await self._db.execute(
            """INSERT INTO intents(client_order_id, state, operation, venue, account_key, market_id,
                                   order_id, target_order_id, book, trader, attempts, request, detail,
                                   created_ts, updated_ts)
               VALUES(?,?,?,?,?,?,NULL,?,?,?,0,?,NULL,?,?)
               ON CONFLICT(client_order_id) DO NOTHING""",
            (client_order_id, IntentState.PLANNED, operation, venue, account.key, request.market_id,
             target_order_id, request.book, request.trader, _dump(body), stamp, stamp),
        )
        if operation == "create":
            await self._order_records.write_request(
                request, venue=venue, account_key=account.key, client_order_id=client_order_id,
                user_id=request.user_id or self.user_id, stamp=stamp,
            )
        await self._db.execute(
            "INSERT INTO events(ts, kind, key, payload) VALUES(?,?,?,?)",
            (stamp, "intent.planned", client_order_id,
             _dump({"operation": operation, "venue": venue, "account": account.key, "request": body,
                    "target_order_id": target_order_id})),
        )

    async def mark_intent(
        self,
        client_order_id: str,
        state: str,
        *,
        order_id: str | None = None,
        detail: str | None = None,
        bump_attempt: bool = False,
    ) -> None:
        stamp = now_ms()
        async with self._write:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                await self._db.execute(
                    f"""UPDATE intents SET state=?, updated_ts=?,
                        order_id=COALESCE(?, order_id), detail=?,
                        attempts=attempts+{1 if bump_attempt else 0}
                        WHERE client_order_id=?""",
                    (state, stamp, order_id, detail, client_order_id),
                )
                await self._db.execute(
                    "INSERT INTO events(ts, kind, key, payload) VALUES(?,?,?,?)",
                    (stamp, f"intent.{state}", client_order_id, _dump({"order_id": order_id, "detail": detail})),
                )
                await self._db.execute("COMMIT")
            except BaseException:
                await self._db.execute("ROLLBACK")
                raise

    async def intent(self, client_order_id: str) -> Intent | None:
        async with self._db.execute("SELECT * FROM intents WHERE client_order_id=?", (client_order_id,)) as cursor:
            row = await cursor.fetchone()
        return self._intent_of(row) if row else None

    async def intents(self, *, state: str | None = None, states: Iterable[str] | None = None, older_than_ms: int | None = None) -> list[Intent]:
        sql = "SELECT * FROM intents"
        args: list[Any] = []
        clauses = []
        names = [state] if state else list(states or [])
        if names:
            clauses.append(f"state IN ({','.join('?' * len(names))})")
            args += names
        if older_than_ms is not None:
            clauses.append("updated_ts <= ?")
            args.append(now_ms() - older_than_ms)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY updated_ts"
        async with self._db.execute(sql, args) as cursor:
            return [self._intent_of(row) async for row in cursor]

    async def in_doubt(self, older_than_ms: int = 0) -> list[Intent]:
        """Intents handed to a venue whose answer never came back."""
        return await self.intents(states=IN_DOUBT, older_than_ms=older_than_ms)

    @staticmethod
    def _intent_of(row: Any) -> Intent:
        return Intent(
            client_order_id=row["client_order_id"], state=row["state"], operation=row["operation"], venue=row["venue"],
            account_key=row["account_key"], market_id=row["market_id"], order_id=row["order_id"],
            target_order_id=row["target_order_id"], book=row["book"], trader=row["trader"], attempts=row["attempts"],
            request=json.loads(row["request"]), detail=row["detail"], created_ts=row["created_ts"],
            updated_ts=row["updated_ts"],
        )

    # -- orders and fills -----------------------------------------------------

    async def upsert_order(self, order: Order, *, event: str = "order.updated") -> None:
        stamp = now_ms()
        body = order.model_dump(mode="json")
        account_key = order.account.key if order.account else f"{order.venue}:default"
        async with self._write:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                await self._db.execute(
                    """INSERT INTO orders(venue, id, client_order_id, account_key, market_id, side,
                                          status, terminal, book, trader, quantity, filled, price, parent_id, payload,
                                          created_ts, updated_ts)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(venue, id) DO UPDATE SET
                          client_order_id=COALESCE(excluded.client_order_id, orders.client_order_id),
                          status=excluded.status, terminal=excluded.terminal, filled=excluded.filled,
                          price=excluded.price, quantity=excluded.quantity, payload=excluded.payload,
                          book=COALESCE(excluded.book, orders.book), trader=COALESCE(excluded.trader, orders.trader),
                          parent_id=COALESCE(excluded.parent_id, orders.parent_id),
                          updated_ts=excluded.updated_ts""",
                    (order.venue, order.id, order.client_order_id, account_key, order.market_id,
                     _word(order.side), _word(order.status), 1 if order.is_terminal else 0, order.book, order.trader,
                     str(order.quantity), str(order.filled), _text(order.price),
                     order.parent_id or order.tags.get("parent"), _dump(body),
                     order.created_at or stamp, stamp),
                )
                if order.held_by.value == "venue":
                    await self._order_records.write_order(order, stamp=stamp)
                await self._db.execute(
                    "UPDATE submissions SET result_id=?, state='accepted', updated_ts=? "
                    "WHERE client_order_id=? AND user_id=? AND account_key=?",
                    (order.id, stamp, order.client_order_id, order.user_id, account_key),
                )
                await self._db.execute(
                    "INSERT INTO events(ts, kind, key, payload) VALUES(?,?,?,?)",
                    (stamp, event, f"{order.venue}:{order.id}", _dump(body)),
                )
                await self._db.execute("COMMIT")
            except BaseException:
                await self._db.execute("ROLLBACK")
                raise

    async def record_fill(self, fill: Fill, *, book: str | None = None, trader: str | None = None) -> bool:
        """Store a fill. `False` means this fill id was already known, which
        happens on every venue that delivers at least once."""
        stamp = now_ms()
        body = fill.model_dump(mode="json")
        account_key = fill.account.key if fill.account else f"{fill.venue}:default"
        async with self._write:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                async with self._db.execute(
                    "SELECT settlement FROM fills WHERE venue=? AND id=?", (fill.venue, fill.id),
                ) as cursor:
                    existing = await cursor.fetchone()
                changed = existing is None or existing["settlement"] != _word(fill.settlement)
                await self._db.execute(
                    """INSERT INTO fills(venue, id, order_id, client_order_id, account_key, market_id,
                                         side, price, quantity, fee, settlement, book, trader, ts, payload)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(venue, id) DO UPDATE SET
                          settlement=excluded.settlement, fee=COALESCE(excluded.fee, fills.fee),
                          payload=excluded.payload,
                          book=COALESCE(fills.book, excluded.book), trader=COALESCE(fills.trader, excluded.trader)""",
                    (fill.venue, fill.id, fill.order_id, fill.client_order_id, account_key, fill.market_id,
                     _word(fill.side), str(fill.price), str(fill.quantity), _text(fill.fee),
                     _word(fill.settlement), book, trader, fill.timestamp or stamp, _dump(body)),
                )
                if changed:
                    await self._db.execute(
                        "INSERT INTO events(ts, kind, key, payload) VALUES(?,?,?,?)",
                        (stamp, "fill.new" if existing is None else "fill.settlement", f"{fill.venue}:{fill.id}", _dump(body)),
                    )
                await self._db.execute("COMMIT")
            except BaseException:
                await self._db.execute("ROLLBACK")
                raise
        return changed

    async def order(self, venue: str, order_id: str) -> Order | None:
        async with self._db.execute("SELECT payload FROM orders WHERE venue=? AND id=?", (venue, order_id)) as cursor:
            row = await cursor.fetchone()
        return Order.model_validate(json.loads(row["payload"])) if row else None

    async def order_by_client_id(self, client_order_id: str) -> Order | None:
        async with self._db.execute(
            "SELECT payload FROM orders WHERE client_order_id=? ORDER BY updated_ts DESC LIMIT 1", (client_order_id,),
        ) as cursor:
            row = await cursor.fetchone()
        return Order.model_validate(json.loads(row["payload"])) if row else None

    async def open_orders(self, *, venue: str | None = None, account_key: str | None = None, book: str | None = None) -> list[Order]:
        sql = "SELECT payload FROM orders WHERE terminal=0"
        args: list[Any] = []
        for column, value in (("venue", venue), ("account_key", account_key), ("book", book)):
            if value is not None:
                sql += f" AND {column}=?"
                args.append(value)
        async with self._db.execute(sql + " ORDER BY updated_ts", args) as cursor:
            return [Order.model_validate(json.loads(row["payload"])) async for row in cursor]

    async def fills(self, *, since_ts: int | None = None, venue: str | None = None, book: str | None = None) -> list[Fill]:
        """Fills as recorded. The strategy that placed the order is carried in
        `info["book"]`, because a venue's fill does not know about books and
        the ledger needs it to roll up."""
        sql = "SELECT payload, book, trader FROM fills WHERE 1=1"
        args: list[Any] = []
        if since_ts is not None:
            sql += " AND ts >= ?"
            args.append(since_ts)
        for column, value in (("venue", venue), ("book", book)):
            if value is not None:
                sql += f" AND {column}=?"
                args.append(value)
        async with self._db.execute(sql + " ORDER BY ts, id", args) as cursor:
            return [self._fill_of(row) async for row in cursor]

    @staticmethod
    def _fill_of(row: Any) -> Fill:
        fill = Fill.model_validate(json.loads(row["payload"]))
        extra = {k: row[k] for k in ("book", "trader") if row[k]}
        return fill.model_copy(update={"info": {**fill.info, **extra}}) if extra else fill

    async def has_fill(self, venue: str, fill_id: str) -> bool:
        async with self._db.execute("SELECT 1 FROM fills WHERE venue=? AND id=?", (venue, fill_id)) as cursor:
            return await cursor.fetchone() is not None

    async def orders_for_parent(self, parent_id: str) -> list[Order]:
        """Every venue order a parent put out, by the column, oldest first."""
        async with self._db.execute(
            "SELECT payload FROM orders WHERE parent_id=? ORDER BY created_ts, id", (parent_id,)
        ) as cursor:
            return [Order.model_validate(json.loads(row["payload"])) async for row in cursor]

    # -- buckets --------------------------------------------------------------

    async def save_bucket(self, bucket: dict[str, Any]) -> None:
        """Write a bucket and its members in one transaction; the member list
        is replaced whole, so a save is also an edit."""
        stamp = now_ms()
        members = bucket.get("members") or []
        async with self._write:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                await self._db.execute(
                    """INSERT INTO buckets(id, book, name, status, created_ts, updated_ts)
                       VALUES(?,?,?,?,?,?)
                       ON CONFLICT(id) DO UPDATE SET book=excluded.book, name=excluded.name,
                           status=excluded.status, updated_ts=excluded.updated_ts""",
                    (bucket["id"], bucket["book"], bucket["name"], bucket.get("status", "active"),
                     int(bucket.get("created_ms") or stamp), stamp),
                )
                await self._db.execute("DELETE FROM bucket_members WHERE bucket_id=?", (bucket["id"],))
                for position, member in enumerate(members):
                    await self._db.execute(
                        "INSERT INTO bucket_members(bucket_id, market_id, flip, position) VALUES(?,?,?,?)",
                        (bucket["id"], member["market_id"], 1 if member.get("flip") else 0, position),
                    )
                await self._db.execute(
                    "INSERT INTO events(ts, kind, key, payload) VALUES(?,?,?,?)",
                    (stamp, "bucket.saved", bucket["id"], _dump(bucket)),
                )
                await self._db.execute("COMMIT")
            except BaseException:
                await self._db.execute("ROLLBACK")
                raise

    async def bucket(self, bucket_id: str) -> dict[str, Any] | None:
        async with self._db.execute("SELECT * FROM buckets WHERE id=?", (bucket_id,)) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        async with self._db.execute(
            "SELECT market_id, flip FROM bucket_members WHERE bucket_id=? ORDER BY position", (bucket_id,)
        ) as cursor:
            members = [{"market_id": m["market_id"], "flip": bool(m["flip"])} async for m in cursor]
        return {"id": row["id"], "book": row["book"], "name": row["name"], "status": row["status"],
                "created_ms": row["created_ts"], "updated_ms": row["updated_ts"], "members": members}

    async def buckets(self, *, book: str | None = None, status: str | None = "active") -> list[dict[str, Any]]:
        sql, args = "SELECT id FROM buckets", []
        where = []
        if book is not None:
            where.append("book=?"); args.append(book)
        if status is not None:
            where.append("status=?"); args.append(status)
        if where:
            sql += " WHERE " + " AND ".join(where)
        async with self._db.execute(sql + " ORDER BY created_ts", args) as cursor:
            ids = [row["id"] async for row in cursor]
        out = []
        for bucket_id in ids:
            found = await self.bucket(bucket_id)
            if found is not None:
                out.append(found)
        return out

    async def archive_bucket(self, bucket_id: str) -> bool:
        """Soft delete: orders that referenced it still resolve."""
        async with self._write:
            cursor = await self._db.execute(
                "UPDATE buckets SET status='archived', updated_ts=? WHERE id=? AND status<>'archived'",
                (now_ms(), bucket_id),
            )
            return cursor.rowcount > 0

    # -- engine-held orders ---------------------------------------------------

    async def save_managed(self, snapshot: dict[str, Any]) -> None:
        """Commit the authoritative snapshot, shared projection and event."""
        async with self._write:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                await self._write_managed(snapshot, stamp=now_ms())
                await self._db.execute("COMMIT")
            except BaseException:
                await self._db.execute("ROLLBACK")
                raise

    async def plan_managed_child(self, snapshot: dict[str, Any], request: OrderRequest,
                                 *, venue: str, account: Account) -> None:
        """The irreversible parent transition and child dispatch commit together."""
        async with self._write:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                stamp = now_ms()
                await self._write_managed(snapshot, stamp=stamp)
                await self._write_intent(request, client_order_id=request.client_order_id,
                                         venue=venue, account=account, stamp=stamp)
                await self._db.execute("COMMIT")
            except BaseException:
                await self._db.execute("ROLLBACK")
                raise

    async def _write_managed(self, snapshot: dict[str, Any], *, stamp: int) -> None:
        account = snapshot.get("account") or {}
        account_key: str | None
        if not account or not account.get("venue"):
            account_key = None
        elif account.get("name"):
            account_key = f"{account['venue']}:{account['name']}"
        else:
            account_key = str(account["venue"])
        request = snapshot.get("request") or {}
        children = snapshot.get("children") or []
        await self._db.execute(
            """INSERT INTO managed_orders(id, kind, state, account_key, market_id, book, trader,
                                          payload, child_order_id, created_ts, updated_ts)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(id) DO UPDATE SET state=excluded.state, payload=excluded.payload,
                   child_order_id=excluded.child_order_id, updated_ts=excluded.updated_ts""",
            (snapshot["id"], snapshot.get("kind", ""), snapshot.get("state", ""), account_key,
             request.get("market_id", ""), request.get("book"), request.get("trader"),
             _dump(snapshot), children[-1]["order_id"] if children else None,
             int(snapshot.get("created_ms") or stamp), stamp),
        )
        await self._order_records.write_managed(snapshot, user_id=self.user_id, stamp=stamp)
        await self._db.execute(
            "INSERT INTO events(ts, kind, key, payload) VALUES(?,?,?,?)",
            (stamp, f"managed.{snapshot.get('state', 'updated')}", snapshot["id"], _dump(snapshot)),
        )
        await self._db.execute(
            "UPDATE submissions SET result_id=?, state='accepted', updated_ts=? "
            "WHERE client_order_id=? AND user_id=?",
            (snapshot["id"], stamp, request.get("client_order_id"), request.get("user_id")),
        )

    async def managed(self, *, states: Iterable[str] | None = None) -> list[dict[str, Any]]:
        sql = f"SELECT payload FROM ({MANAGED_SOURCE}) AS managed_source"
        args: list[Any] = []
        names = list(states or [])
        if names:
            sql += f" WHERE state IN ({','.join('?' * len(names))})"
            args += names
        async with self._db.execute(sql + " ORDER BY created_ts", args) as cursor:
            return [json.loads(row["payload"]) async for row in cursor]

    async def managed_on(self, market_id: str) -> list[tuple[str, dict[str, Any]]]:
        """Every engine-held order on one instrument, live or finished, as
        `(kind, snapshot)`, oldest first. A bucket's orders are found by
        `bucket:<id>`."""
        async with self._db.execute(
            f"SELECT kind, payload FROM ({MANAGED_SOURCE}) AS managed_source WHERE market_id=? ORDER BY created_ts", (market_id,)
        ) as cursor:
            return [(row["kind"], json.loads(row["payload"])) async for row in cursor]

    async def managed_one(self, parent_id: str) -> tuple[str, dict[str, Any]] | None:
        async with self._db.execute(f"SELECT kind, payload FROM ({MANAGED_SOURCE}) AS managed_source WHERE id=?", (parent_id,)) as cursor:
            row = await cursor.fetchone()
        return (row["kind"], json.loads(row["payload"])) if row else None

    # -- cursors, config, fair values ----------------------------------------

    async def set_cursor(self, name: str, value: Any) -> None:
        async with self._write:
            await self._db.execute(
                "INSERT INTO cursors(name, value, updated_ts) VALUES(?,?,?) "
                "ON CONFLICT(name) DO UPDATE SET value=excluded.value, updated_ts=excluded.updated_ts",
                (name, str(value), now_ms()),
            )

    async def cursor(self, name: str, default: Any = None) -> Any:
        async with self._db.execute("SELECT value FROM cursors WHERE name=?", (name,)) as cursor:
            row = await cursor.fetchone()
        return row["value"] if row else default

    async def save_config(self, kind: str, payload: dict[str, Any], *, author: str | None = None) -> int:
        """Store a configuration version. Risk configuration is versioned so a
        rejection can be explained by the rules that were in force."""
        async with self._write:
            cursor = await self._db.execute(
                "INSERT INTO config_versions(kind, ts, author, payload) VALUES(?,?,?,?)",
                (kind, now_ms(), author, _dump(payload)),
            )
            return int(cursor.lastrowid)

    async def config(self, kind: str, version: int | None = None) -> tuple[int, dict[str, Any]] | None:
        if version is None:
            sql, args = "SELECT version, payload FROM config_versions WHERE kind=? ORDER BY version DESC LIMIT 1", (kind,)
        else:
            sql, args = "SELECT version, payload FROM config_versions WHERE kind=? AND version=?", (kind, version)
        async with self._db.execute(sql, args) as cursor:
            row = await cursor.fetchone()
        return (int(row["version"]), json.loads(row["payload"])) if row else None

    async def set_fair_value(self, account_key: str, market_id: str, value: Decimal, *, source: str | None = None) -> None:
        async with self._write:
            await self._db.execute(
                "INSERT INTO fair_values(account_key, market_id, value, source, ts) VALUES(?,?,?,?,?) "
                "ON CONFLICT(account_key, market_id) DO UPDATE SET value=excluded.value, source=excluded.source, ts=excluded.ts",
                (account_key, market_id, str(value), source, now_ms()),
            )

    async def fair_values(self, account_key: str | None = None) -> dict[tuple[str, str], Decimal]:
        sql = "SELECT account_key, market_id, value FROM fair_values"
        args: list[Any] = []
        if account_key is not None:
            sql += " WHERE account_key=?"
            args.append(account_key)
        async with self._db.execute(sql, args) as cursor:
            return {(row["account_key"], row["market_id"]): Decimal(row["value"]) async for row in cursor}

    # -- the single-writer lease ---------------------------------------------

    async def acquire_lease(self, name: str = "engine", *, ttl_ms: int = 30_000, steal_expired: bool = True) -> Lease:
        """Take the right to write. Raises `LeaseLost` if another engine holds it."""
        # `_Held` carries the refusal out of the transaction, so the rollback
        # and the message that names the other engine do not tangle.
        stamp = now_ms()
        held_by: Any = None
        async with self._write:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                async with self._db.execute("SELECT * FROM leases WHERE name=?", (name,)) as cursor:
                    row = await cursor.fetchone()
                if row is not None and row["owner"] != self.owner and (row["expires_ts"] > stamp or not steal_expired):
                    held_by = (row["owner"], row["expires_ts"])
                    await self._db.execute("ROLLBACK")
                    raise _Held()
                await self._db.execute(
                    "INSERT INTO leases(name, owner, holder, acquired_ts, expires_ts) VALUES(?,?,?,?,?) "
                    "ON CONFLICT(name) DO UPDATE SET owner=excluded.owner, holder=excluded.holder, "
                    "acquired_ts=excluded.acquired_ts, expires_ts=excluded.expires_ts",
                    (name, self.owner, socket.gethostname(), stamp, stamp + ttl_ms),
                )
                await self._db.execute(
                    "INSERT INTO events(ts, kind, key, payload) VALUES(?,?,?,?)",
                    (stamp, "lease.acquired", name, _dump({"owner": self.owner, "ttl_ms": ttl_ms})),
                )
                await self._db.execute("COMMIT")
            except _Held:
                pass
            except BaseException:
                try:
                    await self._db.execute("ROLLBACK")
                except Exception:  # pragma: no cover - the transaction was already gone
                    pass
                raise
        if held_by is not None:
            raise LeaseLost(
                f"the journal at {self.path} is held by {held_by[0]} until {held_by[1]}; this engine will not trade"
            )
        self.lease = Lease(name=name, owner=self.owner, holder=socket.gethostname(), acquired_ts=stamp,
                           expires_ts=stamp + ttl_ms, ttl_ms=ttl_ms)
        return self.lease

    async def renew_lease(self) -> bool:
        """Extend this engine's lease. `False` means someone else took it."""
        if self.lease is None:
            return False
        stamp = now_ms()
        async with self._write:
            cursor = await self._db.execute(
                "UPDATE leases SET expires_ts=? WHERE name=? AND owner=?",
                (stamp + self.lease.ttl_ms, self.lease.name, self.owner),
            )
            if cursor.rowcount == 0:
                return False
        self.lease.expires_ts = stamp + self.lease.ttl_ms
        return True

    async def holds_lease(self) -> bool:
        if self.lease is None:
            return False
        async with self._db.execute("SELECT owner, expires_ts FROM leases WHERE name=?", (self.lease.name,)) as cursor:
            row = await cursor.fetchone()
        return bool(row and row["owner"] == self.owner and row["expires_ts"] > now_ms())

    async def release_lease(self) -> None:
        lease, self.lease = self.lease, None
        if lease is None:
            return
        async with self._write:
            await self._db.execute("DELETE FROM leases WHERE name=? AND owner=?", (lease.name, self.owner))
            await self._db.execute(
                "INSERT INTO events(ts, kind, key, payload) VALUES(?,?,?,?)",
                (now_ms(), "lease.released", lease.name, _dump({"owner": self.owner})),
            )
