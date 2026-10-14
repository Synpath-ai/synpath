"""PostgreSQL transport and lease for the shared journal operations.

No SQLite file is used. A connection serializes whole transactions, including
reads from other tasks, so a task never observes another task's partial write.
"""
from __future__ import annotations

import asyncio
import re
import socket
from typing import Any

from .journal import Journal, SCHEMA, SCHEMA_VERSION, _dump
from .order_storage import OrderRecords, schema as order_schema
from .storage_types import Lease, LeaseLost

SERVER_MS = "(extract(epoch from clock_timestamp()) * 1000)::bigint"


def postgres_schema() -> str:
    """The same logical columns; PostgreSQL identities and 64-bit timestamps."""
    source = SCHEMA + order_schema()
    source = source.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
    return re.sub(r"\bINTEGER\b", "BIGINT", source)


def postgres_query(query: str) -> str:
    """Compile the journal's fixed SQL dialect, never interpolating values."""
    if query == "BEGIN IMMEDIATE":
        return "BEGIN"
    query = query.replace("json_extract(snapshot, '$.kind')", "(snapshot::jsonb ->> 'kind')")
    query = query.replace("?", "%s")
    if query.startswith("INSERT INTO events(") and "RETURNING" not in query:
        query += " RETURNING seq"
    elif query.startswith("INSERT INTO config_versions(") and "RETURNING" not in query:
        query += " RETURNING version"
    return query


class Cursor:
    def __init__(self, rows, rowcount):
        self.rows = iter(rows)
        self.rowcount = rowcount
        self.lastrowid = rows[0].get("seq", rows[0].get("version")) if rows else None

    async def fetchone(self):
        return next(self.rows, None)

    async def fetchall(self):
        return list(self.rows)

    def __aiter__(self):
        return self

    async def __anext__(self):
        row = next(self.rows, None)
        if row is None:
            raise StopAsyncIteration
        return row


class Execution:
    """The await/context-manager query form used by the shared journal."""
    def __init__(self, connection, query, args):
        self.connection, self.query, self.args = connection, query, args
        self.cursor = None

    async def run(self):
        if self.cursor is None:
            self.cursor = await self.connection.run(self.query, self.args)
        return self.cursor

    def __await__(self):
        return self.run().__await__()

    async def __aenter__(self):
        return await self.run()

    async def __aexit__(self, *exc):
        pass


class Connection:
    def __init__(self, connection):
        self.connection = connection
        self.lock = asyncio.Lock()
        self.transaction_owner = None

    def execute(self, query, args=None):
        return Execution(self, query, args)

    async def run(self, query, args):
        task = asyncio.current_task()
        query = postgres_query(query)
        if query == "BEGIN":
            if self.transaction_owner is task:
                raise RuntimeError("nested journal transaction")
            await self.lock.acquire()
            try:
                await self.connection.execute("BEGIN")
                self.transaction_owner = task
            except BaseException:
                try:
                    await asyncio.shield(self.connection.execute("ROLLBACK"))
                finally:
                    self.lock.release()
                raise
            return Cursor([], 0)
        if query in ("COMMIT", "ROLLBACK"):
            if self.transaction_owner is not task:
                raise RuntimeError("transaction belongs to another task")
            try:
                await self.connection.execute(query)
            except BaseException:
                # A failed commit still needs the caller's rollback. Keep ownership
                # until rollback finishes so another task cannot use this session.
                if query == "ROLLBACK":
                    self.transaction_owner = None
                    self.lock.release()
                raise
            self.transaction_owner = None
            self.lock.release()
            return Cursor([], 0)
        if self.transaction_owner is task:
            return await self._execute(query, args)
        async with self.lock:
            return await self._execute(query, args)

    async def _execute(self, query, args):
        async with self.connection.cursor() as cursor:
            await cursor.execute(query, tuple(args) if args else None)
            rows = await cursor.fetchall() if cursor.description else []
            return Cursor(rows, cursor.rowcount)

    async def close(self):
        await self.connection.close()


class PostgresJournal(Journal):
    """Shared journal semantics on one explicitly selected PostgreSQL schema."""
    def __init__(self, dsn: str, *, schema: str = "public", **kwargs: Any):
        if not dsn.startswith(("postgresql://", "postgres://")) and "dbname=" not in dsn:
            raise ValueError("PostgreSQL storage requires an explicit connection string in journal_path")
        if (not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) or len(schema) > 63
                or schema.lower().startswith("pg_") or schema.lower() == "information_schema"):
            raise ValueError("invalid PostgreSQL schema name")
        # The display path is deliberately not the connection string.
        super().__init__(f"postgresql:{schema}", **kwargs)
        self._dsn, self.schema = dsn, schema

    async def open(self):
        if self.is_open:
            return self
        self._db = await self._connect()
        try:
            await self._initialize_schema()
            self._order_records = OrderRecords(self._db)
        except BaseException:
            await self._db.close()
            self._db = None
            raise
        return self

    async def _connect(self):
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError:
            raise ImportError("PostgreSQL storage needs: pip install 'synpath[postgres]'") from None
        try:
            connection = await psycopg.AsyncConnection.connect(
                self._dsn, autocommit=True, row_factory=dict_row, connect_timeout=10,
            )
        except psycopg.Error:
            raise ConnectionError("cannot connect to configured PostgreSQL journal") from None
        return Connection(connection)

    async def _initialize_schema(self):
        await self._db.execute("SET synchronous_commit=on")
        await self._db.execute("BEGIN IMMEDIATE")
        try:
            # Serialize first-start DDL even when two workers start together.
            await self._db.execute("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))",
                                   (f"synpath-schema:{self.schema}",))
            await self._select_schema()
            await self._apply_schema()
            await self._db.execute("COMMIT")
        except BaseException:
            await self._db.execute("ROLLBACK")
            raise

    async def _select_schema(self):
        await self._db.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"')
        await self._db.execute(f'SET search_path TO "{self.schema}", pg_catalog')
        await self._db.execute("CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        expected_versions = {"schema_version": str(SCHEMA_VERSION), "order_records_version": "1", "submissions_version": "1"}
        async with self._db.execute("SELECT key,value FROM meta WHERE key IN ('schema_version','order_records_version','submissions_version')") as cursor:
            versions = await cursor.fetchall()
        for version in versions:
            if version["value"] != expected_versions[version["key"]]:
                label = version["key"].replace("_", " ")
                raise ValueError(f"unsupported PostgreSQL journal {label}")

    async def _apply_schema(self):
        for statement in postgres_schema().split(";"):
            if statement.strip():
                await self._db.execute(statement)
        await self._db.execute("CREATE INDEX IF NOT EXISTS orders_parent ON orders(parent_id)")
        for key, value in (("schema_version", str(SCHEMA_VERSION)),
                           ("order_records_version", "1"), ("submissions_version", "1")):
            await self._db.execute(
                "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    async def acquire_lease(self, name="engine", *, ttl_ms=30_000, steal_expired=True):
        if ttl_ms <= 0:
            raise ValueError("lease duration must be positive")
        async with self._write:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                async with self._db.execute(f"""
                    INSERT INTO leases(name,owner,holder,acquired_ts,expires_ts)
                    VALUES(?,?,?,{SERVER_MS},{SERVER_MS}+?)
                    ON CONFLICT(name) DO UPDATE SET owner=excluded.owner,holder=excluded.holder,
                        acquired_ts=excluded.acquired_ts,expires_ts=excluded.expires_ts
                    WHERE leases.owner=? OR (? AND leases.expires_ts <= {SERVER_MS})
                    RETURNING acquired_ts,expires_ts
                """, (name, self.owner, socket.gethostname(), ttl_ms, self.owner, steal_expired)) as cursor:
                    row = await cursor.fetchone()
                if row is None:
                    raise LeaseLost("PostgreSQL journal already has an active execution worker")
                await self._db.execute("INSERT INTO events(ts,kind,key,payload) VALUES(?,?,?,?)",
                                       (row["acquired_ts"], "lease.acquired", name, _dump({"owner": self.owner, "ttl_ms": ttl_ms})))
                await self._db.execute("COMMIT")
            except BaseException:
                await self._db.execute("ROLLBACK")
                raise
        self.lease = Lease(name, self.owner, socket.gethostname(), row["acquired_ts"], row["expires_ts"], ttl_ms)
        return self.lease

    async def renew_lease(self):
        if self.lease is None:
            return False
        async with self._write:
            async with self._db.execute(f"UPDATE leases SET expires_ts={SERVER_MS}+? WHERE name=? AND owner=? AND expires_ts>{SERVER_MS} RETURNING expires_ts",
                                       (self.lease.ttl_ms, self.lease.name, self.owner)) as cursor:
                row = await cursor.fetchone()
        if row is None:
            return False
        self.lease.expires_ts = row["expires_ts"]
        return True

    async def holds_lease(self):
        if self.lease is None:
            return False
        async with self._db.execute(f"SELECT 1 FROM leases WHERE name=? AND owner=? AND expires_ts>{SERVER_MS}", (self.lease.name, self.owner)) as cursor:
            return await cursor.fetchone() is not None
