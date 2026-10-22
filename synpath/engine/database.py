"""SQLAlchemy transport preserving the journal's transaction boundary."""
from __future__ import annotations

import asyncio
from weakref import WeakSet
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.engine import URL, make_url
from sqlalchemy.pool import NullPool


async def finish_database_operation(awaitable):
    """Finish I/O before propagating cancellation to the rollback owner."""
    operation = asyncio.create_task(awaitable)
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError:
        while not operation.done():
            try:
                await asyncio.shield(operation)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        try:
            operation.result()
        except Exception:
            pass
        raise


class Cursor:
    def __init__(self, result):
        rows = list(result.mappings()) if result.returns_rows else []
        self.rows = iter(rows)
        self.rowcount = result.rowcount
        self.lastrowid = getattr(result, "lastrowid", None)
        if rows:
            self.lastrowid = rows[0].get("seq", rows[0].get("version", self.lastrowid))

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
    def __init__(self, engine, connection, *, postgres=False):
        self.engine, self.connection, self.postgres = engine, connection, postgres
        self.lock = asyncio.Lock()
        self.transaction_owner = None
        self.completed_transaction_owners = WeakSet()

    def execute(self, query, args=None):
        return Execution(self, query, args)

    async def executescript(self, source):
        for statement in source.split(";"):
            if statement.strip():
                await self.execute(statement)

    async def _execute(self, query, args=None):
        try:
            # Cancellation must not invalidate SQLAlchemy's connection while
            # another journal operation is about to roll back on that session.
            result = await finish_database_operation(
                self.connection.exec_driver_sql(query, tuple(args) if args else ()),
            )
        except DBAPIError as exc:
            # Keep existing public native exception contracts, including
            # definitive constraint failures, while SQLAlchemy owns transport.
            raise exc.orig from None
        return Cursor(result)

    async def run(self, query, args):
        if self.postgres:
            from .postgres import postgres_query
            query = postgres_query(query)
        task = asyncio.current_task()
        if query in ("BEGIN", "BEGIN IMMEDIATE"):
            self.completed_transaction_owners.discard(task)
            if self.transaction_owner is task:
                raise RuntimeError("nested journal transaction")
            await self.lock.acquire()
            try:
                await self._execute(query)
                self.transaction_owner = task
            except BaseException:
                try:
                    await finish_database_operation(self._execute("ROLLBACK"))
                finally:
                    self.lock.release()
                raise
            return CursorResult()
        if query in ("COMMIT", "ROLLBACK"):
            # A canceled caller may clean up after its shielded COMMIT already
            # succeeded. Do not roll back a completed or another task's transaction.
            if query == "ROLLBACK" and task in self.completed_transaction_owners:
                self.completed_transaction_owners.discard(task)
                return CursorResult()
            if self.transaction_owner is not task:
                raise RuntimeError("transaction belongs to another task")
            completed = False
            async def finish_transaction():
                nonlocal completed
                try:
                    result = await self._execute(query)
                except BaseException:
                    if query == "ROLLBACK":
                        self.transaction_owner = None
                        self.lock.release()
                    raise
                self.transaction_owner = None
                self.lock.release()
                completed = True
                return result
            try:
                return await finish_database_operation(finish_transaction())
            except asyncio.CancelledError:
                if completed and query == "COMMIT":
                    self.completed_transaction_owners.add(task)
                raise
        if self.transaction_owner is task:
            return await self._execute(query, args)
        async with self.lock:
            return await self._execute(query, args)

    async def close(self):
        try:
            await self.connection.close()
        finally:
            await self.engine.dispose()


class CursorResult:
    rowcount = 0
    lastrowid = None


async def connect_sqlite(path):
    return await _connect(URL.create("sqlite+aiosqlite", database=path))


async def connect_postgres(dsn):
    if dsn.startswith(("postgresql://", "postgres://")):
        url = make_url(dsn.replace("postgres://", "postgresql://", 1)).set(drivername="postgresql+psycopg")
        return await _connect(url, postgres=True, connect_args={"connect_timeout": 10})
    # Preserve libpq keyword DSNs without parsing or exposing secrets.
    from psycopg import AsyncConnection
    async def creator():
        return await AsyncConnection.connect(dsn, connect_timeout=10)
    return await _connect("postgresql+psycopg://", postgres=True, async_creator=creator)


async def _connect(url, *, postgres=False, **options):
    engine = create_async_engine(url, isolation_level="AUTOCOMMIT", poolclass=NullPool, **options)
    try:
        connection = await engine.connect()
    except BaseException:
        await engine.dispose()
        raise
    return Connection(engine, connection, postgres=postgres)
