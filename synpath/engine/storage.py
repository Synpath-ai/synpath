"""Storage boundary for execution, independent of database SQL.

SQLite and PostgreSQL implement the same transaction and recovery contract.
Each backend owns its connection, schema initialization and execution lease.
A deployment selects exactly one store.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, AsyncIterator, Iterable, Protocol, runtime_checkable

from ..trading.types import Account, Fill, Order, OrderRequest
from .storage_types import Intent, JournalEvent, Lease


@runtime_checkable
class JournalStore(Protocol):
    owner: str
    path: str
    user_id: str
    lease: Lease | None
    async def __aenter__(self) -> JournalStore:
        ...

    async def __aexit__(self, *exc: Any) -> None:
        ...

    async def open(self) -> 'JournalStore':
        ...

    @property
    def is_open(self) -> bool:
        ...

    async def typed_order(self, kind: str, venue: str, application_id: str) -> dict[str, Any] | None:
        ...

    async def lease_records(self) -> list[dict[str, Any]]:
        ...

    async def schema_version(self) -> int:
        ...

    async def close(self) -> None:
        ...

    async def append(self, kind: str, payload: dict[str, Any] | None=None, *, key: str | None=None) -> int:
        ...

    def replay(self, since: int=0, *, kinds: Iterable[str] | None=None, limit: int | None=None) -> AsyncIterator[JournalEvent]:
        ...

    async def last_seq(self) -> int:
        ...

    async def submission(self, client_order_id: str) -> dict[str, Any] | None:
        ...

    async def record_submission(self, original: OrderRequest, effective: OrderRequest,
                                *, venue: str, account: Account | None) -> None:
        ...

    async def reject_submission(self, client_order_id: str, error: dict[str, Any]) -> None:
        ...

    async def plan_managed_child(self, snapshot: dict[str, Any], request: OrderRequest,
                                 *, venue: str, account: Account, parent_order: Order | None = None) -> None:
        ...

    async def record_intent(self, request: OrderRequest, *, client_order_id: str, venue: str, account: Account, operation: str='create', target_order_id: str | None=None) -> Intent:
        ...

    async def mark_intent(self, client_order_id: str, state: str, *, order_id: str | None=None, detail: str | None=None, bump_attempt: bool=False) -> None:
        ...

    async def intent(self, client_order_id: str) -> Intent | None:
        ...

    async def intents(self, *, state: str | None=None, states: Iterable[str] | None=None, older_than_ms: int | None=None) -> list[Intent]:
        ...

    async def in_doubt(self, older_than_ms: int=0) -> list[Intent]:
        ...

    async def upsert_order(self, order: Order, *, event: str='order.updated') -> None:
        ...

    async def record_fill(self, fill: Fill, *, book: str | None=None, trader: str | None=None) -> bool:
        ...

    async def order(self, venue: str | None, order_id: str) -> Order | None:
        ...

    async def order_by_client_id(self, client_order_id: str) -> Order | None:
        ...

    async def open_orders(self, *, venue: str | None=None, account_key: str | None=None, book: str | None=None) -> list[Order]:
        ...

    async def events_page(self, *, kinds: Iterable[str]=(), prefixes: Iterable[str]=(),
                          keys: Iterable[str] | None=None, before: int | None=None,
                          limit: int=500) -> list[JournalEvent]:
        ...

    async def orders_page(self, *, venue: str | None=None, book: str | None=None, market_id: str | None=None,
                          since_ts: int | None=None, before: tuple[int, str, str] | None=None,
                          open_only: bool=False, limit: int | None=100) -> list[tuple[Order, tuple[int, str, str]]]:
        ...

    async def fills(self, *, since_ts: int | None=None, venue: str | None=None, book: str | None=None) -> list[Fill]:
        ...

    async def has_fill(self, venue: str, fill_id: str) -> bool:
        ...

    async def orders_for_parent(self, parent_id: str) -> list[Order]:
        ...

    async def save_bucket(self, bucket: dict[str, Any]) -> None:
        ...

    async def bucket(self, bucket_id: str) -> dict[str, Any] | None:
        ...

    async def buckets(self, *, book: str | None=None, status: str | None='active') -> list[dict[str, Any]]:
        ...

    async def archive_bucket(self, bucket_id: str) -> bool:
        ...

    async def save_managed(self, snapshot: dict[str, Any], *, order: Order | None = None) -> None:
        ...

    async def managed(self, *, states: Iterable[str] | None=None) -> list[dict[str, Any]]:
        ...

    async def managed_on(self, market_id: str) -> list[tuple[str, dict[str, Any]]]:
        ...

    async def routed_account_keys(self, venue: str) -> list[str]:
        ...

    async def managed_one(self, parent_id: str) -> tuple[str, dict[str, Any]] | None:
        ...

    async def set_cursor(self, name: str, value: Any) -> None:
        ...

    async def cursor(self, name: str, default: Any=None) -> Any:
        ...

    async def save_config(self, kind: str, payload: dict[str, Any], *, author: str | None=None) -> int:
        ...

    async def config(self, kind: str, version: int | None=None) -> tuple[int, dict[str, Any]] | None:
        ...

    async def acquire_lease(self, name: str='engine', *, ttl_ms: int=30000, steal_expired: bool=True) -> Lease:
        ...

    async def renew_lease(self) -> bool:
        ...

    async def holds_lease(self) -> bool:
        ...

    async def release_lease(self) -> None:
        ...


def create_journal(*, backend: str = "sqlite", path: str = "synpath.db",
                   user_id: str = "local-user", schema: str = "public") -> JournalStore:
    """Reject unsupported backends instead of silently using a local file."""
    if backend == "postgresql":
        from .postgres import PostgresJournal
        return PostgresJournal(path, user_id=user_id, schema=schema)
    if backend != "sqlite":
        raise ValueError(f"storage backend {backend!r} is not available; choose sqlite or postgresql")
    from .journal import Journal
    return Journal(path, user_id=user_id)
