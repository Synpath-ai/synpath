"""Shared order-type records, written inside the journal's transaction.

The four instruction tables own their type's request and durable state.
`orders` remains the shared execution projection used by the existing
ledger, reconciliation, and API. These writes never commit independently:
a type row and its journal event either both happen or neither happens.
"""
from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from ..trading.types import Account, Order, OrderRequest

UNATTRIBUTED_USER = "legacy:unattributed"
ORDER_TABLES = {
    "market": "market_orders",
    "limit": "limit_orders",
    "stop_market": "stop_market_orders",
    "stop_limit": "stop_limit_orders",
}
TYPE_COLUMNS = {
    "market": {"price": "TEXT"},
    "limit": {"price": "TEXT CHECK (request IS NULL OR price IS NOT NULL)"},
    "stop_market": {"stop_price": "TEXT CHECK (request IS NULL OR stop_price IS NOT NULL)", "trigger_source": "TEXT CHECK (request IS NULL OR trigger_source IS NOT NULL)"},
    "stop_limit": {"price": "TEXT CHECK (request IS NULL OR price IS NOT NULL)", "stop_price": "TEXT CHECK (request IS NULL OR stop_price IS NOT NULL)", "trigger_source": "TEXT CHECK (request IS NULL OR trigger_source IS NOT NULL)"},
}


def _dump(value: Any) -> str:
    return json.dumps(value, default=str, separators=(",", ":"))


def schema() -> str:
    """Only identifiers from the fixed order registry enter generated SQL."""
    statements = []
    for kind, table in ORDER_TABLES.items():
        columns = ", ".join(f"{name} {definition}" for name, definition in TYPE_COLUMNS[kind].items())
        statements.append(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                venue TEXT NOT NULL, id TEXT NOT NULL,
                user_id TEXT NOT NULL CHECK(length(trim(user_id)) > 0), account_key TEXT NOT NULL,
                client_order_id TEXT, parent_id TEXT,
                market_id TEXT NOT NULL, side TEXT NOT NULL,
                quantity TEXT NOT NULL, filled TEXT NOT NULL DEFAULT '0',
                remaining TEXT, state TEXT NOT NULL,
                time_in_force TEXT NOT NULL, expires_at INTEGER,
                reduce_only INTEGER NOT NULL DEFAULT 0,
                {columns},
                request TEXT, snapshot TEXT NOT NULL,
                payload_version INTEGER NOT NULL DEFAULT 1,
                venue_order_id TEXT, created_ts INTEGER NOT NULL,
                updated_ts INTEGER NOT NULL,
                PRIMARY KEY (venue, id)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS {table}_client
                ON {table}(user_id, account_key, client_order_id)
                WHERE client_order_id IS NOT NULL;
            CREATE INDEX IF NOT EXISTS {table}_state
                ON {table}(user_id, state, updated_ts);
        """)
    return "\n".join(statements)


class OrderRecords:
    def __init__(self, connection: Any, *, user_id: str | None = None):
        self.connection = connection
        self.user_id = user_id

    async def write_request(self, request: OrderRequest, *, venue: str, account_key: str,
                            client_order_id: str, user_id: str, stamp: int, state: str = "planned") -> None:
        body = {**request.model_dump(mode="json"), "client_order_id": client_order_id, "user_id": user_id}
        await self._write(request.type.value, venue, client_order_id, user_id, account_key, body,
                          state=state, request=body, snapshot=body, stamp=stamp)

    async def write_order(self, order: Order, *, stamp: int, user_id: str | None = None) -> None:
        kind = order.type.value
        if kind not in ORDER_TABLES:
            return
        account_key = order.account.key if order.account else f"{order.venue}:default"
        owner = order.user_id or user_id or UNATTRIBUTED_USER
        application_id = order.id
        if order.client_order_id:
            async with self.connection.execute(
                f"SELECT id FROM {ORDER_TABLES[kind]} WHERE user_id=? AND account_key=? AND client_order_id=?",
                (owner, account_key, order.client_order_id),
            ) as cursor:
                existing = await cursor.fetchone()
            if existing:
                application_id = existing["id"]
        body = order.model_dump(mode="json")
        await self._write(kind, order.venue, application_id, owner, account_key, body,
                          state=order.status.value, snapshot=body, stamp=stamp,
                          venue_order_id=order.id)

    async def write_managed(self, snapshot: dict[str, Any], *, user_id: str, stamp: int) -> None:
        request = snapshot.get("request") or {}
        kind = snapshot.get("kind", "")
        if kind not in ORDER_TABLES:
            return
        account = Account.model_validate(snapshot["account"])
        account_key = account.key
        await self._write(kind, snapshot["venue"], snapshot["id"], request.get("user_id") or user_id,
                          account_key, {**request, "filled": snapshot.get("filled", "0"),
                                        "remaining": str(max(Decimal("0"), Decimal(request["quantity"]) - Decimal(snapshot.get("filled", "0"))))},
                          state=snapshot["state"], request=request,
                          snapshot=snapshot, stamp=stamp)

    async def _write(self, kind: str, venue: str, application_id: str, user_id: str,
                     account_key: str, body: dict[str, Any], *, state: str,
                     snapshot: dict[str, Any], stamp: int, request: dict[str, Any] | None = None,
                     venue_order_id: str | None = None) -> None:
        table = ORDER_TABLES.get(kind)
        if table is None:
            return
        columns = ["venue", "id", "user_id", "account_key", "client_order_id", "parent_id",
                   "market_id", "side", "quantity", "filled", "remaining", "state",
                   "time_in_force", "expires_at", "reduce_only", *TYPE_COLUMNS[kind],
                   "request", "snapshot", "venue_order_id", "created_ts", "updated_ts"]
        values = [venue, application_id, user_id, account_key, body.get("client_order_id"),
                  body.get("parent_id") or (body.get("tags") or {}).get("parent"),
                  body["market_id"], body["side"], str(body["quantity"]), str(body.get("filled", "0")),
                  body.get("remaining"), state, body.get("time_in_force", "gtc"), body.get("expires_at"),
                  # Left from before orders said their outcome and action (now in
                  # `request` and `snapshot`); always 0 for an order written since.
                  int(body.get("reduce_only", False))]
        values.extend(body.get(name, "touch" if name == "trigger_source" else None)
                      for name in TYPE_COLUMNS[kind])
        values.extend([_dump(request) if request is not None else None, _dump(snapshot), venue_order_id,
                       int(body.get("created_at") or snapshot.get("created_ms") or stamp), stamp])
        if self.user_id is not None and user_id != self.user_id and request is None:
            record = {**dict(zip(columns, values)), "payload_version": 1}
            await self.connection.execute(
                "INSERT INTO historical_order_records(kind,venue,id,user_id,record) VALUES(?,?,?,?,?) "
                "ON CONFLICT(kind,venue,id) DO UPDATE SET record=excluded.record",
                (kind, venue, application_id, user_id, _dump(record)),
            )
            return
        # The submitted instruction and owner never change when a venue result
        # arrives. Its mutable execution state is a separate snapshot.
        updates = [f"{name}=excluded.{name}" for name in
                   ("filled", "remaining", "state", "snapshot", "updated_ts")]
        updates += [f"request=COALESCE({table}.request, excluded.request)",
                    f"venue_order_id=COALESCE(excluded.venue_order_id, {table}.venue_order_id)"]
        await self.connection.execute(
            f"INSERT INTO {table}({','.join(columns)}) VALUES({','.join('?' for _ in columns)}) "
            f"ON CONFLICT(venue,id) DO UPDATE SET {','.join(updates)}", values,
        )

    async def get(self, kind: str, venue: str, application_id: str) -> dict[str, Any] | None:
        table = ORDER_TABLES.get(kind)
        if table is None:
            raise ValueError(f"no instruction table for order type {kind!r}")
        async with self.connection.execute(f"SELECT * FROM {table} WHERE venue=? AND id=?",
                                           (venue, application_id)) as cursor:
            row = await cursor.fetchone()
        if row is None:
            async with self.connection.execute("SELECT record FROM historical_order_records WHERE kind=? AND venue=? AND id=?",
                                               (kind, venue, application_id)) as cursor:
                historical = await cursor.fetchone()
            if historical is None:
                return None
            row = json.loads(historical["record"])
        result = dict(row)
        result["request"] = json.loads(result["request"]) if result["request"] else None
        result["snapshot"] = json.loads(result["snapshot"])
        return result
