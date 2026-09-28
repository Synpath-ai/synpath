"""Who may do what, and a record of every time that changed.

The read API needs no accounts. The moment the server can place an order it
needs to know who is asking, what they are allowed to touch, and -- when
somebody later asks why a key could trade an account it should not have --
what the permissions were at the time and who changed them.

Three tables and one rule:

**Keys are stored as hashes.** A key is shown once, when it is issued. What
is kept is `sha256` of it, so a stolen database cannot be used to trade. Keys
carry a prefix in clear (`sk_live_9f2a…`) purely so a human can tell two
keys apart in a list.

**Grants are per subaccount.** A grant is `(user, account, permission)`
where the account is a `venue:name` key or `*`, and the permission is one of
`view`, `trade`, `manage_credentials`, `manage_members`. Nothing is implied:
a user who may trade may not grant, and a user who may grant may not trade.

**The audit table is append-only, enforced by the database.** Every insert,
update or delete on the keys or grants tables fires a trigger that writes
the actor, the request, and the row before and after. Triggers on the audit
table itself raise on update and delete, so no route, no bug and no hand at
a SQL prompt can quietly change history -- and there is no route that tries.
"""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Literal

Permission = Literal["view", "trade", "manage_credentials", "manage_members"]
PERMISSIONS: tuple[Permission, ...] = ("view", "trade", "manage_credentials", "manage_members")
ALL_ACCOUNTS = "*"

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_ts INTEGER NOT NULL,
    disabled INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS api_keys (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id),
    prefix TEXT NOT NULL,
    hash TEXT NOT NULL UNIQUE,
    label TEXT,
    created_ts INTEGER NOT NULL,
    last_used_ts INTEGER,
    revoked_ts INTEGER
);
CREATE INDEX IF NOT EXISTS api_keys_user ON api_keys(user_id);

CREATE TABLE IF NOT EXISTS grants (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id),
    account TEXT NOT NULL,
    permission TEXT NOT NULL,
    granted_ts INTEGER NOT NULL,
    UNIQUE (user_id, account, permission)
);
CREATE INDEX IF NOT EXISTS grants_user ON grants(user_id);

CREATE TABLE IF NOT EXISTS audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    actor TEXT,
    request TEXT,
    table_name TEXT NOT NULL,
    action TEXT NOT NULL,
    row_id TEXT,
    before TEXT,
    after TEXT
);
CREATE INDEX IF NOT EXISTS audit_ts ON audit(ts);

-- One row, rewritten at the start of every request that changes anything, so
-- the triggers can record who asked and under which request.
CREATE TABLE IF NOT EXISTS audit_context (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    actor TEXT,
    request TEXT
);
INSERT INTO audit_context(id, actor, request) VALUES(1, NULL, NULL)
    ON CONFLICT(id) DO NOTHING;
"""

TRIGGERS = """
CREATE TRIGGER IF NOT EXISTS audit_grants_insert AFTER INSERT ON grants BEGIN
    INSERT INTO audit(ts, actor, request, table_name, action, row_id, before, after)
    VALUES (CAST(strftime('%s','now') AS INTEGER) * 1000,
            (SELECT actor FROM audit_context WHERE id = 1),
            (SELECT request FROM audit_context WHERE id = 1),
            'grants', 'insert', NEW.id, NULL,
            json_object('user_id', NEW.user_id, 'account', NEW.account, 'permission', NEW.permission));
END;

CREATE TRIGGER IF NOT EXISTS audit_grants_update AFTER UPDATE ON grants BEGIN
    INSERT INTO audit(ts, actor, request, table_name, action, row_id, before, after)
    VALUES (CAST(strftime('%s','now') AS INTEGER) * 1000,
            (SELECT actor FROM audit_context WHERE id = 1),
            (SELECT request FROM audit_context WHERE id = 1),
            'grants', 'update', NEW.id,
            json_object('user_id', OLD.user_id, 'account', OLD.account, 'permission', OLD.permission),
            json_object('user_id', NEW.user_id, 'account', NEW.account, 'permission', NEW.permission));
END;

CREATE TRIGGER IF NOT EXISTS audit_grants_delete AFTER DELETE ON grants BEGIN
    INSERT INTO audit(ts, actor, request, table_name, action, row_id, before, after)
    VALUES (CAST(strftime('%s','now') AS INTEGER) * 1000,
            (SELECT actor FROM audit_context WHERE id = 1),
            (SELECT request FROM audit_context WHERE id = 1),
            'grants', 'delete', OLD.id,
            json_object('user_id', OLD.user_id, 'account', OLD.account, 'permission', OLD.permission), NULL);
END;

CREATE TRIGGER IF NOT EXISTS audit_keys_insert AFTER INSERT ON api_keys BEGIN
    INSERT INTO audit(ts, actor, request, table_name, action, row_id, before, after)
    VALUES (CAST(strftime('%s','now') AS INTEGER) * 1000,
            (SELECT actor FROM audit_context WHERE id = 1),
            (SELECT request FROM audit_context WHERE id = 1),
            'api_keys', 'insert', NEW.id, NULL,
            json_object('user_id', NEW.user_id, 'prefix', NEW.prefix, 'label', NEW.label));
END;

CREATE TRIGGER IF NOT EXISTS audit_keys_revoke AFTER UPDATE OF revoked_ts ON api_keys
WHEN NEW.revoked_ts IS NOT NULL AND OLD.revoked_ts IS NULL BEGIN
    INSERT INTO audit(ts, actor, request, table_name, action, row_id, before, after)
    VALUES (CAST(strftime('%s','now') AS INTEGER) * 1000,
            (SELECT actor FROM audit_context WHERE id = 1),
            (SELECT request FROM audit_context WHERE id = 1),
            'api_keys', 'revoke', NEW.id,
            json_object('user_id', OLD.user_id, 'prefix', OLD.prefix, 'revoked_ts', OLD.revoked_ts),
            json_object('user_id', NEW.user_id, 'prefix', NEW.prefix, 'revoked_ts', NEW.revoked_ts));
END;

-- History is written once. These are the reason there is no route that
-- edits it: even a mistaken one cannot.
CREATE TRIGGER IF NOT EXISTS audit_is_append_only_update BEFORE UPDATE ON audit BEGIN
    SELECT RAISE(ABORT, 'the audit log is append-only');
END;

CREATE TRIGGER IF NOT EXISTS audit_is_append_only_delete BEFORE DELETE ON audit BEGIN
    SELECT RAISE(ABORT, 'the audit log is append-only');
END;
"""


def now_ms() -> int:
    return int(time.time() * 1000)


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def new_key(prefix: str = "sk") -> str:
    """A key with enough entropy that the hash needs no salt."""
    return f"{prefix}_{secrets.token_urlsafe(32)}"


@dataclass(frozen=True, slots=True)
class User:
    id: str
    name: str
    created_ts: int
    disabled: bool = False


@dataclass(frozen=True, slots=True)
class IssuedKey:
    """What issuing a key returns. `secret` is shown once and never stored."""

    id: str
    user_id: str
    prefix: str
    secret: str
    label: str | None = None


@dataclass(frozen=True, slots=True)
class Grant:
    id: str
    user_id: str
    account: str
    permission: Permission
    granted_ts: int


@dataclass(frozen=True, slots=True)
class Principal:
    """Who is asking, and what they may touch."""

    user_id: str
    name: str
    key_id: str
    grants: tuple[Grant, ...] = ()

    def may(self, permission: Permission, account: str | None = None) -> bool:
        for grant in self.grants:
            if grant.permission != permission:
                continue
            if grant.account == ALL_ACCOUNTS or account is None or grant.account == account:
                return True
        return False

    def accounts(self, permission: Permission) -> set[str]:
        return {g.account for g in self.grants if g.permission == permission}


class ControlStore:
    """Users, keys, grants and the audit log."""

    def __init__(self, path: str | os.PathLike[str] = "synpath-control.db"):
        self.path = str(path)
        self._db: Any = None

    async def open(self) -> "ControlStore":
        try:
            import aiosqlite
        except ImportError as exc:  # pragma: no cover - depends on the environment
            raise ImportError("the server's account store needs aiosqlite: pip install synpath") from exc
        connection = aiosqlite.connect(self.path, isolation_level=None)
        connection.daemon = True
        self._db = await connection
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA foreign_keys=ON")
        await self._db.executescript(SCHEMA)
        await self._db.executescript(TRIGGERS)
        return self

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def __aenter__(self) -> "ControlStore":
        return await self.open()

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    # -- who is acting --------------------------------------------------------

    async def acting_as(self, actor: str | None, request: str | None = None) -> None:
        """Name the actor for the audit rows the next writes will produce."""
        await self._db.execute("UPDATE audit_context SET actor=?, request=? WHERE id=1", (actor, request))

    # -- users and keys -------------------------------------------------------

    async def create_user(self, name: str, *, user_id: str | None = None) -> User:
        user = User(id=user_id or f"u_{secrets.token_hex(8)}", name=name, created_ts=now_ms())
        await self._db.execute("INSERT INTO users(id, name, created_ts, disabled) VALUES(?,?,?,0)",
                               (user.id, user.name, user.created_ts))
        return user

    async def issue_key(self, user_id: str, *, label: str | None = None) -> IssuedKey:
        secret = new_key()
        key_id = f"k_{secrets.token_hex(8)}"
        await self._db.execute(
            "INSERT INTO api_keys(id, user_id, prefix, hash, label, created_ts) VALUES(?,?,?,?,?,?)",
            (key_id, user_id, secret[:12], hash_key(secret), label, now_ms()),
        )
        return IssuedKey(id=key_id, user_id=user_id, prefix=secret[:12], secret=secret, label=label)

    async def revoke_key(self, key_id: str) -> bool:
        cursor = await self._db.execute(
            "UPDATE api_keys SET revoked_ts=? WHERE id=? AND revoked_ts IS NULL", (now_ms(), key_id),
        )
        return cursor.rowcount > 0

    async def principal(self, secret: str) -> Principal | None:
        """Who a key belongs to, with the grants it carries right now."""
        async with self._db.execute(
            "SELECT k.id AS key_id, k.user_id, u.name, u.disabled FROM api_keys k JOIN users u ON u.id = k.user_id "
            "WHERE k.hash=? AND k.revoked_ts IS NULL",
            (hash_key(secret),),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None or row["disabled"]:
            return None
        await self._db.execute("UPDATE api_keys SET last_used_ts=? WHERE id=?", (now_ms(), row["key_id"]))
        grants = await self.grants(row["user_id"])
        return Principal(user_id=row["user_id"], name=row["name"], key_id=row["key_id"], grants=tuple(grants))

    async def keys(self, user_id: str) -> list[dict[str, Any]]:
        async with self._db.execute(
            "SELECT id, prefix, label, created_ts, last_used_ts, revoked_ts FROM api_keys WHERE user_id=? ORDER BY created_ts",
            (user_id,),
        ) as cursor:
            return [dict(row) async for row in cursor]

    # -- grants ---------------------------------------------------------------

    async def grant(self, user_id: str, account: str, permission: Permission) -> Grant:
        if permission not in PERMISSIONS:
            raise ValueError(f"{permission!r} is not a permission; known: {PERMISSIONS}")
        row = Grant(id=f"g_{secrets.token_hex(8)}", user_id=user_id, account=account, permission=permission,
                    granted_ts=now_ms())
        await self._db.execute(
            "INSERT INTO grants(id, user_id, account, permission, granted_ts) VALUES(?,?,?,?,?) "
            "ON CONFLICT(user_id, account, permission) DO NOTHING",
            (row.id, row.user_id, row.account, row.permission, row.granted_ts),
        )
        return row

    async def revoke(self, user_id: str, account: str, permission: Permission) -> bool:
        cursor = await self._db.execute(
            "DELETE FROM grants WHERE user_id=? AND account=? AND permission=?", (user_id, account, permission),
        )
        return cursor.rowcount > 0

    async def grants(self, user_id: str) -> list[Grant]:
        async with self._db.execute(
            "SELECT id, user_id, account, permission, granted_ts FROM grants WHERE user_id=? ORDER BY granted_ts",
            (user_id,),
        ) as cursor:
            return [Grant(row["id"], row["user_id"], row["account"], row["permission"], row["granted_ts"])
                    async for row in cursor]

    # -- the audit log --------------------------------------------------------

    async def audit(self, *, since_id: int = 0, limit: int = 200, table: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT id, ts, actor, request, table_name, action, row_id, before, after FROM audit WHERE id > ?"
        args: list[Any] = [since_id]
        if table:
            sql += " AND table_name = ?"
            args.append(table)
        sql += " ORDER BY id LIMIT ?"
        args.append(limit)
        async with self._db.execute(sql, args) as cursor:
            rows = [dict(row) async for row in cursor]
        for row in rows:
            for key in ("before", "after"):
                if row[key]:
                    row[key] = json.loads(row[key])
        return rows

    async def has_users(self) -> bool:
        """Whether anyone has been created yet: false on a new control database."""
        async with self._db.execute("SELECT 1 FROM users LIMIT 1") as cursor:
            return await cursor.fetchone() is not None

    async def bootstrap(self, name: str = "owner") -> tuple[User, IssuedKey]:
        """The first user, with every permission on every account.

        Called once, by the operator starting the server, so there is a key to
        make the next key with.
        """
        user = await self.create_user(name)
        await self.acting_as(f"bootstrap:{user.id}", "bootstrap")
        for permission in PERMISSIONS:
            await self.grant(user.id, ALL_ACCOUNTS, permission)
        key = await self.issue_key(user.id, label="bootstrap")
        await self.acting_as(None, None)
        return user, key
