"""Journal schema 3: the upgrade from 2, the parent column, and bucket storage."""
from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from synpath.engine.journal import SCHEMA_VERSION, Journal
from synpath.trading.types import Account, Order, OrderStatus, OrderType, Side, TimeInForce

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


V2_TABLES = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE events (seq INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, kind TEXT NOT NULL,
                     key TEXT, payload TEXT NOT NULL);
CREATE TABLE orders (
    venue TEXT NOT NULL, id TEXT NOT NULL, client_order_id TEXT, account_key TEXT NOT NULL,
    market_id TEXT NOT NULL, side TEXT NOT NULL, status TEXT NOT NULL, terminal INTEGER NOT NULL DEFAULT 0,
    book TEXT, trader TEXT, amount TEXT NOT NULL, filled TEXT NOT NULL, price TEXT, payload TEXT NOT NULL,
    created_ts INTEGER, updated_ts INTEGER NOT NULL, PRIMARY KEY (venue, id));
CREATE TABLE managed_orders (
    id TEXT PRIMARY KEY, kind TEXT NOT NULL, state TEXT NOT NULL, account_key TEXT NOT NULL,
    market_id TEXT NOT NULL, book TEXT, trader TEXT, payload TEXT NOT NULL, child_order_id TEXT,
    created_ts INTEGER NOT NULL, updated_ts INTEGER NOT NULL);
INSERT INTO meta VALUES ('schema_version', '2');
"""


def write_v2_file(path: str) -> None:
    """A journal as version 2 left it, with one row in each table that changes."""
    db = sqlite3.connect(path)
    db.executescript(V2_TABLES)
    db.execute(
        "INSERT INTO orders VALUES ('kalshi','K-1','c-1','kalshi:default','kalshi:KX-A','buy','open',0,"
        "'alpha','t','5','0','0.40','{}',1,1)"
    )
    db.execute(
        "INSERT INTO managed_orders VALUES ('mo-1','iceberg','working','kalshi:default','kalshi:KX-A',"
        "'alpha','t','{\"id\":\"mo-1\"}',NULL,1,1)"
    )
    db.commit()
    db.close()


def columns(path: str, table: str) -> dict[str, bool]:
    db = sqlite3.connect(path)
    rows = db.execute(f"PRAGMA table_info({table})").fetchall()
    db.close()
    return {row[1]: bool(row[3]) for row in rows}   # name -> notnull


class TestUpgradeFromTwo:
    async def test_a_fresh_file_is_version_three_with_the_new_shape(self, tmp_path: Path):
        path = str(tmp_path / "fresh.db")
        async with Journal(path) as journal:
            assert await journal.schema_version() == SCHEMA_VERSION == 3
        assert "parent_id" in columns(path, "orders")
        assert columns(path, "managed_orders")["account_key"] is False
        assert "bucket_id" in columns(path, "bucket_members")

    async def test_a_version_two_file_is_upgraded_and_keeps_its_rows(self, tmp_path: Path):
        path = str(tmp_path / "old.db")
        write_v2_file(path)
        assert "parent_id" not in columns(path, "orders")
        assert columns(path, "managed_orders")["account_key"] is True

        async with Journal(path) as journal:
            assert await journal.schema_version() == 3
            parents = await journal.managed(states=("working",))
            assert [p["id"] for p in parents] == ["mo-1"]
        assert "parent_id" in columns(path, "orders")
        assert columns(path, "managed_orders")["account_key"] is False
        # The old order row survived the column add, with the new column empty.
        db = sqlite3.connect(path)
        assert db.execute("SELECT count(*), max(parent_id) FROM orders").fetchone() == (1, None)
        assert db.execute("SELECT name FROM sqlite_master WHERE name='orders_parent'").fetchone()
        db.close()

    async def test_opening_twice_is_idempotent(self, tmp_path: Path):
        path = str(tmp_path / "twice.db")
        write_v2_file(path)
        async with Journal(path):
            pass
        async with Journal(path) as journal:
            assert await journal.schema_version() == 3
            assert len(await journal.managed(states=("working",))) == 1


def order(**kw) -> Order:
    base = dict(id="K-9", venue="kalshi", market_id="kalshi:KX-A", side=Side.BUY, type=OrderType.LIMIT,
                time_in_force=TimeInForce.GTC, status=OrderStatus.OPEN, amount=Decimal("5"),
                account=Account(venue="kalshi"), book="alpha")
    return Order(**{**base, **kw})


class TestParentColumn:
    async def test_a_child_names_its_parent_in_the_column(self, tmp_path: Path):
        async with Journal(str(tmp_path / "p.db")) as journal:
            await journal.upsert_order(order(id="K-9", tags={"parent": "mo-7"}))
            await journal.upsert_order(order(id="P-3", venue="polymarket", market_id="polymarket:123",
                                             account=Account(venue="polymarket"), parent_id="mo-7"))
            await journal.upsert_order(order(id="K-1"))
            children = await journal.orders_for_parent("mo-7")
        assert sorted((c.venue, c.id) for c in children) == [("kalshi", "K-9"), ("polymarket", "P-3")]

    async def test_a_later_write_without_the_tag_keeps_the_parent(self, tmp_path: Path):
        async with Journal(str(tmp_path / "p.db")) as journal:
            await journal.upsert_order(order(tags={"parent": "mo-7"}))
            await journal.upsert_order(order(status=OrderStatus.CLOSED, filled=Decimal("5")))
            children = await journal.orders_for_parent("mo-7")
        assert [c.id for c in children] == ["K-9"]
        assert children[0].status == OrderStatus.CLOSED


class TestManagedAccount:
    async def test_a_parent_without_an_account_saves_with_null(self, tmp_path: Path):
        path = str(tmp_path / "m.db")
        async with Journal(path) as journal:
            await journal.save_managed({"id": "mo-b", "kind": "routed_limit", "state": "working",
                                        "request": {"market_id": "bucket:abc", "book": "alpha"}})
            assert [p["id"] for p in await journal.managed(states=("working",))] == ["mo-b"]
        db = sqlite3.connect(path)
        assert db.execute("SELECT account_key FROM managed_orders WHERE id='mo-b'").fetchone()[0] is None
        db.close()


class TestBuckets:
    async def test_round_trip_keeps_member_order_and_flip(self, tmp_path: Path):
        bucket = {"id": "b-1", "book": "alpha", "name": "Falcons -7.5 2H",
                  "members": [{"market_id": "kalshi:KX-A", "flip": False},
                              {"market_id": "polymarket:123", "flip": True}]}
        async with Journal(str(tmp_path / "b.db")) as journal:
            await journal.save_bucket(bucket)
            found = await journal.bucket("b-1")
            assert found["name"] == "Falcons -7.5 2H" and found["status"] == "active"
            assert found["members"] == bucket["members"]
            assert await journal.bucket("nope") is None

    async def test_saving_again_replaces_the_members(self, tmp_path: Path):
        async with Journal(str(tmp_path / "b.db")) as journal:
            await journal.save_bucket({"id": "b-1", "book": "alpha", "name": "x",
                                       "members": [{"market_id": "kalshi:KX-A"}, {"market_id": "polymarket:1"}]})
            await journal.save_bucket({"id": "b-1", "book": "alpha", "name": "x",
                                       "members": [{"market_id": "polymarket:2", "flip": True}]})
            found = await journal.bucket("b-1")
        assert found["members"] == [{"market_id": "polymarket:2", "flip": True}]

    async def test_listing_filters_by_book_and_archive_is_soft(self, tmp_path: Path):
        async with Journal(str(tmp_path / "b.db")) as journal:
            await journal.save_bucket({"id": "b-1", "book": "alpha", "name": "a", "members": []})
            await journal.save_bucket({"id": "b-2", "book": "beta", "name": "b", "members": []})
            assert [b["id"] for b in await journal.buckets(book="alpha")] == ["b-1"]
            assert await journal.archive_bucket("b-1") is True
            assert await journal.archive_bucket("b-1") is False
            assert [b["id"] for b in await journal.buckets()] == ["b-2"]
            assert [b["id"] for b in await journal.buckets(status=None)] == ["b-1", "b-2"]
            assert (await journal.bucket("b-1"))["status"] == "archived"
