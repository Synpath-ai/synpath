"""One explicitly authorized, bounded Kalshi stop-limit check.

Without --execute, this script only reads the market and fees. With it, the
only permitted create is one YES contract at 1 cent, post-only, expiring
within 90 seconds. Cleanup targets only that child's verified identity.
Credentials and the private SQLite journal are never printed or committed.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
import time
import uuid
from decimal import Decimal as D
from pathlib import Path

import httpx

from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.trading.credentials import load_credentials, read_dotenv
from synpath.trading.errors import OrderNotFound
from synpath.trading.kalshi import KalshiTrading
from synpath.trading.types import OrderRequest, OrderStatus, Side
from synpath.ws.base import LocalBook

TICKER = "KXFEDDECISION-27JAN-H0"
MARKET = f"kalshi:{TICKER}"
PRICE = D("0.01")


class OneOrderGuard:
    """Enforce the authorization on the actual HTTP payload, not a preview."""

    def __init__(self):
        self.enabled = False
        self.sent: dict | None = None
        self.cancel_ids: set[str] = set()
        self.created_at = time.time()

    async def check(self, request: httpx.Request) -> None:
        if request.url.host != "external-api.kalshi.com":
            raise ValueError("unexpected Kalshi host")
        if request.method == "GET":
            return
        prefix = "/trade-api/v2/portfolio/events/orders"
        if request.method == "DELETE" and request.url.path in {f"{prefix}/{oid}" for oid in self.cancel_ids}:
            return
        if request.method != "POST" or request.url.path != prefix or not self.enabled or self.sent is not None:
            raise ValueError("write exceeds the single-order authorization")
        body = json.loads(request.content)
        expiry = body.get("expiration_time", 0)
        if not (body.get("ticker") == TICKER and body.get("side") == "bid"
                and D(body.get("count", "0")) == 1 and D(body.get("price", "0")) == PRICE
                and body.get("post_only") is True and body.get("reduce_only") is False
                and body.get("time_in_force") == "good_till_canceled"
                and time.time() < expiry <= self.created_at + 90
                and not (set(body) - {"ticker", "client_order_id", "side", "count", "price", "time_in_force",
                                    "self_trade_prevention_type", "post_only", "reduce_only", "expiration_time"})):
            raise ValueError("unexpected size, price, flags or expiry")
        self.sent = body  # reserve before the network; never retry an uncertain send


def credentials_from(directory: Path):
    values = {k: v for k, v in read_dotenv(directory / ".env").items() if k.startswith("KALSHI_")}
    path = Path(values.get("KALSHI_PRIVATE_KEY_PATH", "")).expanduser()
    if not path.is_absolute():
        path = directory / path
    if not path.is_file():
        path = directory / "kalshi-prod.pem"
    values["KALSHI_PRIVATE_KEY_PATH"] = str(path)
    credentials = load_credentials(env=values, dotenv=directory / "__no_extra_credentials__")["kalshi"]
    if credentials is None or credentials.env != "prod":
        raise ValueError("this bounded check requires the authorized production Kalshi credentials")
    return credentials


async def read_book(adapter: KalshiTrading) -> LocalBook:
    raw = await adapter._call("GET", f"/markets/{TICKER}/orderbook", params={"depth": 5})
    levels = raw.get("orderbook_fp") or {}
    book = LocalBook()
    book.replace([(D(p), D(q)) for p, q in levels.get("yes_dollars", [])],
                 [(D("1") - D(p), D(q)) for p, q in levels.get("no_dollars", [])])
    if book.best_bid is None or book.best_ask is None or not PRICE < book.best_bid < book.best_ask < 1:
        raise ValueError("no suitable uncrossed book above the one-cent test limit")
    return book


async def preflight(adapter: KalshiTrading) -> dict:
    market = (await adapter._call("GET", f"/markets/{TICKER}"))["market"]
    if market.get("status") != "active" or market.get("price_level_structure") != "linear_cent":
        raise ValueError("market is not active on the expected cent grid")
    fee = await adapter.fetch_fee_estimate(MARKET, Side.BUY, PRICE, D("1"))
    if fee.maker_fee is None or fee.maker_fee > PRICE:
        raise ValueError("fee estimate exceeds the small test budget")
    book = await read_book(adapter)
    return {"market_id": MARKET, "quantity": "1", "limit_price": str(PRICE),
            "estimated_maximum_cost_usd": str(PRICE + fee.maker_fee),
            "observed_bid": str(book.best_bid), "observed_ask": str(book.best_ask)}


def new_engine(adapter: KalshiTrading, path: Path) -> Engine:
    return Engine({"kalshi": adapter}, EngineConfig(journal_path=str(path), user_id="kalshi-stop-verification"),
                  risk=RiskConfig(price_collar=None, max_order_contracts=D("1"), max_order_notional=PRICE,
                                  closing_soon_s=None, duplicate_window_ms=0))


async def read_child(adapter: KalshiTrading, order_id: str):
    """Wait for the read projection without repeating the accepted create."""
    for attempt in range(12):
        try:
            return await adapter.fetch_order(order_id, attempts=1)
        except OrderNotFound:
            if attempt == 11:
                raise
            await asyncio.sleep(0.5)


async def cleanup(adapter: KalshiTrading, guard: OneOrderGuard) -> dict:
    """Find even a response-lost child, cancel it, and read the venue back."""
    if guard.sent is None:
        return {"child_created": False, "cleanup_verified": True, "filled": "0"}
    for attempt in range(12):
        orders = []
        cursor = None
        while True:
            page = await adapter.fetch_orders(market_id=MARKET, cursor=cursor, limit=200)
            orders.extend(o for o in page if o.client_order_id == guard.sent["client_order_id"])
            cursor = page.next_cursor
            if not cursor:
                break
        if len(orders) > 1:
            raise RuntimeError("multiple children share the verification key")
        if orders:
            order = orders[0]
            guard.cancel_ids.add(order.id)
            if not order.is_terminal:
                await adapter.cancel_order(order.id, market_id=MARKET)
            final = await read_child(adapter, order.id)
            if final.is_terminal and (final.remaining or D("0")) == 0:
                return {"child_created": True, "venue_status": final.status.value,
                        "filled": str(final.filled), "cleanup_verified": True}
            # Canceled reads may report canceled quantity as remaining.
            if final.status == OrderStatus.CANCELED:
                return {"child_created": True, "venue_status": final.status.value,
                        "filled": str(final.filled), "cleanup_verified": True}
        await asyncio.sleep(0.5)
    raise RuntimeError("child cleanup not confirmed; inspect the retained journal and client key")


async def verify(adapter: KalshiTrading, guard: OneOrderGuard, directory: Path) -> dict:
    report = await preflight(adapter)
    path = directory / "orders.db"
    engine = new_engine(adapter, path)
    parent_id = None
    request = OrderRequest(market_id=MARKET, side="buy", quantity="1", type="stop_limit",
                           stop_price=PRICE, price=PRICE, post_only=True, time_in_force="gtd",
                           expires_at=int((guard.created_at + 85) * 1000),
                           client_order_id=f"verify-{uuid.uuid4().hex}", book="bounded-stop-check")
    try:
        await engine.start()
        parent = await engine.submit(request)
        parent_id = parent.id
        if parent.status != OrderStatus.WAITING or guard.sent is not None:
            raise RuntimeError("arming sent an order or failed to remain waiting")
        await engine.stop()
        engine = new_engine(adapter, path)
        await engine.start()
        replay = await engine.submit(request)
        if replay.id != parent_id or replay.status != OrderStatus.WAITING or guard.sent is not None:
            raise RuntimeError("waiting restart did not restore exactly one parent")
        report["waiting_restart_verified"] = True
        engine.set_book(MARKET, await read_book(adapter))
        guard.enabled = True
        try:
            # The actual ask is above the deliberately low trigger. No fake
            # price is injected; this intentionally triggers on first observation.
            await engine.on_book({"market_id": MARKET})
        finally:
            guard.enabled = False
        managed = engine.orders.get(parent_id)
        if managed.state != "working" or len(managed.children) != 1 or guard.sent is None:
            raise RuntimeError("trigger did not create exactly one child")
        child = managed.children[0]
        guard.cancel_ids.add(child.order_id)
        live = await read_child(adapter, child.order_id)
        if live.client_order_id != guard.sent["client_order_id"] or live.quantity != 1 or live.price != PRICE:
            raise RuntimeError("venue read-back differs from the guarded request")
        report["child_venue_status"] = live.status.value
        report["trigger_and_child_verified"] = True
        await engine.cancel(parent_id)
    finally:
        guard.enabled = False
        try:
            managed = engine.orders.get(parent_id) if parent_id else None
            if managed is not None and managed.live and managed.children:
                guard.cancel_ids.update(c.order_id for c in managed.children)
                try:
                    await engine.cancel(parent_id)
                except Exception as error:
                    # History-based cleanup still runs if the engine cannot
                    # complete its cancellation. Never print credential data.
                    report["engine_cleanup_error"] = type(error).__name__
            report.update(await cleanup(adapter, guard))
        finally:
            await engine.stop()
            (directory / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    engine = new_engine(adapter, path)
    try:
        await engine.start()
        replay = await engine.submit(request)
        if replay.id != parent_id or not replay.is_terminal:
            raise RuntimeError("terminal restart did not retain the completed parent")
        report["terminal_restart_verified"] = True
        report["parent_status"] = replay.status.value
    finally:
        await engine.stop()
        (directory / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credentials-dir", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    guard = OneOrderGuard()
    directory = Path(tempfile.mkdtemp(prefix="synpath-kalshi-stop-"))
    os.chmod(directory, 0o700)
    async with httpx.AsyncClient(timeout=10, follow_redirects=False, event_hooks={"request": [guard.check]}) as client:
        async with KalshiTrading(credentials_from(args.credentials_dir), client=client) as adapter:
            adapter.http.attempts = 1
            report = await verify(adapter, guard, directory) if args.execute else await preflight(adapter)
    print(json.dumps(report, indent=2))
    print(f"Private verification records: {directory}")


if __name__ == "__main__":
    asyncio.run(main())
