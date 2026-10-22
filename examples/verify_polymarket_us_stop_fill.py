"""Explicitly authorized fractional stop-market test using the retail API.

One entry of 0.01 contract, then native close-position only after an empty
starting position and a matching filled child are verified. All other writes
are blocked. Without --execute this only checks funds, public books and preview.
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
from synpath.trading.credentials import read_dotenv, load_credentials
from synpath.trading.errors import OrderNotFound
from synpath.trading.instruments import MarketSpec
from synpath.trading.polymarket_us import PolymarketUSTrading
from synpath.trading.types import OrderRequest, Side
from synpath.ws.base import LocalBook

QUANTITY = D("0.01")
BOUND = D("0.99")


class Guard:
    def __init__(self, slug, directory):
        self.slug, self.directory = slug, directory
        self.stage = "blocked"
        self.sent = {}
        self.ids = {}
        self.cancel_ids = set()

    def entry_ok(self, body):
        return (body.get("marketSlug") == self.slug and D(str(body.get("quantity", 0))) == QUANTITY
                and D(str((body.get("price") or {}).get("value", 0))) == BOUND
                and (body.get("price") or {}).get("currency") == "USD"
                and body.get("type") == "ORDER_TYPE_LIMIT" and body.get("outcomeSide") == "OUTCOME_SIDE_YES"
                and body.get("action") == "ORDER_ACTION_BUY" and body.get("tif") == "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL"
                and not (set(body) - {"marketSlug", "type", "price", "quantity", "tif", "outcomeSide", "action", "manualOrderIndicator"}))

    async def check(self, request):
        if request.url.host not in ("api.polymarket.us", "gateway.polymarket.us"):
            raise ValueError("unexpected host")
        if request.method == "GET":
            return
        body = json.loads(request.content)
        path = request.url.path
        if request.method == "POST" and path == "/v1/order/preview" and self.entry_ok(body.get("request") or {}):
            return
        if request.method == "POST" and path in {f"/v1/order/{oid}/cancel" for oid in self.cancel_ids} and body.get("marketSlug") == self.slug:
            return
        if self.stage in self.sent or request.method != "POST":
            raise ValueError("write exceeds authorization")
        if self.stage == "entry" and path == "/v1/orders" and self.entry_ok(body):
            self.sent["entry"] = body
        elif self.stage == "close" and path == "/v1/order/close-position" and body == {
                "marketSlug": self.slug, "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC"}:
            self.sent["close"] = body
        else:
            raise ValueError("unexpected order or cleanup")

    async def observe(self, response):
        if response.request.method != "POST" or response.status_code >= 400:
            return
        stage = {"/v1/orders": "entry", "/v1/order/close-position": "close"}.get(response.request.url.path)
        if stage:
            await response.aread()
            raw = response.json()
            oid = raw.get("id") or (((raw.get("executions") or [{}])[-1].get("order") or {}).get("id"))
            if oid:
                self.ids[stage] = oid
                # Retain the response identity separately for targeted cleanup.
                with (self.directory / "receipts.json").open("w") as stream:
                    json.dump(self.ids, stream)
                    stream.flush()
                    os.fsync(stream.fileno())


def credentials_from(directory):
    names = {"POLYMARKET_US_KEY_ID", "POLYMARKET_US_SECRET_KEY"}
    values = {k: v for k, v in read_dotenv(directory / ".env").items() if k in names}
    creds = load_credentials(env=values, dotenv=directory / "__no_extra_credentials__")["polymarket_us"]
    if creds is None:
        raise ValueError("Polymarket US credentials missing")
    return creds


async def positions_for(adapter, market_id):
    return [p for p in await adapter.fetch_positions(market_id=market_id) if p.market_id == market_id]


async def book_for(adapter, slug):
    raw = await adapter.gateway.get(f"/v1/markets/{slug}/book")
    raw = raw.get("marketData") or raw
    def levels(rows):
        return [(D(str(row["px"]["value"])), D(str(row["qty"]))) for row in rows or [] if D(str(row["qty"])) > 0]
    book = LocalBook()
    book.replace(levels(raw.get("bids")), levels(raw.get("offers")))
    return book


async def choose_market(adapter):
    balance = await adapter.fetch_balance()
    if balance.available is None or balance.available < D("0.05"):
        raise RuntimeError("insufficient funds for the tiny test budget")
    raw = await adapter.gateway.get("/v1/search", {"query": "up or down", "limit": 10, "page": 1})
    for event in raw.get("events", []):
        if "15m" not in event.get("slug", "") or not event.get("live") or event.get("ended"):
            continue
        for market in event.get("markets", []):
            if not market.get("active") or market.get("closed"):
                continue
            slug = market["slug"]
            rules = await adapter.market_rules(slug, refresh=True)
            if rules.min_quantity > QUANTITY or rules.tick != D("0.01"):
                continue
            book = await book_for(adapter, slug)
            if book.best_bid is None or book.best_ask is None or not D("0.01") <= book.best_bid < book.best_ask < D("0.95"):
                continue
            market_id = f"polymarket_us:{slug}"
            if await positions_for(adapter, market_id) or await adapter.fetch_open_orders(market_id=market_id):
                continue
            fee = await adapter.fetch_fee_estimate(market_id, Side.BUY, D("0.50"), QUANTITY)
            if fee.taker_fee is None or fee.taker_fee > D("0.01"):
                continue
            return slug, rules, book, book.best_ask + rules.tick
    raise RuntimeError("no suitable liquid live BTC 15-minute market, initially flat")


def engine_for(adapter, directory):
    return Engine({"polymarket_us": adapter}, EngineConfig(journal_path=str(directory / "orders.db"), user_id="us-stop-verification"),
                  risk=RiskConfig(price_collar=None, max_order_contracts=QUANTITY, max_order_notional=QUANTITY,
                                  duplicate_window_ms=0, closing_soon_s=None))


async def terminal_order(adapter, oid, guard):
    for _ in range(40):
        try:
            order = await adapter.fetch_order(oid)
            if order.is_terminal:
                return order
        except OrderNotFound:
            pass
        await asyncio.sleep(0.25)
    guard.cancel_ids.add(oid)
    await adapter.cancel_order(oid, market_id=f"polymarket_us:{guard.slug}")
    for _ in range(40):
        order = await adapter.fetch_order(oid)
        if order.is_terminal:
            return order
        await asyncio.sleep(0.25)
    raise RuntimeError("test order still unresolved after targeted cancellation")


async def verify(adapter, guard, directory, rules, initial, trigger, *, wait_s=240, poll_s=0.5, initially_eligible=False):
    market_id = f"polymarket_us:{guard.slug}"
    report = {"market_id": market_id, "quantity": str(QUANTITY), "initial_bid": str(initial.best_bid),
              "initial_ask": str(initial.best_ask), "stop_price": str(trigger), "child_price_bound": str(BOUND),
              "maximum_entry_value_usd": str(BOUND * QUANTITY),
              "trigger_mode": "initially eligible" if initially_eligible else "future crossing"}
    request = OrderRequest(market_id=market_id, side="buy", quantity=QUANTITY, type="stop_market", stop_price=trigger,
                           time_in_force="gtd", expires_at=int((time.time() + wait_s) * 1000), client_order_id=f"us-stop-{uuid.uuid4().hex}")
    engine = engine_for(adapter, directory)
    parent_id = None
    try:
        await engine.start()
        # This finite host drives market observations/timers itself, but must
        # still renew the execution lease throughout the price wait.
        engine._tasks.append(asyncio.create_task(engine._lease_loop()))
        engine.market_master.put([MarketSpec(market_id=market_id, venue="polymarket_us", precision=rules.precision,
                                            tradable=True, status="open")])
        parent = await engine.submit(request)
        parent_id = parent.id
        engine.set_book(market_id, initial)
        # An execution check may arm an already eligible stop. It still
        # persists waiting before the next actual venue book observation.
        if not initially_eligible:
            await engine.on_book({"market_id": market_id})
        assert engine.managed_order_controller.get(parent_id).state == "waiting" and not guard.sent
        guard.stage = "entry"
        deadline = time.monotonic() + wait_s
        while engine.managed_order_controller.get(parent_id).state == "waiting" and time.monotonic() < deadline:
            book = await book_for(adapter, guard.slug)
            engine.set_book(market_id, book)
            await engine.on_book({"market_id": market_id})
            await engine.managed_order_controller.on_timer()
            if engine.managed_order_controller.get(parent_id).state != "waiting":
                report["observed_trigger_ask"] = str(book.best_ask)
                break
            await asyncio.sleep(poll_s)
        guard.stage = "blocked"
        oid = guard.ids.get("entry")
        if not oid:
            if "entry" in guard.sent:
                raise RuntimeError("create response unresolved; do not resend or infer acceptance")
            await engine.cancel(parent_id)
            managed = engine.managed_order_controller.get(parent_id)
            report["outcome"] = ("triggered but local dispatch failed" if managed.state == "rejected"
                                 else "no threshold crossing; no venue order")
            report["parent_status"] = managed.as_order().status.value
            report["detail"] = managed.detail
            return report
        entry = await terminal_order(adapter, oid, guard)
        await engine.on_order(entry)
        report.update(entry_status=entry.status.value, entry_filled=str(entry.filled), entry_average_price=str(entry.average_price),
                      entry_fee=str(entry.fee), parent_status=engine.managed_order_controller.get(parent_id).as_order().status.value)
        if entry.filled <= 0:
            report["outcome"] = "triggered without a fill"
            return report
        positions = []
        for _ in range(40):
            positions = await positions_for(adapter, market_id)
            if positions:
                break
            await asyncio.sleep(0.25)
        owned = sum((p.contracts for p in positions), D("0"))
        if owned != entry.filled or owned > QUANTITY:
            raise RuntimeError("position does not equal the tiny test fill; do not close unrelated inventory")
        guard.stage = "close"
        try:
            await adapter.close_position(market_id)
        finally:
            guard.stage = "blocked"
        close_id = guard.ids.get("close")
        if not close_id:
            raise RuntimeError("close response unresolved; do not repeat it")
        closed = await terminal_order(adapter, close_id, guard)
        await engine.on_order(closed)
        report.update(exit_status=closed.status.value, exit_filled=str(closed.filled), exit_average_price=str(closed.average_price), exit_fee=str(closed.fee))
        for _ in range(40):
            positions = await positions_for(adapter, market_id)
            if not positions:
                break
            await asyncio.sleep(0.25)
        report["remaining_position_contracts"] = str(sum((p.contracts for p in positions), D("0")))
        report["outcome"] = "filled and closed" if not positions else "filled; close incomplete"
        return report
    finally:
        guard.stage = "blocked"
        try:
            if parent_id and engine.managed_order_controller.get(parent_id) and engine.managed_order_controller.get(parent_id).state == "waiting":
                await engine.cancel(parent_id)
            for stage, oid in guard.ids.items():
                order = await terminal_order(adapter, oid, guard)
                report[f"final_{stage}_status"] = order.status.value
            report["creates_attempted"] = len(guard.sent)
        finally:
            await engine.stop()
            (directory / "result.json").write_text(json.dumps(report, indent=2) + "\n")


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credentials-dir", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    directory = Path(tempfile.mkdtemp(prefix="synpath-us-stop-"))
    os.chmod(directory, 0o700)
    async with httpx.AsyncClient(timeout=10, follow_redirects=False) as client:
        async with PolymarketUSTrading(credentials_from(args.credentials_dir), client=client) as adapter:
            adapter.api.attempts = 1
            slug, rules, initial, trigger = await choose_market(adapter)
            guard = Guard(slug, directory)
            client.event_hooks["request"].append(guard.check)
            client.event_hooks["response"].append(guard.observe)
            preview = await adapter.preview_order(OrderRequest(market_id=f"polymarket_us:{slug}", side="buy", quantity=QUANTITY,
                                                               type="market", price=BOUND, time_in_force="ioc"))
            print(json.dumps({"selected_market": slug, "quantity": str(QUANTITY), "initial_ask": str(initial.best_ask),
                              "stop_price": str(trigger), "maximum_entry_value_usd": str(QUANTITY * BOUND),
                              "preview_status": preview.status.value}), flush=True)
            if args.execute:
                print(json.dumps(await verify(adapter, guard, directory, rules, initial, trigger), indent=2))
    print(f"Private records: {directory}")


if __name__ == "__main__":
    asyncio.run(main())
