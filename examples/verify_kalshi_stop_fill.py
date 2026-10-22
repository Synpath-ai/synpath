"""Authorized one-contract stop-limit fill test on a 15-minute crypto market.

Call only with explicit live-trading authorization. The HTTP guard permits
one bounded entry and one Sell YES exit of exactly that entry's filled quantity.
No cancel-all, retries of creates, or use of existing positions is allowed.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
import time
import uuid
from datetime import datetime
from decimal import Decimal as D, ROUND_CEILING
from pathlib import Path

import httpx

from examples.verify_kalshi_stop_limit import credentials_from, read_child
from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.trading.kalshi import KalshiTrading
from synpath.trading.types import OrderRequest, Precision, Side
from synpath.ws.base import LocalBook

CENT = D("0.01")
MAX_LIMIT = D("0.75")
EXIT_LIMIT = CENT


class FillGuard:
    def __init__(self, ticker, limit):
        self.ticker, self.limit = ticker, limit
        self.stage = "blocked"
        self.sent = {}
        self.rejected = {}
        self.exit_quantity = D("0")
        self.cancel_ids = set()

    async def check(self, request):
        if request.url.host != "external-api.kalshi.com":
            raise ValueError("unexpected host")
        if request.method == "GET":
            return
        prefix = "/trade-api/v2/portfolio/events/orders"
        if request.method == "DELETE" and request.url.path in {f"{prefix}/{i}" for i in self.cancel_ids}:
            return
        if request.method != "POST" or request.url.path != prefix or self.stage not in ("entry", "exit") or self.stage in self.sent:
            raise ValueError("write exceeds authorized entry/exit")
        body = json.loads(request.content)
        entry = self.stage == "entry"
        quantity = D("1") if entry else self.exit_quantity
        price = self.limit if entry else EXIT_LIMIT
        # The entry is the stop-limit's child: it lives as long as the stop
        # (gtd at the check's deadline), and is cancelled the moment it is
        # read back. The exit is an immediate market order.
        allowed = {"ticker", "client_order_id", "side", "count", "price", "time_in_force", "self_trade_prevention_type",
                   "post_only", "expiration_time"}
        bounded = (body.get("time_in_force") == "good_till_canceled" and bool(body.get("expiration_time"))
                   if entry else body.get("time_in_force") == "immediate_or_cancel" and "expiration_time" not in body)
        if not (body.get("ticker") == self.ticker and body.get("side") == ("bid" if entry else "ask")
                and D(body.get("count", "0")) == quantity and 0 < quantity <= 1
                and D(body.get("price", "0")) == price and (not entry or price <= MAX_LIMIT)
                and bounded
                and "reduce_only" not in body and body.get("post_only") is False
                and not (set(body) - allowed)):
            raise ValueError("unexpected order size, price or flags")
        self.sent[self.stage] = body


    async def observe(self, response):
        # A definitive refusal is not an ambiguous accepted order. Never
        # retry it or wait for it to appear in venue history.
        if response.request.method != "POST" or response.status_code not in (400, 401, 403, 422):
            return
        for stage, body in self.sent.items():
            request_body = json.loads(response.request.content)
            if body["client_order_id"] == request_body.get("client_order_id"):
                self.rejected[stage] = f"HTTP {response.status_code}"


async def book_for(adapter, ticker):
    raw = await adapter._call("GET", f"/markets/{ticker}/orderbook", params={"depth": 5})
    rows = raw.get("orderbook_fp") or {}
    book = LocalBook()
    book.replace([(D(p), D(q)) for p, q in rows.get("yes_dollars", [])],
                 [(1 - D(p), D(q)) for p, q in rows.get("no_dollars", [])])
    return book


async def choose_market(adapter):
    for series in ("KXBTC15M", "KXETH15M", "KXSOL15M"):
        raw = await adapter._call("GET", "/markets", params={"series_ticker": series, "status": "open", "limit": 10})
        for market in raw.get("markets", []):
            closes = datetime.fromisoformat(market["close_time"].replace("Z", "+00:00")).timestamp()
            if market.get("status") != "active" or not 300 < closes - time.time() < 1200:
                continue
            ticker = market["ticker"]
            book = await book_for(adapter, ticker)
            if (book.best_bid is None or book.best_ask is None or not D("0.10") <= book.best_bid < book.best_ask <= D("0.65")):
                continue
            if await adapter.fetch_positions(market_id=f"kalshi:{ticker}"):
                continue
            if await adapter.fetch_open_orders(market_id=f"kalshi:{ticker}"):
                continue
            trigger = book.best_ask.quantize(CENT, rounding=ROUND_CEILING) + CENT
            limit = trigger + D("0.03")
            fee = await adapter.fetch_fee_estimate(f"kalshi:{ticker}", Side.BUY, D("0.50"), D("1"))
            if fee.taker_fee is None or fee.taker_fee > D("0.02") or limit > MAX_LIMIT:
                continue
            shard = int(market.get("exchange_index") or 0)
            balance = await adapter._call("GET", "/portfolio/balance", params={"exchange_index": shard})
            available = D(str(balance["balance_dollars"])) if balance.get("balance_dollars") is not None else D(str(balance.get("balance", 0))) / 100
            if available < limit + D("0.04"):
                continue
            return market, book, trigger, limit
    raise RuntimeError("no suitable funded crypto market with time, liquidity and an empty starting position")


def engine_for(adapter, path):
    return Engine({"kalshi": adapter}, EngineConfig(journal_path=str(path), user_id="kalshi-fill-verification"),
                  risk=RiskConfig(price_collar=None, max_order_contracts=D("1"), max_order_notional=MAX_LIMIT,
                                  closing_soon_s=60, duplicate_window_ms=0))


async def find_sent(adapter, guard, stage):
    body = guard.sent.get(stage)
    if not body or stage in guard.rejected:
        return None
    for _ in range(12):
        cursor = None
        while True:
            page = await adapter.fetch_orders(market_id=f"kalshi:{guard.ticker}", limit=200, cursor=cursor)
            matching = [o for o in page if o.client_order_id == body["client_order_id"]]
            if matching:
                return matching[0]
            cursor = page.next_cursor
            if not cursor:
                break
        await asyncio.sleep(0.5)
    raise RuntimeError("accepted status unresolved; keep the journal for reconciliation")


async def cancel_remainder(adapter, guard, stage):
    order = await find_sent(adapter, guard, stage)
    if order is None:
        return None
    guard.cancel_ids.add(order.id)
    if not order.is_terminal:
        await adapter.cancel_order(order.id, market_id=order.market_id)
    final = await read_child(adapter, order.id)
    if not final.is_terminal:
        raise RuntimeError("test order still open after cleanup")
    return final


async def close_entry(engine, adapter, guard, entry):
    if entry.filled <= 0:
        return None
    # Only the quantity confirmed on the test entry is allowed to close.
    guard.exit_quantity = entry.filled
    guard.stage = "exit"
    try:
        await engine.submit(OrderRequest(market_id=entry.market_id, outcome="yes", action="sell", quantity=entry.filled,
                                         type="market", price=EXIT_LIMIT, time_in_force="ioc",
                                         client_order_id=f"close-{uuid.uuid4().hex}", book="bounded-fill-check"),
                            precision=Precision(tick=CENT, min_quantity=D("0.01"), quantity_step=D("0.01")))
    finally:
        guard.stage = "blocked"
    return await cancel_remainder(adapter, guard, "exit")


async def verify(adapter, guard, directory, market, initial_book, trigger, limit, *, wait_s=240, poll_s=0.5):
    market_id = f"kalshi:{market['ticker']}"
    report = {"market_id": market_id, "title": market.get("title"), "quantity": "1", "initial_bid": str(initial_book.best_bid),
              "initial_ask": str(initial_book.best_ask), "stop_price": str(trigger), "limit_price": str(limit),
              "maximum_entry_value_usd": str(limit), "estimated_fee_budget_usd": "0.04"}
    engine = engine_for(adapter, directory / "orders.db")
    request = OrderRequest(market_id=market_id, side="buy", quantity="1", type="stop_limit", stop_price=trigger,
                           price=limit, time_in_force="gtd", expires_at=int((time.time() + wait_s) * 1000),
                           client_order_id=f"stop-fill-{uuid.uuid4().hex}", book="bounded-fill-check")
    parent_id = None
    try:
        await engine.start()
        engine.set_book(market_id, initial_book)
        parent = await engine.submit(request)
        parent_id = parent.id
        await engine.on_book({"market_id": market_id})
        if engine.managed_order_controller.get(parent_id).state != "waiting" or guard.sent:
            raise RuntimeError("stop was not below its trigger when armed")
        await engine.stop()
        engine = engine_for(adapter, directory / "orders.db")
        await engine.start()
        assert (await engine.submit(request)).id == parent_id
        engine._tasks.append(asyncio.create_task(engine._lease_loop()))
        report["waiting_restart_verified"] = True
        guard.stage = "entry"
        deadline = time.monotonic() + wait_s
        while engine.managed_order_controller.get(parent_id).state == "waiting" and time.monotonic() < deadline:
            book = await book_for(adapter, guard.ticker)
            engine.set_book(market_id, book)
            await engine.on_book({"market_id": market_id})
            await engine.managed_order_controller.on_timer()
            parent = engine.managed_order_controller.get(parent_id)
            if parent.state != "waiting":
                report["observed_trigger_ask"] = str(book.best_ask)
                break
            await asyncio.sleep(poll_s)
        guard.stage = "blocked"
        entry = await cancel_remainder(adapter, guard, "entry")
        if entry is None:
            await engine.cancel(parent_id)
            report["outcome"] = ("venue rejected the entry; no fill" if "entry" in guard.rejected
                                 else "no threshold crossing; no venue order sent")
            report["venue_refusal"] = guard.rejected.get("entry")
            return report
        await engine.on_order(entry)
        report.update(entry_status=entry.status.value, entry_filled=str(entry.filled), entry_average_price=str(entry.average_price),
                      entry_cost=str(entry.cost), entry_fee=str(entry.fee), parent_status=engine.managed_order_controller.get(parent_id).as_order().status.value)
        fills = await adapter.fetch_my_trades(market_id=market_id, order_id=entry.id)
        report["entry_fill_records"] = [{"quantity": str(f.quantity), "price": str(f.price), "fee": str(f.fee)} for f in fills]
        if entry.filled <= 0:
            report["outcome"] = "triggered but no fill before the entry was cancelled"
            return report
        exit_order = await close_entry(engine, adapter, guard, entry)
        if exit_order:
            await engine.on_order(exit_order)
            report.update(exit_status=exit_order.status.value, exit_filled=str(exit_order.filled),
                          exit_average_price=str(exit_order.average_price), exit_cost=str(exit_order.cost), exit_fee=str(exit_order.fee))
        for _ in range(12):
            positions = await adapter.fetch_positions(market_id=market_id)
            if not positions:
                break
            await asyncio.sleep(0.5)
        report["remaining_position_contracts"] = str(sum((p.contracts for p in positions), D("0")))
        report["outcome"] = "filled and closed" if not positions else "filled; residual position held until market settlement"
        await engine.stop()
        engine = engine_for(adapter, directory / "orders.db")
        await engine.start()
        replay = await engine.submit(request)
        report["terminal_restart_verified"] = replay.id == parent_id and replay.is_terminal
        return report
    finally:
        guard.stage = "blocked"
        try:
            if parent_id and engine.managed_order_controller.get(parent_id) and engine.managed_order_controller.get(parent_id).state == "waiting":
                await engine.cancel(parent_id)
            # The exit is IOC and already terminal; the entry may rest. Read back and cancel only
            # these keys even after a failed/ambiguous response. Never resend.
            for stage in ("entry", "exit"):
                final = await cancel_remainder(adapter, guard, stage)
                if final:
                    report[f"final_{stage}_status"] = final.status.value
                    report[f"final_{stage}_filled"] = str(final.filled)
            report["creates_attempted"] = len(guard.sent)
            report["cleanup_verified"] = True
        finally:
            await engine.stop()
            (directory / "result.json").write_text(json.dumps(report, indent=2) + "\n")


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credentials-dir", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    directory = Path(tempfile.mkdtemp(prefix="synpath-kalshi-fill-"))
    os.chmod(directory, 0o700)
    async with httpx.AsyncClient(timeout=10, follow_redirects=False) as client:
        async with KalshiTrading(credentials_from(args.credentials_dir), client=client) as adapter:
            adapter.http.attempts = 1
            market, book, trigger, limit = await choose_market(adapter)
            guard = FillGuard(market["ticker"], limit)
            client.event_hooks["request"].append(guard.check)
            client.event_hooks["response"].append(guard.observe)
            print(json.dumps({"selected_market": market["ticker"], "title": market.get("title"), "current_ask": str(book.best_ask),
                              "stop_price": str(trigger), "limit_price": str(limit), "quantity": "1"}), flush=True)
            if args.execute:
                report = await verify(adapter, guard, directory, market, book, trigger, limit)
                print(json.dumps(report, indent=2))
    print(f"Private records: {directory}")


if __name__ == "__main__":
    asyncio.run(main())
