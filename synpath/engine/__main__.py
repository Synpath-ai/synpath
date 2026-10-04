"""`python -m synpath.engine`: run the engine as a daemon, or ask it what it knows.

```bash
python -m synpath.engine run --config engine.toml      # trade
python -m synpath.engine status --journal trading.db   # what the journal says
python -m synpath.engine halt --journal trading.db --reason "manual"
python -m synpath.engine resume --journal trading.db
python -m synpath.engine eod --config engine.toml      # close the day and print the report
```

Three things make this a daemon rather than a script:

**One writer.** Starting takes the journal's lease. A second process on the
same journal exits with a message naming the one that holds it, rather than
quietly trading the same account twice.

**Signals stop it properly.** `SIGINT` and `SIGTERM` apply the configured
halt policy first (by default, cancel everything resting through each
venue's own cancel-all), then release the lease and close the journal. Kill
it with `SIGKILL` and the journal still has every intent, which is what the
recovery path is for.

**Halting does not need this process.** `halt` writes the request into the
journal; the running engine picks it up within a second and applies it. An
operator who cannot reach the process can still stop it from the same
machine, and the request is recorded with who asked and when.

The configuration is TOML (or JSON), and names venues, the journal, and the
risk rules. Credentials never appear in it: they come from the environment
or a `.env`, the same way the adapters read them.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
from decimal import Decimal
from typing import Any

from ..trading.credentials import load_credentials
from ..trading.errors import CredentialsMissing
from .engine import Engine, EngineConfig
from .feeds import Feeds, default_streams
from .eod import EndOfDay
from .journal import Journal, LeaseLost, now_ms
from .reconcile import Reconciler
from .risk import RiskConfig

log = logging.getLogger("synpath.engine")

HALT_REQUEST = "control:halt"
RESUME_REQUEST = "control:resume"

ADAPTERS = {
    "kalshi": ("..trading.kalshi", "KalshiTrading"),
    "polymarket": ("..trading.polymarket", "PolymarketTrading"),
    "polymarket_us": ("..trading.polymarket_us", "PolymarketUSTrading"),
    "polymarket_us_exchange": ("..trading.polymarket_us_exchange", "PolymarketUSExchangeTrading"),
    "opinion": ("..trading.opinion", "OpinionTrading"),
    "hyperliquid": ("..trading.hyperliquid", "HyperliquidTrading"),
}


def load_config(path: str) -> dict[str, Any]:
    text = open(path, "rb").read()
    if path.endswith(".json"):
        return json.loads(text)
    try:
        import tomllib
    except ImportError:  # pragma: no cover - Python 3.10, where the same parser is the tomli package
        import tomli as tomllib
    return tomllib.loads(text.decode())


def build_adapters(config: dict[str, Any], *, dotenv: str | None = None) -> dict[str, Any]:
    """One adapter per venue named in the configuration, from env credentials."""
    import importlib

    wanted = [name for name, section in (config.get("venues") or {}).items() if section.get("enabled", True)]
    if not wanted:
        raise SystemExit("the configuration names no venues: add [venues.kalshi] or another")
    credentials = load_credentials(dotenv=dotenv)
    adapters: dict[str, Any] = {}
    for name in wanted:
        if name == "paper":
            from .paper import PaperVenue, quadratic_fee

            section = config["venues"][name]
            adapters[name] = PaperVenue(
                venue=section.get("mirrors", "paper"), cash=Decimal(str(section.get("cash", "10000"))),
                fees=quadratic_fee(Decimal(str(section.get("fee_rate", "0.07")))),
            )
            continue
        if name not in ADAPTERS:
            raise SystemExit(f"unknown venue {name!r}; known: {sorted(ADAPTERS) + ['paper']}")
        creds = credentials.get(name)
        if creds is None:
            raise SystemExit(f"{name} is enabled but its credentials are not configured; run `python -m synpath.trading doctor`")
        module_name, class_name = ADAPTERS[name]
        module = importlib.import_module(module_name, package=__package__)
        adapters[name] = getattr(module, class_name)(creds)
    return adapters


def risk_from(config: dict[str, Any]) -> RiskConfig:
    section = dict(config.get("risk") or {})
    for key in ("price_collar", "max_order_contracts", "max_order_notional", "max_position_contracts",
                "max_event_contracts", "max_venue_notional", "daily_loss_limit", "min_price", "max_price"):
        if key in section and section[key] is not None:
            section[key] = Decimal(str(section[key]))
    if "exchange_limits" in section:
        section["exchange_limits"] = {k: Decimal(str(v)) for k, v in section["exchange_limits"].items()}
    return RiskConfig(**section)


async def run(args: argparse.Namespace) -> int:
    config = load_config(args.config) if args.config else {}
    logging.basicConfig(level=getattr(logging, (args.log_level or "INFO").upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s")
    engine_config = EngineConfig(
        journal_path=args.journal or config.get("journal", "synpath.db"),
        poll_interval_s=float(config.get("poll_interval_s", 5)),
        reconcile_interval_s=float(config.get("reconcile_interval_s", 60)),
        sweep_interval_s=float(config.get("sweep_interval_s", 10)),
        in_doubt_timeout_s=float(config.get("in_doubt_timeout_s", 20)),
        halt_policy=config.get("halt_policy", "cancel"),
    )
    adapters = build_adapters(config, dotenv=args.dotenv)
    engine = Engine(adapters, engine_config, risk=risk_from(config))
    feeds = (Feeds(engine, default_streams(adapters, load_credentials(dotenv=args.dotenv)))
             if config.get("streams", True) else None)
    reconciler = Reconciler(engine, orphan_policy=config.get("orphan_policy", "report"))
    eod = EndOfDay(engine, hour_utc=int(config.get("eod_hour_utc", 0)))

    try:
        recovery = await engine.start()
    except LeaseLost as exc:
        print(str(exc), file=sys.stderr)
        return 3
    print(f"engine {engine.journal.owner} started on {engine_config.journal_path}: "
          f"{recovery.orders_open} open orders, {recovery.fills_replayed} fills replayed, "
          f"{recovery.in_doubt} in doubt ({recovery.adopted} adopted, {recovery.swept} swept, "
          f"{recovery.unresolved} unresolved)")

    if feeds is not None:
        await feeds.start()

    stopping = asyncio.Event()

    def _signal(name: str) -> None:
        log.warning("synpath.engine: %s received, halting", name)
        stopping.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal, sig.name)
        except NotImplementedError:  # pragma: no cover - Windows
            pass

    # The engine's own loops (lease, sweep, poll, managed) and the streams'
    # pumps, from the lists they publish, so none can be left out.
    loops = {**engine.background(), **(feeds.background() if feeds is not None else {})}
    tasks = [loop.create_task(coro, name=name) for name, coro in loops.items()] + [
        loop.create_task(_reconcile_loop(engine, reconciler, engine_config.reconcile_interval_s), name="reconcile"),
        loop.create_task(_control_loop(engine, stopping), name="control"),
        loop.create_task(eod.loop(), name="eod"),
        loop.create_task(_status_loop(engine, float(config.get("status_interval_s", 60))), name="status"),
    ]
    waiter = loop.create_task(stopping.wait())
    done, _ = await asyncio.wait([*tasks, waiter], return_when=asyncio.FIRST_COMPLETED)
    for task in tasks:
        task.cancel()
    failure = next((t for t in done if t is not waiter and t.exception()), None)
    if failure is not None:
        log.error("synpath.engine: %s stopped the engine: %s", failure.get_name(), failure.exception())
    if config.get("halt_on_exit", True):
        await engine.halt("the engine is shutting down", policy=engine_config.halt_policy)
    if feeds is not None:
        await feeds.close()
    await engine.stop()
    for adapter in adapters.values():
        try:
            await adapter.close()
        except Exception:
            pass
    print("engine stopped; the lease is released")
    return 0 if failure is None else 4


async def _reconcile_loop(engine: Engine, reconciler: Reconciler, interval: float) -> None:
    while engine.running:
        await asyncio.sleep(interval)
        try:
            for report in await reconciler.run():
                if not report.clean:
                    log.warning("synpath.engine: reconciliation on %s found %s", report.venue, report.summary())
        except Exception:
            log.exception("synpath.engine: reconciliation failed")


async def _control_loop(engine: Engine, stopping: asyncio.Event) -> None:
    """Apply halt and resume requests written into the journal by the CLI."""
    seen_halt = await engine.journal.cursor(HALT_REQUEST)
    seen_resume = await engine.journal.cursor(RESUME_REQUEST)
    while engine.running:
        await asyncio.sleep(1.0)
        halt = await engine.journal.cursor(HALT_REQUEST)
        if halt and halt != seen_halt:
            seen_halt = halt
            request = json.loads(halt)
            await engine.halt(request.get("reason", "requested"), scope=request.get("scope", "*"),
                              policy=request.get("policy"))
            log.warning("synpath.engine: halted by request: %s", request.get("reason"))
            if request.get("stop"):
                stopping.set()
        resume = await engine.journal.cursor(RESUME_REQUEST)
        if resume and resume != seen_resume:
            seen_resume = resume
            await engine.resume(scope=json.loads(resume).get("scope"))
            log.warning("synpath.engine: resumed by request")


async def _status_loop(engine: Engine, interval: float) -> None:
    while engine.running:
        await asyncio.sleep(interval)
        marks = engine.fair_values.marks()
        total = engine.ledger.total(marks)
        log.info(
            "synpath.engine: %s open orders, %s positions, realized %s, fees %s, events %s, halted=%s",
            len(engine.open_orders()), len(engine.ledger.open_positions()), total.realized, total.fees,
            engine.bus.published, engine.risk.kill.engaged,
        )


async def status(args: argparse.Namespace) -> int:
    """Read a journal without taking its lease."""
    journal = Journal(args.journal)
    await journal.open()
    try:
        async with journal._db.execute("SELECT name, owner, expires_ts FROM leases") as cursor:
            leases = [dict(row) async for row in cursor]
        open_orders = await journal.open_orders()
        in_doubt = await journal.in_doubt()
        fills = await journal.fills()
        last = await journal.last_seq()
        print(f"journal {args.journal}: {last} events")
        for lease in leases:
            alive = lease["expires_ts"] > now_ms()
            print(f"  lease {lease['name']}: {lease['owner']} ({'live' if alive else 'expired'})")
        print(f"  open orders: {len(open_orders)}")
        for order in open_orders[:20]:
            print(f"    {order.venue} {order.id} {order.side.value} {order.remaining} {order.market_id} "
                  f"at {order.price} ({order.book or 'no book'})")
        print(f"  intents in doubt: {len(in_doubt)}")
        for intent in in_doubt[:20]:
            print(f"    {intent.client_order_id} {intent.operation} {intent.venue} {intent.market_id} "
                  f"({round(intent.age_ms / 1000)}s)")
        print(f"  fills recorded: {len(fills)}")
        config = await journal.config("risk")
        if config:
            print(f"  risk configuration: version {config[0]}")
    finally:
        await journal.close()
    return 0


async def control(args: argparse.Namespace, *, resume: bool = False) -> int:
    journal = Journal(args.journal)
    await journal.open()
    try:
        payload = {"ts": now_ms(), "by": os.environ.get("USER", "unknown")}
        if resume:
            payload["scope"] = args.scope
            await journal.set_cursor(RESUME_REQUEST, json.dumps(payload))
            print("resume requested; a running engine applies it within a second")
        else:
            payload |= {"reason": args.reason, "scope": args.scope, "policy": args.policy, "stop": args.stop}
            await journal.set_cursor(HALT_REQUEST, json.dumps(payload))
            print(f"halt requested ({args.policy}); a running engine applies it within a second")
    finally:
        await journal.close()
    return 0


async def eod_command(args: argparse.Namespace) -> int:
    config = load_config(args.config) if args.config else {}
    adapters = build_adapters(config, dotenv=args.dotenv) if config.get("venues") else {}
    engine = Engine(adapters, EngineConfig(journal_path=args.journal or config.get("journal", "synpath.db"),
                                           require_lease=False))
    await engine.journal.open()
    await engine.recover()
    report = await EndOfDay(engine).run(book_settlements=bool(adapters))
    print(json.dumps(report.summary(), indent=2))
    await engine.journal.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m synpath.engine", description=__doc__.splitlines()[0])
    parser.add_argument("--journal", default=None, help="path to the journal database")
    parser.add_argument("--dotenv", default=None, help="path to a .env file with venue credentials")
    parser.add_argument("--log-level", default="INFO")
    # The same flags after the subcommand, where a hand naturally types them.
    # SUPPRESS so an unset one does not overwrite the value given before it.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--journal", default=argparse.SUPPRESS)
    common.add_argument("--dotenv", default=argparse.SUPPRESS)
    common.add_argument("--log-level", default=argparse.SUPPRESS)
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="run the engine until stopped", parents=[common])
    run_parser.add_argument("--config", default=None, help="TOML or JSON configuration")

    sub.add_parser("status", help="print what a journal knows, without taking its lease", parents=[common])

    halt_parser = sub.add_parser("halt", help="ask a running engine to stop trading", parents=[common])
    halt_parser.add_argument("--reason", default="requested by an operator")
    halt_parser.add_argument("--scope", default="*", help="a venue id, or * for everything")
    halt_parser.add_argument("--policy", default="cancel", choices=["cancel", "hold", "rearm"])
    halt_parser.add_argument("--stop", action="store_true", help="also stop the process")

    resume_parser = sub.add_parser("resume", help="lift a halt", parents=[common])
    resume_parser.add_argument("--scope", default=None)

    eod_parser = sub.add_parser("eod", help="close the day and print the report", parents=[common])
    eod_parser.add_argument("--config", default=None)

    args = parser.parse_args(argv)
    if args.command in ("status", "halt", "resume") and not args.journal:
        parser.error("--journal is required for this command")
    try:
        if args.command == "run":
            return asyncio.run(run(args))
        if args.command == "status":
            return asyncio.run(status(args))
        if args.command == "halt":
            return asyncio.run(control(args))
        if args.command == "resume":
            return asyncio.run(control(args, resume=True))
        if args.command == "eod":
            return asyncio.run(eod_command(args))
    except CredentialsMissing as exc:
        print(f"credentials: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
