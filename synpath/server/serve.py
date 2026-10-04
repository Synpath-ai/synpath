"""`synpath serve`: everything a self-hosted synpath runs, in one process.

```bash
synpath serve                       # every venue whose credentials are in the environment or .env
synpath serve --config engine.toml  # the venues, risk rules and journal the file names
```

One process, one event loop:

* the **engine**, with every loop it publishes (`Engine.background()`): the
  journal's lease, the in-doubt sweep, the poll, the managed-order clock;
* the **feeds** (`synpath.engine.feeds`): each venue's market and account
  streams, so stops see prices and parents hear their children fill;
* **reconciliation**, the **end-of-day** close, **halt/resume** requests
  written by `synpath halt`, and a periodic status line;
* the **HTTP API**: the read app at `/` and the trading app at `/trading`,
  with `/trading/ws/events`.

With no configuration it trades every venue whose credentials it finds and
still serves market data if it finds none. The first start on a new control
database creates an owner access token; that token makes every other.
`SIGINT`/`SIGTERM` stop the HTTP server, apply the halt policy (cancel
everything resting, by default), close the streams, release the lease.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass, field
from typing import Any

from . import local

log = logging.getLogger("synpath.serve")

VENUES = ("kalshi", "polymarket", "polymarket_us", "opinion", "hyperliquid")


def venues_config(config: dict[str, Any], credentials: dict[str, Any]) -> dict[str, Any]:
    """The configuration's venues if it names any; otherwise every venue
    whose credentials are present. Nothing to write for the common case."""
    if config.get("venues"):
        return config
    return {**config, "venues": {venue: {} for venue in VENUES if credentials.get(venue) is not None}}


@dataclass
class Stack:
    engine: Any
    feeds: Any | None
    store: Any
    app: Any
    reconciler: Any
    eod: Any
    config: dict[str, Any]
    owner_key: str | None = None
    """The owner access token created on this start; `None` if the control database
    already had users."""
    local_key: str | None = None
    """The access token on record in the local registry for this server's address."""
    extra_tasks: dict[str, Any] = field(default_factory=dict)


async def build(
    *,
    config: dict[str, Any] | None = None,
    journal: str = "synpath.db",
    control: str = "synpath-control.db",
    dotenv: str | None = None,
    streams: bool = True,
    adapters: dict[str, Any] | None = None,
    stream_map: dict[str, Any] | None = None,
    host: str = "127.0.0.1",
    port: int = 8000,
    home_dir: str | None = None,
) -> Stack:
    """Assemble and start everything except the HTTP server. `adapters` and
    `stream_map` let a test inject paper venues and fake streams."""
    from ..engine.__main__ import build_adapters, risk_from
    from ..engine.engine import Engine, EngineConfig
    from ..engine.eod import EndOfDay
    from ..engine.feeds import Feeds, default_streams
    from ..engine.reconcile import Reconciler
    from ..trading.credentials import load_credentials
    from .api import create_app
    from .store import ControlStore
    from .trading import create_trading_app

    config = dict(config or {})
    credentials = load_credentials(dotenv=dotenv) if adapters is None or (streams and stream_map is None) else {}
    if adapters is None:
        config = venues_config(config, credentials)
        adapters = build_adapters(config, dotenv=dotenv) if config.get("venues") else {}
        if not adapters:
            log.warning("synpath serve: no venue credentials found; serving market data only. "
                        "Run `synpath doctor` to see what is missing.")
    engine_config = EngineConfig(
        journal_path=config.get("journal", journal),
        poll_interval_s=float(config.get("poll_interval_s", 5)),
        reconcile_interval_s=float(config.get("reconcile_interval_s", 60)),
        sweep_interval_s=float(config.get("sweep_interval_s", 10)),
        in_doubt_timeout_s=float(config.get("in_doubt_timeout_s", 20)),
        halt_policy=config.get("halt_policy", "cancel"),
    )
    engine = Engine(adapters, engine_config, risk=risk_from(config))
    feeds = None
    if streams and config.get("streams", True):
        feeds = Feeds(engine, stream_map if stream_map is not None else default_streams(adapters, credentials))
    reconciler = Reconciler(engine, orphan_policy=config.get("orphan_policy", "report"))
    eod = EndOfDay(engine, hour_utc=int(config.get("eod_hour_utc", 0)))

    await engine.start()
    store = await ControlStore(config.get("control", control)).open()
    owner_key = None
    if not await store.has_users():
        _, issued = await store.bootstrap("owner")
        owner_key = issued.secret
    # Leave the token where clients on this machine find it (synpath.server.local).
    stack_key = local.remember(host, port, control=config.get("control", control),
                               journal=engine_config.journal_path, key=owner_key, home_dir=home_dir)
    if feeds is not None:
        await feeds.start()

    app = create_app()
    app.mount("/trading", create_trading_app(engine, store))
    app.state.trading_path = "/trading"
    return Stack(engine=engine, feeds=feeds, store=store, app=app, reconciler=reconciler, eod=eod,
                 config=config, owner_key=owner_key, local_key=stack_key)


def background(stack: Stack) -> dict[str, Any]:
    """Every coroutine the stack needs running beside the HTTP server."""
    from ..engine.__main__ import _control_loop, _reconcile_loop, _status_loop

    engine = stack.engine
    loops: dict[str, Any] = dict(engine.background())
    if stack.feeds is not None:
        loops.update(stack.feeds.background())
    loops["reconcile"] = _reconcile_loop(engine, stack.reconciler, engine.config.reconcile_interval_s)
    loops["control"] = _control_loop(engine, asyncio.Event())
    loops["eod"] = stack.eod.loop()
    loops["status"] = _status_loop(engine, float(stack.config.get("status_interval_s", 60)))
    return loops


async def shutdown(stack: Stack) -> None:
    engine = stack.engine
    if stack.config.get("halt_on_exit", True) and engine.running:
        try:
            await engine.halt("the server is shutting down", policy=engine.config.halt_policy)
        except Exception:
            log.exception("synpath serve: halting on exit failed")
    if stack.feeds is not None:
        await stack.feeds.close()
    await engine.stop()
    await stack.store.close()
    for adapter in engine.adapters.values():
        try:
            await adapter.close()
        except Exception:
            pass


async def serve(args: argparse.Namespace) -> int:
    import uvicorn

    from ..engine.__main__ import load_config
    from ..engine.journal import LeaseLost

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s")
    config = load_config(args.config) if args.config else {}
    try:
        stack = await build(config=config, journal=args.journal, control=args.control, dotenv=args.dotenv,
                            streams=not args.no_streams, host=args.host, port=args.port)
    except LeaseLost as exc:
        print(str(exc), file=sys.stderr)
        return 3
    if args.host in local.LOOPBACK:
        # On this machine the token is found, not typed: the library reads the
        # registry. Only say where it is.
        if stack.owner_key:
            print(f"\n  A new control database. Its owner access token is in {local.registry_path()} (readable by you only);\n"
                  "  synpath.Client(server=...) and the TypeScript client on this machine use it automatically.\n"
                  "  To reach this server from elsewhere, send that token as `Authorization: Bearer <token>`.\n")
    elif stack.owner_key:
        print("\n  A new control database: this is the owner access token. It is shown once.\n"
              f"    {stack.owner_key}\n"
              "  Send it as `Authorization: Bearer <token>` to /trading; it can issue every other token.\n")
    if args.host not in ("127.0.0.1", "localhost", "::1"):
        print(f"warning: binding to {args.host}. The market-data routes at / have no authentication; "
              "/trading requires an access token. Put a proxy with TLS in front of anything public.")

    server = uvicorn.Server(uvicorn.Config(stack.app, host=args.host, port=args.port, log_level=args.log_level.lower()))
    loop = asyncio.get_running_loop()
    tasks = [loop.create_task(coro, name=f"synpath-{name}") for name, coro in background(stack).items()]

    def _died(task: asyncio.Task) -> None:
        if not task.cancelled() and task.exception() is not None:
            log.error("synpath serve: %s failed: %s; stopping", task.get_name(), task.exception())
            server.should_exit = True
    for task in tasks:
        task.add_done_callback(_died)

    engine = stack.engine
    print(f"synpath serving on http://{args.host}:{args.port} (trading at /trading), "
          f"venues: {', '.join(sorted(engine.adapters)) or 'none'}, journal: {engine.config.journal_path}")
    try:
        await server.serve()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await shutdown(stack)
        print("synpath stopped; the lease is released")
    return 0


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="synpath serve", description="Run the engine, the venues' streams and the "
                                "HTTP API (market data at /, trading at /trading) in one process.")
    p.add_argument("--host", default="127.0.0.1", help="default: 127.0.0.1")
    p.add_argument("--port", type=int, default=8000, help="default: 8000")
    p.add_argument("--config", default=None, help="TOML or JSON: venues, risk, journal. Optional")
    p.add_argument("--journal", default="synpath.db", help="the engine's journal. Default: synpath.db")
    p.add_argument("--control", default="synpath-control.db", help="users and keys. Default: synpath-control.db")
    p.add_argument("--dotenv", default=None, help="a .env file with venue credentials")
    p.add_argument("--no-streams", action="store_true", help="do not subscribe the venues' streams; poll only")
    p.add_argument("--log-level", default="info")
    return p


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(serve(parser().parse_args(argv)))
