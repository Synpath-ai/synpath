"""`python -m synpath.server` — run the API locally, or write its schema out.

```bash
python -m synpath.server                      # the read API on 127.0.0.1:8000
python -m synpath.server schema --out openapi.json          # the read contract
python -m synpath.server schema --trading --out openapi.json   # the trading one
python -m synpath.server bootstrap --control control.db     # the first API key
```

`schema` exists so a typed client can be generated in CI without starting a
server or holding credentials:

```bash
python -m synpath.server schema --trading --out openapi.json
npx @hey-api/openapi-ts -i openapi.json -o ./src/synpath
```
"""
from __future__ import annotations

import argparse
import asyncio
import json


def schema(args: argparse.Namespace) -> None:
    """Write the OpenAPI document without running anything."""
    if args.trading:
        from ..engine.engine import Engine, EngineConfig
        from .store import ControlStore
        from .trading import create_trading_app

        # The document depends on the routes, not on the engine's state, so an
        # engine that was never started describes the same contract.
        engine = Engine({}, EngineConfig(journal_path=":memory:", require_lease=False))
        app = create_trading_app(engine, ControlStore(":memory:"))
    else:
        from .api import create_app

        app = create_app()
    document = json.dumps(app.openapi(), indent=2, sort_keys=True)
    if args.out:
        with open(args.out, "w") as handle:
            handle.write(document + "\n")
        print(f"wrote {args.out}")
    else:
        print(document)


def bootstrap(args: argparse.Namespace) -> None:
    """Create the first user and key for a control database."""
    from .store import ControlStore

    async def run() -> None:
        store = await ControlStore(args.control).open()
        try:
            user, key = await store.bootstrap(args.name)
            print(f"user {user.id} ({user.name})")
            print(f"key  {key.secret}")
            print("This key is shown once. It has every permission on every account.")
        finally:
            await store.close()

    asyncio.run(run())


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m synpath.server",
        description="Serve the synpath API. The read app has no authentication: bind to localhost, "
                    "or mount it behind your own middleware for anything public.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=8000, help="default: 8000")
    parser.add_argument("--reload", action="store_true", help="reload on code changes")
    parser.add_argument("--log-level", default="info")
    sub = parser.add_subparsers(dest="command")

    schema_parser = sub.add_parser("schema", help="write the OpenAPI document and exit")
    schema_parser.add_argument("--trading", action="store_true", help="the trading contract instead of the read one")
    schema_parser.add_argument("--out", default=None, help="a file to write, or stdout")

    boot = sub.add_parser("bootstrap", help="create the first user and API key")
    boot.add_argument("--control", default="synpath-control.db", help="the control database")
    boot.add_argument("--name", default="owner")

    args = parser.parse_args(argv)
    if args.command == "schema":
        return schema(args)
    if args.command == "bootstrap":
        return bootstrap(args)

    try:
        import uvicorn
    except ImportError:  # pragma: no cover - depends on env
        raise SystemExit(
            'synpath.server needs an ASGI server, which the base install carries. Reinstall with:\n'
            '    pip install synpath'
        ) from None

    if args.host not in ("127.0.0.1", "localhost", "::1"):
        print(
            f"warning: binding to {args.host} exposes an API with no "
            "authentication. Mount create_app() behind your own middleware "
            "instead of serving this directly."
        )

    uvicorn.run(
        "synpath.server:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=args.log_level,
        factory=False,
    )


if __name__ == "__main__":
    main()
