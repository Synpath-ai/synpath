"""HTTP layer over the library. `create_app`, `create_trading_app`,
`VenueRegistry`, `ControlStore` and `Principal` are also exported from the
top-level package: `from synpath import create_app`.

```bash
pip install synpath
python -m synpath.server          # http://127.0.0.1:8000/docs
```

Two ways to use it:

**Standalone**, as above, or with any ASGI server:

```bash
uvicorn synpath.server:app --port 8000
```

**Mounted**, when something else owns authentication, quotas and metering:

```python
from fastapi import Depends, FastAPI
from synpath.server import create_app

outer = FastAPI()
outer.include_router(create_app(docs=False).router, dependencies=[Depends(my_auth)])
```

The read app has no auth of its own and never will — a service that ships
authentication inside the open-source core forces every host to work around
it. Everything cross-venue (matching the same question across exchanges,
routing an order) belongs outside too.

**Trading is a second app**, because placing an order does need to know who
is asking:

```python
from synpath.server import create_trading_app, ControlStore

store = await ControlStore("control.db").open()
user, key = await store.bootstrap("owner")      # once; the key is shown once
app = create_trading_app(engine, store)
```

It carries per-key grants scoped to a subaccount (`view`, `trade`,
`manage_credentials`, `manage_members`), an append-only audit log the
database itself enforces, and `/ws/events`, which replays the engine's event
stream from a cursor before following it live. See
[docs/server.md](../docs/server.md).

`GET /openapi.json` is the contract. Generate a typed client from it rather
than hand-writing one:

```bash
npx @hey-api/openapi-ts -i http://127.0.0.1:8000/openapi.json -o ./src/synpath
```
"""
from __future__ import annotations

import importlib
from typing import Any

__all__ = ["create_app", "VenueRegistry", "app", "main", "create_trading_app", "ControlStore", "Principal"]

_app = None


def __getattr__(name: str) -> Any:
    """Import FastAPI lazily.

    `synpath` itself must not require it: someone using the library in a
    notebook should not be made to install a web framework. Resolving the
    import here also means a missing dependency fails with a sentence saying
    what to install, rather than a bare ImportError three frames down.
    """
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    try:
        # importlib rather than `from . import api`, which would look the name
        # up on this package and re-enter this function forever.
        api = importlib.import_module(f"{__name__}.api")
    except ImportError as exc:  # pragma: no cover - depends on env
        raise ImportError(
            "synpath.server needs FastAPI, which the base install carries. Reinstall with:\n"
            "    pip install synpath"
        ) from exc
    if name == "app":
        # One module-level instance, so `uvicorn synpath.server:app` works.
        global _app
        if _app is None:
            _app = api.create_app()
        return _app
    if name == "main":
        return importlib.import_module(f"{__name__}.__main__").main
    if name == "create_trading_app":
        return importlib.import_module(f"{__name__}.trading").create_trading_app
    if name in ("ControlStore", "Principal"):
        return getattr(importlib.import_module(f"{__name__}.store"), name)
    return getattr(api, name)
