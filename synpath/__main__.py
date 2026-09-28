"""`synpath`: one command for everything the package runs.

```bash
synpath init                   # asks for your venue keys and writes .env (readable by you only)
synpath serve                  # engine + streams + HTTP API, in one process
synpath doctor                 # which venue credentials load (prints no secret)
synpath status | halt | resume | eod     # ask or tell a running engine, through its journal
synpath run --config engine.toml         # the engine alone, no HTTP
synpath schema [--trading] --out openapi.json
synpath bootstrap              # the first key for a control database
synpath login                  # Google sign-in in your browser; return to CLI
synpath keys create|list|revoke # manage hosted history API keys
synpath logout                 # end the key-management session
```

`synpath engine ...`, `synpath server ...` and `synpath trading ...` reach
the underlying `python -m synpath.<module>` commands with every option.
"""
from __future__ import annotations

import importlib
import sys

MODULES = {
    "serve": "synpath.server.serve",
    "engine": "synpath.engine.__main__",
    "server": "synpath.server.__main__",
    "trading": "synpath.trading.__main__",
    "init": "synpath.trading.init",
    "login": "synpath.hosted_auth",
    "keys": "synpath.hosted_auth",
    "logout": "synpath.hosted_auth",
}
SHORTCUTS = {
    "run": ("engine", ["run"]),
    "status": ("engine", ["status"]),
    "halt": ("engine", ["halt"]),
    "resume": ("engine", ["resume"]),
    "eod": ("engine", ["eod"]),
    "schema": ("server", ["schema"]),
    "bootstrap": ("server", ["bootstrap"]),
    "doctor": ("trading", ["doctor"]),
}

USAGE = __doc__.split("```bash", 1)[1].split("```", 1)[0].strip()


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ("-h", "--help", "help"):
        print("usage: synpath <command> [options]\n\n" + USAGE)
        return 0
    command, rest = argv[0], argv[1:]
    if command in SHORTCUTS:
        command, prefix = SHORTCUTS[command]
        rest = prefix + rest
    if command not in MODULES:
        print(f"synpath: unknown command {command!r}\n\nusage: synpath <command> [options]\n\n" + USAGE, file=sys.stderr)
        return 2
    module = importlib.import_module(MODULES[command])
    result = getattr(module, command if command in ("login", "keys", "logout") else "main")(rest)
    return int(result or 0)


if __name__ == "__main__":
    raise SystemExit(main())
