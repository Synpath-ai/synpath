"""Where a self-hosted server leaves its access token for clients on the same machine.

A `synpath serve` on your own machine should need no token from you: the
server makes one on first start and the library finds it. So the server
records `{host:port -> access token, control database, pid}` in a file only your user
can read (`~/.synpath/servers.json`, mode 0600), and `synpath.Client(server=
"http://127.0.0.1:8000")` reads the token from there when the address is
loopback. A server reached over the network is a different matter: nothing
is looked up, and the caller passes the token (`access_token=`, or
`SYNPATH_ACCESS_TOKEN`), which the server printed once when it made it.

The secret is written only when it is created, because the control database
keeps a hash and cannot give it back. Deleting the file loses the local
copy; `synpath bootstrap` makes a new owner access token.
"""
from __future__ import annotations

import json
import os
import stat
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

LOOPBACK = {"127.0.0.1", "localhost", "::1", "[::1]"}
ENV_KEY = "SYNPATH_ACCESS_TOKEN"
ENV_HOME = "SYNPATH_HOME"


def home(override: str | None = None) -> Path:
    return Path(override or os.environ.get(ENV_HOME) or (Path.home() / ".synpath"))


def registry_path(override: str | None = None) -> Path:
    return home(override) / "servers.json"


def is_loopback(url: str) -> bool:
    host = urlsplit(url if "://" in url else f"http://{url}").hostname
    return host in LOOPBACK


def address(host: str, port: int) -> str:
    """One name for every way of saying this machine, so a server started on
    `localhost` is found by a client that says `127.0.0.1`."""
    host = host.strip("[]")
    if host in ("0.0.0.0", "", "::") or host in LOOPBACK:
        host = "127.0.0.1"
    return f"{host}:{port}"


def _read(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _write(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, stat.S_IRWXU)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    os.chmod(tmp, stat.S_IRUSR | stat.S_IWUSR)
    tmp.replace(path)


def remember(host: str, port: int, *, control: str, journal: str, key: str | None, home_dir: str | None = None) -> str | None:
    """Record this server. `key` is the secret just made, or `None` on a
    later start, when the earlier record's token is kept if it was for the
    same control database. Returns the token on record, if any."""
    path = registry_path(home_dir)
    data = _read(path)
    name = address(host, port)
    previous = data.get(name) or {}
    kept = key or (previous.get("access_token") if previous.get("control") == str(control) else None)
    data[name] = {"access_token": kept, "control": str(control), "journal": str(journal), "pid": os.getpid(),
                  "started_ms": int(time.time() * 1000)}
    _write(path, data)
    return kept


def lookup(server: str, *, home_dir: str | None = None) -> str | None:
    """The stored access token for a loopback server address, or `None`."""
    if not is_loopback(server):
        return None
    parts = urlsplit(server if "://" in server else f"http://{server}")
    name = address(parts.hostname or "127.0.0.1", parts.port or (443 if parts.scheme == "https" else 80))
    entry = _read(registry_path(home_dir)).get(name) or {}
    return entry.get("access_token") or None


def resolve_key(server: str, explicit: str | None = None, *, home_dir: str | None = None) -> str | None:
    """Explicit, then the environment, then the local registry for loopback."""
    return explicit or os.environ.get(ENV_KEY) or lookup(server, home_dir=home_dir)
