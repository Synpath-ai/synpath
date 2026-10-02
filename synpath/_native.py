"""Which implementation of the hot paths is in use: the Rust core or Python.

The Rust core ships inside the wheel as `synpath._core`. Every class it
provides has a pure-Python twin with the same interface, so the library works
the same either way and the choice is invisible to callers:

* the Rust one, when the extension is installed (every published wheel);
* the Python one, when it is not (a source checkout without Rust), or when
  `SYNPATH_PURE_PYTHON=1` is set -- which the test suite uses to run every
  test against both.
"""
from __future__ import annotations

import os
from typing import Any

FORCED_PURE = os.environ.get("SYNPATH_PURE_PYTHON", "").strip().lower() in ("1", "true", "yes")

core: Any = None
if not FORCED_PURE:
    try:
        from . import _core as core  # type: ignore[no-redef]
    except ImportError:  # pragma: no cover - only without a built extension
        core = None

ENABLED = core is not None
"""True when the Rust core is in use."""


def pick(name: str, fallback: Any) -> Any:
    """`synpath._core.<name>` when the Rust core is in use, else `fallback`."""
    return getattr(core, name) if core is not None else fallback
