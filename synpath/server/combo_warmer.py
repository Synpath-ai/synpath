"""serve's combo boards, kept warm: built at start-up, refreshed on a timer.

A combo venue's board (`synpath.engine.combo_board.board_for`) reads the
venue's whole combo-eligible list (Polymarket: about 5,000 markets, plus
Gamma's markets and events for them; Kalshi: every open collection's events)
and takes seconds to minutes to build. A request must never wait for that,
so serve builds each combo venue's board in the background
(`ComboBoardWarmer.loop`, one of serve's background tasks) and the board
routes (`GET /trading/combos/board`, `/combos/board/categories`) answer from
the last build:

* **start-up**: every combo venue's first build starts at once; until it ends
  the routes answer `status: "building"` with no events, at once;
* **refresh**: every `refresh_interval_s` (60 s) after the last build ended,
  each board is built again, through the venue's whole list (no page cap),
  sleeping `pause_s` between two venue reads;
* **the first build fills in**: Polymarket lists about 214,000 combo-eligible
  markets (214 pages), so its first build takes minutes; the board so far is
  served (`complete: false`) after its first 5 pages (the busiest markets) and
  every 40 after. Gamma's markets and events are kept between builds
  (`BoardCache`), so a refresh reads Gamma only for new markets;
* **the last good board stays**: a refresh that fails, or is still running,
  leaves the last board served; `refreshing` and `error` say so.

The warmer also names markets for the RFQ groups' skip reasons
(`labels_for`, the engine's `Labeler`): a leg on a warm board by its pick
(`Al Fayha to win`), else by its market's question from the catalogue,
shortened (`U.S. invades Iran`).

Configured in `engine.toml`:

```toml
[combo_board]
warm = true                # false: build on request, as a bare trading app does
refresh_interval_s = 60
pause_s = 0.1              # between two reads of one venue during a build
```
"""
from __future__ import annotations

import asyncio
import gc
import logging
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable

from ..engine.combo_board import BoardCache, ComboBoard, board_for, label_of, market_labels, short_question
from ..engine.rfq_group import lists_combo_markets
from ..trading.types import ComboLeg

log = logging.getLogger("synpath.serve.combo_boards")

REFRESH_INTERVAL_S = 60.0
PAUSE_S = 0.1
SWITCH_INTERVAL_S = 0.0005
"""The GIL switch interval while boards are warmed (`loop`)."""


@dataclass
class WarmBoard:
    """One venue's board as the warmer holds it."""

    board: ComboBoard | None = None
    built_at: int | None = None
    """When the board held was built, ms."""
    build_ms: int | None = None
    """How long that build took."""
    refreshing: bool = False
    error: str | None = None
    """Why the last build failed, while the board held is an older one (or none)."""

    @property
    def status(self) -> str:
        return "ready" if self.board is not None else "building"


class ComboBoardWarmer:
    """Every combo venue's board, built in the background (module doc)."""

    def __init__(self, engine: Any, registry: Any | None = None, *, refresh_interval_s: float = REFRESH_INTERVAL_S,
                 pause_s: float = PAUSE_S, clock: Callable[[], float] = time.time):
        self.engine = engine
        self.registry = registry
        self.refresh_interval_s = float(refresh_interval_s)
        self.pause_s = float(pause_s)
        self.clock = clock
        self.boards: dict[str, WarmBoard] = {}
        self.labels: dict[str, dict[str, str]] = {}
        """Per venue, `market_labels` of its last board."""
        self._builds: dict[str, asyncio.Task] = {}
        self.caches: dict[str, BoardCache] = {}
        """Per venue, what a build keeps for the next (Gamma's markets and events)."""

    # -- which venues -----------------------------------------------------------

    def venues(self) -> list[str]:
        """The engine's venues with a combo board: Kalshi's collections, or a
        venue's own eligible list."""
        return [name for name, adapter in self.engine.adapters.items()
                if adapter.has.get("combos") and (callable(getattr(adapter, "fetch_combo_collections", None))
                                                  or lists_combo_markets(adapter))]

    def warms(self, venue: str) -> bool:
        return venue in self.venues()

    # -- what the routes read -------------------------------------------------

    def board(self, venue: str) -> ComboBoard:
        """The venue's last board, with its `status` and `built_at`; while its
        first build runs, an empty board with `status: "building"`. Never
        waits: a venue not yet being built starts building here."""
        held = self.boards.get(venue)
        if held is None or held.board is None:
            self.ensure(venue)
            return ComboBoard(venue=venue, count=0, complete=False, status="building",
                              error=held.error if held is not None else None)
        return held.board.model_copy(update={"status": "ready", "built_at": held.built_at, "error": held.error})

    def state(self, venue: str) -> dict[str, Any]:
        held = self.boards.get(venue) or WarmBoard()
        board = held.board
        return {"venue": venue, "status": held.status, "built_at": held.built_at, "build_ms": held.build_ms,
                "complete": board.complete if board is not None else False, "refreshing": held.refreshing,
                "events": len(board.events) if board is not None else 0, "error": held.error}

    # -- building ---------------------------------------------------------------

    def ensure(self, venue: str) -> asyncio.Task:
        """The venue's build: the one running, else a new one."""
        task = self._builds.get(venue)
        if task is None or task.done():
            task = asyncio.get_running_loop().create_task(self.build(venue), name=f"synpath-combo-board-{venue}")
            self._builds[venue] = task
        return task

    async def build(self, venue: str) -> None:
        """Build one venue's board through its whole list. A failure keeps
        the last good board and is logged; it never raises."""
        held = self.boards.setdefault(venue, WarmBoard())
        held.refreshing = True
        started = self.clock()
        try:
            board = await board_for(venue, self.engine.adapters[venue], self.reader(venue), pages=None,
                                    max_events=None, pause_s=self.pause_s,
                                    cache=self.caches.setdefault(venue, BoardCache()),
                                    progress=lambda partial: self.partial(venue, partial))
        except Exception as exc:
            held.error = f"{type(exc).__name__}: {exc}"
            log.warning("synpath serve: building the %s combo board failed (%s); %s", venue, held.error,
                        "keeping the last board" if held.board is not None else "no board yet")
            return
        finally:
            held.refreshing = False
        held.board, held.error = board, None
        held.built_at = int(self.clock() * 1000)
        held.build_ms = int((self.clock() - started) * 1000)
        self.labels[venue] = await asyncio.to_thread(market_labels, board)
        keep_out_of_gc_scans()
        log.info("synpath serve: %s combo board built: %d events from %d markets in %.1f s%s", venue,
                 len(board.events), board.count, held.build_ms / 1000, "" if board.complete else " (incomplete)")

    async def partial(self, venue: str, board: ComboBoard) -> None:
        """The first build's board so far (`complete: false`), served until it
        ends; a refresh keeps serving the last whole board instead."""
        held = self.boards.setdefault(venue, WarmBoard())
        if held.board is not None and held.board.complete:
            return
        held.board, held.built_at = board, int(self.clock() * 1000)
        self.labels[venue] = await asyncio.to_thread(market_labels, board)
        keep_out_of_gc_scans()

    async def refresh_all(self) -> None:
        venues = self.venues()
        await asyncio.gather(*(self.ensure(venue) for venue in venues), return_exceptions=True)

    async def loop(self) -> None:
        """serve's background task: build every board now, then again each
        `refresh_interval_s` after the last build ended."""
        # Builds run in worker threads (`share_the_lock`). Under the GIL,
        # serve's event loop also waits up to one switch interval (5 ms by
        # default) each time it takes the lock back, and one request takes it
        # many times; a 0.5 ms interval keeps requests fast while a board builds.
        sys.setswitchinterval(min(sys.getswitchinterval(), SWITCH_INTERVAL_S))
        while True:
            try:
                await self.refresh_all()
            except Exception:   # a board is never a reason to stop serve
                log.exception("synpath serve: refreshing the combo boards failed")
            await asyncio.sleep(self.refresh_interval_s)

    async def close(self) -> None:
        for task in self._builds.values():
            task.cancel()
        await asyncio.gather(*self._builds.values(), return_exceptions=True)

    def reader(self, venue: str) -> Any | None:
        if self.registry is None:
            return None
        try:
            return self.registry.get(venue)
        except Exception:
            return None

    # -- naming legs (the RFQ groups' `Labeler`) -----------------------------------

    async def labels_for(self, legs: list[ComboLeg]) -> dict[str, str]:
        """A short name for each leg's market: its pick on a warm board, else
        its question from the catalogue, shortened. Legs neither knows are
        left out (the group names them by id)."""
        out: dict[str, str] = {}
        unknown: dict[str, list[str]] = {}
        for leg in legs:
            venue = leg.market_id.partition(":")[0]
            found = label_of(self.labels.get(venue) or {}, leg.market_id, leg.outcome)
            if found:
                out[leg.market_id] = found
            else:
                unknown.setdefault(venue, []).append(leg.market_id)
        for venue, wanted in unknown.items():
            reader = self.reader(venue)
            if reader is None or not callable(getattr(reader, "fetch_markets_by_ids", None)):
                continue
            try:
                markets = await asyncio.to_thread(reader.fetch_markets_by_ids, wanted)
            except Exception as exc:
                log.info("synpath serve: reading %s legs' titles failed: %s", venue, exc)
                continue
            for market in markets:
                if market.title:
                    out.setdefault(market.id, short_question(market.title))
        return out


def keep_out_of_gc_scans() -> None:
    """Move every object alive now out of the cyclic collector's reach.

    A whole Polymarket board is millions of objects, and each full collection
    rescans them all while every thread waits: with a board held, collections
    took 0.3-1.4 s each and froze serve's requests. The board and its cache
    are long-lived and acyclic, so `gc.freeze` loses nothing: reference
    counting still frees them when the next build replaces them."""
    gc.freeze()

