"""The market master: what an order must satisfy, cached and dated.

Every order is checked against reference data before it is signed -- the
tick, the minimum size, whether the market is taking orders right now, when
it closes. The read API has all of it on a `Market`; this turns one into a
compact spec per market, applies the rules each venue does not write into
its payload, and keeps the result with the time it was read so a stale spec
is visibly stale rather than silently trusted.

Venue rules that are not in any payload:

  Kalshi          contracts are fixed-point with two decimals; tick from the
                  market's price ladder, 0.01 when it publishes none
  Polymarket      five shares minimum; tick per market, 0.01 by default
  Polymarket US   whole contracts only; tick per market
"""
from __future__ import annotations

import time
from decimal import Decimal
from typing import Any, Awaitable, Callable

from pydantic import BaseModel, ConfigDict, Field

from .. import ids
from ..types import Market
from .money import D
from .types import Precision

VENUE_RULES: dict[str, dict[str, Any]] = {
    "kalshi": {"default_tick": Decimal("0.01"), "min_amount": Decimal("0.01"), "amount_step": Decimal("0.01"), "whole": False},
    # Shares carry two decimals; the minimum (usually 5) is per market, read
    # from its book by the trading adapter.
    "polymarket": {"default_tick": Decimal("0.01"), "min_amount": Decimal("5"), "amount_step": Decimal("0.01"), "whole": False},
    # Every live market trades whole contracts today, but a market may set a
    # fractional `minimumTradeQty`; the trading adapters read it per market.
    "polymarket_us": {"default_tick": Decimal("0.001"), "min_amount": Decimal("1"), "amount_step": Decimal("1"), "whole": True},
}


class MarketSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market_id: str
    """Synpath id, `venue:native`."""
    venue: str
    precision: Precision
    tradable: bool
    """Accepting orders right now, as far as the catalog knew."""
    tradable_yes: bool = True
    tradable_no: bool = True
    """Per side, where a venue says (Polymarket US). Both true elsewhere."""
    status: str
    close_at: int | None = None
    resolution_at: int | None = None
    position_limit: Decimal | None = None
    """A per-market cap the venue imposes, where it publishes one."""
    yes_token: str | None = None
    no_token: str | None = None
    """The venue's own ids for the two sides, where it has them (Polymarket)."""
    read_at: int = Field(default_factory=lambda: int(time.time() * 1000))
    info: dict[str, Any] = Field(default_factory=dict)

    def age_s(self, now_ms: int | None = None) -> float:
        now = now_ms if now_ms is not None else int(time.time() * 1000)
        return max(0.0, (now - self.read_at) / 1000)


InstrumentSpec = MarketSpec
"""The old name, kept so an import written against it still resolves."""


def precision_for(market: Market) -> Precision:
    rules = VENUE_RULES.get(market.venue, VENUE_RULES["polymarket"])
    tick = D(market.tick_size) if market.tick_size else rules["default_tick"]
    return Precision(
        tick=tick,
        min_amount=rules["min_amount"],
        amount_step=rules["amount_step"],
        whole_contracts=rules["whole"],
        face_value=D(market.face_value),
    )


def spec_from_market(market: Market) -> MarketSpec:
    yes_info, no_info = market.yes.info or {}, market.no.info or {}
    tradable_yes = bool(yes_info.get("tradable", True))   # Polymarket US publishes it per side
    tradable_no = bool(no_info.get("tradable", True))
    return MarketSpec(
        market_id=market.id,
        venue=market.venue,
        precision=precision_for(market),
        tradable=bool(market.active) and tradable_yes and tradable_no,
        tradable_yes=tradable_yes,
        tradable_no=tradable_no,
        status=market.status,
        close_at=market.close_timestamp,
        resolution_at=market.resolution_timestamp,
        position_limit=None,
        yes_token=market.yes.venue_token_id,
        no_token=market.no.venue_token_id,
        info={"native_status": market.native_status},
    )


Loader = Callable[[str, list[str]], Awaitable[list[Market]]]
"""`(venue, market_ids) -> markets`, however the caller reaches the venue."""


class MarketMaster:
    """Specs by Synpath market id, with a freshness rule.

    Reads never block on the network: `get` returns what is held or raises
    `KeyError`, and `stale` says which held specs are past `ttl_s` so the
    caller can refresh them through a loader of its choosing. Loading is the
    caller's concern because the read adapters are synchronous today and the
    engine will not run them on its loop thread.
    """

    def __init__(self, *, ttl_s: float = 300.0):
        self.ttl_s = ttl_s
        self._specs: dict[str, MarketSpec] = {}

    def put(self, specs: list[MarketSpec]) -> None:
        for spec in specs:
            self._specs[spec.market_id] = spec

    def put_market(self, market: Market) -> None:
        self.put([spec_from_market(market)])

    def get(self, market_id: str) -> MarketSpec:
        return self._specs[market_id]

    def find(self, market_id: str) -> MarketSpec | None:
        return self._specs.get(market_id)

    def stale(self, market_ids: list[str] | None = None, *, now_ms: int | None = None) -> list[str]:
        """Held specs older than `ttl_s`, plus ids not held at all."""
        wanted = market_ids if market_ids is not None else list(self._specs)
        out = []
        for market_id in wanted:
            spec = self._specs.get(market_id)
            if spec is None or spec.age_s(now_ms) > self.ttl_s:
                out.append(market_id)
        return out

    async def refresh(self, loader: Loader, market_ids: list[str]) -> None:
        """Reload `market_ids` through `loader`, grouped by venue. The venue
        is read off the id itself, which is what the prefix is for."""
        wanted: dict[str, list[str]] = {}
        for market_id in market_ids:
            wanted.setdefault(ids.venue_of(market_id), []).append(market_id)
        for venue, batch in wanted.items():
            for market in await loader(venue, list(dict.fromkeys(batch))):
                self.put_market(market)

    def __len__(self) -> int:
        return len(self._specs)


InstrumentMaster = MarketMaster
"""The old name, kept so an import written against it still resolves."""
