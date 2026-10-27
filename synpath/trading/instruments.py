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
  Gemini          tick per contract (`priceIncrement`), contracts in hundredths
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
    "kalshi": {"default_tick": Decimal("0.01"), "min_quantity": Decimal("0.01"), "quantity_step": Decimal("0.01"), "whole": False},
    # Shares carry two decimals; the minimum (usually 5) is per market, read
    # from its book by the trading adapter.
    "polymarket": {"default_tick": Decimal("0.01"), "min_quantity": Decimal("5"), "quantity_step": Decimal("0.01"), "whole": False},
    # Whole-contract fallback until per-market `minimumTradeQty` is loaded.
    # Live crypto markets can be fractional; the adapter reads their rules.
    "polymarket_us": {"default_tick": Decimal("0.001"), "min_quantity": Decimal("1"), "quantity_step": Decimal("1"), "whole": True,
                      "min_price": Decimal("0.01"), "max_price": Decimal("0.99")},
    # Prices to a tenth of a cent on every book read, and at least 5 USDT an
    # order, counted on the token actually bought.
    "opinion": {"default_tick": Decimal("0.001"), "min_quantity": Decimal("1"), "quantity_step": Decimal("0.01"), "whole": False,
                "min_notional": Decimal("5")},
    # Prices take at most five significant figures, which a 0.0001 grid
    # always meets inside (0, 1); whole contracts; at least 10 in quote an order.
    "hyperliquid": {"default_tick": Decimal("0.0001"), "min_quantity": Decimal("1"), "quantity_step": Decimal("1"), "whole": True,
                    "min_notional": Decimal("10")},
    # Two or three decimals of price per market (its `decimalPrecision`);
    # sizes to a hundredth of a share at least, as the venue's SDK checks.
    "predict_fun": {"default_tick": Decimal("0.01"), "min_quantity": Decimal("0.01"), "quantity_step": Decimal("0.01"), "whole": False},
    # Prices to a tenth of a cent between 1c and 99c; sizes in thousandths of a share.
    "limitless": {"default_tick": Decimal("0.001"), "min_quantity": Decimal("0.001"), "quantity_step": Decimal("0.001"), "whole": False},
    # Prices on a 0.125% ladder; the stake is what is counted, at least 1 USDC,
    # so contracts take any size to a millionth.
    "sx_bet": {"default_tick": Decimal("0.00125"), "min_quantity": Decimal("0.000001"), "quantity_step": Decimal("0.000001"),
               "whole": False, "min_notional": Decimal("1")},
    # Prices are decimal odds on Betfair's CLASSIC ladder (1.01 to 1000), so
    # the step in YES price runs from about 0.0097 near 1.01 to 0.00001 near
    # 1000: the tick here is the finest, and the trading adapter snaps each
    # order onto the ladder in odds. Stakes are in the account's currency,
    # to the penny, at least 1 (GBP) a bet unless its payout is 10 or more,
    # which the adapter checks; the notional here is the plain minimum.
    "betfair": {"default_tick": Decimal("0.00001"), "min_quantity": Decimal("0.01"), "quantity_step": Decimal("0.0001"),
                "whole": False, "min_notional": Decimal("1")},
    # Prices are basis points of 100% on an odds ladder (5000, 4762, 4545 are
    # odds 2.0, 2.1, 2.2), so the tick here is the finest, one basis point,
    # and the trading adapter snaps each order onto the ladder. Quantities
    # are pots in 1/10,000 of the account's currency, so contracts step by
    # 0.0001; the minimum back stake is £0.05.
    "smarkets": {"default_tick": Decimal("0.0001"), "min_quantity": Decimal("0.0001"), "quantity_step": Decimal("0.0001"),
                 "whole": False, "min_notional": Decimal("0.05")},
    # Prices on a grid of 0.001 near the ends (to 0.050, from 0.950) and
    # 0.005 between, so the tick here is the finest and an order is snapped
    # onto the grid on the cheaper side. The venue's contract pays 1 cent, so
    # dollar contracts step by 0.01.
    "novig": {"default_tick": Decimal("0.001"), "min_quantity": Decimal("0.01"), "quantity_step": Decimal("0.01"),
              "whole": False},
    # Prices are decimal odds on Matchbook's ladder (1.01 to 1000, the same
    # bands as Betfair's), so the tick here is the finest and the trading
    # adapter snaps each order onto the ladder in odds. Stakes are in the
    # account's currency to the penny; Matchbook publishes no minimum.
    "matchbook": {"default_tick": Decimal("0.00001"), "min_quantity": Decimal("0.01"), "quantity_step": Decimal("0.0001"),
                  "whole": False},
    # Prices are American odds on ProphetX's ladder (-10000 to +10000), whose
    # steps are 0.0017 to 0.0076 in YES price: the tick here is finer than
    # any, and the adapter snaps an order onto the ladder on the cheaper side.
    # The stake is what is counted, in USD to the cent, at least $1, so
    # contracts take any size to a millionth.
    "prophetx": {"default_tick": Decimal("0.0001"), "min_quantity": Decimal("0.000001"),
                 "quantity_step": Decimal("0.000001"), "whole": False, "min_notional": Decimal("1")},
    # A bet's price is a worst price, turned into minimum odds of `1 / price`
    # at 12 decimals, so any price in (0, 1) is accepted: the tick here is
    # only the grid prices are quoted on. The stake is in the bet token (USDT
    # to six decimals, WETH to eighteen), so contracts take any size to a
    # millionth; the pool's own minimum bet is read per selection.
    "azuro": {"default_tick": Decimal("0.0001"), "min_quantity": Decimal("0.000001"), "quantity_step": Decimal("0.000001"),
              "whole": False},
    # Order books only: prices in whole cents; sizes are 18-decimal shares and
    # the venue publishes no minimum, so a millionth of a share is the step.
    "myriad": {"default_tick": Decimal("0.01"), "min_quantity": Decimal("0.000001"), "quantity_step": Decimal("0.000001"),
               "whole": False},
    # Each contract publishes its grid (priceIncrement, quantityIncrement,
    # quantityMinimum); every one read so far is a cent and a hundredth of a
    # contract, which is the default here. The tick comes from the market.
    "gemini": {"default_tick": Decimal("0.01"), "min_quantity": Decimal("0.01"), "quantity_step": Decimal("0.01"),
               "whole": False},
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
        min_quantity=rules["min_quantity"],
        quantity_step=rules["quantity_step"],
        whole_contracts=rules["whole"],
        face_value=D(market.face_value),
        min_notional=rules.get("min_notional"),
    )


def venue_precision(venue: str, *, tick: Decimal | None = None) -> Precision:
    """A venue's order rules with no market loaded, from `VENUE_RULES`: for
    sizing an amount on a combo the catalog has not read yet. `KeyError` for
    a venue with no rules, rather than another venue's."""
    rules = VENUE_RULES[venue]
    return Precision(
        tick=tick or rules["default_tick"],
        min_quantity=rules["min_quantity"],
        quantity_step=rules["quantity_step"],
        whole_contracts=rules["whole"],
        min_notional=rules.get("min_notional"),
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
