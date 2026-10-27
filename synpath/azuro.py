"""Azuro: an onchain sports betting protocol, read from its feed API and its bets subgraph.

Market data needs no key. The feed (`api.onchainfeed.org`, the `market-manager`
routes) lists sports, games, their conditions and every outcome's current
odds; the bets subgraph (`thegraph.onchainfeed.org`) has every bet placed,
which is the public tape. The feed's bet, cash-out and stream routes refuse
requests from some regions with a 503.

Five things about this venue that shape the adapter:

**A pool, not a book.** Every bet is taken by a shared liquidity pool at the
odds the protocol quotes for its outcome; nothing rests and nobody else's
order is matched. The quoted odds are this library's quote: backing an
outcome at decimal odds `O` is buying its YES at `1 / O` (one contract pays 1
token, and a stake `S` returns `S * O`). Prices here are exactly `1 / O`,
rounded to 8 decimal places; the venue's odds stay on `info`. Odds of 1 or
less, and outcomes not taking bets, are no price at all (`None`).

**A condition is the question, its outcomes the answers.** A game ("Arsenal -
Chelsea") carries dozens of conditions ("Match Winner", "Total Goals 2.5",
"Correct Score"), each with two or more outcomes, exactly one of which wins
(or the condition is cancelled and every bet is returned). Following SX Bet,
a two-outcome condition is one Synpath market, `azuro:<conditionId>`: the
outcome with the lower `outcomeId` is YES and the other NO, each under the
venue's name for it, so betting the second outcome is selling YES. Following
Betfair, a condition with three or more outcomes is one Synpath market per
outcome, `azuro:<conditionId>-<outcomeId>`, each answering "does this outcome
win?"; only its YES can be bought, since the pool sells no "not this
outcome". In every case the game is the event, `azuro:<gameId>`.

**One id on every chain.** The protocol runs a pool per chain (Polygon USDT,
Base WETH, and the discontinued Gnosis and Chiliz pools), but the feed names
a game and its conditions with the same ids on all of them, so ids carry no
chain. The chain the adapter is built for (`chain="polygon"`, the default)
decides which pool's state and odds are read, and the currency a contract
pays out in: USDT on Polygon, WETH on Base.

**Depth is the pool's limit.** A pool takes a bet up to the largest payout it
will risk on that outcome, which the feed computes per selection
(`/bet/calculation`). `fetch_order_book` is the quote as one level a side,
sized by that limit in contracts; `fetch_limits` returns it raw.

**No fee on top.** The pool's margin is inside the odds it quotes. A bet
placed through the relayer also pays the relayer a flat fee in the bet token
for the transaction (`AzuroTrading.fetch_fee_estimate`).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterator

import httpx

from . import ids
from .base import (
    MAX_PAGE_LIMIT, Capability, Exchange, HttpClient, RateLimiter, check_sort, check_status, page_limit,
    pick_bars, timeframe_seconds,
)
from .errors import BadRequest, ExchangeError, MarketNotFound, NotSupported
from .types import (
    BookSide, Candle, Event, FeeSchedule, Market, MarketStats, OrderBook, OrderLevel, Outcome, Page, Quote, Trade,
    iso,
)

VENUE = "azuro"

API_URL = "https://api.onchainfeed.org/api/v1/public"
DEV_API_URL = "https://dev-api.onchainfeed.org/api/v1/public"
SOCKET_URL = "wss://streams.onchainfeed.org/v1/streams"
DEV_SOCKET_URL = "wss://dev-streams.onchainfeed.org/v1/streams"
SUBGRAPH_URL = "https://thegraph.onchainfeed.org/subgraphs/name/azuro-protocol/azuro-api-{name}-v3"


@dataclass(frozen=True)
class Chain:
    """One Azuro deployment: a chain, its pool's contracts and its bet token,
    as `@azuro-org/toolkit` 7.2.0 lists them."""

    name: str
    chain_id: int
    environment: str
    """The feed's name for this deployment, sent with every request."""
    subgraph: str
    """The bets subgraph's name for this chain."""
    core: str
    relayer: str
    lp: str
    cashout: str
    token: str
    symbol: str
    decimals: int
    rpc_url: str
    """A public RPC endpoint, used when the caller gives none."""
    testnet: bool = False
    discontinued: bool = False

    @property
    def api_url(self) -> str:
        return DEV_API_URL if self.testnet else API_URL

    @property
    def socket_url(self) -> str:
        return DEV_SOCKET_URL if self.testnet else SOCKET_URL

    @property
    def subgraph_url(self) -> str:
        return SUBGRAPH_URL.format(name=self.subgraph)


CHAINS: dict[str, Chain] = {
    "polygon": Chain(
        "polygon", 137, "PolygonUSDT", "polygon",
        core="0xF9548Be470A4e130c90ceA8b179FCD66D2972AC7", relayer="0x8dA05c0021e6b35865FDC959c54dCeF3A4AbBa9d",
        lp="0x0FA7FB5407eA971694652E6E16C12A52625DE1b8", cashout="0x4a2BB4211cCF9b9eA6eF01D0a61448154ED19095",
        token="0xc2132d05d31c914a87c6611c10748aeb04b58e8f", symbol="USDT", decimals=6,
        rpc_url="https://polygon-bor-rpc.publicnode.com",
    ),
    "base": Chain(
        "base", 8453, "BaseWETH", "base",
        core="0xF40cF1dD7d16C098cff5F8B5650A8FaEf1F4640d", relayer="0xD2D508d66dB4fCE4B384e4C3EA2fa53BA43e73b5",
        lp="0x1eD7368bc515E928A4007cEa61FB8a6F8863Af87", cashout="0x6EDff24761F4473611B45BDAe4a779ff31af14Be",
        token="0x4200000000000000000000000000000000000006", symbol="WETH", decimals=18,
        rpc_url="https://mainnet.base.org",
    ),
    "gnosis": Chain(
        "gnosis", 100, "GnosisXDAI", "gnosis",
        core="0x0e00E6ffcb5E207B800eec9e02999D1Fd9e448f2", relayer="0x40eb85018fA18dDd10a433C8c40ca8D6744d8B32",
        lp="0xeb7cDA87D00d677A6Dc73EB569723b0fA51D97E7", cashout="0x25de227af58188e656524d1C32cCf7C078cdC891",
        token="0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d", symbol="WXDAI", decimals=18,
        rpc_url="https://rpc.gnosischain.com", discontinued=True,
    ),
    "chiliz": Chain(
        "chiliz", 88888, "ChilizWCHZ", "chiliz",
        core="0xa5061617Ee6565CF48d7f0FEF06910b9fb9dE2b0", relayer="0x81F72B93ABaf061535aaF5D831F05e0CC4084b32",
        lp="0xEf6b12580301b04CD2551182C88623524B6e47b8", cashout="0x3995eebB51793Ee353162E7400DB455B17dE3692",
        token="0x677F7e16C7Dd57be1D4C8aD1244883214953DC47", symbol="WCHZ", decimals=18,
        rpc_url="https://rpc.chiliz.com", discontinued=True,
    ),
    "polygon_amoy": Chain(
        "polygon_amoy", 80002, "PolygonAmoyUSDT", "polygon-amoy-dev",
        core="0xCD0Db5ef28C3Bd3a69283372dE923Eb4DA0585F6", relayer="0x48c9bE88706F22838070eE7C4bC74Ad7A8eeF114",
        lp="0x0a75395Ff15d9557424b632cEBCac448D66F9779", cashout="0x7dF132Ad2334a667A004049a75a4a8a530dc24F2",
        token="0xCf1b86ceD971b88C042C64A9c099377e2738073C", symbol="USDT", decimals=6,
        rpc_url="https://rpc-amoy.polygon.technology", testnet=True,
    ),
    "base_sepolia": Chain(
        "base_sepolia", 84532, "BaseSepoliaWETH", "base-sepolia-dev",
        core="0x679CC42F69F98631668348d088086359D4217Ffa", relayer="0x2De54cABa6A8198D9Dc51931485Bfdc8017aB0c5",
        lp="0x4fE86EddCC22dc992b7994878169201Ec7ea50AB", cashout="0xf52Eb6ec19d81Ea623A7d7Ad397287C0C40f2F37",
        token="0x9e09f213Ff75e53D52e9e777A6567A68683E935f", symbol="WETH", decimals=18,
        rpc_url="https://sepolia.base.org", testnet=True,
    ),
}
"""Every deployment by this library's name for it. Gnosis and Chiliz are
discontinued: the feed still answers for them, with every condition stopped."""


def chain_of(name: str | int | Chain) -> Chain:
    """A deployment by name (`polygon`, `base`, ...) or chain id."""
    if isinstance(name, Chain):
        return name
    key = str(name).strip().lower().replace("-", "_")
    if key in CHAINS:
        return CHAINS[key]
    for chain in CHAINS.values():
        if key == str(chain.chain_id):
            return chain
    raise BadRequest(f"{VENUE}: unknown chain {name!r}; expected one of {', '.join(CHAINS)}")


LIMITER = RateLimiter(5.0, burst=10)
"""The feed publishes no rate limit; kept modest and shared process-wide."""

GAMES_PAGE = 10
"""Games per listing page: the feed's minimum. A game carries up to a few
hundred conditions, so pages stay small."""

MAX_PAGES = 200
"""Most game pages one walk reads."""

MAX_GAMES = 10
"""Games one conditions request asks for."""

TRADES_PAGE = 100
MAX_TRADE_PAGES = 20
"""Most tape pages one candle read walks back through: 2,000 bets."""

PRICE_PLACES = 8

CONDITION_ID = re.compile(r"\d{1,80}")
OPEN_STATES = ("Live", "Prematch")
"""The game states the feed lists, live games first."""


# ---------------------------------------------------------------------------
# Prices and ids
# ---------------------------------------------------------------------------

def odds_value(value: Any) -> Decimal | None:
    """The venue's decimal odds, or `None` when there are none to bet at."""
    if value in (None, ""):
        return None
    try:
        odds = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return odds if odds > 1 else None


def odds_price(value: Any) -> float | None:
    """Decimal odds as the price of the outcome's YES: `1 / odds`, to 8 places."""
    odds = odds_value(value)
    if odds is None:
        return None
    return round(float(Decimal(1) / odds), PRICE_PLACES)


def market_native(condition_id: str, outcome_id: str | int | None = None) -> str:
    """`<conditionId>` for a two-outcome condition, `<conditionId>-<outcomeId>` for one outcome of a larger one."""
    return str(condition_id) if outcome_id is None else f"{condition_id}-{outcome_id}"


def parse_market_id(native: str) -> tuple[str, str | None]:
    """(condition id, outcome id or `None`) from a market's native id."""
    condition_id, sep, outcome_id = str(native).partition("-")
    if not CONDITION_ID.fullmatch(condition_id) or (sep and not outcome_id.isdigit()):
        raise BadRequest(
            f"{VENUE}: {native!r} is not an Azuro market id: expected <conditionId> or <conditionId>-<outcomeId>"
        )
    return condition_id, (outcome_id if sep else None)


def game_of_condition(condition_id: str) -> str | None:
    """The game a v3 condition id names. Every feed condition id read so far
    is 48 digits whose 5th to 26th are its game's 22-digit id; anything else
    is `None` and is looked up by condition instead."""
    if len(condition_id) == 48 and condition_id[4:6] == "10":
        return condition_id[4:26]
    return None


def outcome_order(condition: dict[str, Any]) -> list[dict[str, Any]]:
    """A condition's outcomes by `outcomeId`, which fixes YES and NO: the feed
    lists them in display order, which differs between its own routes."""
    rows = [o for o in condition.get("outcomes") or [] if isinstance(o, dict) and o.get("outcomeId") is not None]
    return sorted(rows, key=lambda o: int(str(o["outcomeId"])))


def is_binary(condition: dict[str, Any]) -> bool:
    return len(outcome_order(condition)) == 2


def outcome_odds(outcome: dict[str, Any]) -> Any:
    """The odds an outcome is offered at now: the stream's `currentOdds`, the REST feed's `odds`."""
    return outcome.get("currentOdds") if outcome.get("currentOdds") is not None else outcome.get("odds")


def takes_bets(condition: dict[str, Any], outcome: dict[str, Any]) -> bool:
    return str(condition.get("state") or "") == "Active" and str(outcome.get("state") or "Active") == "Active"


def quote_of(condition: dict[str, Any], yes: dict[str, Any], no: dict[str, Any] | None) -> tuple[Quote, Quote]:
    """The YES and NO quotes from the pool's odds. Buying YES costs `1 / O`
    of the YES outcome; on a two-outcome condition, selling YES is backing the
    other outcome, so the YES bid is one minus that outcome's price."""
    ask = odds_price(outcome_odds(yes)) if takes_bets(condition, yes) else None
    no_ask = odds_price(outcome_odds(no)) if no is not None and takes_bets(condition, no) else None
    bid = round(1 - no_ask, PRICE_PLACES) if no_ask is not None else None
    mid = round((bid + ask) / 2, PRICE_PLACES) if bid is not None and ask is not None else None
    yes_quote = Quote(bid=bid, ask=ask, mid=mid)
    no_quote = Quote(
        bid=round(1 - ask, PRICE_PLACES) if ask is not None else None, ask=no_ask,
        mid=round(1 - mid, PRICE_PLACES) if mid is not None else None,
    )
    return yes_quote, no_quote


def seconds_ms(value: Any) -> int | None:
    """Unix seconds (as the feed and the subgraph write them) as milliseconds."""
    try:
        return int(Decimal(str(value)) * 1000)
    except (InvalidOperation, ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------------

GAME_DONE = ("Finished", "Canceled")


def status_of(condition: dict[str, Any], game: dict[str, Any] | None = None) -> tuple[str, str, bool]:
    """(normalized status, native word, taking bets) for a condition."""
    native = str(condition.get("state") or "")
    if native in ("Resolved", "Canceled"):
        return "settled", native.lower(), False
    game_state = str((game or {}).get("state") or "")
    if native == "Active" and game_state not in GAME_DONE:
        return "open", "active", True
    return "closed", native.lower() or game_state.lower(), False


def winner_of(condition: dict[str, Any], yes_id: str, no_id: str | None) -> str | None:
    """`yes`, `no` or `void` once the condition is settled, else `None`."""
    if str(condition.get("state") or "") == "Canceled":
        return "void"
    won = [str(w) for w in condition.get("wonOutcomeIds") or []]
    if not won:
        won = [str(o.get("outcomeId")) for o in condition.get("outcomes") or [] if str(o.get("state")) == "Won"]
    if not won:
        return None
    if yes_id in won:
        return "yes"
    if no_id is None or no_id in won:
        return "no"
    return None


def game_title(game: dict[str, Any] | None) -> str:
    game = game or {}
    if game.get("title"):
        return str(game["title"])
    names = [p.get("name") for p in game.get("participants") or [] if isinstance(p, dict) and p.get("name")]
    return " - ".join(names) if names else str(game.get("gameId") or "")


def tags_of(game: dict[str, Any] | None) -> list[str]:
    """Sport, league and country, lowercase, without duplicates."""
    game = game or {}
    words = [(game.get("sport") or {}).get("name"), (game.get("league") or {}).get("name"),
             (game.get("country") or {}).get("name")]
    seen: dict[str, None] = {}
    for word in words:
        if word and str(word).strip():
            seen.setdefault(str(word).strip().lower(), None)
    return list(seen)


def _game_summary(game: dict[str, Any] | None) -> dict[str, Any]:
    game = game or {}
    return {
        "game_id": game.get("gameId"), "title": game.get("title"), "state": game.get("state"),
        "starts_at": seconds_ms(game.get("startsAt")), "slug": game.get("slug"),
        "sport": (game.get("sport") or {}).get("slug"), "league": (game.get("league") or {}).get("slug"),
        "country": (game.get("country") or {}).get("slug"),
    }


def _market(
    condition: dict[str, Any], game: dict[str, Any] | None, *, native: str, title: str, description: str,
    yes: Outcome, no: Outcome, outcome_label: str, winner: str | None, extra: dict[str, Any],
) -> Market:
    status, native_status, accepting = status_of(condition, game)
    game = game or {}
    game_id = str(game.get("gameId") or (condition.get("game") or {}).get("gameId")
                  or game_of_condition(str(condition.get("conditionId"))) or "")
    tags = tags_of(game)
    league = game.get("league") or {}
    return Market(
        id=ids.qualify(VENUE, native),
        venue=VENUE,
        venue_market_id=native,
        event_id=ids.qualify(VENUE, game_id) if game_id else None,
        title=title,
        description=description,
        slug=None,
        yes=yes,
        no=no,
        status=status,  # type: ignore[arg-type]
        native_status=native_status,
        active=accepting,
        market_type="binary",
        open_timestamp=None, open_datetime=None, close_timestamp=None, close_datetime=None,
        resolution_timestamp=None, resolution_datetime=None,
        tick_size=None,
        face_value=1.0,
        book_model="shared_complement",
        stats=MarketStats(),
        url=None,
        image_url=None,
        category=str((game.get("sport") or {}).get("slug") or "") or None,
        tags=tags,
        series_id=str(league.get("id") or league.get("slug") or "") or None,
        outcome_label=outcome_label,
        neg_risk=False,
        settlement_sources=[],
        info={
            "condition": condition,
            "condition_id": str(condition.get("conditionId")),
            "condition_title": condition.get("title"),
            "category": condition.get("category"),
            "market_type_id": condition.get("marketId"),
            "hidden": condition.get("hidden"),
            "prematch_enabled": condition.get("isPrematchEnabled"),
            "live_enabled": condition.get("isLiveEnabled"),
            "combo_allowed": None if condition.get("isExpressForbidden") is None else not condition["isExpressForbidden"],
            "game": _game_summary(game),
            "game_time": seconds_ms(game.get("startsAt")),
            "winning_outcome": winner,
            **extra,
        },
    )


def normalize_condition(condition: dict[str, Any], game: dict[str, Any] | None = None) -> list[Market]:
    """A condition's Synpath markets: one for a two-outcome condition, one per
    outcome for a larger one. See the module docstring."""
    outcomes = outcome_order(condition)
    if len(outcomes) < 2:
        return []
    condition_id = str(condition["conditionId"])
    head = f"{game_title(game) or condition_id} — {condition.get('title') or 'Condition'}"
    if len(outcomes) == 2:
        one, two = outcomes
        yes_quote, no_quote = quote_of(condition, one, two)
        yes_id, no_id = str(one["outcomeId"]), str(two["outcomeId"])
        label_one, label_two = str(one.get("title") or "Yes"), str(two.get("title") or "No")
        return [_market(
            condition, game, native=market_native(condition_id),
            title=f"{head}: {label_one}",
            description=f"{condition.get('title') or 'Condition'}: {label_one} or {label_two}.",
            yes=Outcome(label=label_one, quote=yes_quote, info=_outcome_info(one)),
            no=Outcome(label=label_two, quote=no_quote, info=_outcome_info(two)),
            outcome_label=label_one, winner=winner_of(condition, yes_id, no_id),
            extra={"outcome_ids": {"yes": yes_id, "no": no_id}, "outcome_count": 2,
                   "odds": {"yes": outcome_odds(one), "no": outcome_odds(two)}},
        )]
    markets = []
    names = [str(o.get("title") or o["outcomeId"]) for o in outcomes]
    for outcome in outcomes:
        outcome_id = str(outcome["outcomeId"])
        label = str(outcome.get("title") or outcome_id)
        yes_quote, no_quote = quote_of(condition, outcome, None)
        markets.append(_market(
            condition, game, native=market_native(condition_id, outcome_id),
            title=f"{head}: {label}",
            description=f"{condition.get('title') or 'Condition'}: does {label} win? One of {len(outcomes)} "
                        f"outcomes ({', '.join(names)}).",
            yes=Outcome(label=label, quote=yes_quote, info=_outcome_info(outcome)),
            no=Outcome(label=f"Not {label}", quote=no_quote, info={"offered": False}),
            outcome_label=label, winner=winner_of(condition, outcome_id, None),
            extra={"outcome_ids": {"yes": outcome_id, "no": None}, "outcome_count": len(outcomes),
                   "odds": {"yes": outcome_odds(outcome), "no": None}},
        ))
    return markets


def _outcome_info(outcome: dict[str, Any]) -> dict[str, Any]:
    return {
        "outcome_id": str(outcome.get("outcomeId")), "odds": outcome_odds(outcome), "state": outcome.get("state"),
        "hidden": outcome.get("hidden"), "point": outcome.get("point"),
    }


def event_status(game: dict[str, Any], markets: list[Market]) -> str:
    state = str(game.get("state") or "")
    if state == "Canceled" or (markets and all(m.status == "settled" for m in markets)):
        return "settled"
    if state in OPEN_STATES and any(m.status == "open" for m in markets):
        return "open"
    if state in OPEN_STATES and not markets:
        return "open"
    return "closed"


def normalize_event(game: dict[str, Any], conditions: list[dict[str, Any]]) -> Event:
    """A game with its conditions' markets."""
    game_id = str(game.get("gameId") or game.get("id"))
    markets = [m for c in conditions for m in normalize_condition(c, game)]
    tags = tags_of(game)
    league = game.get("league") or {}
    return Event(
        id=ids.qualify(VENUE, game_id),
        venue=VENUE,
        venue_event_id=game_id,
        title=game_title(game),
        description=None,
        slug=game.get("slug"),
        markets=markets,
        status=event_status(game, markets),  # type: ignore[arg-type]
        native_status=str(game.get("state") or "").lower() or None,
        category=str((game.get("sport") or {}).get("slug") or "") or None,
        tags=tags,
        series_id=str(league.get("id") or league.get("slug") or "") or None,
        mutually_exclusive=None,
        close_timestamp=None,
        close_datetime=None,
        url=None,
        image_url=None,
        settlement_sources=[],
        info={
            **_game_summary(game),
            "turnover": game.get("turnover"),
            "participants": [p.get("name") for p in game.get("participants") or [] if isinstance(p, dict)],
            "game": game,
        },
    )


def normalize_trade(bet: dict[str, Any], *, market_id: str, yes_outcome: str, binary: bool) -> Trade | None:
    """One single bet from the subgraph, in the YES price with the bettor's
    side: backing the YES outcome at odds `O` buys YES at `1 / O`; backing
    the other outcome of a two-outcome condition sells YES at `1 - 1 / O`.
    The contracts are the bet's potential payout, one token each."""
    selections = bet.get("selections") or []
    if len(selections) != 1:
        return None
    outcome_id = str(((selections[0] or {}).get("outcome") or {}).get("outcomeId"))
    buying = outcome_id == yes_outcome
    if not buying and not binary:
        return None
    odds = odds_value(selections[0].get("odds") or bet.get("odds"))
    if odds is None:
        return None
    price = Decimal(1) / odds
    if not buying:
        price = Decimal(1) - price
    stamp = seconds_ms(bet.get("createdBlockTimestamp")) or 0
    payout = bet.get("potentialPayout")
    try:
        quantity = float(Decimal(str(payout))) if payout not in (None, "") else float(Decimal(str(bet["amount"])) * odds)
    except (InvalidOperation, KeyError, ValueError):
        quantity = 0.0
    return Trade(
        id=str(bet.get("id") or bet.get("betId") or stamp),
        market_id=market_id,
        timestamp=stamp,
        datetime=iso(stamp) or "",
        price=round(float(price), PRICE_PLACES),
        quantity=quantity,
        side="buy" if buying else "sell",
        info={k: v for k, v in bet.items() if k not in ("bettor", "actor", "owner")},
    )


def fee_schedule_of(market_id: str, *, chain: Chain | None = None) -> FeeSchedule:
    """No fee on top of the odds: the pool's margin is in them. A bet placed
    through the relayer pays it a flat fee for the transaction, read per bet."""
    chain = chain or CHAINS["polygon"]
    return FeeSchedule(
        venue=VENUE, scope="market", scope_id=market_id, fee_type="pool_margin",
        taker_rate=0.0, maker_rate=None,
        info={"currency": chain.symbol, "chain": chain.name,
              "charged": "the pool's margin is inside its quoted odds; the relayer takes a flat fee per bet "
                         "for the transaction (/bet/gas-info)"},
    )


# ---------------------------------------------------------------------------
# GraphQL
# ---------------------------------------------------------------------------

BETS_QUERY = """query Bets($first: Int!, $where: V3_Bet_filter!) {
  v3Bets(first: $first, where: $where, orderBy: createdBlockTimestamp, orderDirection: desc, subgraphError: allow) {
    id betId amount odds potentialPayout status result type createdBlockTimestamp createdTxHash
    selections { odds outcome { outcomeId condition { conditionId gameId } } }
  }
}"""


def bets_where(condition_id: str, cursor: str | None) -> dict[str, Any]:
    """The subgraph filter for one condition's single bets, older than the
    cursor. The cursor is `<seconds>:<ids at that second already returned>`,
    so bets sharing the boundary second are neither repeated nor lost."""
    where: dict[str, Any] = {"_conditionIds_contains": [condition_id], "type": "Ordinar"}
    if cursor:
        stamp, _, seen = cursor.partition(":")
        if not stamp.isdigit():
            raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
        where["createdBlockTimestamp_lte"] = stamp
        if seen:
            where["id_not_in"] = seen.split(",")
    return where


def next_bets_cursor(rows: list[dict[str, Any]], cursor: str | None, *, full: bool) -> str | None:
    if not full or not rows:
        return None
    oldest = min(int(r.get("createdBlockTimestamp") or 0) for r in rows)
    seen = [str(r["id"]) for r in rows if int(r.get("createdBlockTimestamp") or 0) == oldest]
    if cursor:
        stamp, _, before = cursor.partition(":")
        if stamp == str(oldest) and before:
            seen = before.split(",") + seen
    return f"{oldest}:{','.join(seen)}"


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class Azuro(Exchange):
    """Azuro, read-only. No key needed.

    ```python
    import synpath

    azuro = synpath.Azuro()                    # the Polygon pool; Azuro(chain="base") for Base
    markets = azuro.fetch_markets(limit=5)
    print(markets[0].title, markets[0].yes.quote.ask)
    ```
    """

    id = VENUE
    name = "Azuro"
    book_model = "shared_complement"
    has: dict[str, Capability] = {
        "fetch_markets": True,
        "fetch_events": True,
        "fetch_market": True,
        "fetch_markets_by_ids": True,
        # The feed orders games by start time or turnover, not markets.
        "sort": False,
        # A pool has no book: the quote as one level a side, sized by the
        # pool's limit on the outcome.
        "fetch_order_book": "partial",
        # One request per outcome for the limits.
        "fetch_order_books": True,
        # Every single bet, from the bets subgraph.
        "fetch_trades": True,
        # Built from that tape.
        "fetch_ohlcv": "partial",
        "fetch_series": False,
        "fetch_fee_schedule": True,
        "search": True,
        "match_market": False,
        "match_event": False,
    }

    def __init__(
        self, *, chain: str | int | Chain = "polygon", api_url: str | None = None, subgraph_url: str | None = None,
        timeout: float = 30.0, limiter: RateLimiter | None = LIMITER, client: Any = None,
    ):
        """`chain` picks the pool whose state and odds are read: `polygon`
        (USDT, the default), `base` (WETH), the testnets `polygon_amoy` and
        `base_sepolia`, or the discontinued `gnosis` and `chiliz`."""
        self.chain = chain_of(chain)
        client = client or httpx.Client(timeout=timeout, follow_redirects=True)
        self.http = HttpClient(api_url or self.chain.api_url, limiter=limiter, timeout=timeout, client=client, venue=VENUE)
        self.graph = HttpClient(subgraph_url or self.chain.subgraph_url, limiter=limiter, timeout=timeout,
                                client=client, venue=VENUE)

    @property
    def environment(self) -> str:
        return self.chain.environment

    # -- feed calls -----------------------------------------------------------

    def _games_page(self, state: str, page: int, per_page: int, **filters: Any) -> dict[str, Any]:
        data = self.http.get("/market-manager/games-by-filters", {
            "environment": self.environment, "gameState": state, "orderBy": "startsAt", "orderDirection": "asc",
            "perPage": max(GAMES_PAGE, min(per_page, MAX_PAGE_LIMIT)), "page": page,
            "sportSlug": filters.get("sport"), "leagueSlug": filters.get("league"),
        })
        return data if isinstance(data, dict) else {}

    def _conditions(self, game_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
        """Each game's conditions, in the feed's order, ten games a request."""
        out: dict[str, list[dict[str, Any]]] = {g: [] for g in game_ids}
        for start in range(0, len(game_ids), MAX_GAMES):
            chunk = game_ids[start:start + MAX_GAMES]
            data = self.http.post("/market-manager/conditions-by-game-ids", {
                "gameIds": chunk, "environment": self.environment, "extended": True,
            })
            for row in (data or {}).get("conditions") or [] if isinstance(data, dict) else []:
                if not isinstance(row, dict) or not row.get("conditionId"):
                    continue
                game_id = str((row.get("game") or {}).get("gameId") or game_of_condition(str(row["conditionId"])) or "")
                out.setdefault(game_id, []).append(row)
        return out

    def _games(self, game_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not game_ids:
            return {}
        data = self.http.post("/market-manager/games-by-ids", {"gameIds": game_ids})
        rows = (data or {}).get("games") or [] if isinstance(data, dict) else []
        return {str(g.get("gameId")): g for g in rows if isinstance(g, dict) and g.get("gameId")}

    def fetch_condition_states(self, condition_ids: list[str]) -> list[dict[str, Any]]:
        """Fresh state and odds of many conditions in one request, without
        their games (`/market-manager/condition-batch`)."""
        if not condition_ids:
            return []
        data = self.http.post("/market-manager/condition-batch", {
            "conditionIds": list(dict.fromkeys(condition_ids)), "environment": self.environment,
        })
        rows = (data or {}).get("conditions") or [] if isinstance(data, dict) else []
        return [r for r in rows if isinstance(r, dict) and r.get("conditionId")]

    def fetch_conditions(self, condition_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Many conditions by id, each as its game's condition listing has it
        (`/market-manager/conditions-by-game-ids`): state, outcomes and odds,
        the game, and `isExpressForbidden`. A condition whose id names no game
        is read from the condition batch, without those last two."""
        wanted = list(dict.fromkeys(str(c) for c in condition_ids))
        games = list(dict.fromkeys(g for g in (game_of_condition(c) for c in wanted) if g))
        found = {str(row["conditionId"]): row for rows in self._conditions(games).values() for row in rows
                 if str(row["conditionId"]) in wanted}
        missing = [c for c in wanted if c not in found]
        if missing:
            found.update({str(row["conditionId"]): row for row in self.fetch_condition_states(missing)})
        return {c: found[c] for c in wanted if c in found}

    def condition_outcomes(self, condition_ids: list[str]) -> dict[str, list[str]]:
        """Each condition's outcome ids, lowest first: what decides its
        markets' ids and which outcome is YES."""
        return {str(c["conditionId"]): [str(o["outcomeId"]) for o in outcome_order(c)]
                for c in self.fetch_condition_states(condition_ids)}

    def fetch_sports(self, *, sport_hub: str | None = None) -> list[dict[str, Any]]:
        """Sports, their countries and leagues, with how many games each has
        open, live and prematch (`/market-manager/navigation`), as the feed
        gives them. `sport_hub` is `sports` or `esports`."""
        data = self.http.get("/market-manager/navigation", {"environment": self.environment, "sportHub": sport_hub})
        return list((data or {}).get("sports") or []) if isinstance(data, dict) else []

    def fetch_limits(self, market_id: str, *, side: str = "buy", account: str | None = None) -> dict[str, Any]:
        """The pool's limits on one bet (`/bet/calculation`): `min_bet` and
        `max_bet` in the bet token, `max_payout` the most it pays out, which
        is the most contracts it sells. `side="sell"` asks for the NO outcome
        of a two-outcome market. `account` (a wallet address) makes `max_bet`
        that wallet's own."""
        condition_id, outcome_id = self._selection(market_id, side)
        body: dict[str, Any] = {"bets": [{"conditionId": condition_id, "outcomeId": int(outcome_id)}],
                                "environment": self.environment}
        if account:
            body["wallet"] = account
        data = self.http.post("/bet/calculation", body)
        raw = (data or {}).get("response", data) if isinstance(data, dict) else {}
        raw = raw if isinstance(raw, dict) else {}
        return {
            "condition_id": condition_id, "outcome_id": outcome_id, "currency": self.chain.symbol,
            "min_bet": _number(raw.get("minBet")), "max_bet": _number(raw.get("maxBet")),
            "max_payout": _number(raw.get("maxPayout")), "info": raw,
        }

    def _selection(self, market_id: str, side: str) -> tuple[str, str]:
        """(condition id, outcome id) a side of a market bets."""
        condition_id, outcome_id = parse_market_id(self.native(market_id))
        if outcome_id is not None:
            if side != "buy":
                raise BadRequest(f"{VENUE}: only YES is sold on one outcome of a {VENUE} condition; there is no NO to bet")
            return condition_id, outcome_id
        outcomes = self.condition_outcomes([condition_id]).get(condition_id)
        if not outcomes:
            raise MarketNotFound(f"{VENUE}: no condition {condition_id}")
        if len(outcomes) != 2:
            raise BadRequest(_one_per_outcome(condition_id, outcomes))
        return condition_id, outcomes[0] if side == "buy" else outcomes[1]

    # -- catalog --------------------------------------------------------------

    def fetch_markets(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open",
        sort: str | None = None, sport: str | None = None, league: str | None = None,
    ) -> Page[Market]:
        """One page of open markets, live games first, then upcoming ones by
        start time; `sport` and `league` are the feed's slugs (`football`,
        `premier-league`). With `query`, the markets of the games whose
        teams, league or country match (the first ten games)."""
        check_status(status)
        if status != "open":
            raise NotSupported(f"{VENUE}: the feed lists only open games; look up a settled market with fetch_market")
        check_sort(sort, venue=VENUE, supported=False)
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        if query:
            games = self._search(query, 1)["games"]
            conditions = self._conditions([str(g["gameId"]) for g in games])
            markets = [m for g in games for c in conditions.get(str(g["gameId"]), [])
                       for m in normalize_condition(c, g) if m.status == "open"]
            return Page(markets[:wanted], next_cursor=None)
        state, page, game_index, skip = _cursor(cursor, parts=4)
        out: list[Market] = []
        for _ in range(MAX_PAGES):
            data = self._games_page(state, page, GAMES_PAGE, sport=sport, league=league)
            games = [g for g in data.get("games") or [] if isinstance(g, dict) and g.get("gameId")]
            for index in range(game_index, len(games)):
                game = games[index]
                rows = self._conditions([str(game["gameId"])]).get(str(game["gameId"]), [])
                batch = [m for c in rows for m in normalize_condition(c, game) if m.status == "open"][skip:]
                room = wanted - len(out)
                out += batch[:room]
                if len(batch) > room:
                    return Page(out, next_cursor=f"{state}:{page}:{index}:{skip + room}")
                skip = 0
                if len(out) >= wanted:
                    following = _following(state, page, index + 1, len(games), data)
                    return Page(out, next_cursor=following)
            game_index = 0
            following = _following(state, page, len(games), len(games), data)
            if following is None:
                return Page(out, next_cursor=None)
            state, page, game_index, skip = _cursor(following, parts=4)
        return Page(out, next_cursor=f"{state}:{page}:0:0")

    def _search(self, query: str, page: int) -> dict[str, Any]:
        if len(query.strip()) < 3:
            raise BadRequest(f"{VENUE}: a search needs at least 3 characters")
        data = self.http.get("/market-manager/search", {
            "environment": self.environment, "request": query.strip(), "page": page, "perPage": GAMES_PAGE,
        })
        data = data if isinstance(data, dict) else {}
        data["games"] = [g for g in data.get("games") or [] if isinstance(g, dict) and g.get("gameId")]
        return data

    def fetch_events(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open", sport: str | None = None, league: str | None = None,
    ) -> Page[Event]:
        """Open games with every condition's markets, live games first, then
        upcoming ones by start time. With `query`, the games whose teams,
        league or country match, ten a page."""
        check_status(status)
        if status != "open":
            raise NotSupported(f"{VENUE}: the feed lists only open games; look one up with fetch_market")
        wanted = page_limit(limit) or GAMES_PAGE
        if query:
            page = int(cursor) if cursor and cursor.isdigit() else 1
            if cursor and not cursor.isdigit():
                raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
            data = self._search(query, page)
            games = data["games"][:wanted]
            conditions = self._conditions([str(g["gameId"]) for g in games])
            events = [normalize_event(g, conditions.get(str(g["gameId"]), [])) for g in games]
            more = int(data.get("totalPages") or 0) > page
            return Page(events, next_cursor=str(page + 1) if more else None)
        state, page, _, _ = _cursor(cursor, parts=2)
        per_page = max(GAMES_PAGE, wanted)
        data = self._games_page(state, page, per_page, sport=sport, league=league)
        games = [g for g in data.get("games") or [] if isinstance(g, dict) and g.get("gameId")][:wanted]
        conditions = self._conditions([str(g["gameId"]) for g in games])
        events = [normalize_event(g, conditions.get(str(g["gameId"]), [])) for g in games]
        following = _following(state, page, 0, 0, data)
        return Page(events, next_cursor=":".join(following.split(":")[:2]) if following else None)

    def iter_events(self, *, status: str = "open") -> Iterator[Event]:
        cursor: str | None = None
        for _ in range(MAX_PAGES):
            page = self.fetch_events(cursor=cursor, status=status)
            yield from page
            cursor = page.next_cursor
            if not cursor:
                return

    def fetch_market(self, market_id: str) -> Market:
        """One market by id, open or settled."""
        native = self.native(market_id)
        found = self.fetch_markets_by_ids([native])
        if found:
            return found[0]
        condition_id, outcome_id = parse_market_id(native)
        states = self.fetch_condition_states([condition_id])
        if not states:
            raise MarketNotFound(f"{VENUE}: no market {native}")
        outcomes = [str(o["outcomeId"]) for o in outcome_order(states[0])]
        if outcome_id is None and len(outcomes) != 2:
            raise BadRequest(_one_per_outcome(condition_id, outcomes))
        if outcome_id is not None and len(outcomes) == 2:
            raise BadRequest(f"{VENUE}: {condition_id} has two outcomes and is one market, {VENUE}:{condition_id}")
        raise MarketNotFound(f"{VENUE}: no market {native}")

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        """Many markets in the order asked for: each game's conditions are read
        once, ten games a request. Ids the feed does not know are left out."""
        natives = list(dict.fromkeys(self.native(m) for m in market_ids))
        wanted = {n: parse_market_id(n) for n in natives}
        games: dict[str, None] = {}
        orphans: list[str] = []
        for condition_id, _ in wanted.values():
            game_id = game_of_condition(condition_id)
            if game_id:
                games.setdefault(game_id, None)
            else:
                orphans.append(condition_id)
        by_id: dict[str, Market] = {}
        if games:
            conditions = self._conditions(list(games))
            details = self._games(list(games))
            for game_id, rows in conditions.items():
                for row in rows:
                    for market in normalize_condition(row, details.get(game_id)):
                        by_id[market.venue_market_id] = market
        missing = [c for n, (c, _) in wanted.items() if n not in by_id]
        if missing:
            rows = self.fetch_condition_states(list(dict.fromkeys(missing + orphans)))
            for row in rows:
                for market in normalize_condition(row, None):
                    by_id.setdefault(market.venue_market_id, market)
        return [by_id[n] for n in natives if n in by_id]

    def refresh_quotes(self, market: Market) -> Market:
        """`market` with its quotes read from the pool's current odds."""
        condition_id, outcome_id = parse_market_id(market.venue_market_id)
        rows = self.fetch_condition_states([condition_id])
        if not rows:
            raise MarketNotFound(f"{VENUE}: no condition {condition_id}")
        fresh = next((m for m in normalize_condition(rows[0], None) if m.venue_market_id == market.venue_market_id), None)
        if fresh is None:
            raise MarketNotFound(f"{VENUE}: no market {market.venue_market_id}")
        return market.model_copy(update={
            "yes": market.yes.model_copy(update={"quote": fresh.yes.quote}),
            "no": market.no.model_copy(update={"quote": fresh.no.quote}),
        })

    # -- market data ----------------------------------------------------------

    def fetch_order_book(
        self, market_id: str, *, side: BookSide = "yes", depth: int | None = None,
    ) -> OrderBook:
        """The pool's quote as a book: on YES an ask at `1 / O` of the YES
        outcome and, on a two-outcome market, a bid at one minus the price of
        the other, each sized by the pool's limit on that outcome in contracts
        (its maximum payout). `side="no"` mirrors it. Two requests for the
        odds and limits of each outcome quoted."""
        if side not in ("yes", "no"):
            raise BadRequest(f"{VENUE}: unknown side {side!r}; expected 'yes' or 'no'")
        native = self.native(market_id)
        condition_id, outcome_id = parse_market_id(native)
        rows = self.fetch_condition_states([condition_id])
        if not rows:
            raise MarketNotFound(f"{VENUE}: no order book for {native}: no condition {condition_id}")
        condition = rows[0]
        outcomes = outcome_order(condition)
        if outcome_id is None and len(outcomes) != 2:
            raise BadRequest(_one_per_outcome(condition_id, [str(o["outcomeId"]) for o in outcomes]))
        if outcome_id is None:
            yes, no = outcomes
        else:
            yes = next((o for o in outcomes if str(o["outcomeId"]) == outcome_id), None)
            no = None
            if yes is None:
                raise MarketNotFound(f"{VENUE}: no outcome {outcome_id} on condition {condition_id}")
        limits: dict[str, Any] = {}
        asks, bids = [], []
        ask = odds_price(outcome_odds(yes)) if takes_bets(condition, yes) else None
        if ask is not None:
            limits["yes"] = self.fetch_limits(native, side="buy")
            size = contracts_of_limits(limits["yes"], outcome_odds(yes))
            if size:
                asks.append(OrderLevel(price=ask, size=size))
        no_price = odds_price(outcome_odds(no)) if no is not None and takes_bets(condition, no) else None
        if no_price is not None:
            limits["no"] = self.fetch_limits(native, side="sell")
            size = contracts_of_limits(limits["no"], outcome_odds(no))
            if size:
                bids.append(OrderLevel(price=round(1 - no_price, PRICE_PLACES), size=size))
        derived = side == "no"
        if derived:
            bids, asks = (
                [OrderLevel(price=round(1 - level.price, PRICE_PLACES), size=level.size) for level in asks],
                [OrderLevel(price=round(1 - level.price, PRICE_PLACES), size=level.size) for level in bids],
            )
        return OrderBook(
            market_id=self.qualify(native), side=side, venue=VENUE, bids=bids, asks=asks,
            timestamp=None, datetime=None, book_model="shared_complement", derived=derived,
            depth_scope="top_n",
            info={"pool": True, "condition_state": condition.get("state"),
                  "odds": {"yes": outcome_odds(yes), "no": outcome_odds(no) if no is not None else None},
                  "limits": {k: {kk: vv for kk, vv in v.items() if kk != "info"} for k, v in limits.items()}},
        )

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        """Books for many markets, three requests or fewer each."""
        return {self.qualify(self.native(m)): self.fetch_order_book(m, side=side, depth=depth)
                for m in dict.fromkeys(market_ids)}

    def _tape(self, condition_id: str, *, first: int, cursor: str | None) -> tuple[list[dict[str, Any]], str | None]:
        data = self.graph.post("", {"query": BETS_QUERY, "variables": {
            "first": first, "where": bets_where(condition_id, cursor),
        }})
        if isinstance(data, dict) and data.get("errors"):
            raise ExchangeError(f"{VENUE}: bets subgraph: {data['errors']}", body=data)
        rows = list(((data or {}).get("data") or {}).get("v3Bets") or []) if isinstance(data, dict) else []
        return rows, next_bets_cursor(rows, cursor, full=len(rows) >= first)

    def _trades_of(self, native: str, rows: list[dict[str, Any]], outcomes: list[str]) -> list[Trade]:
        _, outcome_id = parse_market_id(native)
        binary = outcome_id is None
        yes = outcomes[0] if binary and outcomes else str(outcome_id)
        trades = [normalize_trade(row, market_id=self.qualify(native), yes_outcome=yes, binary=binary) for row in rows]
        return [t for t in trades if t is not None]

    def _outcomes_for_tape(self, native: str) -> list[str]:
        condition_id, outcome_id = parse_market_id(native)
        if outcome_id is not None:
            return [outcome_id]
        outcomes = self.condition_outcomes([condition_id]).get(condition_id) or []
        if len(outcomes) > 2:
            raise BadRequest(_one_per_outcome(condition_id, outcomes))
        return outcomes

    def fetch_trades(
        self, market_id: str, *, since: int | None = None, limit: int | None = None,
        cursor: str | None = None,
    ) -> Page[Trade]:
        """The market's single bets from the bets subgraph, a page newest
        first and returned oldest first, in the YES price with the bettor's
        side. Combo bets are left out: no one price of theirs is this market's."""
        native = self.native(market_id)
        condition_id, _ = parse_market_id(native)
        outcomes = self._outcomes_for_tape(native)
        rows, following = self._tape(condition_id, first=page_limit(limit) or 50, cursor=cursor)
        trades = self._trades_of(native, rows, outcomes)
        if since:
            trades = [t for t in trades if t.timestamp >= since]
        return Page(sorted(trades, key=lambda t: t.timestamp), next_cursor=following)

    def fetch_ohlcv(
        self, market_id: str, *, timeframe: str = "1h", since: int | None = None,
        until: int | None = None, limit: int | None = None,
    ) -> list[Candle]:
        """Bars in the YES price built from the market's single bets, with
        their volume in contracts (`price_source="trade"`). The tape is read
        back page by page until `since`, at most 2,000 bets."""
        from .sx_bet import candles_from_trades

        native = self.native(market_id)
        condition_id, _ = parse_market_id(native)
        outcomes = self._outcomes_for_tape(native)
        seconds = timeframe_seconds(timeframe)
        trades: list[Trade] = []
        cursor = None
        for _ in range(MAX_TRADE_PAGES):
            rows, cursor = self._tape(condition_id, first=TRADES_PAGE, cursor=cursor)
            batch = self._trades_of(native, rows, outcomes)
            trades += batch
            oldest = min((int(r.get("createdBlockTimestamp") or 0) * 1000 for r in rows), default=0)
            if not cursor or (since and rows and oldest < since):
                break
        candles = candles_from_trades(trades, interval_seconds=seconds)
        if since:
            candles = [c for c in candles if c.timestamp + seconds * 1000 > since]
        if until:
            candles = [c for c in candles if c.timestamp <= until]
        return pick_bars(candles, since=since, limit=limit)

    # -- reference ------------------------------------------------------------

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        return fee_schedule_of(self.native(market_id), chain=self.chain)

    def close(self) -> None:
        self.http.close()


def contracts_of_limits(limits: dict[str, Any], odds: Any) -> float | None:
    """The most contracts the pool sells on one outcome: its maximum payout,
    or its maximum stake times the odds when that is smaller."""
    sizes = []
    if limits.get("max_payout") is not None:
        sizes.append(float(limits["max_payout"]))
    price = odds_value(odds)
    if limits.get("max_bet") is not None and price is not None:
        sizes.append(float(Decimal(str(limits["max_bet"])) * price))
    sizes = [s for s in sizes if s > 0]
    return round(min(sizes), 6) if sizes else None


def _number(value: Any) -> float | None:
    try:
        return float(Decimal(str(value))) if value not in (None, "") else None
    except (InvalidOperation, ValueError):
        return None


def _one_per_outcome(condition_id: str, outcomes: list[str]) -> str:
    names = ", ".join(f"{VENUE}:{condition_id}-{o}" for o in outcomes)
    return f"{VENUE}: condition {condition_id} has {len(outcomes)} outcomes, one market each: {names}"


def _cursor(cursor: str | None, *, parts: int) -> tuple[str, int, int, int]:
    """`state:page[:game:skip]`: the game state being walked (`Live`, then
    `Prematch`), its 1-based page, and how far into that page the last call got."""
    if not cursor:
        return OPEN_STATES[0], 1, 0, 0
    pieces = cursor.split(":")
    if len(pieces) != parts or pieces[0] not in OPEN_STATES or not all(p.isdigit() for p in pieces[1:]):
        raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
    numbers = [int(p) for p in pieces[1:]] + [0] * (3 - (parts - 1))
    return pieces[0], max(1, numbers[0]), numbers[1], numbers[2]


def _following(state: str, page: int, index: int, count: int, data: dict[str, Any]) -> str | None:
    """Where the walk goes next: the rest of this page, the next page, or the
    next game state once this one's pages are done."""
    if index < count:
        return f"{state}:{page}:{index}:0"
    total = int(data.get("totalPages") or 0)
    if page < total:
        return f"{state}:{page + 1}:0:0"
    position = OPEN_STATES.index(state)
    if position + 1 < len(OPEN_STATES):
        return f"{OPEN_STATES[position + 1]}:1:0:0"
    return None
