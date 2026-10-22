"""The combo board: a venue's combo-eligible markets as games and questions.

A combo venue lists the markets a combo may take as legs, but not in a shape
a person can pick from: Polymarket's catalogue is a flat list by volume,
Polymarket US nests markets in events, and Kalshi lists eligibility by
collection (events only). `board_of(...)` turns those rows into what a combo
builder draws (polymarket.com's "Combos", Kalshi's combo builder):

* a **game**: an event with two (or three, with a draw) participants and, per
  participant, a moneyline, a spread and a total pick where the venue lists
  that market type. A pick is one market and the outcome that backs it: a
  Polymarket moneyline "Patriots vs. Bears" is YES for the Patriots and NO
  for the Bears; a total is YES for the over and NO for the under; Kalshi's
  game event has one YES market per team;
* a **question**: any other eligible market, with a YES and a NO pick;
* **categories**: leagues (NFL, NBA, Soccer...) and other kinds (Crypto,
  Politics), each with the number of events on the board.

Everything is read from the venue's own fields, never from URL slugs or
title wording: market types (Polymarket's Gamma `sportsMarketType`, Polymarket
US's `marketType`, Kalshi's event `product_metadata.competition_scope`),
participants (Gamma's and the US gateway's event `teams` with their
abbreviations, the US `marketSides[].team`, Kalshi's `yes_sub_title`,
`custom_strike` and event `sub_title` codes), lines (Gamma's `line`, Kalshi's
`floor_strike`), tags and series. A participant with no venue abbreviation
shows its name. What cannot be placed in a game is a question, so a market
is never dropped for not fitting. Prices on
a pick are the catalogue's (`yes_bid`, `yes_ask`; `price` is what the pick's
outcome costs now: the YES ask, or 1 - the YES bid), for a caller to replace
with live books where it has them.
"""
from __future__ import annotations

import asyncio
import gc
import inspect
import re
import time
from decimal import Decimal
from typing import Any, Awaitable, Callable, Iterable, Literal

from pydantic import Field

from ..types import Event, Market, _Base

MarketKind = Literal["moneyline", "spread", "total"]
KINDS: tuple[MarketKind, ...] = ("moneyline", "spread", "total")

# League and category, from a venue's tags or a Kalshi series ticker. The first
# match wins, so more specific names come first.
LEAGUES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    # key, label, words (tag slugs, tag labels, Kalshi series prefixes after "KX")
    ("cfb", "College Football", ("cfb", "ncaaf", "college-football", "college football")),
    ("nfl", "NFL", ("nfl",)),
    ("cbb", "College Basketball", ("cbb", "ncaab", "college-basketball", "college basketball")),
    ("nba", "NBA", ("nba",)),
    ("wnba", "WNBA", ("wnba",)),
    ("nhl", "NHL", ("nhl",)),
    ("mlb", "MLB", ("mlb",)),
    ("tennis", "Tennis", ("atp", "wta", "tennis")),
    ("soccer", "Soccer", ("soccer", "epl", "ucl", "uel", "lal", "laliga", "la-liga", "bundesliga", "seriea",
                          "serie-a", "ligue-1", "mls", "uefa", "fifa", "premier-league", "champions-league",
                          "europa-league")),
    ("ufc", "UFC", ("ufc", "mma")),
    ("golf", "Golf", ("golf", "pga")),
    ("esports", "Esports", ("esports", "lol", "csgo", "cs2", "dota", "valorant")),
    ("crypto", "Crypto", ("crypto", "btc", "bitcoin", "eth", "ethereum", "sol", "solana", "xrp", "doge")),
    ("politics", "Politics", ("politics", "elections", "election", "trump", "midterms", "congress")),
    ("economics", "Economics", ("economics", "economy", "fed", "cpi", "inflation", "gdp", "jobs")),
)
SPORTS = frozenset({"cfb", "nfl", "cbb", "nba", "wnba", "nhl", "mlb", "tennis", "soccer", "ufc", "golf", "esports"})
LABELS = {key: label for key, label, _ in LEAGUES}
OTHER = ("other", "Other")

QUESTIONS_PER_EVENT = 3
"""A question event (a price bracket, a field of candidates) shows at most this
many markets: those priced nearest even, where a choice is live."""



class BoardPick(_Base):
    """One price button: a market and the outcome that backs the pick."""

    market_id: str
    outcome: Literal["yes", "no"]
    label: str = Field(description="What the button says: `CHI`, `CHI -2.5`, `O 41.5`, `YES`")
    kind: str = Field(description="`moneyline`, `spread`, `total` or `question`")
    line: str | None = None
    title: str | None = Field(default=None, description="The market's own title")
    yes_bid: Decimal | None = None
    yes_ask: Decimal | None = None
    price: Decimal | None = Field(default=None, description="What one contract of this pick costs now "
                                                            "(catalogue): YES ask, or 1 - YES bid")


class BoardRow(_Base):
    """One participant of a game and its picks by market type."""

    name: str
    abbr: str | None = None
    alias: str | None = Field(default=None, description="The venue's short name for the participant, where it has "
                                                        "one (Gamma's team `alias`: `Al Fayha`)")
    picks: dict[str, BoardPick | None] = Field(default_factory=dict, description="By `moneyline`, `spread`, `total`")


class BoardEvent(_Base):
    """A game (participants x market types) or a question (YES / NO)."""

    id: str
    venue: str
    kind: Literal["game", "question"]
    title: str
    category: str = Field(description="The category key: a league (`nfl`) or a kind (`crypto`), else `other`")
    category_label: str
    group: Literal["sports", "other"]
    start_time: int | None = Field(default=None, description="Game start or market close, ms")
    volume: Decimal | None = None
    market_types: list[str] = Field(default_factory=list, description="The game's market types, in board order")
    rows: list[BoardRow] = Field(default_factory=list, description="A game's participants")
    yes: BoardPick | None = Field(default=None, description="A question's YES pick")
    no: BoardPick | None = Field(default=None, description="A question's NO pick")
    market_ids: list[str] = Field(default_factory=list, description="Every market behind the event's picks")


class BoardCategory(_Base):
    key: str
    label: str
    group: Literal["sports", "other"]
    count: int = Field(description="Events on the board in this category")


BoardStatus = Literal["building", "ready"]


class ComboBoard(_Base):
    """A venue's combo-eligible markets, grouped for a builder: the whole
    board, or one page of one category of it (`category`, `next_cursor`)."""

    venue: str
    count: int = Field(description="Combo-eligible markets read")
    events: list[BoardEvent] = Field(default_factory=list)
    categories: list[BoardCategory] = Field(default_factory=list, description="By count, sports first")
    complete: bool = Field(default=True, description="Every page of the venue's list was read")
    as_of: int | None = None
    status: BoardStatus = Field(default="ready", description="`building` while serve's first build of this venue's "
                                                             "board runs (no events yet), else `ready`")
    built_at: int | None = Field(default=None, description="When the board served was built, ms")
    category: str | None = Field(default=None, description="A page: the category it is of (`all` for every one)")
    total: int | None = Field(default=None, description="A page: the events in its category (and search)")
    next_cursor: str | None = Field(default=None, description="A page: the cursor of the next one; null at the end")
    error: str | None = Field(default=None, description="Why serve's last build of this board failed, while the "
                                                        "board served is an older one (or none yet)")


class ComboBoardCategories(_Base):
    """A board's rail: its categories with counts, without the events."""

    venue: str
    status: BoardStatus = "ready"
    built_at: int | None = None
    complete: bool = True
    count: int = Field(default=0, description="Combo-eligible markets read")
    events: int = Field(default=0, description="Events on the board")
    categories: list[BoardCategory] = Field(default_factory=list, description="By count, sports first")
    error: str | None = None


# ---------------------------------------------------------------------------
# Building a board
# ---------------------------------------------------------------------------

class Side(_Base):
    """A game's participant as the venue names it: a team, a player, or the draw."""

    key: str = Field(description="The participant's identity within the event: the venue's team id where it has "
                                 "one, else its name")
    name: str
    abbr: str | None = Field(default=None, description="The venue's own abbreviation, where it has one")
    alias: str | None = Field(default=None, description="The venue's own short name, where it has one")
    draw: bool = False


class BoardMarket(_Base):
    """One eligible market, in the terms the board needs, whatever the venue.
    Every field comes from the venue's own fields (`*_markets` below)."""

    market_id: str
    title: str
    event_key: str = Field(description="Markets of one game share this")
    event_title: str | None = None
    kind: str | None = Field(default=None, description="`moneyline`, `spread`, `total` from the venue's market "
                                                       "type, else None (a question)")
    outcomes: list[str] = Field(default_factory=list, description="The venue's outcome names, YES first")
    label: str | None = Field(default=None, description="The market's own short label (Kalshi `yes_sub_title`, "
                                                        "Polymarket `groupItemTitle`)")
    participants: list[Side] = Field(default_factory=list, description="The event's participants, in the "
                                                                       "venue's order")
    yes_side: Side | None = Field(default=None, description="Who the YES outcome backs")
    no_side: Side | None = Field(default=None, description="Who the NO outcome backs (a two-sided market)")
    line: Decimal | None = Field(default=None, description="A spread's line for the YES side (-2.5: it gives "
                                                           "2.5), a total's line")
    tags: list[str] = Field(default_factory=list)
    series: str | None = None
    yes_bid: Decimal | None = None
    yes_ask: Decimal | None = None
    start_time: int | None = None
    volume: Decimal | None = None


# The builders below run in worker threads beside serve's event loop
# (`board_for`). Under the GIL a thread building the whole catalogue starves
# the loop for seconds: health checks and order routes waited 1-6 s. Sleeping
# 1 ms every `LOCK_SHARE_EVERY` items hands the loop the lock on a fixed beat,
# at the cost of a slower background build.
LOCK_SHARE_EVERY = 100
LOCK_SHARE_S = 0.001


def share_the_lock(items_done: int) -> None:
    if items_done % LOCK_SHARE_EVERY == 0:
        time.sleep(LOCK_SHARE_S)


def board_of(venue: str, markets: Iterable[BoardMarket], *, complete: bool = True,
             as_of: int | None = None) -> ComboBoard:
    """Games and questions from a venue's eligible markets (already
    normalised as `BoardMarket`s: `polymarket_markets`, `polymarket_us_markets`,
    `kalshi_markets`)."""
    rows = [m for m in markets if m.market_id]
    by_event: dict[str, list[BoardMarket]] = {}
    for market in rows:
        by_event.setdefault(market.event_key, []).append(market)
    events: list[BoardEvent] = []
    for events_done, (key, members) in enumerate(by_event.items(), 1):
        share_the_lock(events_done)
        game = game_of(venue, key, members)
        if game is not None:
            events.append(game)
            # A game's other lines (spreads and totals not nearest even) stay off the board.
            members = [m for m in members if m.kind not in KINDS]
        events.extend(questions_of(venue, members))
    events.sort(key=lambda e: (e.group != "sports", e.category_label, e.start_time or 2**62, e.title))
    return ComboBoard(venue=venue, count=len(rows), events=events, categories=categories_of(events),
                      complete=complete, as_of=as_of)


def categories_of(events: list[BoardEvent]) -> list[BoardCategory]:
    counts: dict[str, BoardCategory] = {}
    for event in events:
        held = counts.get(event.category)
        if held is None:
            counts[event.category] = BoardCategory(key=event.category, label=event.category_label,
                                                   group=event.group, count=1)
        else:
            held.count += 1
    return sorted(counts.values(), key=lambda c: (c.group != "sports", -c.count, c.label))


def game_of(venue: str, key: str, members: list[BoardMarket]) -> BoardEvent | None:
    """The game these markets make, if any is a moneyline with participants."""
    typed = [m for m in members if m.kind in KINDS]
    moneylines = [m for m in typed if m.kind == "moneyline"]
    if not moneylines:
        return None
    sides = participants(typed)
    if len([s for s in sides if not s.draw]) < 2:
        return None
    first = members[0]
    category, label, group = category_of([t for m in members for t in m.tags], first.series)
    rows = [BoardRow(name=s.name, abbr=s.abbr, alias=s.alias) for s in sides]
    used: list[str] = []
    for kind in KINDS:
        for row, pick in zip(rows, picks_for(kind, [m for m in typed if m.kind == kind], sides)):
            row.picks[kind] = pick
            if pick is not None and pick.market_id not in used:
                used.append(pick.market_id)
    kinds = [k for k in KINDS if any(r.picks.get(k) for r in rows)]
    return BoardEvent(
        id=key, venue=venue, kind="game", title=first.event_title or " vs. ".join(s.name for s in sides),
        category=category, category_label=label, group=group,
        start_time=min((m.start_time for m in members if m.start_time), default=None),
        volume=sum((m.volume for m in members if m.volume is not None), Decimal(0)) or None,
        market_types=kinds, rows=rows, market_ids=used)


def questions_of(venue: str, members: list[BoardMarket]) -> list[BoardEvent]:
    """YES / NO cards: at most `QUESTIONS_PER_EVENT` of an event's markets,
    the nearest even first. A market whose outcomes are named (`Cavaliers` /
    `76ers`, `Odd` / `Even`) says them on its buttons."""
    def evenness(m: BoardMarket) -> Decimal:
        price = mid(m)
        return abs(price - Decimal("0.5")) if price is not None else Decimal(1)

    out = []
    for market in sorted(members, key=evenness)[:QUESTIONS_PER_EVENT]:
        category, label, group = category_of(market.tags, market.series)
        title = market.title
        if market.label and market.label.lower() not in title.lower():
            title = f"{title}: {market.label}"
        named = len(market.outcomes) == 2 and not yes_no(market.outcomes)
        yes_label, no_label = (market.outcomes[0], market.outcomes[1]) if named else ("YES", "NO")
        out.append(BoardEvent(
            id=market.market_id, venue=venue, kind="question", title=title, category=category,
            category_label=label, group=group, start_time=market.start_time, volume=market.volume,
            yes=pick_of(market, "yes", yes_label, "question"), no=pick_of(market, "no", no_label, "question"),
            market_ids=[market.market_id]))
    return out


def participants(typed: list[BoardMarket]) -> list[Side]:
    """A game's participants, in the venue's order: the event's own list
    (Polymarket's and Polymarket US's teams, Kalshi's title and codes) where
    a moneyline backs them, else the moneylines' sides; a draw last."""
    moneylines = [m for m in typed if m.kind == "moneyline"]
    backed: dict[str, Side] = {}
    for market in moneylines:
        for side in (market.yes_side, market.no_side):
            if side is not None:
                backed.setdefault(side.key, side)
    listed = next((m.participants for m in typed if m.participants), [])
    out = [s for s in listed if s.key in backed and not s.draw]
    out += [s for key, s in backed.items() if not s.draw and key not in {o.key for o in out}]
    return out + [s for s in backed.values() if s.draw][:1]


def picks_for(kind: str, markets: list[BoardMarket], sides: list[Side]) -> list[BoardPick | None]:
    """One pick per participant for this market type (None where the venue has none)."""
    out: list[BoardPick | None] = [None] * len(sides)
    if not markets:
        return out
    if kind == "total":
        market = nearest_even(markets)
        over, under = (market.outcomes[0], market.outcomes[1]) if len(market.outcomes) == 2 \
            and not yes_no(market.outcomes) else ("Over", "Under")
        text = f" {format_line(abs(market.line))}" if market.line is not None else ""
        out[0] = pick_of(market, "yes", f"{short(over)}{text}", kind)
        if len(sides) > 1:
            out[1] = pick_of(market, "no", f"{short(under)}{text}", kind)
        return out
    index = {s.key: i for i, s in enumerate(sides)}
    two_sided = [m for m in markets if m.yes_side is not None and m.no_side is not None
                 and m.yes_side.key in index and m.no_side.key in index]
    if two_sided:
        market = nearest_even(two_sided)
        for outcome, side in (("yes", market.yes_side), ("no", market.no_side)):
            line = market.line if outcome == "yes" else (-market.line if market.line is not None else None)
            at = index[side.key]   # type: ignore[union-attr]
            out[at] = pick_of(market, outcome, button_label(kind, sides[at], line), kind)  # type: ignore[arg-type]
        return out
    for at, side in enumerate(sides):
        mine = [m for m in markets if m.yes_side is not None and m.yes_side.key == side.key]
        if mine:
            market = nearest_even(mine)
            out[at] = pick_of(market, "yes", button_label(kind, side, market.line), kind)
    return out


def button_label(kind: str, side: Side, line: Decimal | None) -> str:
    """`CHI`, `CHI -2.5`, `NE +2.5`; a participant with no venue abbreviation
    by its name (the page truncates it)."""
    name = side.abbr or side.name
    if kind == "spread" and line is not None:
        return f"{name} {'+' if line > 0 else ''}{format_line(line)}"
    return name


def pick_of(market: BoardMarket, outcome: Literal["yes", "no"], label: str, kind: str) -> BoardPick:
    price = market.yes_ask if outcome == "yes" else (1 - market.yes_bid if market.yes_bid is not None else None)
    return BoardPick(market_id=market.market_id, outcome=outcome, label=label, kind=kind,
                     line=format_line(market.line) if market.line is not None else None, title=market.title,
                     yes_bid=market.yes_bid, yes_ask=market.yes_ask,
                     price=price if price is not None and 0 < price < 1 else None)


# ---------------------------------------------------------------------------
# Venue rows -> BoardMarket
# ---------------------------------------------------------------------------

# Polymarket (Gamma `sportsMarketType`) and Polymarket US (`marketType`,
# `sportsMarketTypeV2`): the full-game types a game card draws. Every other
# type (a half, a map, an exact score, a player prop) is a question.
POLYMARKET_KINDS = {"moneyline": "moneyline", "spreads": "spread", "totals": "total"}
POLYMARKET_US_KINDS = {"moneyline": "moneyline", "drawable_outcome": "moneyline", "spread": "spread",
                       "spreads": "spread", "total": "total", "totals": "total",
                       "SPORTS_MARKET_TYPE_MONEYLINE": "moneyline",
                       "SPORTS_MARKET_TYPE_DRAWABLE_OUTCOME": "moneyline",
                       "SPORTS_MARKET_TYPE_SPREAD": "spread", "SPORTS_MARKET_TYPE_TOTAL": "total"}
# Kalshi: the event's `product_metadata.competition_scope` (the series'
# `product_metadata.scope` says the same).
KALSHI_SCOPES = {"game": "moneyline", "match": "moneyline", "fight": "moneyline", "spread": "spread",
                 "point total": "total", "total": "total"}
DRAW_WORDS = ("draw", "tie")


def polymarket_markets(rows: Iterable[Any], details: dict[str, dict[str, Any]] | None = None,
                       events: dict[str, dict[str, Any]] | None = None) -> list[BoardMarket]:
    """Polymarket's catalogue rows (`ComboMarket`, `info` the catalogue's own
    row), with Gamma's market (`details`, by market id: `sportsMarketType`,
    `line`, `groupItemTitle`, `gameStartTime`, `events`) and event (`events`,
    by event id: `teams` with `abbreviation`, `alias`, `ordering`) where read.
    Without Gamma's market a row is a question: the catalogue names no type."""
    details, events = details or {}, events or {}
    out = []
    for rows_done, row in enumerate(rows, 1):
        share_the_lock(rows_done)
        info = dict(getattr(row, "info", None) or {})
        native = str(row.market_id).partition(":")[2] or str(row.market_id)
        gamma = details.get(native) or {}
        event = (gamma.get("events") or [{}])[0] if isinstance(gamma.get("events"), list) else {}
        event_id = str(event.get("id") or "")
        teams = teams_of((events.get(event_id) or {}).get("teams") or event.get("teams") or [])
        outcomes = outcome_names(gamma.get("outcomes") or info.get("outcomes"))
        prices = [to_decimal(p) for p in outcome_names(gamma.get("outcomePrices") or info.get("outcome_prices"))]
        kind = POLYMARKET_KINDS.get(str(gamma.get("sportsMarketType") or ""))
        label = gamma.get("groupItemTitle") or None
        yes_side, no_side = listed_sides(outcomes, label, teams)
        title = str(gamma.get("question") or row.title or info.get("title") or row.market_id)
        out.append(BoardMarket(
            market_id=row.market_id, title=title,
            event_key=f"polymarket-event:{event_id}" if event_id else str(row.market_id),
            event_title=event.get("title") or None, kind=kind if yes_side is not None or kind == "total" else None,
            outcomes=outcomes, label=label, participants=teams, yes_side=yes_side, no_side=no_side,
            line=to_decimal(gamma.get("line")), tags=[str(t) for t in row.tags or []],
            series=event.get("seriesSlug") or None,
            yes_bid=to_decimal(gamma.get("bestBid")) or (prices[0] if prices and prices[0] else None),
            yes_ask=to_decimal(gamma.get("bestAsk")) or (prices[0] if prices and prices[0] else None),
            start_time=millis(gamma.get("gameStartTime") or event.get("startTime") or gamma.get("endDate")),
            volume=to_decimal(info.get("volume") or gamma.get("volume")),
        ))
    return out


def polymarket_us_markets(rows: Iterable[Any]) -> list[BoardMarket]:
    """Polymarket US's eligible rows (`ComboMarket`, `info` the gateway's
    market with its `event`): the type from `marketType` /
    `sportsMarketTypeV2`, each outcome's participant from `marketSides[].team`
    (`name`, `displayAbbreviation`), the event's `teams` in order."""
    out = []
    for rows_done, row in enumerate(rows, 1):
        share_the_lock(rows_done)
        info = dict(getattr(row, "info", None) or {})
        event = info.get("event") if isinstance(info.get("event"), dict) else {}
        teams = teams_of(event.get("teams") or [], abbr_field="displayAbbreviation")
        outcomes = outcome_names(info.get("outcomes"))
        prices = [to_decimal(p) for p in outcome_names(info.get("outcomePrices"))]
        kind = POLYMARKET_US_KINDS.get(str(info.get("marketType") or "")) \
            or POLYMARKET_US_KINDS.get(str(info.get("sportsMarketTypeV2") or ""))
        sides = [side_of_team(s.get("team"), teams) for s in info.get("marketSides") or [] if isinstance(s, dict)]
        yes_side = no_side = None
        if kind == "moneyline" and yes_no(outcomes):
            # A drawable result: one market per team, and one for the draw (no team).
            yes_side = sides[0] if sides and sides[0] is not None else Side(key="draw", name="Draw", draw=True)
        elif kind in ("moneyline", "spread") and len(sides) == 2:
            yes_side, no_side = sides
        best_bid, best_ask = info.get("bestBidQuote") or {}, info.get("bestAskQuote") or {}
        out.append(BoardMarket(
            market_id=row.market_id, title=str(info.get("title") or row.title or info.get("question") or row.market_id),
            event_key=row.event_id or str(row.market_id), event_title=row.event_title or event.get("title"),
            kind=kind if yes_side is not None or kind == "total" else None, outcomes=outcomes,
            label=info.get("titleShort") or None, participants=teams, yes_side=yes_side, no_side=no_side,
            line=to_decimal(info.get("line")), tags=[str(t) for t in row.tags or []],
            series=info.get("seriesSlug") or event.get("seriesSlug"),
            yes_bid=to_decimal(best_bid.get("value") if isinstance(best_bid, dict) else None)
            or (prices[0] if prices and prices[0] else None),
            yes_ask=to_decimal(best_ask.get("value") if isinstance(best_ask, dict) else None)
            or (prices[0] if prices and prices[0] else None),
            start_time=millis(info.get("gameStartTime") or event.get("startTime") or info.get("endDate")),
            volume=to_decimal(info.get("volume")),
        ))
    return out


def listed_markets(rows: Iterable[Any]) -> list[BoardMarket]:
    """A listed venue's rows, Polymarket US's or Polymarket's (no Gamma details)."""
    rows = list(rows)
    us = [r for r in rows if getattr(r, "venue", "") == "polymarket_us"]
    return polymarket_us_markets(us) + polymarket_markets([r for r in rows if r not in us])


def kalshi_markets(events: Iterable[Event]) -> list[BoardMarket]:
    """Kalshi events (with their markets) a combo collection takes. A game's
    events (winner, spread, total) share the event's `sub_title` within one
    competition (`NE vs CHI (Oct 22)`); the type is the event's
    `product_metadata.competition_scope`; a market's participant is its
    `custom_strike` (the team's id) and `yes_sub_title`; codes are the event
    `sub_title`'s, side by side with the title's names (`New England vs
    Chicago`); a spread's and a total's line is the market's `floor_strike`."""
    out = []
    for events_done, event in enumerate(events, 1):
        share_the_lock(events_done)
        raw = dict(event.info or {})
        meta = raw.get("product_metadata") or {}
        kind = KALSHI_SCOPES.get(str(meta.get("competition_scope") or "").strip().lower())
        sub = str(raw.get("sub_title") or "")
        teams = kalshi_sides(event.title, sub)
        series = str(raw.get("series_ticker") or event.venue_event_id.partition("-")[0])
        game = f"kalshi-game:{meta.get('competition') or series}:{sub}" if kind and sub else event.id
        tags = [*(event.tags or []), *([event.category] if event.category else [])]
        rows = []
        for market in event.markets:
            if market.status not in ("open", "active") and not market.active:
                continue
            info = dict(market.info or {})
            label = info.get("yes_sub_title") or getattr(market, "outcome_label", None) or None
            side = kalshi_side_of(info, label, teams) if kind in ("moneyline", "spread") else None
            floor = to_decimal(info.get("floor_strike"))
            line = -floor if kind == "spread" and floor is not None else floor if kind == "total" else None
            quote = market.yes.quote
            rows.append(BoardMarket(
                market_id=market.id, title=market.title, event_key=game, event_title=game_title(event.title),
                kind=kind if side is not None or kind == "total" else None,
                label=label if label and label.lower() not in ("yes", "no") else None,
                participants=teams, yes_side=side, line=line, tags=tags,
                series=series.upper().removeprefix("KX"), yes_bid=to_decimal(quote.bid), yes_ask=to_decimal(quote.ask),
                start_time=market.close_timestamp,
                volume=to_decimal(market.stats.volume_total) if getattr(market, "stats", None) else None,
            ))
        out.extend(rows)
    return relink_kalshi_sides(out)


def relink_kalshi_sides(markets: list[BoardMarket]) -> list[BoardMarket]:
    """A game's events list one participant list; spread markets name their
    team by the same custom strike as the winner market, so every market of a
    game takes the moneyline's sides (names, codes) by that key."""
    by_game: dict[str, dict[str, Side]] = {}
    for m in markets:
        if m.kind == "moneyline" and m.yes_side is not None:
            by_game.setdefault(m.event_key, {})[m.yes_side.key] = m.yes_side
    for m in markets:
        known = by_game.get(m.event_key) or {}
        if m.yes_side is not None and m.yes_side.key in known:
            m.yes_side = known[m.yes_side.key]
        ordered = [next((s for s in known.values() if same_name(s.name, t.name)), t) for t in m.participants]
        m.participants = [s for s in ordered] or list(known.values())
    return markets


def kalshi_sides(title: str, sub_title: str) -> list[Side]:
    """`New England vs Chicago` and `NE vs CHI (Oct 22)` -> both sides with codes."""
    names = vs_sides(title)
    codes = vs_sides(re.sub(r"\s*\([^)]*\)\s*$", "", sub_title))
    if len(names) != 2:
        return []
    return [Side(key=f"name:{n.lower()}", name=n, abbr=codes[i] if len(codes) == 2 else None)
            for i, n in enumerate(names)]


def kalshi_side_of(info: dict[str, Any], label: str | None, teams: list[Side]) -> Side | None:
    """The participant a Kalshi market's YES backs: its custom strike (the
    team's id) and its subtitle (`Chicago`, `Tie`, `Chicago wins by over 2.5 points`)."""
    strike = info.get("custom_strike") or {}
    key = next((str(v) for v in strike.values()), None) if isinstance(strike, dict) else None
    if not label:
        return None
    if label.strip().lower() in DRAW_WORDS:
        return Side(key=key or "draw", name="Draw", draw=True)
    team = next((t for t in teams if same_name(t.name, label)), None) \
        or next((t for t in teams if label.lower().startswith(t.name.lower() + " ")), None)
    if team is None:
        return Side(key=key, name=label) if key else None
    # A winner market names its team in full (`Clement Chidekh`); a spread names it as the title does.
    return Side(key=key or team.key, name=label if same_name(team.name, label) else team.name, abbr=team.abbr)


def teams_of(raw: list[Any], *, abbr_field: str = "abbreviation") -> list[Side]:
    """An event's `teams` (Gamma, the Polymarket US gateway) as sides."""
    out = []
    for team in raw:
        if not isinstance(team, dict) or not team.get("name"):
            continue
        abbr = team.get(abbr_field) or team.get("abbreviation") or None
        alias = str(team.get("alias") or "").strip() or None
        out.append(Side(key=f"team:{team.get('id') or team['name']}", name=str(team["name"]),
                        abbr=str(abbr).upper() if abbr else None, alias=alias if alias != team["name"] else None))
    return out


def side_of_team(team: Any, teams: list[Side]) -> Side | None:
    if not isinstance(team, dict) or not team.get("name"):
        return None
    key = f"team:{team.get('id') or team['name']}"
    return next((t for t in teams if t.key == key), None) or teams_of([team], abbr_field="displayAbbreviation")[0]


def listed_sides(outcomes: list[str], label: str | None, teams: list[Side]) -> tuple[Side | None, Side | None]:
    """Who a Polymarket market's outcomes back: a two-sided market's outcome
    names (`Patriots` / `Bears`, matched to the event's teams by name or
    alias), else a YES / NO market's own label (`Athletic Club`, `Draw (...)`)."""
    def team(name: str) -> Side:
        return next((t for t in teams if same_name(t.name, name)), None) \
            or Side(key=f"name:{name.lower()}", name=name)

    if len(outcomes) == 2 and not yes_no(outcomes):
        if {o.lower() for o in outcomes} == {"over", "under"}:
            return None, None
        return team(outcomes[0]), team(outcomes[1])
    if label:
        if label.strip().lower().split(" ")[0] in DRAW_WORDS:
            return Side(key="draw", name="Draw", draw=True), None
        return team(label), None
    return None, None


def category_of(tags: Iterable[str], series: str | None = None) -> tuple[str, str, Literal["sports", "other"]]:
    words = [str(t).lower() for t in tags if t]
    if series:
        words.insert(0, series.lower())
    for key, label, names in LEAGUES:
        for word in words:
            if word in names or any(word.startswith(n) for n in names if len(n) >= 3 and word.isalpha()):
                return key, label, "sports" if key in SPORTS else "other"
    if any(w in ("sports", "games") for w in words):
        return "sports", "Sports", "sports"
    return OTHER[0], OTHER[1], "other"


def league_of(tags: Iterable[str], series: str | None = None) -> str:
    return category_of(tags, series)[0]


def league_of_series(series: str) -> str:
    return category_of([], series.upper().removeprefix("KX"))[0]


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def yes_no(outcomes: list[str]) -> bool:
    return [o.lower() for o in outcomes] == ["yes", "no"]


def outcome_names(raw: Any) -> list[str]:
    if isinstance(raw, str):
        import json
        try:
            raw = json.loads(raw)
        except ValueError:
            return []
    return [str(x) for x in raw] if isinstance(raw, list) else []


def same_name(a: str, b: str) -> bool:
    """`Patriots` and `New England Patriots`, `Chidekh` and `Clement Chidekh`
    (one name inside the other, by whole words); not `Ohio State` and `Penn State`."""
    a, b = " ".join(a.lower().split()), " ".join(b.lower().split())
    if not a or not b:
        return False
    return a == b or f" {a} " in f" {b} " or f" {b} " in f" {a} "


def vs_sides(text: str) -> list[str]:
    """`333: Jasmin Lucindo vs Nicolle Caliari` -> both names; a `: Spread` tail is dropped."""
    parts = re.split(r"\s+(?:vs\.?|v\.?|at|@)\s+", text.strip(), flags=re.I)
    if len(parts) != 2:
        return []
    first = parts[0].rsplit(": ", 1)[-1].strip()
    second = parts[1].split(": ", 1)[0].strip()
    return [first, second] if first and second else []


def short(outcome: str) -> str:
    return {"over": "O", "under": "U"}.get(outcome.lower(), outcome)


def nearest_even(markets: list[BoardMarket]) -> BoardMarket:
    def distance(m: BoardMarket) -> Decimal:
        price = mid(m)
        return abs(price - Decimal("0.5")) if price is not None else Decimal(1)
    return min(markets, key=distance)


def mid(market: BoardMarket) -> Decimal | None:
    if market.yes_bid is not None and market.yes_ask is not None:
        return (market.yes_bid + market.yes_ask) / 2
    return market.yes_ask if market.yes_ask is not None else market.yes_bid


def format_line(value: Decimal) -> str:
    return f"{value.normalize():f}"


def game_title(title: str) -> str:
    """`New England vs Chicago: Spread` -> `New England vs Chicago`."""
    return re.split(r":\s(?=[^:]*$)|\s[-–]\s|\?$", title, maxsplit=1)[0].strip() or title


def to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def millis(value: Any) -> int | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        return int(value if value > 1e11 else value * 1000)
    from datetime import datetime
    text = str(value).replace("Z", "+00:00")
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    if text.endswith("+00"):
        text += ":00"
    try:
        return int(datetime.fromisoformat(text).timestamp() * 1000)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Leg labels: a short name for each market on a board
# ---------------------------------------------------------------------------

LABEL_LENGTH = 60
"""A label longer than this is cut, with an ellipsis."""


def market_labels(board: ComboBoard) -> dict[str, str]:
    """A short readable name for every market on the board, for a person to
    recognise a leg by (`Al Fayha to win`, `Patriots -2.5`, `Patriots vs.
    Bears over 41.5`, `U.S. invades Iran`): keyed by market id, and by
    `market_id|outcome` where the two outcomes back different participants
    (a two-sided moneyline or spread, a total). `label_of` reads it."""
    labels: dict[str, str] = {}
    for events_done, event in enumerate(board.events, 1):
        share_the_lock(events_done)
        if event.kind == "game":
            game_labels(event, labels)
        else:
            for pick in (event.yes, event.no):
                if pick is not None:
                    labels.setdefault(pick.market_id, short_question(pick.title or event.title))
    return labels


def game_labels(event: BoardEvent, labels: dict[str, str]) -> None:
    """A game's picks: the participant (by its short name) and what it backs."""
    for row in event.rows:
        name = row.alias or row.name
        for kind, pick in row.picks.items():
            if pick is None:
                continue
            if kind == "moneyline":
                text = f"Draw in {event.title}" if name == "Draw" else f"{name} to win"
            elif kind == "spread":
                # The button says `CHI -2.5`: the signed line is its last word.
                text = f"{name} {pick.label.rsplit(' ', 1)[-1]}" if " " in pick.label else name
            else:
                text = f"{event.title} {'over' if pick.outcome == 'yes' else 'under'} {pick.line or ''}".strip()
            labels[f"{pick.market_id}|{pick.outcome}"] = text
            two_sided = kind == "total" or any(
                other is not row and (other.picks.get(kind) or None) is not None
                and other.picks[kind].market_id == pick.market_id for other in event.rows)   # type: ignore[union-attr]
            labels.setdefault(pick.market_id, f"{event.title} {kind}" if two_sided else text)


def label_of(labels: dict[str, str], market_id: str, outcome: str | None = None) -> str | None:
    """A leg's label from `market_labels`: its outcome's where the outcomes
    differ, else its market's."""
    return (labels.get(f"{market_id}|{outcome}") if outcome else None) or labels.get(market_id)


_WHEN = re.compile(
    r"\s+(?:on|by|before|after|in|during|at|through|until|ending)\s+(?:the\s+end\s+of\s+)?"
    r"(?:\d{4}-\d{2}-\d{2}|(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\.?\s+\d{1,2}(?:st|nd|rd|th)?"
    r"(?:,?\s+\d{4})?|(?:q[1-4]\s+)?\d{4}|today|tonight|tomorrow|this\s+(?:week|month|year))$", re.I)
_IRREGULAR = {"be": "is", "have": "has", "do": "does", "go": "goes"}


def short_question(title: str, *, length: int = LABEL_LENGTH) -> str:
    """A market's question as a short statement: `Will the U.S. invade Iran
    before 2027?` -> `U.S. invades Iran`; `Will Al Fayha Saudi Club win on
    2026-10-22?` -> `Al Fayha Saudi Club wins`. The question's own words,
    without its `Will`, its question mark and a trailing date; anything else
    is kept as it is, cut to `length`."""
    text = " ".join(str(title or "").split()).rstrip("?").strip()
    text = _WHEN.sub("", text).strip()
    if text.lower().startswith("will "):
        words = text[5:].split(" ")
        if len(words) > 1 and words[0].lower() == "the" and words[1][:1].isupper():
            words = words[1:]
        for at, word in enumerate(words):
            if at and word.isalpha() and word.islower():
                if word != "not":
                    words[at] = third_person(word)
                break
        text = " ".join(words)
    if len(text) > length:
        text = text[:length - 1].rstrip() + "…"
    return text or str(title)


def third_person(verb: str) -> str:
    """`win` -> `wins`, `reach` -> `reaches`, `carry` -> `carries`, `be` -> `is`."""
    if verb in _IRREGULAR:
        return _IRREGULAR[verb]
    if verb.endswith(("s", "sh", "ch", "x", "z", "o")):
        return verb + "es"
    if verb.endswith("y") and len(verb) > 1 and verb[-2] not in "aeiou":
        return verb[:-1] + "ies"
    return verb + "s"


# ---------------------------------------------------------------------------
# Serving a board in parts: the rail, and one category a page at a time
# ---------------------------------------------------------------------------

PAGE_LIMIT = 50
MAX_PAGE_LIMIT = 500
BoardSort = Literal["board", "volume", "start"]


def categories_view(board: ComboBoard) -> ComboBoardCategories:
    return ComboBoardCategories(venue=board.venue, status=board.status, built_at=board.built_at,
                                complete=board.complete, count=board.count, events=len(board.events),
                                categories=board.categories, error=board.error)


def board_page(board: ComboBoard, *, category: str | None = None, cursor: str | None = None,
               limit: int = PAGE_LIMIT, query: str | None = None, sort: BoardSort = "board") -> ComboBoard:
    """One page of the board: the events of `category` (`all` or none: every
    one) whose title, category or participants hold every word of `query`,
    in board order (sports first, the most events first, by start), or by
    `volume` (largest first) or `start` within each category; from `cursor`
    (an offset from the last page), `limit` events. The categories are the
    whole board's, for the rail."""
    events = [e for e in board.events if category in (None, "", "all") or e.category == category]
    words = [w for w in (query or "").lower().split() if w]
    if words:
        events = [e for e in events if all(w in searchable(e) for w in words)]
    if sort != "board":
        order = {c.key: index for index, c in enumerate(board.categories)}

        def key(e: BoardEvent) -> tuple:
            within = -(e.volume or Decimal(0)) if sort == "volume" else Decimal(e.start_time or 2**62)
            return (order.get(e.category, len(order)), within, e.title)
        events = sorted(events, key=key)
    start = page_offset(cursor)
    limit = max(1, min(int(limit), MAX_PAGE_LIMIT))
    page = events[start:start + limit]
    more = start + limit < len(events)
    return board.model_copy(update={"events": page, "category": category or "all", "total": len(events),
                                    "next_cursor": str(start + limit) if more else None})


def searchable(event: BoardEvent) -> str:
    return " ".join([event.title, event.category_label,
                     *(f"{r.name} {r.abbr or ''} {r.alias or ''}" for r in event.rows)]).lower()


def page_offset(cursor: str | None) -> int:
    if not cursor:
        return 0
    try:
        return max(0, int(cursor))
    except ValueError:
        from ..errors import BadRequest
        raise BadRequest(f"not a cursor of this board: {cursor!r}") from None


# ---------------------------------------------------------------------------
# Reading a venue
# ---------------------------------------------------------------------------

BOARD_PAGES = 5
"""Pages of a venue's eligible list (or of Kalshi's collections) a board read
on request reads. serve's warmer (`synpath.server.combo_warmer`) reads every
page (`pages=None`)."""
BOARD_EVENTS = 150
"""Kalshi: the most collection events whose markets a board read on request
reads (`max_events=None`: every one)."""
PRICE_BATCH = 100
"""Gamma's largest batch."""
KALSHI_EVENT_BATCH = 100
EVENT_BATCH = 20


FIRST_PUBLISH_PAGES = 5
"""A whole-list build with `progress` publishes its board after this many
pages (the venue lists by volume, so the busiest markets come first)..."""
PUBLISH_EVERY_PAGES = 40
"""...and again every this many pages, until the list ends."""


class BoardCache:
    """What a board keeps between builds of one venue: Polymarket's Gamma
    markets (their type, line, label and event: fields that do not change;
    prices are left out, so each build takes the catalogue's own) and the
    games' events (their teams). A rebuild reads Gamma only for markets it
    has not seen; what a build no longer lists is dropped."""

    def __init__(self) -> None:
        self.markets: dict[str, dict[str, Any]] = {}
        self.events: dict[str, dict[str, Any]] = {}

    def prune(self, market_natives: set[str], event_ids: set[str]) -> None:
        self.markets = {k: v for k, v in self.markets.items() if k in market_natives}
        self.events = {k: v for k, v in self.events.items() if k in event_ids}


GAMMA_FIELDS = ("question", "sportsMarketType", "line", "groupItemTitle", "gameStartTime", "endDate", "outcomes")
GAMMA_EVENT_FIELDS = ("id", "title", "seriesSlug", "startTime", "teams")


def static_gamma(info: dict[str, Any]) -> dict[str, Any]:
    """A Gamma market's fields a board reads that do not move with the market."""
    out = {k: info[k] for k in GAMMA_FIELDS if info.get(k) is not None}
    events = info.get("events")
    if isinstance(events, list) and events and isinstance(events[0], dict):
        out["events"] = [{k: events[0][k] for k in GAMMA_EVENT_FIELDS if events[0].get(k) is not None}]
    return out


async def board_for(venue: str, adapter: Any, reader: Any | None = None, *, pages: int | None = BOARD_PAGES,
                    max_events: int | None = BOARD_EVENTS, pause_s: float = 0.0, cache: BoardCache | None = None,
                    progress: Callable[[ComboBoard], None | Awaitable[None]] | None = None) -> ComboBoard:
    """The board of one combo venue. `adapter` is the engine's trading
    adapter (its eligibility: Kalshi's collections, or the venue's eligible
    list); `reader` is the server's read adapter for the venue, where there is
    one: Kalshi's event markets come from it (`fetch_events_by_ids`);
    Polymarket's rows get its Gamma markets (types, lines, labels, quotes) and
    the games' events (teams); Polymarket US's rows carry their own.

    `pages` (None: until the venue's list ends) and `max_events` (None: no
    limit) bound the read; `pause_s` is slept between two venue reads, so a
    whole-catalogue build stays polite to the venue. `cache` keeps
    Polymarket's Gamma reads between builds (`BoardCache`). `progress`, where
    given, is handed the board so far (`complete: false`) after the first
    `FIRST_PUBLISH_PAGES` pages of a listed venue and every
    `PUBLISH_EVERY_PAGES` after: Polymarket lists about 214,000 markets.

    The CPU-heavy steps (rows into board markets, markets into the board) run
    in worker threads: on the whole catalogue they take seconds, and serve's
    event loop must keep answering the terminal's health checks meanwhile."""
    import time

    from .rfq_group import lists_combo_markets

    now = int(time.time() * 1000)
    if callable(getattr(adapter, "fetch_combo_collections", None)):
        tickers, complete = await collection_events(adapter, pages, pause_s)
        fetch = getattr(reader, "fetch_events_by_ids", None)
        if fetch is None or not tickers:
            return ComboBoard(venue=venue, count=0, complete=complete and not tickers, as_of=now)
        wanted = tickers if max_events is None else tickers[:max_events]
        events = await kalshi_events_of(venue, wanted, fetch, pause_s)
        markets = await asyncio.to_thread(kalshi_markets, events)
        return await asyncio.to_thread(board_of, venue, markets, complete=complete and len(wanted) == len(tickers),
                                       as_of=now)
    if not lists_combo_markets(adapter):
        return ComboBoard(venue=venue, count=0, complete=True, as_of=now)
    return await listed_board(venue, adapter, reader, pages=pages, pause_s=pause_s, cache=cache, progress=progress,
                              now=now)


async def listed_board(venue: str, adapter: Any, reader: Any | None, *, pages: int | None, pause_s: float,
                       cache: BoardCache | None, progress: Callable[[ComboBoard], None | Awaitable[None]] | None,
                       now: int) -> ComboBoard:
    """A listed venue's board (Polymarket, Polymarket US): its eligible list
    page by page, each chunk of rows turned into board markets as it comes,
    the board so far handed to `progress` along the way."""
    cache = cache if cache is not None else BoardCache()
    markets: list[BoardMarket] = []
    seen: set[str] = set()
    pending: list[Any] = []
    cursor: str | None = None
    read = 0
    complete = False
    while pages is None or read < pages:
        if read:
            await pause(pause_s)
        page = await adapter.fetch_combo_eligible_markets(cursor=cursor)
        read += 1
        for row in page:
            if row.market_id not in seen:
                seen.add(row.market_id)
                pending.append(row)
        cursor = page.next_cursor
        if not cursor:
            complete = True
            break
        if progress is not None and (read == FIRST_PUBLISH_PAGES or read % PUBLISH_EVERY_PAGES == 0):
            markets += await listed_markets_of(venue, pending, reader, pause_s, cache)
            pending = []
            # The board so far is long-lived: keep the cyclic collector from
            # rescanning it on every full collection (`share_the_lock`).
            gc.freeze()
            published = progress(await asyncio.to_thread(board_of, venue, markets, complete=False, as_of=now))
            if inspect.isawaitable(published):
                await published
    markets += await listed_markets_of(venue, pending, reader, pause_s, cache)
    if complete:
        cache.prune({m.market_id.partition(":")[2] for m in markets},
                    {m.event_key.partition(":")[2] for m in markets if m.event_key.startswith("polymarket-event:")})
    return await asyncio.to_thread(board_of, venue, markets, complete=complete, as_of=now)


async def listed_markets_of(venue: str, rows: list[Any], reader: Any | None, pause_s: float,
                            cache: BoardCache) -> list[BoardMarket]:
    """Board markets for a chunk of a listed venue's rows: Polymarket US's
    carry their own fields; Polymarket's take Gamma's market (from `cache`,
    else read now, with its prices) and their game's event (teams)."""
    if not rows:
        return []
    can_read = reader is not None and callable(getattr(reader, "fetch_markets_by_ids", None))
    if venue == "polymarket_us" or all(getattr(r, "venue", "") == "polymarket_us" for r in rows):
        markets = await asyncio.to_thread(polymarket_us_markets, rows)
        if can_read:
            await with_catalogue_quotes(markets, reader, pause_s)
        return markets
    natives, unseen = await asyncio.to_thread(natives_not_cached, rows, cache)
    quotes: dict[str, Market] = {}
    if can_read and unseen:
        quotes = await catalogue_markets(unseen, reader, pause_s)
    details = await asyncio.to_thread(remember_gamma_markets, natives, unseen if can_read else [], quotes, cache)
    fetch_events = getattr(reader, "fetch_events_by_ids", None)
    if callable(fetch_events):
        missing = await asyncio.to_thread(game_events_not_cached, details, cache)
        for event_id, info in (await game_events(missing, fetch_events, pause_s)).items():
            cache.events[event_id] = {"teams": info.get("teams") or []}
    markets = await asyncio.to_thread(polymarket_markets, rows, details, cache.events)
    await asyncio.to_thread(apply_quotes, markets, quotes)
    return markets


# The bookkeeping of one chunk of Polymarket rows (about 40,000 markets), kept
# off serve's event loop like the builders above (`share_the_lock`).

def natives_not_cached(rows: list[Any], cache: BoardCache) -> tuple[dict[str, str], list[str]]:
    """Each row's native Gamma id, by market id, and the market ids whose
    Gamma market the cache does not hold yet."""
    natives: dict[str, str] = {}
    for rows_done, row in enumerate(rows, 1):
        share_the_lock(rows_done)
        natives[row.market_id] = str(row.market_id).partition(":")[2] or str(row.market_id)
    return natives, [m for m, n in natives.items() if n not in cache.markets]


def remember_gamma_markets(natives: dict[str, str], unseen: list[str], quotes: dict[str, Market],
                           cache: BoardCache) -> dict[str, dict[str, Any]]:
    """Gamma's market for each row, by native id: the one just read (with
    its prices), else the cache's. What was just read goes into the cache;
    a market Gamma did not serve is cached empty, so it is not asked for again."""
    for market_id in unseen:
        cache.markets.setdefault(natives[market_id], {})
    fresh: dict[str, dict[str, Any]] = {}
    for quotes_done, market in enumerate(quotes.values(), 1):
        share_the_lock(quotes_done)
        info = dict(market.info or {})
        fresh[market.venue_market_id] = info
        cache.markets[market.venue_market_id] = static_gamma(info)
    return {n: fresh.get(n) or cache.markets[n] for n in natives.values() if n in fresh or n in cache.markets}


def game_events_not_cached(details: dict[str, dict[str, Any]], cache: BoardCache) -> list[str]:
    """The Gamma event ids of the chunk's games whose teams the cache lacks."""
    wanted = list(dict.fromkeys(
        str((d.get("events") or [{}])[0].get("id")) for d in details.values()
        if POLYMARKET_KINDS.get(str(d.get("sportsMarketType") or "")) and d.get("events")))
    return [e for e in wanted if e not in cache.events]


async def pause(seconds: float) -> None:
    if seconds > 0:
        import asyncio
        await asyncio.sleep(seconds)


async def collection_events(adapter: Any, pages: int | None, pause_s: float = 0.0) -> tuple[list[str], bool]:
    """Every event ticker an open collection takes, in the venue's order."""
    tickers: list[str] = []
    cursor: str | None = None
    read = 0
    while pages is None or read < pages:
        if read:
            await pause(pause_s)
        page = await adapter.fetch_combo_collections(status="open", limit=200, cursor=cursor)
        read += 1
        for row in page:
            tickers.extend(str(t) for t in row.get("associated_event_tickers") or [])
        cursor = page.next_cursor
        if not cursor or not page:
            return list(dict.fromkeys(tickers)), True
    return list(dict.fromkeys(tickers)), False


async def kalshi_events_of(venue: str, tickers: list[str], fetch: Any, pause_s: float) -> list[Event]:
    """Kalshi's collection events with their markets, in batches."""
    import asyncio

    out: list[Event] = []
    for start in range(0, len(tickers), KALSHI_EVENT_BATCH):
        if start:
            await pause(pause_s)
        out.extend(await asyncio.to_thread(fetch, [f"{venue}:{t}" for t in tickers[start:start + KALSHI_EVENT_BATCH]]))
    return out


async def catalogue_markets(wanted: list[str], reader: Any, pause_s: float = 0.0) -> dict[str, Market]:
    """The catalogue's markets for these ids, best effort, by id."""
    import asyncio

    found: dict[str, Market] = {}
    for start in range(0, len(wanted), PRICE_BATCH):
        if start:
            await pause(pause_s)
        try:
            batch = await asyncio.to_thread(reader.fetch_markets_by_ids, wanted[start:start + PRICE_BATCH])
        except Exception:
            continue
        found.update((m.id, m) for m in batch)
    return found


async def game_events(event_ids: list[str], fetch: Any, pause_s: float = 0.0) -> dict[str, dict[str, Any]]:
    """Polymarket's game events (their `teams`), by event id, best effort."""
    import asyncio

    out: dict[str, dict[str, Any]] = {}
    for start in range(0, len(event_ids), EVENT_BATCH):
        if start:
            await pause(pause_s)
        try:
            batch = await asyncio.to_thread(fetch, [f"polymarket:{e}" for e in event_ids[start:start + EVENT_BATCH]])
        except Exception:
            continue
        for event in batch:
            out[event.venue_event_id] = dict(event.info or {})
    return out


def apply_quotes(markets: list[BoardMarket], quotes: dict[str, Market]) -> None:
    for market in markets:
        held = quotes.get(market.market_id)
        if held is None:
            continue
        quote = held.yes.quote
        if quote.bid is not None or quote.ask is not None:
            market.yes_bid, market.yes_ask = to_decimal(quote.bid), to_decimal(quote.ask)


async def with_catalogue_quotes(markets: list[BoardMarket], reader: Any, pause_s: float = 0.0) -> None:
    """Replace listed prices with the catalogue's quotes, best effort."""
    apply_quotes(markets, await catalogue_markets([m.market_id for m in markets], reader, pause_s))
