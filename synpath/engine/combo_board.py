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

import re
from decimal import Decimal
from typing import Any, Iterable, Literal

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


class ComboBoard(_Base):
    """A venue's combo-eligible markets, grouped for a builder."""

    venue: str
    count: int = Field(description="Combo-eligible markets read")
    events: list[BoardEvent] = Field(default_factory=list)
    categories: list[BoardCategory] = Field(default_factory=list, description="By count, sports first")
    complete: bool = Field(default=True, description="Every page of the venue's list was read")
    as_of: int | None = None


# ---------------------------------------------------------------------------
# Building a board
# ---------------------------------------------------------------------------

class Side(_Base):
    """A game's participant as the venue names it: a team, a player, or the draw."""

    key: str = Field(description="The participant's identity within the event: the venue's team id where it has "
                                 "one, else its name")
    name: str
    abbr: str | None = Field(default=None, description="The venue's own abbreviation, where it has one")
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
    for key, members in by_event.items():
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
    rows = [BoardRow(name=s.name, abbr=s.abbr) for s in sides]
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
    for row in rows:
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
    for row in rows:
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
    for event in events:
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
        out.append(Side(key=f"team:{team.get('id') or team['name']}", name=str(team["name"]),
                        abbr=str(abbr).upper() if abbr else None))
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
# Reading a venue
# ---------------------------------------------------------------------------

BOARD_PAGES = 5
"""Pages of a venue's eligible list (or of Kalshi's collections) a board reads."""
BOARD_EVENTS = 150
"""Kalshi: the most collection events whose markets a board reads."""
PRICE_BATCH = 50


async def board_for(venue: str, adapter: Any, reader: Any | None = None, *, pages: int = BOARD_PAGES,
                    max_events: int = BOARD_EVENTS) -> ComboBoard:
    """The board of one combo venue. `adapter` is the engine's trading
    adapter (its eligibility: Kalshi's collections, or the venue's eligible
    list); `reader` is the server's read adapter for the venue, where there is
    one: Kalshi's event markets come from it (`fetch_events_by_ids`);
    Polymarket's rows get its Gamma markets (types, lines, labels, quotes) and
    the games' events (teams); Polymarket US's rows carry their own."""
    import asyncio
    import time

    from .rfq_group import lists_combo_markets

    now = int(time.time() * 1000)
    if callable(getattr(adapter, "fetch_combo_collections", None)):
        tickers, complete = await collection_events(adapter, pages)
        fetch = getattr(reader, "fetch_events_by_ids", None)
        if fetch is None or not tickers:
            return ComboBoard(venue=venue, count=0, complete=complete and not tickers, as_of=now)
        events = await asyncio.to_thread(fetch, [f"{venue}:{t}" for t in tickers[:max_events]])
        return board_of(venue, kalshi_markets(events), complete=complete and len(tickers) <= max_events, as_of=now)
    if not lists_combo_markets(adapter):
        return ComboBoard(venue=venue, count=0, complete=True, as_of=now)
    rows: list[Any] = []
    cursor: str | None = None
    complete = False
    for _ in range(pages):
        page = await adapter.fetch_combo_eligible_markets(cursor=cursor)
        rows.extend(page)
        cursor = page.next_cursor
        if not cursor:
            complete = True
            break
    if venue == "polymarket_us" or all(getattr(r, "venue", "") == "polymarket_us" for r in rows if rows):
        markets = polymarket_us_markets(rows)
        if reader is not None and callable(getattr(reader, "fetch_markets_by_ids", None)):
            await with_catalogue_quotes(markets, reader)
        return board_of(venue, markets, complete=complete, as_of=now)
    details: dict[str, dict[str, Any]] = {}
    quotes: dict[str, Market] = {}
    if reader is not None and callable(getattr(reader, "fetch_markets_by_ids", None)):
        quotes = await catalogue_markets([r.market_id for r in rows], reader)
        details = {m.venue_market_id: dict(m.info or {}) for m in quotes.values()}
    events: dict[str, dict[str, Any]] = {}
    fetch_events = getattr(reader, "fetch_events_by_ids", None)
    if callable(fetch_events):
        wanted = list(dict.fromkeys(
            str((d.get("events") or [{}])[0].get("id")) for d in details.values()
            if POLYMARKET_KINDS.get(str(d.get("sportsMarketType") or "")) and d.get("events")))
        events = await game_events(wanted, fetch_events)
    markets = polymarket_markets(rows, details, events)
    apply_quotes(markets, quotes)
    return board_of(venue, markets, complete=complete, as_of=now)


async def collection_events(adapter: Any, pages: int) -> tuple[list[str], bool]:
    """Every event ticker an open collection takes, in the venue's order."""
    tickers: list[str] = []
    cursor: str | None = None
    for _ in range(pages):
        page = await adapter.fetch_combo_collections(status="open", limit=200, cursor=cursor)
        for row in page:
            tickers.extend(str(t) for t in row.get("associated_event_tickers") or [])
        cursor = page.next_cursor
        if not cursor or not page:
            return list(dict.fromkeys(tickers)), True
    return list(dict.fromkeys(tickers)), False


async def catalogue_markets(wanted: list[str], reader: Any) -> dict[str, Market]:
    """The catalogue's markets for these ids, best effort, by id."""
    import asyncio

    found: dict[str, Market] = {}
    for start in range(0, len(wanted), PRICE_BATCH):
        try:
            batch = await asyncio.to_thread(reader.fetch_markets_by_ids, wanted[start:start + PRICE_BATCH])
        except Exception:
            continue
        found.update((m.id, m) for m in batch)
    return found


async def game_events(event_ids: list[str], fetch: Any) -> dict[str, dict[str, Any]]:
    """Polymarket's game events (their `teams`), by event id, best effort."""
    import asyncio

    out: dict[str, dict[str, Any]] = {}
    for start in range(0, len(event_ids), EVENT_BATCH):
        try:
            batch = await asyncio.to_thread(fetch, [f"polymarket:{e}" for e in event_ids[start:start + EVENT_BATCH]])
        except Exception:
            continue
        for event in batch:
            out[event.venue_event_id] = dict(event.info or {})
    return out


EVENT_BATCH = 20


def apply_quotes(markets: list[BoardMarket], quotes: dict[str, Market]) -> None:
    for market in markets:
        held = quotes.get(market.market_id)
        if held is None:
            continue
        quote = held.yes.quote
        if quote.bid is not None or quote.ask is not None:
            market.yes_bid, market.yes_ask = to_decimal(quote.bid), to_decimal(quote.ask)


async def with_catalogue_quotes(markets: list[BoardMarket], reader: Any) -> None:
    """Replace listed prices with the catalogue's quotes, best effort."""
    apply_quotes(markets, await catalogue_markets([m.market_id for m in markets], reader))
