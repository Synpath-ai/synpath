"""The combo board (synpath/engine/combo_board.py), offline, on real venue data.

The samples are trimmed reads of the venues' public catalogues on 2026-10-22
(no auth):

* `samples/polymarket_combo_board_live.json`: Polymarket's combo catalogue
  rows (`/v1/rfq/combo-markets`), Gamma's markets for them (`sportsMarketType`,
  `line`, `groupItemTitle`) and Gamma's events (`teams` with `abbreviation`):
  an NFL game, an NBA game, a La Liga match (one market per result, and the
  draw), an ATP match and a politics question;
* `samples/polymarket_us_combo_board_live.json`: the US gateway's events with
  their combo-enabled markets (`marketType`, `marketSides[].team` with
  `displayAbbreviation`): a CBA, KHL, NPB, cricket, esports and table tennis
  match, and a Liga Colombia match (drawable);
* `samples/kalshi_combo_board_live.json`: the events of Kalshi's open
  collection (`product_metadata.competition_scope`, `sub_title`,
  `yes_sub_title`, `custom_strike`, `floor_strike`): NFL, NBA, NHL and
  Serie A games (winner, spread and total events), an ATP match, NFL team
  totals and an NCAAF first half (questions).

Covered: each venue's games with their participants, codes, the side each
pick backs and its label, by the venue's own type and team fields (never a
URL slug or title wording); questions for everything else, with named
outcomes on their buttons; categories; and the readers (`board_for`).
"""
from __future__ import annotations

import json
from decimal import Decimal as D
from pathlib import Path

import pytest

from synpath.engine.combo_board import (
    board_for, board_of, kalshi_markets, polymarket_markets, polymarket_us_markets,
)
from synpath.engine.paper import PaperRfqVenue
from synpath.kalshi import normalize_event as kalshi_event
from synpath.polymarket import normalize_event as gamma_event
from synpath.polymarket import normalize_market as gamma_market
from synpath.trading.polymarket_rfq import combo_market_of
from synpath.trading.polymarket_us import combo_markets_of
from synpath.types import Page

SAMPLES = Path(__file__).parent / "samples"
pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def sample(name: str) -> dict:
    return json.loads((SAMPLES / name).read_text())


POLY = sample("polymarket_combo_board_live.json")
US = sample("polymarket_us_combo_board_live.json")
KALSHI = sample("kalshi_combo_board_live.json")


def poly_board():
    rows = [combo_market_of(r) for r in POLY["catalogue"]]
    return board_of("polymarket", polymarket_markets(rows, {m["id"]: m for m in POLY["gamma_markets"]},
                                                     {e["id"]: e for e in POLY["gamma_events"]}))


def us_rows():
    return [row for event in US["events"] for row in combo_markets_of(event)]


def kalshi_events():
    return [kalshi_event(e) for e in KALSHI["events"]]


def game(board, title):
    return next(e for e in board.events if e.kind == "game" and e.title == title)


def rows(event):
    return [(r.name, r.abbr) for r in event.rows]


def picks(event, kind):
    return [(p.label, p.outcome, p.market_id) if p else None for p in (r.picks[kind] for r in event.rows)]


class TestPolymarketLive:
    def test_an_nfl_game_by_gamma_types_with_the_events_teams_and_codes(self):
        nfl = game(poly_board(), "Patriots vs. Bears")
        assert nfl.id == "polymarket-event:1004101" and nfl.category == "nfl"
        assert nfl.market_types == ["moneyline", "spread", "total"]
        # Gamma's teams in the event's order, with their abbreviations; the outcomes say Patriots / Bears.
        assert rows(nfl) == [("New England Patriots", "NE"), ("Chicago Bears", "CHI")]
        assert picks(nfl, "moneyline") == [("NE", "yes", "polymarket:4465810"), ("CHI", "no", "polymarket:4465810")]
        # A spread is YES for its first outcome at its line (Bears -2.5), NO for the other at the opposite.
        assert picks(nfl, "spread") == [("NE +2.5", "no", "polymarket:5377299"), ("CHI -2.5", "yes", "polymarket:5377299")]
        assert [p[:2] for p in picks(nfl, "total")] == [("O 50.5", "yes"), ("U 50.5", "no")]

    def test_an_nba_game(self):
        nba = game(poly_board(), "Cavaliers vs. 76ers")
        assert nba.category == "nba" and rows(nba) == [("Cavaliers", "CLE"), ("76ers", "PHI")]
        assert [p[:2] for p in picks(nba, "moneyline")] == [("CLE", "yes"), ("PHI", "no")]
        assert [p[:2] for p in picks(nba, "spread")] == [("CLE +3.5", "no"), ("PHI -3.5", "yes")]
        assert [p[:2] for p in picks(nba, "total")] == [("O 235.5", "yes"), ("U 235.5", "no")]

    def test_a_soccer_match_is_one_yes_no_market_per_result_by_its_label(self):
        match = game(poly_board(), "Rayo Vallecano de Madrid vs. Athletic Club")
        assert match.category == "soccer"
        assert rows(match) == [("Rayo Vallecano de Madrid", "RAY"), ("Athletic Club", "BIL"), ("Draw", None)]
        assert picks(match, "moneyline") == [("RAY", "yes", "polymarket:5014977"), ("BIL", "yes", "polymarket:5014980"),
                                             ("Draw", "yes", "polymarket:5014978")]

    def test_a_tennis_match_and_a_politics_question(self):
        board = poly_board()
        tennis = game(board, "Almaty Open: Daniil Medvedev vs Sho Shimabukuro")
        assert tennis.category == "tennis" and [r.name for r in tennis.rows] == ["Daniil Medvedev", "Sho Shimabukuro"]
        assert [p[1] for p in picks(tennis, "moneyline")] == ["yes", "no"]
        iran = next(e for e in board.events if e.id == "polymarket:665374")
        assert iran.kind == "question" and iran.category == "politics" and (iran.yes.label, iran.no.label) == ("YES", "NO")

    def test_without_gammas_market_type_a_row_is_a_question_not_a_title_guess(self):
        rows_ = [combo_market_of(r) for r in POLY["catalogue"]]
        board = board_of("polymarket", polymarket_markets(rows_))
        assert all(e.kind == "question" for e in board.events)
        named = next(e for e in board.events if e.id == "polymarket:4465810")
        assert (named.yes.label, named.no.label) == ("Patriots", "Bears"), "named outcomes say their names"


class TestPolymarketUsLive:
    def test_moneylines_take_their_teams_and_codes_from_market_sides(self):
        board = board_of("polymarket_us", polymarket_us_markets(us_rows()))
        cba = game(board, "Shandong Hi-Speed Kirin vs. Tianjin Pioneers")
        assert rows(cba) == [("Shandong Hi-Speed Kirin", "SDK"), ("Tianjin Pioneers", "TIA")]
        assert picks(cba, "moneyline") == [("SDK", "yes", "polymarket_us:aec-cba-sdk-tia-2026-10-25"),
                                           ("TIA", "no", "polymarket_us:aec-cba-sdk-tia-2026-10-25")]
        khl = game(board, "Amur Khabarovsk vs. Admiral Vladivostok")
        assert rows(khl) == [("Amur Khabarovsk", "AMR"), ("Admiral Vladivostok", "ADM")]
        cricket = game(board, "New Zealand vs. India")
        assert rows(cricket) == [("New Zealand", "NZE"), ("India", "INDIA")]
        assert all(e.market_types == ["moneyline"] for e in board.events if e.kind == "game")

    def test_a_drawable_match_is_one_market_per_team_and_one_for_the_draw(self):
        board = board_of("polymarket_us", polymarket_us_markets(us_rows()))
        match = game(board, "CD Tolima vs. AD Pasto")
        assert match.category == "soccer"
        assert rows(match) == [("CD Tolima", "TOL"), ("AD Pasto", "ADP"), ("Draw", None)]
        assert [(p[0], p[1], p[2].rsplit("-", 1)[-1]) for p in picks(match, "moneyline")] == [
            ("TOL", "yes", "tol"), ("ADP", "yes", "adp"), ("Draw", "yes", "draw")]

    def test_every_listed_game_is_a_game(self):
        board = board_of("polymarket_us", polymarket_us_markets(us_rows()))
        assert len([e for e in board.events if e.kind == "game"]) == len(US["events"]) == 7
        assert board.count == len(us_rows())


class TestKalshiLive:
    def test_a_game_joins_its_winner_spread_and_total_events_by_competition_scope(self):
        board = board_of("kalshi", kalshi_markets(kalshi_events()))
        nfl = game(board, "New England vs Chicago")
        assert nfl.category == "nfl" and nfl.market_types == ["moneyline", "spread", "total"]
        # Names from the winner markets' yes_sub_title, codes from the event's sub_title (NE vs CHI).
        assert rows(nfl) == [("New England", "NE"), ("Chicago", "CHI")]
        assert picks(nfl, "moneyline") == [("NE", "yes", "kalshi:KXNFLGAME-26OCT22NECHI-NE"),
                                           ("CHI", "yes", "kalshi:KXNFLGAME-26OCT22NECHI-CHI")]
        # Each team's own spread market (its custom_strike is the team's), at floor_strike: "wins by over 2.5".
        assert picks(nfl, "spread") == [("NE -1.5", "yes", "kalshi:KXNFLSPREAD-26OCT22NECHI-NE2"),
                                        ("CHI -2.5", "yes", "kalshi:KXNFLSPREAD-26OCT22NECHI-CHI3")]
        assert picks(nfl, "total") == [("O 50.5", "yes", "kalshi:KXNFLTOTAL-26OCT22NECHI-51"),
                                       ("U 50.5", "no", "kalshi:KXNFLTOTAL-26OCT22NECHI-51")]

    def test_nhl_nba_and_a_soccer_match_with_its_tie(self):
        board = board_of("kalshi", kalshi_markets(kalshi_events()))
        nhl = game(board, "Vegas vs Columbus")
        assert nhl.category == "nhl" and rows(nhl) == [("Vegas", "VGK"), ("Columbus", "CBJ")]
        assert [p[0] for p in picks(nhl, "spread")] == ["VGK -1.5", "CBJ -1.5"]
        nba = game(board, "New Orleans vs Utah")
        assert nba.category == "nba" and rows(nba) == [("New Orleans", "NOP"), ("Utah", "UTA")]
        serie_a = game(board, "Monza vs Napoli")
        assert serie_a.category == "soccer"
        assert rows(serie_a) == [("Monza", "MON"), ("Napoli", "NAP"), ("Draw", None)]
        assert picks(serie_a, "moneyline")[2] == ("Draw", "yes", "kalshi:KXSERIEAGAME-26OCT28MONNAP-TIE")
        assert [p[0] for p in picks(serie_a, "total")[:2]] == ["O 2.5", "U 2.5"]

    def test_a_tennis_match_names_its_players_in_full_with_the_sub_titles_short_names(self):
        board = board_of("kalshi", kalshi_markets(kalshi_events()))
        tennis = game(board, "Chidekh vs Cerundolo")
        assert tennis.category == "tennis"
        assert rows(tennis) == [("Clement Chidekh", "Chidekh"), ("Francisco Cerundolo", "Cerundolo")]

    def test_other_scopes_are_questions(self):
        board = board_of("kalshi", kalshi_markets(kalshi_events()))
        questions = [e.title for e in board.events if e.kind == "question"]
        assert any("1st half" in q for q in questions), "an NCAAF first half is not the game's moneyline"
        assert any("over" in q and "points scored" in q for q in questions), "a team total is not the game's total"
        assert len([e for e in board.events if e.kind == "game"]) == 5

    def test_an_event_without_a_scope_is_questions_not_a_title_guess(self):
        events = [dict(e, product_metadata=None) for e in KALSHI["events"]]
        board = board_of("kalshi", kalshi_markets([kalshi_event(e) for e in events]))
        assert all(e.kind == "question" for e in board.events)


# -- the readers -------------------------------------------------------------

class CollectionPaper(PaperRfqVenue):
    async def fetch_combo_collections(self, *, status=None, event_id=None, limit=None, cursor=None, **_):
        return Page([{"collection_ticker": "KXMVE", "associated_event_tickers": [e["event_ticker"]
                                                                                 for e in KALSHI["events"]]}])


class ListedPaper(PaperRfqVenue):
    def __init__(self, rows, **kw):
        super().__init__(**kw)
        self.rows = rows

    async def fetch_combo_eligible_markets(self, *, event_id=None, tag=None, limit=None, cursor=None):
        half = len(self.rows) // 2
        return Page(self.rows[:half], next_cursor="p2") if cursor is None else Page(self.rows[half:])


class KalshiReader:
    def __init__(self):
        self.asked: list[list[str]] = []

    def fetch_events_by_ids(self, event_ids):
        self.asked.append(list(event_ids))
        return [e for e in kalshi_events() if e.id in event_ids]


class GammaReader:
    """The server's Polymarket read adapter: Gamma's markets and events."""

    def __init__(self, ask: str | None = None):
        self.ask = ask
        self.events_asked: list[list[str]] = []

    def fetch_markets_by_ids(self, market_ids):
        wanted = {m.partition(":")[2] for m in market_ids}
        out = []
        for raw in POLY["gamma_markets"]:
            if raw["id"] in wanted:
                out.append(gamma_market({**raw, **({"bestAsk": self.ask} if self.ask and raw["id"] == "4465810" else {})}))
        return out

    def fetch_events_by_ids(self, event_ids):
        self.events_asked.append(list(event_ids))
        return [gamma_event(e) for e in POLY["gamma_events"] if f"polymarket:{e['id']}" in event_ids]


class TestReaders:
    async def test_kalshi_reads_its_collections_events_from_the_catalogue(self):
        reader = KalshiReader()
        board = await board_for("kalshi", CollectionPaper(venue="kalshi"), reader)
        assert reader.asked == [[f"kalshi:{e['event_ticker']}" for e in KALSHI["events"]]]
        assert board.complete and len([e for e in board.events if e.kind == "game"]) == 5

    async def test_kalshi_without_a_catalogue_has_an_empty_board(self):
        board = await board_for("kalshi", CollectionPaper(venue="kalshi"), None)
        assert board.events == [] and not board.complete

    async def test_polymarket_takes_gammas_markets_and_the_games_teams(self):
        reader = GammaReader(ask="0.47")
        rows_ = [combo_market_of(r) for r in POLY["catalogue"]]
        board = await board_for("polymarket", ListedPaper(rows_, venue="polymarket"), reader)
        nfl = game(board, "Patriots vs. Bears")
        assert rows(nfl) == [("New England Patriots", "NE"), ("Chicago Bears", "CHI")]
        assert nfl.rows[0].picks["moneyline"].price == D("0.47"), "the catalogue's ask"
        assert board.complete and board.count == len(rows_)
        assert sorted(reader.events_asked[0]) == sorted(f"polymarket:{e['id']}" for e in POLY["gamma_events"])

    async def test_polymarket_us_rows_carry_their_own_teams(self):
        board = await board_for("polymarket_us", ListedPaper(us_rows(), venue="polymarket_us"), None)
        assert rows(game(board, "CD Tolima vs. AD Pasto"))[:2] == [("CD Tolima", "TOL"), ("AD Pasto", "ADP")]

    async def test_a_venue_without_combo_lists_has_an_empty_board(self):
        board = await board_for("limitless", PaperRfqVenue(venue="limitless"), None)
        assert board.events == [] and board.count == 0


class TestGammaEventsByIds:
    def test_batched_in_the_order_asked_with_the_teams_kept(self):
        from synpath.polymarket import Polymarket

        calls = []

        class Gamma:
            def get(self, path, params=None):
                calls.append((path, params))
                return [dict(e, markets=[]) for e in reversed(POLY["gamma_events"])]

        poly = Polymarket.__new__(Polymarket)
        poly.gamma = Gamma()
        ids_ = [f"polymarket:{e['id']}" for e in POLY["gamma_events"]] + ["polymarket:999"]
        found = poly.fetch_events_by_ids(ids_)
        assert [e.venue_event_id for e in found] == [e["id"] for e in POLY["gamma_events"]]
        assert found[0].info["teams"][0]["abbreviation"]
        assert calls[0][0] == "/events" and ("id", "999") in calls[0][1]


def test_names_match_by_whole_words_only():
    from synpath.engine.combo_board import same_name

    assert same_name("Patriots", "New England Patriots") and same_name("Chidekh", "Clement Chidekh")
    assert not same_name("Ohio State", "Penn State"), "two teams sharing a last word are two teams"
    assert not same_name("Bears", "Bearsden")
