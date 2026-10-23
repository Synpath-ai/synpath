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

    def test_a_game_names_its_competition_from_the_events_series(self):
        board = poly_board()
        assert game(board, "Rayo Vallecano de Madrid vs. Athletic Club").competition == "La Liga"
        assert game(board, "Patriots vs. Bears").competition == "NFL"

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

    def test_a_game_names_its_competition_unless_the_category_already_does(self):
        board = board_of("kalshi", kalshi_markets(kalshi_events()))
        assert game(board, "Monza vs Napoli").competition == "Serie A"
        assert game(board, "New England vs Chicago").competition is None  # "Pro Football": the category says NFL

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


def test_series_slugs_read_as_names():
    from synpath.engine.combo_board import competition_of_series
    assert competition_of_series("premier-league-2025") == "Premier League"
    assert competition_of_series("la-liga") == "La Liga"
    assert competition_of_series("dota-2") == "Dota 2"
    assert competition_of_series("atp-shanghai") == "ATP Shanghai"
    assert competition_of_series(None) is None


def test_the_board_search_finds_a_game_by_its_competition():
    from synpath.engine.combo_board import board_page
    board = poly_board()
    found = board_page(board, query="la liga").events
    assert found and all(e.competition == "La Liga" for e in found)


def test_a_polymarket_game_takes_its_series_title_over_its_slug():
    from synpath.engine.combo_board import competition_of_event
    assert competition_of_event({"seriesSlug": "ecu1-games", "series": [{"title": "LigaPro Primera A"}]}) == "LigaPro Primera A"
    assert competition_of_event({"seriesSlug": "premier-league-2025", "series": [{"title": "Premier League 2025"}]}) == "Premier League"
    assert competition_of_event({"series": [{"title": "La Liga 2025-26"}]}) == "La Liga"
    assert competition_of_event({"seriesSlug": "dota-2"}) == "Dota 2"
    assert competition_of_event({}) is None


def test_the_cached_gamma_event_keeps_only_series_titles():
    from synpath.engine.combo_board import static_gamma
    kept = static_gamma({"events": [{"id": "1", "seriesSlug": "ecu1-games", "series": [{"title": "LigaPro Primera A", "image": "x", "id": 9}]}]})
    assert kept["events"][0]["series"] == [{"title": "LigaPro Primera A"}]



def test_the_start_order_puts_games_ahead_first_then_old_games_then_questions():
    from synpath.engine.combo_board import BoardEvent, ComboBoard, board_page
    now = int(__import__("time").time() * 1000)
    hour = 3_600_000

    def event(event_id, kind, start, category="soccer"):
        return BoardEvent(id=event_id, venue="polymarket", kind=kind, title=event_id, category=category,
                          category_label=category.title(), group="sports", start_time=start)
    board = ComboBoard(venue="polymarket", count=5, complete=True, events=[
        event("question-soon", "question", now + hour),
        event("stale", "game", now - 300 * hour),
        event("tomorrow", "game", now + 24 * hour, category="nba"),
        event("live", "game", now - hour),
        event("tonight", "game", now + 5 * hour),
    ])
    page = board_page(board, sort="start")
    assert [e.id for e in page.events] == ["live", "tonight", "tomorrow", "stale", "question-soon"]


def test_a_category_names_its_leagues_by_volume_and_a_page_takes_one_league():
    from synpath.engine.combo_board import BoardEvent, board_page, categories_of, ComboBoard
    from decimal import Decimal

    def event(event_id, competition, volume):
        return BoardEvent(id=event_id, venue="polymarket", kind="game", title=event_id, category="soccer",
                          category_label="Soccer", group="sports", competition=competition, volume=Decimal(volume))
    events = [event("a", "Eerste Divisie", 5_000), event("b", "Eerste Divisie", 4_000), event("c", "Premier League", 434_000),
              event("d", "Brazil Serie A", 8_000), event("e", "Serie A", 20_000)]
    soccer = categories_of(events)[0]
    assert soccer.competitions == ["Premier League", "Serie A", "Eerste Divisie", "Brazil Serie A"]
    board = ComboBoard(venue="polymarket", count=5, complete=True, events=events)
    assert [e.id for e in board_page(board, competition="Serie A").events] == ["e"]


def test_a_polymarket_us_game_takes_the_gateways_series_title():
    from synpath.engine.combo_board import us_competition
    titles = {"alsv": "Allsvenskan", "eflch": "EFL Championship", "ipl-2026": "IPL 2026"}
    assert us_competition("alsv", titles) == "Allsvenskan"
    assert us_competition("ipl-2026", titles) == "IPL"
    assert us_competition("odi-cr", titles) == "Odi Cr"  # not listed: the slug read as words


def test_a_settled_polymarket_us_market_is_left_off_the_board():
    from synpath.engine.combo_board import is_finished_us_market
    assert is_finished_us_market({"status": "MARKET_STATUS_RESOLVED", "closed": True, "outcomePrices": ["0", "1"]})
    assert is_finished_us_market({"closed": True})
    assert not is_finished_us_market({"status": "MARKET_STATUS_OPEN", "closed": False})


def test_a_market_with_its_result_proposed_leaves_the_board_and_an_empty_side_takes_the_venues_price():
    from synpath.engine.combo_board import polymarket_markets, pick_of
    from types import SimpleNamespace
    rows = [SimpleNamespace(market_id="polymarket:1", title="A vs B", tags=[], info={}),
            SimpleNamespace(market_id="polymarket:2", title="C vs D", tags=[], info={})]
    details = {"1": {"question": "A vs B", "umaResolutionStatus": "proposed", "outcomePrices": '["0.0005", "0.9995"]'},
               "2": {"question": "C vs D", "outcomePrices": '["0.30", "0.70"]', "bestAsk": "0.31"}}
    markets = polymarket_markets(rows, details, {})
    assert [m.market_id for m in markets] == ["polymarket:2"]
    market = markets[0].model_copy(update={"yes_bid": None})
    assert pick_of(market, "no", "D", "moneyline").price == __import__("decimal").Decimal("0.70")  # 1 - the venue's YES price



def test_a_game_polymarket_marks_ended_leaves_the_board_before_its_result_is_proposed():
    from synpath.engine.combo_board import polymarket_markets
    from types import SimpleNamespace
    rows = [SimpleNamespace(market_id="polymarket:1", title="maybe vs eternal premium", tags=[], info={})]
    ended = {"1": {"question": "maybe vs eternal premium", "outcomePrices": '["0.9995", "0.0005"]',
                   "events": [{"id": "9", "ended": True, "score": "000-000|2-1|Bo3", "period": "3/3"}]}}
    assert polymarket_markets(rows, ended, {}) == []
    ended["1"]["events"][0].update(ended=False, live=True)
    assert len(polymarket_markets(rows, ended, {})) == 1

def test_games_under_way_are_read_from_gamma_again_each_build():
    from synpath.engine.combo_board import BoardCache, natives_not_cached
    from types import SimpleNamespace
    cache = BoardCache()
    cache.markets = {"10": {"gameStartTime": "2026-10-23 10:00:00+00"}, "11": {"gameStartTime": "2027-01-01 10:00:00+00"}}
    rows = [SimpleNamespace(market_id=f"polymarket:{n}") for n in ("10", "11", "12")]
    now = 1_792_760_000_000  # 2026-10-23, after market 10's start
    _, read_now = natives_not_cached(rows, cache, now_ms=now)
    assert read_now == ["polymarket:10", "polymarket:12"]



def test_an_empty_side_takes_the_last_trade_then_the_other_side_of_the_book():
    from decimal import Decimal as D

    from synpath.engine.combo_board import BoardMarket, pick_of

    traded = BoardMarket(market_id="k:A", title="A", event_key="g", yes_bid=D("0.40"), yes_price=D("0.45"))
    assert pick_of(traded, "yes", "A", "moneyline").price == D("0.45")
    bid_only = BoardMarket(market_id="k:B", title="B", event_key="g", yes_bid=D("0.40"))
    assert pick_of(bid_only, "yes", "B", "moneyline").price == D("0.40")
    assert pick_of(bid_only, "no", "B", "moneyline").price == D("0.60")


def test_a_game_whose_winner_side_is_empty_stays_off_the_board():
    from decimal import Decimal as D

    from synpath.engine.combo_board import BoardMarket, Side, board_of

    sp, boom = Side(key="sp", name="Saigon Phantom", abbr="SP"), Side(key="boom", name="BOOM Esports", abbr="BOOM")
    def game(ask_sp):
        return [
            BoardMarket(market_id="k:SP", title="SP", event_key="g", kind="moneyline", yes_side=sp,
                        participants=[sp, boom], yes_ask=ask_sp),
            BoardMarket(market_id="k:BOOM", title="BOOM", event_key="g", kind="moneyline", yes_side=boom,
                        participants=[sp, boom], yes_ask=D("0.50")),
        ]
    assert board_of("kalshi", game(None)).events == []
    assert [e.kind for e in board_of("kalshi", game(D("0.52"))).events] == ["game"]
    question = BoardMarket(market_id="k:Q", title="Will it rain?", event_key="q")
    assert board_of("kalshi", [question]).events == []



def test_a_polymarket_game_with_no_liquidity_and_no_volume_stays_off_the_board():
    from synpath.engine.combo_board import board_of, polymarket_markets
    from types import SimpleNamespace
    teams = [{"name": "Hermans/Houkes", "abbreviation": "HERMHOU", "ordering": "home"},
             {"name": "Bianchi/Vrba", "abbreviation": "BIANVRB", "ordering": "away"}]
    def game(**prices):
        return {"1": {"question": "Florence (Doubles): Hermans/Houkes vs Bianchi/Vrba", "sportsMarketType": "moneyline",
                      "outcomes": '["Hermans/Houkes", "Bianchi/Vrba"]',
                      "events": [{"id": "9", "title": "Florence (Doubles): Hermans/Houkes vs Bianchi/Vrba", "teams": teams}],
                      **prices}}
    rows = [SimpleNamespace(market_id="polymarket:1", title="Florence", tags=["sports", "tennis"], info={})]
    never_quoted = polymarket_markets(rows, game(outcomePrices='["0.5", "0.5"]', bestAsk=1, liquidity=None), {})
    assert board_of("polymarket", never_quoted).events == []
    # A thin book (money resting, nothing traded, no best bid or ask read) is listed, as Polymarket lists it.
    thin = polymarket_markets(rows, game(outcomePrices='["0.715", "0.285"]', liquidityNum=1601.07), {})
    assert [e.kind for e in board_of("polymarket", thin).events] == ["game"]
    traded = polymarket_markets([SimpleNamespace(market_id="polymarket:1", title="Florence", tags=["sports", "tennis"],
                                                 info={"volume": "12.5"})], game(outcomePrices='["0.5", "0.5"]'), {})
    assert [e.kind for e in board_of("polymarket", traded).events] == ["game"]



def test_the_main_total_is_the_line_priced_nearest_even_not_a_wide_book_whose_midpoint_is():
    from decimal import Decimal as D

    from synpath.engine.combo_board import BoardMarket, Side, picks_for

    buf, wsh = Side(key="buf", name="Buffalo", abbr="BUF"), Side(key="wsh", name="Washington", abbr="WSH")
    def line(n, bid, ask, last=None):
        return BoardMarket(market_id=f"k:T{n}", title=f"Over {n}", event_key="g", kind="total", line=D(n),
                           yes_bid=D(bid), yes_ask=D(ask), yes_price=D(last) if last else None)
    # Kalshi, 2026-10-23: Buffalo vs Washington's 8.5 had a 1¢ bid and a 97¢ ask; 6.5 traded at 57¢.
    lines = [line("8.5", "0.01", "0.97"), line("6.5", "0.45", "0.51", "0.57"), line("7.5", "0.21", "0.32")]
    over, under = picks_for("total", lines, [buf, wsh])
    assert over.market_id == "k:T6.5" and over.price == D("0.51") and under.price == D("0.55")
    wide = picks_for("total", [line("8.5", "0.01", "0.97"), line("7.5", "0.21", "0.32")], [buf, wsh])
    assert wide[0].market_id == "k:T7.5", "a wide book is still a market; only the edges of one are not"
    assert picks_for("total", [line("3.5", "0.01", "0.99")], [buf, wsh]) == [None, None], "only edges: no total"
