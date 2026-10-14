"""SX Bet normalizers and adapter, against payloads recorded from the live venue."""
from __future__ import annotations

import pytest

from synpath import BadRequest, MarketNotFound, NotSupported
from synpath.sx_bet import (
    SxBet, candles_from_trades, events_of, fee_schedule_of, normalize_event, normalize_market,
    normalize_order_book, normalize_trade, status_of,
)

from conftest import load

ACTIVE = load("sx_bet_active.json")["data"]
BOOK = load("sx_bet_book.json")["data"]
TRADES = load("sx_bet_trades.json")["data"]["trades"]
MARKET = load("sx_bet_market.json")["data"][0]
SEARCH = load("sx_bet_search.json")
LEAGUES = load("sx_bet_leagues.json")
ROWS = ACTIVE["markets"]


class FakeHttp:
    """Answers GETs by path from recorded payloads, and records them."""

    def __init__(self, answers):
        self.calls: list[tuple[str, object]] = []
        self.answers = answers

    def get(self, path, params=None):
        self.calls.append((path, params))
        if path in self.answers:
            answer = self.answers[path]
            return answer(params) if callable(answer) else answer
        raise MarketNotFound(f"sx_bet: no {path}")

    def close(self):
        pass


def venue_with(answers):
    venue = SxBet(limiter=None)
    venue.http = FakeHttp(answers)
    return venue


def row_of(kind):
    return next(r for r in ROWS if r["type"] == kind)


class TestMarket:
    def test_a_market_is_named_by_its_hash_and_its_fixture_is_the_event(self):
        market = normalize_market(MARKET)
        assert market.id == f"sx_bet:{MARKET['marketHash'].lower()}"
        assert market.event_id == f"sx_bet:{MARKET['sportXeventId']}"
        assert market.book_model == "shared_complement" and market.tick_size == 0.00125

    def test_outcome_one_is_yes_under_the_venues_names(self):
        total = normalize_market(row_of(28))
        assert total.yes.label.startswith("Over") and total.no.label.startswith("Under")
        assert total.outcome_label == total.yes.label
        assert total.title.endswith(f"— {total.yes.label}")
        assert total.info["line"] == row_of(28)["line"] and total.info["type_name"] == "Under/Over Including Overtime"

    def test_the_title_names_both_teams(self):
        row = row_of(226)
        market = normalize_market(row)
        assert market.title.startswith(f"{row['teamOneName']} vs {row['teamTwoName']}")

    def test_a_player_prop_keeps_its_player(self):
        prop = normalize_market(row_of(995))
        assert prop.yes.label.endswith("- Yes") and prop.info["type_name"] == "Anytime Touchdown Scorer"

    def test_sport_and_league_are_the_tags(self):
        row = row_of(52)
        market = normalize_market(row)
        assert market.tags[:2] == [row["sportLabel"].lower(), row["leagueLabel"].lower()]
        assert market.series_id == str(row["leagueId"])

    @pytest.mark.parametrize("raw, expected", [
        ({"status": "ACTIVE"}, ("open", True)),
        ({"status": "INACTIVE"}, ("closed", False)),
        ({"status": "INACTIVE", "reportedDate": 1791900000, "outcome": 1}, ("settled", False)),
        ({"status": "ACTIVE", "outcome": 0}, ("settled", False)),
    ])
    def test_statuses(self, raw, expected):
        status, _, accepting = status_of(raw)
        assert (status, accepting) == expected

    @pytest.mark.parametrize("outcome, winner", [(1, "yes"), (2, "no"), (0, "void")])
    def test_a_settled_market_says_who_won(self, outcome, winner):
        market = normalize_market({**MARKET, "status": "INACTIVE", "reportedDate": 1791900000, "outcome": outcome})
        assert market.status == "settled" and market.info["winning_outcome"] == winner
        assert market.resolution_timestamp == 1791900000 * 1000


class TestEvent:
    def test_rows_group_into_their_fixtures(self):
        events = events_of(ROWS)
        assert sum(len(e.markets) for e in events) == len(ROWS)
        for event in events:
            assert {m.event_id for m in event.markets} == {event.id}

    def test_an_event_is_named_by_its_teams(self):
        rows = [r for r in ROWS if r["sportXeventId"] == ROWS[0]["sportXeventId"]]
        event = normalize_event(rows)
        assert event.title == f"{rows[0]['teamOneName']} vs {rows[0]['teamTwoName']}"
        assert event.status == "open" and event.info["game_time"] == rows[0]["gameTime"] * 1000


class TestOrderBook:
    def test_outcome_one_bids_and_outcome_two_asks(self):
        book = normalize_order_book(BOOK, market_id="sx_bet:x")
        top_one = max(int(r["percentageOdds"]) for r in BOOK["outcomeOne"]) / 1e20
        top_two = max(int(r["percentageOdds"]) for r in BOOK["outcomeTwo"]) / 1e20
        assert book.bids[0].price == pytest.approx(top_one)
        assert book.asks[0].price == pytest.approx(1 - top_two)
        assert all(a.price > b.price for a, b in zip(book.asks[:1], book.bids[:1]))

    def test_a_levels_size_is_its_stake_over_its_own_price(self):
        level = {"percentageOdds": "40000000000000000000", "size": "2000000"}
        book = normalize_order_book({"outcomeOne": [level], "outcomeTwo": [level]}, market_id="sx_bet:x")
        assert book.bids[0].price == 0.4 and book.bids[0].size == pytest.approx(5.0)
        assert book.asks[0].price == pytest.approx(0.6) and book.asks[0].size == pytest.approx(5.0)

    def test_no_is_the_mirror(self):
        yes = normalize_order_book(BOOK, market_id="sx_bet:x")
        no = normalize_order_book(BOOK, market_id="sx_bet:x", side="no")
        assert no.derived and no.bids[0].price == pytest.approx(1 - yes.asks[0].price)
        assert no.asks[0].size == yes.bids[0].size

    def test_depth_and_unknown_side(self):
        assert len(normalize_order_book(BOOK, market_id="sx_bet:x", depth=1).bids) <= 1
        with pytest.raises(BadRequest):
            normalize_order_book(BOOK, market_id="sx_bet:x", side="maybe")


class TestTrades:
    def test_a_bet_on_outcome_one_buys_yes_at_its_price(self):
        row = {"tradeId": "t", "isBettingOutcomeOne": True, "weightedAverageOdds": "40000000000000000000",
               "totalStake": "2000000", "totalReturn": "5000000", "betTime": "2026-10-13T09:00:00.000Z"}
        trade = normalize_trade(row, market_id="sx_bet:x")
        assert (trade.side, trade.price, trade.quantity) == ("buy", 0.4, 5.0)

    def test_a_bet_on_outcome_two_sells_yes_at_one_minus_its_price(self):
        row = {"tradeId": "t", "isBettingOutcomeOne": False, "weightedAverageOdds": "40000000000000000000",
               "totalStake": "2000000", "totalReturn": "5000000", "betTime": "2026-10-13T09:00:00.000Z"}
        trade = normalize_trade(row, market_id="sx_bet:x")
        assert (trade.side, trade.price, trade.quantity) == ("sell", 0.6, 5.0)

    def test_recorded_tape(self):
        trades = [normalize_trade(r, market_id="sx_bet:x") for r in TRADES]
        assert trades and all(0 < t.price < 1 and t.quantity > 0 for t in trades)
        assert not any("userAddress" in t.info for t in trades)

    def test_candles_carry_volume(self):
        trades = [normalize_trade(r, market_id="sx_bet:x") for r in TRADES]
        candles = candles_from_trades(trades, interval_seconds=3600)
        assert candles and sum(c.trade_count for c in candles) == len(trades)
        assert all(c.price_source == "trade" and c.low <= c.close <= c.high for c in candles)


class TestFees:
    def test_a_taker_pays_one_percent_of_the_profit(self):
        fee = fee_schedule_of("0xabc")
        assert fee.fee_type == "profit_on_win"
        assert fee.estimate(0.4, 100) == pytest.approx(0.6)
        assert fee.estimate(0.4, 100, taker=False) == 0.0


class TestAdapter:
    def test_listing_pages_follow_the_venues_key(self):
        pages = {None: {"markets": ROWS[:3], "nextKey": "k2"}, "k2": {"markets": ROWS[3:5]}}
        venue = venue_with({"/markets/active": lambda p: {"status": "success", "data": pages[p.get("paginationKey")]}})
        first = venue.fetch_markets(limit=2)
        assert len(first) == 2 and first.next_cursor == ":2"
        rest = venue.fetch_markets(limit=10, cursor=first.next_cursor)
        assert [m.venue_market_id for m in rest] == [r["marketHash"].lower() for r in ROWS[2:5]]
        assert rest.next_cursor is None

    def test_only_open_markets_and_no_sort(self):
        venue = venue_with({})
        with pytest.raises(NotSupported):
            venue.fetch_markets(status="settled")
        with pytest.raises(NotSupported):
            venue.fetch_markets(sort="volume")

    def test_search_reads_each_matching_fixture(self):
        event_ids = [f["eventId"] for f in SEARCH["data"]]
        venue = venue_with({
            "/search": SEARCH,
            "/markets/active": lambda p: {"status": "success", "data": {"markets": [
                {**ROWS[0], "sportXeventId": p["eventId"], "marketHash": "0x" + f"{event_ids.index(p['eventId']):064x}"}]}},
        })
        events = venue.fetch_events(query="lakers")
        assert [e.venue_event_id for e in events] == event_ids
        with pytest.raises(BadRequest):
            venue.fetch_markets(query="la")

    def test_events_walk_one_league_a_page(self):
        leagues = [row["leagueId"] for row in LEAGUES["data"]]
        venue = venue_with({
            "/leagues/active": LEAGUES,
            "/markets/active": lambda p: {"status": "success", "data": {"markets": [
                {**r, "leagueId": p["leagueId"]} for r in ROWS]}},
        })
        page = venue.fetch_events()
        assert page.next_cursor == "1" and all(e.series_id == str(leagues[0]) for e in page)
        last = venue.fetch_events(cursor=str(len(leagues) - 1))
        assert last.next_cursor is None

    def test_lookups_skip_malformed_hashes_and_batch(self):
        venue = venue_with({"/markets/find": lambda p: {"status": "success", "data": [
            {**MARKET, "marketHash": h} for h in p["marketHashes"].split(",")]}})
        hashes = ["0x" + f"{n:064x}" for n in range(35)]
        found = venue.fetch_markets_by_ids(["sx_bet:0xdead", *hashes])
        assert [m.venue_market_id for m in found] == hashes
        assert len([c for c in venue.http.calls if c[0] == "/markets/find"]) == 2
        with pytest.raises(MarketNotFound):
            venue.fetch_market("0xdead")

    def test_book_and_trades(self):
        venue = venue_with({
            "/orderbook-v3/snapshot": load("sx_bet_book.json"),
            "/trades-v3/public": load("sx_bet_trades.json"),
        })
        market_hash = BOOK["marketHash"]
        book = venue.fetch_order_book(market_hash)
        assert book.market_id == f"sx_bet:{market_hash}" and book.info["version"] == BOOK["version"]
        trades = venue.fetch_trades(f"sx_bet:{market_hash}")
        assert [t.timestamp for t in trades] == sorted(t.timestamp for t in trades)

    def test_a_failure_status_is_an_error(self):
        from synpath import ExchangeError
        venue = venue_with({"/markets/find": {"status": "failure", "errorCode": "X"}})
        with pytest.raises(ExchangeError):
            venue.fetch_markets_by_ids(["0x" + "0" * 64])
