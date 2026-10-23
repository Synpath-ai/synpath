"""Azuro normalizers and adapter, against payloads recorded from the live feed and bets subgraph."""
from __future__ import annotations

import pytest

import synpath
from synpath import BadRequest, MarketNotFound, NotSupported
from synpath.azuro import (
    CHAINS, Azuro, bets_where, chain_of, contracts_of_limits, fee_schedule_of, game_of_condition, market_native,
    next_bets_cursor, normalize_condition, normalize_event, normalize_trade, odds_price, outcome_order,
    parse_market_id, status_of,
)

from conftest import load

GAMES = load("azuro_games.json")
CONDITIONS = load("azuro_conditions.json")["conditions"]
RESOLVED = load("azuro_conditions_resolved.json")["conditions"]
BY_IDS = load("azuro_games_by_ids.json")["games"]
BATCH = load("azuro_condition_batch.json")["conditions"]
SEARCH = load("azuro_search.json")
NAVIGATION = load("azuro_navigation.json")
BETS = load("azuro_bets.json")["data"]["v3Bets"]

GAME = next(g for g in BY_IDS if g["gameId"] == "1006000000000094301994")
FINISHED = next(g for g in BY_IDS if g["gameId"] == "1006000000000002866602")
THREE_WAY = next(c for c in CONDITIONS if c["conditionId"] == "600610060000000000943019940000000000001406934671")
CORNERS = next(c for c in CONDITIONS if c["conditionId"] == "600610060000000000943019940000000000001802657415")
SETTLED_ID = "600610060000000000028666020000000000001624778125"


class FakeHttp:
    """Answers by method and path from recorded payloads, and records the calls."""

    def __init__(self, answers):
        self.calls: list[tuple[str, str, object]] = []
        self.answers = answers

    def _answer(self, method, path, payload):
        self.calls.append((method, path, payload))
        answer = self.answers.get(f"{method} {path}")
        if answer is None:
            raise MarketNotFound(f"azuro: no {path}")
        return answer(payload) if callable(answer) else answer

    def get(self, path, params=None):
        return self._answer("GET", path, params)

    def post(self, path, json=None):
        return self._answer("POST", path, json)

    def close(self):
        pass


def conditions_for(body):
    wanted = set(body["gameIds"])
    rows = [c for c in CONDITIONS + RESOLVED if c["game"]["gameId"] in wanted]
    return {"conditions": rows}


def batch_for(body):
    return {"conditions": [c for c in BATCH if c["conditionId"] in body["conditionIds"]]}


def venue_with(answers, graph=None):
    venue = Azuro(limiter=None)
    venue.http = FakeHttp(answers)
    venue.graph = FakeHttp(graph or {})
    return venue


FEED = {
    "POST /market-manager/conditions-by-game-ids": conditions_for,
    "POST /market-manager/games-by-ids": {"games": BY_IDS},
    "POST /market-manager/condition-batch": batch_for,
}


class TestPrices:
    @pytest.mark.parametrize("odds, price", [("2", 0.5), ("1.36", 0.73529412), ("4.95", 0.2020202), (3, 0.33333333)])
    def test_a_price_is_one_over_the_odds(self, odds, price):
        assert odds_price(odds) == price

    @pytest.mark.parametrize("odds", ["1", "0.9", None, "", "x"])
    def test_odds_of_one_or_less_are_no_price(self, odds):
        assert odds_price(odds) is None


class TestIds:
    def test_two_outcomes_are_one_market_larger_conditions_one_per_outcome(self):
        assert market_native("123") == "123" and market_native("123", 2) == "123-2"
        assert parse_market_id("123") == ("123", None) and parse_market_id("123-2") == ("123", "2")

    @pytest.mark.parametrize("bad", ["0xabc", "12-", "12-x", "", "12 3"])
    def test_anything_else_is_refused(self, bad):
        with pytest.raises(BadRequest):
            parse_market_id(bad)

    def test_a_condition_names_its_game(self):
        assert game_of_condition(THREE_WAY["conditionId"]) == THREE_WAY["game"]["gameId"]
        assert all(game_of_condition(c["conditionId"]) == c["game"]["gameId"] for c in CONDITIONS + RESOLVED)
        assert game_of_condition("123") is None

    def test_the_venue_is_registered(self):
        assert "azuro" in synpath.ids.VENUES and synpath.exchanges["azuro"] is Azuro
        assert synpath.ids.split("azuro:123-4") == ("azuro", "123-4")


class TestMarkets:
    def test_a_two_outcome_condition_is_one_market_lower_outcome_yes(self):
        [market] = normalize_condition(CORNERS, GAME)
        assert market.id == f"azuro:{CORNERS['conditionId']}" and market.event_id == f"azuro:{GAME['gameId']}"
        assert (market.yes.label, market.no.label) == ("Over 10.5", "Under 10.5")
        assert market.title == "Trefelin FC - Flint Town United — Total Corners Over/Under: Over 10.5"
        assert market.status == "open" and market.active and market.book_model == "shared_complement"

    def test_its_quote_is_the_pools_odds(self):
        [market] = normalize_condition(CORNERS, GAME)
        over, under = 1 / 2.04, 1 / 1.65
        assert market.yes.quote.ask == round(over, 8) and market.yes.quote.bid == round(1 - under, 8)
        assert market.no.quote.ask == round(under, 8) and market.no.quote.bid == round(1 - over, 8)
        assert market.yes.quote.bid < market.yes.quote.ask, "the pool's margin keeps the bid under the ask"
        assert market.info["odds"] == {"yes": "2.04", "no": "1.65"}

    def test_yes_is_fixed_by_outcome_id_not_the_feeds_order(self):
        reordered = {**CORNERS, "outcomes": list(reversed(CORNERS["outcomes"]))}
        assert normalize_condition(reordered, GAME)[0].yes.label == "Over 10.5"
        assert [o["outcomeId"] for o in outcome_order(THREE_WAY)] == ["1", "2", "3"]

    def test_a_larger_condition_is_one_market_per_outcome_with_only_yes_sold(self):
        markets = normalize_condition(THREE_WAY, GAME)
        assert [m.id for m in markets] == [f"azuro:{THREE_WAY['conditionId']}-{i}" for i in (1, 2, 3)]
        draw = markets[0]
        assert draw.yes.label == "Draw (+1)" and draw.outcome_label == "Draw (+1)"
        assert draw.yes.quote.ask == round(1 / 4.95, 8) and draw.yes.quote.bid is None and draw.no.quote.ask is None
        assert draw.event_id == f"azuro:{GAME['gameId']}" and "One of 3 outcomes" in draw.description

    def test_every_condition_in_a_game_normalizes(self):
        markets = [m for c in CONDITIONS for m in normalize_condition(c, GAME)]
        assert len(markets) == sum(1 if len(c["outcomes"]) == 2 else len(c["outcomes"]) for c in CONDITIONS)
        for market in markets:
            for quote in (market.yes.quote, market.no.quote):
                for price in (quote.bid, quote.ask, quote.mid):
                    assert price is None or 0 < price < 1

    def test_a_stopped_condition_is_closed_without_a_quote(self):
        stopped = next(c for c in CONDITIONS if c["state"] == "Stopped")
        [market] = normalize_condition(stopped, GAME)
        assert market.status == "closed" and not market.active and market.yes.quote.ask is None

    def test_a_resolved_condition_is_settled_with_its_winner(self):
        condition = next(c for c in BATCH if c["conditionId"] == SETTLED_ID)
        [market] = normalize_condition(condition, FINISHED)
        assert market.status == "settled" and market.native_status == "resolved"
        assert market.yes.label == "Over 11.5" and market.info["winning_outcome"] == "yes"
        assert market.yes.quote.ask is None

    def test_a_cancelled_condition_is_void(self):
        cancelled = {**CORNERS, "state": "Canceled"}
        assert status_of(cancelled)[0] == "settled"
        assert normalize_condition(cancelled, GAME)[0].info["winning_outcome"] == "void"

    def test_sport_league_and_country_are_the_tags(self):
        [market] = normalize_condition(CORNERS, GAME)
        assert market.tags == ["football", "cymru premier", "wales"] and market.category == "football"
        assert market.series_id == GAME["league"]["id"]

    def test_a_game_is_the_event(self):
        event = normalize_event(GAME, [c for c in CONDITIONS if c["game"]["gameId"] == GAME["gameId"]])
        assert event.id == f"azuro:{GAME['gameId']}" and event.title == GAME["title"]
        assert event.status == "open" and event.info["starts_at"] == int(GAME["startsAt"]) * 1000
        assert {m.event_id for m in event.markets} == {event.id}

    def test_a_finished_game_with_every_condition_settled_or_stopped(self):
        event = normalize_event(FINISHED, RESOLVED)
        assert event.status in ("closed", "settled") and event.native_status == "finished"


class TestTrades:
    def test_a_bet_on_the_no_outcome_is_a_sell_at_one_minus_its_price(self):
        bet = BETS[0]
        trade = normalize_trade(bet, market_id="azuro:x", yes_outcome="1", binary=True)
        assert trade.side == "sell" and trade.price == round(1 - 1 / 2.6, 8)
        assert trade.quantity == 28.6 and trade.timestamp == int(bet["createdBlockTimestamp"]) * 1000

    def test_a_bet_on_yes_is_a_buy(self):
        trade = normalize_trade(BETS[0], market_id="azuro:x", yes_outcome="2", binary=True)
        assert trade.side == "buy" and trade.price == round(1 / 2.6, 8)

    def test_on_one_outcome_of_a_larger_condition_other_outcomes_are_not_its_trades(self):
        assert normalize_trade(BETS[0], market_id="azuro:x-1", yes_outcome="1", binary=False) is None

    def test_a_combo_is_not_a_trade(self):
        combo = {**BETS[0], "selections": BETS[0]["selections"] * 2}
        assert normalize_trade(combo, market_id="azuro:x", yes_outcome="1", binary=True) is None

    def test_the_cursor_keeps_bets_that_share_the_boundary_second(self):
        rows = [{"id": "a", "createdBlockTimestamp": "20"}, {"id": "b", "createdBlockTimestamp": "10"},
                {"id": "c", "createdBlockTimestamp": "10"}]
        cursor = next_bets_cursor(rows, None, full=True)
        assert cursor == "10:b,c"
        where = bets_where("123", cursor)
        assert where["createdBlockTimestamp_lte"] == "10" and where["id_not_in"] == ["b", "c"]
        assert next_bets_cursor([{"id": "d", "createdBlockTimestamp": "10"}], cursor, full=True) == "10:b,c,d"
        assert next_bets_cursor(rows, None, full=False) is None
        with pytest.raises(BadRequest):
            bets_where("123", "x:y")


class TestFees:
    def test_nothing_on_top_of_the_odds(self):
        schedule = fee_schedule_of("123")
        assert schedule.fee_type == "pool_margin" and schedule.estimate(0.4, 100) == 0.0
        assert schedule.info["currency"] == "USDT"
        assert Azuro(chain="base").fetch_fee_schedule("azuro:123").info["currency"] == "WETH"


class TestChains:
    def test_chains_by_name_or_id(self):
        assert chain_of("polygon").chain_id == 137 and chain_of(8453).name == "base"
        assert chain_of("polygon-amoy").testnet and chain_of("gnosis").discontinued
        with pytest.raises(BadRequest):
            chain_of("solana")

    def test_each_chain_has_its_own_environment_and_endpoints(self):
        assert Azuro().environment == "PolygonUSDT" and Azuro(chain="base").environment == "BaseWETH"
        assert CHAINS["polygon_amoy"].api_url.startswith("https://dev-api.")
        assert CHAINS["base"].subgraph_url.endswith("azuro-api-base-v3")


class TestAdapter:
    def test_fetch_markets_walks_live_games_then_prematch(self):
        def games(params):
            if params["gameState"] == "Live":
                return {"games": [], "page": 1, "totalPages": 0}
            return {**GAMES, "games": [GAME]} if params["page"] == 1 else {"games": [], "totalPages": 1}

        venue = venue_with({**FEED, "GET /market-manager/games-by-filters": games})
        page = venue.fetch_markets(limit=5)
        assert len(page) == 5 and all(m.status == "open" for m in page)
        assert page.next_cursor == "Prematch:1:0:5"
        following = venue.fetch_markets(limit=500, cursor=page.next_cursor)
        assert following[0].id not in {m.id for m in page}
        params = [c[2] for c in venue.http.calls if c[1] == "/market-manager/games-by-filters"]
        assert params[0]["environment"] == "PolygonUSDT" and params[0]["perPage"] == 10

    def test_only_open_markets_are_listed(self):
        venue = venue_with({})
        for status in ("closed", "settled", "all"):
            with pytest.raises(NotSupported):
                venue.fetch_markets(status=status)
        assert not venue.http.calls

    def test_a_bad_cursor_is_refused(self):
        venue = venue_with({})
        for cursor in ("x", "Live:1", "Finished:1:0:0", "Live:a:0:0"):
            with pytest.raises(BadRequest):
                venue.fetch_markets(cursor=cursor)

    def test_search(self):
        venue = venue_with({**FEED, "GET /market-manager/search": SEARCH})
        markets = venue.fetch_markets(query="Trefelin", limit=3)
        assert len(markets) == 3 and markets[0].event_id == f"azuro:{GAME['gameId']}"
        events = venue.fetch_events(query="Trefelin")
        assert [e.id for e in events] == [f"azuro:{GAME['gameId']}"] and events.next_cursor is None
        with pytest.raises(BadRequest):
            venue.fetch_markets(query="ab")

    def test_fetch_events_pages_by_game_state(self):
        venue = venue_with({**FEED, "GET /market-manager/games-by-filters": {**GAMES, "games": [GAME]}})
        page = venue.fetch_events(limit=1)
        assert page[0].id == f"azuro:{GAME['gameId']}" and page.next_cursor == "Live:2"

    def test_fetch_market_reads_the_game_the_condition_names(self):
        venue = venue_with(FEED)
        market = venue.fetch_market(f"azuro:{CORNERS['conditionId']}")
        assert market.title.startswith(GAME["title"]) and market.yes.label == "Over 10.5"
        assert [c[1] for c in venue.http.calls] == ["/market-manager/conditions-by-game-ids", "/market-manager/games-by-ids"]

    def test_a_settled_market_is_found_too(self):
        market = venue_with(FEED).fetch_market(f"azuro:{SETTLED_ID}")
        assert market.status == "settled" and market.info["winning_outcome"] == "yes"

    def test_a_larger_condition_needs_an_outcome(self):
        venue = venue_with(FEED)
        with pytest.raises(BadRequest, match="3 outcomes, one market each"):
            venue.fetch_market(f"azuro:{THREE_WAY['conditionId']}")
        with pytest.raises(BadRequest, match="two outcomes"):
            venue.fetch_market(f"azuro:{CORNERS['conditionId']}-1")
        assert venue.fetch_market(f"azuro:{THREE_WAY['conditionId']}-3").yes.label == "Trefelin FC (-1)"

    def test_an_unknown_market(self):
        venue = venue_with({**FEED, "POST /market-manager/conditions-by-game-ids": {"conditions": []},
                            "POST /market-manager/condition-batch": {"conditions": []}})
        with pytest.raises(MarketNotFound):
            venue.fetch_market("azuro:600610060000000000943019940000000000000000000001")

    def test_many_markets_in_the_order_asked(self):
        ids = [f"azuro:{THREE_WAY['conditionId']}-2", f"azuro:{SETTLED_ID}", "azuro:999", f"azuro:{CORNERS['conditionId']}"]
        markets = venue_with(FEED).fetch_markets_by_ids(ids)
        assert [m.id for m in markets] == [ids[0], ids[1], ids[3]]

    def test_the_book_is_the_quote_sized_by_the_pools_limits(self):
        def limits(body):
            outcome = body["bets"][0]["outcomeId"]
            return {"response": {"minBet": 1, "maxBet": 100 if outcome == 1 else 10, "maxPayout": "150"}}

        venue = venue_with({**FEED, "POST /bet/calculation": limits})
        book = venue.fetch_order_book(f"azuro:{CORNERS['conditionId']}")
        assert book.asks[0].price == round(1 / 2.04, 8) and book.asks[0].size == 150.0
        assert book.bids[0].price == round(1 - 1 / 1.65, 8) and book.bids[0].size == 16.5
        assert book.depth_scope == "top_n" and book.info["pool"] is True
        sent = [c[2] for c in venue.http.calls if c[1] == "/bet/calculation"]
        assert [s["bets"][0]["outcomeId"] for s in sent] == [1, 2] and sent[0]["environment"] == "PolygonUSDT"
        no = venue.fetch_order_book(f"azuro:{CORNERS['conditionId']}", side="no")
        assert no.derived and no.asks[0].price == round(1 / 1.65, 8)

    def test_one_outcome_has_only_an_ask(self):
        venue = venue_with({**FEED, "POST /bet/calculation": {"response": {"maxBet": 5, "maxPayout": "20"}}})
        book = venue.fetch_order_book(f"azuro:{THREE_WAY['conditionId']}-1")
        assert book.bids == [] and book.asks[0].size == round(min(20.0, 5 * 4.95), 6)

    def test_limits_size_in_contracts(self):
        assert contracts_of_limits({"max_payout": 150.0, "max_bet": 100.0}, "2.04") == 150.0
        assert contracts_of_limits({"max_payout": None, "max_bet": None}, "2.04") is None

    def test_limits_are_read_per_selection(self):
        venue = venue_with({**FEED, "POST /bet/calculation": {"response": {"minBet": None, "maxBet": 50,
                                                                          "maxPayout": "120.5"}}})
        limits = venue.fetch_limits(f"azuro:{CORNERS['conditionId']}", side="sell", account="0x" + "1" * 40)
        assert limits["outcome_id"] == "2" and limits["max_payout"] == 120.5 and limits["min_bet"] is None
        assert venue.http.calls[-1][2]["wallet"] == "0x" + "1" * 40
        with pytest.raises(BadRequest):
            venue.fetch_limits(f"azuro:{THREE_WAY['conditionId']}-1", side="sell")

    def test_trades_come_from_the_subgraph(self):
        graph = {"POST ": {"data": {"v3Bets": BETS}}}
        venue = venue_with(FEED, graph)
        trades = venue.fetch_trades(f"azuro:{SETTLED_ID}")
        [(_, _, query)] = venue.graph.calls
        assert query["variables"]["where"] == {"_conditionIds_contains": [SETTLED_ID], "type": "Ordinar"}
        assert all(t.market_id == f"azuro:{SETTLED_ID}" for t in trades)
        assert [t.timestamp for t in trades] == sorted(t.timestamp for t in trades)
        # The recorded bets are on the game's other conditions too; the tape
        # here is filtered by the subgraph, so every row is read as this market's.
        assert trades.next_cursor is None

    def test_candles_from_the_tape(self):
        graph = {"POST ": {"data": {"v3Bets": BETS}}}
        candles = venue_with(FEED, graph).fetch_ohlcv(f"azuro:{SETTLED_ID}", timeframe="1h")
        assert candles and all(c.price_source == "trade" for c in candles)
        assert sum(c.trade_count or 0 for c in candles) == len(BETS)

    def test_subgraph_errors_raise(self):
        from synpath import ExchangeError

        venue = venue_with(FEED, {"POST ": {"errors": [{"message": "bad"}]}})
        with pytest.raises(ExchangeError):
            venue.fetch_trades(f"azuro:{SETTLED_ID}")

    def test_sports(self):
        sports = venue_with({"GET /market-manager/navigation": NAVIGATION}).fetch_sports()
        assert sports and {"slug", "countries", "activeGamesCount"} <= set(sports[0])

    def test_refresh_quotes(self):
        venue = venue_with(FEED)
        [market] = normalize_condition({**CORNERS, "outcomes": [{**o, "odds": "1.9"} for o in CORNERS["outcomes"]]}, GAME)
        fresh = venue.refresh_quotes(market)
        assert fresh.yes.quote.ask == round(1 / 2.04, 8) and fresh.title == market.title
