"""Hyperliquid normalizers and adapter, against payloads recorded from the live venue."""
from __future__ import annotations

import pytest

from synpath import BadRequest, Hyperliquid, MarketNotFound, NotSupported
from synpath.errors import ExchangeError
from synpath.hyperliquid import (
    BASE_MAKER_RATE, BASE_TAKER_RATE, Catalog, bucket_label, coin, fee_multiplier,
    fee_schedule_of, merge_candles, normalize_candle, normalize_order_book,
    normalize_trade, parse_fields, parse_time, render, show_time,
)

from conftest import load


def templates():
    return {row["id"]: row for row in load("hyperliquid_outcome_templates.json")}


def contexts():
    return {row["coin"]: row for row in load("hyperliquid_spot_ctxs.json")[1] if row["coin"].startswith("#")}


@pytest.fixture(scope="module")
def catalog():
    return Catalog(load("hyperliquid_outcome_meta.json"), templates(), contexts())


class FakeHttp:
    """Answers the venue's POST /info by request type, and records the bodies."""

    def __init__(self, answers):
        self.calls: list[dict] = []
        self.answers = answers

    def post(self, path, json=None):
        assert path == "/info"
        self.calls.append(json)
        answer = self.answers.get(json["type"])
        if answer is None:
            raise AssertionError(f"unexpected request {json}")
        return answer(json) if callable(answer) else answer

    def get(self, path, params=None):  # pragma: no cover - the venue has no GETs
        raise AssertionError(f"unexpected GET {path}")

    def close(self):
        pass


RECORDED = {
    "outcomeMeta": lambda body: load("hyperliquid_outcome_meta.json"),
    "outcomeTemplates": lambda body: load("hyperliquid_outcome_templates.json"),
    "spotMetaAndAssetCtxs": lambda body: load("hyperliquid_spot_ctxs.json"),
    "l2Book": lambda body: load("hyperliquid_book.json"),
    "recentTrades": lambda body: load("hyperliquid_trades.json"),
    "candleSnapshot": lambda body: load("hyperliquid_candles_1h.json"),
}


def venue_with(answers=None):
    venue = Hyperliquid(limiter=None)
    venue.http = FakeHttp({**RECORDED, **(answers or {})})
    return venue


class TestFields:
    def test_fields_split_on_the_first_colon_only(self):
        """A HIP-3 perp is named `dex:COIN`, so a value can hold a colon."""
        assert parse_fields("perp:xyz:OURA|threshold:50") == {"perp": "xyz:OURA", "threshold": "50"}

    def test_a_bare_word_has_no_fields(self):
        assert parse_fields("other") == {}

    def test_times_are_utc(self):
        assert parse_time("20261005-1330") == 1791207000000
        assert show_time("20261005-1330") == "2026-10-05 13:30 UTC"
        assert parse_time("soon") is None

    def test_a_missing_field_is_left_out_not_printed(self):
        assert render("{competition} {stage}: {a} v {b}", {"competition": "NFL", "a": "X", "b": "Y"}) == "NFL: X v Y"

    def test_coins(self):
        assert coin(7544) == "#75440" and coin("7544", 1) == "#75441"


class TestCatalog:
    def test_every_outcome_is_a_market(self, catalog):
        meta = load("hyperliquid_outcome_meta.json")
        assert set(catalog.markets) == {str(raw["outcome"]) for raw in meta["outcomes"]}

    def test_a_template_title_is_rendered(self, catalog):
        market = catalog.markets["7173"]
        assert market.title == "BTC touches 87500 by 2026-11-01 00:00 UTC"
        assert market.close_timestamp == parse_time("20261101-0000")
        assert market.category == "price"
        assert "{" not in (market.description or "")
        assert "metadata=" not in (market.description or "")

    def test_side_names_come_from_the_fields(self, catalog):
        nfl = catalog.markets["6208"]
        assert nfl.title == "NFL Regular Season: Washington Commanders v Indianapolis Colts"
        assert (nfl.yes.label, nfl.no.label) == ("WAS", "IND")
        total = catalog.markets["7032"]
        assert (total.yes.label, total.no.label) == ("Over", "Under")

    def test_question_outcomes_belong_to_their_question(self, catalog):
        arsenal = catalog.markets["1473"]
        assert arsenal.event_id == "hyperliquid:q198"
        assert arsenal.outcome_label == "Arsenal"
        assert arsenal.title == "2026/2027 English Premier League winner: Arsenal"
        assert arsenal.tags == ["sports", "football/soccer"]

    def test_the_fallback_is_other(self, catalog):
        other = catalog.markets["1472"]
        assert other.outcome_label == "Other" and other.event_id == "hyperliquid:q198"

    def test_a_question_is_an_event_with_every_outcome(self, catalog):
        event = catalog.events["q198"]
        assert event.id == "hyperliquid:q198"
        assert event.title == "2026/2027 English Premier League winner"
        assert event.mutually_exclusive is True
        assert {m.venue_market_id for m in event.markets} == {"1472", *map(str, range(1473, 1479))}

    def test_a_standalone_outcome_is_its_own_event(self, catalog):
        event = catalog.events["7173"]
        assert event.id == "hyperliquid:7173" and [m.id for m in event.markets] == ["hyperliquid:7173"]
        assert event.mutually_exclusive is None

    def test_recurring_markets_get_titles_from_their_fields(self, catalog):
        binary = catalog.markets["7544"]
        assert binary.title.startswith("BTC at or above 85971 at 2026-10-03 06:00 UTC")
        assert binary.category == "price" and "btc" in binary.tags
        assert catalog.markets["7551"].outcome_label == "Below 84251"
        assert catalog.markets["7552"].outcome_label == "84251 to 87690"
        assert catalog.markets["7553"].outcome_label == "87690 or above"
        assert catalog.markets["7550"].outcome_label == "Other"

    def test_bucket_labels(self):
        assert [bucket_label(i, ["1", "2"]) for i in range(3)] == ["Below 1", "1 to 2", "2 or above"]

    def test_volume_is_counted_in_contracts(self, catalog):
        market = catalog.markets["7544"]
        ctx = contexts()["#75440"]
        assert market.stats.volume_24h == float(ctx["dayBaseVlm"])
        assert market.stats.volume_unit == "contracts"

    def test_markets_are_one_book(self, catalog):
        market = catalog.markets["7544"]
        assert market.book_model == "shared_complement"
        assert (market.yes.venue_token_id, market.no.venue_token_id) == ("#75440", "#75441")

    def test_catalog_quotes_are_empty_not_invented(self, catalog):
        market = catalog.markets["7544"]
        assert market.yes.quote.bid is None and market.yes.quote.last is None


class TestBook:
    def test_the_yes_book_is_best_first(self):
        book = normalize_order_book(load("hyperliquid_book.json"), market_id="hyperliquid:7544")
        assert book.bids == sorted(book.bids, key=lambda level: -level.price)
        assert book.asks == sorted(book.asks, key=lambda level: level.price)
        assert book.best_bid.price < book.best_ask.price
        assert book.depth_scope == "top_n" and not book.derived

    def test_the_no_side_is_the_venues_no_coin(self):
        """Mirroring the YES book reproduces the NO coin's own book exactly."""
        derived = normalize_order_book(load("hyperliquid_book.json"), market_id="m", side="no")
        native = normalize_order_book(load("hyperliquid_book_no_coin.json"), market_id="m", side="yes")
        assert derived.derived
        assert [(l.price, l.size) for l in derived.bids] == [(l.price, l.size) for l in native.bids]
        assert [(l.price, l.size) for l in derived.asks] == [(l.price, l.size) for l in native.asks]

    def test_an_unknown_side_is_refused(self):
        with pytest.raises(BadRequest):
            normalize_order_book(load("hyperliquid_book.json"), market_id="m", side="maybe")  # type: ignore[arg-type]


class TestTrades:
    def test_the_taker_side_reads_from_the_yes_leg(self):
        rows = load("hyperliquid_trades.json")
        trades = [normalize_trade(row, market_id="hyperliquid:7544") for row in rows]
        for row, trade in zip(rows, trades):
            assert trade.side == {"B": "buy", "A": "sell"}[row["side"]]
            assert trade.price == float(row["px"]) and trade.amount == float(row["sz"])
            assert trade.id == str(row["tid"])


class TestCandles:
    def test_bars_are_traded_prices_with_volume(self):
        candles = [normalize_candle(row) for row in load("hyperliquid_candles_1h.json")]
        assert candles and all(c.price_source == "trade" for c in candles)
        assert all(c.volume is not None and c.trade_count is not None for c in candles)

    def test_merging_keeps_open_close_and_sums_volume(self):
        candles = [normalize_candle(row) for row in load("hyperliquid_candles_1h.json")]
        merged = merge_candles(candles, 6 * 3600)
        assert sum(c.volume for c in merged) == pytest.approx(sum(c.volume for c in candles))
        first = [c for c in candles if c.timestamp // 21_600_000 == merged[0].timestamp // 21_600_000]
        assert merged[0].open == first[0].open and merged[0].close == first[-1].close


class TestFees:
    def test_the_multiplier_follows_the_deployer_scale(self):
        assert fee_multiplier(None) == 1.0
        assert fee_multiplier("0.5") == 1.5
        assert fee_multiplier("1.0") == 2.0
        assert fee_multiplier("3") == 6.0

    def test_a_schedule_charges_the_notional_on_closing(self, catalog):
        raw = catalog.markets["7173"].info["outcome"]
        schedule = fee_schedule_of(raw, market_id="7173")
        assert schedule.fee_type == "hyperliquid_outcome"
        assert schedule.taker_rate == pytest.approx(BASE_TAKER_RATE * 2)
        assert schedule.maker_rate == pytest.approx(BASE_MAKER_RATE * 2)
        assert schedule.estimate(0.5, 100) == pytest.approx(0.07)


class TestAdapter:
    def test_markets_are_paged_by_volume(self):
        venue = venue_with()
        first = venue.fetch_markets(limit=5)
        second = venue.fetch_markets(limit=5, cursor=first.next_cursor)
        volumes = [m.stats.volume_24h or 0 for m in [*first, *second]]
        assert volumes == sorted(volumes, reverse=True)
        assert not {m.id for m in first} & {m.id for m in second}

    def test_the_catalog_is_read_once(self):
        venue = venue_with()
        venue.fetch_markets(limit=5)
        venue.fetch_events(limit=5)
        venue.fetch_market("hyperliquid:7173")
        assert [call["type"] for call in venue.http.calls].count("outcomeMeta") == 1

    def test_search_covers_the_whole_catalog(self):
        found = venue_with().fetch_markets(query="premier league arsenal", limit=100)
        assert [m.venue_market_id for m in found] == ["1473"]

    def test_newest_first(self):
        found = venue_with().fetch_markets(sort="newest", limit=10)
        assert [int(m.venue_market_id) for m in found] == sorted((int(m.venue_market_id) for m in found), reverse=True)

    def test_unsupported_orderings_and_states_raise(self):
        venue = venue_with()
        with pytest.raises(NotSupported):
            venue.fetch_markets(sort="liquidity")
        with pytest.raises(NotSupported):
            venue.fetch_markets(status="closed")
        with pytest.raises(BadRequest):
            venue.fetch_markets(cursor="p2")

    def test_a_question_id_is_not_a_market(self):
        with pytest.raises(MarketNotFound, match="question"):
            venue_with().fetch_market("hyperliquid:q198")

    def test_an_unknown_outcome_is_not_found_after_a_fresh_read(self):
        venue = venue_with()
        with pytest.raises(MarketNotFound):
            venue.fetch_market("99999999")
        assert [call["type"] for call in venue.http.calls].count("outcomeMeta") == 2

    def test_by_ids_keeps_order_and_skips_unknown(self):
        found = venue_with().fetch_markets_by_ids(["hyperliquid:7173", "1", "6208"])
        assert [m.venue_market_id for m in found] == ["7173", "6208"]

    def test_a_book_reads_the_yes_coin(self):
        venue = venue_with()
        book = venue.fetch_order_book("hyperliquid:7544", side="no")
        assert venue.http.calls[-1] == {"type": "l2Book", "coin": "#75440"}
        assert book.market_id == "hyperliquid:7544" and book.side == "no" and book.derived

    def test_quotes_come_from_the_book_and_the_last_print(self, catalog):
        venue = venue_with()
        market = venue.refresh_quotes(catalog.markets["7544"])
        book = load("hyperliquid_book.json")
        assert market.yes.quote.bid == float(book["levels"][0][0]["px"])
        assert market.no.quote.ask == pytest.approx(1 - market.yes.quote.bid)
        assert market.yes.quote.last is not None and market.yes.quote.last_timestamp

    def test_trades_are_oldest_first_and_have_no_further_pages(self):
        venue = venue_with()
        trades = venue.fetch_trades("hyperliquid:7544")
        assert [t.timestamp for t in trades] == sorted(t.timestamp for t in trades)
        assert trades.next_cursor is None
        with pytest.raises(BadRequest):
            venue.fetch_trades("hyperliquid:7544", cursor="i1")

    def test_ohlcv_asks_the_venue_for_the_yes_coin(self):
        venue = venue_with()
        candles = venue.fetch_ohlcv("hyperliquid:7544", timeframe="1h", limit=5)
        request = venue.http.calls[-1]["req"]
        assert request["coin"] == "#75440" and request["interval"] == "1h"
        assert len(candles) <= 5

    def test_a_timeframe_the_venue_lacks_is_built(self):
        venue = venue_with()
        venue.fetch_ohlcv("hyperliquid:7544", timeframe="6h", limit=2)
        assert venue.http.calls[-1]["req"]["interval"] == "2h"

    def test_fee_schedule(self):
        schedule = venue_with().fetch_fee_schedule("hyperliquid:7173")
        assert schedule.scope_id == "7173" and schedule.taker_rate == pytest.approx(0.0014)

    def test_an_unexpected_catalog_is_an_error(self):
        venue = venue_with({"outcomeMeta": lambda body: ["nope"]})
        with pytest.raises(ExchangeError):
            venue.fetch_markets()
