"""Polymarket US normalizers, against payloads recorded from the live gateway."""
from __future__ import annotations

import pytest

from synpath import BadRequest, NotSupported, PolymarketUS
from synpath.polymarket_us import (
    MAKER_THETA, amount, candles_from_price_history, fee_schedule_of, normalize_event,
    normalize_market, normalize_order_book, normalize_series, parse_ts, quoted,
    reflect_candle, _status_flags,
)


class FakeHttp:
    """Records what the adapter asked for and answers with what it was given."""

    def __init__(self, answers=None):
        self.calls: list[tuple[str, object]] = []
        self.answers = answers or {}

    def get(self, path, params=None):
        self.calls.append((path, params))
        answer = self.answers.get(path, {"events": [], "markets": [], "series": [], "history": []})
        return answer(params) if callable(answer) else answer

    def close(self):
        pass


class TestMarket:
    def test_identity_is_the_slug(self, polyus_market):
        """Every data endpoint on this venue keys on the slug, so it is the id."""
        market = normalize_market(polyus_market)
        assert market.venue == "polymarket_us"
        assert market.id == f"polymarket_us:{polyus_market['slug']}"
        assert market.venue_market_id == polyus_market["slug"]
        assert market.info["id"] == polyus_market["id"]

    def test_one_book_two_views(self, polyus_market):
        market = normalize_market(polyus_market)
        assert market.book_model == "shared_complement"

    def test_labels_come_from_the_sides(self, polyus_market):
        market = normalize_market(polyus_market)
        assert [market.yes.label, market.no.label] == ["Yes", "No"]

    def test_catalog_quote_is_the_top_of_the_yes_book(self, polyus_market):
        market = normalize_market(polyus_market)
        assert market.yes.quote.bid == 0.106
        assert market.yes.quote.ask == 0.107
        assert market.yes.quote.mid == pytest.approx(0.1065)

    def test_no_view_is_the_same_two_orders_reflected(self, polyus_market):
        market = normalize_market(polyus_market)
        assert market.no.quote.bid == pytest.approx(1 - 0.107)
        assert market.no.quote.ask == pytest.approx(1 - 0.106)

    def test_what_the_catalog_does_not_carry_is_none(self, polyus_market):
        """No last trade, no sizes, no volume in the listing payload. `None`,
        never zero: a zero would claim the venue said so."""
        market = normalize_market(polyus_market)
        for instrument in (market.yes, market.no):
            assert instrument.quote.last is None
            assert instrument.quote.bid_size is None and instrument.quote.ask_size is None
        assert market.stats.volume_24h is None
        assert market.stats.volume_total is None
        assert market.stats.open_interest is None
        assert market.stats.volume_unit == "contracts"

    def test_status_open_and_accepting(self, polyus_market):
        market = normalize_market(polyus_market)
        assert market.status == "open" and market.active is True
        assert market.native_status == "MARKET_STATUS_OPEN"

    def test_resolved_market_is_settled_with_no_prices(self, polyus_closed_market):
        """A resolved market is `active: true` and `closed: true` at once and
        prints its sides at 1 and 0. The enum decides; the edge values are not
        prices."""
        assert polyus_closed_market["active"] is True and polyus_closed_market["closed"] is True
        market = normalize_market(polyus_closed_market)
        assert market.status == "settled" and market.active is False
        assert market.yes.quote.bid is None and market.yes.quote.ask is None

    def test_sports_sides_are_labelled_by_team(self, polyus_closed_market):
        market = normalize_market(polyus_closed_market)
        labels = [market.yes.label, market.no.label]
        assert labels == [s["description"] for s in polyus_closed_market["marketSides"]]
        assert "Yes" not in labels

    def test_closed_but_unresolved_is_closed(self, polyus_market):
        market = normalize_market({**polyus_market, "closed": True, "status": "MARKET_STATUS_CLOSED"})
        assert market.status == "closed" and market.active is False

    def test_open_but_halted_is_open_and_not_accepting(self, polyus_market):
        market = normalize_market({**polyus_market, "status": "MARKET_STATUS_HALTED"})
        assert market.status == "open" and market.active is False

    def test_outcome_label_is_the_row_inside_the_question(self, polyus_market):
        market = normalize_market(polyus_market)
        assert market.title == polyus_market["question"]
        assert market.outcome_label == polyus_market["titleShort"]
        assert market.outcome_label != market.title

    def test_outcome_label_absent_when_it_would_repeat_the_question(self, polyus_market):
        market = normalize_market({**polyus_market, "titleShort": polyus_market["question"]})
        assert market.outcome_label is None

    def test_tick_size_and_face_value(self, polyus_market):
        market = normalize_market(polyus_market)
        assert market.tick_size == 0.001
        assert market.face_value == 1.0

    def test_neg_risk_and_exclusivity_are_unknown_not_false(self, polyus_market, polyus_event):
        assert normalize_market(polyus_market).neg_risk is None
        assert normalize_event(polyus_event).mutually_exclusive is None

    def test_timestamps_are_ms_plus_iso(self, polyus_market):
        market = normalize_market(polyus_market)
        assert market.close_timestamp > 10**12
        assert market.close_datetime.endswith("Z")

    def test_url_without_event_context(self, polyus_market):
        market = normalize_market(polyus_market)
        assert market.url == f"https://polymarket.us/market/{polyus_market['slug']}"

    def test_event_context_supplies_event_id_series_and_url(self, polyus_event):
        raw = polyus_event["markets"][0]
        market = normalize_market(raw, polyus_event)
        assert market.event_id == f"polymarket_us:{polyus_event['slug']}"
        assert market.series_id == polyus_event["seriesSlug"]
        assert market.url == f"https://polymarket.us/event/{polyus_event['slug']}"
        assert market.tags


class TestEvent:
    def test_nests_markets_and_keys_on_the_slug(self, polyus_event):
        event = normalize_event(polyus_event)
        assert event.venue == "polymarket_us"
        assert event.id == f"polymarket_us:{polyus_event['slug']}"
        assert event.venue_event_id == polyus_event["slug"]
        assert len(event.markets) == len(polyus_event["markets"])
        assert event.url == f"https://polymarket.us/event/{event.venue_event_id}"

    def test_tags_are_slugs(self, polyus_event):
        event = normalize_event(polyus_event)
        assert event.tags == [t["slug"] for t in polyus_event["tags"]]
        assert event.series_id == polyus_event["seriesSlug"]

    def test_raw_payload_kept_without_the_markets(self, polyus_event):
        event = normalize_event(polyus_event)
        assert "markets" not in event.info and event.info["id"] == polyus_event["id"]


class TestOrderBook:
    def test_yes_view_is_the_venues_own(self, polyus_book):
        book = normalize_order_book(polyus_book, slug="m", side="yes")
        data = polyus_book["marketData"]
        assert book.derived is False
        assert book.best_bid.price == float(data["bids"][0]["px"]["value"])
        assert book.best_ask.price == float(data["offers"][0]["px"]["value"])
        assert book.best_bid.size == float(data["bids"][0]["qty"])

    def test_best_first_on_both_sides(self, polyus_book):
        book = normalize_order_book(polyus_book, slug="m", side="yes")
        assert book.bids[0].price >= book.bids[-1].price
        assert book.asks[0].price <= book.asks[-1].price
        assert book.best_ask.price > book.best_bid.price

    def test_no_view_is_reflected_and_says_so(self, polyus_book):
        yes = normalize_order_book(polyus_book, slug="m", side="yes")
        no = normalize_order_book(polyus_book, slug="m", side="no")
        assert no.derived is True and no.book_model == "shared_complement"
        assert no.best_bid.price == pytest.approx(1 - yes.best_ask.price)
        assert no.best_ask.price == pytest.approx(1 - yes.best_bid.price)
        assert no.best_bid.size == yes.best_ask.size

    def test_the_book_names_its_market_and_side(self, polyus_book):
        book = normalize_order_book(polyus_book, slug="m", side="no")
        assert book.market_id == "polymarket_us:m" and book.side == "no"

    def test_depth_truncates(self, polyus_book):
        book = normalize_order_book(polyus_book, slug="m", side="yes", depth=2)
        assert len(book.bids) == 2 and len(book.asks) == 2
        assert book.depth_scope == "top_n"

    def test_nanosecond_timestamp_is_read(self, polyus_book):
        book = normalize_order_book(polyus_book, slug="m", side="yes")
        assert book.timestamp == parse_ts(polyus_book["marketData"]["transactTime"])
        assert book.datetime.endswith("Z")

    def test_state_is_kept_in_the_raw_payload(self, polyus_book):
        book = normalize_order_book(polyus_book, slug="m", side="yes")
        assert book.info["marketData"]["state"] == "MARKET_STATE_OPEN"


class TestPriceHistoryCandles:
    def test_bars_say_they_are_quote_derived(self, polyus_prices):
        candles = candles_from_price_history(polyus_prices["history"], interval_seconds=3600)
        assert candles
        assert all(c.price_source == "bid_ask_mid" for c in candles)
        assert all(c.volume is None for c in candles)

    def test_the_pair_behind_each_sample_is_recovered(self, polyus_prices):
        """`longPrice` is the best ask, `shortPrice` is one minus the best
        bid, by the venue's own documentation."""
        first = polyus_prices["history"][0]
        candle = candles_from_price_history([first], interval_seconds=3600)[0]
        assert candle.ask_close == first["longPrice"]
        assert candle.bid_close == pytest.approx(1 - first["shortPrice"])
        assert candle.close == pytest.approx((candle.bid_close + candle.ask_close) / 2)

    def test_bars_are_ordered_and_aligned(self, polyus_prices):
        candles = candles_from_price_history(polyus_prices["history"], interval_seconds=3600)
        stamps = [c.timestamp for c in candles]
        assert stamps == sorted(stamps)
        assert all(stamp % (3600 * 1000) == 0 for stamp in stamps)

    def test_reflection_swaps_extremes_and_sides(self, polyus_prices):
        yes = candles_from_price_history(polyus_prices["history"], interval_seconds=86400)[0]
        no = reflect_candle(yes)
        assert no.high == pytest.approx(1 - yes.low)
        assert no.low == pytest.approx(1 - yes.high)
        assert no.bid_close == pytest.approx(1 - yes.ask_close)


class TestFees:
    def test_published_thetas(self, polyus_market):
        fee = fee_schedule_of(polyus_market)
        assert fee.fee_type == "quadratic_theta"
        assert fee.taker_rate == polyus_market["feeCoefficient"]
        assert fee.maker_rate == MAKER_THETA < 0

    def test_taker_estimate_matches_the_venues_worked_example(self, polyus_market):
        """The fee document: $1.74 per 100 contracts at 50c for a taker."""
        fee = fee_schedule_of(polyus_market)
        assert fee.estimate(price=0.5, contracts=100) == pytest.approx(1.74, abs=0.005)

    def test_maker_estimate_is_a_rebate(self, polyus_market):
        fee = fee_schedule_of(polyus_market)
        assert fee.estimate(price=0.5, contracts=100, taker=False) == pytest.approx(-0.3125)

    def test_no_coefficient_means_no_schedule(self, polyus_market):
        assert fee_schedule_of({**polyus_market, "feeCoefficient": None}) is None


class TestSeries:
    def test_keys_on_the_slug_and_carries_no_fee(self, polyus_series):
        series = normalize_series(polyus_series["series"][0])
        assert series.id == polyus_series["series"][0]["slug"]
        assert series.fee is None


class TestHelpers:
    def test_nanosecond_fractions_parse(self):
        """Regression: the trimmer once swept up the digits of `+00:00` too,
        which dropped the zone, and a naive datetime is read as local time --
        every book timestamp came out an hour off in London."""
        from datetime import datetime, timezone

        expected = int(datetime(2026, 9, 17, 12, 37, 57, 586746, tzinfo=timezone.utc).timestamp() * 1000)
        assert parse_ts("2026-09-17T12:37:57.586746678Z") == expected
        assert parse_ts("2026-09-17T12:37:57.586746678+00:00") == expected
        assert parse_ts("2026-09-17T12:37:57Z") == expected - 586
        assert parse_ts("2026-09-17T12:37:57") == expected - 586, "no zone means UTC, not local"
        assert parse_ts(1789131600) == 1789131600000
        assert parse_ts(None) is None

    def test_money_fields_and_placeholders(self):
        assert amount({"value": "0.1060", "currency": "USD"}) == 0.106
        assert quoted({"value": "1.0000"}) is None
        assert quoted({"value": "0"}) is None
        assert quoted(None) is None
        assert quoted({"value": "0.42"}) == 0.42


class TestSort:
    """The venue ignores `orderBy` except by id, so the page is ordered here."""

    def _rows(self, polyus_market, n=3):
        rows = []
        for index in range(n):
            row = dict(polyus_market)
            row["slug"] = f"m-{index}"
            row["id"] = str(100 + index)
            row["createdAt"] = f"2026-0{index + 1}-01T00:00:00Z"
            rows.append(row)
        return rows

    def test_newest_reads_the_catalog_and_costs_nothing_extra(self, polyus_market):
        venue = PolymarketUS(limiter=None)
        venue.http = FakeHttp({"/markets": {"markets": self._rows(polyus_market)}})
        page = venue.fetch_markets(sort="newest", limit=3)
        assert [m.venue_market_id for m in page] == ["m-2", "m-1", "m-0"]
        assert len(venue.http.calls) == 1

    def test_volume_reads_each_market_once_and_carries_the_figures(self, polyus_market):
        shares = {"m-0": "10", "m-1": "300", "m-2": "20"}

        def bbo(slug):
            return {"marketData": {"sharesTraded": shares[slug], "openInterest": "5",
                                   "bidShares": "1", "askShares": "2"}}

        answers = {"/markets": {"markets": self._rows(polyus_market)}}
        answers.update({f"/markets/{s}/bbo": bbo(s) for s in shares})
        venue = PolymarketUS(limiter=None)
        venue.http = FakeHttp(answers)
        page = venue.fetch_markets(sort="volume", limit=3)
        assert [m.venue_market_id for m in page] == ["m-1", "m-2", "m-0"]
        assert [m.stats.volume_total for m in page] == [300.0, 20.0, 10.0]
        assert page[0].stats.liquidity == 3.0 and page[0].stats.open_interest == 5.0
        bbo_calls = [p for p, _ in venue.http.calls if p.endswith("/bbo")]
        assert len(bbo_calls) == 3

    def test_liquidity_is_resting_shares(self, polyus_market):
        rows = self._rows(polyus_market, n=2)
        answers = {"/markets": {"markets": rows},
                   "/markets/m-0/bbo": {"marketData": {"bidShares": "5", "askShares": "5"}},
                   "/markets/m-1/bbo": {"marketData": {"bidShares": "50", "askShares": "0"}}}
        venue = PolymarketUS(limiter=None)
        venue.http = FakeHttp(answers)
        assert [m.venue_market_id for m in venue.fetch_markets(sort="liquidity", limit=2)] == ["m-1", "m-0"]


class TestCapabilities:
    def test_sort_is_honoured(self):
        assert PolymarketUS.has["sort"] is True

    def test_no_trade_tape_over_rest(self):
        assert PolymarketUS.has["fetch_trades"] is False
        with pytest.raises(NotSupported, match="trade tape"):
            PolymarketUS(limiter=None).fetch_trades("anything")

    def test_ohlcv_is_partial(self):
        assert PolymarketUS.has["fetch_ohlcv"] == "partial"

    def test_series_and_fees_and_search(self):
        assert PolymarketUS.has["fetch_series"] is True
        assert PolymarketUS.has["fetch_fee_schedule"] is True
        assert PolymarketUS.has["search"] is True


class TestPaging:
    """The venue pages by offset; the cursor carries it, tagged with the query."""

    def _venue(self, answers=None):
        venue = PolymarketUS(limiter=None)
        venue.http = FakeHttp(answers)
        return venue

    def test_listing_walks_in_id_order(self, polyus_market):
        venue = self._venue({"/markets": {"markets": [polyus_market]}})
        venue.fetch_markets(limit=1)
        path, params = venue.http.calls[0]
        assert path == "/markets"
        assert params["orderBy"] == "id" and params["orderDirection"] == "ASC"
        assert params["active"] == "true" and params["closed"] == "false"

    def test_full_page_carries_a_cursor_and_the_cursor_carries_the_offset(self, polyus_market):
        venue = self._venue({"/markets": {"markets": [polyus_market] * 3}})
        first = venue.fetch_markets(limit=3)
        assert first.next_cursor
        venue.fetch_markets(limit=3, cursor=first.next_cursor)
        assert venue.http.calls[-1][1]["offset"] == 3

    def test_short_page_ends_the_walk(self, polyus_market):
        venue = self._venue({"/markets": {"markets": [polyus_market]}})
        assert venue.fetch_markets(limit=5).next_cursor is None

    def test_a_cursor_from_another_query_is_refused(self, polyus_market):
        venue = self._venue({"/markets": {"markets": [polyus_market] * 2}})
        cursor = venue.fetch_markets(limit=2, status="open").next_cursor
        with pytest.raises(BadRequest, match="different query"):
            venue.fetch_markets(limit=2, status="all", cursor=cursor)

    def test_a_foreign_cursor_is_refused(self):
        with pytest.raises(BadRequest, match="not a cursor"):
            self._venue().fetch_markets(cursor="12345")

    def test_page_limit_is_the_shared_ceiling(self, polyus_market):
        from synpath.base import MAX_PAGE_LIMIT

        venue = self._venue()
        venue.fetch_markets(limit=5000)
        assert venue.http.calls[0][1]["limit"] == MAX_PAGE_LIMIT

    def test_events_page_the_same_way(self, polyus_event):
        venue = self._venue({"/events": {"events": [polyus_event] * 2}})
        page = venue.fetch_events(limit=2)
        assert page.next_cursor and venue.http.calls[0][1]["orderBy"] == "id"


class TestStatusVocabulary:
    def test_flags_for_each_word(self):
        assert _status_flags("open") == ("true", "false")
        assert _status_flags("closed") == (None, "true")
        assert _status_flags("settled") == (None, "true")
        assert _status_flags("all") == (None, None)

    def test_unknown_status_is_refused(self):
        with pytest.raises(BadRequest, match="unknown status"):
            _status_flags("bogus")

    def test_settled_and_closed_are_told_apart_by_the_enum(self, polyus_market, polyus_closed_market):
        """The venue's flags cannot separate them; a row's own status can."""
        unresolved = {**polyus_market, "closed": True, "status": "MARKET_STATUS_CLOSED"}
        venue = PolymarketUS(limiter=None)
        venue.http = FakeHttp({"/markets": {"markets": [polyus_closed_market, unresolved]}})
        assert [m.status for m in venue.fetch_markets(status="settled")] == ["settled"]
        assert [m.status for m in venue.fetch_markets(status="closed")] == ["closed"]


class TestSearch:
    def test_uses_the_query_parameter_and_pages_by_number(self, polyus_search):
        """The parameter is `query`; `q` is silently ignored and answers the
        default listing, which reads as results rather than as an error."""
        venue = PolymarketUS(limiter=None)
        venue.http = FakeHttp({"/search": polyus_search})
        page = venue.fetch_events(query="fed", limit=2)
        path, params = venue.http.calls[0]
        assert path == "/search" and params["query"] == "fed" and params["page"] == 1
        assert page and page.next_cursor
        venue.fetch_events(query="fed", limit=2, cursor=page.next_cursor)
        assert venue.http.calls[-1][1]["page"] == 2

    def test_market_search_flattens_the_events(self, polyus_search):
        venue = PolymarketUS(limiter=None)
        venue.http = FakeHttp({"/search": polyus_search})
        markets = venue.fetch_markets(query="fed", limit=100, status="all")
        expected = sum(len(e["markets"]) for e in polyus_search["events"])
        assert len(markets) == expected

    def test_market_search_honours_limit_and_resumes(self, polyus_search):
        """Regression: `limit` bounded the events asked for, so `limit=5`
        returned every market of five events (21 of them, measured)."""
        venue = PolymarketUS(limiter=None)
        venue.http = FakeHttp({"/search": polyus_search})
        first = venue.fetch_markets(query="fed", limit=2, status="all")
        assert len(first) == 2 and first.next_cursor
        rest = venue.fetch_markets(query="fed", limit=100, status="all", cursor=first.next_cursor)
        ids = [m.id for m in first] + [m.id for m in rest]
        assert len(ids) == len(set(ids)) == sum(len(e["markets"]) for e in polyus_search["events"])


class TestLookups:
    def test_fetch_market_by_slug_and_by_numeric_id(self, polyus_market):
        venue = PolymarketUS(limiter=None)
        venue.http = FakeHttp({
            f"/market/slug/{polyus_market['slug']}": {"market": polyus_market},
            f"/market/id/{polyus_market['id']}": {"market": polyus_market},
        })
        slug = polyus_market["slug"]
        assert venue.fetch_market(slug).id == f"polymarket_us:{slug}"
        assert venue.fetch_market(polyus_market["id"]).id == f"polymarket_us:{slug}"
        assert venue.fetch_market(f"polymarket_us:{slug}").id == f"polymarket_us:{slug}"

    def test_batch_lookup_keeps_the_order_and_drops_the_missing(self, polyus_market, polyus_closed_market):
        venue = PolymarketUS(limiter=None)
        venue.http = FakeHttp({"/markets": {"markets": [polyus_closed_market, polyus_market]}})
        wanted = [polyus_market["slug"], "gone-market", polyus_closed_market["slug"]]
        got = venue.fetch_markets_by_ids(wanted)
        assert [m.venue_market_id for m in got] == [polyus_market["slug"], polyus_closed_market["slug"]]
        params = venue.http.calls[0][1]
        assert ("slug", polyus_market["slug"]) in params and ("slug", "gone-market") in params

    def test_batch_lookup_batches_numeric_ids_separately(self, polyus_market):
        venue = PolymarketUS(limiter=None)
        venue.http = FakeHttp({"/markets": {"markets": [polyus_market]}})
        venue.fetch_markets_by_ids([polyus_market["slug"], polyus_market["id"]])
        fields = [{k for k, _ in params if k != "limit"} for _, params in venue.http.calls]
        assert fields == [{"slug"}, {"id"}]

    def test_series_by_slug_and_by_id(self, polyus_series):
        raw = polyus_series["series"][0]
        venue = PolymarketUS(limiter=None)
        venue.http = FakeHttp({
            "/series": polyus_series,
            f"/series/id/{raw['id']}": {"series": raw},
        })
        assert venue.fetch_series(raw["slug"]).id == raw["slug"]
        assert venue.fetch_series(raw["id"]).id == raw["slug"]

    def test_fee_schedule_reads_the_market(self, polyus_market):
        venue = PolymarketUS(limiter=None)
        venue.http = FakeHttp({f"/market/slug/{polyus_market['slug']}": {"market": polyus_market}})
        fee = venue.fetch_fee_schedule(polyus_market["slug"])
        assert fee.taker_rate == polyus_market["feeCoefficient"]


class TestRefreshQuotes:
    def test_the_book_fills_what_the_catalog_lacks(self, polyus_market, polyus_book):
        venue = PolymarketUS(limiter=None)
        venue.http = FakeHttp({f"/markets/{polyus_market['slug']}/book": polyus_book})
        listed = normalize_market(polyus_market)
        assert listed.yes.quote.last is None and listed.stats.open_interest is None
        fresh = venue.refresh_quotes(listed)
        stats = polyus_book["marketData"]["stats"]
        assert fresh.yes.quote.last == float(stats["lastTradePx"]["value"])
        assert fresh.yes.quote.last_timestamp == parse_ts(stats["lastTradeSetTime"])
        assert fresh.no.quote.last == pytest.approx(1 - fresh.yes.quote.last)
        assert fresh.yes.quote.bid_size and fresh.no.quote.ask_size == fresh.yes.quote.bid_size
        assert fresh.stats.open_interest == float(stats["openInterest"])
        assert fresh.stats.volume_total == float(stats["sharesTraded"])
        assert fresh.native_status == "MARKET_STATE_OPEN" and fresh.active is True
        assert listed.yes.quote.last is None, "the input is not mutated"

    def test_a_halted_book_turns_active_off(self, polyus_market, polyus_book):
        halted = {"marketData": {**polyus_book["marketData"], "state": "MARKET_STATE_HALTED"}}
        venue = PolymarketUS(limiter=None)
        venue.http = FakeHttp({f"/markets/{polyus_market['slug']}/book": halted})
        fresh = venue.refresh_quotes(normalize_market(polyus_market))
        assert fresh.status == "open" and fresh.active is False


class TestOhlcvRequests:
    def _venue(self, history):
        venue = PolymarketUS(limiter=None)
        venue.http = FakeHttp({"/price-history": {"history": history}})
        return venue

    def test_relative_window_uses_the_venues_fixed_interval(self):
        venue = self._venue([])
        venue.fetch_ohlcv("m", timeframe="1h")
        params = venue.http.calls[0][1]
        assert params["symbol"] == "m" and params["fidelity"] == 1, "coarser fidelities come back empty"
        assert params["fixedInterval"] == "INTERVAL_1W" and "timestamp.startTimestamp" not in params

    def test_daily_reads_the_whole_history_at_daily_fidelity(self):
        venue = self._venue([])
        venue.fetch_ohlcv("m", timeframe="1d")
        params = venue.http.calls[0][1]
        assert params["fixedInterval"] == "INTERVAL_ALL" and params["fidelity"] == 1440

    def test_minute_samples_are_bucketed_into_hours(self):
        base = 1_700_000_000 // 3600 * 3600
        history = [{"timestamp": base + 60 * i, "longPrice": 0.40 + i / 1000, "shortPrice": 0.62} for i in range(120)]
        candles = self._venue(history).fetch_ohlcv("m", timeframe="1h")
        assert [c.timestamp for c in candles] == [base * 1000, (base + 3600) * 1000]

    def test_absolute_window_is_sent_and_enforced(self):
        until = 1_700_000_000_000
        inside = until // 1000 - 3600
        history = [
            {"timestamp": inside, "longPrice": 0.4, "shortPrice": 0.7},
            {"timestamp": until // 1000 + 999_999, "longPrice": 0.9, "shortPrice": 0.2},
        ]
        venue = self._venue(history)
        candles = venue.fetch_ohlcv("m", timeframe="1h", until=until, limit=10)
        params = venue.http.calls[0][1]
        assert params["fidelity"] == 1
        assert params["timestamp.endTimestamp"] == until // 1000
        assert params["timestamp.startTimestamp"] == until // 1000 - 3600 * 10
        assert [c.ask_close for c in candles] == [0.4]

    def test_candles_are_in_the_yes_price(self):
        history = [{"timestamp": 1_700_000_000, "longPrice": 0.4, "shortPrice": 0.7}]
        venue = self._venue(history)
        candle = venue.fetch_ohlcv("polymarket_us:m", timeframe="1h")[0]
        assert candle.ask_close == pytest.approx(0.4)

    def test_another_venues_id_is_refused(self):
        with pytest.raises(BadRequest, match="belongs to kalshi"):
            self._venue([]).fetch_ohlcv("kalshi:m", timeframe="1h")
