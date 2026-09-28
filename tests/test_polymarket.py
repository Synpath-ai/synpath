"""Polymarket normalizers, against payloads recorded from the live venue."""
from __future__ import annotations

import pytest

from synpath import BadRequest, Polymarket
from synpath.polymarket import (
    candles_from_price_history, candles_from_trades, fee_schedule_of, normalize_event,
    normalize_market, normalize_order_book, normalize_trade, parse_ts, quoted,
)


class TestMarket:
    def test_identity_and_shape(self, poly_market):
        market = normalize_market(poly_market)
        assert market.venue == "polymarket"
        assert market.id == f"polymarket:{poly_market['id']}"
        assert market.venue_market_id == str(poly_market["id"])
        assert market.book_model == "native_per_outcome"

    def test_token_ids_stay_on_the_outcomes(self, poly_market):
        market = normalize_market(poly_market)
        assert market.yes.venue_token_id.isdigit() and len(market.yes.venue_token_id) > 40
        assert market.no.venue_token_id != market.yes.venue_token_id

    def test_labels_come_from_the_venue(self, poly_market):
        market = normalize_market(poly_market)
        assert [market.yes.label, market.no.label] == ["Yes", "No"]

    def test_gamma_summary_quote_is_first_outcome_only(self, poly_market):
        """Gamma sends one bid/ask pair. Mirroring it onto the complement would
        publish a quote the venue never sent."""
        market = normalize_market(poly_market)
        assert market.yes.quote.bid == 0.10
        assert market.yes.quote.ask == 0.11
        assert market.no.quote.bid is None
        assert market.no.quote.ask is None

    def test_complement_last_is_derived_from_the_pair(self, poly_market):
        market = normalize_market(poly_market)
        assert market.yes.quote.last == 0.11
        assert market.no.quote.last == pytest.approx(0.89)

    def test_gamma_outcome_price_is_kept_out_of_the_quote(self, poly_market):
        """`outcomePrices` is Gamma's own mark, not one of the four book
        prices, so it stays in `info` where it cannot be mistaken for a quote."""
        market = normalize_market(poly_market)
        assert market.yes.info["gamma_outcome_price"] == 0.105
        assert market.yes.quote.mid == pytest.approx(0.105)

    def test_volume_unit_differs_from_kalshi(self, poly_market):
        stats = normalize_market(poly_market).stats
        assert stats.volume_unit == "collateral"
        assert stats.liquidity == poly_market["liquidityNum"]

    def test_open_interest_absent_rather_than_zero(self, poly_market):
        assert normalize_market(poly_market).stats.open_interest is None

    def test_status_active_and_accepting(self, poly_market):
        market = normalize_market(poly_market)
        assert market.status == "open"
        assert market.active is True

    def test_closed_market_is_not_settled_until_resolved(self, poly_market):
        market = normalize_market({
            **poly_market, "closed": True, "active": False, "acceptingOrders": False,
        })
        assert market.status == "closed"

    def test_resolved_market_is_settled(self, poly_market):
        market = normalize_market({
            **poly_market, "closed": True, "umaResolutionStatus": "resolved",
        })
        assert market.status == "settled"

    def test_tick_size_and_neg_risk(self, poly_market):
        market = normalize_market(poly_market)
        assert market.tick_size == 0.01
        assert market.neg_risk is True

    def test_timestamps_are_ms_plus_iso(self, poly_market):
        market = normalize_market(poly_market)
        assert market.close_timestamp > 10**12
        assert market.close_datetime.endswith("Z")


class TestEvent:
    def test_event_nests_markets(self, poly_event):
        event = normalize_event(poly_event)
        assert event.venue == "polymarket"
        assert len(event.markets) == len(poly_event.get("markets") or [])

    def test_neg_risk_absent_stays_unknown(self, poly_event):
        event = normalize_event({**poly_event, "negRisk": None})
        assert event.mutually_exclusive is None


class TestOrderBook:
    """The venue sends bids ascending and asks descending: worst price first."""

    def test_venue_really_does_send_them_backwards(self, poly_book):
        assert float(poly_book["bids"][0]["price"]) < float(poly_book["bids"][-1]["price"])
        assert float(poly_book["asks"][0]["price"]) > float(poly_book["asks"][-1]["price"])

    def test_best_first_after_normalizing(self, poly_book):
        book = normalize_order_book(poly_book)
        assert book.best_bid.price == 0.10
        assert book.best_ask.price == 0.11

    def test_spread_is_positive(self, poly_book):
        book = normalize_order_book(poly_book)
        assert book.best_ask.price > book.best_bid.price

    def test_not_derived(self, poly_book):
        book = normalize_order_book(poly_book)
        assert book.derived is False
        assert book.book_model == "native_per_outcome"

    def test_timestamp_parsed_from_ms_string(self, poly_book):
        book = normalize_order_book(poly_book)
        assert book.timestamp == int(poly_book["timestamp"])
        assert book.datetime.endswith("Z")

    def test_depth_truncates(self, poly_book):
        book = normalize_order_book(poly_book, depth=2)
        assert len(book.bids) == 2 and len(book.asks) == 2
        assert book.bids[0].price > book.bids[1].price


class TestTrade:
    def test_fields(self, poly_trades):
        raw = poly_trades[0]
        trade = normalize_trade(raw, market_id="polymarket:1", no_token="other")
        assert trade.price == raw["price"]
        assert trade.amount == raw["size"]
        assert trade.side == "buy"
        assert trade.market_id == "polymarket:1"

    def test_a_no_token_trade_is_reported_on_the_yes_leg(self, poly_trades):
        raw = {**poly_trades[0], "price": 0.30, "side": "BUY"}
        trade = normalize_trade(raw, market_id="polymarket:1", no_token=str(raw["asset"]))
        assert trade.price == pytest.approx(0.70)
        assert trade.side == "sell"

    def test_seconds_timestamp_becomes_ms(self, poly_trades):
        raw = poly_trades[0]
        trade = normalize_trade(raw)
        assert trade.timestamp == raw["timestamp"] * 1000


class TestPriceHistoryCandles:
    def test_bars_are_labelled_as_samples_not_trades(self, poly_prices):
        candles = candles_from_price_history(poly_prices["history"], interval_seconds=3600)
        assert candles
        assert all(candle.price_source == "sampled_mid" for candle in candles)

    def test_volume_is_null_not_zero(self, poly_prices):
        """The venue publishes no volume with price history. Zero would be a
        claim that nothing traded, which this data cannot support."""
        candles = candles_from_price_history(poly_prices["history"], interval_seconds=3600)
        assert all(candle.volume is None for candle in candles)

    def test_ohlc_within_the_bucket(self, poly_prices):
        candles = candles_from_price_history(poly_prices["history"], interval_seconds=3600)
        for candle in candles:
            assert candle.low <= candle.open <= candle.high
            assert candle.low <= candle.close <= candle.high

    def test_bars_are_ordered_and_aligned(self, poly_prices):
        candles = candles_from_price_history(poly_prices["history"], interval_seconds=3600)
        stamps = [candle.timestamp for candle in candles]
        assert stamps == sorted(stamps)
        assert all(stamp % (3600 * 1000) == 0 for stamp in stamps)


class TestFees:
    def test_schedule_read_from_the_market(self, poly_market):
        fee = fee_schedule_of(poly_market)
        assert fee is not None
        assert fee.fee_type == "quadratic_theta"
        assert fee.taker_rate == 0.05

    def test_taker_only_means_makers_pay_nothing(self, poly_market):
        fee = fee_schedule_of(poly_market)
        assert poly_market["feeSchedule"]["takerOnly"] is True
        assert fee.maker_rate == 0.0
        assert fee.estimate(price=0.5, contracts=100, taker=False) == 0.0

    def test_estimate_follows_the_venue_fee_table(self, poly_market):
        # Polymarket's own table: 100 shares on a 0.07 crypto market pay
        # $1.75 at 0.50 and $0.63 at 0.10; the curve is symmetric.
        fee = fee_schedule_of({**poly_market, "feeSchedule": {"rate": 0.07, "exponent": 1, "takerOnly": True}})
        assert fee.estimate(price=0.50, contracts=100) == pytest.approx(1.75)
        assert round(fee.estimate(price=0.10, contracts=100), 2) == 0.63
        assert fee.estimate(price=0.30, contracts=100) == pytest.approx(fee.estimate(price=0.70, contracts=100))

    def test_exponent_is_applied(self, poly_market):
        fee = fee_schedule_of({**poly_market, "feeSchedule": {"rate": 0.05, "exponent": 2, "takerOnly": True}})
        assert fee.exponent == 2
        assert fee.estimate(price=0.5, contracts=100) == pytest.approx(0.05 * 100 * 0.25 ** 2)


class TestHelpers:
    def test_placeholder_prices_are_absent(self):
        assert quoted(0) is None
        assert quoted(1) is None
        assert quoted(0.42) == 0.42

    def test_timestamp_forms(self):
        assert parse_ts(1789471790532) == 1789471790532       # already ms
        assert parse_ts(1789471790) == 1789471790000          # seconds
        assert parse_ts("1789471790532") == 1789471790532     # ms as string
        assert parse_ts("2026-09-14T06:18:43Z") is not None   # ISO
        assert parse_ts(None) is None


class TestTradeCandles:
    """Bars built from the tape, both tokens folded into the asked-for side."""

    YES, NO = "111", "222"

    def _trades(self):
        t0 = 1_700_000_000
        return [
            {"asset": self.YES, "outcomeIndex": 0, "price": 0.40, "size": 10, "timestamp": t0 + 10},
            {"asset": self.NO, "outcomeIndex": 1, "price": 0.55, "size": 5, "timestamp": t0 + 20},   # YES 0.45
            {"asset": self.YES, "outcomeIndex": 0, "price": 0.42, "size": 1, "timestamp": t0 + 30},
            {"asset": self.NO, "outcomeIndex": 1, "price": 0.50, "size": 2, "timestamp": t0 + 3700},  # next hour
        ]

    def test_other_token_is_folded_in_at_one_minus_price(self):
        candles = candles_from_trades(self._trades(), yes_token=self.YES, interval_seconds=3600)
        first = candles[0]
        assert (first.open, first.high, first.low, first.close) == (0.40, 0.45, 0.40, 0.42)
        assert first.volume == 16 and first.trade_count == 3
        assert first.price_source == "trade"

    def test_the_tape_is_always_in_the_yes_price(self):
        """Handing the NO token as `yes_token` folds the YES trades instead: the
        function prices on whatever it is told is YES, and the adapter always
        tells it the YES token."""
        candles = candles_from_trades(self._trades(), yes_token=self.NO, interval_seconds=3600)
        first = candles[0]
        assert (first.open, first.high, first.low, first.close) == (0.60, 0.60, 0.55, 0.58)

    def test_bars_are_ordered_and_aligned(self):
        candles = candles_from_trades(self._trades(), yes_token=self.YES, interval_seconds=3600)
        stamps = [c.timestamp for c in candles]
        assert stamps == sorted(stamps) and len(stamps) == 2
        assert all(stamp % (3600 * 1000) == 0 for stamp in stamps)

    def test_a_row_without_a_size_is_skipped_not_counted_as_zero(self):
        rows = self._trades() + [{"asset": self.YES, "price": 0.9, "timestamp": 1_700_000_050}]
        candles = candles_from_trades(rows, yes_token=self.YES, interval_seconds=3600)
        assert candles[0].trade_count == 3 and candles[0].high == 0.45


class TestFetchOhlcvFromTrades:
    """The adapter walks the tape newest-first until it passes the window."""

    def _venue(self, pages, *, condition="0xabc"):
        calls = {"offsets": [], "gamma": []}

        class FakeData:
            def get(self, path, params=None):
                calls["offsets"].append(params["offset"])
                index = params["offset"] // 500
                return pages[index] if index < len(pages) else []

        poly = Polymarket(limiter=None)
        poly.data = FakeData()
        # The market is already known to the client, as it would be after any
        # catalog read: no Gamma round trip is needed for its tokens.
        poly._tokens["1"] = ("T", "N", condition)
        return poly, calls

    def _page(self, stamps, asset="T"):
        return [{"asset": asset, "price": 0.5, "size": 1, "timestamp": s} for s in stamps]

    def test_stops_once_the_page_passes_the_start_of_the_window(self):
        until = 1_700_000_000_000
        end = until // 1000
        newest = self._page(range(end, end - 500, -1))            # all inside the last hour
        older = self._page(range(end - 500, end - 1000, -1))       # crosses out of a 10-minute window
        poly, calls = self._venue([newest, older, self._page([1])])
        candles = poly.fetch_ohlcv("1", timeframe="1m", until=until, limit=10)
        assert calls["offsets"] == [0, 500]
        assert len(candles) == 10 and all(c.info.get("complete", True) for c in candles)

    def test_hitting_the_page_cap_marks_the_earliest_bar_incomplete(self):
        from synpath.polymarket import MAX_TRADE_PAGES

        until = 1_700_000_000_000
        end = until // 1000
        pages = [self._page(range(end - i, end - i - 1, -1) or [end - i]) * 500 for i in range(MAX_TRADE_PAGES + 2)]
        # every page sits inside the window, so the walk never passes `start`
        poly, calls = self._venue(pages)
        candles = poly.fetch_ohlcv("1", timeframe="1h", until=until, limit=3)
        assert len(calls["offsets"]) == MAX_TRADE_PAGES
        assert candles[0].info["complete"] is False

    def test_an_unseen_market_is_read_from_gamma_once(self):
        poly, calls = self._venue([[]])

        class FakeGamma:
            def get(self, path, params=None):
                calls["gamma"].append(path)
                return {"id": "2", "conditionId": "0xdef", "question": "q", "outcomes": '["Yes","No"]',
                        "clobTokenIds": '["Y2","N2"]'}

        poly.gamma = FakeGamma()
        poly.fetch_ohlcv("polymarket:2", timeframe="1h", limit=1)
        poly.fetch_ohlcv("2", timeframe="1h", limit=1)
        assert calls["gamma"] == ["/markets/2"]

    def test_unknown_source_is_refused(self):
        from synpath import BadRequest

        poly, _ = self._venue([[]])
        with pytest.raises(BadRequest, match="unknown source"):
            poly.fetch_ohlcv("1", source="ticks")


class TestCapabilities:
    def test_ohlcv_is_trade_built(self):
        assert Polymarket.has["fetch_ohlcv"] is True

    def test_no_series_tier(self):
        assert Polymarket.has["fetch_series"] is False

    def test_server_side_search(self):
        assert Polymarket.has["search"] is True


class TestMarketPaging:
    """Regression: `fetch_markets` accepted a `cursor` and threw it away, and
    always answered `next_cursor=None`. Only the first page of the catalog was
    reachable, with nothing to indicate more existed."""

    def test_cursor_reaches_the_venue(self):
        sent = {}

        class FakeGamma:
            def get(self, path, params=None):
                sent["path"] = path
                sent.update(params or {})
                return {"markets": [], "next_cursor": "NEXT"}

        poly = Polymarket(limiter=None)
        poly.gamma = FakeGamma()
        page = poly.fetch_markets(limit=5, cursor="ABC")
        assert sent["after_cursor"] == "ABC"
        assert page.next_cursor == "NEXT"

    def test_uses_the_keyset_endpoint(self):
        """`/markets` offers only offset and rejects it past 2000, so it cannot
        walk the catalog at all."""
        sent = {}

        class FakeGamma:
            def get(self, path, params=None):
                sent["path"] = path
                return {"markets": [], "next_cursor": None}

        poly = Polymarket(limiter=None)
        poly.gamma = FakeGamma()
        poly.fetch_markets(limit=5)
        assert sent["path"] == "/markets/keyset"

    def test_sort_order_is_preserved_across_pages(self):
        sent = {}

        class FakeGamma:
            def get(self, path, params=None):
                sent.update(params or {})
                return {"markets": [], "next_cursor": None}

        poly = Polymarket(limiter=None)
        poly.gamma = FakeGamma()
        poly.fetch_markets(limit=5, cursor="ABC")
        assert sent["order"] == "volume24hr"
        assert sent["ascending"] == "false"

    def test_search_results_carry_no_cursor(self):
        """Gamma's search is not paged; claiming a cursor would invent one."""
        class FakeGamma:
            def get(self, path, params=None):
                return {"events": []}

        poly = Polymarket(limiter=None)
        poly.gamma = FakeGamma()
        assert poly.fetch_markets(query="fed").next_cursor is None


class TestStatusVocabulary:
    """Regression: `status='settled'` fell through to "no filter", so it
    returned live markets here while returning settled ones on Kalshi -- the
    same argument answering two different questions."""

    def test_settled_is_refused_not_approximated(self):
        from synpath import NotSupported
        from synpath.polymarket import _status_flags

        with pytest.raises(NotSupported, match="resolution status"):
            _status_flags("settled")

    def test_the_error_says_what_to_do_instead(self):
        from synpath import NotSupported
        from synpath.polymarket import _status_flags

        with pytest.raises(NotSupported, match="status='closed'"):
            _status_flags("settled")

    def test_unknown_status_is_refused(self):
        from synpath import BadRequest
        from synpath.polymarket import _status_flags

        with pytest.raises(BadRequest, match="unknown status"):
            _status_flags("bogus")

    def test_known_words_still_map(self):
        from synpath.polymarket import _status_flags

        assert _status_flags("open") == ("true", "false")
        assert _status_flags("closed") == (None, "true")
        assert _status_flags("all") == (None, None)


class TestPageLimit:
    """Regression: Polymarket clamped to 100 while Kalshi served up to 500, so
    the same `limit` produced different page sizes per venue."""

    def test_request_is_clamped_to_the_shared_ceiling(self):
        from synpath.base import MAX_PAGE_LIMIT

        sent = {}

        class FakeGamma:
            def get(self, path, params=None):
                sent.update(params or {})
                return {"markets": [], "next_cursor": None}

        poly = Polymarket(limiter=None)
        poly.gamma = FakeGamma()
        poly.fetch_markets(limit=5000)
        assert sent["limit"] == MAX_PAGE_LIMIT


class TestOhlcvWindow:
    """Regression: `until` was read only alongside `since`, so a request for
    bars ending a month ago silently returned today's."""

    def _venue(self, history):
        sent = {}

        class FakeClob:
            def get(self, path, params=None):
                if not sent:        # a long window is read in pieces; keep the first
                    sent.update(params or {})
                return {"history": history}

        poly = Polymarket(limiter=None)
        poly.clob = FakeClob()
        poly._tokens["1"] = ("token", "other", "0xabc")
        return poly, sent

    def test_until_alone_sets_an_absolute_window(self):
        poly, sent = self._venue([])
        until = 1_700_000_000_000
        poly.fetch_ohlcv("1", timeframe="1h", until=until, limit=5, source="quotes")
        assert sent["endTs"] == until // 1000
        assert sent["startTs"] == until // 1000 - 3600 * 5
        assert "interval" not in sent

    def test_since_alone_still_works(self):
        poly, sent = self._venue([])
        poly.fetch_ohlcv("1", timeframe="1h", since=1_700_000_000_000, source="quotes")
        assert sent["startTs"] == 1_700_000_000
        assert sent["endTs"] >= sent["startTs"]

    def test_neither_falls_back_to_a_relative_interval(self):
        poly, sent = self._venue([])
        poly.fetch_ohlcv("1", timeframe="1h", source="quotes")
        assert "interval" in sent and "startTs" not in sent

    def test_the_venues_trailing_live_sample_is_dropped(self):
        """prices-history appends a sample at the current price whatever window
        was asked for. Left in, that bar is the future a backtest must not see."""
        until = 1_700_000_000_000
        inside = until // 1000 - 3600
        history = [{"t": inside, "p": 0.4}, {"t": until // 1000 + 2_000_000, "p": 0.9}]
        poly, _ = self._venue(history)
        candles = poly.fetch_ohlcv("1", timeframe="1h", until=until, limit=10, source="quotes")
        assert [c.close for c in candles] == [0.4]

    def test_samples_before_the_window_are_dropped_too(self):
        until = 1_700_000_000_000
        history = [{"t": until // 1000 - 999_999, "p": 0.1},
                   {"t": until // 1000 - 3600, "p": 0.4}]
        poly, _ = self._venue(history)
        candles = poly.fetch_ohlcv("1", timeframe="1h", until=until, limit=10, source="quotes")
        assert [c.close for c in candles] == [0.4]


class TestLongQuoteWindows:
    """The venue refuses a `prices-history` window longer than about 15 days,
    so a longer one is read in `QUOTE_WINDOW` pieces and joined."""

    def test_a_long_window_is_split_and_joined(self):
        from synpath.polymarket import QUOTE_WINDOW

        calls = []

        class FakeClob:
            def get(self, path, params=None):
                calls.append(dict(params))
                start, end = params["startTs"], params["endTs"]
                assert end - start <= QUOTE_WINDOW
                # One sample at each end of the piece, so the shared boundary repeats.
                return {"history": [{"t": start, "p": 0.1}, {"t": end, "p": 0.2}]}

        poly = Polymarket(limiter=None)
        poly.clob = FakeClob()
        poly._tokens["1"] = ("token", "other", "0xabc")
        since, until = 1_700_000_000_000, 1_700_000_000_000 + 40 * 86_400_000
        candles = poly.fetch_ohlcv("1", timeframe="1d", since=since, until=until, source="quotes")
        assert len(calls) == 3
        assert calls[0]["startTs"] == since // 1000 and calls[-1]["endTs"] == until // 1000
        assert [c["startTs"] for c in calls[1:]] == [c["endTs"] for c in calls[:-1]]
        stamps = [c.timestamp for c in candles]
        assert stamps == sorted(set(stamps))

    def test_a_short_window_is_one_request(self):
        calls = []

        class FakeClob:
            def get(self, path, params=None):
                calls.append(params)
                return {"history": []}

        poly = Polymarket(limiter=None)
        poly.clob = FakeClob()
        poly._tokens["1"] = ("token", "other", "0xabc")
        poly.fetch_ohlcv("1", timeframe="1h", since=1_700_000_000_000,
                         until=1_700_000_000_000 + 86_400_000, source="quotes")
        assert len(calls) == 1


class TestCatalogGaps:
    """Closed markets in batch lookups, and search that pages and filters."""

    @staticmethod
    def raw(n, closed=False):
        return {"id": str(n), "question": f"Q{n}", "conditionId": f"0x{n}", "closed": closed,
                "active": not closed, "clobTokenIds": f'["y{n}", "n{n}"]', "outcomes": '["Yes", "No"]'}

    def test_batch_lookup_asks_again_for_closed_markets(self):
        calls = []

        class FakeGamma:
            def get(self, path, params=None):
                calls.append(params)
                closed = ("closed", "true") in params
                return [TestCatalogGaps.raw(1, closed=True)] if closed else [TestCatalogGaps.raw(2)]

        poly = Polymarket(limiter=None)
        poly.gamma = FakeGamma()
        found = poly.fetch_markets_by_ids(["1", "2"])
        assert [m.venue_market_id for m in found] == ["1", "2"]
        assert ("id", "1") in calls[1] and ("id", "2") not in calls[1]

    def search(self, pages):
        calls = []

        class FakeGamma:
            def get(self, path, params=None):
                calls.append(dict(params))
                events, more = pages[params["page"] - 1]
                return {"events": events, "pagination": {"hasMore": more}}

        poly = Polymarket(limiter=None)
        poly.gamma = FakeGamma()
        return poly, calls

    def event(self, n, markets):
        return {"id": str(n), "title": f"E{n}", "slug": f"e{n}", "markets": markets}

    def test_search_honours_limit_and_resumes_mid_page(self):
        page = [self.event(1, [self.raw(n) for n in range(1, 6)])]
        poly, _ = self.search([(page, False)])
        first = poly.fetch_markets(query="x", limit=3)
        assert [m.venue_market_id for m in first] == ["1", "2", "3"]
        rest = poly.fetch_markets(query="x", limit=3, cursor=first.next_cursor)
        assert [m.venue_market_id for m in rest] == ["4", "5"] and rest.next_cursor is None

    def test_search_reads_further_pages(self):
        poly, calls = self.search([([self.event(1, [self.raw(1)])], True), ([self.event(2, [self.raw(2)])], False)])
        found = poly.fetch_markets(query="x", limit=10)
        assert [m.venue_market_id for m in found] == ["1", "2"] and found.next_cursor is None
        assert [c["page"] for c in calls] == [1, 2]

    def test_search_filters_by_status(self):
        poly, calls = self.search([([self.event(1, [self.raw(1), self.raw(2, closed=True)])], False)])
        assert [m.venue_market_id for m in poly.fetch_markets(query="x", status="open")] == ["1"]
        assert calls[0]["events_status"] == "active"
        assert [m.venue_market_id for m in poly.fetch_markets(query="x", status="closed")] == ["2"]

    def test_a_foreign_search_cursor_is_refused(self):
        poly, _ = self.search([([], False)])
        with pytest.raises(BadRequest):
            poly.fetch_markets(query="x", cursor="nope")


class TestTradePagingEnds:
    def poly(self, rows):
        class FakeData:
            def get(self, path, params=None):
                return rows

        poly = Polymarket(limiter=None)
        poly.data = FakeData()
        poly._tokens["1"] = ("y", "n", "0xabc")
        return poly

    @staticmethod
    def trade(ts):
        return {"asset": "y", "side": "BUY", "price": 0.5, "size": 1, "timestamp": ts, "transactionHash": f"h{ts}"}

    def test_paging_stops_once_past_since(self):
        poly = self.poly([self.trade(2000), self.trade(1000)])
        page = poly.fetch_trades("1", since=1_500_000, limit=2)
        assert [t.timestamp for t in page] == [2_000_000] and page.next_cursor is None

    def test_paging_stops_at_the_venues_deepest_offset(self):
        from synpath.polymarket import MAX_TRADE_OFFSET

        poly = self.poly([self.trade(n) for n in range(500)])
        assert poly.fetch_trades("1", limit=500, cursor=str(MAX_TRADE_OFFSET)).next_cursor is None
        assert poly.fetch_trades("1", limit=500, cursor="0").next_cursor == "500"
