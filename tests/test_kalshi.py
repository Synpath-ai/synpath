"""Kalshi normalizers, against payloads recorded from the live venue."""
from __future__ import annotations

import pytest

from synpath import BadRequest, Kalshi, MarketNotFound, NotSupported
from synpath.kalshi import (
    normalize_candle, normalize_event, normalize_market, normalize_order_book,
    normalize_series, normalize_trade, quoted, tick_size_of,
)


class TestPlaceholders:
    """0 and 1 are how Kalshi says "empty", and must never surface as prices."""

    def test_zero_bid_is_absent(self):
        assert quoted("0.0000") is None

    def test_face_value_ask_is_absent(self):
        assert quoted("1.0000") is None

    def test_beyond_face_value_is_absent(self):
        assert quoted("1.2000") is None

    def test_real_price_survives(self):
        assert quoted("0.1200") == 0.12

    def test_placeholder_is_relative_to_face_value(self):
        # A market with a face value of 5.00 quotes real prices well above 1.
        assert quoted("1.2000", face_value=5.0) == 1.2
        assert quoted("5.0000", face_value=5.0) is None

    def test_missing_field(self):
        assert quoted(None) is None
        assert quoted("") is None


class TestMarket:
    def test_identity_and_shape(self, kalshi_market, kalshi_event):
        market = normalize_market(kalshi_market, kalshi_event)
        assert market.venue == "kalshi"
        assert market.id == f"kalshi:{kalshi_market['ticker']}"
        assert market.venue_market_id == kalshi_market["ticker"]
        assert market.book_model == "shared_complement"

    def test_event_id_is_qualified_too(self, kalshi_market, kalshi_event):
        market = normalize_market(kalshi_market, kalshi_event)
        assert market.event_id == f"kalshi:{kalshi_event['event_ticker']}"

    def test_sides_are_found_by_position_not_label(self, kalshi_market, kalshi_event):
        # This market labels both sides "Mars", so anything keying on the label
        # text would pick the wrong one. The sides are the two fields.
        market = normalize_market(kalshi_market, kalshi_event)
        assert market.yes.label == market.no.label
        assert market.yes.quote.bid == 0.10
        assert market.no.quote.bid == 0.88

    def test_quotes_match_the_payload(self, kalshi_market, kalshi_event):
        market = normalize_market(kalshi_market, kalshi_event)
        assert market.yes.quote.bid == 0.10
        assert market.yes.quote.ask == 0.12
        assert market.yes.quote.mid == 0.11
        assert market.no.quote.bid == 0.88
        assert market.no.quote.ask == 0.90

    def test_complement_prices_sum_to_face_value(self, kalshi_market, kalshi_event):
        """A NO bid is the mirror of the YES ask in the same shared book."""
        market = normalize_market(kalshi_market, kalshi_event)
        assert market.no.quote.bid + market.yes.quote.ask == pytest.approx(market.face_value)
        assert market.no.quote.ask + market.yes.quote.bid == pytest.approx(market.face_value)

    def test_no_side_sizes_mirror_the_yes_side(self, kalshi_market, kalshi_event):
        """The orders resting on NO's bid are the same orders on YES's ask."""
        market = normalize_market(kalshi_market, kalshi_event)
        assert market.no.quote.bid_size == market.yes.quote.ask_size
        assert market.no.quote.ask_size == market.yes.quote.bid_size

    def test_last_carries_its_timestamp(self, kalshi_market, kalshi_event):
        market = normalize_market(kalshi_market, kalshi_event)
        assert market.yes.quote.last == 0.10
        assert market.yes.quote.last_timestamp is not None
        assert market.yes.quote.last_datetime.endswith("Z")

    def test_zero_liquidity_is_absent_not_zero(self, kalshi_market, kalshi_event):
        """Kalshi reports 0.0000 liquidity on every open market; that is a
        missing figure, not an empty book."""
        assert kalshi_market["liquidity_dollars"] == "0.0000"
        assert normalize_market(kalshi_market, kalshi_event).stats.liquidity is None

    def test_volume_unit_is_labelled(self, kalshi_market, kalshi_event):
        stats = normalize_market(kalshi_market, kalshi_event).stats
        assert stats.volume_unit == "contracts"
        assert stats.volume_total == float(kalshi_market["volume_fp"])

    def test_face_value_read_from_venue(self, kalshi_market, kalshi_event):
        assert normalize_market(kalshi_market, kalshi_event).face_value == 1.0

    def test_tick_size_from_price_ladder(self, kalshi_market):
        assert tick_size_of(kalshi_market) == 0.01

    def test_tick_size_takes_the_finest_step(self):
        market = {"price_ranges": [
            {"start": "0.0000", "end": "0.0500", "step": "0.0010"},
            {"start": "0.0500", "end": "1.0000", "step": "0.0100"},
        ]}
        assert tick_size_of(market) == 0.001

    def test_status_and_active(self, kalshi_market, kalshi_event):
        market = normalize_market(kalshi_market, kalshi_event)
        assert market.status == "open"
        assert market.native_status == "active"
        assert market.active is True

    def test_settled_is_not_active(self, kalshi_market):
        market = normalize_market({**kalshi_market, "status": "finalized"})
        assert market.status == "settled"
        assert market.active is False

    def test_timestamps_are_ms_plus_iso(self, kalshi_market, kalshi_event):
        market = normalize_market(kalshi_market, kalshi_event)
        assert market.close_timestamp > 10**12
        assert market.close_datetime.endswith("Z")

    def test_info_keeps_the_raw_payload(self, kalshi_market, kalshi_event):
        market = normalize_market(kalshi_market, kalshi_event)
        assert market.info["yes_ask_dollars"] == "0.1200"


class TestEvent:
    def test_event_carries_markets_and_series(self, kalshi_event):
        event = normalize_event(kalshi_event)
        assert event.venue == "kalshi"
        assert event.series_id == kalshi_event["series_ticker"]
        assert len(event.markets) == len(kalshi_event["markets"])

    def test_mutual_exclusivity_is_tri_state(self, kalshi_event):
        """A venue that did not say gets None, never False."""
        event = normalize_event({**kalshi_event, "mutually_exclusive": None})
        assert event.mutually_exclusive is None


class TestOrderBook:
    """Both arrays the venue returns are bids; one side has to be reflected."""

    def test_yes_view(self, kalshi_orderbook):
        book = normalize_order_book(kalshi_orderbook, ticker="KXELONMARS-99", side="yes")
        assert book.market_id == "kalshi:KXELONMARS-99"
        assert book.side == "yes"
        assert book.best_bid.price == 0.10
        # Highest NO bid is 0.88, so the best YES ask is 1 - 0.88.
        assert book.best_ask.price == pytest.approx(0.12)
        assert book.derived is True
        assert book.book_model == "shared_complement"

    def test_no_view_is_the_mirror(self, kalshi_orderbook):
        book = normalize_order_book(kalshi_orderbook, ticker="KXELONMARS-99", side="no")
        assert book.best_bid.price == 0.88
        assert book.best_ask.price == pytest.approx(0.90)

    def test_views_agree_with_each_other(self, kalshi_orderbook):
        yes = normalize_order_book(kalshi_orderbook, ticker="T", side="yes")
        no = normalize_order_book(kalshi_orderbook, ticker="T", side="no")
        assert yes.best_bid.price + no.best_ask.price == pytest.approx(1.0)
        assert yes.best_ask.price + no.best_bid.price == pytest.approx(1.0)

    def test_bids_descend_and_asks_ascend(self, kalshi_orderbook):
        book = normalize_order_book(kalshi_orderbook, ticker="T", side="yes")
        assert [level.price for level in book.bids] == sorted(
            (level.price for level in book.bids), reverse=True
        )
        assert [level.price for level in book.asks] == sorted(
            level.price for level in book.asks
        )

    def test_sizes_survive_the_reflection(self, kalshi_orderbook):
        raw = kalshi_orderbook["orderbook_fp"]
        best_no_bid = max(raw["no_dollars"], key=lambda row: float(row[0]))
        book = normalize_order_book(kalshi_orderbook, ticker="T", side="yes")
        assert book.best_ask.size == float(best_no_bid[1])

    def test_depth_truncates_both_sides(self, kalshi_orderbook):
        book = normalize_order_book(kalshi_orderbook, ticker="T", side="yes", depth=3)
        assert len(book.bids) <= 3 and len(book.asks) <= 3
        assert book.depth_scope == "top_n"


class TestTrade:
    def test_a_no_taker_is_a_sell_at_the_yes_price(self, kalshi_trades):
        raw = kalshi_trades["trades"][0]
        assert raw["taker_side"] == "no"
        trade = normalize_trade(raw)
        assert trade.price == float(raw["yes_price_dollars"])
        assert trade.side == "sell"
        assert trade.market_id == f"kalshi:{raw['ticker']}"

    def test_a_yes_taker_is_a_buy(self, kalshi_trades):
        raw = {**kalshi_trades["trades"][0], "taker_side": "yes"}
        trade = normalize_trade(raw)
        assert trade.price == float(raw["yes_price_dollars"])
        assert trade.side == "buy"

    def test_amount_and_time(self, kalshi_trades):
        raw = kalshi_trades["trades"][0]
        trade = normalize_trade(raw)
        assert trade.amount == float(raw["count_fp"])
        assert trade.timestamp > 10**12
        assert trade.datetime.endswith("Z")


class TestCandle:
    def test_untraded_period_is_labelled_not_invented(self, kalshi_candles):
        """No trades in the period means no traded OHLC. The book still has a
        spread, so the bar reports the midpoint and says where it came from."""
        raw = kalshi_candles["candlesticks"][0]
        assert "close_dollars" not in raw["price"]
        candle = normalize_candle(raw, interval_seconds=3600)
        assert candle.price_source == "bid_ask_mid"
        assert candle.close == pytest.approx(0.11)
        assert candle.bid_close == 0.10
        assert candle.ask_close == 0.12
        assert candle.volume is None

    def test_traded_period_uses_executions(self, kalshi_candles):
        raw = {**kalshi_candles["candlesticks"][0]}
        raw["price"] = {
            "open_dollars": "0.1000", "high_dollars": "0.1500",
            "low_dollars": "0.0900", "close_dollars": "0.1400",
        }
        raw["volume_fp"] = "250.00"
        candle = normalize_candle(raw, interval_seconds=3600)
        assert candle.price_source == "trade"
        assert (candle.open, candle.high, candle.low, candle.close) == (0.10, 0.15, 0.09, 0.14)
        assert candle.volume == 250.0

    def test_timestamp_is_bar_start(self, kalshi_candles):
        raw = kalshi_candles["candlesticks"][0]
        candle = normalize_candle(raw, interval_seconds=3600)
        assert candle.timestamp == (raw["end_period_ts"] - 3600) * 1000


class TestSeries:
    def test_fee_schedule_is_read_before_trading(self, kalshi_series):
        series = normalize_series(kalshi_series["series"])
        assert series.fee is not None
        assert series.fee.fee_type == "quadratic"
        assert series.fee.scope == "series"

    def test_quadratic_fee_peaks_at_the_middle(self, kalshi_series):
        fee = normalize_series(kalshi_series["series"]).fee
        middle = fee.estimate(price=0.50, contracts=100)
        edge = fee.estimate(price=0.05, contracts=100)
        assert middle > edge > 0

    def test_kalshi_fees_follow_the_venues_published_types(self):
        from synpath.types import FeeSchedule

        def schedule(fee_type, multiplier=1.0):
            return FeeSchedule(venue="kalshi", scope="series", scope_id="S", fee_type=fee_type,
                               multiplier=multiplier, rounding="up_to_cent")
        plain = schedule("quadratic")
        assert plain.estimate(0.50, 100) == 1.75
        assert plain.estimate(0.05, 100) == 0.34, "0.3325 is charged as 0.34: up to the cent"
        assert plain.estimate(0.50, 100, taker=False) == 0.0, "makers pay nothing on plain quadratic"
        makers = schedule("quadratic_with_maker_fees")
        assert makers.estimate(0.50, 100) == 1.75
        assert makers.estimate(0.50, 100, taker=False) == 0.44, "0.0175 * 100 * 0.25 = 0.4375, rounded up"
        combo = schedule("quadratic_with_combo_maker_fees")
        assert combo.estimate(0.50, 100, taker=False) == 0.88
        assert schedule("quadratic", 0.5).estimate(0.50, 100) == 0.88
        assert schedule("flat").estimate(0.50, 100) is None, "no published table in this library"


class TestIds:
    def test_a_qualified_id_and_a_bare_ticker_both_work(self):
        kalshi = Kalshi(limiter=None)
        assert kalshi.native("kalshi:KXFOO-25") == "KXFOO-25"
        assert kalshi.native("KXFOO-25") == "KXFOO-25"

    def test_another_venues_id_is_refused(self):
        kalshi = Kalshi(limiter=None)
        with pytest.raises(BadRequest, match="belongs to polymarket"):
            kalshi.fetch_order_book("polymarket:2252244")

    def test_unknown_side_is_rejected(self):
        kalshi = Kalshi(limiter=None)
        with pytest.raises(BadRequest, match="unknown side"):
            kalshi.fetch_order_book("KXFOO-25", side="maybe")


class TestCapabilities:
    def test_unsupported_timeframe_is_refused_not_rounded(self):
        kalshi = Kalshi(limiter=None)
        with pytest.raises(BadRequest, match="not offered"):
            kalshi.fetch_ohlcv("KXFOO-25:yes", timeframe="15m")

    def test_search_is_native_now(self):
        """It was marked as built locally while search meant scanning the catalog."""
        assert Kalshi.has["search"] is True
        assert Kalshi.has["fetch_ohlcv"] is True


class TestCandleReflection:
    """Regression: `fetch_ohlcv` discarded the side and returned the YES series
    for a NO instrument, unreflected. Both sides read 0.11 on a market where
    NO should have been 0.89 — silently wrong, and invisible near 0.50."""

    def _yes_candle(self, kalshi_candles):
        return normalize_candle(kalshi_candles["candlesticks"][0], interval_seconds=3600)

    def test_prices_invert(self, kalshi_candles):
        from synpath.kalshi import reflect_candle

        yes = self._yes_candle(kalshi_candles)
        no = reflect_candle(yes, face_value=1.0)
        assert no.close == pytest.approx(1 - yes.close)
        assert no.open == pytest.approx(1 - yes.open)

    def test_extremes_swap(self):
        """The period's highest YES price is its lowest NO price."""
        from synpath.kalshi import reflect_candle
        from synpath.types import Candle

        yes = Candle(
            timestamp=0, datetime="1970-01-01T00:00:00Z",
            open=0.20, high=0.60, low=0.10, close=0.30,
        )
        no = reflect_candle(yes, face_value=1.0)
        assert (no.high, no.low) == (pytest.approx(0.90), pytest.approx(0.40))
        assert no.low == pytest.approx(1 - yes.high)

    def test_book_sides_swap_too(self, kalshi_candles):
        from synpath.kalshi import reflect_candle

        yes = self._yes_candle(kalshi_candles)
        no = reflect_candle(yes, face_value=1.0)
        assert no.bid_close == pytest.approx(1 - yes.ask_close)
        assert no.ask_close == pytest.approx(1 - yes.bid_close)

    def test_face_value_is_respected(self):
        from synpath.kalshi import reflect_candle
        from synpath.types import Candle

        yes = Candle(timestamp=0, datetime="x", close=1.5)
        assert reflect_candle(yes, face_value=5.0).close == pytest.approx(3.5)

    def test_nulls_stay_null(self):
        from synpath.kalshi import reflect_candle
        from synpath.types import Candle

        reflected = reflect_candle(Candle(timestamp=0, datetime="x"), face_value=1.0)
        assert reflected.close is None and reflected.high is None


class TestStatusAll:
    """Regression: the server advertises `status=all`, which is this library's
    word for "no filter". Passing it through as a Kalshi status was a 400."""

    def test_all_sends_no_status_filter(self, monkeypatch):
        sent = {}

        class FakeHttp:
            def get(self, path, params=None):
                sent.update(params or {})
                return {"events": []}

        kalshi = Kalshi(limiter=None)
        kalshi.http = FakeHttp()
        kalshi.fetch_events(status="all")
        assert sent["status"] is None

    def test_a_real_status_is_forwarded(self):
        sent = {}

        class FakeHttp:
            def get(self, path, params=None):
                sent.update(params or {})
                return {"events": []}

        kalshi = Kalshi(limiter=None)
        kalshi.http = FakeHttp()
        kalshi.fetch_events(status="closed")
        assert sent["status"] == "closed"


class TestFetchMarketsCost:
    """Regression: fetch_markets pulled the maximum 200-event page (about
    1,700 markets) no matter how few were asked for, normalizing all of them."""

    def test_event_page_is_sized_for_a_market_page(self):
        from synpath.kalshi import EVENT_PAGE

        sent = {}

        class FakeHttp:
            def get(self, path, params=None):
                sent.update(params or {})
                return {"events": []}

        kalshi = Kalshi(limiter=None)
        kalshi.http = FakeHttp()
        kalshi.fetch_markets(limit=5)
        assert sent["limit"] == EVENT_PAGE
        assert EVENT_PAGE < 200, "still pulling the venue's maximum page"


class TestFaceValueCache:
    """Regression: the cache had no bound, and the server holds one adapter for
    the life of the process."""

    def test_cache_is_bounded(self):
        from synpath.kalshi import FACE_VALUE_CACHE

        kalshi = Kalshi(limiter=None)
        for n in range(FACE_VALUE_CACHE + 50):
            kalshi._remember_face_value(f"T-{n}", 1.0)
        assert len(kalshi._face_values) == FACE_VALUE_CACHE

    def test_oldest_is_evicted_first(self):
        from synpath.kalshi import FACE_VALUE_CACHE

        kalshi = Kalshi(limiter=None)
        for n in range(FACE_VALUE_CACHE + 1):
            kalshi._remember_face_value(f"T-{n}", 1.0)
        assert "T-0" not in kalshi._face_values
        assert f"T-{FACE_VALUE_CACHE}" in kalshi._face_values

    def test_listing_markets_does_not_fill_the_cache(self):
        """Listing thousands of markets used to write an entry for each."""
        class FakeHttp:
            def get(self, path, params=None):
                return {"events": []}

        kalshi = Kalshi(limiter=None)
        kalshi.http = FakeHttp()
        kalshi.fetch_markets(limit=5)
        assert len(kalshi._face_values) == 0


class TestMarketCursor:
    """Regression: the cursor advanced past a whole event page while only
    `limit` markets were returned, so everything trimmed was unreachable. A
    walk with limit=5 reached 15 of 120 markets and reported no problem."""

    def _venue(self, events_per_page, markets_per_event, pages):
        """A Kalshi whose catalog is a known, checkable shape."""
        catalog = []
        for page in range(pages):
            catalog.append([
                {
                    "event_ticker": f"E{page}-{e}",
                    "series_ticker": "SER",
                    "markets": [
                        {"ticker": f"M{page}-{e}-{m}", "status": "active",
                         "event_ticker": f"E{page}-{e}"}
                        for m in range(markets_per_event)
                    ],
                }
                for e in range(events_per_page)
            ])

        class FakeHttp:
            requests = 0

            def get(self, path, params=None):
                FakeHttp.requests += 1
                index = int((params or {}).get("cursor") or 0)
                nxt = index + 1
                return {
                    "events": catalog[index] if index < len(catalog) else [],
                    "cursor": str(nxt) if nxt < len(catalog) else None,
                }

        venue = Kalshi(limiter=None)
        venue.http = FakeHttp()
        expected = [m["ticker"] for page in catalog for e in page for m in e["markets"]]
        return venue, expected

    def _walk(self, venue, limit):
        seen, cursor = [], None
        for _ in range(200):
            page = venue.fetch_markets(limit=limit, cursor=cursor)
            seen += [market.venue_market_id for market in page]
            cursor = page.next_cursor
            if not cursor:
                return seen
        raise AssertionError("walk did not terminate")

    def test_a_walk_visits_every_market_once(self):
        venue, expected = self._venue(events_per_page=3, markets_per_event=7, pages=4)
        assert self._walk(venue, limit=5) == expected

    def test_no_duplicates_and_no_gaps_for_any_page_size(self):
        for limit in (1, 2, 3, 7, 20, 21, 100):
            venue, expected = self._venue(events_per_page=3, markets_per_event=7, pages=3)
            assert self._walk(venue, limit=limit) == expected, f"limit={limit}"

    def test_page_size_is_honoured_while_rows_remain(self):
        venue, expected = self._venue(events_per_page=4, markets_per_event=9, pages=2)
        page = venue.fetch_markets(limit=5)
        assert len(page) == 5
        assert page.next_cursor

    def test_last_page_ends_with_a_null_cursor(self):
        venue, expected = self._venue(events_per_page=1, markets_per_event=2, pages=1)
        page = venue.fetch_markets(limit=100)
        assert len(page) == len(expected)
        assert page.next_cursor is None

    def test_resuming_mid_event_page_does_not_refetch_from_the_start(self):
        """The offset is in the cursor, so a resumed walk continues where it
        stopped rather than replaying the page."""
        venue, expected = self._venue(events_per_page=2, markets_per_event=10, pages=2)
        first = venue.fetch_markets(limit=5)
        second = venue.fetch_markets(limit=5, cursor=first.next_cursor)
        assert [m.venue_market_id for m in first] == expected[:5]
        assert [m.venue_market_id for m in second] == expected[5:10]

    def test_an_event_with_no_markets_is_stepped_over(self):
        venue, _ = self._venue(events_per_page=2, markets_per_event=0, pages=2)
        assert self._walk(venue, limit=5) == []

    def test_cursor_survives_a_new_client(self):
        """State lives in the cursor, not the adapter, so a walk survives a
        restart or a move to another process."""
        venue, expected = self._venue(events_per_page=2, markets_per_event=6, pages=2)
        first = venue.fetch_markets(limit=4)
        fresh, _ = self._venue(events_per_page=2, markets_per_event=6, pages=2)
        second = fresh.fetch_markets(limit=4, cursor=first.next_cursor)
        assert [m.venue_market_id for m in second] == expected[4:8]

    def test_a_foreign_cursor_is_rejected(self):
        """Silently restarting the walk would read as duplicate data."""
        venue = Kalshi(limiter=None)
        with pytest.raises(BadRequest, match="not a cursor this API issued"):
            venue.fetch_markets(cursor="not-ours")

    def test_cursor_round_trips(self):
        from synpath.kalshi import decode_market_cursor, encode_market_cursor

        assert decode_market_cursor(encode_market_cursor("EVT", 7, "f1"), "f1") == ("EVT", 7)
        assert decode_market_cursor(encode_market_cursor("EVT", "KX-A", "f1"), "f1") == ("EVT", "KX-A")
        assert decode_market_cursor(encode_market_cursor(None, None, "f1"), "f1") == (None, None)
        assert decode_market_cursor(None, "f1") == (None, None)


class TestStatusVocabulary:
    def test_unknown_status_is_refused(self):
        venue = Kalshi(limiter=None)
        with pytest.raises(BadRequest, match="unknown status"):
            venue.fetch_markets(status="bogus")

    def test_settled_is_forwarded(self):
        sent = {}

        class FakeHttp:
            def get(self, path, params=None):
                sent.update(params or {})
                return {"events": []}

        venue = Kalshi(limiter=None)
        venue.http = FakeHttp()
        venue.fetch_markets(status="settled")
        assert sent["status"] == "settled"


class TestFaceValueCost:
    """Regression: resolving a face value went through fetch_market, which also
    reads the parent event for display fields a book read never uses."""

    def test_one_request_not_two(self):
        paths = []

        class FakeHttp:
            def get(self, path, params=None):
                paths.append(path)
                return {"market": {"ticker": "T", "notional_value_dollars": "1.0000"}}

        venue = Kalshi(limiter=None)
        venue.http = FakeHttp()
        assert venue._face_value("T") == 1.0
        assert paths == ["/markets/T"], paths

    def test_the_value_is_read_from_the_venue(self):
        class FakeHttp:
            def get(self, path, params=None):
                return {"market": {"ticker": "T", "notional_value_dollars": "5.0000"}}

        venue = Kalshi(limiter=None)
        venue.http = FakeHttp()
        assert venue._face_value("T") == 5.0

    def test_a_missing_market_falls_back_rather_than_failing(self):
        class FakeHttp:
            def get(self, path, params=None):
                raise MarketNotFound("kalshi: gone")

        venue = Kalshi(limiter=None)
        venue.http = FakeHttp()
        assert venue._face_value("T") == 1.0


class TestSearch:
    """Kalshi does have text search -- on an undocumented host. Before this,
    `query` scanned the catalog: ~520 requests and a hundred seconds to learn
    that a term matched nothing."""

    def _hit(self, event="E1", tickers=("T1", "T2")):
        return {"event_ticker": event, "series_ticker": "SER",
                "markets": [{"ticker": t} for t in tickers]}

    def _event(self, event="E1", tickers=("T1", "T2"), exclusive=True, category="Politics"):
        return {
            "event_ticker": event, "series_ticker": "SER", "category": category,
            "title": "Will it?", "mutually_exclusive": exclusive,
            "markets": [{"ticker": t, "status": "active", "event_ticker": event,
                         "notional_value_dollars": "1.0000"} for t in tickers],
        }

    def _venue(self, hits, events, next_cursor=""):
        class FakeSearch:
            calls: list = []

            def get(self, path, params=None):
                FakeSearch.calls.append((path, params))
                return {"current_page": hits, "next_cursor": next_cursor}

        class FakeHttp:
            calls: list = []

            def get(self, path, params=None):
                FakeHttp.calls.append((path, params))
                return {"events": events}

        venue = Kalshi(limiter=None, search_limiter=None)
        venue.search, venue.http = FakeSearch(), FakeHttp()
        FakeSearch.calls, FakeHttp.calls = [], []
        return venue

    def test_query_goes_to_the_venue_not_a_local_scan(self):
        venue = self._venue([self._hit()], [self._event()])
        venue.fetch_markets(query="fed", limit=5)
        assert venue.search.calls[0][0] == "/v1/search/series"
        assert venue.search.calls[0][1]["query"] == "fed"

    def test_a_page_costs_two_requests(self):
        venue = self._venue([self._hit()], [self._event()])
        venue.fetch_markets(query="fed", limit=5)
        assert len(venue.search.calls) == 1 and len(venue.http.calls) == 1

    def test_events_are_batched_with_tickers_not_event_ticker(self):
        """`event_ticker=a,b` answers 200 and is silently ignored: the venue
        returns its unfiltered first page, which reads as results."""
        venue = self._venue([self._hit()], [self._event()])
        venue.fetch_markets(query="fed", limit=5)
        path, params = venue.http.calls[0]
        assert path == "/events"
        assert params["tickers"] == "E1"
        assert "event_ticker" not in params

    def test_results_carry_the_real_event_context(self):
        """Search hits have no mutual-exclusivity flag. Building context from
        them gave neg_risk=None where a listing of the same market gave True."""
        venue = self._venue([self._hit()], [self._event(exclusive=True)])
        market = venue.fetch_markets(query="fed", limit=5)[0]
        assert market.neg_risk is True
        assert market.category == "Politics"
        assert market.series_id == "SER"

    def test_results_keep_search_order_even_when_the_batch_reorders(self):
        venue = self._venue(
            [self._hit("E1", ("T1",)), self._hit("E2", ("T2",)), self._hit("E3", ("T3",))],
            [self._event("E3", ("T3",)), self._event("E1", ("T1",)), self._event("E2", ("T2",))],
        )
        assert [m.venue_market_id for m in venue.fetch_markets(query="fed", limit=5)] == ["T1", "T2", "T3"]

    def test_market_order_within_an_event_follows_the_hit(self):
        """Relevance order inside an event is the hit's, not the event payload's."""
        venue = self._venue(
            [self._hit("E1", ("T3", "T1", "T2"))], [self._event("E1", ("T1", "T2", "T3"))],
        )
        assert [m.venue_market_id for m in venue.fetch_markets(query="fed", limit=5)] == ["T3", "T1", "T2"]

    def test_a_ticker_the_venue_dropped_is_skipped(self):
        venue = self._venue([self._hit("E1", ("T1", "GONE"))], [self._event("E1", ("T1",))])
        assert [m.venue_market_id for m in venue.fetch_markets(query="fed", limit=5)] == ["T1"]

    def test_no_matches_ends_the_walk(self):
        venue = self._venue([], [])
        page = venue.fetch_markets(query="nothingmatches", limit=5)
        assert list(page) == [] and page.next_cursor is None

    def test_a_long_result_set_resumes_mid_page(self):
        tickers = ("T1", "T2", "T3", "T4")
        venue = self._venue([self._hit("E1", tickers)], [self._event("E1", tickers)])
        first = venue.fetch_markets(query="fed", limit=2)
        assert [m.venue_market_id for m in first] == ["T1", "T2"] and first.next_cursor
        second = venue.fetch_markets(query="fed", limit=2, cursor=first.next_cursor)
        assert [m.venue_market_id for m in second] == ["T3", "T4"]

    def test_fetch_events_uses_search_too(self):
        """It filtered one page locally: 'trump' found 10 markets through
        fetch_markets and 0 events through fetch_events."""
        venue = self._venue(
            [self._hit("E2", ("T2",)), self._hit("E1", ("T1",))],
            [self._event("E1", ("T1",)), self._event("E2", ("T2",))],
        )
        events = venue.fetch_events(query="fed", limit=5)
        assert [event.venue_event_id for event in events] == ["E2", "E1"]
        assert venue.search.calls

    def test_search_events_pages(self):
        hits = [self._hit(f"E{n}", (f"T{n}",)) for n in range(4)]
        events = [self._event(f"E{n}", (f"T{n}",)) for n in range(4)]
        venue = self._venue(hits, events)
        first = venue.fetch_events(query="fed", limit=2)
        second = venue.fetch_events(query="fed", limit=2, cursor=first.next_cursor)
        assert [e.venue_event_id for e in first] == ["E0", "E1"]
        assert [e.venue_event_id for e in second] == ["E2", "E3"]


class TestSearchShape:
    """The search endpoint is undocumented, so a change of shape that still
    answers 200 is its most likely failure -- and reading it with a default
    turned that into 'no matches', which nobody would question."""

    def _venue(self, payload):
        class FakeSearch:
            def get(self, path, params=None):
                return payload

        venue = Kalshi(limiter=None, search_limiter=None)
        venue.search = FakeSearch()
        return venue

    def test_a_renamed_field_raises(self):
        from synpath import ExchangeError

        venue = self._venue({"results": [{"event_ticker": "E1"}], "cursor": "x"})
        with pytest.raises(ExchangeError, match="current_page"):
            venue.fetch_markets(query="trump", limit=5)

    def test_the_events_path_raises_too(self):
        from synpath import ExchangeError

        venue = self._venue({"results": []})
        with pytest.raises(ExchangeError):
            venue.fetch_events(query="trump", limit=5)

    def test_a_genuine_no_match_does_not_raise(self):
        """The venue's own empty answer: `current_page: []`, `next_cursor: ''`."""
        venue = self._venue(
            {"current_page": [], "next_cursor": "", "total_results_count": 0},
        )
        page = venue.fetch_markets(query="zzz", limit=5)
        assert list(page) == [] and page.next_cursor is None

    def test_a_non_list_is_refused(self):
        from synpath import ExchangeError

        venue = self._venue({"current_page": None})
        with pytest.raises(ExchangeError):
            venue.fetch_markets(query="trump", limit=5)


class TestListingKeyset:
    """The listing cursor records the last ticker returned, not a row number.
    A row number repeats a market when one is added ahead of it and skips one
    when a market ahead of it is removed; "after ticker X" does neither."""

    def _venue(self, pages):
        """`pages` is a list of callables returning the event list for a call,
        so a test can change the catalog between one call and the next."""
        state = {"call": 0}

        class FakeHttp:
            def get(self, path, params=None):
                events = pages[min(state["call"], len(pages) - 1)]()
                state["call"] += 1
                return {"events": events, "cursor": ""}

        venue = Kalshi(limiter=None)
        venue.http = FakeHttp()
        return venue

    def _event(self, tickers):
        return [{"event_ticker": "E", "series_ticker": "S",
                 "markets": [{"ticker": t, "status": "active", "event_ticker": "E"}
                             for t in tickers]}]

    def test_order_is_by_ticker_not_by_venue_order(self):
        venue = self._venue([lambda: self._event(["KX-E", "KX-A", "KX-C"])])
        assert [m.venue_market_id for m in venue.fetch_markets(limit=10)] == ["KX-A", "KX-C", "KX-E"]

    def test_a_market_added_ahead_of_the_cursor_repeats_nothing(self):
        before = ["KX-A", "KX-C", "KX-E", "KX-G", "KX-I"]
        after = ["KX-A", "KX-B", "KX-C", "KX-E", "KX-G", "KX-H", "KX-I"]
        venue = self._venue([lambda: self._event(before), lambda: self._event(after)])
        seen, cursor = [], None
        for _ in range(10):
            page = venue.fetch_markets(limit=2, cursor=cursor)
            seen += [m.venue_market_id for m in page]
            cursor = page.next_cursor
            if not cursor:
                break
        assert len(seen) == len(set(seen)), "a market was repeated"
        assert set(before) <= set(seen), "a market that existed all along was skipped"
        assert "KX-H" in seen, "a market added after the cursor should be seen"

    def test_a_market_removed_ahead_of_the_cursor_skips_nothing(self):
        before = ["KX-A", "KX-C", "KX-E", "KX-G", "KX-I"]
        after = ["KX-C", "KX-E", "KX-G", "KX-I"]
        venue = self._venue([lambda: self._event(before), lambda: self._event(after)])
        seen, cursor = [], None
        for _ in range(10):
            page = venue.fetch_markets(limit=2, cursor=cursor)
            seen += [m.venue_market_id for m in page]
            cursor = page.next_cursor
            if not cursor:
                break
        assert seen == before, seen

    def test_venue_reordering_between_calls_changes_nothing(self):
        tickers = ["KX-A", "KX-B", "KX-C", "KX-D"]
        venue = self._venue([
            lambda: self._event(tickers),
            lambda: self._event(list(reversed(tickers))),
        ])
        first = venue.fetch_markets(limit=2)
        second = venue.fetch_markets(limit=2, cursor=first.next_cursor)
        assert [m.venue_market_id for m in first] + [m.venue_market_id for m in second] == tickers


class TestEventPaging:
    def test_fetch_events_defaults_to_the_shared_page_size(self):
        """It defaulted to 25 on Kalshi and 100 on Polymarket for the same call."""
        from synpath.base import MAX_PAGE_LIMIT

        sent = {}

        class FakeHttp:
            def get(self, path, params=None):
                sent.update(params or {})
                return {"events": []}

        venue = Kalshi(limiter=None)
        venue.http = FakeHttp()
        venue.fetch_events()
        assert sent["limit"] == MAX_PAGE_LIMIT

    def test_iter_events_pages_at_the_venue_maximum(self):
        """It inherited a 25-event page: 521 requests for the catalog."""
        from synpath.kalshi import VENUE_EVENT_PAGE

        limits = []

        class FakeHttp:
            def get(self, path, params=None):
                limits.append((params or {}).get("limit"))
                return {"events": [{"event_ticker": "E", "markets": []}], "cursor": ""}

        venue = Kalshi(limiter=None)
        venue.http = FakeHttp()
        list(venue.iter_events())
        assert limits == [VENUE_EVENT_PAGE]


class TestCursorFingerprint:
    """An offset counts rows of one particular query. Reusing it under
    different arguments used to land somewhere arbitrary, silently."""

    def test_a_cursor_from_another_status_is_refused(self):
        from synpath.kalshi import encode_market_cursor, query_fingerprint

        venue = Kalshi(limiter=None)
        stale = encode_market_cursor(
            "EVT", 3, query_fingerprint(status="open", kind="markets"),
        )
        with pytest.raises(BadRequest, match="different query"):
            venue.fetch_markets(status="closed", cursor=stale)

    def test_a_market_cursor_is_refused_by_search(self):
        from synpath.kalshi import encode_market_cursor, query_fingerprint

        venue = Kalshi(limiter=None)
        listing = encode_market_cursor(
            None, 0, query_fingerprint(status="open", kind="markets"),
        )
        with pytest.raises(BadRequest, match="different query"):
            venue.fetch_markets(query="fed", cursor=listing)

    def test_the_same_arguments_are_accepted(self):
        from synpath.kalshi import decode_market_cursor, encode_market_cursor, query_fingerprint

        fingerprint = query_fingerprint(status="open", kind="markets")
        cursor = encode_market_cursor("EVT", 3, fingerprint)
        assert decode_market_cursor(cursor, fingerprint) == ("EVT", 3)


class TestStatusIsJudgedPerMarket:
    """Kalshi's status filter selects events, not markets. Measured: an
    `/events?status=settled` page carried 54 markets, 40 of them still active,
    so status="settled" returned markets that were trading."""

    def _venue(self, events):
        class FakeHttp:
            def get(self, path, params=None):
                return {"events": events, "cursor": ""}

        venue = Kalshi(limiter=None)
        venue.http = FakeHttp()
        return venue

    def _mixed_event(self):
        return [{"event_ticker": "E", "series_ticker": "S", "markets": [
            {"ticker": "KX-A", "status": "finalized", "event_ticker": "E"},
            {"ticker": "KX-B", "status": "active", "event_ticker": "E"},
            {"ticker": "KX-C", "status": "finalized", "event_ticker": "E"},
        ]}]

    def test_settled_returns_only_settled_markets(self):
        markets = self._venue(self._mixed_event()).fetch_markets(status="settled", limit=10)
        assert [m.venue_market_id for m in markets] == ["KX-A", "KX-C"]

    def test_open_excludes_settled_markets_in_an_open_event(self):
        markets = self._venue(self._mixed_event()).fetch_markets(status="open", limit=10)
        assert [m.venue_market_id for m in markets] == ["KX-B"]

    def test_all_keeps_everything(self):
        markets = self._venue(self._mixed_event()).fetch_markets(status="all", limit=10)
        assert len(markets) == 3

    def test_events_are_judged_the_same_way(self):
        """An event with any trading market is open, whatever the venue filed it under."""
        venue = self._venue(self._mixed_event())
        assert [e.venue_event_id for e in venue.fetch_events(status="open")] == ["E"]
        assert list(venue.fetch_events(status="settled")) == []
        assert [e.venue_event_id for e in venue.iter_events(status="open")] == ["E"]


class TestFillIsBounded:
    """A sparse status filter must not turn one call into a catalog walk."""

    def test_stops_after_the_page_budget_with_a_cursor(self):
        from synpath.kalshi import MAX_FILL_PAGES

        calls = {"n": 0}

        class FakeHttp:
            def get(self, path, params=None):
                calls["n"] += 1
                # Every page holds only trading markets, so status="settled"
                # never fills.
                return {"events": [{"event_ticker": f"E{calls['n']}", "markets": [
                    {"ticker": f"KX-{calls['n']}", "status": "active"}]}],
                    "cursor": f"C{calls['n']}"}

        venue = Kalshi(limiter=None)
        venue.http = FakeHttp()
        page = venue.fetch_markets(status="settled", limit=10)
        assert calls["n"] == MAX_FILL_PAGES
        assert list(page) == []
        assert page.next_cursor, "a short page must say there may be more"


class TestHistorical:
    """Kalshi moves markets settled, and trades created, before a cutoff off
    the live endpoints and onto `/historical/*`. Reads fall back to it."""

    CUTOFF = {"trades_created_ts": "2026-07-27T00:00:00Z", "market_settled_ts": "2026-07-27T00:00:00Z"}

    @staticmethod
    def trade(n, when):
        return {"trade_id": f"t{n}", "ticker": "OLD-1", "created_time": when, "count_fp": "1.00",
                "yes_price_dollars": "0.4000", "taker_side": "yes"}

    def kalshi(self, routes):
        calls = []

        class FakeHttp:
            def get(self, path, params=None):
                calls.append((path, dict(params or {})))
                answer = routes.get(path)
                if answer is None:
                    raise MarketNotFound("kalshi: not found")
                return answer(params or {}) if callable(answer) else answer

        kalshi = Kalshi(limiter=None)
        kalshi.http = FakeHttp()
        return kalshi, calls

    def test_market_settled_before_the_cutoff_is_found(self):
        raw = {"ticker": "OLD-1", "event_ticker": "OLD", "status": "finalized", "title": "Old",
               "notional_value_dollars": "1.0000"}
        kalshi, calls = self.kalshi({"/historical/markets/OLD-1": {"market": raw}})
        assert kalshi.fetch_market("kalshi:OLD-1").venue_market_id == "OLD-1"
        assert [path for path, _ in calls][:2] == ["/markets/OLD-1", "/historical/markets/OLD-1"]

    def test_batch_fills_missing_tickers_from_historical(self):
        live = {"ticker": "NEW-1", "status": "active"}
        old = {"ticker": "OLD-1", "status": "finalized"}
        kalshi, calls = self.kalshi({"/markets": {"markets": [live]},
                                     "/historical/markets": {"markets": [old]}})
        found = kalshi.fetch_markets_by_ids(["kalshi:OLD-1", "kalshi:NEW-1"])
        assert [m.venue_market_id for m in found] == ["OLD-1", "NEW-1"]
        assert calls[1] == ("/historical/markets", {"tickers": "OLD-1", "limit": 1})

    def test_trades_continue_into_historical_on_the_same_page(self):
        kalshi, calls = self.kalshi({
            "/markets/trades": {"trades": [self.trade(1, "2026-08-02T00:00:00Z")], "cursor": ""},
            "/historical/trades": {"trades": [self.trade(2, "2026-07-01T00:00:00Z")], "cursor": "H2"},
            "/markets/OLD-1": {"market": {"ticker": "OLD-1", "notional_value_dollars": "1.0000"}},
        })
        page = kalshi.fetch_trades("kalshi:OLD-1", limit=5)
        assert [t.id for t in page] == ["t2", "t1"]
        assert page.next_cursor == "historical:H2"
        assert ("/historical/trades", {"ticker": "OLD-1", "limit": 4, "min_ts": None, "cursor": None}) in calls

    def test_a_historical_cursor_reads_only_historical(self):
        kalshi, calls = self.kalshi({
            "/historical/trades": {"trades": [self.trade(3, "2026-06-01T00:00:00Z")], "cursor": ""},
            "/markets/OLD-1": {"market": {"ticker": "OLD-1"}},
        })
        page = kalshi.fetch_trades("kalshi:OLD-1", cursor="historical:H2")
        assert [t.id for t in page] == ["t3"] and page.next_cursor is None
        assert all(path != "/markets/trades" for path, _ in calls)
        assert calls[0][1]["cursor"] == "H2"

    def test_a_full_live_page_defers_historical_to_the_cursor(self):
        rows = [self.trade(n, "2026-08-02T00:00:00Z") for n in range(2)]
        kalshi, calls = self.kalshi({"/markets/trades": {"trades": rows, "cursor": ""},
                                     "/markets/OLD-1": {"market": {"ticker": "OLD-1"}}})
        page = kalshi.fetch_trades("kalshi:OLD-1", limit=2)
        assert len(page) == 2 and page.next_cursor == "historical:"
        assert all(path != "/historical/trades" for path, _ in calls)

    def test_since_after_the_cutoff_never_asks_historical(self):
        kalshi, calls = self.kalshi({
            "/markets/trades": {"trades": [], "cursor": ""},
            "/historical/cutoff": self.CUTOFF,
            "/markets/OLD-1": {"market": {"ticker": "OLD-1"}},
        })
        since = 1_788_000_000_000  # 2026-08-29, after the cutoff
        page = kalshi.fetch_trades("kalshi:OLD-1", since=since)
        assert page.next_cursor is None
        assert all(path != "/historical/trades" for path, _ in calls)

    def test_candles_of_a_settled_market_come_from_historical(self):
        bar = {"end_period_ts": 1683172800, "volume": "12.00",
               "price": {"open": "0.3000", "high": "0.4000", "low": "0.3000", "close": "0.3500"},
               "yes_bid": {"close": "0.3400"}, "yes_ask": {"close": "0.3600"}}
        kalshi, calls = self.kalshi({"/historical/markets/OLD-1/candlesticks": {"candlesticks": [bar]}})
        [candle] = kalshi.fetch_ohlcv("kalshi:OLD-1", timeframe="1d", since=1683000000000, until=1683200000000)
        assert (candle.open, candle.high, candle.close, candle.volume) == (0.3, 0.4, 0.35, 12.0)
        assert candle.price_source == "trade" and candle.bid_close == 0.34
        assert calls[0][0] == "/series/OLD/markets/OLD-1/candlesticks"


class TestLongCandleWindows:
    """Kalshi answers at most 5,000 bars per request and refuses a wider
    window, so a longer one is read in pieces and joined."""

    def test_a_long_window_is_split_and_joined(self):
        from synpath.kalshi import MAX_CANDLES

        calls = []

        class FakeHttp:
            def get(self, path, params=None):
                if path == "/markets/KX-1":     # the market's dates, for clipping the window
                    return {"market": {"ticker": "KX-1"}}
                calls.append(dict(params))
                start, end = params["start_ts"], params["end_ts"]
                assert (end - start) / 60 < MAX_CANDLES
                # A bar at each end of the piece, so the shared boundary repeats.
                return {"candlesticks": [
                    {"end_period_ts": stamp, "price": {"close_dollars": "0.5000"}} for stamp in (start + 60, end)
                ]}

        kalshi = Kalshi(limiter=None)
        kalshi.http = FakeHttp()
        since = 1_700_000_000_000
        until = since + 10 * 86_400_000       # 14,400 one-minute bars
        candles = kalshi.fetch_ohlcv("kalshi:KX-1", timeframe="1m", since=since, until=until)
        assert len(calls) == 3
        assert calls[0]["start_ts"] == since // 1000 and calls[-1]["end_ts"] == until // 1000
        stamps = [c.timestamp for c in candles]
        assert stamps == sorted(set(stamps))

    def test_the_historical_fallback_sticks_across_pieces(self):
        paths = []

        class FakeHttp:
            def get(self, path, params=None):
                if path.endswith("/OLD-1"):     # the market's dates, for clipping the window
                    return {"market": {"ticker": "OLD-1"}}
                paths.append(path)
                if not path.startswith("/historical/"):
                    raise MarketNotFound("kalshi: not found")
                return {"candlesticks": []}

        kalshi = Kalshi(limiter=None)
        kalshi.http = FakeHttp()
        since = 1_700_000_000_000
        kalshi.fetch_ohlcv("kalshi:OLD-1", timeframe="1m", since=since, until=since + 10 * 86_400_000)
        assert paths[0].startswith("/series/")
        assert all(path.startswith("/historical/") for path in paths[1:]) and len(paths) == 4

    def test_an_unknown_market_still_raises(self):
        class FakeHttp:
            def get(self, path, params=None):
                raise MarketNotFound("kalshi: not found")

        kalshi = Kalshi(limiter=None)
        kalshi.http = FakeHttp()
        with pytest.raises(MarketNotFound):
            kalshi.fetch_ohlcv("kalshi:NOPE-1", timeframe="1h", limit=5)


class TestForwardCandleReads:
    """With `since`, bars are read forward and the first `limit` returned,
    fetching only as many pieces as that takes; a long window is first
    clipped to the market's life."""

    def kalshi(self, market):
        calls = []

        class FakeHttp:
            def get(self, path, params=None):
                if path == "/markets/KX-1":
                    return {"market": market}
                calls.append(dict(params))
                start, end = params["start_ts"], params["end_ts"]
                # A bar every hour of the piece.
                return {"candlesticks": [{"end_period_ts": t, "price": {"close_dollars": "0.5000"}}
                                         for t in range(start - start % 3600 + 3600, end + 1, 3600)]}

        kalshi = Kalshi(limiter=None)
        kalshi.http = FakeHttp()
        return kalshi, calls

    def test_since_returns_the_first_bars_and_stops_early(self):
        kalshi, calls = self.kalshi({"ticker": "KX-1"})
        since = 1_700_000_000_000
        candles = kalshi.fetch_ohlcv("kalshi:KX-1", timeframe="1m", since=since,
                                     until=since + 30 * 86_400_000, limit=10)
        assert len(candles) == 10 and candles[0].timestamp >= since
        assert len(calls) == 1, "read past the piece that already had enough bars"

    def test_without_since_the_newest_bars_are_returned(self):
        kalshi, _ = self.kalshi({"ticker": "KX-1"})
        until = 1_700_000_000_000
        candles = kalshi.fetch_ohlcv("kalshi:KX-1", timeframe="1h", until=until, limit=3)
        assert len(candles) == 3 and candles[-1].timestamp <= until

    def test_a_long_window_is_clipped_to_the_markets_life(self):
        since = 1_600_000_000_000                  # years before the market opened
        opened, closed = "2023-11-14T22:13:20Z", "2023-11-24T22:13:20Z"
        kalshi, calls = self.kalshi({"ticker": "KX-1", "open_time": opened, "close_time": closed})
        kalshi.fetch_ohlcv("kalshi:KX-1", timeframe="1m", since=since, until=1_760_000_000_000)
        assert calls[0]["start_ts"] >= 1_700_000_000 - 60
        assert calls[-1]["end_ts"] <= 1_700_864_000 + 60
        assert len(calls) == 3, "requested time the market did not exist"


class TestArchivedListings:
    """Events settled before Kalshi's cutoff are still listed live, but with no
    markets inside; their markets are only on `/historical/markets`."""

    @staticmethod
    def market(ticker, event, status="finalized"):
        return {"ticker": ticker, "event_ticker": event, "status": status, "title": ticker}

    def kalshi(self, routes, search=None):
        calls = []

        class FakeHttp:
            def __init__(self, table):
                self.table = table

            def get(self, path, params=None):
                calls.append((path, dict(params or {})))
                answer = self.table.get(path)
                if answer is None:
                    raise MarketNotFound("kalshi: not found")
                return answer(params or {}) if callable(answer) else answer

        kalshi = Kalshi(limiter=None)
        kalshi.http = FakeHttp(routes)
        kalshi.search = FakeHttp(search or {})
        return kalshi, calls

    def test_live_events_without_markets_are_not_returned_empty(self):
        live = {"events": [{"event_ticker": "OLD", "markets": []},
                           {"event_ticker": "NEW", "markets": [self.market("NEW-1", "NEW", "active")]}],
                "cursor": "C2"}
        kalshi, _ = self.kalshi({"/events": live})
        page = kalshi.fetch_events(status="all", limit=10)
        assert [e.venue_event_id for e in page] == ["NEW"]

    def test_settled_events_continue_into_the_archive(self):
        kalshi, calls = self.kalshi({
            "/events": lambda p: ({"events": [], "cursor": ""} if "tickers" not in p
                                  else {"events": [{"event_ticker": "OLD", "title": "Old event", "markets": []}]}),
            "/historical/markets": {"markets": [self.market("OLD-1", "OLD"), self.market("OLD-2", "OLD")],
                                    "cursor": "H2"},
        })
        first = kalshi.fetch_events(status="settled", limit=10)
        assert first.next_cursor == "historical:"
        archived = kalshi.fetch_events(status="settled", limit=10, cursor=first.next_cursor)
        [event] = archived
        assert event.title == "Old event" and [m.venue_market_id for m in event.markets] == ["OLD-1", "OLD-2"]
        assert archived.next_cursor == "historical:H2"
        assert ("/historical/markets", {"limit": 10, "cursor": None}) in calls

    def test_open_listings_never_reach_the_archive(self):
        kalshi, calls = self.kalshi({"/events": {"events": [], "cursor": ""}})
        assert kalshi.fetch_events(status="open", limit=10).next_cursor is None
        assert all(path != "/historical/markets" for path, _ in calls)

    def test_market_walk_fills_its_page_from_the_archive(self):
        kalshi, _ = self.kalshi({
            "/events": {"events": [{"event_ticker": "NEW", "markets": [self.market("NEW-1", "NEW")]}], "cursor": ""},
            "/historical/markets": lambda p: (
                {"markets": [self.market("OLD-1", "OLD")], "cursor": "H2"} if not p.get("cursor")
                else {"markets": [self.market("OLD-2", "OLD")], "cursor": ""}),
        })
        page = kalshi.fetch_markets(status="settled", limit=2)
        assert [m.venue_market_id for m in page] == ["NEW-1", "OLD-1"]
        again = kalshi.fetch_markets(status="settled", limit=2, cursor=page.next_cursor)
        assert [m.venue_market_id for m in again] == ["OLD-2"], "the archive cursor resumes there"
        assert again.next_cursor is None

    def test_search_fills_archived_events_in_one_batch(self):
        hits = {"current_page": [{"event_ticker": "OLD", "markets": [{"ticker": "OLD-1"}, {"ticker": "OLD-2"}]}]}
        kalshi, calls = self.kalshi(
            {"/events": {"events": [{"event_ticker": "OLD", "markets": []}]},
             "/historical/markets": {"markets": [self.market("OLD-1", "OLD"), self.market("OLD-2", "OLD")]}},
            search={"/v1/search/series": hits},
        )
        found = kalshi.search_markets("old", status="settled")
        assert [m.venue_market_id for m in found] == ["OLD-1", "OLD-2"]
        assert ("/historical/markets", {"tickers": "OLD-1,OLD-2", "limit": 2}) in calls

    def test_open_search_skips_the_archive(self):
        hits = {"current_page": [{"event_ticker": "OLD", "markets": [{"ticker": "OLD-1"}]}]}
        kalshi, calls = self.kalshi({"/events": {"events": [{"event_ticker": "OLD", "markets": []}]}},
                                    search={"/v1/search/series": hits})
        assert kalshi.search_markets("old", status="open") == []
        assert all(path != "/historical/markets" for path, _ in calls)
