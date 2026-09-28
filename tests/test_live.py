"""End-to-end tests against the real venues.

Deselected by default (`-m live` to run them). They exist because recorded
payloads prove the normalizers are right about yesterday's API, not that the
adapter still talks to today's.
"""
from __future__ import annotations

import pytest

import synpath

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def kalshi():
    with synpath.Kalshi() as venue:
        yield venue


@pytest.fixture(scope="module")
def polymarket():
    with synpath.Polymarket() as venue:
        yield venue


class TestKalshiLive:
    def test_fetch_markets(self, kalshi):
        markets = kalshi.fetch_markets(limit=5)
        assert markets
        assert all(market.venue == "kalshi" for market in markets)
        assert all(len(market.instruments) == 2 for market in markets)

    def test_order_book_sides_agree(self, kalshi):
        market = next(m for m in kalshi.fetch_markets(limit=25) if m.yes.quote.bid)
        yes = kalshi.fetch_order_book(market.yes.id)
        no = kalshi.fetch_order_book(market.no.id)
        assert yes.best_bid and no.best_ask
        assert yes.best_bid.price + no.best_ask.price == pytest.approx(market.face_value)

    def test_book_agrees_with_the_catalog_quote(self, kalshi):
        market = next(m for m in kalshi.fetch_markets(limit=25) if m.yes.quote.bid)
        book = kalshi.fetch_order_book(market.yes.id)
        assert book.best_bid.price == pytest.approx(market.yes.quote.bid, abs=0.02)

    def test_trades(self, kalshi):
        market = next(
            m for m in kalshi.fetch_markets(limit=40)
            if (m.stats.volume_total or 0) > 0
        )
        trades = kalshi.fetch_trades(market.id, limit=5)
        assert all(0 < trade.price < market.face_value for trade in trades)
        assert trades == sorted(trades, key=lambda trade: trade.timestamp)

    def test_ohlcv(self, kalshi):
        # Daily bars: Kalshi emits one only for periods where something moved,
        # so a few hours on a thin market can legitimately hold none.
        market = kalshi.fetch_markets(limit=1)[0]
        candles = kalshi.fetch_ohlcv(market.yes.id, timeframe="1d", limit=30)
        assert candles
        assert all(candle.price_source in ("trade", "bid_ask_mid") for candle in candles)

    def test_fee_schedule_before_trading(self, kalshi):
        market = kalshi.fetch_markets(limit=1)[0]
        fee = kalshi.fetch_fee_schedule(market.id)
        assert fee.fee_type
        assert fee.estimate(price=0.5, contracts=100) is not None


class TestPolymarketLive:
    def test_fetch_markets(self, polymarket):
        markets = polymarket.fetch_markets(limit=5)
        assert markets
        assert all(market.venue == "polymarket" for market in markets)

    def test_server_side_search(self, polymarket):
        events = polymarket.fetch_events(query="Fed", limit=3)
        assert events

    def test_order_book_is_best_first(self, polymarket):
        market = next(m for m in polymarket.fetch_markets(limit=10) if m.yes.venue_token_id)
        book = polymarket.fetch_order_book(market.yes.id)
        assert book.best_bid.price < book.best_ask.price
        assert book.bids[0].price >= book.bids[-1].price
        assert book.asks[0].price <= book.asks[-1].price

    def test_refresh_quotes_fills_both_sides(self, polymarket):
        """The catalog only quotes the first outcome; the book quotes both."""
        market = next(m for m in polymarket.fetch_markets(limit=10) if m.yes.venue_token_id)
        assert market.no.quote.bid is None
        refreshed = polymarket.refresh_quotes(market)
        assert refreshed.no.quote.bid is not None
        assert refreshed.yes.quote.ask is not None

    def test_batched_books(self, polymarket):
        markets = polymarket.fetch_markets(limit=5)
        tokens = [i.venue_token_id for m in markets for i in m.instruments if i.venue_token_id]
        books = polymarket.fetch_order_books(tokens)
        assert books

    def test_trades(self, polymarket):
        market = polymarket.fetch_markets(limit=1)[0]
        trades = polymarket.fetch_trades(market.id, limit=5)
        assert all(0 < trade.price < 1 for trade in trades)

    def test_ohlcv_is_built_from_the_tape(self, polymarket):
        market = next(m for m in polymarket.fetch_markets(limit=5) if m.yes.venue_token_id)
        candles = polymarket.fetch_ohlcv(market.yes.id, timeframe="1h", limit=3)
        assert candles
        assert all(c.price_source == "trade" and c.volume and c.trade_count for c in candles)
        no_side = polymarket.fetch_ohlcv(market.no.id, timeframe="1h", limit=3)
        assert no_side[-1].close == pytest.approx(1 - candles[-1].close, abs=1e-6)

    def test_ohlcv_quote_samples_remain_an_option(self, polymarket):
        market = next(m for m in polymarket.fetch_markets(limit=5) if m.yes.venue_token_id)
        candles = polymarket.fetch_ohlcv(market.yes.id, timeframe="1h", limit=5, source="quotes")
        assert all(candle.price_source == "sampled_mid" for candle in candles)
        assert all(candle.volume is None for candle in candles)


@pytest.fixture(scope="module")
def polymarket_us():
    with synpath.PolymarketUS() as venue:
        yield venue


class TestPolymarketUSLive:
    def test_fetch_markets(self, polymarket_us):
        markets = polymarket_us.fetch_markets(limit=5)
        assert markets
        assert all(market.venue == "polymarket_us" for market in markets)
        assert all(len(market.instruments) == 2 for market in markets)

    def test_server_side_search_is_relevant(self, polymarket_us):
        events = polymarket_us.fetch_events(query="fed", limit=3)
        assert events
        assert any("fed" in event.title.lower() for event in events)

    def test_order_book_sides_agree(self, polymarket_us):
        market = next(m for m in polymarket_us.fetch_markets(limit=25) if m.yes.quote.bid)
        yes = polymarket_us.fetch_order_book(market.yes.id)
        no = polymarket_us.fetch_order_book(market.no.id)
        assert yes.derived is False and no.derived is True
        assert yes.best_bid.price + no.best_ask.price == pytest.approx(market.face_value)

    def test_refresh_quotes_fills_the_book_side_of_the_quote(self, polymarket_us):
        market = next(m for m in polymarket_us.fetch_markets(limit=25) if m.yes.quote.bid)
        assert market.yes.quote.bid_size is None
        fresh = polymarket_us.refresh_quotes(market)
        assert fresh.yes.quote.bid_size is not None
        assert fresh.stats.open_interest is not None

    def test_single_market_by_slug_and_by_id_agree(self, polymarket_us):
        listed = polymarket_us.fetch_markets(limit=1)[0]
        by_slug = polymarket_us.fetch_market(listed.id)
        by_id = polymarket_us.fetch_market(str(listed.info["id"]))
        assert by_slug.id == by_id.id == listed.id

    def test_batch_lookup_keeps_order(self, polymarket_us):
        ids = [m.id for m in polymarket_us.fetch_markets(limit=10)]
        assert [m.id for m in polymarket_us.fetch_markets_by_ids(ids)] == ids

    def test_pages_forward_without_repeats(self, polymarket_us):
        first = polymarket_us.fetch_markets(limit=5)
        assert first.next_cursor
        second = polymarket_us.fetch_markets(limit=5, cursor=first.next_cursor)
        assert not {m.id for m in first} & {m.id for m in second}

    def test_ohlcv_is_labelled_quote_derived(self, polymarket_us):
        market = next(m for m in polymarket_us.fetch_markets(limit=10) if m.yes.quote.bid)
        candles = polymarket_us.fetch_ohlcv(market.yes.id, timeframe="1d", limit=10)
        assert candles
        assert all(c.price_source == "bid_ask_mid" and c.volume is None for c in candles)

    def test_fee_schedule_before_trading(self, polymarket_us):
        market = polymarket_us.fetch_markets(limit=1)[0]
        fee = polymarket_us.fetch_fee_schedule(market.id)
        assert fee.fee_type == "quadratic_theta"
        assert fee.estimate(price=0.5, contracts=100) > 0 > fee.estimate(price=0.5, contracts=100, taker=False)

    def test_series_by_slug(self, polymarket_us):
        event = next(e for e in polymarket_us.fetch_events(limit=10) if e.series_id)
        assert polymarket_us.fetch_series(event.series_id).id == event.series_id

    def test_no_trade_tape(self, polymarket_us):
        with pytest.raises(synpath.NotSupported):
            polymarket_us.fetch_trades("anything")

    def test_settled_filter_returns_only_settled(self, polymarket_us):
        page = polymarket_us.fetch_markets(status="settled", limit=5)
        assert page and all(m.status == "settled" for m in page)


class TestCrossVenueLive:
    def test_same_code_runs_on_both(self):
        """The point of the library, exercised the way a caller would."""
        for venue_id in synpath.exchanges:
            with synpath.exchange(venue_id) as venue:
                markets = venue.fetch_markets(limit=3)
                assert markets, venue_id
                market = markets[0]
                assert market.yes and market.no
                book = venue.fetch_order_book(market.yes.id)
                assert book.instrument_id


class TestServerLive:
    """The HTTP layer in front of the real venues.

    Proves the wiring end to end: a request arrives, the adapter calls the
    venue, and the response carries the same shape the library returns.
    """

    @pytest.fixture(scope="class")
    def client(self):
        pytest.importorskip("fastapi")
        from fastapi.testclient import TestClient

        from synpath.server import create_app

        with TestClient(create_app()) as client:
            yield client

    def test_markets_over_http(self, client):
        body = client.get("/venues/kalshi/markets", params={"limit": 3}).json()
        assert body["count"] == len(body["data"]) == 3
        assert body["next_cursor"]

    def test_cursor_advances_to_new_rows(self, client):
        first = client.get("/venues/kalshi/markets", params={"limit": 5}).json()
        second = client.get("/venues/kalshi/markets", params={
            "limit": 5, "cursor": first["next_cursor"],
        }).json()
        assert {m["id"] for m in first["data"]} != {m["id"] for m in second["data"]}

    def test_book_over_http_matches_the_library(self, client):
        import synpath

        body = client.get("/venues/kalshi/markets", params={"limit": 1}).json()
        instrument_id = body["data"][0]["instruments"][0]["id"]
        over_http = client.get(f"/venues/kalshi/instruments/{instrument_id}/book").json()
        assert over_http["derived"] is True
        assert synpath.OrderBook.model_validate(over_http)

    def test_polymarket_search_over_http(self, client):
        body = client.get("/venues/polymarket/markets", params={
            "query": "Fed", "limit": 3,
        }).json()
        assert body["data"]

    def test_capability_gap_is_501(self, client):
        response = client.get("/venues/polymarket/series/anything")
        assert response.status_code == 501
        assert response.json()["type"] == "NotSupported"


class TestFixesLive:
    """The eight review findings, checked against the real venues."""

    def test_polymarket_markets_page_forward(self, polymarket):
        first = polymarket.fetch_markets(limit=3)
        assert first.next_cursor, "no cursor means the catalog ends at page one"
        second = polymarket.fetch_markets(limit=3, cursor=first.next_cursor)
        assert not {m.id for m in first} & {m.id for m in second}

    def test_polymarket_paging_keeps_the_sort(self, polymarket):
        page = polymarket.fetch_markets(limit=5)
        volumes = [m.stats.volume_24h or 0 for m in page]
        assert volumes == sorted(volumes, reverse=True)

    def test_kalshi_no_side_candles_are_reflected(self, kalshi):
        # Daily bars over a wide window: Kalshi emits a bar only where
        # something moved, so a few hours on a thin market can hold none.
        market = kalshi.fetch_markets(limit=1)[0]
        yes = kalshi.fetch_ohlcv(market.yes.id, timeframe="1d", limit=30)
        no = kalshi.fetch_ohlcv(market.no.id, timeframe="1d", limit=30)
        assert yes and len(yes) == len(no)
        for a, b in zip(yes, no):
            assert a.close + b.close == pytest.approx(market.face_value)
            assert a.bid_close + b.ask_close == pytest.approx(market.face_value)

    def test_status_all_works_on_every_venue(self):
        for venue_id in synpath.exchanges:
            with synpath.exchange(venue_id) as venue:
                assert venue.fetch_markets(status="all", limit=2) is not None, venue_id

    def test_kalshi_single_market_matches_the_listing(self, kalshi):
        listed = kalshi.fetch_markets(limit=1)[0]
        fetched = kalshi.fetch_market(listed.id)
        for field in ("category", "tags", "neg_risk", "series_id"):
            assert getattr(fetched, field) == getattr(listed, field), field

    def test_listing_markets_leaves_the_cache_small(self, kalshi):
        from synpath.kalshi import FACE_VALUE_CACHE

        for _ in range(3):
            kalshi.fetch_markets(limit=5)
        assert len(kalshi._face_values) <= FACE_VALUE_CACHE

    def test_computed_fields_are_on_the_wire(self):
        pytest.importorskip("fastapi")
        from fastapi.testclient import TestClient

        from synpath.server import create_app

        with TestClient(create_app()) as client:
            market = client.get("/venues/kalshi/markets", params={"limit": 1}).json()["data"][0]
            assert market["yes_id"].endswith(":yes")
            assert "spread" in market["instruments"][0]["quote"]
            book = client.get(f"/venues/kalshi/instruments/{market['yes_id']}/book").json()
            assert book["best_bid"] is None or "price" in book["best_bid"]


class TestSecondPassFixesLive:
    """The four findings from the second review, against the real venues."""

    def test_walking_kalshi_markets_skips_nothing(self, kalshi):
        """The cursor used to advance past a whole event page while returning
        only `limit` markets: a walk with limit=5 reached 15 of 120."""
        walked, cursor = [], None
        for _ in range(6):
            page = kalshi.fetch_markets(limit=5, cursor=cursor)
            walked += [market.id for market in page]
            cursor = page.next_cursor
            if not cursor:
                break

        # Markets come back ordered by ticker within each event page, so this
        # checks the walk against the same pages by membership, not by order.
        from synpath.kalshi import EVENT_PAGE

        flattened, event_cursor = set(), None
        for _ in range(len(walked) // EVENT_PAGE + 2):
            payload = kalshi._event_page(cursor=event_cursor, status="open", limit=EVENT_PAGE)
            flattened |= {
                market.id
                for raw in payload.get("events") or []
                for market in synpath.kalshi.normalize_event(raw).markets
                if market.status == "open"
            }
            event_cursor = payload.get("cursor")
            if not event_cursor:
                break

        assert len(walked) == len(set(walked)), "a market was repeated"
        assert set(walked) <= flattened

    def test_face_value_costs_one_request(self, kalshi):
        """It went through fetch_market, which also reads the parent event."""
        market = kalshi.fetch_markets(limit=1)[0]
        fresh = synpath.Kalshi()
        paths: list[str] = []
        original = fresh.http.get
        fresh.http.get = lambda path, params=None: (paths.append(path), original(path, params))[1]
        try:
            fresh.fetch_order_book(market.yes.id)
        finally:
            fresh.close()
        assert len(paths) == 2, paths

    def test_settled_means_the_same_thing_or_is_refused(self):
        for venue_id in synpath.exchanges:
            with synpath.exchange(venue_id) as venue:
                try:
                    page = venue.fetch_markets(status="settled", limit=3)
                except synpath.NotSupported:
                    continue
                assert all(m.status == "settled" for m in page), venue_id

    def test_bad_status_is_refused_before_any_request(self):
        for venue_id in synpath.exchanges:
            with synpath.exchange(venue_id) as venue:
                with pytest.raises(synpath.BadRequest):
                    venue.fetch_markets(status="bogus")

    def test_page_size_agrees_across_venues(self):
        sizes = {}
        for venue_id in synpath.exchanges:
            with synpath.exchange(venue_id) as venue:
                sizes[venue_id] = len(venue.fetch_markets(limit=100))
        assert len(set(sizes.values())) == 1, sizes


class TestThirdPassFixesLive:
    """The three findings from the third review, against the real venues."""

    def test_kalshi_search_finds_terms_a_catalog_scan_missed(self, kalshi):
        """'trump' and 'bitcoin' had zero hits in the first 200 events, so the
        old local scan returned empty pages for them page after page."""
        for term in ("trump", "bitcoin", "nba"):
            page = kalshi.fetch_markets(query=term, limit=5)
            assert page, term
            assert page.next_cursor, term

    def test_kalshi_search_is_bounded(self, kalshi):
        """It used to walk ~13,000 events before concluding nothing matched."""
        import time

        started = time.time()
        page = kalshi.fetch_markets(query="zzzqqqnonexistentxyz", limit=5)
        assert list(page) == []
        assert page.next_cursor is None, "no matches must end the walk"
        assert time.time() - started < 5

    def test_search_results_are_full_markets(self, kalshi):
        market = kalshi.fetch_markets(query="nba", limit=1)[0]
        assert market.status and market.tick_size and market.face_value
        assert market.series_id and market.category

    def test_search_pages_forward_without_repeats(self, kalshi):
        first = kalshi.fetch_markets(query="nba", limit=5)
        second = kalshi.fetch_markets(query="nba", limit=5, cursor=first.next_cursor)
        assert not {m.id for m in first} & {m.id for m in second}

    def test_a_cursor_from_another_query_is_refused(self, kalshi):
        page = kalshi.fetch_markets(query="nba", limit=3)
        with pytest.raises(synpath.BadRequest, match="different query"):
            kalshi.fetch_markets(query="fed", cursor=page.next_cursor, limit=3)

    def test_polymarket_until_selects_a_past_window(self, polymarket):
        import time

        # A market that already existed a month ago: the top of the volume
        # listing is usually a match listed this week, with no history then.
        until = int(time.time() * 1000) - 30 * 86_400_000
        market = next(
            (m for m in polymarket.fetch_markets(limit=100)
             if m.yes.venue_token_id and (m.open_timestamp or until) < until - 86_400_000),
            None,
        )
        if market is None:
            pytest.skip("no month-old market in the first page of the listing")
        candles = polymarket.fetch_ohlcv(market.yes.id, timeframe="1h", until=until, limit=5, source="quotes")
        assert candles
        assert all(candle.timestamp <= until for candle in candles)


class TestFourthPassFixesLive:
    """The fourth review's findings, against the real venues."""

    def test_event_search_finds_what_market_search_finds(self, kalshi):
        """fetch_events(query=) filtered one page locally and found nothing."""
        markets = kalshi.fetch_markets(query="trump", limit=10)
        events = kalshi.fetch_events(query="trump", limit=10)
        assert markets and events

    def test_search_results_carry_real_neg_risk(self, kalshi):
        """Search hits have no mutual-exclusivity flag; context built from them
        reported neg_risk=None for markets a listing reported True or False."""
        for market in kalshi.fetch_markets(query="nba", limit=5):
            assert market.neg_risk is not None
            assert market.neg_risk == kalshi.fetch_market(market.id).neg_risk

    def test_listing_walk_repeats_nothing(self, kalshi):
        seen, cursor = [], None
        for _ in range(6):
            page = kalshi.fetch_markets(limit=15, cursor=cursor)
            seen += [market.id for market in page]
            cursor = page.next_cursor
            if not cursor:
                break
        assert len(seen) == len(set(seen))

    def test_iter_events_pages_at_the_venue_maximum(self):
        from synpath.kalshi import VENUE_EVENT_PAGE

        venue = synpath.Kalshi()
        limits: list = []
        original = venue.http.get
        venue.http.get = lambda path, params=None: (
            limits.append((params or {}).get("limit")) if path == "/events" else None,
            original(path, params),
        )[1]
        try:
            iterator = venue.iter_events()
            for _ in range(3):
                next(iterator)
        finally:
            venue.close()
        assert limits[0] == VENUE_EVENT_PAGE


class TestVenueCapabilitiesLive:
    def test_kalshi_orders_the_page_by_volume(self, kalshi):
        volumes = [m.stats.volume_24h for m in kalshi.fetch_markets(sort="volume", limit=20)]
        known = [v for v in volumes if v is not None]
        assert known == sorted(known, reverse=True)
        assert volumes[len(known):] == [None] * (len(volumes) - len(known)), "unreported sorts last"

    def test_polymarket_us_orders_the_page_by_newest(self, polymarket_us):
        opened = [m.info["createdAt"] for m in polymarket_us.fetch_markets(sort="newest", limit=10)]
        assert opened == sorted(opened, reverse=True)

    def test_polymarket_us_orders_the_page_by_volume_from_the_book(self, polymarket_us):
        page = polymarket_us.fetch_markets(sort="volume", limit=5)
        volumes = [m.stats.volume_total for m in page]
        known = [v for v in volumes if v is not None]
        assert known and known == sorted(known, reverse=True)

    def test_batched_books_on_every_venue(self):
        for venue_id in synpath.exchanges:
            with synpath.exchange(venue_id) as venue:
                markets = venue.fetch_markets(limit=3)
                ids = [i.id for m in markets for i in m.instruments]
                books = venue.fetch_order_books(ids)
                assert set(books) == set(ids), venue_id

    def test_polymarket_sorts(self, polymarket):
        volumes = [m.stats.volume_24h or 0 for m in polymarket.fetch_markets(sort="volume", limit=5)]
        assert volumes == sorted(volumes, reverse=True)

    def test_newest_really_is_newest(self, polymarket):
        opens = [m.open_timestamp or 0 for m in polymarket.fetch_markets(sort="newest", limit=5)]
        assert opens == sorted(opens, reverse=True)

    def test_batch_lookup_is_one_request_per_batch(self, kalshi):
        ids = [market.id for market in kalshi.fetch_markets(limit=40)]
        paths: list[str] = []
        original = kalshi.http.get
        kalshi.http.get = lambda path, params=None: (paths.append(path), original(path, params))[1]
        try:
            markets = kalshi.fetch_markets_by_ids(ids)
        finally:
            kalshi.http.get = original
        assert [m.id for m in markets] == ids
        assert len(paths) == 1

    def test_outcome_label_names_the_market_inside_its_event(self, polymarket):
        event = next(
            e for e in polymarket.fetch_events(query="Fed decision", limit=5)
            if len(e.markets) > 3
        )
        labels = [market.outcome_label for market in event.markets]
        assert all(labels), labels
        assert len(set(labels)) == len(labels), "each market needs its own name in the event"
