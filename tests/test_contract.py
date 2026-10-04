"""The promises that hold on every venue.

A unified API is only worth the name if code written against one adapter runs
against the next one. These tests are parameterized over every registered
venue, so adding an adapter that breaks the contract fails here rather than in
somebody's strategy.
"""
from __future__ import annotations

import inspect

import httpx
import pytest

import synpath
from synpath import (
    BadRequest, Exchange, ExchangeNotAvailable, Kalshi, MarketNotFound,
    NotSupported, Polymarket, RateLimitExceeded, RequestTimeout,
)
from synpath.base import CAPABILITIES, HttpClient
from synpath.types import iso, ms

VENUES = list(synpath.exchanges.values())
VENUE_IDS = list(synpath.exchanges)


@pytest.fixture(params=VENUES, ids=VENUE_IDS)
def venue_class(request):
    return request.param


class TestInterface:
    def test_every_venue_is_an_exchange(self, venue_class):
        assert issubclass(venue_class, Exchange)

    def test_required_methods_exist(self, venue_class):
        for method in ("fetch_markets", "fetch_events", "fetch_order_book", "fetch_trades"):
            assert callable(getattr(venue_class, method))

    def test_capabilities_are_declared_for_every_public_method(self, venue_class):
        """`has` is the thing callers branch on, so it may not go stale."""
        for name in ("fetch_markets", "fetch_events", "fetch_order_book",
                     "fetch_trades", "fetch_ohlcv", "fetch_series", "fetch_fee_schedule"):
            assert name in venue_class.has, f"{venue_class.id} does not declare {name}"

    def test_capability_values_are_legal(self, venue_class):
        assert set(venue_class.has.values()) <= {True, False, "partial"}

    def test_every_venue_answers_every_capability_question(self, venue_class):
        """The capability matrix is complete, not sparse.

        A caller writing `venue.has["fetch_order_books"]` gets False on a venue
        without it, never a KeyError — otherwise checking before calling would
        be more dangerous than not checking. `Exchange.__init_subclass__`
        guarantees this; the test states the guarantee.
        """
        assert set(venue_class.has) == set(CAPABILITIES)

    def test_fetch_markets_signature_is_shared(self, venue_class):
        params = inspect.signature(venue_class.fetch_markets).parameters
        for expected in ("query", "limit", "cursor", "status"):
            assert expected in params, f"{venue_class.id}.fetch_markets lacks {expected}"

    def test_offset_paging_is_not_offered(self, venue_class):
        """Offset paging silently drops rows when the catalog changes between
        calls, so no adapter may expose it."""
        assert "offset" not in inspect.signature(venue_class.fetch_markets).parameters

    def test_unsupported_capability_raises(self, venue_class):
        venue = venue_class(limiter=None)
        for name, supported in venue.has.items():
            if supported is not False:
                continue
            method = getattr(venue, name, None)
            if method is None:
                continue
            with pytest.raises(NotSupported):
                method("anything")


class TestNormalizedShapes:
    """Both venues produce the same types from their own recorded payloads."""

    @pytest.fixture
    def markets(self, kalshi_market, kalshi_event, poly_market, polyus_market):
        from synpath.kalshi import normalize_market as kalshi_normalize
        from synpath.polymarket import normalize_market as poly_normalize
        from synpath.polymarket_us import normalize_market as polyus_normalize

        return {
            "kalshi": kalshi_normalize(kalshi_market, kalshi_event),
            "polymarket": poly_normalize(poly_market),
            "polymarket_us": polyus_normalize(polyus_market),
        }

    def test_ids_carry_the_venue(self, markets):
        for venue, market in markets.items():
            assert market.id == f"{venue}:{market.venue_market_id}"
            assert market.event_id is None or market.event_id.startswith(f"{venue}:")

    def test_yes_and_no_accessors_work_everywhere(self, markets):
        for venue, market in markets.items():
            assert market.yes is not None, venue
            assert market.no is not None, venue

    def test_prices_are_probabilities(self, markets):
        for market in markets.values():
            for instrument in (market.yes, market.no):
                for price in (instrument.quote.bid, instrument.quote.ask,
                              instrument.quote.mid, instrument.quote.last):
                    if price is not None:
                        assert 0 < price < market.face_value

    def test_mid_requires_both_sides(self, markets):
        """Never quietly substituted with whichever side happens to be quoted."""
        for market in markets.values():
            for instrument in (market.yes, market.no):
                quote = instrument.quote
                if quote.mid is not None:
                    assert quote.bid is not None and quote.ask is not None

    def test_last_always_carries_its_time(self, markets):
        for market in markets.values():
            for instrument in (market.yes, market.no):
                if instrument.quote.last is not None:
                    assert instrument.quote.last_timestamp is not None

    def test_stats_declare_their_units(self, markets):
        for market in markets.values():
            if market.stats.volume_24h is not None:
                assert market.stats.volume_unit in ("contracts", "collateral")

    def test_every_object_keeps_the_raw_payload(self, markets):
        for market in markets.values():
            assert market.info

    def test_timestamps_are_ms_and_iso(self, markets):
        for market in markets.values():
            for stamp, text in (
                (market.open_timestamp, market.open_datetime),
                (market.close_timestamp, market.close_datetime),
            ):
                if stamp is not None:
                    assert stamp > 10**12
                    assert text and text.endswith("Z")

    def test_serialises_to_json_for_the_server_layer(self, markets):
        """Everything a method returns has to survive the trip over HTTP
        unchanged, so the server layer stays a mechanical wrapper."""
        for market in markets.values():
            payload = market.model_dump_json()
            assert market.model_validate_json(payload) == market

    def test_status_vocabulary_is_shared(self, markets):
        for market in markets.values():
            assert market.status in ("unopened", "open", "closed", "settled")


class TestTimeHelpers:
    def test_ms_and_iso_round_trip(self):
        from datetime import datetime, timezone

        moment = datetime(2026, 9, 14, 6, 18, 43, tzinfo=timezone.utc)
        assert iso(ms(moment)) == "2026-09-14T06:18:43Z"

    def test_naive_datetime_is_read_as_utc(self):
        from datetime import datetime, timezone

        naive = datetime(2026, 9, 14, 6, 18, 43)
        aware = naive.replace(tzinfo=timezone.utc)
        assert ms(naive) == ms(aware)

    def test_none_passes_through(self):
        assert ms(None) is None and iso(None) is None


class TestErrorMapping:
    """A caller branches on the error type, so the mapping has to be exact."""

    def _client(self, handler, **kwargs):
        transport = httpx.MockTransport(handler)
        return HttpClient(
            "https://venue.test", limiter=None, attempts=2,
            client=httpx.Client(transport=transport),
            **kwargs,
        )

    def test_404_is_market_not_found(self):
        client = self._client(lambda request: httpx.Response(404, text="no such market"))
        with pytest.raises(MarketNotFound):
            client.get("/markets/nope")

    def test_400_is_bad_request(self):
        client = self._client(lambda request: httpx.Response(400, text="bad"))
        with pytest.raises(BadRequest):
            client.get("/markets")

    def test_503_is_venue_unavailable(self):
        client = self._client(lambda request: httpx.Response(503, text="maintenance"))
        with pytest.raises(ExchangeNotAvailable):
            client.get("/markets")

    def test_timeout_is_request_timeout(self):
        def handler(request):
            raise httpx.ReadTimeout("too slow", request=request)

        with pytest.raises(RequestTimeout):
            self._client(handler).get("/markets")

    def test_429_retries_then_raises(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            return httpx.Response(429, headers={"retry-after": "0"})

        with pytest.raises(RateLimitExceeded):
            self._client(handler).get("/markets")
        assert calls["n"] == 2, "a rate limit should be retried, not given up on"

    def test_429_that_clears_succeeds(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(429, headers={"retry-after": "0"})
            return httpx.Response(200, json={"ok": True})

        assert self._client(handler).get("/markets") == {"ok": True}

    def test_404_is_not_retried(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            return httpx.Response(404)

        with pytest.raises(MarketNotFound):
            self._client(handler).get("/markets")
        assert calls["n"] == 1, "waiting will not make a 404 valid"

    def test_none_params_are_dropped(self):
        seen = {}

        def handler(request):
            seen["url"] = str(request.url)
            return httpx.Response(200, json={})

        self._client(handler).get("/markets", {"limit": 10, "cursor": None})
        assert "cursor" not in seen["url"]
        assert "limit=10" in seen["url"]


class TestRegistry:
    def test_lookup_by_id(self):
        assert isinstance(synpath.exchange("kalshi", limiter=None), Kalshi)
        assert isinstance(synpath.exchange("polymarket", limiter=None), Polymarket)

    def test_unknown_venue_lists_the_known_ones(self):
        with pytest.raises(BadRequest, match="kalshi"):
            synpath.exchange("betfair")

    def test_context_manager_closes(self):
        with synpath.exchange("kalshi", limiter=None) as venue:
            assert venue.id == "kalshi"


class TestPaging:
    """Cursors belong to a response, not to the adapter.

    Regression: the adapters used to stash the cursor on `self`. One caller at
    a time never noticed; the server layer reuses one adapter per venue across
    requests, so two concurrent calls overwrote each other's position in the
    catalog and a page silently came back from the wrong place.
    """

    def test_no_adapter_keeps_cursor_state(self, venue_class):
        venue = venue_class(limiter=None)
        leaked = [name for name in vars(venue) if "cursor" in name.lower()]
        assert not leaked, f"{venue_class.id} stores {leaked} on the instance"

    def test_page_is_a_list(self):
        from synpath import Page

        page = Page([1, 2, 3], next_cursor="abc")
        assert page == [1, 2, 3]
        assert len(page) == 3 and page[0] == 1
        assert [item for item in page] == [1, 2, 3]
        assert page.next_cursor == "abc"

    def test_page_defaults_to_exhausted(self):
        from synpath import Page

        assert Page().next_cursor is None

    def test_concurrent_calls_keep_their_own_cursor(self):
        """The failure this was written for: shared adapter, parallel callers."""
        from concurrent.futures import ThreadPoolExecutor

        from synpath import Page

        class Stub(Exchange):
            id = "stub"
            name = "Stub"
            has: dict = {}

            def fetch_markets(self, *, query=None, limit=None, cursor=None, status="open"):
                return Page([], next_cursor=f"after-{cursor}")

            def fetch_events(self, **kwargs):  # pragma: no cover - unused
                raise NotSupported("stub")

            def fetch_order_book(self, instrument_id, **kwargs):  # pragma: no cover
                raise NotSupported("stub")

            def fetch_trades(self, market_id, **kwargs):  # pragma: no cover
                raise NotSupported("stub")

        shared = Stub()

        def call(n: int) -> tuple[str, str | None]:
            return f"p{n}", shared.fetch_markets(cursor=f"p{n}").next_cursor

        with ThreadPoolExecutor(max_workers=16) as pool:
            results = list(pool.map(call, range(200)))
        assert all(got == f"after-{sent}" for sent, got in results)


class TestCapabilityDeclaration:
    """A venue cannot be defined with a broken capability map.

    Every failure below used to be possible and silent. They now raise while
    the class is being created, so a broken venue is not importable, let alone
    constructible.
    """

    def test_silence_means_no_not_a_hole(self):
        """Regression: Kalshi once omitted `fetch_order_books` entirely, so
        `kalshi.has["fetch_order_books"]` raised KeyError on a caller who was
        doing the right thing."""
        venue = type("Quiet", (Exchange,), {"has": {"fetch_markets": True}})
        assert set(venue.has) == set(CAPABILITIES)
        assert venue.has["fetch_order_books"] is False
        assert venue.has["fetch_markets"] is True

    def test_declaring_nothing_still_answers_everything(self):
        venue = type("Silent", (Exchange,), {})
        assert set(venue.has) == set(CAPABILITIES)
        assert set(venue.has.values()) == {False}

    def test_a_typo_fails_at_import(self):
        """`fetch_orderbooks` for `fetch_order_books` was a silent no-op that
        left the real capability defaulting to False — a venue advertising
        that it cannot do something it can."""
        with pytest.raises(TypeError, match="unknown capability"):
            type("Typo", (Exchange,), {"has": {"fetch_orderbooks": True}})

    def test_the_error_names_the_legal_keys(self):
        with pytest.raises(TypeError, match="fetch_order_books"):
            type("Typo", (Exchange,), {"has": {"fetch_orderbooks": True}})

    def test_an_illegal_value_fails_at_import(self):
        with pytest.raises(TypeError, match="illegal values"):
            type("Loose", (Exchange,), {"has": {"fetch_ohlcv": "yes"}})

    def test_has_must_be_a_mapping(self):
        with pytest.raises(TypeError, match="must be a dict"):
            type("Listy", (Exchange,), {"has": ["fetch_markets"]})

    def test_subclassing_an_adapter_inherits_rather_than_resets(self):
        """A sandbox or demo variant changes one answer, not all of them."""
        demo = type("KalshiDemo", (Kalshi,), {"has": {"fetch_trades": False}})
        assert demo.has["fetch_series"] is Kalshi.has["fetch_series"]
        assert demo.has["fetch_trades"] is False

    def test_the_base_class_itself_is_complete(self):
        assert set(Exchange.has) == set(CAPABILITIES)

    def test_capability_list_covers_every_fetch_method(self):
        """Adding a method without naming it in CAPABILITIES would leave a
        feature no venue could ever advertise."""
        methods = {name for name in vars(Exchange) if name.startswith("fetch_")}
        assert methods <= set(CAPABILITIES), sorted(methods - set(CAPABILITIES))


class TestComputedFieldsReachTheWire:
    """Regression: `spread`, `best_bid`, `best_ask` and the yes/no lookup were
    plain Python properties, so they appeared in neither the JSON nor the
    OpenAPI schema. Every non-Python client had to reimplement them — including
    picking a side, which on a Kalshi market that labels both sides identically
    is the one mistake that silently inverts a position."""

    def test_spread_is_serialized(self):
        from synpath import Quote

        payload = Quote(bid=0.40, ask=0.42).model_dump()
        assert payload["spread"] == pytest.approx(0.02)

    def test_best_levels_are_serialized(self):
        from synpath import OrderBook, OrderLevel

        book = OrderBook(
            market_id="m", venue="v",
            bids=[OrderLevel(price=0.4, size=1), OrderLevel(price=0.3, size=2)],
            asks=[OrderLevel(price=0.5, size=3)],
        )
        payload = book.model_dump()
        assert payload["best_bid"]["price"] == 0.4
        assert payload["best_ask"]["price"] == 0.5

    def test_empty_side_serializes_as_null(self):
        from synpath import OrderBook

        payload = OrderBook(market_id="m", venue="v").model_dump()
        assert payload["best_bid"] is None and payload["best_ask"] is None

    def test_both_sides_are_serialized_by_position(self, kalshi_market, kalshi_event):
        """A non-Python client reads `yes` and `no` as fields, never by label
        text: some Kalshi markets label both sides identically."""
        from synpath.kalshi import normalize_market

        payload = normalize_market(kalshi_market, kalshi_event).model_dump()
        assert payload["yes"]["quote"]["bid"] == 0.10
        assert payload["no"]["quote"]["bid"] == 0.88

    def test_a_serialized_model_can_be_read_back(self, kalshi_market, kalshi_event):
        """`extra='forbid'` would otherwise reject the very fields this library
        added on the way out, so a client could not echo a response back."""
        from synpath.kalshi import normalize_market

        market = normalize_market(kalshi_market, kalshi_event)
        assert market.model_validate(market.model_dump()) == market
        assert market.model_validate_json(market.model_dump_json()) == market

    def test_a_real_typo_is_still_refused(self):
        from synpath import Quote

        with pytest.raises(Exception, match="extra_forbidden|Extra inputs"):
            Quote.model_validate({"bid": 0.4, "aks": 0.42})


class TestRetryWait:
    """Regression: the 429 path slept for whatever the venue asked, uncapped.
    In the server that thread belongs to a shared pool, so a few rate-limited
    calls could take the whole service down."""

    def test_wait_is_capped(self, monkeypatch):
        from synpath.base import MAX_RETRY_WAIT

        slept: list[float] = []
        monkeypatch.setattr("synpath.base.time.sleep", slept.append)

        def handler(request):
            return httpx.Response(429, headers={"retry-after": "600"})

        client = HttpClient(
            "https://venue.test", limiter=None, attempts=2,
            client=httpx.Client(transport=httpx.MockTransport(handler)),
        )
        with pytest.raises(RateLimitExceeded):
            client.get("/markets")
        assert slept and max(slept) <= MAX_RETRY_WAIT

    def test_the_venues_own_value_is_still_reported(self):
        def handler(request):
            return httpx.Response(429, headers={"retry-after": "600"})

        client = HttpClient(
            "https://venue.test", limiter=None, attempts=1,
            client=httpx.Client(transport=httpx.MockTransport(handler)),
        )
        with pytest.raises(RateLimitExceeded) as caught:
            client.get("/markets")
        assert caught.value.retry_after == 600


class TestSharedVocabulary:
    """The same argument must mean the same thing on every venue. This is the
    one promise a unified API cannot compromise on."""

    def test_unknown_status_is_refused_everywhere(self, venue_class):
        venue = venue_class(limiter=None)
        with pytest.raises(BadRequest, match="unknown status"):
            venue.fetch_markets(status="bogus")

    def test_every_status_is_either_handled_or_refused(self, venue_class):
        """A venue filters by a status word or says it cannot. Quietly
        answering a different question -- which is what `settled` used to do on
        Polymarket -- is the failure this guards."""
        from synpath import NotSupported
        from synpath.base import MARKET_STATUSES

        venue = venue_class(limiter=None)
        venue_reached = []

        # Empty answers to a venue that reads its catalog over POST
        # (Hyperliquid's `/info`), by request type.
        empty_posts = {
            "outcomeMeta": {"outcomes": [], "questions": []},
            "outcomeTemplates": [],
            "spotMetaAndAssetCtxs": [{}, []],
        }

        class FakeHttp:
            def get(self, path, params=None):
                venue_reached.append(params or {})
                return {"events": [], "markets": [], "next_cursor": None}

            def post(self, path, json=None):
                venue_reached.append(json or {})
                return empty_posts.get((json or {}).get("type"), {})

        for attribute in ("http", "gamma"):
            if hasattr(venue, attribute):
                setattr(venue, attribute, FakeHttp())

        for status in MARKET_STATUSES:
            venue_reached.clear()
            if hasattr(venue, "_catalog"):
                venue._catalog = None  # a cached catalog would answer without asking
            try:
                venue.fetch_markets(status=status, limit=1)
            except NotSupported:
                assert not venue_reached, (
                    f"{venue_class.id} refused {status!r} only after asking the venue"
                )
                continue
            assert venue_reached, f"{venue_class.id} answered {status!r} without a request"

    def test_page_limit_ceiling_is_shared(self, venue_class):
        from synpath.base import MAX_PAGE_LIMIT, page_limit

        assert page_limit(MAX_PAGE_LIMIT * 10) == MAX_PAGE_LIMIT
        assert page_limit(None) is None
        assert page_limit(0) == 1


class TestSettlementSources:
    """Who rules on the outcome, in one shape on both venues.

    Needed to judge whether two venues listing the same question would settle
    it the same way; `description` says what has to happen, this says who
    decides whether it did.
    """

    def test_kalshi_inherits_them_from_its_event(self, kalshi_market, kalshi_event):
        from synpath.kalshi import normalize_market

        market = normalize_market(kalshi_market, kalshi_event)
        assert market.settlement_sources == kalshi_event["settlement_sources"]

    def test_kalshi_event_carries_them(self, kalshi_event):
        from synpath.kalshi import normalize_event

        assert normalize_event(kalshi_event).settlement_sources

    def test_polymarket_normalizes_its_single_field(self):
        from synpath.polymarket import settlement_sources_of

        assert settlement_sources_of({"resolutionSource": "https://wtatennis.com/scores"}) == [
            {"name": "https://wtatennis.com/scores", "url": "https://wtatennis.com/scores"}
        ]

    def test_a_non_url_source_keeps_the_name_without_inventing_a_link(self):
        from synpath.polymarket import settlement_sources_of

        assert settlement_sources_of({"resolutionSource": "Associated Press"}) == [
            {"name": "Associated Press", "url": None}
        ]

    def test_an_empty_field_is_no_source_not_a_blank_one(self):
        from synpath.polymarket import settlement_sources_of

        assert settlement_sources_of({"resolutionSource": "  "}) == []
        assert settlement_sources_of({}) == []

    def test_a_market_falls_back_to_its_event(self, poly_market, poly_event):
        from synpath.polymarket import normalize_market

        market = normalize_market(
            {**poly_market, "resolutionSource": ""},
            {**poly_event, "resolutionSource": "Reuters"},
        )
        assert [s["name"] for s in market.settlement_sources] == ["Reuters"]

    def test_both_venues_report_the_same_shape(self, kalshi_market, kalshi_event, poly_market):
        from synpath.kalshi import normalize_market as kalshi_normalize
        from synpath.polymarket import normalize_market as poly_normalize

        for market in (
            kalshi_normalize(kalshi_market, kalshi_event),
            poly_normalize({**poly_market, "resolutionSource": "Reuters"}),
        ):
            assert all(set(source) >= {"name"} for source in market.settlement_sources)


class TestSortVocabulary:
    """One set of sort keys, honoured on every venue, each by its own figure."""

    def test_unknown_sort_is_refused_everywhere(self, venue_class):
        venue = venue_class(limiter=None)
        with pytest.raises(BadRequest, match="unknown sort"):
            venue.fetch_markets(sort="by_vibes")

    def test_every_venue_sorts(self, venue_class):
        """Every key on every venue, or -- where the venue publishes no figure
        for a key at all (Opinion has no liquidity) -- `partial`, and that key
        refused before any request rather than answered unsorted."""
        from synpath.base import MARKET_SORTS

        assert venue_class.has["sort"] in (True, "partial")
        if venue_class.has["sort"] is True:
            return
        venue = venue_class(limiter=None)

        class Unreachable:
            def get(self, path, params=None):
                raise AssertionError(f"{venue_class.id} asked the venue before refusing a sort")

            def post(self, path, json=None):
                raise AssertionError(f"{venue_class.id} asked the venue before refusing a sort")

        venue.http = Unreachable()
        refused = 0
        for key in MARKET_SORTS:
            try:
                venue.fetch_markets(sort=key, limit=1)
            except NotSupported:
                refused += 1
            except AssertionError:
                continue
        assert 0 < refused < len(MARKET_SORTS)

    def test_kalshi_orders_the_page_it_read(self, kalshi_event):
        """The venue ignores its own sort parameters, so the page is ordered
        after it is read, the way ccxt and pmxt do it."""
        import copy

        rows = []
        for index, volume in enumerate(("5.00", "50.00", "0.50")):
            event = copy.deepcopy(kalshi_event)
            event["event_ticker"] = f"EV-{index}"
            market = event["markets"][0]
            market["ticker"] = f"EV-{index}-M"
            market["volume_24h_fp"] = volume
            market["open_time"] = f"2026-0{index + 1}-01T00:00:00Z"
            event["markets"] = [market]
            rows.append(event)

        class FakeHttp:
            def get(self, path, params=None):
                return {"events": rows, "cursor": None}

        venue = Kalshi(limiter=None)
        venue.http = FakeHttp()
        by_volume = [m.stats.volume_24h for m in venue.fetch_markets(sort="volume", status="all")]
        assert by_volume == sorted(by_volume, reverse=True)
        by_age = [m.open_timestamp for m in venue.fetch_markets(sort="newest", status="all")]
        assert by_age == sorted(by_age, reverse=True)

    def test_a_row_without_the_figure_sorts_last_not_first(self):
        from synpath.base import sort_page

        class Row:
            def __init__(self, v):
                self.v = v

        ordered = sort_page([Row(None), Row(1.0), Row(3.0)], key=lambda r: r.v)
        assert [r.v for r in ordered] == [3.0, 1.0, None]

    def test_polymarket_maps_every_key_to_a_gamma_field(self):
        from synpath.base import MARKET_SORTS
        from synpath.polymarket import SORT_FIELDS

        assert set(SORT_FIELDS) == set(MARKET_SORTS)

    def test_the_sort_reaches_the_venue(self):
        sent = {}

        class FakeGamma:
            def get(self, path, params=None):
                sent.update(params or {})
                return {"markets": [], "next_cursor": None}

        poly = Polymarket(limiter=None)
        poly.gamma = FakeGamma()
        poly.fetch_markets(sort="newest")
        assert sent["order"] == "startDate" and sent["ascending"] == "false"


class TestBatchLookup:
    """One request instead of one per market: 40 markets measured at 0.2s
    batched against 11.6s one at a time."""

    def test_both_venues_offer_it(self, venue_class):
        assert venue_class.has["fetch_markets_by_ids"] is True

    def test_kalshi_keeps_the_order_asked_for(self):
        class FakeHttp:
            def get(self, path, params=None):
                return {"markets": [
                    {"ticker": "C", "status": "active"},
                    {"ticker": "A", "status": "active"},
                ]}

        venue = Kalshi(limiter=None)
        venue.http = FakeHttp()
        assert [m.id for m in venue.fetch_markets_by_ids(["A", "kalshi:C"])] == ["kalshi:A", "kalshi:C"]

    def test_an_id_the_venue_dropped_is_left_out_not_raised(self):
        """A market closing is normal; what that means is the caller's call."""
        class FakeHttp:
            def get(self, path, params=None):
                return {"markets": [{"ticker": "A", "status": "active"}]}

        venue = Kalshi(limiter=None)
        venue.http = FakeHttp()
        assert [m.id for m in venue.fetch_markets_by_ids(["A", "GONE"])] == ["kalshi:A"]

    def test_polymarket_repeats_the_id_parameter(self):
        """`?id=1&id=2` cannot be expressed as a dict, which is why the HTTP
        layer accepts a list of pairs."""
        sent = {}

        class FakeGamma:
            def get(self, path, params=None):
                sent["params"] = params
                return []

        poly = Polymarket(limiter=None)
        poly.gamma = FakeGamma()
        poly.fetch_markets_by_ids(["1", "2"])
        assert ("id", "1") in sent["params"] and ("id", "2") in sent["params"]


class TestBatchedBooks:
    """Many books in one call on every venue. What differs is the cost, and
    the docstring says: one round trip on Polymarket, one request per market
    on Kalshi and Polymarket US."""

    def test_every_venue_offers_it(self, venue_class):
        assert venue_class.has["fetch_order_books"] is True

    def test_kalshi_reads_each_market_once(self, kalshi_orderbook):
        paths: list[str] = []

        class FakeHttp:
            def get(self, path, params=None):
                paths.append(path)
                if path.endswith("/orderbook"):
                    return kalshi_orderbook
                return {"market": {"ticker": "T", "notional_value_dollars": "1.00"}}

        venue = Kalshi(limiter=None)
        venue.http = FakeHttp()
        books = venue.fetch_order_books(["A", "kalshi:A", "B"])
        assert set(books) == {"kalshi:A", "kalshi:B"}
        assert [p for p in paths if p.endswith("/orderbook")] == ["/markets/A/orderbook", "/markets/B/orderbook"]
        no = venue.fetch_order_books(["A"], side="no")["kalshi:A"]
        assert books["kalshi:A"].best_bid.price + no.best_ask.price == pytest.approx(1.0)

    def test_polymarket_us_reads_each_market_once(self, polyus_book):
        paths: list[str] = []

        class FakeHttp:
            def get(self, path, params=None):
                paths.append(path)
                return polyus_book

        from synpath import PolymarketUS

        venue = PolymarketUS(limiter=None)
        venue.http = FakeHttp()
        books = venue.fetch_order_books(["a", "b"], side="no")
        assert set(books) == {"polymarket_us:a", "polymarket_us:b"}
        assert paths == ["/markets/a/book", "/markets/b/book"]
        assert books["polymarket_us:a"].derived is True
        assert venue.fetch_order_books(["a"])["polymarket_us:a"].derived is False


class TestOutcomeLabel:
    """A market's short name inside its event: `title` is the whole question,
    this is what the venue lists it under."""

    def test_kalshi_uses_the_yes_side_subtitle(self, kalshi_market, kalshi_event):
        from synpath.kalshi import normalize_market

        market = normalize_market(kalshi_market, kalshi_event)
        assert market.outcome_label == kalshi_market["yes_sub_title"]

    def test_polymarket_uses_the_group_item_title(self, poly_market):
        from synpath.polymarket import normalize_market

        market = normalize_market({**poly_market, "groupItemTitle": "No change"})
        assert market.outcome_label == "No change"

    def test_absent_stays_none_rather_than_echoing_the_question(self, poly_market):
        from synpath.polymarket import normalize_market

        assert normalize_market({**poly_market, "groupItemTitle": ""}).outcome_label is None


class TestMatching:
    """Matching spans venues, so no venue adapter can answer it; a hosted service does, over
    HTTP, and this module (`synpath.matching`) is only the client for that service."""

    def test_no_adapter_claims_it(self, venue_class):
        assert venue_class.has["match_market"] is False
        assert venue_class.has["match_event"] is False

    def test_without_a_base_url_it_calls_synpaths_own_service(self, monkeypatch):
        from synpath.matching import BASE_URL_ENV, DEFAULT_URL

        monkeypatch.delenv(BASE_URL_ENV, raising=False)
        seen = []

        def handler(request):
            seen.append(str(request.url))
            return httpx.Response(200, json={"anchor": "kalshi:KXFOO-25", "event_id": None, "matched": None})
        synpath.match_market("kalshi:KXFOO-25", client=self._client(handler))
        assert seen[0].startswith(DEFAULT_URL + "/match/market") and DEFAULT_URL == "https://api.synpath.dev"
        monkeypatch.setenv(BASE_URL_ENV, "http://staging.test")
        synpath.match_market("kalshi:KXFOO-25", client=self._client(handler))
        assert seen[1].startswith("http://staging.test/match/market"), "the variable still overrides"

    def test_the_anchor_must_be_a_synpath_id(self):
        from synpath import BadRequest

        for bad in ("KXFOO-25", "river:123", "kalshi:"):
            with pytest.raises(BadRequest):
                synpath.match_market(bad, base_url="http://matching.test")
            with pytest.raises(BadRequest):
                synpath.match_event(bad, base_url="http://matching.test")

    def _client(self, handler):
        return httpx.Client(transport=httpx.MockTransport(handler))

    def test_match_market_returns_the_link_when_the_other_venue_has_one(self):
        def handler(request):
            assert request.url.params["id"] == "kalshi:KXHIGHTATL-26SEP23-B80"
            return httpx.Response(200, json={
                "anchor": "kalshi:KXHIGHTATL-26SEP23-B80",
                "event_id": "weather:atlanta:temperature:max:at:2026-09-23",
                "matched": {"id": "polymarket:4784493", "venue": "polymarket", "side_map": {"yes": "yes", "no": "no"}},
            })
        res = synpath.match_market("kalshi:KXHIGHTATL-26SEP23-B80", base_url="http://matching.test", client=self._client(handler))
        assert res.event_id == "weather:atlanta:temperature:max:at:2026-09-23"
        assert res.matched == synpath.MarketLink(id="polymarket:4784493", venue="polymarket", side_map={"yes": "yes", "no": "no"})

    def test_match_market_is_none_not_an_error_when_there_is_no_pair(self):
        """A real listing with nothing on the other venue asking the same question is a
        different, and much more common, answer than 'no such market'."""
        def handler(request):
            return httpx.Response(200, json={"anchor": "kalshi:KXHIGHNY-26SEP23-B65.5", "event_id": "weather:new-york-city:temperature:max:at:2026-09-23", "matched": None})
        res = synpath.match_market("kalshi:KXHIGHNY-26SEP23-B65.5", base_url="http://matching.test", client=self._client(handler))
        assert res.matched is None
        assert res.event_id is not None   # the anchor still parsed onto a real card

    def test_match_market_404_raises_market_not_found(self):
        res = self._client(lambda request: httpx.Response(404, json={"detail": "market not found"}))
        with pytest.raises(synpath.MarketNotFound):
            synpath.match_market("kalshi:NOPE-1", base_url="http://matching.test", client=res)

    def test_match_event_returns_a_list_per_venue_and_none_for_a_venue_with_nothing(self):
        def handler(request):
            return httpx.Response(200, json={
                "anchor": "kalshi:KXHIGHTATL", "event_ids": ["weather:atlanta:temperature:max:at:2026-09-23"],
                "events": {"polymarket": ["polymarket:1003798"]},
            })
        res = synpath.match_event("kalshi:KXHIGHTATL", base_url="http://matching.test", client=self._client(handler))
        assert res.event_ids == ["weather:atlanta:temperature:max:at:2026-09-23"]
        assert res.events == {"polymarket": ["polymarket:1003798"]}

    def test_side_map_survives_json(self):
        link = synpath.MarketLink(id="polymarket:2252244", venue="polymarket", side_map={"yes": "no", "no": "yes"})
        assert synpath.MarketLink.model_validate_json(link.model_dump_json()) == link

    def test_injected_client_is_not_closed_by_the_call(self):
        """A caller pooling its own httpx.Client keeps owning its lifecycle; only a client this
        call built for itself (no `client=` given) is closed after."""
        client = self._client(lambda request: httpx.Response(200, json={"anchor": "a", "event_id": None, "matched": None}))
        synpath.match_market("kalshi:KXFOO-25", base_url="http://matching.test", client=client)
        assert not client.is_closed
