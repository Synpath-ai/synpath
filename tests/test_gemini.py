"""Gemini Predictions: ids, the YES-space book, sports taxonomy, the catalog
walk, status, trades, candles, fees.

The samples under `tests/samples/gemini_*.json` were recorded from
api.gemini.com's public endpoints, with no key, on 2026-10-23, then trimmed:
contract and child-event lists cut to two, an event's combo list to one.
Every request goes to an `httpx.MockTransport`; nothing here reaches Gemini.
"""
from __future__ import annotations

import copy
import re

import httpx
import pytest

import synpath
from synpath import BadRequest, MarketNotFound, NotSupported
from synpath.gemini import (
    MAKER_PROMO, SCHEDULE_FROM, Gemini, candidate_event_tickers, fee_schedule_of, normalize_candle,
    normalize_event, normalize_market, normalize_order_book, normalize_trade, parse_ts, plain_text, status_of,
)

from conftest import load

EVENTS = load("gemini_events.json")
EVENT = load("gemini_event.json")                 # Chicago vs Milwaukee, moneyline, with its spread and total
CRYPTO = load("gemini_crypto_event.json")         # a BTC 5-minute Up/Down, binary
BOOK = load("gemini_book.json")
TRADES = load("gemini_trades.json")
CANDLES = load("gemini_candles.json")
CATEGORIES = load("gemini_categories.json")
NOT_FOUND = load("gemini_not_found.json")

MIL = "gemini:GEMI-NBA-2610232000-CHI-MIL-M-MIL"
GAME = "gemini:NBA-2610232000-CHI-MIL-M"
OUTSIDE_PROMO = MAKER_PROMO[1] + 1
INSIDE_PROMO = MAKER_PROMO[0] + 1


class Api:
    """api.gemini.com, answered from the samples. Records every request."""

    def __init__(self, events: list | None = None, total: int | None = None):
        self.requests: list[httpx.Request] = []
        self.events = events if events is not None else EVENTS["data"]
        self.total = total if total is not None else len(self.events)

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if path == "/v1/prediction-markets/events":
            offset = int(request.url.params.get("offset", 0))
            limit = int(request.url.params.get("limit", 50))
            rows = self.events[offset:offset + limit]
            return httpx.Response(200, json={"data": rows, "pagination": {"limit": limit, "offset": offset,
                                                                          "total": self.total}})
        found = re.fullmatch(r"/v1/prediction-markets/events/([^/]+)", path)
        if found:
            ticker = found.group(1).upper()
            for event in [EVENT, CRYPTO, *EVENT["events"]]:
                if event["ticker"].upper() == ticker:
                    return httpx.Response(200, json=event)
            return httpx.Response(404, json=NOT_FOUND)
        if path == "/v1/prediction-markets/categories":
            return httpx.Response(200, json=CATEGORIES)
        if path.startswith("/v1/book/"):
            return httpx.Response(200, json=BOOK)
        if path.startswith("/v1/trades/"):
            return httpx.Response(200, json=TRADES)
        if path.startswith("/v2/candles/"):
            return httpx.Response(200, json=CANDLES)
        return httpx.Response(404, json={"error": "no route"})


def adapter(api: Api | None = None) -> tuple[Gemini, Api]:
    api = api or Api()
    return Gemini(limiter=None, client=httpx.Client(transport=httpx.MockTransport(api.handler))), api


class TestIds:
    def test_a_contract_is_a_market_named_by_its_instrument_symbol(self):
        market = normalize_market(EVENT["contracts"][0], EVENT)
        assert market.id == MIL and market.venue_market_id == "GEMI-NBA-2610232000-CHI-MIL-M-MIL"
        assert market.event_id == GAME

    def test_the_venue_is_registered(self):
        assert synpath.exchanges["gemini"] is Gemini and "gemini" in synpath.ids.VENUES

    def test_candidate_events_drop_one_segment_at_a_time(self):
        assert candidate_event_tickers("GEMI-NBA-2610232000-CHI-MIL-M-MIL")[:2] == [
            "NBA-2610232000-CHI-MIL-M", "NBA-2610232000-CHI-MIL"]
        assert candidate_event_tickers("gemi-btc05m2610231640-up") == ["BTC05M2610231640"]
        with pytest.raises(BadRequest):
            candidate_event_tickers("NBA-2610232000")


class TestNormalize:
    def test_the_quote_is_the_yes_book_and_no_is_its_mirror(self):
        market = normalize_market(EVENT["contracts"][0], EVENT)
        assert (market.yes.quote.bid, market.yes.quote.ask) == (0.52, 0.53)
        assert (market.no.quote.bid, market.no.quote.ask) == (0.47, 0.48)
        assert market.yes.quote.mid == 0.525 and market.no.quote.mid == 0.475
        assert market.book_model == "shared_complement" and market.tick_size == 0.01

    def test_a_last_price_without_a_time_stays_out_of_the_quote(self):
        market = normalize_market(EVENT["contracts"][0], EVENT)
        assert market.yes.quote.last is None and market.info["last_trade_price"] == 0.52

    def test_labels_and_titles(self):
        market = normalize_market(EVENT["contracts"][0], EVENT)
        assert market.title == "Chicago vs Milwaukee: Milwaukee"
        assert market.yes.label == "Milwaukee" and market.no.label == "Not Milwaukee"
        up = normalize_market(CRYPTO["contracts"][0], CRYPTO)
        assert up.title == CRYPTO["title"] and up.yes.label == "Up"

    def test_the_sports_taxonomy_rides_on_the_market(self):
        market = normalize_market(EVENT["contracts"][0], EVENT)
        sports = market.info["sports_market"]
        assert sports["sport"] == "basketball" and sports["type"] == "moneyline" and sports["scope"]["type"] == "full_contest"
        assert "basketball" in market.tags and "moneyline" in market.tags

    def test_status_and_times(self):
        market = normalize_market(EVENT["contracts"][0], EVENT)
        assert market.status == "open" and market.active
        assert market.close_timestamp == parse_ts(EVENT["contracts"][0]["expiryDate"])
        assert market.close_datetime.endswith("Z")

    def test_the_contract_grid_is_kept(self):
        info = normalize_market(EVENT["contracts"][0], EVENT).info
        assert info["quantity_increment"] == "0.01" and info["quantity_minimum"] == "0.01"
        assert info["price_increment"] == "0.01"

    def test_an_up_down_contract_keeps_its_strike_and_source(self):
        up = normalize_market(CRYPTO["contracts"][0], CRYPTO)
        assert up.info["strike"]["type"] == "reference"
        assert up.settlement_sources == [{"name": "lukka", "index": "LS-LRRR-XBTUSD", "url": None}]
        assert up.description and "Bitcoin" in up.description

    def test_an_event_holds_its_own_contracts_and_names_its_children(self):
        event = normalize_event(EVENT)
        assert event.id == GAME and [m.id for m in event.markets][0] == MIL and len(event.markets) == 2
        assert event.info["child_events"] == ["NBA-2610232000-CHI-MIL-S", "NBA-2610232000-CHI-MIL-T"]
        assert event.mutually_exclusive is True
        assert event.info["volume_usd"] == float(EVENT["volume"])

    def test_a_spread_event_is_not_called_exclusive(self):
        spread = normalize_event(EVENT["events"][0])
        assert spread.info["sports_market"]["type"] == "spread" and spread.mutually_exclusive is None

    @pytest.mark.parametrize("event, contract, state, expiry, status, active", [
        ("active", "active", "open", None, "open", True),
        ("active", "active", "closed", 10**15, "open", False),     # a pause before expiry
        ("active", "active", "closed", 1, "closed", False),        # past expiry
        ("approved", None, None, None, "unopened", False),
        ("closed", "closed", "closed", None, "closed", False),
        ("under_review", None, None, None, "closed", False),
        ("settled", "settled", "closed", None, "settled", False),
        ("invalid", None, None, None, "settled", False),
    ])
    def test_status(self, event, contract, state, expiry, status, active):
        assert status_of(event, contract, state, expiry, now_ms=10**13) == (status, active)

    def test_rich_text_becomes_text(self):
        doc = {"nodeType": "document", "content": [{"nodeType": "paragraph", "content": [
            {"nodeType": "text", "value": "Resolves "}, {"nodeType": "text", "value": "Yes."}]}]}
        assert plain_text(doc) == "Resolves Yes." and plain_text(None) is None and plain_text("x") == "x"


class TestBookTradesCandles:
    def test_the_book_is_yes_space(self):
        book = normalize_order_book(BOOK, symbol="GEMI-NBA-2610232000-CHI-MIL-M-MIL")
        assert book.market_id == MIL and book.side == "yes" and not book.derived
        assert [lvl.price for lvl in book.bids] == [0.52, 0.51, 0.48] and book.best_ask.price == 0.53
        assert book.best_bid.size == 5000.0 and book.timestamp == 1792773399000

    def test_the_no_book_mirrors_it(self):
        book = normalize_order_book(BOOK, symbol="GEMI-NBA-2610232000-CHI-MIL-M-MIL", side="no", depth=1)
        assert book.derived and book.depth_scope == "top_n"
        assert (book.best_bid.price, book.best_bid.size) == (0.47, 8050.0)
        assert (book.best_ask.price, book.best_ask.size) == (0.48, 5000.0)

    def test_a_trade_is_the_yes_price_with_the_takers_side(self):
        trade = normalize_trade(TRADES[3], symbol="gemi-nba-2610232000-chi-mil-m-mil")
        assert trade.market_id == MIL and trade.price == 0.5 and trade.quantity == 20.93
        assert trade.side == "sell" and trade.timestamp == 1792725449821 and trade.id == "1893456012673292"

    def test_a_bar_without_trades_is_the_last_close_repeated(self):
        empty, traded = normalize_candle(CANDLES[0], interval_seconds=3600), normalize_candle(CANDLES[2], interval_seconds=3600)
        assert empty.price_source == "sampled_last" and empty.volume is None
        assert traded.price_source == "trade" and traded.volume == 76.23 and traded.close == 0.52


class TestFees:
    CURRENT = {"instrumentSymbol": "GEMI-X-Y", "createdAt": "2026-10-20T00:00:47.320Z"}
    LEGACY = {"instrumentSymbol": "GEMI-X-Y", "createdAt": "2026-02-20T00:00:00.000Z"}

    @pytest.mark.parametrize("price, contracts, taker, fee", [
        # Gemini's own fee examples (gemini.com/fees/predictions).
        (0.40, 100, True, 1.68), (0.90, 100, True, 0.63), (0.10, 100, False, 0.16), (0.50, 100, False, 0.44),
    ])
    def test_the_published_examples(self, price, contracts, taker, fee):
        schedule = fee_schedule_of(self.CURRENT, now_ms=OUTSIDE_PROMO)
        assert schedule.fee_type == "quadratic_theta" and schedule.rounding == "up_to_cent"
        assert schedule.estimate(price, contracts, taker=taker) == fee

    def test_the_fee_rounds_up_to_the_cent(self):
        assert fee_schedule_of(self.CURRENT, now_ms=OUTSIDE_PROMO).estimate(0.5, 1) == 0.02   # 0.0175

    def test_makers_pay_nothing_during_the_promotion(self):
        schedule = fee_schedule_of(self.CURRENT, now_ms=INSIDE_PROMO)
        assert schedule.maker_rate == 0.0 and schedule.estimate(0.5, 100, taker=False) == 0.0
        assert schedule.info["maker_promotion"] is True

    def test_contracts_listed_before_the_schedule_pay_the_legacy_one(self):
        schedule = fee_schedule_of(self.LEGACY, now_ms=OUTSIDE_PROMO)
        assert schedule.fee_type == "min_price" and (schedule.taker_rate, schedule.maker_rate) == (0.05, 0.01)
        assert schedule.estimate(0.9, 100) == 0.5      # 0.05 * 0.10 * 100
        assert parse_ts(self.LEGACY["createdAt"]) < SCHEDULE_FROM


class TestAdapter:
    def test_markets_page_through_events_with_a_cursor(self):
        gemini, api = adapter()
        first = gemini.fetch_markets(limit=3)
        assert [m.id for m in first][:2] == [MIL, "gemini:GEMI-NBA-2610232000-CHI-MIL-M-CHI"] and len(first) == 3
        rest = gemini.fetch_markets(limit=3, cursor=first.next_cursor)
        assert [m.id for m in rest] == ["gemini:GEMI-NBA-2610232300-NYK-BOS-M-NYK"] and rest.next_cursor is None
        assert api.requests[0].url.params.get_list("status") == ["active"]

    def test_sports_filters_go_to_the_venue(self):
        gemini, api = adapter()
        gemini.fetch_markets(sport=["basketball", "baseball"], sports_market_type="moneyline", category="Sports",
                             query="Chicago")
        params = api.requests[0].url.params
        assert params.get_list("sport") == ["basketball", "baseball"]
        assert params["sports_market_type"] == "moneyline" and params["category"] == "Sports"
        assert params["search"] == "Chicago"

    def test_a_cursor_from_another_query_is_refused(self):
        gemini, _ = adapter()
        cursor = gemini.fetch_markets(limit=1).next_cursor
        with pytest.raises(BadRequest):
            gemini.fetch_markets(limit=1, cursor=cursor, sport="soccer")
        with pytest.raises(BadRequest):
            gemini.fetch_markets(cursor="not-a-cursor")

    def test_a_settled_listing_asks_for_settled_and_invalid(self):
        gemini, api = adapter(Api(events=[]))
        assert gemini.fetch_markets(status="settled") == []
        assert api.requests[0].url.params.get_list("status") == ["settled", "invalid"]

    def test_sort_is_refused(self):
        gemini, _ = adapter()
        with pytest.raises(NotSupported):
            gemini.fetch_markets(sort="volume")

    def test_events_page_by_offset(self):
        gemini, _ = adapter(Api(total=35))
        page = gemini.fetch_events(limit=2)
        assert [e.id for e in page] == [GAME, "gemini:NBA-2610232300-NYK-BOS-M"] and page.next_cursor

    def test_a_market_is_found_through_the_events_its_symbol_could_belong_to(self):
        gemini, api = adapter()
        market = gemini.fetch_market("gemini:GEMI-NBA-2610232000-CHI-MIL-S-MIL1")
        assert market.event_id == "gemini:NBA-2610232000-CHI-MIL-S" and market.info["sports_market"]["type"] == "spread"
        assert [r.url.path.rsplit("/", 1)[1] for r in api.requests] == ["NBA-2610232000-CHI-MIL-S"]

    def test_a_listing_remembers_each_symbols_event(self):
        gemini, api = adapter()
        gemini.fetch_markets(limit=2)
        found = gemini.fetch_markets_by_ids([MIL, "gemini:GEMI-NBA-2610232000-CHI-MIL-M-CHI", "gemini:GEMI-NOPE-X"])
        assert [m.id for m in found] == [MIL, "gemini:GEMI-NBA-2610232000-CHI-MIL-M-CHI"]
        event_reads = [r.url.path for r in api.requests if r.url.path.startswith("/v1/prediction-markets/events/")]
        assert event_reads == ["/v1/prediction-markets/events/NBA-2610232000-CHI-MIL-M",
                               "/v1/prediction-markets/events/NOPE"]

    def test_an_unknown_symbol_is_market_not_found(self):
        gemini, _ = adapter()
        with pytest.raises(MarketNotFound):
            gemini.fetch_market("gemini:GEMI-NOPE-XYZ-ABC")

    def test_fetch_event(self):
        gemini, _ = adapter()
        assert gemini.fetch_event("gemini:BTC05M2610231640").markets[0].id == "gemini:GEMI-BTC05M2610231640-UP"

    def test_the_book_asks_for_every_level(self):
        gemini, api = adapter()
        book = gemini.fetch_order_book(MIL)
        assert book.depth_scope == "full" and api.requests[0].url.path == "/v1/book/GEMI-NBA-2610232000-CHI-MIL-M-MIL"
        assert api.requests[0].url.params["limit_bids"] == "0"
        assert list(gemini.fetch_order_books([MIL, MIL])) == [MIL]

    def test_trades_come_oldest_first_and_page_forward_by_trade_id(self):
        gemini, api = adapter()
        recent = gemini.fetch_trades(MIL)
        assert [t.timestamp for t in recent] == sorted(t.timestamp for t in recent) and recent.next_cursor is None
        forward = gemini.fetch_trades(MIL, cursor="0", limit=6)
        assert api.requests[-1].url.params["since_tid"] == "0"
        assert forward.next_cursor == "1893456012680104"
        with pytest.raises(BadRequest):
            gemini.fetch_trades(MIL, cursor="x")

    def test_candles_use_the_venues_timeframe_names(self):
        gemini, api = adapter()
        candles = gemini.fetch_ohlcv(MIL, timeframe="1h", limit=2)
        assert api.requests[0].url.path.endswith("/1hr")
        assert [c.timestamp for c in candles] == [1792764000000, 1792767600000]
        with pytest.raises(BadRequest):
            gemini.fetch_ohlcv(MIL, timeframe="2h")

    def test_the_fee_schedule_reads_the_contract(self):
        gemini, _ = adapter()
        schedule = gemini.fetch_fee_schedule(MIL)
        assert schedule.scope == "market" and schedule.scope_id == "GEMI-NBA-2610232000-CHI-MIL-M-MIL"
        assert schedule.fee_type == "quadratic_theta" and schedule.taker_rate == 0.07

    def test_categories(self):
        gemini, api = adapter()
        assert "Sports" in gemini.fetch_categories(status="active")
        assert api.requests[0].url.params["status"] == "active"

    def test_another_venues_id_is_refused(self):
        gemini, _ = adapter()
        with pytest.raises(BadRequest):
            gemini.fetch_order_book("kalshi:KXFOO")

    def test_sandbox(self):
        assert Gemini(sandbox=True, limiter=None).http.base_url == "https://api.sandbox.gemini.com"

    def test_the_listing_does_not_change_the_sample(self):
        before = copy.deepcopy(EVENTS)
        gemini, _ = adapter()
        gemini.fetch_markets()
        assert EVENTS == before
