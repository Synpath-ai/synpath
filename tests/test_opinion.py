"""Opinion normalizers and adapter, against payloads recorded from the live venue."""
from __future__ import annotations

import json

import pytest

from synpath import BadRequest, MarketNotFound, NotSupported, Opinion
from synpath.errors import ExchangeError
from synpath.opinion import (
    FEE_MANAGER, GET_FEE_RATE_SETTINGS, MAX_SEARCH_PAGES, MIN_FEE, PAGE,
    candles_from_price_history, fee_schedule_of, last_trade_of, markets_of, matches,
    normalize_event, normalize_market, normalize_order_book, unwrap,
)

from conftest import load


def envelope(result):
    return {"errmsg": "", "errno": 0, "result": result}


class FakeHttp:
    """Records what the adapter asked for and answers with what it was given."""

    def __init__(self, answers=None):
        self.calls: list[tuple[str, object]] = []
        self.answers = answers or {}

    def get(self, path, params=None):
        self.calls.append((path, params))
        answer = self.answers.get(path)
        if answer is None:
            raise AssertionError(f"unexpected request {path} {params}")
        return answer(params) if callable(answer) else answer

    def post(self, path, json=None):
        self.calls.append((path, json))
        answer = self.answers["post"]
        return answer(json) if callable(answer) else answer

    def close(self):
        pass


def venue_with(answers, rpc=None):
    venue = Opinion(limiter=None)
    venue.http = FakeHttp(answers)
    if rpc is not None:
        venue.rpc = FakeHttp({"post": rpc})
    return venue


class TestEnvelope:
    def test_the_result_is_unwrapped(self):
        assert unwrap(envelope({"list": []})) == {"list": []}

    def test_an_unknown_id_is_market_not_found(self):
        """The venue says so with HTTP 200 and errno 10200; the status code alone
        would read it as a success."""
        with pytest.raises(MarketNotFound, match="Topic ID does not exist"):
            unwrap(load("opinion_not_found.json"))

    def test_any_other_errno_is_a_bad_request(self):
        with pytest.raises(BadRequest, match="token_id is required"):
            unwrap({"errmsg": "token_id is required", "errno": 10000, "result": None})

    def test_a_payload_that_is_not_the_envelope_is_refused(self):
        with pytest.raises(ExchangeError):
            unwrap(["not", "an", "envelope"])


class TestMarket:
    def test_a_binary_topic_is_a_market_and_its_own_event(self, opinion_market):
        market = normalize_market(opinion_market)
        assert market.id == "opinion:8453"
        assert market.venue_market_id == "8453"
        assert market.event_id == "opinion:8453"
        assert market.outcome_label is None
        assert market.title == opinion_market["marketTitle"]

    def test_sides_carry_their_tokens_and_labels(self, opinion_market):
        market = normalize_market(opinion_market)
        assert market.yes.venue_token_id == opinion_market["yesTokenId"]
        assert market.no.venue_token_id == opinion_market["noTokenId"]
        assert [market.yes.label, market.no.label] == ["YES", "NO"]
        assert market.book_model == "native_per_outcome"

    def test_the_catalog_carries_no_prices_so_quotes_are_empty(self, opinion_market):
        """None, never zero or a guess: the listing has no bid, ask or last."""
        market = normalize_market(opinion_market)
        for side in (market.yes, market.no):
            assert side.quote.bid is None and side.quote.ask is None and side.quote.last is None

    def test_volume_is_collateral(self, opinion_market):
        market = normalize_market(opinion_market)
        assert market.stats.volume_total == pytest.approx(float(opinion_market["volume"]))
        assert market.stats.volume_unit == "collateral"
        assert market.stats.liquidity is None

    def test_times_are_milliseconds(self, opinion_market):
        market = normalize_market(opinion_market)
        assert market.close_timestamp == opinion_market["cutoffAt"] * 1000
        assert market.open_timestamp == opinion_market["createdAt"] * 1000
        assert market.close_datetime.endswith("Z")

    def test_status_and_labels(self, opinion_market):
        market = normalize_market(opinion_market)
        assert (market.status, market.native_status, market.active) == ("open", "activated", True)
        assert market.tags == [label.lower() for label in opinion_market["labels"]]
        assert market.url == f"https://opinion.trade/market/{opinion_market['slug']}"

    def test_tick_size_is_unknown_not_guessed(self, opinion_market):
        assert normalize_market(opinion_market).tick_size is None

    def test_a_child_takes_the_question_and_event_from_its_topic(self, opinion_child, opinion_categorical):
        market = normalize_market(opinion_child, opinion_categorical)
        assert market.id == "opinion:5342"
        assert market.event_id == "opinion:337"
        assert market.outcome_label == "Perplexity AI"
        assert market.title == f"{opinion_categorical['marketTitle']} — Perplexity AI"
        # The child's own cutoff is 0 (absent), so the topic's applies.
        assert opinion_child["cutoffAt"] == 0
        assert market.close_timestamp == opinion_categorical["cutoffAt"] * 1000
        assert market.tags == [label.lower() for label in opinion_categorical["labels"]]

    def test_a_child_alone_still_normalizes(self, opinion_child):
        market = normalize_market(opinion_child)
        assert market.title == "Perplexity AI"
        assert market.close_timestamp is None

    def test_a_categorical_topic_is_not_a_market(self, opinion_categorical):
        with pytest.raises(BadRequest, match="categorical"):
            normalize_market(opinion_categorical)

    def test_a_resolved_child_is_settled(self, opinion_resolved):
        topic = opinion_resolved[0]
        markets = markets_of(topic)
        assert {m.status for m in markets} == {"settled"}
        assert {m.native_status for m in markets} == {"resolved"}
        assert not any(m.active for m in markets)


class TestEvent:
    def test_a_categorical_topic_holds_its_options(self, opinion_categorical):
        event = normalize_event(opinion_categorical)
        assert event.id == "opinion:337"
        assert [m.id for m in event.markets] == [f"opinion:{c['marketId']}" for c in opinion_categorical["childMarkets"]]
        assert all(m.event_id == event.id for m in event.markets)
        assert "childMarkets" not in event.info

    def test_options_keep_their_own_status(self, opinion_categorical):
        """One option can resolve while the topic stays open."""
        event = normalize_event(opinion_categorical)
        assert event.status == "open"
        assert {m.id: m.status for m in event.markets}["opinion:5346"] == "settled"

    def test_exclusivity_is_unknown(self, opinion_categorical):
        assert normalize_event(opinion_categorical).mutually_exclusive is None

    def test_a_topic_whose_options_trade_is_open_whatever_its_own_field_says(self, opinion_categorical):
        assert opinion_categorical["status"] == 1
        assert normalize_event(opinion_categorical).status == "open"

    def test_a_binary_topic_is_an_event_of_one(self, opinion_market):
        event = normalize_event(opinion_market)
        assert [m.id for m in event.markets] == [event.id] == ["opinion:8453"]


class TestOrderBook:
    def test_best_first_on_both_sides(self):
        book = normalize_order_book(load("opinion_book.json")["result"], market_id="opinion:6143")
        assert book.best_bid.price == 0.66 and book.best_ask.price == 0.709
        assert [level.price for level in book.bids] == sorted((l.price for l in book.bids), reverse=True)
        assert [level.price for level in book.asks] == sorted(l.price for l in book.asks)
        assert book.derived is False and book.depth_scope == "full"

    def test_the_no_book_is_the_yes_book_mirrored_by_the_venue(self):
        """Read, not derived -- but the venue keeps the two in step."""
        yes = normalize_order_book(load("opinion_book.json")["result"], market_id="opinion:6143")
        no = normalize_order_book(load("opinion_book_no.json")["result"], market_id="opinion:6143", side="no")
        assert no.best_bid.price == pytest.approx(1 - yes.best_ask.price)
        assert no.best_ask.price == pytest.approx(1 - yes.best_bid.price)
        assert no.best_bid.size == pytest.approx(yes.best_ask.size)

    def test_depth_cuts_the_book(self):
        book = normalize_order_book(load("opinion_book.json")["result"], market_id="opinion:6143", depth=2)
        assert len(book.bids) <= 2 and len(book.asks) <= 2
        assert book.depth_scope == "top_n"


class TestPrices:
    def test_last_trade(self):
        price, stamp = last_trade_of(load("opinion_latest_price.json")["result"])
        assert price == 0.719 and stamp == 1790506505000

    def test_never_traded_is_none_not_zero(self):
        assert last_trade_of({"price": "0.0", "side": "", "size": "0", "timestamp": 0}) == (None, None)

    def test_samples_become_last_trade_bars_oldest_first(self):
        history = load("opinion_prices_1h.json")["result"]["history"]
        assert history[0]["t"] > history[-1]["t"]           # the venue sends newest first
        candles = candles_from_price_history(history, interval_seconds=3600)
        assert [c.timestamp for c in candles] == sorted(c.timestamp for c in candles)
        assert {c.price_source for c in candles} == {"sampled_last"}
        assert all(c.volume is None and c.trade_count is None for c in candles)

    def test_wider_bars_take_open_and_close_in_time_order(self):
        history = [{"t": 7200, "p": "0.3"}, {"t": 3600, "p": "0.2"}, {"t": 0, "p": "0.1"}]
        [bar] = candles_from_price_history(history, interval_seconds=14400)
        assert (bar.open, bar.high, bar.low, bar.close) == (0.1, 0.3, 0.1, 0.3)
        assert bar.info["samples"] == 3


class TestFees:
    @pytest.fixture
    def schedule(self):
        call = load("opinion_fee_rate_call.json")
        return fee_schedule_of(call["result"], market_id="6143", token_id="1")

    def test_rates_come_from_the_contract(self, schedule):
        assert schedule.fee_type == "opinion_curve"
        assert schedule.taker_rate == 0.04
        assert schedule.maker_rate == 0.0
        assert schedule.min_fee == MIN_FEE
        assert schedule.info["taker_fee_rate_bps"] == 400

    def test_one_percent_of_notional_at_fifty_cents(self, schedule):
        """The venue's fee docs: 0% to 1%, highest at 50c."""
        assert schedule.estimate(0.5, 1000) == pytest.approx(0.01 * 0.5 * 1000)

    def test_small_orders_pay_the_floor(self, schedule):
        assert schedule.estimate(0.5, 10) == MIN_FEE

    def test_makers_pay_nothing(self, schedule):
        assert schedule.estimate(0.5, 1000, taker=False) == 0.0

    def test_a_disabled_fee_is_free_without_a_floor(self):
        words = [0, 400, 0, 0]
        schedule = fee_schedule_of("0x" + "".join(f"{w:064x}" for w in words), market_id="1", token_id="1")
        assert schedule.taker_rate == 0.0 and schedule.min_fee is None
        assert schedule.estimate(0.5, 1000) == 0.0

    def test_a_short_answer_is_refused(self):
        with pytest.raises(ExchangeError):
            fee_schedule_of("0x", market_id="1", token_id="1")


class TestSearch:
    def test_every_word_must_appear(self, opinion_categorical):
        assert matches(opinion_categorical, "acquired perplexity")
        assert not matches(opinion_categorical, "acquired bitcoin")

    def test_options_and_labels_count(self, opinion_categorical):
        assert matches(opinion_categorical, "gitlab")
        assert matches(opinion_categorical, opinion_categorical["labels"][0])


class TestAdapter:
    def test_categorical_topics_flatten_into_their_options(self, opinion_topics):
        venue = venue_with({"/market": envelope({"list": opinion_topics, "total": len(opinion_topics)})})
        markets = venue.fetch_markets(limit=100)
        expected = [m.id for topic in opinion_topics for m in markets_of(topic) if m.status == "open"]
        assert len(expected) > 100
        assert [m.id for m in markets] == expected[:100]
        assert markets.next_cursor == "p1.100"
        path, params = venue.http.calls[0]
        assert params["status"] == "activated" and params["sortBy"] == 5 and params["limit"] == PAGE

    def test_a_cursor_resumes_inside_a_page(self, opinion_topics):
        """A page of 20 topics flattens into many more markets, so the cursor
        says where on the page to pick up, and nothing repeats or drops."""
        venue = venue_with({"/market": envelope({"list": opinion_topics, "total": len(opinion_topics)})})
        everything = [m.id for m in venue.fetch_markets(limit=100)]
        first = venue.fetch_markets(limit=7)
        second = venue.fetch_markets(limit=7, cursor=first.next_cursor)
        assert first.next_cursor == "p1.7"
        assert [m.id for m in first] + [m.id for m in second] == everything[:14]

    def test_pages_are_walked_until_the_venue_runs_out(self, opinion_topics):
        small = next(t for t in opinion_topics if len(t.get("childMarkets") or []) == 2)
        pages = {1: [small] * PAGE, 2: opinion_topics[17:18]}

        def answer(params):
            return envelope({"list": pages.get(params["page"], []), "total": PAGE + 1})

        venue = venue_with({"/market": answer})
        markets = venue.fetch_markets(limit=100)
        assert [params["page"] for _, params in venue.http.calls] == [1, 2]
        assert markets[-1].id == "opinion:8453"
        assert markets.next_cursor is None

    def test_closed_is_refused_before_asking(self):
        venue = venue_with({})
        with pytest.raises(NotSupported, match="closed"):
            venue.fetch_markets(status="closed")
        assert venue.http.calls == []

    def test_settled_and_all_reach_the_venue(self, opinion_resolved):
        venue = venue_with({"/market": envelope({"list": opinion_resolved, "total": 5})})
        venue.fetch_markets(status="settled")
        venue.fetch_markets(status="all")
        assert [params["status"] for _, params in venue.http.calls] == ["resolved", None]

    def test_liquidity_sort_is_refused(self):
        venue = venue_with({})
        with pytest.raises(NotSupported, match="liquidity"):
            venue.fetch_markets(sort="liquidity")

    def test_a_query_is_matched_here_over_bounded_pages(self, opinion_topics):
        def answer(params):
            return envelope({"list": opinion_topics, "total": 10_000})

        venue = venue_with({"/market": answer})
        markets = venue.fetch_markets(query="no such words anywhere")
        assert list(markets) == []
        assert len(venue.http.calls) == MAX_SEARCH_PAGES
        assert markets.next_cursor == f"p{MAX_SEARCH_PAGES + 1}"

    def test_a_bad_cursor_is_refused(self):
        with pytest.raises(BadRequest, match="cursor"):
            venue_with({}).fetch_markets(cursor="search:1")

    def test_events_are_topics(self, opinion_topics):
        venue = venue_with({"/market": envelope({"list": opinion_topics, "total": len(opinion_topics)})})
        events = venue.fetch_events(limit=3)
        assert [e.id for e in events] == [f"opinion:{t['marketId']}" for t in opinion_topics[:3]]
        assert events.next_cursor == "p1.3"

    def test_fetch_market_finds_a_childs_topic_by_slug(self, opinion_child, opinion_categorical):
        venue = venue_with({
            "/market/5342": envelope({"data": opinion_child}),
            f"/market/slug/{opinion_child['slug']}": envelope({"data": opinion_categorical}),
        })
        market = venue.fetch_market("opinion:5342")
        assert market.event_id == "opinion:337"
        assert market.outcome_label == "Perplexity AI"

    def test_fetch_market_of_a_binary_topic_is_one_request(self, opinion_market):
        venue = venue_with({"/market/8453": envelope({"data": opinion_market})})
        assert venue.fetch_market("8453").id == "opinion:8453"
        assert len(venue.http.calls) == 1

    def test_an_unknown_market_is_not_found(self):
        venue = venue_with({"/market/1": load("opinion_not_found.json")})
        with pytest.raises(MarketNotFound):
            venue.fetch_market("opinion:1")

    def test_a_market_of_another_venue_is_refused(self):
        with pytest.raises(BadRequest, match="belongs to kalshi"):
            venue_with({}).fetch_market("kalshi:KXFOO")

    def test_books_are_read_by_token(self, opinion_market):
        venue = venue_with({
            "/market/8453": envelope({"data": opinion_market}),
            "/token/orderbook": load("opinion_book.json"),
        })
        book = venue.fetch_order_book("opinion:8453", side="no")
        assert book.market_id == "opinion:8453" and book.side == "no"
        assert venue.http.calls[-1] == ("/token/orderbook", {"token_id": opinion_market["noTokenId"]})
        venue.fetch_order_book("opinion:8453")
        assert len([c for c in venue.http.calls if c[0] == "/market/8453"]) == 1   # tokens cached

    def test_refresh_quotes_reads_both_books_and_the_last_trade(self, opinion_market):
        books = {
            opinion_market["yesTokenId"]: load("opinion_book.json"),
            opinion_market["noTokenId"]: load("opinion_book_no.json"),
        }
        venue = venue_with({
            "/token/orderbook": lambda params: books[params["token_id"]],
            "/token/latest-price": load("opinion_latest_price.json"),
        })
        market = venue.refresh_quotes(normalize_market(opinion_market))
        assert (market.yes.quote.bid, market.yes.quote.ask) == (0.66, 0.709)
        assert (market.no.quote.bid, market.no.quote.ask) == (0.291, 0.34)
        assert market.yes.quote.last == 0.719
        assert market.no.quote.last == pytest.approx(0.281)
        assert market.yes.quote.last_timestamp == 1790506505000

    def test_trades_are_not_available(self):
        with pytest.raises(NotSupported, match="trade tape"):
            venue_with({}).fetch_trades("opinion:8453")

    def test_ohlcv_walks_back_with_end_at(self, opinion_market):
        hour = 3600
        now = 1_000 * hour

        def answer(params):
            end = params["end_at"]
            return envelope({"history": [{"t": t, "p": "0.5"} for t in range(end - end % hour, end - 3 * hour, -hour)]})

        venue = venue_with({"/market/8453": envelope({"data": opinion_market}), "/token/price-history": answer})
        candles = venue.fetch_ohlcv("8453", timeframe="1h", since=(now - 6 * hour) * 1000, until=now * 1000)
        assert [c.timestamp // 1000 for c in candles] == [now - k * hour for k in range(6, -1, -1)]
        ends = [params["end_at"] for path, params in venue.http.calls if path == "/token/price-history"]
        assert ends == [now, now - 2 * hour - 1, now - 5 * hour - 1]
        assert all("start_at" not in params for path, params in venue.http.calls if path == "/token/price-history")

    def test_ohlcv_finer_than_an_hour_is_refused(self):
        with pytest.raises(BadRequest, match="hourly"):
            venue_with({}).fetch_ohlcv("opinion:8453", timeframe="15m")

    def test_daily_bars_ask_for_daily_samples(self, opinion_market):
        venue = venue_with({
            "/market/8453": envelope({"data": opinion_market}),
            "/token/price-history": load("opinion_prices_1d.json"),
        })
        venue.fetch_ohlcv("8453", timeframe="1d", limit=3)
        assert venue.http.calls[-1][1]["interval"] == "1d"

    def test_fee_schedule_is_an_eth_call_to_the_fee_manager(self, opinion_market):
        venue = venue_with({"/market/8453": envelope({"data": opinion_market})},
                           rpc=load("opinion_fee_rate_call.json"))
        schedule = venue.fetch_fee_schedule("opinion:8453")
        _, body = venue.rpc.calls[0]
        call = body["params"][0]
        assert body["method"] == "eth_call" and call["to"] == FEE_MANAGER
        assert call["data"] == GET_FEE_RATE_SETTINGS + int(opinion_market["yesTokenId"]).to_bytes(32, "big").hex()
        assert schedule.scope_id == "8453" and schedule.taker_rate == 0.04

    def test_a_failed_fee_lookup_raises(self, opinion_market):
        venue = venue_with({"/market/8453": envelope({"data": opinion_market})},
                           rpc={"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "boom"}})
        with pytest.raises(ExchangeError, match="fee lookup failed"):
            venue.fetch_fee_schedule("opinion:8453")

    def test_an_api_key_is_sent_as_the_apikey_header(self):
        venue = Opinion(api_key="k", limiter=None)
        assert venue.http._client.headers["apikey"] == "k"
        assert "apikey" not in Opinion(limiter=None).http._client.headers
