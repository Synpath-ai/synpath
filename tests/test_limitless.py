"""Limitless normalizers and adapter, against payloads recorded from the live venue."""
from __future__ import annotations

import pytest

from synpath import BadRequest, MarketNotFound, NotSupported
from synpath.limitless import (
    Limitless, candles_from_prices, fee_schedule_of, lookback_for, markets_of, normalize_event,
    normalize_market, normalize_order_book, normalize_trade, status_of,
)
from synpath.types import limitless_buy_rate

from conftest import load

ACTIVE = load("limitless_active.json")
GROUP = load("limitless_group.json")
MARKET = load("limitless_market.json")


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
        raise MarketNotFound(f"limitless: no {path}")

    def close(self):
        pass


def venue_with(answers):
    venue = Limitless(limiter=None)
    venue.http = FakeHttp(answers)
    return venue


class TestMarket:
    def test_a_market_is_named_by_its_slug(self):
        market = normalize_market(MARKET)
        assert market.id == f"limitless:{MARKET['slug']}" and market.event_id == market.id
        assert market.yes.venue_token_id == MARKET["tokens"]["yes"] and market.no.venue_token_id == MARKET["tokens"]["no"]
        assert market.book_model == "shared_complement" and market.tick_size == 0.001
        assert market.stats.volume_total == int(MARKET["volume"]) / 1_000_000

    def test_a_group_option_carries_its_question(self):
        options = markets_of(GROUP)
        assert options and all(m.event_id == f"limitless:{GROUP['slug']}" for m in options)
        first = options[0]
        assert first.outcome_label == GROUP["markets"][0]["title"].strip()
        assert first.title.startswith(GROUP["title"])

    def test_the_venue_names_the_polymarket_market_it_copies(self):
        linked = [normalize_market(m) for m in ACTIVE["data"] if (m.get("metadata") or {}).get("externalProvider") == "polymarket"]
        assert linked and all(m.info["polymarket_slug"] for m in linked)

    @pytest.mark.parametrize("raw, expected", [
        ({"status": "FUNDED", "expired": False}, "open"),
        ({"status": "FUNDED", "expired": True}, "closed"),
        ({"status": "LOCKED"}, "closed"),
        ({"status": "RESOLVED"}, "settled"),
        ({"status": "FUNDED", "winningOutcomeIndex": 0}, "settled"),
        ({"status": "CREATED"}, "closed"),
    ])
    def test_statuses(self, raw, expected):
        assert status_of(raw)[0] == expected

    def test_a_group_is_an_event_of_its_options(self):
        event = normalize_event(GROUP)
        assert event.id == f"limitless:{GROUP['slug']}" and len(event.markets) == len(GROUP["markets"])
        single = normalize_event(MARKET)
        assert len(single.markets) == 1 and single.markets[0].event_id == single.id


class TestBookTradesCandles:
    def test_the_book_is_in_shares_and_mirrors(self):
        payload = load("limitless_book.json")
        yes = normalize_order_book(payload, market_id="m")
        no = normalize_order_book(payload, market_id="m", side="no")
        assert yes.bids == sorted(yes.bids, key=lambda lv: -lv.price)
        assert yes.bids[0].size == payload["bids"][0]["size"] / 1_000_000
        assert no.derived and [round(1 - lv.price, 6) for lv in no.asks] == [lv.price for lv in yes.bids]
        with pytest.raises(BadRequest):
            normalize_order_book(payload, market_id="m", side="maybe")  # type: ignore[arg-type]

    def test_trades_read_from_the_taker_on_the_yes_leg(self):
        rows = load("limitless_trades.json")["events"]
        no_token = MARKET["tokens"]["no"]
        for row in rows:
            trade = normalize_trade(row, market_id="m", no_token=no_token)
            on_no = row["tokenId"] == no_token
            assert trade.price == pytest.approx(1 - row["price"] if on_no else row["price"], abs=1e-6)
            assert trade.side == ("buy" if (row["side"] == 0) != on_no else "sell")
            assert trade.amount == int(row["matchedSize"]) / 1_000_000

    def test_candles_are_price_samples(self):
        points = load("limitless_history.json")["prices"]
        candles = candles_from_prices(points, interval_seconds=3600)
        assert candles and all(c.volume is None and c.price_source == "sampled_mid" for c in candles)
        assert candles == sorted(candles, key=lambda c: c.timestamp)

    @pytest.mark.parametrize("seconds, preset", [(60, "5m"), (3600, "1h"), (86400, "1d"), (3 * 86400, "1w"), (90 * 86400, "all")])
    def test_the_lookback_covers_the_request(self, seconds, preset):
        assert lookback_for(seconds) == preset


class TestFees:
    def test_the_buy_curve(self):
        assert limitless_buy_rate(0.3) == 0.03 and limitless_buy_rate(0.7) == pytest.approx(0.0151)
        assert limitless_buy_rate(0.725) == pytest.approx((0.0151 + 0.0126) / 2)
        schedule = fee_schedule_of(MARKET, market_id="m")
        assert schedule.estimate(0.4, 100) == pytest.approx(0.03 * 0.4 * 100)
        assert schedule.estimate(0.4, 100, taker=False) == 0

    def test_a_fee_free_market(self):
        free = fee_schedule_of({**MARKET, "metadata": {"fee": False}}, market_id="m")
        assert free.estimate(0.4, 100) == 0


class TestAdapter:
    def test_markets_page_across_the_venue_pages_without_repeats(self):
        venue = venue_with({"/markets/active": ACTIVE})
        first = venue.fetch_markets(limit=5)
        assert len(first) == 5 and first.next_cursor == "1:5"
        second = venue.fetch_markets(limit=5, cursor=first.next_cursor)
        assert not {m.id for m in first} & {m.id for m in second}

    def test_a_group_wider_than_the_page_is_split_by_the_cursor(self):
        venue = venue_with({"/markets/active": {"data": [GROUP], "totalMarketsCount": 1}})
        first = venue.fetch_markets(limit=2)
        rest = venue.fetch_markets(limit=100, cursor=first.next_cursor)
        assert len(first) + len(rest) == len(GROUP["markets"]) and rest.next_cursor is None

    def test_a_resolved_option_of_a_listed_group_is_not_open(self):
        settled = {**GROUP["markets"][0], "status": "RESOLVED", "winningOutcomeIndex": 1}
        group = {**GROUP, "markets": [settled, *GROUP["markets"][1:]]}
        venue = venue_with({"/markets/active": {"data": [group], "totalMarketsCount": 1}})
        page = venue.fetch_markets(limit=100)
        assert settled["slug"] not in {m.venue_market_id for m in page} and len(page) == len(GROUP["markets"]) - 1

    def test_unsupported_states_and_sorts(self):
        venue = venue_with({})
        with pytest.raises(NotSupported):
            venue.fetch_markets(status="settled")
        with pytest.raises(NotSupported):
            venue.fetch_markets(sort="volume")
        assert venue.http.calls == []
        venue = venue_with({"/markets/active": ACTIVE})
        venue.fetch_markets(sort="newest", limit=1)
        assert venue.http.calls[0][1]["sortBy"] == "newest"

    def test_an_option_is_looked_up_with_its_group(self):
        child = load("limitless_child.json")
        venue = venue_with({f"/markets/{child['slug']}": child, f"/markets/{child['groupSlug']}": GROUP})
        market = venue.fetch_market(child["slug"])
        assert market.event_id == f"limitless:{child['groupSlug']}" and market.title.startswith(GROUP["title"])

    def test_a_group_slug_is_not_a_market(self):
        venue = venue_with({f"/markets/{GROUP['slug']}": GROUP})
        with pytest.raises(MarketNotFound, match="group"):
            venue.fetch_market(GROUP["slug"])

    def test_search_uses_the_venue(self):
        venue = venue_with({"/markets/search": load("limitless_search.json")})
        found = venue.fetch_markets(query="bitcoin", limit=5)
        assert found and found.next_cursor is None and venue.http.calls[0][0] == "/markets/search"

    def test_refresh_reads_the_book(self):
        venue = venue_with({f"/markets/{MARKET['slug']}/orderbook": load("limitless_book.json")})
        market = venue.refresh_quotes(normalize_market(MARKET))
        book = load("limitless_book.json")
        assert market.yes.quote.bid == max(level["price"] for level in book["bids"])
        assert market.no.quote.ask == pytest.approx(1 - market.yes.quote.bid)

    def test_a_bad_cursor(self):
        with pytest.raises(BadRequest):
            venue_with({}).fetch_markets(cursor="nonsense")
