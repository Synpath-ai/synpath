"""predict.fun normalizers and adapter, against payloads recorded from the live venue."""
from __future__ import annotations

import pytest

from synpath import AuthenticationError, BadRequest, MarketNotFound, NotSupported, PredictFun
from synpath.predict_fun import (
    WEI, candles_from_series, fee_schedule_of, normalize_event, normalize_market,
    normalize_order_book, normalize_trade, quotes_of, unwrap,
)

from conftest import load

MARKETS = load("predict_fun_markets.json")["data"]
CATEGORIES = load("predict_fun_categories.json")["data"]


class FakeHttp:
    """Answers GETs by path from recorded payloads, and records them."""

    def __init__(self, answers):
        self.calls: list[tuple[str, object]] = []
        self.answers = answers

    def get(self, path, params=None):
        self.calls.append((path, params))
        for prefix, answer in self.answers.items():
            if path == prefix or path.startswith(prefix):
                return answer(params) if callable(answer) else answer
        raise AssertionError(f"unexpected request {path} {params}")

    def close(self):
        pass


def venue_with(answers, *, has_key=True):
    venue = PredictFun(api_key="test-key" if has_key else None, limiter=None)
    venue.http = FakeHttp(answers)
    venue.has_key = has_key
    return venue


class TestEnvelope:
    def test_data_is_unwrapped(self):
        assert unwrap({"success": True, "data": [1]}) == [1]

    def test_not_found(self):
        with pytest.raises(MarketNotFound, match="market not found"):
            unwrap(load("predict_fun_not_found.json"))

    def test_an_unexpected_payload(self):
        from synpath.errors import ExchangeError

        with pytest.raises(ExchangeError):
            unwrap(["no"])


class TestMarket:
    def test_ids_quotes_and_tokens(self):
        raw = MARKETS[0]
        market = normalize_market(raw)
        assert market.id == f"predict_fun:{raw['id']}" and market.event_id == f"predict_fun:{raw['categorySlug']}"
        yes = next(o for o in raw["outcomes"] if o["indexSet"] == 1)
        assert market.yes.venue_token_id == yes["onChainId"]
        assert market.book_model == "shared_complement" and market.stats.volume_unit == "collateral"
        assert market.tick_size == 10 ** -raw["decimalPrecision"]

    def test_no_is_the_yes_quote_mirrored(self):
        for raw in MARKETS:
            yes, no = quotes_of(raw)
            if yes.bid is not None and yes.ask is not None:
                assert no.bid == pytest.approx(1 - yes.ask) and no.ask == pytest.approx(1 - yes.bid)
                assert no.bid_size == yes.ask_size
                return
        pytest.skip("no two-sided market in the sample")

    def test_an_option_names_itself(self):
        options = [normalize_market(m) for m in MARKETS if m.get("question") and m.get("title") and m["title"] not in m["question"]]
        assert options and all(m.outcome_label for m in options)

    def test_links_to_polymarket_are_kept(self):
        linked = [normalize_market(m) for m in MARKETS if m.get("polymarketConditionIds")]
        assert linked and all(m.info["polymarket_condition_ids"] for m in linked)

    def test_statuses(self):
        assert {normalize_market(m).status for m in load("predict_fun_resolved.json")["data"]} == {"settled"}
        open_market = normalize_market(MARKETS[0])
        assert open_market.status == "open" and open_market.active


class TestEvent:
    def test_a_category_holds_its_markets(self):
        raw = CATEGORIES[0]
        event = normalize_event(raw)
        assert event.id == f"predict_fun:{raw['slug']}" and len(event.markets) == len(raw["markets"])
        assert all(m.event_id == event.id for m in event.markets)
        assert event.tags == [t["name"].lower() for t in raw["tags"]]

    def test_a_neg_risk_category_is_exclusive(self):
        neg = [c for c in CATEGORIES if c.get("isNegRisk")]
        if not neg:
            pytest.skip("no neg-risk category in the sample")
        assert normalize_event(neg[0]).mutually_exclusive is True


class TestBookAndTrades:
    def test_the_book_is_best_first_and_mirrors(self):
        payload = load("predict_fun_book.json")["data"]
        yes = normalize_order_book(payload, market_id="m")
        no = normalize_order_book(payload, market_id="m", side="no")
        assert yes.bids == sorted(yes.bids, key=lambda lv: -lv.price)
        assert no.derived and [round(1 - lv.price, 6) for lv in no.asks] == [lv.price for lv in yes.bids]

    def test_trades_read_from_the_taker(self):
        rows = load("predict_fun_matches.json")["data"]
        for row in rows:
            trade = normalize_trade(row, market_id="m")
            taker = row["taker"]
            price = int(row["priceExecuted"]) / WEI
            on_no = taker["outcome"]["indexSet"] == 2
            assert trade.price == pytest.approx(1 - price if on_no else price, abs=1e-6)
            bought = (taker["quoteType"] == "Bid") != on_no
            assert trade.side == ("buy" if bought else "sell")
            assert trade.amount == pytest.approx(int(row["amountFilled"]) / WEI)

    def test_the_recorded_taker_fee_is_the_schedule(self):
        """A taker pays `rate * min(p, 1 - p)` a share, as the venue charged."""
        schedule = fee_schedule_of({"feeRateBps": 200}, market_id="m")
        for row in load("predict_fun_matches.json")["data"]:
            fee = row["taker"].get("fee") or {}
            if fee.get("type") != "COLLATERAL" or not int(fee.get("amount") or 0):
                continue
            price = int(row["taker"]["price"]) / WEI
            shares = int(row["taker"]["amount"]) / WEI
            assert schedule.estimate(price, shares) == pytest.approx(int(fee["amount"]) / WEI, rel=1e-3)
            return
        pytest.skip("no taker fee in the sample")

    def test_candles_are_probability_samples(self):
        series = load("predict_fun_timeseries.json")["data"]["series"]
        candles = candles_from_series(series, interval_seconds=3600)
        assert candles and all(c.volume is None and c.price_source == "sampled_mid" for c in candles)
        assert all(0 <= c.close <= 1 for c in candles)


class TestAdapter:
    def test_markets_page_with_the_venue_cursor(self):
        venue = venue_with({"/markets": load("predict_fun_markets.json")})
        page = venue.fetch_markets(limit=20)
        assert len(page) == 20 and page.next_cursor == load("predict_fun_markets.json")["cursor"]
        path, params = venue.http.calls[0]
        assert params["sort"] == "VOLUME_24H_DESC" and params["status"] == "OPEN"

    def test_a_resolving_market_is_not_open(self):
        payload = load("predict_fun_markets.json")
        rows = [dict(payload["data"][0], status="PRICE_PROPOSED")] + payload["data"][1:3]
        venue = venue_with({"/markets": {**payload, "data": rows}})
        page = venue.fetch_markets(limit=3)
        assert [m.venue_market_id for m in page] == [str(r["id"]) for r in rows[1:]]
        assert page.next_cursor == payload["cursor"]

    def test_unsupported_states_and_sorts(self):
        venue = venue_with({})
        with pytest.raises(NotSupported):
            venue.fetch_markets(status="closed")
        with pytest.raises(NotSupported):
            venue.fetch_markets(sort="newest")
        assert venue.http.calls == []

    def test_search_uses_the_venue(self):
        venue = venue_with({"/search": load("predict_fun_search.json")})
        found = venue.fetch_markets(query="bitcoin", limit=5)
        assert found and found.next_cursor is None
        assert venue.http.calls[0][0] == "/search"

    def test_events(self):
        venue = venue_with({"/categories": load("predict_fun_categories.json")})
        events = venue.fetch_events(limit=5)
        assert len(events) == 5 and all(e.markets for e in events)

    def test_a_slug_is_not_a_market(self):
        with pytest.raises(MarketNotFound, match="slug"):
            venue_with({}).fetch_market("predict_fun:lol-navi-fly-2026-10-05")

    def test_books_are_batched(self):
        venue = venue_with({"/markets/orderbooks": load("predict_fun_books.json")})
        ids = [str(row["marketId"]) for row in load("predict_fun_books.json")["data"]]
        books = venue.fetch_order_books(ids)
        assert set(books) == {f"predict_fun:{i}" for i in ids} and len(venue.http.calls) == 1

    def test_a_missing_key_says_how_to_get_one(self):
        from synpath.errors import AuthenticationError as Auth

        def refuse(params):
            raise Auth("predict_fun: credentials refused (401)")

        venue = venue_with({"/markets": refuse}, has_key=False)
        with pytest.raises(AuthenticationError, match="developers.predict.fun"):
            venue.fetch_markets()

    def test_an_unknown_side_is_refused(self):
        with pytest.raises(BadRequest):
            normalize_order_book({"bids": [], "asks": []}, market_id="m", side="maybe")  # type: ignore[arg-type]
