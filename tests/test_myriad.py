"""Myriad normalizers and adapter, against payloads recorded from the live venue."""
from __future__ import annotations

import httpx
import pytest

from synpath import BadRequest, MarketNotFound
from synpath.myriad import (
    Myriad, amm_book_levels, amm_order_book, candles_from_chart, chart_for, fee_schedule_of, markets_of,
    normalize_amm_trade, normalize_event, normalize_market, normalize_order_book, normalize_trades, parse_id,
    status_of, units,
)
from synpath.trading.myriad_signing import request_signature

from conftest import load

MARKETS = load("myriad_markets.json")["data"]
STANDALONE, CHILD, SPORTS = MARKETS
AMM_MARKETS = load("myriad_markets_amm.json")["data"]
MARKET = load("myriad_market.json")
AMM = load("myriad_market_amm.json")
GROUPED = load("myriad_grouped.json")
EVENT = load("myriad_event.json")
TRADES = load("myriad_trades.json")


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
        raise MarketNotFound(f"myriad: no {path}")

    def close(self):
        pass


def venue_with(answers):
    venue = Myriad(limiter=None)
    venue.http = FakeHttp(answers)
    return venue


class TestIds:
    def test_a_market_is_its_network_kind_and_number(self):
        assert parse_id("56-2385").market == 2385 and not parse_id("56-2385").amm
        amm = parse_id("56-amm-2385")
        assert amm.amm and amm.network == 56 and amm.native == "56-amm-2385"
        outcome = parse_id("2741-amm-412-3")
        assert outcome.outcome == 3 and outcome.market_native == "2741-amm-412"
        assert parse_id("patriots-vs-bears-who-wins") is None, "a slug is not an id"

    def test_an_order_book_market_has_no_outcome_suffix(self):
        with pytest.raises(BadRequest, match="binary"):
            parse_id("56-2385-1")

    def test_the_same_number_names_two_markets_on_bnb_chain(self):
        """Order-book market 2385 and AMM market 2385 are different markets."""
        ob = normalize_market(STANDALONE)
        assert ob.id == "myriad:56-2385" and ob.info["trading_model"] == "ob"
        amm = normalize_market({**AMM, "id": 2385})
        assert amm.id == "myriad:56-amm-2385"


class TestMarket:
    def test_an_order_book_market_is_yes_first(self):
        market = normalize_market(STANDALONE)
        yes, no = STANDALONE["outcomes"]
        assert market.yes.label == yes["title"] and market.no.label == no["title"]
        assert market.yes.quote.bid == yes["bestBid"] and market.yes.quote.ask == yes["bestAsk"]
        assert market.no.quote.bid == no["bestBid"] and market.no.quote.ask == no["bestAsk"]
        assert market.yes.venue_token_id == yes["tokenId"] and market.tick_size == 0.01
        assert market.book_model == "shared_complement" and market.event_id == market.id
        assert market.stats.volume_24h == STANDALONE["volume24h"] and market.stats.volume_unit == "collateral"
        assert market.url == f"https://myriad.markets/markets/{STANDALONE['slug']}"
        assert {"crypto", "politics"} <= set(market.tags)

    def test_an_event_child_carries_its_short_name(self):
        market = normalize_market(CHILD)
        assert market.event_id == f"myriad:{CHILD['eventId']}"
        assert market.outcome_label == CHILD["shortName"] and market.title == CHILD["title"]
        assert market.settlement_sources[0]["name"] == CHILD["oracle"]["name"]

    def test_a_sports_market_names_its_teams_and_scoreboard(self):
        market = normalize_market(SPORTS)
        assert (market.yes.label, market.no.label) == ("Patriots", "Bears")
        assert market.category == "sports" and market.info["scoreboard"]["type"] == "american-football"
        assert market.yes.quote.mid is None, "no book, no mid"

    def test_the_polymarket_market_it_names_is_kept(self):
        linked = [normalize_market(m) for m in MARKETS + [MARKET] if m.get("externalSources")]
        assert linked and all(m.info["polymarket_ids"] for m in linked)

    def test_an_amm_market_quotes_its_pool_price(self):
        market = normalize_market(AMM)
        price = AMM["outcomes"][0]["price"]
        assert market.id == "myriad:56-amm-120473" and market.tick_size is None
        assert market.yes.quote.mid == pytest.approx(price, abs=1e-6)
        assert market.yes.quote.bid == market.yes.quote.ask == market.yes.quote.mid, "a fee-free pool"

    def test_an_amm_fee_widens_the_quote(self):
        fees = {"buy": {"fee": 0.01, "treasury_fee": 0, "distributor_fee": 0.01},
                "sell": {"fee": 0.01, "treasury_fee": 0, "distributor_fee": 0}}
        quote = normalize_market({**AMM, "fees": fees}).yes.quote
        assert quote.bid < quote.mid < quote.ask
        assert quote.ask == pytest.approx(quote.mid / 0.98, abs=1e-6) and quote.bid == pytest.approx(quote.mid * 0.99, abs=1e-6)

    def test_a_many_outcome_amm_market_is_an_event_of_binary_markets(self):
        many = AMM_MARKETS[1]
        markets = markets_of(many)
        assert [m.id for m in markets] == [f"myriad:2741-amm-412-{i}" for i in range(4)]
        assert all(m.event_id == "myriad:2741-amm-412" for m in markets)
        first = markets[1]
        assert first.yes.label == "Prediction Markets" and first.no.label == "Not Prediction Markets"
        assert first.yes.quote.mid + first.no.quote.mid == pytest.approx(1.0)
        event = normalize_event(many)
        assert event.id == "myriad:2741-amm-412" and event.mutually_exclusive is True and len(event.markets) == 4

    @pytest.mark.parametrize("state, expected", [("open", "open"), ("closed", "closed"), ("resolved", "settled")])
    def test_statuses(self, state, expected):
        assert status_of({"state": state})[0] == expected

    def test_a_resolved_market_names_its_winner(self):
        market = normalize_market({**STANDALONE, "state": "resolved", "resolvedOutcomeId": 1})
        assert market.status == "settled" and market.info["winning_outcome"] == "no" and not market.active

    def test_amounts_come_in_wei_or_in_tokens(self):
        assert units("1000000000000000000") == 1.0 and units("98.00000000") == 98.0 and units(None) is None


class TestEvents:
    def test_a_venue_event_holds_its_markets_in_order(self):
        event = normalize_event({**EVENT, "type": "event"})
        assert event.id == f"myriad:{EVENT['id']}" and [m.outcome_label for m in event.markets] == \
            [m["shortName"] for m in EVENT["markets"]]
        assert event.mutually_exclusive is (True if EVENT["negRisk"] else None)

    def test_the_grouped_listing_mixes_events_and_markets(self):
        events = [normalize_event(row) for row in GROUPED["data"]]
        kinds = {row["type"] for row in GROUPED["data"]}
        assert kinds == {"event", "market"} and all(e.markets for e in events)
        for row, event in zip(GROUPED["data"], events):
            if row["type"] == "market":
                assert len(event.markets) == 1 and event.markets[0].event_id == event.id


class TestBooks:
    def test_each_outcome_book_is_read_as_served(self):
        yes = normalize_order_book(load("myriad_book_yes.json"), market_id="myriad:56-2385")
        no = normalize_order_book(load("myriad_book_no.json"), market_id="myriad:56-2385", side="no")
        assert yes.best_bid.price == 0.03 and yes.best_ask.price == 0.06 and not no.derived
        assert yes.best_ask.price + no.best_bid.price == pytest.approx(1.0)
        assert yes.best_ask.size == pytest.approx(no.best_bid.size), "the venue's books are each other's mirror"
        assert yes.bids == sorted(yes.bids, key=lambda level: -level.price)

    def test_the_pool_book_matches_the_venues_own_quotes(self):
        """The recorded quotes were taken with the pool in this state: buying
        25 USD1 of the first outcome, and selling 20 shares of it."""
        bids, asks = amm_book_levels([o["shares"] for o in AMM["outcomes"]], 0)
        buy, sell = load("myriad_quote_buy.json"), load("myriad_quote_sell.json")
        spent = shares = 0.0
        for level in asks:
            cost = level.price * level.size
            if spent + cost >= buy["value"]:
                shares += (buy["value"] - spent) / level.price
                break
            spent, shares = spent + cost, shares + level.size
        assert shares == pytest.approx(buy["shares"], rel=2e-3)
        received = sold = 0.0
        for level in bids:
            if sold + level.size >= sell["shares"]:
                received += (sell["shares"] - sold) * level.price
                break
            received, sold = received + level.price * level.size, sold + level.size
        assert received == pytest.approx(sell["value"], rel=2e-3)

    def test_the_pool_book_moves_a_cent_a_level(self):
        book = amm_order_book(AMM, market_id="myriad:56-amm-120473", index=0)
        mid = AMM["outcomes"][0]["price"]
        assert book.info["depth_source"] == "amm" and book.best_bid.price < mid < book.best_ask.price
        assert len(book.asks) == 99 - int(mid * 100) and len(book.bids) == int(mid * 100)
        no = amm_order_book(AMM, market_id="myriad:56-amm-120473", index=0, side="no")
        assert no.best_ask.price == pytest.approx(1 - book.best_bid.price, abs=0.011) and not no.derived

    def test_one_outcome_of_many_mirrors_its_yes_book_for_no(self):
        many = AMM_MARKETS[2] | {"outcomes": AMM_MARKETS[1]["outcomes"]}
        book = amm_order_book(many, market_id="myriad:2741-amm-317-1", index=1, side="no", native_no=False)
        assert book.derived and book.bids and book.asks

    def test_the_adapter_reads_outcome_one_for_no(self):
        venue = venue_with({"/markets/2385/orderbook": lambda p: load(f"myriad_book_{'yes' if p['outcome'] == 0 else 'no'}.json")})
        books = venue.fetch_order_books(["myriad:56-2385", "56-2385"], side="no")
        assert list(books) == ["myriad:56-2385"] and books["myriad:56-2385"].side == "no"
        assert venue.http.calls == [("/markets/2385/orderbook", {"network_id": 56, "outcome": 1})]


class TestTrades:
    def test_one_trade_per_taker_leg(self):
        trades = normalize_trades(TRADES, market_id="myriad:56-2385")
        legs = [row for row in TRADES if row["side"] not in ("split", "merge")]
        takers = [row for row in legs if float(row["fees"]["total"]) > 0]
        assert len(trades) == len(takers) and all(t.side in ("buy", "sell") for t in trades)
        assert trades == sorted(trades, key=lambda t: t.timestamp)

    def test_the_yes_price_and_the_takers_side(self):
        trades = {t.info["txHash"][:10] + ":" + str(t.info["outcome"]) + t.info["side"]: t
                  for t in normalize_trades(TRADES, market_id="myriad:56-2385")}
        sold_yes = trades["0xc3906b47:0sell"]
        assert sold_yes.side == "sell" and sold_yes.price == 0.03
        bought_no = next(t for k, t in trades.items() if k.endswith(":1sell"))
        assert bought_no.side == "buy", "selling NO is buying YES"

    def test_a_match_without_a_fee_is_reported_once_side_unknown(self):
        rows = [{**row, "fees": {"total": "0"}} for row in TRADES if row["txHash"].startswith("0xc3906b47")]
        trades = normalize_trades(rows, market_id="myriad:56-2385")
        assert len(trades) == 1 and trades[0].side == "unknown"

    def test_pool_trades(self):
        rows = load("myriad_amm_events.json")["data"]
        trades = [t for t in (normalize_amm_trade(r, market_id="myriad:56-amm-120473", index=0, many=False) for r in rows) if t]
        assert trades and all(0 < t.price < 1 for t in trades)
        first = next(t for t in trades if t.info["outcomeId"] == 0 and t.info["action"] == "buy")
        assert first.side == "buy" and first.price == pytest.approx(first.info["value"] / first.info["shares"], abs=1e-6)

    def test_the_adapter_pages_the_order_book_tape(self):
        venue = venue_with({"/markets/2385/trades": TRADES})
        page = venue.fetch_trades("myriad:56-2385", limit=20)
        assert page.next_cursor == "2" and venue.http.calls[0][1]["page"] == 1


class TestCandlesAndFees:
    def test_the_chart_covers_the_span(self):
        assert chart_for(3600) == "24h" and chart_for(3 * 86400) == "7d" and chart_for(90 * 86400) == "all"

    def test_samples_become_bars(self):
        points = MARKET["outcomes"][0]["price_charts"][0]["prices"]
        candles = candles_from_chart(points, interval_seconds=3600)
        assert candles and all(c.volume is None and c.price_source == "sampled_mid" for c in candles)
        assert sum(c.info["samples"] for c in candles) == len(points)

    def test_the_order_book_fee_follows_the_venues_table(self):
        fee = fee_schedule_of(MARKET, market_id="56-3722")
        table = MARKET["fees"]["taker_fee_bps_array"]
        assert fee.fee_type == "bps_by_price" and fee.taker_bps_by_cent == table and fee.maker_rate == 0
        assert fee.estimate(0.5, 100) == pytest.approx(table[49] / 10_000 * 50)
        assert fee.estimate(0.05, 12.03007519) == pytest.approx(0.00108271, abs=1e-6), "a recorded taker fill's fee"
        assert fee.estimate(0.5, 100, taker=False) == 0

    def test_an_amm_fee_is_a_share_of_the_value(self):
        fee = fee_schedule_of({**AMM, "fees": {"buy": {"fee": 0.01, "treasury_fee": 0.01, "distributor_fee": 0},
                                               "sell": {"fee": 0, "treasury_fee": 0, "distributor_fee": 0}}},
                              market_id="56-amm-120473")
        assert fee.fee_type == "share_of_value" and fee.taker_rate == 0.02 and fee.estimate(0.5, 10) == 0.1


class TestAdapter:
    def test_every_read_asks_for_both_kinds_of_market_and_drops_points(self):
        seen = {}

        def listing(params):
            seen.update(params)
            return {"data": AMM_MARKETS, "pagination": {"hasNext": False}}

        markets = venue_with({"/markets": listing}).fetch_markets(topic="sports", tag="nfl", in_play=True)
        assert seen["trading_model"] == "all" and seen["network_id"] == "56,2741,59144"
        assert seen["topics"] == "Sports" and seen["tags"] == "nfl" and seen["in_play"] == "true"
        assert [m.id for m in markets] == ["myriad:2741-amm-317"], "points markets are not money"

    def test_sort_and_status_reach_the_venue(self):
        seen = {}
        venue = venue_with({"/markets": lambda p: seen.update(p) or {"data": [], "pagination": {}}})
        venue.fetch_markets(sort="newest", status="settled")
        assert seen["sort"] == "published_at" and seen["state"] == "resolved"
        venue.fetch_markets(status="all")
        assert seen["state"] is None

    def test_paging_counts_markets_not_rows(self):
        pages = {1: {"data": MARKETS, "pagination": {"hasNext": True}}, 2: {"data": [AMM], "pagination": {"hasNext": False}}}
        venue = venue_with({"/markets": lambda p: pages[p["page"]]})
        first = venue.fetch_markets(limit=2)
        assert len(first) == 2 and first.next_cursor == "1:2"
        rest = venue.fetch_markets(limit=10, cursor=first.next_cursor)
        assert [m.id for m in rest] == ["myriad:56-3785", "myriad:56-amm-120473"] and rest.next_cursor is None

    def test_one_market_by_id_or_slug(self):
        venue = venue_with({"/markets/3722": MARKET, "/markets/btc-slug": MARKET})
        assert venue.fetch_market("myriad:56-3722").id == "myriad:56-3722"
        assert venue.http.calls[-1][1] == {"network_id": 56, "trading_model": "ob"}
        assert venue.fetch_market("btc-slug").id == "myriad:56-3722"
        with pytest.raises(MarketNotFound):
            venue.fetch_market("myriad:56-amm-3722"), "the AMM market with that number is another market"

    def test_batch_lookup_groups_by_network_and_kind(self):
        def by_ids(params):
            rows = MARKETS if params["trading_model"] == "ob" else [AMM]
            wanted = set(params["ids"].split(","))
            return {"data": [r for r in rows if str(r["id"]) in wanted]}

        venue = venue_with({"/markets": by_ids})
        found = venue.fetch_markets_by_ids(["myriad:56-amm-120473", "myriad:56-3785", "myriad:56-999", "56-2385"])
        assert [m.id for m in found] == ["myriad:56-amm-120473", "myriad:56-3785", "myriad:56-2385"]
        assert len(venue.http.calls) == 2

    def test_events_read_the_grouped_listing(self):
        seen = {}
        venue = venue_with({"/markets": lambda p: seen.update(p) or GROUPED})
        events = venue.fetch_events(limit=10)
        assert seen["group_by_event"] == "true" and len(events) == len(GROUPED["data"])
        assert events.next_cursor == "2:10"
        venue.fetch_events(cursor=events.next_cursor)
        assert seen["page"] == 2 and seen["limit"] == 10, "the next page is read at the size the cursor came from"

    def test_a_key_signs_every_read(self):
        sent = []

        def handler(request):
            sent.append(request)
            return httpx.Response(200, json={"data": [], "pagination": {}})

        venue = Myriad(api_key="key-1", api_secret="secret-1", limiter=None)
        venue.http._client = httpx.Client(transport=httpx.MockTransport(handler),
                                          auth=venue.http._client.auth)
        venue.fetch_markets(query="btc")
        request = sent[0]
        path = request.url.raw_path.decode()
        assert request.headers["x-api-key"] == "key-1"
        assert request.headers["x-api-signature"] == request_signature(
            "secret-1", request.headers["x-api-timestamp"], "GET", path, "")

    def test_an_unknown_market_is_not_found(self):
        venue = Myriad(limiter=None)
        venue.http._client = httpx.Client(transport=httpx.MockTransport(
            lambda request: httpx.Response(404, json=load("myriad_not_found.json"))))
        with pytest.raises(MarketNotFound, match="56-999999"):
            venue.fetch_market("myriad:56-999999")

    def test_a_key_without_its_secret_is_refused(self):
        with pytest.raises(BadRequest, match="secret"):
            Myriad(api_key="key-only")
