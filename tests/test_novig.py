"""Novig normalizers and adapter, against payloads recorded from the live
venue's public catalog. Every request goes to an `httpx.MockTransport`
answering from the samples."""
from __future__ import annotations

import re

import httpx
import pytest

from synpath import BadRequest, MarketNotFound, NotSupported
from synpath.novig import (
    Novig, fee_schedule_of, normalize_event, normalize_market, normalize_order_book, normalize_trade, quotes_of,
    result_of, side_ids, status_of,
)

from conftest import load

EVENTS = load("novig_events.json")
EVENT = load("novig_event.json")
EVENT_MARKETS = load("novig_event_markets.json")["items"]
MARKETS = load("novig_markets.json")
MARKET = load("novig_market.json")
BOOK = load("novig_book.json")
TRADES = load("novig_trades.json")

MIL = MARKET["marketId"]
MIL_EVENT = next(e for e in EVENTS["items"] if e["eventId"] == MARKET["eventId"])
YES, NO = sorted(o["outcomeId"] for o in MARKET["outcomes"])
ALL_MARKETS = [*MARKETS["items"], *EVENT_MARKETS]


class Api:
    """The public catalog, answered from the samples. Records every request."""

    def __init__(self, overrides: dict | None = None):
        self.requests: list[httpx.Request] = []
        self.overrides = overrides or {}

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        for pattern, answer in self.overrides.items():
            if re.fullmatch(pattern, path):
                return answer(request) if callable(answer) else answer
        params = request.url.params
        if path == "/v3/public/catalog/events":
            return httpx.Response(200, json=EVENTS if not params.get("after") else {"items": []})
        if m := re.fullmatch(r"/v3/public/catalog/events/([^/]+)", path):
            found = [e for e in EVENTS["items"] if e["eventId"] == m.group(1)]
            return httpx.Response(200, json=found[0]) if found else not_found("EVENT_NOT_FOUND")
        if path == "/v3/public/catalog/markets":
            if params.get("event"):
                return httpx.Response(200, json={"items": [x for x in ALL_MARKETS if x["eventId"] == params["event"]]})
            return httpx.Response(200, json=MARKETS)
        if m := re.fullmatch(r"/v3/public/catalog/markets/([^/]+)/book", path):
            return httpx.Response(200, json=BOOK) if m.group(1) == MIL else httpx.Response(
                200, json={"marketId": m.group(1), "seq": 0, "orders": {}})
        if m := re.fullmatch(r"/v3/public/catalog/markets/([^/]+)/trades", path):
            first = m.group(1) == MIL and not params.get("after")
            return httpx.Response(200, json=TRADES if first else {"items": []})
        if m := re.fullmatch(r"/v3/public/catalog/markets/([^/]+)", path):
            found = [x for x in ALL_MARKETS if x["marketId"] == m.group(1)]
            return httpx.Response(200, json=found[0]) if found else not_found("MARKET_NOT_FOUND")
        return not_found("NOT_FOUND")

    def paths(self) -> list[str]:
        return [r.url.path for r in self.requests]


def not_found(code: str) -> httpx.Response:
    return httpx.Response(404, json={"code": code, "message": "Not found"})


def venue(api: Api | None = None, **kwargs) -> tuple[Novig, Api]:
    api = api or Api()
    return Novig(limiter=None, client=httpx.Client(transport=httpx.MockTransport(api.handler)), **kwargs), api


def market_of_type(kind: str) -> dict:
    return next(m for m in ALL_MARKETS if m["marketType"] == kind)


class TestMarket:
    def test_a_market_is_named_by_its_id_and_its_event(self):
        market = normalize_market(MARKET, MIL_EVENT)
        assert market.id == f"novig:{MIL}" and market.venue_market_id == MIL
        assert market.event_id == f"novig:{MARKET['eventId']}"
        assert market.title == f"{MIL_EVENT['description']} — {MARKET['description']}"
        assert market.book_model == "shared_complement" and market.face_value == 1.0

    def test_the_outcome_listed_first_is_yes(self):
        market = normalize_market(MARKET, MIL_EVENT)
        assert side_ids(MARKET) == (YES, NO)
        assert market.yes.venue_token_id == YES and market.no.venue_token_id == NO
        first = next(o for o in MARKET["outcomes"] if o["outcomeId"] == YES)
        assert market.yes.label == first["name"] and market.outcome_label == first["name"]

    def test_the_order_of_the_outcomes_array_does_not_matter(self):
        flipped = {**MARKET, "outcomes": list(reversed(MARKET["outcomes"]))}
        assert side_ids(flipped) == (YES, NO)

    def test_a_total_is_over_against_under(self):
        total = normalize_market(market_of_type("TOTAL"), EVENT)
        assert total.yes.label.startswith("Over") and total.no.label.startswith("Under")
        assert total.info["strike"] == market_of_type("TOTAL")["strike"]

    def test_a_prop_is_yes_against_no(self):
        prop = next(m for m in ALL_MARKETS if [o["name"] for o in m["outcomes"]] == ["Yes", "No"])
        market = normalize_market(prop)
        assert (market.yes.label, market.no.label) == ("Yes", "No")

    def test_quotes_are_each_outcomes_best_bid(self):
        yes, no = quotes_of(MARKET)
        bids = {o["outcomeId"]: o.get("bid") for o in MARKET["outcomes"]}
        assert yes.bid == float(bids[YES]["price"]) and yes.bid_size == bids[YES]["qty"] / 100
        assert yes.ask == round(1 - float(bids[NO]["price"]), 6) and yes.ask_size == bids[NO]["qty"] / 100
        assert no.bid == round(1 - yes.ask, 6) and no.ask == round(1 - yes.bid, 6)
        last = next(o for o in MARKET["outcomes"] if o["outcomeId"] == YES)["last"]
        assert yes.last == float(last) and no.last == round(1 - float(last), 6)

    def test_an_outcome_without_bids_leaves_that_side_empty(self):
        bare = {**MARKET, "outcomes": [{k: v for k, v in o.items() if k != "bid"} for o in MARKET["outcomes"]]}
        yes, no = quotes_of(bare)
        assert yes.bid is None and yes.ask is None and yes.mid is None and no.bid is None

    def test_volume_and_open_interest_are_dollar_contracts(self):
        market = normalize_market(MARKET, MIL_EVENT)
        assert market.stats.volume_total == MARKET["volume"] / 100
        assert market.stats.open_interest == MARKET["openInterest"] / 100
        assert market.stats.volume_unit == "contracts" and market.info["volume_qty"] == MARKET["volume"]

    def test_sport_and_league_are_the_tags(self):
        market = normalize_market(MARKET, MIL_EVENT)
        assert market.tags == [MIL_EVENT["sport"].lower(), MIL_EVENT["league"].lower()]
        assert market.category == MIL_EVENT["sport"].lower()

    @pytest.mark.parametrize("market,event,expected", [
        ("OPEN", "OPEN_PREGAME", ("open", True)), ("OPEN", "OPEN_INGAME", ("open", True)),
        ("OPEN", "DELAYED", ("open", True)), ("OPEN", "CLOSED_PREGAME", ("open", False)),
        ("OPEN", "FINAL", ("closed", False)), ("OPEN", "CANCELED", ("closed", False)),
        ("CLOSED", "OPEN_INGAME", ("closed", False)), ("SETTLED", "FINAL", ("settled", False)),
    ])
    def test_status_of(self, market, event, expected):
        assert status_of(market, event) == expected

    @pytest.mark.parametrize("yes,no,expected", [
        ("TBD", "TBD", (None, None)), ("WIN", "LOSS", ("yes", None)), ("LOSS", "WIN", ("no", None)),
        ("PUSH", "PUSH", ("void", None)), ("0.420", "0.580", ("void", "0.420")),
    ])
    def test_grades(self, yes, no, expected):
        graded = {**MARKET, "outcomes": [{**o, "status": yes if o["outcomeId"] == YES else no} for o in MARKET["outcomes"]]}
        assert result_of(graded) == expected

    def test_an_in_play_event(self):
        live = next(e for e in EVENTS["items"] if e["status"] == "OPEN_INGAME")
        market = normalize_market(MARKET, live)
        assert market.info["in_play"] and market.active and market.native_status == "OPEN/OPEN_INGAME"


class TestEvent:
    def test_an_event_holds_its_markets(self):
        event = normalize_event(EVENT, EVENT_MARKETS)
        assert event.id == f"novig:{EVENT['eventId']}" and event.title == EVENT["description"]
        assert [m.id for m in event.markets] == [f"novig:{m['marketId']}" for m in EVENT_MARKETS]
        assert event.status == "open" and event.mutually_exclusive is None
        assert event.info["league"] == EVENT["league"]

    def test_a_finished_event_is_closed(self):
        final = next(e for e in EVENTS["items"] if e["status"] == "FINAL")
        assert normalize_event(final, [{**MARKET, "eventId": final["eventId"]}]).status == "closed"


class TestBook:
    def test_yes_bids_and_no_bids_as_asks(self):
        book = normalize_order_book(BOOK, yes_outcome=YES, no_outcome=NO, market_id=f"novig:{MIL}")
        yes_prices = {float(o["price"]) for o in BOOK["orders"][YES]}
        no_prices = {round(1 - float(o["price"]), 6) for o in BOOK["orders"][NO]}
        assert {level.price for level in book.bids} == yes_prices
        assert {level.price for level in book.asks} == no_prices
        assert book.bids == sorted(book.bids, key=lambda level: -level.price)
        assert book.asks == sorted(book.asks, key=lambda level: level.price)
        assert book.best_bid.price < book.best_ask.price

    def test_orders_at_one_price_are_summed(self):
        book = normalize_order_book(BOOK, yes_outcome=YES, no_outcome=NO, market_id=f"novig:{MIL}")
        top = book.bids[0].price
        assert book.bids[0].size == round(sum(o["qty"] for o in BOOK["orders"][YES] if float(o["price"]) == top) / 100, 6)

    def test_the_no_side_mirrors_it(self):
        yes = normalize_order_book(BOOK, yes_outcome=YES, no_outcome=NO, market_id=f"novig:{MIL}")
        no = normalize_order_book(BOOK, yes_outcome=YES, no_outcome=NO, market_id=f"novig:{MIL}", side="no")
        assert no.derived and no.bids[0].price == round(1 - yes.asks[0].price, 6)
        assert no.asks[0].size == yes.bids[0].size

    def test_an_unknown_side_is_refused(self):
        with pytest.raises(BadRequest):
            normalize_order_book(BOOK, yes_outcome=YES, no_outcome=NO, market_id="x", side="maybe")  # type: ignore[arg-type]


class TestTrades:
    def test_a_print_on_yes_is_a_taker_selling_yes(self):
        row = next(r for r in TRADES["items"] if r["outcomeId"] == YES)
        trade = normalize_trade(row, yes_outcome=YES, market_id=f"novig:{MIL}")
        assert trade.side == "sell" and trade.price == float(row["price"])
        assert trade.quantity == row["qty"] / 100 and trade.id == row["tradeId"]

    def test_a_print_on_no_is_a_taker_buying_yes(self):
        rows = [r for r in TRADES["items"] if r["outcomeId"] == NO]
        if not rows:
            rows = [{**TRADES["items"][0], "outcomeId": NO, "price": "0.370"}]
        trade = normalize_trade(rows[0], yes_outcome=YES, market_id=f"novig:{MIL}")
        assert trade.side == "buy" and trade.price == round(1 - float(rows[0]["price"]), 6)


class TestFees:
    def test_the_markets_own_coefficient_and_maker_credit(self):
        schedule = fee_schedule_of(MARKET)
        coefficient, credit = float(MARKET["fee"]["coefficient"]), float(MARKET["fee"]["makerCredit"])
        assert schedule.fee_type == "quadratic_theta" and schedule.taker_rate == coefficient
        assert schedule.maker_rate == -round(coefficient * credit, 9)
        assert schedule.info["charged"] == MARKET["fee"]["charged"]

    def test_the_docs_worked_examples(self):
        # 10,000 venue contracts (100 dollar contracts) at 0.50: $1.50; at 0.30: $1.26.
        schedule = fee_schedule_of({"marketId": "m", "fee": {"coefficient": "0.06", "makerCredit": "0.25",
                                                             "charged": "WHEN_LIVE"}})
        assert schedule.estimate(0.5, 100) == 1.5 and schedule.estimate(0.3, 100) == 1.26
        assert schedule.estimate(0.5, 100, taker=False) == -0.375


class TestAdapter:
    def test_fetch_markets_is_one_page_with_titles(self):
        nv, api = venue()
        page = nv.fetch_markets(limit=10)
        assert [m.id for m in page] == [f"novig:{m['marketId']}" for m in MARKETS["items"]]
        assert page.next_cursor == MARKETS["next"]
        first = api.requests[0]
        assert first.url.path == "/v3/public/catalog/markets" and first.url.params["limit"] == "10"
        assert first.url.params["eventStatus"] == "OPEN_PREGAME,OPEN_INGAME,DELAYED,CLOSED_PREGAME"
        assert page[0].title.startswith(MIL_EVENT["description"])

    def test_a_lookup_by_id_reuses_the_rows_a_listing_returned(self):
        # The catalog has no lookup for several ids and its edge throttles per
        # IP: the terminal lists markets, then asks for them by id.
        nv, api = venue()
        listed = nv.fetch_markets(limit=10)
        before = len(api.paths())
        again = nv.fetch_markets_by_ids([m.id for m in listed])
        assert [m.id for m in again] == [m.id for m in listed]
        assert not [p for p in api.paths()[before:] if re.fullmatch(r"/v3/public/catalog/markets/[^/]+", p)]

    def test_a_page_of_events_reads_their_markets_in_one_listing(self):
        def listing(request: httpx.Request) -> httpx.Response:
            params = request.url.params
            if params.get("event"):
                return httpx.Response(200, json={"items": [x for x in ALL_MARKETS if x["eventId"] == params["event"]]})
            assert params.get("startsAfter") and params.get("startsBefore")
            return httpx.Response(200, json={"items": ALL_MARKETS})
        nv, api = venue(Api({r"/v3/public/catalog/markets": listing}))
        page = nv.fetch_events(limit=20)
        assert api.paths().count("/v3/public/catalog/markets") == 1
        one_by_one, _ = venue()
        assert [[m.id for m in e.markets] for e in page] == \
            [[m.id for m in one_by_one.fetch_event(e.id).markets] for e in page]

    def test_filters_are_passed_through(self):
        nv, api = venue()
        nv.fetch_markets(league=["NFL", "NBA"], market_type="MONEY", cursor="abc", status="closed")
        params = api.requests[0].url.params
        assert (params["league"], params["marketType"], params["after"]) == ("NFL,NBA", "MONEY", "abc")
        assert params["eventStatus"] == "FINAL,CANCELED"

    def test_settled_listings_are_not_in_the_public_catalog(self):
        nv, _ = venue()
        with pytest.raises(NotSupported):
            nv.fetch_markets(status="settled")
        nv.fetch_events(status="all")

    def test_a_search_matches_event_descriptions(self):
        nv, api = venue()
        page = nv.fetch_markets(query="maple leafs")
        assert page and all(m.event_id == f"novig:{EVENT['eventId']}" for m in page)
        assert any(r.url.params.get("event") == EVENT["eventId"] for r in api.requests)

    def test_no_sort(self):
        nv, _ = venue()
        with pytest.raises(NotSupported):
            nv.fetch_markets(sort="volume")

    def test_fetch_events_reads_each_events_markets(self):
        nv, api = venue()
        page = nv.fetch_events(limit=2)
        assert [e.id for e in page] == [f"novig:{e['eventId']}" for e in EVENTS["items"][:2]]
        assert page.next_cursor == EVENTS["next"]
        assert sum(1 for r in api.requests if r.url.params.get("event")) == 2

    def test_fetch_market_and_its_outcomes_are_kept(self):
        nv, api = venue()
        market = nv.fetch_market(f"novig:{MIL}")
        assert market.id == f"novig:{MIL}" and market.title.startswith(MIL_EVENT["description"])
        assert nv.outcome_ids(MIL) == (YES, NO)
        before = len(api.requests)
        nv.fetch_order_book(MIL)
        assert len(api.requests) == before + 1  # the book only: the outcomes were known

    def test_an_unknown_market(self):
        nv, _ = venue()
        with pytest.raises(MarketNotFound, match="history"):
            nv.fetch_market("novig:00000000-0000-0000-0000-000000000000")
        assert nv.fetch_markets_by_ids(["novig:00000000-0000-0000-0000-000000000000", f"novig:{MIL}"])[0].id == f"novig:{MIL}"

    def test_another_venues_id_is_refused(self):
        nv, _ = venue()
        with pytest.raises(BadRequest):
            nv.fetch_market("kalshi:KXFOO")

    def test_a_key_reads_a_closed_market_from_history(self):
        settled = {**MARKET, "status": "SETTLED", "settledTs": 1792800000000,
                   "outcomes": [{**o, "status": "WIN" if o["outcomeId"] == YES else "LOSS"} for o in MARKET["outcomes"]]}
        history: list[httpx.Request] = []

        def signed(request: httpx.Request) -> httpx.Response:
            history.append(request)
            if request.url.path == f"/v3/history/markets/{MIL}":
                return httpx.Response(200, json=settled)
            return not_found("EVENT_NOT_FOUND")

        from test_novig_trading import credentials, verified

        api = Api({rf"/v3/public/catalog/markets/{MIL}": not_found("MARKET_NOT_FOUND")})
        nv, _ = venue(api, credentials=credentials(), signed_client=httpx.Client(transport=httpx.MockTransport(signed)))
        market = nv.fetch_market(MIL)
        assert market.status == "settled" and market.info["result"] == "yes" and market.resolution_timestamp
        assert verified(history[0]) and history[0].url.host == "api.paper.novig.com"

    def test_book_depth_is_capped_at_twenty(self):
        nv, api = venue()
        book = nv.fetch_order_book(f"novig:{MIL}", depth=50)
        assert api.requests[-1].url.params["depth"] == "20" and book.depth_scope == "top_n"
        nv.fetch_order_book(f"novig:{MIL}", depth=1)
        assert api.requests[-1].url.params["depth"] == "1"

    def test_fetch_trades_oldest_first(self):
        nv, _ = venue()
        trades = nv.fetch_trades(f"novig:{MIL}")
        assert [t.timestamp for t in trades] == sorted(t.timestamp for t in trades)
        assert trades.next_cursor == TRADES.get("next")
        assert {t.side for t in trades} <= {"buy", "sell"}

    def test_candles_from_the_tape(self):
        nv, _ = venue()
        candles = nv.fetch_ohlcv(f"novig:{MIL}", timeframe="1m")
        assert candles and all(c.price_source == "trade" for c in candles)
        assert sum(c.trade_count for c in candles) == len(TRADES["items"])

    def test_fee_schedule_reads_the_market(self):
        nv, _ = venue()
        assert nv.fetch_fee_schedule(f"novig:{MIL}").scope_id == MIL

    def test_paper_is_its_own_host(self):
        nv = Novig(env="paper")
        assert nv.http.base_url == "https://api.paper.novig.com"
        with pytest.raises(BadRequest):
            Novig(env="sandbox")

    def test_it_is_a_registered_venue(self):
        import synpath

        assert synpath.exchanges["novig"] is Novig and "novig" in synpath.ids.VENUES
        assert synpath.Novig.has["search"] == "partial" and synpath.Novig.has["create_order"] is False
