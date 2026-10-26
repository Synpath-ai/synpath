"""ProphetX normalizers, session and adapter. Every ProphetX route needs a key,
so the samples are the examples in ProphetX's own documentation
(docs.prophetx.co), copied unedited: the market data guide's v4 markets
(nested selections, `quantity` only) and the auto-fill guide's tournaments,
events and v3 markets (flat selections, with `value`)."""
from __future__ import annotations

import json
from fractions import Fraction

import httpx
import pytest

from synpath import AuthenticationError, BadRequest, MarketNotFound, NotSupported
from synpath.prophetx import (
    ProphetX, ProphetXSession, american_of, implied_probability, lines_of, markets_by_event, markets_of,
    normalize_event, normalize_market, normalize_order_book, parse_line_id, probability, status_of,
)
from synpath.trading.credentials import CredentialsMissing, ProphetXCredentials, load_prophetx

from conftest import load

TOURNAMENTS = load("prophetx_tournaments.json")
EVENTS = load("prophetx_sport_events.json")
V3 = load("prophetx_markets_v3.json")
V4 = load("prophetx_markets_v4.json")
AUTH = load("prophetx_auth.json")
EVENT = EVENTS["data"]["sport_events"][0]
LIONS = "90de769f4f4977a04446ed203e957351"
VIKINGS = "7d75b0c55f5578219f534413d0bd872b"


class TestPrices:
    @pytest.mark.parametrize("american, exact", [
        (-110, Fraction(11, 21)), (120, Fraction(5, 11)), (-470, Fraction(47, 57)), (-136, Fraction(34, 59)),
        (100, Fraction(1, 2)), (-100, Fraction(1, 2)), (10000, Fraction(1, 101)), (-10000, Fraction(100, 101)),
        ("-112", Fraction(28, 53)), (105.0, Fraction(20, 41)),
    ])
    def test_american_odds_are_exact_probabilities(self, american, exact):
        assert implied_probability(american) == exact

    @pytest.mark.parametrize("american", [None, 0, 50, -99, "x", True])
    def test_prices_american_odds_do_not_have(self, american):
        assert implied_probability(american) is None and probability(american) is None

    def test_a_read_price_is_eight_places(self):
        assert probability(-110) == 0.52380952 and probability(120) == 0.45454545

    @pytest.mark.parametrize("american", [-10000, -470, -136, -101, 100, 101, 124, 2500, 10000])
    def test_american_of_inverts_exactly(self, american):
        assert american_of(implied_probability(american)) == american

    def test_american_of_refuses_certainty(self):
        with pytest.raises(BadRequest):
            american_of(Fraction(1))


class TestLines:
    def test_nested_sides_make_one_line(self):
        [line] = lines_of(V4["data"][0])
        assert (line["_yes"], line["_no"]) == ("line_1", "line_2")

    def test_flat_sides_make_one_line_each(self):
        lines = [line for m in markets_of(V3) for line in lines_of(m)]
        assert [(ln["_yes"], ln["_no"]) for ln in lines] == [
            (LIONS, VIKINGS), ("b288cdeffc0f3ba7e384009a3502543f", "52882b69700724c6332375283b6e7f2a")]

    def test_a_three_way_market_is_not_binary(self):
        three = {**V4["data"][0], "type": "moneyline_3_way",
                 "selections": V4["data"][0]["selections"] + [[{"strike_id": "draw", "price": 250}]]}
        assert lines_of(three) == []

    def test_each_market_strike_is_its_own_line(self):
        market = {"id": 9, "name": "Spread", "type": "spread", "market_strikes": [
            {"strike": -3.5, "selections": [[{"strike_id": "a", "price": -110}], [{"strike_id": "b", "price": -110}]]},
            {"strike": -4.5, "selections": [[{"strike_id": "c", "price": 120}], [{"strike_id": "d", "price": -140}]]},
        ]}
        lines = lines_of(market)
        assert [(ln["_yes"], ln["strike"], ln["id"]) for ln in lines] == [("a", -3.5, 9), ("c", -4.5, 9)]

    def test_multiple_markets_keyed_or_flat(self):
        keyed = markets_by_event({"data": {"101": V4["data"], "102": []}})
        flat = markets_by_event({"data": [{**V4["data"][0], "event_id": 101}]})
        assert set(keyed) == {"101", "102"} and len(keyed["101"]) == 1
        assert list(flat) == ["101"]


class TestMarket:
    def test_a_line_is_named_by_its_event_and_yes_strike(self):
        [line] = lines_of(V4["data"][0])
        market = normalize_market({"event_id": 101, "name": "Patriots vs. Jets"}, line)
        assert market.id == "prophetx:101-line_1" and market.event_id == "prophetx:101"
        assert (market.yes.label, market.no.label) == ("Patriots", "Jets")
        assert market.yes.venue_token_id == "line_1" and market.info["no_strike_id"] == "line_2"
        assert market.title == "Patriots vs. Jets — Moneyline — Patriots"
        assert market.book_model == "shared_complement" and market.tick_size is None

    def test_side_one_is_the_yes_ask_and_side_two_the_yes_bid(self):
        [line] = lines_of(V4["data"][0])
        market = normalize_market({"event_id": 101}, line)
        # Patriots -110: YES at 110/210. Jets +105: NO at 100/205, YES bid one minus it.
        assert market.yes.quote.ask == 0.52380952 and market.yes.quote.bid == 0.51219512
        assert market.no.quote.ask == pytest.approx(1 - 0.51219512) and market.no.quote.bid == pytest.approx(1 - 0.52380952)
        # No `value` on these levels: the size falls back to `quantity`, in contracts.
        assert market.yes.quote.ask_size == pytest.approx(2100 * 210 / 110, rel=1e-6)

    def test_a_levels_value_is_the_stake_it_takes(self):
        [moneyline] = lines_of(markets_of(V3)[0])
        market = normalize_market(EVENT, moneyline)
        # Lions -470 can take $470: it returns 470 + 100 = 570 contracts.
        assert market.yes.quote.ask == probability(-470) and market.yes.quote.ask_size == pytest.approx(570, rel=1e-6)
        assert market.yes.quote.bid == pytest.approx(1 - 136 / 236)
        assert market.yes.quote.bid_size == pytest.approx(71.06 * 236 / 136, rel=1e-6)

    def test_the_event_gives_tags_and_series(self):
        market = normalize_market(EVENT, lines_of(markets_of(V3)[1])[0])
        assert market.tags == ["american football", "nfl"] and market.series_id == "31"
        assert market.info["strike"] == -5.5 and market.yes.label == "Detroit Lions -5.5"
        assert market.status == "open" and market.active is True

    @pytest.mark.parametrize("state, expected", [
        ("not_started", ("open", True)), ("live", ("open", True)), ("suspended", ("open", False)),
        ("interrupted", ("open", False)), ("ended", ("closed", False)), ("cancelled", ("closed", False)),
    ])
    def test_statuses(self, state, expected):
        assert status_of({"status": state}) == expected

    def test_placeholder_levels_are_not_liquidity(self):
        market = {"id": 1, "name": "Total", "selections": [
            [{"strike_id": "o", "name": "Over 48.5", "price": 0, "value": 0}],
            [{"strike_id": "u", "name": "Under 48.5", "price": None}]]}
        normalized = normalize_market({"event_id": 5}, lines_of(market)[0])
        assert normalized.yes.quote.bid is None and normalized.yes.quote.ask is None
        assert normalized.id == "prophetx:5-o"


class TestEvent:
    def test_an_event_holds_every_binary_line(self):
        event = normalize_event(EVENT, markets_of(V3))
        assert event.id == "prophetx:18925" and event.title == "Minnesota Vikings at Detroit Lions"
        assert [m.id for m in event.markets] == [f"prophetx:18925-{LIONS}", "prophetx:18925-b288cdeffc0f3ba7e384009a3502543f"]
        assert event.status == "open" and event.series_id == "31"
        assert [c["abbreviation"] for c in event.info["competitors"]] == ["DET", "MIN"]


class TestOrderBook:
    def test_best_first_and_mirrored_for_no(self):
        line = lines_of({"id": 1, "selections": [
            [{"strike_id": "a", "price": -110, "value": 110}, {"strike_id": "a", "price": -120, "value": 60}],
            [{"strike_id": "b", "price": 100, "value": 50}, {"strike_id": "b", "price": -105, "value": 21}]]})[0]
        yes = normalize_order_book(line, market_id="prophetx:1-a")
        assert [lv.price for lv in yes.asks] == [probability(-110), probability(-120)]
        assert [lv.price for lv in yes.bids] == [0.5, round(1 - probability(-105), 8)]
        assert yes.asks[0].size == pytest.approx(210) and yes.bids[0].size == pytest.approx(100)
        no = normalize_order_book(line, market_id="prophetx:1-a", side="no", depth=1)
        assert no.derived and no.asks[0].price == 0.5 and no.bids[0].price == pytest.approx(1 - probability(-110))
        assert len(no.asks) == len(no.bids) == 1 and yes.depth_scope == "top_n"

    def test_an_unknown_side_is_refused(self):
        with pytest.raises(BadRequest):
            normalize_order_book(lines_of(V4["data"][0])[0], market_id="x", side="maybe")  # type: ignore[arg-type]


class TestIds:
    def test_a_market_id_is_event_and_strike(self):
        assert parse_line_id("18925-" + LIONS) == ("18925", LIONS)

    @pytest.mark.parametrize("bad", ["18925", "abc-def", "-x", LIONS])
    def test_anything_else_is_refused(self, bad):
        with pytest.raises(BadRequest):
            parse_line_id(bad)


class FakeHttp:
    """Answers GETs by path, and records them."""

    def __init__(self, answers):
        self.calls: list[tuple[str, object]] = []
        self.answers = answers

    def get(self, path, params=None):
        self.calls.append((path, params))
        if path in self.answers:
            answer = self.answers[path]
            return answer(params) if callable(answer) else answer
        raise MarketNotFound(f"prophetx: no {path}")

    def close(self):
        pass


def venue_with(answers, **kwargs) -> ProphetX:
    venue = ProphetX(limiter=None, affiliate_key="aff", **kwargs)
    venue.http = FakeHttp(answers)
    return venue


NFL = {"data": {"tournaments": [{"id": 31, "name": "NFL"}, {"id": 132, "name": "NBA"}]}}


def events_answer(params):
    tournament = dict(params or []).get("tournament_id")
    return EVENTS if tournament in (None, "31") else {"data": {"sport_events": []}}


ANSWERS = {
    "/affiliate/get_tournaments": NFL,
    "/affiliate/get_sport_events": events_answer,
    "/v4/affiliate/get_markets": V3,
}


class TestAdapter:
    def test_markets_walk_tournaments_events_and_markets(self):
        venue = venue_with(ANSWERS)
        page = venue.fetch_markets(limit=1)
        assert [m.id for m in page] == [f"prophetx:18925-{LIONS}"] and page.next_cursor == "0:1"
        rest = venue.fetch_markets(limit=5, cursor=page.next_cursor)
        assert [m.id for m in rest] == ["prophetx:18925-b288cdeffc0f3ba7e384009a3502543f"] and rest.next_cursor is None
        assert ("/affiliate/get_tournaments", {"has_active_events": "true"}) in venue.http.calls

    def test_the_trading_api_reads_the_same_on_its_own_routes(self):
        venue = ProphetX(limiter=None, session=ProphetXSession(access_key="a", secret_key="s"))
        venue.http = FakeHttp({k.replace("affiliate", "mm"): v for k, v in ANSWERS.items()})
        assert venue.api == "mm" and len(venue.fetch_markets()) == 2

    def test_query_matches_event_and_competitor_names(self):
        venue = venue_with(ANSWERS)
        assert len(venue.fetch_markets(query="vikings")) == 2
        assert venue.fetch_markets(query="chiefs") == []

    def test_events_page_by_tournament(self):
        venue = venue_with(ANSWERS)
        page = venue.fetch_events()
        assert [e.id for e in page] == ["prophetx:18925"] and page.next_cursor == "1:0"
        assert venue.fetch_events(cursor=page.next_cursor) == []

    def test_only_open_events_are_listed(self):
        venue = venue_with(ANSWERS)
        with pytest.raises(NotSupported):
            venue.fetch_markets(status="settled")
        with pytest.raises(BadRequest):
            venue.fetch_markets(cursor="nonsense")

    def test_markets_by_id_and_books(self):
        venue = venue_with(ANSWERS)
        [market] = venue.fetch_markets_by_ids([f"prophetx:18925-{LIONS}", "prophetx:18925-gone"])
        assert market.yes.label == "Detroit Lions" and market.title.startswith("Minnesota Vikings at Detroit Lions")
        book = venue.fetch_order_book(f"prophetx:18925-{LIONS}")
        assert book.asks[0].price == probability(-470)
        assert set(venue.fetch_order_books(["prophetx:18925"])) == {
            f"prophetx:18925-{LIONS}", "prophetx:18925-b288cdeffc0f3ba7e384009a3502543f"}
        with pytest.raises(MarketNotFound):
            venue.fetch_order_book("prophetx:18925-gone")

    def test_many_events_are_read_in_one_request(self):
        venue = venue_with({"/v4/affiliate/get_multiple_markets": {"data": {"18925": markets_of(V3), "7": []}}})
        books = venue.fetch_order_books([f"prophetx:18925-{LIONS}", "prophetx:7-x"])
        assert list(books) == [f"prophetx:18925-{LIONS}"]
        [(path, params)] = venue.http.calls
        assert path == "/v4/affiliate/get_multiple_markets" and params == [("event_ids", "18925"), ("event_ids", "7")]

    def test_what_the_venue_does_not_offer(self):
        venue = venue_with({})
        for call in (venue.fetch_trades, venue.fetch_ohlcv, venue.fetch_fee_schedule):
            with pytest.raises(NotSupported):
                call(f"prophetx:18925-{LIONS}")


def transport(log: list, *, refresh_ok: bool = True, expire_first: bool = False):
    """A ProphetX that logs in, refreshes, and answers markets with a token."""
    state = {"login": 0, "refresh": 0, "rejected": False}

    def handler(request: httpx.Request) -> httpx.Response:
        log.append(request)
        path = request.url.path
        if path.endswith("/auth/login"):
            state["login"] += 1
            body = json.loads(request.content)
            assert body == {"access_key": "acc", "secret_key": "sec"}
            return httpx.Response(200, json={"data": {**AUTH["data"], "access_token": f"tok{state['login']}",
                                                      "refresh_token": "ref"}})
        if path.endswith("/auth/refresh"):
            state["refresh"] += 1
            if not refresh_ok:
                return httpx.Response(401, json={"error": "unauthorized", "message": "token expired"})
            return httpx.Response(200, json={"data": {"access_token": f"fresh{state['refresh']}"}})
        if expire_first and not state["rejected"]:
            state["rejected"] = True
            return httpx.Response(401, json={"error": "unauthorized", "message": "token expired"})
        return httpx.Response(200, json=V4)

    return httpx.MockTransport(handler), state


class TestSession:
    def test_a_login_then_bearer_tokens(self):
        log: list = []
        mock, state = transport(log)
        clock = [1000.0]
        session = ProphetXSession(access_key="acc", secret_key="sec", client=httpx.Client(transport=mock),
                                  clock=lambda: clock[0])
        venue = ProphetX(limiter=None, session=session, client=httpx.Client(transport=mock))
        venue.fetch_event_markets(["101"])
        assert log[-1].headers["authorization"] == "Bearer tok1" and log[-1].url.path == "/partner/v4/mm/get_markets"
        clock[0] += 9 * 60
        venue.fetch_event_markets(["101"])
        assert state == {"login": 1, "refresh": 1, "rejected": False}
        assert log[-1].headers["authorization"] == "Bearer fresh1"

    def test_a_refused_refresh_logs_in_again(self):
        log: list = []
        mock, state = transport(log, refresh_ok=False)
        clock = [1000.0]
        session = ProphetXSession(access_key="acc", secret_key="sec", client=httpx.Client(transport=mock),
                                  clock=lambda: clock[0])
        session.token()
        clock[0] += 9 * 60
        assert session.token() == "tok2" and state["login"] == 2 and state["refresh"] == 1

    def test_a_401_renews_the_token_once(self):
        log: list = []
        mock, state = transport(log, expire_first=True)
        session = ProphetXSession(access_key="acc", secret_key="sec", client=httpx.Client(transport=mock))
        venue = ProphetX(limiter=None, session=session, client=httpx.Client(transport=mock))
        assert venue.fetch_event_markets(["101"])["101"]
        assert state["refresh"] == 1 and log[-1].headers["authorization"] == "Bearer fresh1"

    def test_a_refused_login_is_an_authentication_error(self):
        def refuse(request):
            return httpx.Response(401, json={"error": "invalid_credentials", "message": "no"})

        session = ProphetXSession(access_key="acc", secret_key="sec", client=httpx.Client(transport=httpx.MockTransport(refuse)))
        with pytest.raises(AuthenticationError, match="invalid_credentials"):
            session.token()

    def test_the_affiliate_key_goes_raw(self):
        log: list = []

        def answer(request):
            log.append(request)
            return httpx.Response(200, json=TOURNAMENTS)

        venue = ProphetX(limiter=None, affiliate_key="aff-key", client=httpx.Client(transport=httpx.MockTransport(answer)))
        assert [t["id"] for t in venue.fetch_tournaments()] == [2568, 2739, 4221]
        assert log[0].headers["authorization"] == "aff-key" and log[0].url.path == "/partner/affiliate/get_tournaments"

    def test_without_a_key_nothing_is_sent(self):
        venue = ProphetX(limiter=None, login=False, client=httpx.Client(transport=httpx.MockTransport(
            lambda r: pytest.fail("sent without a key"))))
        with pytest.raises(AuthenticationError, match="PROPHETX_ACCESS_KEY"):
            venue.fetch_tournaments()

    def test_the_sandbox(self):
        venue = ProphetX(limiter=None, credentials=ProphetXCredentials(affiliate_key="k", sandbox=True))
        assert venue.http.base_url == "https://api.sandbox.prophetx.dev/partner"


class TestCredentials:
    def test_the_key_pair_and_the_affiliate_key(self):
        creds = load_prophetx({"PROPHETX_ACCESS_KEY": "acc-123", "PROPHETX_SECRET_KEY": "sec-456",
                               "PROPHETX_AFFILIATE_KEY": "aff-789", "PROPHETX_SANDBOX": "1"})
        assert creds.trades and creds.sandbox and creds.secrets == ["acc-123", "sec-456", "aff-789"]
        assert not any(v in repr(creds) for v in creds.secrets) and "secrets=***" in repr(creds)

    def test_the_affiliate_key_alone_reads_market_data(self):
        creds = load_prophetx({"PROPHETX_AFFILIATE_KEY": "k"})
        assert creds.affiliate_key == "k" and not creds.trades

    def test_nothing_set_is_not_configured(self):
        assert load_prophetx({}) is None

    def test_half_a_key_pair_is_refused(self):
        with pytest.raises(CredentialsMissing, match="PROPHETX_SECRET_KEY"):
            load_prophetx({"PROPHETX_ACCESS_KEY": "a"})
