"""Matchbook: ids, back/lay read on YES, catalog walk, status, books, fees, login.

The market-data samples under `tests/samples/matchbook_*.json` were recorded
from api.matchbook.com anonymously on 2026-10-23, then trimmed: three events
kept (a race and two football fixtures kicking off at the same second), a few
markets each, the race's runners cut to four and one withdrawn, the sports
list to three, and `total` set to what was kept. The login sample is the
example in Matchbook's API reference. Every request goes to an
`httpx.MockTransport`; nothing here reaches Matchbook.
"""
from __future__ import annotations

import json
import re

import httpx
import pytest

import synpath
from synpath import AuthenticationError, BadRequest, MarketNotFound, NotSupported
from synpath.matchbook import (
    DEFAULT_COMMISSION, SESSION_REFRESH_AFTER, Matchbook, MatchbookSession, fee_schedule_of, iso_ms,
    normalize_event, normalize_market, normalize_order_book, parse_market_id, parse_runner_id, plain_line,
    runner_native_id, start_s, status_of,
)
from synpath.trading.credentials import CredentialsMissing, load_matchbook

from conftest import load

EVENTS = load("matchbook_events.json")
SPORTS = load("matchbook_sports.json")
PRICES = load("matchbook_prices.json")
CLOSED = load("matchbook_closed.json")
ERRORS = load("matchbook_errors.json")
LOGIN = load("matchbook_trading.json")["login"]
LOGIN_REFUSED = load("matchbook_trading.json")["login_refused"]
LOGIN_MFA = load("matchbook_trading.json")["login_mfa"]

RACE, ARSENAL_EVERTON, COVENTRY_FULHAM = EVENTS["events"]
MATCH_ODDS, TOTAL, WHOLE_HANDICAP, ASIAN_HANDICAP, BTTS = ARSENAL_EVERTON["markets"]
WIN = RACE["markets"][0]
ARSENAL = MATCH_ODDS["runners"][0]
ARSENAL_ID = "matchbook:34569494493900023-34569495835700023-34569495836701023"
MATCH_ODDS_ID = "matchbook:34569494493900023-34569495835700023"
CLOSED_EVENT = CLOSED["event"]
CLOSED_MARKET = CLOSED["markets"]["markets"][0]
CLOSED_RUNNER = CLOSED["runners"]["runners"][0]
CLOSED_ID = f"matchbook:{CLOSED_EVENT['id']}-{CLOSED_MARKET['id']}-{CLOSED_RUNNER['id']}"


@pytest.fixture(autouse=True)
def no_ambient_credentials(monkeypatch, tmp_path):
    """No test may pick up a real login from the environment or a `.env`."""
    for name in ("MATCHBOOK_USERNAME", "MATCHBOOK_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)


class Api:
    """api.matchbook.com, answered from the samples. Records every request."""

    def __init__(self, overrides: dict | None = None):
        self.requests: list[httpx.Request] = []
        self.overrides = overrides or {}

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path, params = request.url.path, request.url.params
        for pattern, answer in self.overrides.items():
            if re.fullmatch(pattern, path):
                return answer(request) if callable(answer) else answer
        if path == "/bpapi/rest/security/session" and request.method == "POST":
            return httpx.Response(200, json=LOGIN)
        if path == "/edge/rest/lookups/sports":
            return httpx.Response(200, json=SPORTS)
        if path == "/edge/rest/events":
            return httpx.Response(200, json=self.listing(params))
        if m := re.fullmatch(r"/edge/rest/events/(\d+)", path):
            event = self.event(m.group(1))
            return httpx.Response(200, json=event) if event else httpx.Response(404, json=ERRORS["not_found"])
        if m := re.fullmatch(r"/edge/rest/events/(\d+)/markets/(\d+)", path):
            market = self.market(m.group(1), m.group(2))
            return httpx.Response(200, json=market) if market else httpx.Response(404, json=ERRORS["not_found"])
        if m := re.fullmatch(r"/edge/rest/events/(\d+)/markets/(\d+)/runners", path):
            if (m.group(1), m.group(2)) == (str(CLOSED_EVENT["id"]), str(CLOSED_MARKET["id"])):
                return httpx.Response(200, json=CLOSED["runners"])
            return httpx.Response(404, json=ERRORS["not_found"])
        if m := re.fullmatch(r"/edge/rest/events/(\d+)/markets/(\d+)/runners/(\d+)/prices", path):
            if f"matchbook:{m.group(1)}-{m.group(2)}-{m.group(3)}" == ARSENAL_ID:
                return httpx.Response(200, json=PRICES)
            return httpx.Response(404, json=ERRORS["not_found"])
        return httpx.Response(404, json=ERRORS["not_found"])

    @staticmethod
    def event(event_id: str) -> dict | None:
        if event_id == str(CLOSED_EVENT["id"]):
            return CLOSED_EVENT
        return next((e for e in EVENTS["events"] if str(e["id"]) == event_id), None)

    def market(self, event_id: str, market_id: str) -> dict | None:
        if (event_id, market_id) == (str(CLOSED_EVENT["id"]), str(CLOSED_MARKET["id"])):
            return CLOSED_MARKET
        event = self.event(event_id) or {}
        return next((m for m in event.get("markets") or [] if str(m["id"]) == market_id), None)

    @staticmethod
    def listing(params) -> dict:
        """The events listing as Matchbook pages it: ordered by start, `after`
        in whole seconds and inclusive, then `offset` and `per-page`."""
        rows = list(EVENTS["events"]) + [CLOSED_EVENT]
        if params.get("ids"):
            wanted = params["ids"].split(",")
            rows = [e for e in rows if str(e["id"]) in wanted]
        states = (params.get("states") or "open,suspended,closed,graded").split(",")
        rows = [e for e in rows if e["status"] in states]
        if params.get("sport-ids"):
            rows = [e for e in rows if str(e["sport-id"]) in params["sport-ids"].split(",")]
        if params.get("after"):
            rows = [e for e in rows if start_s(e) >= int(params["after"])]
        rows.sort(key=lambda e: iso_ms(e["start"]))
        offset, size = int(params.get("offset") or 0), int(params.get("per-page") or 20)
        return {"offset": offset, "per-page": size, "total": len(rows), "events": rows[offset:offset + size]}

    def paths(self) -> list[str]:
        return [r.url.path for r in self.requests]

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self.handler))


def venue(api: Api | None = None, **kwargs) -> tuple[Matchbook, Api]:
    api = api or Api()
    return Matchbook(limiter=None, navigation_limiter=None, client=api.client(), **kwargs), api


def session(api: Api, **kwargs) -> MatchbookSession:
    return MatchbookSession(username="jbloggs", password="hunter2", client=api.client(), **kwargs)


class TestIds:
    def test_round_trip(self):
        native = runner_native_id(34569494493900023, 34569495835700023, 34569495836701023)
        assert f"matchbook:{native}" == ARSENAL_ID
        assert parse_runner_id(native) == ("34569494493900023", "34569495835700023", "34569495836701023")
        assert parse_market_id(native) == parse_market_id("34569494493900023-34569495835700023")

    @pytest.mark.parametrize("bad", ["34569494493900023", "1-2", "1-2-x", "a-b-c", "1-2-3-4"])
    def test_a_bad_runner_id_is_refused(self, bad):
        with pytest.raises(BadRequest):
            parse_runner_id(bad)

    def test_synpath_ids_route_to_matchbook(self):
        assert synpath.ids.venue_of(ARSENAL_ID) == "matchbook"
        assert "matchbook" in synpath.ids.VENUES and synpath.exchanges["matchbook"] is Matchbook


class TestBackAndLayReadOnYes:
    """A `back` price is one a taker backs at, a resting lay: a YES ask. A
    `lay` price is one a taker lays at, a resting back: a YES bid."""

    def test_lay_prices_are_bids_and_back_prices_asks(self):
        book = normalize_order_book(PRICES["prices"], market_id=ARSENAL_ID)
        assert book.bids[0].price == pytest.approx(1 / 1.50) and book.asks[0].price == pytest.approx(1 / 1.49)
        assert [round(1 / level.price, 2) for level in book.bids] == [1.5, 1.51, 1.52, 1.53, 1.54]
        assert [round(1 / level.price, 2) for level in book.asks] == [1.49, 1.48, 1.47, 1.46, 1.45]

    def test_sizes_are_the_backers_stake_times_the_odds(self):
        book = normalize_order_book(PRICES["prices"], market_id=ARSENAL_ID)
        assert book.bids[0].size == pytest.approx(1388.11838 * 1.5)
        assert book.asks[0].size == pytest.approx(4394.55779 * 1.49)

    def test_no_recorded_book_is_crossed(self):
        for event in EVENTS["events"]:
            for market in event["markets"]:
                for runner in market["runners"]:
                    book = normalize_order_book(runner["prices"], market_id="matchbook:x")
                    if book.bids and book.asks:
                        assert book.bids[0].price < book.asks[0].price, runner["name"]

    def test_the_venues_own_rows_and_currency_are_kept(self):
        book = normalize_order_book(PRICES["prices"], market_id=ARSENAL_ID)
        assert book.info["back"][0] == [1.49, 4394.55779] and book.info["lay"][0] == [1.5, 1388.11838]
        assert book.info["currency"] == "GBP" and book.book_model == "shared_complement"
        assert book.depth_scope == "top_n"

    def test_the_no_side_is_the_mirror(self):
        no = normalize_order_book(PRICES["prices"], market_id=ARSENAL_ID, side="no")
        assert no.derived and no.bids[0].price == pytest.approx(1 - 1 / 1.49) and no.asks[0].price == pytest.approx(1 / 3)
        with pytest.raises(BadRequest):
            normalize_order_book(PRICES["prices"], market_id=ARSENAL_ID, side="maybe")  # type: ignore[arg-type]

    def test_depth_cuts_each_side(self):
        book = normalize_order_book(PRICES["prices"], market_id=ARSENAL_ID, depth=2)
        assert len(book.bids) == len(book.asks) == 2

    def test_an_empty_runner(self):
        withdrawn = WIN["runners"][-1]
        book = normalize_order_book(withdrawn["prices"], market_id="matchbook:x")
        assert book.bids == [] and book.asks == []


class TestCatalogMapping:
    def arsenal(self):
        return normalize_market(ARSENAL_EVERTON, MATCH_ODDS, ARSENAL)

    def test_each_runner_is_a_market_and_the_matchbook_market_its_event(self):
        market = self.arsenal()
        assert market.id == ARSENAL_ID and market.event_id == MATCH_ODDS_ID
        assert market.title == "Arsenal vs Everton — Match Odds — Arsenal"
        assert market.yes.label == "Arsenal" and market.no.label == "Not Arsenal"
        assert market.info["matchbook_event"]["name"] == "Arsenal vs Everton"
        assert "markets" not in market.info["matchbook_event"] and "prices" not in market.info["runner"]

    def test_tags_are_sport_competition_and_country(self):
        assert self.arsenal().tags == ["soccer", "england premier league", "england"]
        assert self.arsenal().category == "soccer"
        race = normalize_market(RACE, WIN, WIN["runners"][0])
        assert race.tags == ["horse racing", "ireland", "dundalk"]

    def test_quotes_and_volume(self):
        market = self.arsenal()
        assert market.yes.quote.bid == pytest.approx(1 / 1.5) and market.yes.quote.ask == pytest.approx(1 / 1.49)
        assert market.no.quote.bid == pytest.approx(1 - 1 / 1.49) and market.yes.quote.last is None
        assert market.stats.volume_total == pytest.approx(6330.96959) and market.stats.volume_unit == "collateral"
        assert market.info["currency"] == "GBP" and market.info["in_play"] is False

    def test_a_market_that_turns_in_play_has_no_known_close(self):
        assert self.arsenal().close_timestamp is None
        handicap = normalize_market(ARSENAL_EVERTON, WHOLE_HANDICAP, WHOLE_HANDICAP["runners"][0])
        assert handicap.close_datetime == "2026-10-24T14:00:00Z"

    def test_lines_that_can_push_or_half_win_are_not_binary(self):
        assert plain_line(MATCH_ODDS) and plain_line(TOTAL) and plain_line(BTTS)
        assert not plain_line(WHOLE_HANDICAP) and not plain_line(ASIAN_HANDICAP)
        assert normalize_market(ARSENAL_EVERTON, TOTAL, TOTAL["runners"][0]).market_type == "binary"
        assert normalize_market(ARSENAL_EVERTON, ASIAN_HANDICAP, ASIAN_HANDICAP["runners"][0]).market_type == "unknown"
        assert normalize_market(ARSENAL_EVERTON, TOTAL, TOTAL["runners"][0]).info["line"] == 2.5

    def test_the_event_holds_every_runner_and_is_exclusive(self):
        event = normalize_event(ARSENAL_EVERTON, MATCH_ODDS)
        assert event.id == MATCH_ODDS_ID and len(event.markets) == 3
        assert {m.outcome_label for m in event.markets} == {"Arsenal", "Everton", "Draw"}
        assert event.mutually_exclusive is True and event.status == "open"
        assert normalize_event(ARSENAL_EVERTON, WHOLE_HANDICAP).mutually_exclusive is None

    def test_serializes(self):
        json.dumps(self.arsenal().model_dump())


class TestStatus:
    @pytest.mark.parametrize("event,market,runner,withdrawn,expected", [
        ("open", "open", "open", False, ("open", True)),
        ("open", "suspended", "open", False, ("open", False)),
        ("open", "open", "suspended", False, ("open", False)),
        ("closed", "closed", "closed", False, ("closed", False)),
        ("graded", "closed", "closed", False, ("settled", False)),
        ("open", "open", "paid", True, ("settled", False)),
        ("open", "open", "closed", True, ("settled", False)),
    ])
    def test_status(self, event, market, runner, withdrawn, expected):
        assert status_of(event, market, runner, withdrawn) == expected

    def test_a_withdrawn_runner_is_settled_void(self):
        withdrawn = normalize_market(RACE, WIN, WIN["runners"][-1])
        assert withdrawn.status == "settled" and not withdrawn.active and withdrawn.info["result"] == "void"
        assert normalize_market(RACE, WIN, WIN["runners"][0]).status == "open"

    def test_a_closed_market_is_closed_with_no_result(self):
        market = normalize_market(CLOSED_EVENT, CLOSED_MARKET, CLOSED_RUNNER)
        assert market.status == "closed" and market.info["result"] is None


class TestAdapter:
    def test_listing_reads_events_with_their_markets_and_prices_in_one_request(self):
        mb, api = venue()
        page = mb.fetch_markets(limit=5)
        assert api.paths() == ["/edge/rest/events"]
        params = api.requests[0].url.params
        assert params["states"] == "open,suspended" and params["include-prices"] == "true"
        assert params["odds-type"] == "DECIMAL" and params["exchange-type"] == "back-lay"
        assert params["price-depth"] == "3" and "after" not in params
        # The race starts first; its withdrawn runner is left out.
        assert [m.outcome_label for m in page] == ["14 Eichan San", "3 Mehman", "11 Iff In Doubt", "5 Faoladh",
                                                   "Arsenal"]

    def test_every_request_carries_a_real_user_agent_and_no_session(self):
        mb, api = venue()
        mb.fetch_markets(limit=2)
        for request in api.requests:
            assert request.headers["User-Agent"].startswith("synpath/")
            assert "session-token" not in request.headers

    def test_the_cursor_resumes_mid_market_without_repeats_or_gaps(self):
        mb, _ = venue()
        whole = [m.id for m in mb.fetch_markets(limit=200)]
        assert len(whole) == 4 + 11 + 5
        seen, cursor = [], None
        for size in (3, 2, 4, 1, 7, 50):
            page = mb.fetch_markets(limit=size, cursor=cursor)
            seen += [m.id for m in page]
            cursor = page.next_cursor
            if cursor is None:
                break
        assert seen == whole

    def test_pages_of_events_anchor_on_start_times_and_page_through_ties(self, monkeypatch):
        """Two fixtures kick off at the same second. With two events a
        request, the second page anchors on the kick-off and skips the one
        fixture at it already read."""
        monkeypatch.setattr("synpath.matchbook.EVENT_PAGE", 2)
        mb, api = venue()
        whole = [m.id for m in mb.fetch_markets(limit=200)]
        assert len(whole) == 20 and len(set(whole)) == 20
        kick_off = str(start_s(ARSENAL_EVERTON))
        assert start_s(ARSENAL_EVERTON) == start_s(COVENTRY_FULHAM)
        assert [(r.url.params.get("after"), r.url.params["offset"]) for r in api.requests] == [(None, "0"), (kick_off, "1")]

    def test_a_cursor_names_the_events_own_start(self):
        mb, _ = venue()
        page = mb.fetch_markets(limit=5)
        assert page.next_cursor == f"{start_s(ARSENAL_EVERTON)}|0|0|1"

    def test_the_end_of_the_listing_has_no_cursor(self):
        mb, _ = venue()
        assert mb.fetch_markets(limit=200).next_cursor is None

    def test_sport_names_are_resolved_through_the_navigation_group(self):
        mb, api = venue()
        page = mb.fetch_markets(sport="soccer", limit=100)
        assert api.paths() == ["/edge/rest/lookups/sports", "/edge/rest/events"]
        assert api.requests[1].url.params["sport-ids"] == "15"
        assert {m.category for m in page} == {"soccer"}
        with pytest.raises(BadRequest, match="no sport named Curling"):
            mb.fetch_markets(sport="Curling")

    def test_market_types_filter_the_markets(self):
        mb, _ = venue()
        page = mb.fetch_markets(market_type="total", limit=100)
        assert {m.info["market_type"] for m in page} == {"total"} and len(page) == 4

    def test_search_sort_and_closed_listings_are_refused(self):
        mb, api = venue()
        with pytest.raises(NotSupported, match="no text search"):
            mb.fetch_markets(query="arsenal")
        with pytest.raises(NotSupported):
            mb.fetch_markets(sort="volume")
        with pytest.raises(NotSupported, match="open events only"):
            mb.fetch_markets(status="settled")
        with pytest.raises(BadRequest):
            mb.fetch_markets(cursor="nonsense")
        assert api.requests == []

    def test_fetch_events_groups_runners_under_their_market(self):
        mb, _ = venue()
        page = mb.fetch_events(limit=3)
        assert [e.title for e in page] == ["17:45 Dundalk — WIN", "Arsenal vs Everton — Match Odds",
                                           "Arsenal vs Everton — Total"]
        assert len(page[1].markets) == 3 and page.next_cursor == f"{start_s(ARSENAL_EVERTON)}|0|2|0"
        rest = mb.fetch_events(limit=50, cursor=page.next_cursor)
        assert rest[0].title == "Arsenal vs Everton — Handicap" and len(page) + len(rest) == 8

    def test_fetch_event_reads_an_open_market(self):
        mb, api = venue()
        event = mb.fetch_event(MATCH_ODDS_ID)
        assert event.id == MATCH_ODDS_ID and len(event.markets) == 3
        assert api.paths() == ["/edge/rest/events"] and api.requests[0].url.params["ids"] == "34569494493900023"

    def test_fetch_event_reads_a_closed_market_and_its_runners(self):
        mb, api = venue()
        event = mb.fetch_event(f"matchbook:{CLOSED_EVENT['id']}-{CLOSED_MARKET['id']}")
        assert event.status == "closed" and len(event.markets) == 3
        assert api.paths()[-1].endswith("/runners")
        assert api.requests[-1].url.params["states"] == "open,suspended,closed,paid"

    def test_fetch_markets_by_ids_keeps_the_order_and_drops_the_unknown(self):
        mb, api = venue()
        handicap = f"matchbook:{ARSENAL_EVERTON['id']}-{WHOLE_HANDICAP['id']}-{WHOLE_HANDICAP['runners'][1]['id']}"
        found = mb.fetch_markets_by_ids([CLOSED_ID, handicap, ARSENAL_ID, "matchbook:1-2-3",
                                         f"matchbook:{ARSENAL_EVERTON['id']}-{MATCH_ODDS['id']}-1"])
        assert [m.id for m in found] == [CLOSED_ID, handicap, ARSENAL_ID]
        assert found[0].status == "closed" and found[1].market_type == "unknown"
        assert api.paths()[0] == "/edge/rest/events"
        assert api.requests[0].url.params["states"] == "open,suspended,closed,graded"

    def test_fetch_market_not_found(self):
        mb, _ = venue()
        with pytest.raises(MarketNotFound):
            mb.fetch_market("matchbook:1-2-3")

    def test_fetch_order_book_reads_the_runners_prices_alone(self):
        mb, api = venue()
        book = mb.fetch_order_book(ARSENAL_ID, depth=5)
        assert book.market_id == ARSENAL_ID and len(book.bids) == 5
        assert api.paths() == ["/edge/rest/events/34569494493900023/markets/34569495835700023/runners/"
                               "34569495836701023/prices"]
        assert api.requests[0].url.params["depth"] == "5"
        with pytest.raises(MarketNotFound):
            mb.fetch_order_book("matchbook:1-2-3")

    def test_fetch_order_books_for_runners_and_whole_markets(self):
        mb, api = venue()
        books = mb.fetch_order_books([ARSENAL_ID, f"matchbook:{RACE['id']}-{WIN['id']}"])
        assert api.paths() == ["/edge/rest/events"]
        assert ARSENAL_ID in books and len(books) == 1 + len(WIN["runners"])
        assert books[ARSENAL_ID].info["status"] == "open"

    def test_a_reason_from_matchbook_reaches_the_error(self):
        refused = {"errors": [{"messages": ["Invalid sport-ids."], "codes": [None]}]}
        mb, _ = venue(Api({r"/edge/rest/events": httpx.Response(400, json=refused)}))
        with pytest.raises(BadRequest, match="matchbook: Invalid sport-ids"):
            mb.fetch_markets(sport="15")
        with pytest.raises(MarketNotFound):
            venue()[0].fetch_event("matchbook:1-2")

    def test_no_trades_and_no_candles(self):
        mb, api = venue()
        with pytest.raises(NotSupported, match="no individual trades"):
            mb.fetch_trades(ARSENAL_ID)
        with pytest.raises(NotSupported):
            mb.fetch_ohlcv(ARSENAL_ID)
        assert api.requests == []

    def test_sports(self):
        mb, _ = venue()
        assert [s["name"] for s in mb.fetch_sports()] == ["Horse Racing", "Soccer", "Tennis"]

    def test_a_currency_asked_for_is_sent(self):
        mb, api = venue(currency="EUR")
        mb.fetch_markets(limit=1)
        assert api.requests[0].url.params["currency"] == "EUR"


class TestFees:
    def test_commission_on_net_market_winnings(self):
        fee = fee_schedule_of("34569494493900023-34569495835700023")
        assert fee.fee_type == "net_market_winnings" and fee.taker_rate == fee.maker_rate == DEFAULT_COMMISSION == 0.02
        assert fee.estimate(price=0.5, contracts=100) == pytest.approx(1.0)

    def test_the_adapter_names_the_matchbook_market_and_reads_nothing(self):
        mb, api = venue(commission=0.04)
        fee = mb.fetch_fee_schedule(ARSENAL_ID)
        assert fee.scope_id == "34569494493900023-34569495835700023" and fee.taker_rate == 0.04
        assert api.requests == []


class TestSession:
    def test_login_posts_json_and_the_token_rides_as_session_token(self):
        api = Api()
        mb, _ = venue(api, session=session(api))
        mb.fetch_order_book(ARSENAL_ID)
        login, prices = api.requests
        assert login.method == "POST" and login.url.path == "/bpapi/rest/security/session"
        assert json.loads(login.content) == {"username": "jbloggs", "password": "hunter2"}
        assert prices.headers["session-token"] == LOGIN["session-token"]

    def test_an_old_session_is_replaced(self):
        api, now = Api(), [0.0]
        s = session(api, clock=lambda: now[0])
        s.token()
        now[0] += SESSION_REFRESH_AFTER - 1
        s.token()
        now[0] += 2
        s.token()
        assert api.paths() == ["/bpapi/rest/security/session"] * 2
        assert s.account["currency"] == "GBP"

    def test_a_refused_token_logs_in_again_and_retries_once(self):
        seen: list[str] = []

        def prices(request):
            seen.append(request.headers.get("session-token", ""))
            return httpx.Response(401, json={"errors": [{"messages": ["Please login to continue"]}]}) \
                if len(seen) == 1 else httpx.Response(200, json=PRICES)

        api = Api({r"/edge/rest/events/\d+/markets/\d+/runners/\d+/prices": prices})
        mb, _ = venue(api, session=session(api, session_token="OLD"))
        mb.fetch_order_book(ARSENAL_ID)
        assert seen == ["OLD", LOGIN["session-token"]]

    def test_two_factor_login_is_refused_clearly(self):
        api = Api({r"/bpapi/rest/security/session": httpx.Response(400, json=LOGIN_MFA)})
        with pytest.raises(AuthenticationError, match="two-factor"):
            session(api).token()

    def test_bad_credentials_say_why(self):
        api = Api({r"/bpapi/rest/security/session": httpx.Response(400, json=LOGIN_REFUSED)})
        with pytest.raises(AuthenticationError, match="Username or password are incorrect"):
            session(api).token()

    def test_the_password_and_token_never_show(self):
        s = session(Api())
        s.token()
        assert "hunter2" not in repr(s) and LOGIN["session-token"] not in repr(s)


class TestCredentials:
    def test_loading(self):
        assert load_matchbook({}) is None
        creds = load_matchbook({"MATCHBOOK_USERNAME": "jbloggs", "MATCHBOOK_PASSWORD": "pw-123456"})
        assert creds.username == "jbloggs" and "pw-123456" not in repr(creds) and creds.secrets == ["pw-123456"]
        with pytest.raises(CredentialsMissing, match="both"):
            load_matchbook({"MATCHBOOK_USERNAME": "jbloggs"})

    def test_the_read_adapter_logs_in_only_when_asked(self, monkeypatch):
        monkeypatch.setenv("MATCHBOOK_USERNAME", "jbloggs")
        monkeypatch.setenv("MATCHBOOK_PASSWORD", "pw")
        assert Matchbook(limiter=None).session is None
        assert Matchbook(limiter=None, login=True).session is not None
        assert Matchbook(limiter=None, session_token="tok").session is not None


class TestCapabilities:
    def test_declared_honestly(self):
        has = Matchbook.has
        assert has["fetch_markets"] is True and has["fetch_order_books"] is True
        assert has["fetch_trades"] is False and has["fetch_ohlcv"] is False
        assert has["search"] is False and has["sort"] is False

    def test_order_entry_is_offered(self):
        from synpath.trading.matchbook import MatchbookTrading

        assert synpath.MatchbookTrading is MatchbookTrading
        with pytest.raises(CredentialsMissing, match="MATCHBOOK_USERNAME"):
            synpath.Client().trading("matchbook")
