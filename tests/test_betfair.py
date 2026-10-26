"""Betfair: login, catalog mapping, back/lay read on YES, weight batching, fees.

Betfair serves nothing without an account, so these samples are hand-built
to the shapes in Betfair's Betting API and identity documentation (and
betfairlightweight's fixtures), not recorded. Every request goes to an
`httpx.MockTransport`; nothing here reaches Betfair.
"""
from __future__ import annotations

import json
from urllib.parse import parse_qs

import httpx
import pytest

import synpath
from synpath import AuthenticationError, BadRequest, MarketNotFound, NotSupported
from synpath.betfair import (
    CLASSIC_LADDER, Betfair, BetfairSession, SESSION_REFRESH_AFTER, fee_schedule_of, markets_per_request,
    normalize_event, normalize_market, normalize_order_book, on_ladder, parse_runner_id, request_weight,
    runner_native_id, status_of,
)
from synpath.trading.credentials import CredentialsMissing, load_betfair

from conftest import load

CATALOGUE = load("betfair_catalogue.json")
BOOKS = load("betfair_book.json")
CLOSED = load("betfair_book_closed.json")
AUTH = load("betfair_auth.json")
EVENT_TYPES = load("betfair_event_types.json")

MATCH_ODDS, TOTALS, HANDICAP = CATALOGUE
MATCH_BOOK, TOTALS_BOOK, HANDICAP_BOOK = BOOKS
ARSENAL = "betfair:1.234567890-1096"


@pytest.fixture(autouse=True)
def no_ambient_credentials(monkeypatch, tmp_path):
    """No test may pick up a real account from the environment or a `.env`."""
    for name in ("BETFAIR_APP_KEY", "BETFAIR_USERNAME", "BETFAIR_PASSWORD", "BETFAIR_CERT_PATH",
                 "BETFAIR_KEY_PATH", "BETFAIR_JURISDICTION"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)


class Identity:
    """Betfair's login host: answers login, certlogin and keepAlive."""

    def __init__(self, *, login=None, certlogin=None, keepalive=None):
        self.calls: list[httpx.Request] = []
        self.answers = {
            "/api/login": login or AUTH["login_success"],
            "/api/certlogin": certlogin or AUTH["certlogin_success"],
            "/api/keepAlive": keepalive or AUTH["keepalive_success"],
        }

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        return httpx.Response(200, json=self.answers[request.url.path])

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self.handler))

    def paths(self) -> list[str]:
        return [request.url.path for request in self.calls]


class Betting:
    """The Betting API: answers each operation from samples, and records requests."""

    def __init__(self, answers):
        self.answers = answers
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        operation = request.url.path.rstrip("/").rsplit("/", 1)[-1]
        answer = self.answers[operation]
        if callable(answer):
            answer = answer(json.loads(request.content))
        if isinstance(answer, httpx.Response):
            return answer
        return httpx.Response(200, json=answer)

    def bodies(self, operation: str) -> list[dict]:
        return [json.loads(r.content) for r in self.requests if r.url.path.rstrip("/").endswith(operation)]


def session(identity: Identity | None = None, **kwargs) -> BetfairSession:
    return BetfairSession(app_key="APP-KEY-DELAYED", username="punter", password="hunter2",
                          client=(identity or Identity()).client(), **kwargs)


def books_for(body):
    return [b for b in BOOKS + CLOSED if b["marketId"] in body["marketIds"]]


def catalogue_for(body):
    ids = body["filter"].get("marketIds")
    return [row for row in CATALOGUE if ids is None or row["marketId"] in ids]


def venue(answers=None, *, identity: Identity | None = None, **kwargs) -> tuple[Betfair, Betting]:
    betting = Betting({"listMarketCatalogue": catalogue_for, "listMarketBook": books_for,
                       "listEventTypes": EVENT_TYPES, "listEvents": load("betfair_events.json"), **(answers or {})})
    adapter = Betfair(session=session(identity), limiter=None,
                      client=httpx.Client(transport=httpx.MockTransport(betting.handler)), **kwargs)
    return adapter, betting


class TestSession:
    def test_interactive_login_posts_the_form_with_the_app_key(self):
        identity = Identity()
        token = session(identity).token()
        assert token == "SESSION-TOKEN-1"
        login = identity.calls[0]
        assert login.url == "https://identitysso.betfair.com/api/login"
        assert login.headers["X-Application"] == "APP-KEY-DELAYED"
        assert login.headers["Accept"] == "application/json"
        assert parse_qs(login.content.decode()) == {"username": ["punter"], "password": ["hunter2"]}

    def test_certificate_login_uses_the_cert_host(self):
        identity = Identity()
        token = session(identity, cert_path="client.crt", key_path="client.key").token()
        assert token == "CERT-SESSION-TOKEN-1"
        assert identity.calls[0].url == "https://identitysso-cert.betfair.com/api/certlogin"

    def test_the_jurisdiction_picks_the_login_host(self):
        identity = Identity()
        session(identity, jurisdiction="it").token()
        assert identity.calls[0].url.host == "identitysso.betfair.it"
        with pytest.raises(BadRequest, match="jurisdiction"):
            session(jurisdiction="us")

    def test_the_token_is_cached_in_memory_and_never_shown(self):
        identity = Identity()
        s = session(identity)
        assert s.token() == s.token()
        assert identity.paths() == ["/api/login"]
        assert "SESSION-TOKEN" not in repr(s) and "hunter2" not in repr(s) and "APP-KEY" not in repr(s)

    def test_an_old_session_is_extended_with_keep_alive(self):
        now = [0.0]
        identity = Identity()
        s = session(identity, clock=lambda: now[0])
        s.token()
        now[0] = SESSION_REFRESH_AFTER + 1
        assert s.token() == "SESSION-TOKEN-1"
        assert identity.paths() == ["/api/login", "/api/keepAlive"]
        keep_alive = identity.calls[1]
        assert keep_alive.headers["X-Authentication"] == "SESSION-TOKEN-1"
        assert keep_alive.headers["X-Application"] == "APP-KEY-DELAYED"

    def test_a_refused_keep_alive_logs_in_again(self):
        now = [0.0]
        identity = Identity(keepalive=AUTH["keepalive_failure"])
        s = session(identity, clock=lambda: now[0])
        s.token()
        now[0] = SESSION_REFRESH_AFTER + 1
        s.token()
        assert identity.paths() == ["/api/login", "/api/keepAlive", "/api/login"]

    def test_a_bad_login_is_an_authentication_error_with_betfairs_reason(self):
        with pytest.raises(AuthenticationError, match="INVALID_USERNAME_OR_PASSWORD"):
            session(Identity(login=AUTH["login_failure"])).token()
        with pytest.raises(AuthenticationError, match="CERT_AUTH_REQUIRED"):
            session(Identity(certlogin=AUTH["certlogin_failure"]), cert_path="c", key_path="k").token()

    def test_no_login_details_says_what_to_set(self):
        s = BetfairSession(app_key="k", client=Identity().client())
        with pytest.raises(AuthenticationError, match="BETFAIR_USERNAME"):
            s.token()

    def test_a_given_session_token_is_used_without_logging_in(self):
        identity = Identity()
        s = BetfairSession(app_key="k", session_token="GIVEN", client=identity.client())
        assert s.token() == "GIVEN" and identity.calls == []

    def test_every_betting_call_carries_the_three_headers(self):
        adapter, betting = venue()
        adapter.fetch_event_types()
        headers = betting.requests[0].headers
        assert headers["X-Application"] == "APP-KEY-DELAYED"
        assert headers["X-Authentication"] == "SESSION-TOKEN-1"
        assert headers["Accept"] == "application/json"
        assert betting.requests[0].url == "https://api.betfair.com/exchange/betting/rest/v1.0/listEventTypes/"

    def test_an_expired_session_logs_in_again_and_retries_once(self):
        answers = iter([httpx.Response(400, json=AUTH["invalid_session"]), httpx.Response(200, json=EVENT_TYPES)])
        identity = Identity()
        adapter, betting = venue({"listEventTypes": lambda body: next(answers)}, identity=identity)
        assert adapter.fetch_event_types() == EVENT_TYPES
        assert identity.paths() == ["/api/login", "/api/login"]
        assert len(betting.requests) == 2

    def test_betfairs_error_code_reaches_the_error(self):
        adapter, _ = venue({"listEventTypes": httpx.Response(400, json=AUTH["invalid_app_key"])})
        with pytest.raises(AuthenticationError, match="INVALID_APP_KEY"):
            adapter.fetch_event_types()
        adapter, _ = venue({"listEventTypes": httpx.Response(400, json=AUTH["too_much_data"])})
        with pytest.raises(BadRequest, match="TOO_MUCH_DATA"):
            adapter.fetch_event_types()

    def test_without_an_account_the_adapter_builds_and_the_first_call_says_what_to_set(self):
        adapter = Betfair(limiter=None, client=httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(200))))
        assert adapter.session is None
        with pytest.raises(AuthenticationError, match="BETFAIR_APP_KEY"):
            adapter.fetch_event_types()


class TestCredentials:
    def test_loaded_from_the_environment(self):
        creds = load_betfair({"BETFAIR_APP_KEY": "k", "BETFAIR_USERNAME": "u", "BETFAIR_PASSWORD": "p",
                              "BETFAIR_JURISDICTION": ".com.au"})
        assert creds.jurisdiction == "com.au" and creds.cert_path is None
        assert "k" not in repr(creds).replace("key", "") and "p'" not in repr(creds)
        assert set(creds.secrets) == {"k", "p"}

    def test_absent_and_incomplete(self, tmp_path):
        assert load_betfair({}) is None
        with pytest.raises(CredentialsMissing, match="BETFAIR_USERNAME"):
            load_betfair({"BETFAIR_APP_KEY": "k"})
        with pytest.raises(CredentialsMissing, match="both"):
            load_betfair({"BETFAIR_APP_KEY": "k", "BETFAIR_USERNAME": "u", "BETFAIR_PASSWORD": "p",
                          "BETFAIR_CERT_PATH": str(tmp_path)})

    def test_the_adapter_reads_the_environment(self, monkeypatch):
        monkeypatch.setenv("BETFAIR_APP_KEY", "env-key")
        monkeypatch.setenv("BETFAIR_USERNAME", "u")
        monkeypatch.setenv("BETFAIR_PASSWORD", "p")
        adapter = Betfair(limiter=None)
        assert adapter.session is not None and adapter.session.app_key == "env-key"


class TestIds:
    @pytest.mark.parametrize("native, parsed", [
        ("1.234567890-47972", ("1.234567890", 47972, 0.0)),
        ("1.234567892-2426--0.5", ("1.234567892", 2426, -0.5)),
        ("1.234567892-48351-0.5", ("1.234567892", 48351, 0.5)),
        ("1.2-7-2.25", ("1.2", 7, 2.25)),
    ])
    def test_round_trip(self, native, parsed):
        assert parse_runner_id(native) == parsed
        assert runner_native_id(*parsed) == native

    def test_a_zero_handicap_is_left_out(self):
        assert runner_native_id("1.234567890", 47972, 0.0) == "1.234567890-47972"

    def test_a_market_id_is_not_a_runner(self):
        with pytest.raises(BadRequest, match="names an event"):
            parse_runner_id("1.234567890")

    def test_synpath_ids_route_to_betfair(self):
        assert synpath.ids.split("betfair:1.234567890-47972") == ("betfair", "1.234567890-47972")
        assert "betfair" in synpath.ids.VENUES and synpath.exchanges["betfair"] is Betfair


class TestCatalogMapping:
    def test_each_runner_is_a_market_and_the_betfair_market_its_event(self):
        market = normalize_market(MATCH_ODDS, MATCH_ODDS["runners"][0], MATCH_BOOK)
        assert market.id == ARSENAL and market.venue_market_id == "1.234567890-1096"
        assert market.event_id == "betfair:1.234567890"
        assert market.title == "Arsenal v Chelsea — Match Odds — Arsenal"
        assert market.outcome_label == "Arsenal" and market.yes.label == "Arsenal" and market.no.label == "Not Arsenal"
        assert market.info["betfair_event"]["id"] == "34812345"
        assert market.tags == ["soccer", "english premier league"] and market.category == "soccer"
        assert market.book_model == "shared_complement" and market.tick_size is None
        assert market.market_type == "binary" and market.neg_risk is False
        assert market.close_timestamp == 1792940400000 and market.close_datetime == "2026-10-25T15:00:00Z"

    def test_the_event_holds_every_runner(self):
        event = normalize_event(MATCH_ODDS, MATCH_BOOK)
        assert event.id == "betfair:1.234567890" and event.title == "Arsenal v Chelsea — Match Odds"
        assert [m.outcome_label for m in event.markets] == ["Arsenal", "Chelsea", "The Draw"]
        assert event.mutually_exclusive is True and event.status == "open" and event.native_status == "OPEN"
        assert event.info["market_base_rate"] == 5.0 and event.info["delayed"] is True

    def test_a_handicap_runner_carries_its_line_and_is_not_a_plain_binary(self):
        market = normalize_market(HANDICAP, HANDICAP["runners"][0], HANDICAP_BOOK)
        assert market.id == "betfair:1.234567892-2426--0.5"
        assert market.market_type == "unknown"
        assert normalize_event(HANDICAP, HANDICAP_BOOK).mutually_exclusive is None

    def test_quotes_come_from_the_best_offers(self):
        market = normalize_market(MATCH_ODDS, MATCH_ODDS["runners"][0], MATCH_BOOK)
        assert market.yes.quote.bid == pytest.approx(1 / 2.1) and market.yes.quote.ask == pytest.approx(1 / 2.08)
        assert market.yes.quote.last is None and market.info["last_traded_odds"] == 2.1
        assert market.no.quote.bid == pytest.approx(1 - 1 / 2.08)

    def test_serializes(self):
        market = normalize_market(MATCH_ODDS, MATCH_ODDS["runners"][0], MATCH_BOOK)
        assert market.model_validate_json(market.model_dump_json()) == market


class TestBackLayAsYes:
    """A back offer at O is liquidity a YES buyer takes: an ask at 1/O."""

    def book(self, side="yes"):
        runner = MATCH_BOOK["runners"][0]
        return normalize_order_book(MATCH_BOOK, runner, market_id=ARSENAL, side=side)

    def test_available_to_back_are_asks_and_available_to_lay_bids(self):
        book = self.book()
        assert [level.price for level in book.asks] == [round(1 / o, 10) for o in (2.08, 2.06, 2.04)]
        assert [level.price for level in book.bids] == [round(1 / o, 10) for o in (2.1, 2.12, 2.14)]

    def test_the_book_is_not_crossed(self):
        """Best back (2.08) sits below best lay (2.10), so the YES bid sits below the ask."""
        book = self.book()
        assert book.best_bid.price < book.best_ask.price
        assert book.bids == sorted(book.bids, key=lambda l: -l.price)
        assert book.asks == sorted(book.asks, key=lambda l: l.price)

    def test_sizes_are_stake_times_odds(self):
        book = self.book()
        assert book.best_ask.size == pytest.approx(1520.4 * 2.08)
        assert book.best_bid.size == pytest.approx(640.25 * 2.1)
        # The cost of the contracts is the stake: price times contracts.
        assert book.best_ask.price * book.best_ask.size == pytest.approx(1520.4)

    def test_the_venues_odds_and_stakes_are_kept(self):
        book = self.book()
        assert book.info["back"][0] == [2.08, 1520.4] and book.info["lay"][0] == [2.1, 640.25]
        assert book.info["currency"] == "account" and book.info["delayed"] is True
        assert all(on_ladder(odds) for odds, _ in book.info["back"] + book.info["lay"])

    def test_the_no_side_is_the_mirror(self):
        no = self.book("no")
        assert no.derived and no.best_bid.price == pytest.approx(1 - 1 / 2.08)
        assert no.best_ask.price == pytest.approx(1 - 1 / 2.1)

    def test_an_empty_runner_has_an_empty_book(self):
        runner = MATCH_BOOK["runners"][2]
        book = normalize_order_book(MATCH_BOOK, runner, market_id="betfair:1.234567890-58805")
        assert book.bids == [] and book.asks == [] and book.best_bid is None


class TestLadder:
    def test_classic_ladder(self):
        assert str(CLASSIC_LADDER[0]) == "1.01"
        assert str(CLASSIC_LADDER[-1]) == "1000" and len(CLASSIC_LADDER) == 350
        assert on_ladder(2.02) and on_ladder(3.05) and on_ladder(4.1) and on_ladder(19.5) and on_ladder(110)
        assert not on_ladder(2.01) and not on_ladder(3.02) and not on_ladder(1001) and not on_ladder(1.0)


class TestWeights:
    def test_projection_weights(self):
        assert request_weight(("EX_BEST_OFFERS",), book=True) == 5
        assert request_weight((), book=True) == 2
        assert request_weight(("EX_ALL_OFFERS",), book=True) == 17
        assert request_weight(("EX_BEST_OFFERS", "EX_TRADED"), book=True) == 20
        assert request_weight(("MARKET_DESCRIPTION", "RUNNER_METADATA", "EVENT"), book=False) == 2
        assert markets_per_request(5) == 40 and markets_per_request(2) == 100 and markets_per_request(0) == 1000

    def test_books_are_batched_so_no_request_passes_200(self):
        ids = [f"betfair:1.{n}-1" for n in range(95)]
        adapter, betting = venue({"listMarketBook": []})
        assert adapter.fetch_order_books(ids) == {}
        sizes = [len(body["marketIds"]) for body in betting.bodies("listMarketBook")]
        assert sizes == [40, 40, 15]
        for body in betting.bodies("listMarketBook"):
            assert body["priceProjection"] == {"priceData": ["EX_BEST_OFFERS"], "virtualise": True}
            assert len(body["marketIds"]) * 5 <= 200

    def test_many_runners_of_one_market_are_one_market_in_the_request(self):
        adapter, betting = venue()
        books = adapter.fetch_order_books([ARSENAL, "betfair:1.234567890-55190", "betfair:1.234567891-47972"])
        assert list(books) == [ARSENAL, "betfair:1.234567890-55190", "betfair:1.234567891-47972"]
        assert betting.bodies("listMarketBook")[0]["marketIds"] == ["1.234567890", "1.234567891"]

    def test_lookups_are_batched_by_catalogue_weight(self):
        ids = [f"betfair:1.{n}-1" for n in range(150)]
        adapter, betting = venue({"listMarketCatalogue": [], "listMarketBook": []})
        adapter.fetch_markets_by_ids(ids)
        assert [len(b["filter"]["marketIds"]) for b in betting.bodies("listMarketCatalogue")] == [100, 50]
        assert all(len(b["marketIds"]) <= 40 for b in betting.bodies("listMarketBook"))


class TestAdapter:
    def test_fetch_markets_lists_runners_with_their_books(self):
        adapter, betting = venue()
        page = adapter.fetch_markets(limit=4)
        assert [m.id for m in page] == [ARSENAL, "betfair:1.234567890-55190", "betfair:1.234567890-58805",
                                         "betfair:1.234567891-47972"]
        assert page.next_cursor == "2026-10-25T15:00:00.000Z|1|1"
        body = betting.bodies("listMarketCatalogue")[0]
        assert body["sort"] == "FIRST_TO_START" and body["maxResults"] == 200
        assert "MARKET_DESCRIPTION" in body["marketProjection"]
        assert page[3].status == "open" and page[3].active is False  # suspended in play

    def test_the_cursor_resumes_mid_market(self):
        def catalogue(body):
            start = (body["filter"].get("marketStartTime") or {}).get("from")
            return [row for row in CATALOGUE if not start or row["marketStartTime"] >= start]

        adapter, betting = venue({"listMarketCatalogue": catalogue})
        first = adapter.fetch_markets(limit=4)
        second = adapter.fetch_markets(limit=4, cursor=first.next_cursor)
        assert [m.id for m in second] == ["betfair:1.234567891-47973", "betfair:1.234567892-2426--0.5",
                                          "betfair:1.234567892-48351-0.5"]
        assert second.next_cursor is None
        assert betting.bodies("listMarketCatalogue")[1]["filter"]["marketStartTime"] == {"from": "2026-10-25T15:00:00.000Z"}

    def test_filters_reach_the_venue_and_event_type_names_are_resolved(self):
        adapter, betting = venue()
        adapter.fetch_markets(query="Arsenal", event_type="soccer", competition="10932509", market_type="MATCH_ODDS")
        market_filter = betting.bodies("listMarketCatalogue")[0]["filter"]
        assert market_filter == {"textQuery": "Arsenal", "eventTypeIds": ["1"], "competitionIds": ["10932509"],
                                 "marketTypeCodes": ["MATCH_ODDS"]}
        with pytest.raises(BadRequest, match="no event type named Curling"):
            adapter.fetch_markets(event_type="Curling")

    def test_sorting_is_at_the_venue_and_one_page(self):
        adapter, betting = venue()
        page = adapter.fetch_markets(sort="volume", limit=2)
        assert len(page) == 2 and page.next_cursor is None
        assert betting.bodies("listMarketCatalogue")[0]["sort"] == "MAXIMUM_TRADED"
        with pytest.raises(NotSupported):
            adapter.fetch_markets(sort="newest")

    def test_only_open_markets_are_listed(self):
        adapter, _ = venue()
        for status in ("closed", "settled", "all"):
            with pytest.raises(NotSupported):
                adapter.fetch_markets(status=status)

    def test_fetch_events_pages_betfair_markets(self):
        adapter, _ = venue()
        page = adapter.fetch_events(limit=2)
        assert [e.id for e in page] == ["betfair:1.234567890", "betfair:1.234567891"]
        assert page.next_cursor == "2026-10-25T15:00:00.000Z|2|0"
        assert page[1].native_status == "SUSPENDED" and page[1].info["in_play"] is True

    def test_fetch_market_reads_a_closed_market_from_its_book(self):
        adapter, _ = venue({"listMarketCatalogue": []})
        winner = adapter.fetch_market("betfair:1.230000001-101")
        assert winner.status == "settled" and winner.info["result"] == "yes"
        assert adapter.fetch_market("1.230000001-102").info["result"] == "no"
        removed = adapter.fetch_market("betfair:1.230000001-103")
        assert removed.status == "settled" and removed.info["result"] == "void"
        with pytest.raises(MarketNotFound):
            adapter.fetch_market("betfair:1.230000001-999")

    def test_fetch_markets_by_ids_keeps_the_order_and_drops_the_unknown(self):
        adapter, _ = venue()
        found = adapter.fetch_markets_by_ids(["betfair:1.234567891-47973", ARSENAL, "betfair:1.999-1"])
        assert [m.id for m in found] == ["betfair:1.234567891-47973", ARSENAL]

    def test_fetch_order_book(self):
        adapter, _ = venue()
        book = adapter.fetch_order_book(ARSENAL, depth=1)
        assert len(book.bids) == len(book.asks) == 1 and book.info["delayed"] is True
        with pytest.raises(MarketNotFound):
            adapter.fetch_order_book("betfair:1.234567890-424242")

    def test_the_raw_lists_and_helpers(self):
        adapter, _ = venue()
        assert adapter.fetch_event_types()[0]["eventType"]["name"] == "Soccer"
        assert adapter.fetch_betfair_events(event_type="1")[0]["event"]["name"] == "Arsenal v Chelsea"

    def test_a_currency_is_asked_for_and_recorded(self):
        adapter, betting = venue(currency="EUR")
        book = adapter.fetch_order_book(ARSENAL)
        assert betting.bodies("listMarketBook")[0]["currencyCode"] == "EUR" and book.info["currency"] == "EUR"


class TestStatus:
    @pytest.mark.parametrize("market, runner, expected", [
        ("OPEN", "ACTIVE", ("open", True)),
        ("SUSPENDED", "ACTIVE", ("open", False)),
        ("INACTIVE", "ACTIVE", ("unopened", False)),
        ("CLOSED", "ACTIVE", ("closed", False)),
        ("CLOSED", "WINNER", ("settled", False)),
        ("CLOSED", "LOSER", ("settled", False)),
        ("OPEN", "REMOVED", ("settled", False)),
        ("OPEN", "HIDDEN", ("open", False)),
        (None, None, ("open", False)),
    ])
    def test_status(self, market, runner, expected):
        assert status_of(market, runner) == expected

    def test_results_come_from_runner_status(self):
        book = CLOSED[0]
        results = [normalize_market({"marketId": book["marketId"], "runners": [r]}, r, book).info["result"]
                   for r in book["runners"]]
        assert results == ["yes", "no", "void", "no"]


class TestDelayed:
    def test_the_response_flag_wins(self):
        book = {**MATCH_BOOK, "isMarketDataDelayed": False}
        assert normalize_order_book(book, book["runners"][0], market_id=ARSENAL).info["delayed"] is False

    def test_without_the_flag_the_adapters_setting_is_used(self):
        book = {k: v for k, v in MATCH_BOOK.items() if k != "isMarketDataDelayed"}
        assert normalize_order_book(book, book["runners"][0], market_id=ARSENAL).info["delayed"] is True
        assert normalize_order_book(book, book["runners"][0], market_id=ARSENAL, delayed=False).info["delayed"] is False


class TestFees:
    def test_commission_on_winnings_at_the_market_base_rate(self):
        fee = fee_schedule_of("1.234567891", 2.0)
        assert fee.fee_type == "net_market_winnings" and fee.taker_rate == fee.maker_rate == 0.02
        # 100 contracts bought at 0.4 win 60 if they win: 2% of 60.
        assert fee.estimate(0.4, 100) == pytest.approx(1.2)
        assert fee.estimate(0.4, 100, taker=False) == pytest.approx(1.2)

    def test_the_default_when_the_rate_is_unread(self):
        assert fee_schedule_of("1.1").taker_rate == 0.05
        assert fee_schedule_of("1.1", default=0.03).info["base_rate_source"] == "default"

    def test_the_adapter_reads_the_markets_rate(self):
        adapter, betting = venue()
        assert adapter.fetch_fee_schedule("betfair:1.234567891-47972").taker_rate == 0.02
        assert adapter.fetch_fee_schedule("betfair:1.234567890").taker_rate == 0.05
        assert betting.bodies("listMarketCatalogue")[0]["marketProjection"] == ["MARKET_DESCRIPTION"]


class TestCapabilities:
    def test_declared_honestly(self):
        has = Betfair.has
        assert has["fetch_trades"] is False and has["fetch_ohlcv"] is False
        assert has["fetch_order_books"] is True and has["fetch_markets_by_ids"] is True
        assert has["sort"] == "partial" and has["fetch_fee_schedule"] is True

    def test_no_trades_and_no_candles(self):
        adapter, betting = venue()
        with pytest.raises(NotSupported, match="no individual trades"):
            adapter.fetch_trades(ARSENAL)
        with pytest.raises(NotSupported):
            adapter.fetch_ohlcv(ARSENAL)
        assert betting.requests == []

    def test_order_entry_needs_the_account(self):
        from synpath.trading.errors import CredentialsMissing

        with pytest.raises(CredentialsMissing, match="BETFAIR_APP_KEY"):
            synpath.Client({}).trading("betfair")

    def test_venue_rules_note_the_minimum_stake(self):
        from synpath.trading.instruments import VENUE_RULES

        assert VENUE_RULES["betfair"]["min_notional"] == 1
