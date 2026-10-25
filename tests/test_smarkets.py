"""Smarkets: ids, units, bids read as YES, catalog walk, status, trades, fees, login.

The samples under `tests/samples/smarkets_*.json` were recorded from
api.smarkets.com anonymously on 2026-10-20, then trimmed: lists cut to a few
rows, the SEO text template shortened, and the listing's `next_page` rewritten
to the last event kept. Every request goes to an `httpx.MockTransport`;
nothing here reaches Smarkets.
"""
from __future__ import annotations

import json
import re

import httpx
import pytest

import synpath
from synpath import AuthenticationError, BadRequest, MarketNotFound, NotSupported
from synpath.smarkets import (
    DEFAULT_COMMISSION, SESSION_IDLE_LIMIT, Smarkets, SmarketsSession, contract_native_id, contracts,
    fee_schedule_of, last_price, market_of, normalize_event, normalize_market, normalize_order_book,
    parse_contract_id, price, status_of,
)
from synpath.trading.credentials import CredentialsMissing, load_smarkets

from conftest import load

EVENTS = load("smarkets_events.json")
MARKETS = load("smarkets_markets.json")["markets"]
CONTRACTS = load("smarkets_contracts.json")["contracts"]
QUOTES = load("smarkets_quotes.json")
VOLUMES = load("smarkets_volumes.json")["volumes"]
LAST = load("smarkets_last_prices.json")["last_executed_prices"]
TRADES = load("smarkets_trades.json")["recent_trades"]
SETTLED = load("smarkets_settled.json")
HALTED = load("smarkets_halted.json")
ERRORS = load("smarkets_errors.json")

FIXTURE = EVENTS["events"][0]                      # Fulham vs Crystal Palace
RESULT, SCORE, BTTS = MARKETS                      # full-time result, correct score, both teams to score
FULHAM = "smarkets:164973231-456060183"

ALL_EVENTS = EVENTS["events"] + SETTLED["events"] + HALTED["events"]
ALL_MARKETS = MARKETS + SETTLED["markets"] + HALTED["markets"]
ALL_CONTRACTS = CONTRACTS + SETTLED["contracts"] + HALTED["contracts"]
ALL_QUOTES = {**QUOTES, **SETTLED["quotes"], **HALTED["quotes"]}
ALL_VOLUMES = VOLUMES + HALTED["volumes"]
LAST_ROWS = {row["contract_id"]: row for rows in LAST.values() for row in rows}


def contracts_of(market: dict) -> list[dict]:
    return [c for c in ALL_CONTRACTS if c["market_id"] == market["id"]]


@pytest.fixture(autouse=True)
def no_ambient_credentials(monkeypatch, tmp_path):
    """No test may pick up a real login from the environment or a `.env`."""
    for name in ("SMARKETS_USERNAME", "SMARKETS_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)


class Api:
    """api.smarkets.com, answered from the samples. Records every request."""

    def __init__(self, overrides: dict | None = None):
        self.requests: list[httpx.Request] = []
        self.overrides = overrides or {}

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        for pattern, answer in self.overrides.items():
            if re.fullmatch(pattern, path):
                return answer(request) if callable(answer) else answer
        if path == "/v3/sessions/":
            return httpx.Response(201, json={"factor": "complete", "token": "TOKEN-1", "stop": None})
        if path == "/v3/events/":
            after = request.url.params.get("pagination_last_id")
            rows = [e for e in EVENTS["events"] if after is None or int(e["id"]) > int(after)]
            return httpx.Response(200, json={"events": rows, "pagination": {"next_page": None}})
        if m := re.fullmatch(r"/v3/search/([^/]+)/", path):
            return httpx.Response(200, json=load("smarkets_search.json"))
        if m := re.fullmatch(r"/v3/events/([\d,]+)/markets/", path):
            wanted = m.group(1).split(",")
            return httpx.Response(200, json={"markets": [x for x in ALL_MARKETS if x["event_id"] in wanted]})
        if m := re.fullmatch(r"/v3/events/([\d,]+)/", path):
            wanted = m.group(1).split(",")
            return httpx.Response(200, json={"events": [e for e in ALL_EVENTS if e["id"] in wanted]})
        if m := re.fullmatch(r"/v3/markets/([\d,]+)/(contracts|quotes|volumes|last_executed_prices)/", path):
            wanted, what = m.group(1).split(","), m.group(2)
            in_wanted = [c for c in ALL_CONTRACTS if c["market_id"] in wanted]
            if what == "contracts":
                return httpx.Response(200, json={"contracts": in_wanted})
            if what == "quotes":
                return httpx.Response(200, json={c["id"]: ALL_QUOTES[c["id"]] for c in in_wanted if c["id"] in ALL_QUOTES})
            if what == "volumes":
                return httpx.Response(200, json={"volumes": [v for v in ALL_VOLUMES if v["market_id"] in wanted]})
            return httpx.Response(200, json={"last_executed_prices": {k: v for k, v in LAST.items() if k in wanted}})
        if m := re.fullmatch(r"/v3/markets/([\d,]+)/", path):
            wanted = m.group(1).split(",")
            return httpx.Response(200, json={"markets": [x for x in ALL_MARKETS if x["id"] in wanted]})
        if m := re.fullmatch(r"/v3/contracts/([\d,]+)/recent_trades/", path):
            wanted = m.group(1).split(",")
            return httpx.Response(200, json={"recent_trades": [t for t in TRADES if t["contract_id"] in wanted]})
        if re.fullmatch(r"/v3/contracts/[\d,]+/executions_time_series/", path):
            return httpx.Response(503, json=ERRORS["charts_unavailable"])
        return httpx.Response(404, json={"error_type": "NOT_FOUND", "data": path})

    def paths(self) -> list[str]:
        return [r.url.path for r in self.requests]

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self.handler))


def venue(api: Api | None = None, **kwargs) -> tuple[Smarkets, Api]:
    api = api or Api()
    return Smarkets(limiter=None, quotes_limiter=None, client=api.client(), **kwargs), api


def session(api: Api, **kwargs) -> SmarketsSession:
    return SmarketsSession(username="punter@example.com", password="hunter2", client=api.client(), **kwargs)


class TestIds:
    def test_round_trip(self):
        assert contract_native_id(164973231, 456060183) == "164973231-456060183"
        assert parse_contract_id("164973231-456060183") == ("164973231", "456060183")

    @pytest.mark.parametrize("bad", ["164973231", "abc-1", "1-", "-1", "1.2-3"])
    def test_a_bad_contract_id_is_refused(self, bad):
        with pytest.raises(BadRequest):
            parse_contract_id(bad)

    def test_a_market_id_names_the_event(self):
        assert market_of("164973231") == "164973231" and market_of("164973231-456060183") == "164973231"

    def test_synpath_ids_route_to_smarkets(self):
        assert synpath.ids.venue_of(FULHAM) == "smarkets"
        assert "smarkets" in synpath.exchanges and synpath.exchanges["smarkets"] is Smarkets


class TestUnits:
    """The OpenAPI document: price in basis points of 100%; quantity the
    total pot in 1/100 of a penny, back stake = quantity * price / 1e8."""

    def test_prices_are_basis_points_of_one(self):
        assert price(5000) == 0.5 and price(9999) == 0.9999 and price(1) == 0.0001

    def test_quantities_are_pots_in_ten_thousandths_of_a_pound(self):
        assert contracts(10_000) == 1.0 and contracts(500) == 0.05

    def test_the_documented_back_stake_example(self):
        # "100000 (quantity) * 5000 (price) / 100000000 = 5 GBP back stake":
        # a pot of £10 is 10 contracts, and buying 10 YES at 0.5 costs £5.
        assert contracts(100_000) * price(5000) == pytest.approx(100_000 * 5000 / 100_000_000)

    def test_volumes_are_whole_pounds_not_ten_thousandths(self):
        """A market whose recent tape alone matched more than £21 of pot
        reports `volume: 376`. Read in 1/10,000 GBP that would be £0.04, less
        than one minimum £0.05 stake: the unit is pounds."""
        volume = next(v["volume"] for v in VOLUMES if v["market_id"] == RESULT["id"])
        tape = sum(contracts(t["quantity"]) for t in TRADES)
        assert tape > 21 and volume == 376
        assert volume / 10_000 < 0.05 < tape <= volume

    def test_the_last_executed_price_is_a_percentage_and_zero_means_never(self):
        assert last_price(LAST_ROWS["456060183"])[0] == 0.5
        assert last_price(LAST_ROWS["456060184"]) == (None, None)
        assert last_price({"last_executed_price": "23.45", "timestamp": None})[0] == 0.2345


class TestBidsAreYesBids:
    def test_bids_are_bids_and_offers_asks(self):
        book = normalize_order_book(QUOTES["456060183"], market_id=FULHAM)
        assert [level.price for level in book.bids] == [0.5, 0.4762, 0.4545]
        assert [level.price for level in book.asks] == [0.5155]
        assert book.bids[0].size == 11.6881 and book.asks[0].size == 350.0831

    def test_no_recorded_book_is_crossed(self):
        crossed = [cid for cid, q in ALL_QUOTES.items()
                   if q["bids"] and q["offers"] and max(b["price"] for b in q["bids"]) >= min(o["price"] for o in q["offers"])]
        assert not crossed
        for cid, q in QUOTES.items():
            book = normalize_order_book(q, market_id=f"smarkets:x-{cid}")
            if book.bids and book.asks:
                assert book.bids[0].price < book.asks[0].price

    def test_the_venues_own_rows_and_currency_are_kept(self):
        book = normalize_order_book(QUOTES["456060183"], market_id=FULHAM)
        assert book.info["bids"][0] == [5000, 116881] and book.info["offers"] == [[5155, 3500831]]
        assert book.info["currency"] == "GBP" and book.info["delayed"] is True
        assert book.book_model == "shared_complement" and book.depth_scope == "unknown"

    def test_the_no_side_is_the_mirror(self):
        no = normalize_order_book(QUOTES["456060183"], market_id=FULHAM, side="no")
        assert no.derived and no.bids[0].price == pytest.approx(1 - 0.5155) and no.asks[0].price == 0.5
        with pytest.raises(BadRequest):
            normalize_order_book(QUOTES["456060183"], market_id=FULHAM, side="maybe")  # type: ignore[arg-type]

    def test_an_empty_side(self):
        book = normalize_order_book(QUOTES["456060186"], market_id="smarkets:164973232-456060186")
        assert book.bids == [] and book.asks[0].price == 0.0714


class TestCatalogMapping:
    def fulham(self):
        contract = next(c for c in CONTRACTS if c["id"] == "456060183")
        volume = next(v for v in VOLUMES if v["market_id"] == RESULT["id"])
        return normalize_market(FIXTURE, RESULT, contract, quote=QUOTES["456060183"], last=LAST_ROWS["456060183"],
                                volume=volume)

    def test_each_contract_is_a_market_and_the_smarkets_market_its_event(self):
        market = self.fulham()
        assert market.id == FULHAM and market.venue_market_id == "164973231-456060183"
        assert market.event_id == "smarkets:164973231"
        assert market.title == "Fulham vs Crystal Palace — Full-time result — Fulham"
        assert market.yes.label == "Fulham" and market.no.label == "Not Fulham"
        assert market.info["smarkets_event"]["name"] == "Fulham vs Crystal Palace"
        assert "seo_description" not in market.info["smarkets_event"]
        assert market.tags == ["football", "single_event", "winner"]

    def test_quotes_last_price_and_volume(self):
        market = self.fulham()
        assert market.yes.quote.bid == 0.5 and market.yes.quote.ask == 0.5155
        assert market.no.quote.bid == pytest.approx(0.4845) and market.no.quote.ask == 0.5
        assert market.yes.quote.last == 0.5 and market.yes.quote.last_timestamp is not None
        # Volume is per Smarkets market, so it is not the contract's own.
        assert market.stats.volume_total is None and market.info["market_volume_gbp"] == 376
        assert market.info["currency"] == "GBP" and market.info["delayed"] is True

    def test_open_and_closing_at_kick_off_when_not_in_play(self):
        market = self.fulham()
        assert market.status == "open" and market.active
        assert market.close_datetime == "2026-10-28T19:45:00Z"
        assert market.market_type == "binary"

    def test_the_event_holds_every_contract_and_is_exclusive(self):
        event = normalize_event(FIXTURE, RESULT, contracts_of(RESULT), quotes=QUOTES, last=LAST_ROWS)
        assert event.id == "smarkets:164973231" and len(event.markets) == 3
        assert {m.outcome_label for m in event.markets} == {"Fulham", "Crystal Palace", "Draw"}
        assert event.mutually_exclusive is True and event.status == "open"

    def test_serializes(self):
        json.dumps(self.fulham().model_dump())


class TestStatus:
    @pytest.mark.parametrize("market,contract,event,expected", [
        ("open", "open", "upcoming", ("open", True)),
        ("live", "live", "live", ("open", True)),
        ("halted", "halted", "live", ("open", False)),
        ("open", "unavailable", None, ("open", False)),
        ("new", "new", "upcoming", ("unopened", False)),
        ("open", "open", "ended", ("closed", False)),
        ("settled", "winner", "ended", ("settled", False)),
        ("open", "reduced", "live", ("settled", False)),
        ("voided", "open", "cancelled", ("settled", False)),
    ])
    def test_status(self, market, contract, event, expected):
        assert status_of(market, contract, event) == expected

    def test_results_from_a_settled_fixture(self):
        event, (result, score) = SETTLED["events"][0], SETTLED["markets"]
        markets = {c["name"]: normalize_market(event, result, c) for c in contracts_of(result)}
        assert markets["São Paulo"].status == "settled" and markets["São Paulo"].info["result"] == "yes"
        assert markets["Draw"].info["result"] == "no" and not markets["Draw"].active
        assert markets["São Paulo"].resolution_datetime is not None
        scores = [normalize_market(event, score, c).info["result"] for c in contracts_of(score)]
        assert scores.count("yes") == 1 and set(scores) == {"yes", "no"}

    def test_a_halted_market_is_open_but_not_accepting(self):
        event, market = HALTED["events"][0], HALTED["markets"][0]
        rows = [normalize_market(event, market, c, quote=HALTED["quotes"].get(c["id"])) for c in contracts_of(market)]
        assert all(m.status == "open" and not m.active for m in rows)

    def test_void(self):
        contract = dict(contracts_of(RESULT)[0], state_or_outcome="voided")
        assert normalize_market(FIXTURE, RESULT, contract).info["result"] == "void"
        market = dict(RESULT, state="voided")
        assert normalize_market(FIXTURE, market, contracts_of(RESULT)[1]).info["result"] == "void"

    def test_a_dead_heat_is_neither_yes_nor_no(self):
        contract = dict(contracts_of(RESULT)[0], state_or_outcome="deadheat")
        assert normalize_market(FIXTURE, RESULT, contract).info["result"] == "dead_heat"


class TestAdapter:
    def test_listing_walks_events_markets_contracts_then_prices(self):
        smk, api = venue()
        page = smk.fetch_markets(limit=5)
        # Contracts in Smarkets' display order, highest first.
        assert [m.id for m in page] == [
            "smarkets:164973231-456060184", "smarkets:164973231-456060185", "smarkets:164973231-456060183",
            "smarkets:164973232-456060204", "smarkets:164973232-456060203",
        ]
        assert api.paths()[:3] == ["/v3/events/", "/v3/events/45325999,45326001/markets/",
                                   "/v3/markets/164973231,164973232,164973233/contracts/"]
        assert {"/v3/markets/164973231,164973232/quotes/", "/v3/markets/164973231,164973232/volumes/",
                "/v3/markets/164973231,164973232/last_executed_prices/"} <= set(api.paths())
        assert {m.yes.label: (m.yes.quote.bid, m.yes.quote.ask) for m in page[:3]}["Fulham"] == (0.5, 0.5155)

    def test_the_listing_asks_for_the_open_states_and_bettable_scopes(self):
        smk, api = venue()
        smk.fetch_markets(limit=1, domain="football")
        params = api.requests[0].url.params
        assert params.get_list("state") == ["upcoming", "live"]
        assert params.get_list("type_domain") == ["football"]
        assert params.get_list("type_scope") == ["single_event", "outright", "antepost"]
        assert params["with_new_type"] == "true" and params["sort"] == "id"

    def test_every_request_carries_a_real_user_agent_and_no_login(self):
        smk, api = venue()
        smk.fetch_markets(limit=2)
        for request in api.requests:
            assert request.headers["User-Agent"].startswith("synpath/")
            assert "Authorization" not in request.headers

    def test_the_cursor_resumes_mid_market_without_repeats_or_gaps(self):
        smk, _ = venue()
        whole = [m.id for m in smk.fetch_markets(limit=24)]
        first = smk.fetch_markets(limit=4)
        assert first.next_cursor == "|1|1"
        second = smk.fetch_markets(limit=10, cursor=first.next_cursor)
        third = smk.fetch_markets(limit=10, cursor=second.next_cursor)
        assert [m.id for m in first + second + third] == whole[:24]

    def test_a_page_ending_on_a_markets_last_contract_moves_to_the_next_market(self):
        smk, _ = venue()
        page = smk.fetch_markets(limit=3)
        assert page.next_cursor == "|1|0"
        assert smk.fetch_markets(limit=1, cursor=page.next_cursor)[0].id.startswith("smarkets:164973232-")

    def test_the_end_of_the_listing_has_no_cursor(self):
        smk, _ = venue()
        assert smk.fetch_markets(limit=200).next_cursor is None

    def test_a_bad_cursor_and_a_sort_are_refused(self):
        smk, _ = venue()
        with pytest.raises(BadRequest):
            smk.fetch_markets(cursor="nonsense")
        with pytest.raises(NotSupported):
            smk.fetch_markets(sort="volume")

    def test_status_picks_the_event_states(self):
        smk, api = venue()
        smk.fetch_markets(limit=1, status="settled")
        assert api.requests[0].url.params.get_list("state") == ["settled", "cancelled"]

    def test_search(self):
        smk, api = venue()
        smk.fetch_markets(query="arsenal", limit=3)
        assert api.paths()[0] == "/v3/search/arsenal/"
        assert api.paths()[1].startswith("/v3/events/45379705,")

    def test_fetch_events_groups_contracts_under_their_market(self):
        smk, _ = venue()
        page = smk.fetch_events(limit=2)
        assert [e.id for e in page] == ["smarkets:164973231", "smarkets:164973232"]
        assert len(page[0].markets) == 3 and page.next_cursor == "|2|0"
        assert smk.fetch_events(limit=5, cursor=page.next_cursor)[0].id == "smarkets:164973233"

    def test_fetch_event_reads_a_settled_market(self):
        smk, _ = venue()
        event = smk.fetch_event("smarkets:169719862")
        assert event.status == "settled" and len(event.markets) == 3

    def test_fetch_markets_by_ids_keeps_the_order_and_drops_the_unknown(self):
        smk, _ = venue()
        settled_id = SETTLED["contracts"][0]["id"]
        found = smk.fetch_markets_by_ids([f"smarkets:169719862-{settled_id}", FULHAM, "smarkets:999-1",
                                          "smarkets:164973231-1"])
        assert [m.id for m in found] == [f"smarkets:169719862-{settled_id}", FULHAM]
        assert found[0].status == "settled" and found[1].info["smarkets_event"]["id"] == "45325999"

    def test_fetch_market_not_found(self):
        smk, _ = venue()
        with pytest.raises(MarketNotFound):
            smk.fetch_market("smarkets:999-1")

    def test_fetch_order_book_and_books_for_a_whole_market(self):
        smk, api = venue()
        book = smk.fetch_order_book(FULHAM)
        assert book.market_id == FULHAM and book.bids[0].price == 0.5
        assert api.paths() == ["/v3/markets/164973231/quotes/"]
        books = smk.fetch_order_books(["smarkets:164973231"])
        assert set(books) == {f"smarkets:164973231-{c['id']}" for c in contracts_of(RESULT)}

    def test_a_settled_market_has_no_book(self):
        smk, _ = venue()
        with pytest.raises(MarketNotFound):
            smk.fetch_order_book(f"smarkets:169719862-{SETTLED['contracts'][0]['id']}")

    def test_fetch_trades_oldest_first_from_the_recent_tape(self):
        smk, api = venue()
        trades = smk.fetch_trades(FULHAM)
        assert api.paths() == ["/v3/contracts/456060183/recent_trades/"]
        assert len(trades) == 5 and [t.timestamp for t in trades] == sorted(t.timestamp for t in trades)
        first = trades[0]
        assert first.price == 0.5 and first.quantity == 1.4881 and first.side == "sell"
        assert first.market_id == FULHAM and len({t.id for t in trades}) == 5
        assert len(smk.fetch_trades(FULHAM, limit=2)) == 2
        assert smk.fetch_trades(FULHAM, since=trades[-1].timestamp) == [trades[-1]]
        with pytest.raises(NotSupported):
            smk.fetch_trades(FULHAM, cursor="x")

    def test_no_candles(self):
        smk, _ = venue()
        with pytest.raises(NotSupported, match="CHARTS_UNAVAILABLE"):
            smk.fetch_ohlcv(FULHAM)

    def test_event_states(self):
        states = load("smarkets_states.json")
        smk, api = venue(Api({r"/v3/events/[\d,]+/states/": httpx.Response(200, json=states)}))
        assert smk.fetch_event_states(["45325999"])["45325999"]["state"] == "upcoming"
        assert api.paths() == ["/v3/events/45325999/states/"]

    def test_a_cloudflare_refusal_says_so(self):
        page = "<html><head><title>Attention Required! | Cloudflare</title></head></html>"
        smk, _ = venue(Api({r"/v3/events/": httpx.Response(403, text=page)}))
        with pytest.raises(AuthenticationError, match="Cloudflare"):
            smk.fetch_markets(limit=1)


class TestFees:
    def test_commission_on_net_market_winnings(self):
        fee = fee_schedule_of("164973231")
        assert fee.fee_type == "net_market_winnings" and fee.taker_rate == fee.maker_rate == DEFAULT_COMMISSION == 0.02
        assert fee.estimate(price=0.5, contracts=100) == pytest.approx(1.0)

    def test_the_adapter_names_the_smarkets_market_and_reads_nothing(self):
        smk, api = venue(commission=0.03)
        fee = smk.fetch_fee_schedule(FULHAM)
        assert fee.scope_id == "164973231" and fee.taker_rate == 0.03 and api.requests == []


class TestSession:
    def test_login_posts_json_and_the_token_rides_as_session_token(self):
        api = Api()
        smk, _ = venue(api, session=session(api))
        smk.fetch_order_book(FULHAM)
        login, quotes = api.requests
        assert login.method == "POST" and login.url.path == "/v3/sessions/"
        assert json.loads(login.content) == {"username": "punter@example.com", "password": "hunter2"}
        assert quotes.headers["Authorization"] == "Session-Token TOKEN-1"
        assert smk.delayed is False and smk.fetch_order_book(FULHAM).info["delayed"] is False

    def test_an_idle_token_is_replaced(self):
        api, now = Api(), [0.0]
        s = session(api, clock=lambda: now[0])
        s.token()
        now[0] += SESSION_IDLE_LIMIT - 1
        s.token()
        now[0] += SESSION_IDLE_LIMIT + 1
        s.token()
        assert api.paths() == ["/v3/sessions/", "/v3/sessions/"]

    def test_a_refused_token_logs_in_again_and_retries_once(self):
        seen: list[str] = []

        def quotes(request):
            seen.append(request.headers.get("Authorization", ""))
            return httpx.Response(401, json={"error_type": "AUTH_REQUIRED", "data": ""}) if len(seen) == 1 \
                else httpx.Response(200, json=QUOTES)

        api = Api({r"/v3/markets/[\d,]+/quotes/": quotes})
        smk, _ = venue(api, session=session(api, session_token="OLD"))
        smk.fetch_order_book(FULHAM)
        assert seen == ["Session-Token OLD", "Session-Token TOKEN-1"]

    def test_a_second_factor_is_refused_clearly(self):
        api = Api({r"/v3/sessions/": httpx.Response(201, json={"factor": "totp", "token": None})})
        with pytest.raises(AuthenticationError, match="second factor"):
            session(api).token()

    def test_bad_credentials(self):
        api = Api({r"/v3/sessions/": httpx.Response(401, json={"error_type": "INVALID_CREDENTIALS", "data": ""})})
        with pytest.raises(AuthenticationError, match="INVALID_CREDENTIALS"):
            session(api).token()

    def test_the_password_and_token_never_show(self):
        s = session(Api())
        s.token()
        assert "hunter2" not in repr(s) and "TOKEN-1" not in repr(s)


class TestCredentials:
    def test_optional(self):
        assert load_smarkets({}) is None
        creds = load_smarkets({"SMARKETS_USERNAME": "u@example.com", "SMARKETS_PASSWORD": "pw"})
        assert creds.username == "u@example.com" and "pw" not in repr(creds) and creds.secrets == ["pw"]
        with pytest.raises(CredentialsMissing, match="both"):
            load_smarkets({"SMARKETS_USERNAME": "u@example.com"})

    def test_the_adapter_logs_in_from_the_environment_unless_told_not_to(self, monkeypatch):
        assert Smarkets(limiter=None).session is None
        monkeypatch.setenv("SMARKETS_USERNAME", "u@example.com")
        monkeypatch.setenv("SMARKETS_PASSWORD", "pw")
        assert Smarkets(limiter=None).session is not None
        assert Smarkets(limiter=None, login=False).session is None


class TestCapabilities:
    def test_declared_honestly(self):
        has = Smarkets.has
        assert has["fetch_markets"] is True and has["fetch_order_books"] is True and has["search"] is True
        assert has["fetch_trades"] == "partial" and has["fetch_ohlcv"] is False and has["sort"] is False

    def test_order_entry_needs_the_login(self):
        with pytest.raises(CredentialsMissing, match="SMARKETS_USERNAME"):
            synpath.Client({"smarkets": None}).trading("smarkets")

    def test_venue_rules_note_the_minimum_stake(self):
        from decimal import Decimal

        from synpath.trading.instruments import VENUE_RULES

        assert VENUE_RULES["smarkets"]["min_notional"] == Decimal("0.05")
