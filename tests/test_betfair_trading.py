"""Betfair order entry: bets built from orders, Betfair's records read on the
YES leg, and the adapter against a scripted venue.

Betting needs an account and a live application key, so nothing here is
recorded. The place, current-order and cleared-order records are the
examples in Betfair's Betting API documentation (placeOrders and Getting
Started), the JSON-RPC envelope taken off as the REST interface answers.
Betfair documents no example for the cancel, replace and update reports,
`getAccountFunds`, `getAccountDetails` or `listMarketProfitAndLoss`; those
are built field by field from its type definitions. Every request goes to
an `httpx.MockTransport`; nothing here reaches Betfair.
"""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from urllib.parse import parse_qs

import httpx
import pytest

from synpath.betfair import BetfairSession, fee_schedule_of
from synpath.errors import BadRequest, RequestTimeout
from synpath.trading.betfair import (
    BetfairTrading, build_instruction, check_minimum, fill_of_current, odds_for, order_of_cleared, order_of_current,
    order_of_report, positions_of_orders, settlement_of, stake_for,
)
from synpath.trading.credentials import BetfairCredentials
from synpath.trading.errors import (
    CredentialsMissing, InsufficientFunds, InvalidOrder, MarketHalted, OrderRejected, PermissionDenied,
)
from synpath.trading.types import EditRequest, OrderRequest, OrderStatus, OrderType, PositionSide, Side, TimeInForce

from conftest import load

S = load("betfair_trading.json")
AUTH = load("betfair_auth.json")
LAY_RUNNER = "betfair:1.109850906-237486"
RUNNER = "betfair:1.117020524-9170340"
BET = "45496907354"
BETTING = "/exchange/betting/rest/v1.0"
ACCOUNT = "/exchange/account/rest/v1.0"


@pytest.fixture(autouse=True)
def no_ambient_credentials(monkeypatch, tmp_path):
    for name in ("BETFAIR_APP_KEY", "BETFAIR_USERNAME", "BETFAIR_PASSWORD", "BETFAIR_CERT_PATH",
                 "BETFAIR_KEY_PATH", "BETFAIR_JURISDICTION"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)


def request(**changes) -> OrderRequest:
    base = {"market_id": LAY_RUNNER, "side": Side.SELL, "quantity": Decimal("6"), "price": Decimal("0.3333333333")}
    return OrderRequest(**{**base, **changes})


def current(**changes) -> dict:
    return {**S["current_orders"]["currentOrders"][0], **changes}


class Venue:
    """Answers Betting and Accounts API operations by name, and records them.
    Logins go to the identity host on the same transport."""

    def __init__(self, **answers):
        self.answers = {
            "getAccountDetails": S["account_details"],
            "login": AUTH["login_success"], "certlogin": AUTH["certlogin_success"],
            **answers,
        }
        self.requests: list[httpx.Request] = []

    def __call__(self, req: httpx.Request) -> httpx.Response:
        self.requests.append(req)
        name = req.url.path.rstrip("/").rsplit("/", 1)[-1]
        answer = self.answers.get(name)
        if answer is None:
            return httpx.Response(404, json={"faultcode": "Client", "faultstring": "not found"})
        if callable(answer):
            answer = answer(req)
        if isinstance(answer, httpx.Response):
            return answer
        return httpx.Response(200, json=answer)

    def sent(self, operation: str) -> list[dict]:
        return [json.loads(r.content) for r in self.requests if r.url.path.rstrip("/").endswith(f"/{operation}")]

    def calls(self, operation: str) -> list[httpx.Request]:
        return [r for r in self.requests if r.url.path.rstrip("/").endswith(f"/{operation}")]


class Catalog:
    def fetch_fee_schedule(self, market_id):
        return fee_schedule_of(market_id, 5)

    def close(self):
        pass


def adapter(venue: Venue, *, session: BetfairSession | None = None, jurisdiction: str = "com", **kwargs) -> BetfairTrading:
    transport = httpx.MockTransport(venue)
    session = session or BetfairSession(app_key="APP-KEY-LIVE", session_token="SESSION-TOKEN-1",
                                        jurisdiction=jurisdiction, client=httpx.Client(transport=transport))
    return BetfairTrading(session=session, catalog=Catalog(),
                          client=httpx.AsyncClient(transport=transport), **kwargs)


def run(coro):
    return asyncio.run(coro)


def go(venue: Venue, call, **kwargs):
    async def body():
        async with adapter(venue, **kwargs) as bf:
            return await call(bf)

    return run(body())


class TestInstructions:
    def test_the_documented_lay_is_what_a_sell_builds(self):
        # 6 contracts sold at 1/3 is a lay at odds 3 for a backer's stake of 2.
        documented = S["place_lay_request"]["instructions"][0]
        built = build_instruction(request())
        assert built["side"] == documented["side"] and built["orderType"] == documented["orderType"]
        assert built["selectionId"] == int(documented["selectionId"]) and built["handicap"] == 0
        limit = built["limitOrder"]
        assert (limit["size"], limit["price"], limit["persistenceType"]) == (2, 3, "LAPSE")

    def test_a_buy_backs_and_carries_the_client_order_id(self):
        built = build_instruction(request(side=Side.BUY, price=Decimal("0.1"), quantity=Decimal("20"),
                                          client_order_id="strat-1:42"))
        assert built["side"] == "BACK" and built["limitOrder"]["size"] == 2 and built["limitOrder"]["price"] == 10
        assert built["customerOrderRef"] == "strat-1:42"

    def test_odds_snap_to_the_ladder_on_the_cheaper_side(self):
        # 1/0.41 = 2.439: a buy pays no more (odds 2.44), a sell receives no less (odds 2.42).
        assert odds_for(Side.BUY, Decimal("0.41")) == Decimal("2.44")
        assert odds_for(Side.SELL, Decimal("0.41")) == Decimal("2.42")
        # Above 100 the ladder steps by 10: 1/0.0075 = 133.3.
        assert odds_for(Side.BUY, Decimal("0.0075")) == Decimal("140")
        assert odds_for(Side.SELL, Decimal("0.0075")) == Decimal("130")

    def test_prices_off_the_ladder_are_refused(self):
        with pytest.raises(InvalidOrder):
            odds_for(Side.BUY, Decimal("0.0005"))
        with pytest.raises(InvalidOrder):
            odds_for(Side.SELL, Decimal("0.995"))

    def test_stakes_round_down_to_the_penny(self):
        assert stake_for(Decimal("10"), Decimal("3")) == Decimal("3.33")
        with pytest.raises(InvalidOrder, match="penny"):
            stake_for(Decimal("0.01"), Decimal("3"))

    def test_persistence_and_fill_or_kill(self):
        assert build_instruction(request(params={"persistence": "PERSIST"}))["limitOrder"]["persistenceType"] == "PERSIST"
        assert build_instruction(request(params={"keep_in_play": True}))["limitOrder"]["persistenceType"] == "PERSIST"
        assert build_instruction(request(params={"persistence": "market_on_close"}))["limitOrder"]["persistenceType"] \
            == "MARKET_ON_CLOSE"
        fok = build_instruction(request(side=Side.BUY, price=Decimal("1") / Decimal("21"), quantity=Decimal("105"),
                                        time_in_force=TimeInForce.FOK, params={"min_fill": "63"}))["limitOrder"]
        # As the documented fill or kill: 5 at 21, at least 3 of it.
        assert fok == {"size": 5, "price": 21, "timeInForce": "FILL_OR_KILL", "minFillSize": 3}
        with pytest.raises(InvalidOrder, match="persist"):
            build_instruction(request(type=OrderType.MARKET, params={"persistence": "PERSIST"}))
        with pytest.raises(InvalidOrder):
            build_instruction(request(params={"persistence": "KEEP"}))

    def test_sell_yes_and_buy_no_are_the_same_lay(self):
        lay = build_instruction(request(side=Side.SELL))
        assert build_instruction(request(side=Side.SELL, outcome="yes", action="sell")) == lay
        assert build_instruction(request(side=Side.SELL, outcome="no", action="buy")) == lay

    @pytest.mark.parametrize("changes", [
        {"time_in_force": TimeInForce.GTD, "expires_at": 1}, {"post_only": True},
        {"type": OrderType.ICEBERG}, {"type": OrderType.MARKET, "price": None},
        {"market_id": "betfair:1.109850906"}, {"client_order_id": "x" * 33},
    ])
    def test_what_betfair_does_not_hold_is_refused(self, changes):
        with pytest.raises(BadRequest):        # InvalidOrder is a BadRequest
            build_instruction(request(**changes))

    def test_the_minimum_stake_or_the_minimum_payout(self):
        check_minimum(Decimal("1"), Decimal("2"), currency="GBP")
        # 10p at 100 pays £10: valid by the payout rule, except on .it and .es.
        check_minimum(Decimal("0.10"), Decimal("100"), currency="GBP")
        with pytest.raises(InvalidOrder, match="payout"):
            check_minimum(Decimal("0.10"), Decimal("50"), currency="GBP")
        with pytest.raises(InvalidOrder):
            check_minimum(Decimal("0.10"), Decimal("100"), currency="EUR", jurisdiction="it")
        check_minimum(Decimal("0.10"), Decimal("2"), currency="XYZ")      # left to Betfair


class TestRecords:
    def test_the_documented_placement_rests(self):
        order = order_of_report(S["place_lay"]["instructionReports"][0], market_id="1.109850906")
        assert order.id == "31242604945" and order.market_id == LAY_RUNNER and order.side == Side.SELL
        assert order.status == OrderStatus.OPEN and order.quantity == Decimal("6")
        assert order.price == Decimal("0.3333333333") and order.filled == 0 and order.remaining == Decimal("6")
        assert order.created_at == 1383142967000

    def test_an_order_status_on_the_report_wins(self):
        order = order_of_report(S["place_persist"]["instructionReports"][0], market_id="1.240636817")
        assert order.status == OrderStatus.OPEN and order.side == Side.BUY and order.quantity == Decimal("20")
        assert order.info["instruction"]["limitOrder"]["persistenceType"] == "PERSIST"

    def test_fill_or_kill_killed_and_partly_filled(self):
        killed = order_of_report(S["place_fok_killed"]["instructionReports"][0], market_id="1.126124422")
        assert killed.status == OrderStatus.EXPIRED and killed.filled == 0 and killed.remaining == 0
        assert killed.time_in_force == TimeInForce.FOK
        partial = order_of_report(S["place_fok_partial"]["instructionReports"][0], market_id="1.126124422")
        assert partial.status == OrderStatus.EXPIRED and partial.quantity == Decimal("105.0")
        assert partial.filled == Decimal("3.4400000000000004") * 21
        assert partial.average_price == (1 / Decimal("21.32267441860465")).quantize(Decimal("1e-10"))

    def test_a_current_order(self):
        order = order_of_current(S["current_orders"]["currentOrders"][0])
        assert order.id == BET and order.market_id == RUNNER and order.status == OrderStatus.OPEN
        assert order.quantity == Decimal("50.0") and order.remaining == Decimal("50.0") and order.price == Decimal("0.1")

    def test_finished_orders_are_closed_or_canceled(self):
        matched = order_of_current(current(status="EXECUTION_COMPLETE", sizeMatched=5.0, sizeRemaining=0.0,
                                           averagePriceMatched=10.5))
        assert matched.status == OrderStatus.CLOSED and matched.filled == Decimal("50.0") and matched.remaining == 0
        assert matched.average_price == (1 / Decimal("10.5")).quantize(Decimal("1e-10"))
        cancelled = order_of_current(current(status="EXECUTION_COMPLETE", sizeMatched=2.0, sizeRemaining=0.0,
                                             sizeCancelled=3.0, averagePriceMatched=10.0))
        assert cancelled.status == OrderStatus.CANCELED and cancelled.filled == Decimal("20.0")

    def test_a_cancelled_bet_from_the_cleared_orders(self):
        order = order_of_cleared({**S["cleared_cancelled"]["clearedOrders"][0], "betStatus": "CANCELLED"})
        assert order.status == OrderStatus.CANCELED and order.quantity == Decimal("50.0") and order.filled == 0

    def test_a_fill_is_the_bet_at_its_average_matched_odds(self):
        fill = fill_of_current(current(status="EXECUTION_COMPLETE", sizeMatched=5.0, averagePriceMatched=10.5,
                                       matchedDate="2015-01-22T13:02:00.000Z"))
        assert fill.id == fill.order_id == BET and fill.quantity == Decimal("52.50")
        assert fill.price == (1 / Decimal("10.5")).quantize(Decimal("1e-10")) and fill.timestamp == 1421931720000
        assert fill_of_current(current()) is None

    def test_positions_net_backs_and_lays_per_runner(self):
        rows = [current(sizeMatched=2.0, averagePriceMatched=12.0),
                current(betId="2", side="LAY", sizeMatched=1.0, averagePriceMatched=10.0)]
        [position] = positions_of_orders(rows)
        assert position.market_id == RUNNER and position.side == PositionSide.LONG
        assert (position.inventory_yes, position.inventory_no, position.contracts) == (24, 10, 14)
        assert position.margin == Decimal("2.0") + Decimal("9.0")
        assert position.entry_price == (Decimal("3") / Decimal("34")).quantize(Decimal("1e-10"))

    def test_settlements_read_the_bets_outcome_as_the_runners(self):
        rows = S["cleared_orders"]["clearedOrders"]
        won = settlement_of(rows[1])
        assert won.market_id == "betfair:1.144157120-2830908" and won.held == PositionSide.LONG
        assert won.result == "yes" and won.won is True and won.quantity == Decimal("2.56") and won.cost == 2
        assert won.pnl == Decimal("0.56") and won.timestamp is not None
        lost = settlement_of(rows[3])
        assert lost.result == "no" and lost.won is False and lost.pnl == Decimal("-2.65")
        lay = settlement_of({**rows[3], "side": "LAY", "betOutcome": "WON"})
        assert lay.held == PositionSide.SHORT and lay.result == "no" and lay.cost is None


class TestPlacing:
    def test_a_bet_is_placed_with_the_session_and_its_client_id(self):
        venue = Venue(placeOrders=S["place_lay"])
        order = go(venue, lambda bf: bf.create_order(request(client_order_id="c-1")))
        [place] = venue.calls("placeOrders")
        assert place.url.path == f"{BETTING}/placeOrders/"
        assert place.headers["X-Application"] == "APP-KEY-LIVE" and place.headers["X-Authentication"] == "SESSION-TOKEN-1"
        assert venue.calls("getAccountDetails")[0].url.path == f"{ACCOUNT}/getAccountDetails/"
        [body] = venue.sent("placeOrders")
        assert body["marketId"] == "1.109850906" and body["customerRef"] == "c-1"
        assert body["instructions"][0]["customerOrderRef"] == "c-1"
        assert order.status == OrderStatus.OPEN and order.client_order_id == "c-1" and order.id == "31242604945"

    def test_a_bet_below_the_minimum_is_refused_before_sending(self):
        venue = Venue(placeOrders=S["place_lay"])
        with pytest.raises(InvalidOrder, match="minimum"):
            go(venue, lambda bf: bf.create_order(request(quantity=Decimal("1.5"))))
        assert venue.calls("placeOrders") == []

    def test_a_market_order_cancels_its_unmatched_rest(self):
        cancelled = current(betId="31242604945", marketId="1.109850906", selectionId=237486, side="LAY",
                            priceSize={"price": 3.0, "size": 2.0}, status="EXECUTION_COMPLETE",
                            sizeRemaining=0.0, sizeCancelled=2.0)
        venue = Venue(placeOrders=S["place_lay"], cancelOrders={**S["cancel"], "marketId": "1.109850906"},
                      listCurrentOrders={"currentOrders": [cancelled], "moreAvailable": False})
        order = go(venue, lambda bf: bf.create_order(request(type=OrderType.MARKET)))
        [cancel] = venue.sent("cancelOrders")
        assert cancel == {"marketId": "1.109850906", "instructions": [{"betId": "31242604945"}]}
        assert order.status == OrderStatus.CANCELED and order.type == OrderType.MARKET

    def test_a_fill_or_kill_is_left_to_betfair(self):
        venue = Venue(placeOrders=S["place_fok_killed"])
        order = go(venue, lambda bf: bf.create_order(request(
            market_id="betfair:1.126124422-10590221", side=Side.BUY, price=Decimal(1) / 21, quantity=Decimal("105"),
            time_in_force=TimeInForce.FOK)))
        assert order.status == OrderStatus.EXPIRED and venue.calls("cancelOrders") == []

    def test_a_timeout_is_looked_up_by_the_client_order_id(self):
        timeout = {"status": "TIMEOUT", "marketId": "1.109850906", "instructionReports": [{"status": "TIMEOUT"}]}
        found = current(betId="31242604945", marketId="1.109850906", selectionId=237486, customerOrderRef="c-9")
        venue = Venue(placeOrders=timeout, listCurrentOrders={"currentOrders": [found], "moreAvailable": False})
        order = go(venue, lambda bf: bf.create_order(request(client_order_id="c-9")))
        assert order.id == "31242604945" and venue.sent("listCurrentOrders")[0]["customerOrderRefs"] == ["c-9"]
        with pytest.raises(RequestTimeout):
            go(Venue(placeOrders=timeout), lambda bf: bf.create_order(request()))

    def test_one_request_per_betfair_market(self):
        def place(req):
            body = json.loads(req.content)
            report = S["place_lay"]["instructionReports"][0]
            return {"status": "SUCCESS", "marketId": body["marketId"],
                    "instructionReports": [report for _ in body["instructions"]]}

        venue = Venue(placeOrders=place)
        results = go(venue, lambda bf: bf.create_orders([
            request(), request(market_id="betfair:1.2-47972"), request(), request(post_only=True)]))
        assert [type(r).__name__ for r in results] == ["Order", "Order", "Order", "InvalidOrder"]
        bodies = venue.sent("placeOrders")
        assert sorted((b["marketId"], len(b["instructions"])) for b in bodies) == [("1.109850906", 2), ("1.2", 1)]
        assert all("customerRef" not in b for b in bodies)
        assert results[1].market_id == "betfair:1.2-237486"   # the report names the runner placed

    def test_the_italian_exchange_takes_fifty_a_request(self):
        def place(req):
            body = json.loads(req.content)
            report = S["place_lay"]["instructionReports"][0]
            return {"status": "SUCCESS", "instructionReports": [report for _ in body["instructions"]]}

        venue = Venue(placeOrders=place, getAccountDetails={"currencyCode": "EUR"})
        go(venue, lambda bf: bf.create_orders([request(quantity=Decimal("6")) for _ in range(60)]), jurisdiction="it")
        assert [len(b["instructions"]) for b in venue.sent("placeOrders")] == [50, 10]

    def test_a_rejected_bet_is_typed(self):
        venue = Venue(placeOrders=S["place_version_rejected"])
        with pytest.raises(OrderRejected) as caught:
            go(venue, lambda bf: bf.create_order(request(params={"market_version": 4249587130})))
        assert caught.value.reason == "bet_taken_or_lapsed"
        assert venue.sent("placeOrders")[0]["marketVersion"] == {"version": 4249587130}

    @pytest.mark.parametrize("code,error", [
        ("PERMISSION_DENIED", PermissionDenied), ("INSUFFICIENT_FUNDS", InsufficientFunds),
        ("MARKET_SUSPENDED", MarketHalted),
    ])
    def test_a_refused_request_is_typed(self, code, error):
        refused = {"status": "FAILURE", "errorCode": code, "marketId": "1.109850906",
                   "instructionReports": [{"status": "FAILURE", "errorCode": "ERROR_IN_ORDER",
                                           "instruction": S["place_lay"]["instructionReports"][0]["instruction"]}]}
        with pytest.raises(error):
            go(Venue(placeOrders=refused), lambda bf: bf.create_order(request()))

    def test_an_expired_session_logs_in_again_once(self):
        answers = iter([httpx.Response(400, json=AUTH["invalid_session"]), httpx.Response(200, json=S["account_funds"])])
        venue = Venue(getAccountFunds=lambda req: next(answers))
        transport = httpx.MockTransport(venue)
        session = BetfairSession(app_key="APP-KEY-LIVE", username="jbloggs", password="pw",
                                 client=httpx.Client(transport=transport))
        balance = go(venue, lambda bf: bf.fetch_balance(), session=session)
        assert len(venue.calls("login")) == 2 and balance.available == Decimal("95.5")

    def test_certificate_login_from_the_credentials(self, tmp_path):
        venue = Venue(getAccountFunds=S["account_funds"])
        creds = BetfairCredentials(app_key="APP-KEY-LIVE", username="jbloggs", password="pw",
                                   cert_path=str(tmp_path / "c.crt"), key_path=str(tmp_path / "c.key"))
        transport = httpx.MockTransport(venue)

        async def body():
            async with BetfairTrading(creds, catalog=Catalog(), client=httpx.AsyncClient(transport=transport),
                                      identity_client=httpx.Client(transport=transport)) as bf:
                return await bf.fetch_balance()

        run(body())
        [login] = venue.calls("certlogin")
        assert login.url.host == "identitysso-cert.betfair.com"
        assert parse_qs(login.content.decode()) == {"username": ["jbloggs"], "password": ["pw"]}
        assert venue.calls("getAccountFunds")[0].headers["X-Authentication"] == AUTH["certlogin_success"]["sessionToken"]


class TestManaging:
    def test_cancel_finds_the_market_and_reads_the_bet_back(self):
        after = current(status="EXECUTION_COMPLETE", sizeRemaining=0.0, sizeCancelled=5.0)
        reads = iter([{"currentOrders": [current()], "moreAvailable": False},
                      {"currentOrders": [after], "moreAvailable": False}])
        venue = Venue(cancelOrders=S["cancel"], listCurrentOrders=lambda req: next(reads))
        order = go(venue, lambda bf: bf.cancel_order(BET))
        assert venue.sent("cancelOrders") == [{"marketId": "1.117020524", "instructions": [{"betId": BET}]}]
        assert order.status == OrderStatus.CANCELED

    def test_a_bet_already_taken_is_read_back(self):
        matched = current(status="EXECUTION_COMPLETE", sizeMatched=5.0, sizeRemaining=0.0, averagePriceMatched=10.0)
        venue = Venue(cancelOrders=S["cancel_taken"], listCurrentOrders={"currentOrders": [matched], "moreAvailable": False})
        order = go(venue, lambda bf: bf.cancel_order(BET, market_id=RUNNER))
        assert order.status == OrderStatus.CLOSED

    def test_cancel_all_everywhere_on_a_market_or_on_a_runner(self):
        venue = Venue(cancelOrders=S["cancel"], listCurrentOrders={"currentOrders": [current()], "moreAvailable": False})

        async def calls(bf):
            return (await bf.cancel_all_orders(), await bf.cancel_all_orders(market_id="betfair:1.117020524"),
                    await bf.cancel_all_orders(market_id=RUNNER))

        assert go(venue, calls) == (1, 1, 1)
        everything, market, runner = venue.sent("cancelOrders")
        assert everything == {} and market == {"marketId": "1.117020524"}
        assert runner == {"marketId": "1.117020524", "instructions": [{"betId": BET}]}

    def test_a_new_price_is_a_replaced_bet(self):
        venue = Venue(replaceOrders=S["replace"])
        order = go(venue, lambda bf: bf.edit_order(EditRequest(order_id=BET, price=Decimal("1") / 12),
                                                   current=order_of_current(current())))
        assert venue.sent("replaceOrders") == [{"marketId": "1.117020524",
                                                "instructions": [{"betId": BET, "newPrice": 12}]}]
        assert order.id == "45496907999" and order.price == (1 / Decimal(12)).quantize(Decimal("1e-10"))
        assert order.queue_priority_preserved is False and order.info["replaced"] == BET

    def test_a_smaller_quantity_is_a_partial_cancel(self):
        after = current(sizeRemaining=3.0, sizeCancelled=2.0)
        venue = Venue(cancelOrders=S["cancel_partial"], listCurrentOrders={"currentOrders": [after], "moreAvailable": False})
        order = go(venue, lambda bf: bf.edit_order(EditRequest(order_id=BET, quantity=Decimal("30")),
                                                   current=order_of_current(current())))
        assert venue.sent("cancelOrders") == [{"marketId": "1.117020524",
                                               "instructions": [{"betId": BET, "sizeReduction": 2}]}]
        assert order.remaining == Decimal("30.0") and order.queue_priority_preserved is True

    def test_a_stake_cannot_be_raised(self):
        with pytest.raises(InvalidOrder, match="raise"):
            go(Venue(), lambda bf: bf.edit_order(EditRequest(order_id=BET, quantity=Decimal("80")),
                                                 current=order_of_current(current())))

    def test_persistence_is_updated_in_place(self):
        kept = current(persistenceType="PERSIST")
        venue = Venue(updateOrders=S["update"], listCurrentOrders={"currentOrders": [kept], "moreAvailable": False})
        order = go(venue, lambda bf: bf.update_persistence(BET, "persist", market_id=RUNNER))
        assert venue.sent("updateOrders") == [{"marketId": "1.117020524",
                                               "instructions": [{"betId": BET, "newPersistenceType": "PERSIST"}]}]
        assert order.info["persistenceType"] == "PERSIST"

    def test_a_bet_gone_from_the_current_orders_is_found_among_the_cleared(self):
        def cleared(req):
            status = json.loads(req.content)["betStatus"]
            return S["cleared_cancelled"] if status == "CANCELLED" else {"clearedOrders": [], "moreAvailable": False}

        venue = Venue(listCurrentOrders={"currentOrders": [], "moreAvailable": False}, listClearedOrders=cleared)
        order = go(venue, lambda bf: bf.fetch_order(BET))
        assert order.status == OrderStatus.CANCELED and order.market_id == RUNNER
        assert [b["betStatus"] for b in venue.sent("listClearedOrders")] == ["CANCELLED"]

    def test_open_orders_on_a_runner_page_through(self):
        pages = iter([{"currentOrders": [current()], "moreAvailable": True},
                      {"currentOrders": [current(betId="2", selectionId=1)], "moreAvailable": False}])
        venue = Venue(listCurrentOrders=lambda req: next(pages))
        orders = go(venue, lambda bf: bf.fetch_open_orders(market_id=RUNNER))
        assert [o.id for o in orders] == [BET]
        first, second = venue.sent("listCurrentOrders")
        assert first["orderProjection"] == "EXECUTABLE" and first["marketIds"] == ["1.117020524"]
        assert second["fromRecord"] == 1

    def test_fetch_orders_pages_by_offset(self):
        venue = Venue(listCurrentOrders={"currentOrders": [current()], "moreAvailable": True})
        page = go(venue, lambda bf: bf.fetch_orders(status="open", limit=1, since=1421931713000))
        assert page.next_cursor == "1" and len(page) == 1
        [sent] = venue.sent("listCurrentOrders")
        assert sent["recordCount"] == 1 and sent["dateRange"] == {"from": "2015-01-22T13:01:53Z"}

    def test_my_trades_are_the_bets_that_stopped_matching(self):
        rows = [current(status="EXECUTION_COMPLETE", sizeMatched=5.0, sizeRemaining=0.0, averagePriceMatched=10.0,
                        matchedDate="2015-01-22T13:02:00.000Z"),
                current(betId="2", sizeMatched=1.0, averagePriceMatched=10.0)]
        venue = Venue(listCurrentOrders={"currentOrders": rows, "moreAvailable": False})
        fills = go(venue, lambda bf: bf.fetch_my_trades())
        assert [f.id for f in fills] == [BET] and fills[0].quantity == Decimal("50.0")
        assert venue.sent("listCurrentOrders")[0]["orderBy"] == "BY_MATCH_TIME"

    def test_balance_positions_profit_and_loss_and_settlements(self):
        venue = Venue(
            getAccountFunds=S["account_funds"], listMarketProfitAndLoss=S["profit_and_loss"],
            listCurrentOrders={"currentOrders": [current(sizeMatched=2.0, averagePriceMatched=12.0)], "moreAvailable": False},
            listClearedOrders=S["cleared_orders"],
        )

        async def calls(bf):
            return (await bf.fetch_balance(), await bf.fetch_positions(market_id=RUNNER),
                    await bf.fetch_profit_and_loss([RUNNER]), await bf.fetch_settlements())

        balance, positions, pnl, settlements = go(venue, calls)
        assert (balance.total, balance.available, balance.locked, balance.currency) == (
            Decimal("100.0"), Decimal("95.5"), Decimal("4.5"), "GBP")
        assert positions[0].contracts == Decimal("24.0")
        assert pnl[0]["profitAndLosses"][0]["ifWin"] == 45.0
        assert venue.sent("listMarketProfitAndLoss")[0]["marketIds"] == ["1.117020524"]
        # The documented report mixes a market roll-up in; only bets naming a runner settle one.
        assert len(settlements) == 6 and venue.sent("listClearedOrders")[0]["betStatus"] == "SETTLED"

    def test_the_fee_is_commission_on_what_the_bet_wins(self):
        async def calls(bf):
            return (await bf.fetch_fee_estimate(RUNNER, Side.BUY, Decimal("0.4"), Decimal("100")),
                    await bf.fetch_fee_estimate(RUNNER, Side.SELL, Decimal("0.4"), Decimal("100")))

        buy, sell = go(Venue(), calls)
        assert buy.taker_fee == Decimal("3.0") and sell.taker_fee == Decimal("2.0") and buy.maker_fee == buy.taker_fee
        assert buy.currency == "GBP"


class TestWiring:
    def test_credentials_are_needed(self):
        with pytest.raises(CredentialsMissing):
            BetfairTrading(None)

    def test_the_client_the_engine_and_serve_build_it(self):
        import synpath
        from synpath.engine.__main__ import ADAPTERS
        from synpath.server.serve import VENUES

        creds = BetfairCredentials(app_key="k", username="u", password="p")
        trading = synpath.Client({"betfair": creds}).trading("betfair")
        try:
            assert isinstance(trading, BetfairTrading) and trading.has["edit_order"] is True
        finally:
            run(trading.close())
        assert ADAPTERS["betfair"] == ("..trading.betfair", "BetfairTrading") and "betfair" in VENUES
        assert synpath.BetfairTrading is BetfairTrading

    def test_the_engine_streams_share_the_adapters_session(self):
        from synpath.engine.feeds import venue_streams
        from synpath.ws.betfair import BetfairMarketStream, BetfairUserStream

        creds = BetfairCredentials(app_key="k", username="u", password="p")
        trading = BetfairTrading(creds, catalog=Catalog())
        try:
            streams = venue_streams("betfair", creds, trading)
            assert isinstance(streams.market, BetfairMarketStream) and isinstance(streams.private, BetfairUserStream)
            assert streams.private.session is trading.session is streams.market.session
        finally:
            run(trading.close())

    def test_doctor_names_the_login(self, tmp_path, capsys):
        from synpath.trading.__main__ import doctor

        env = tmp_path / ".env"
        env.write_text("BETFAIR_APP_KEY=app-key-secret-1\nBETFAIR_USERNAME=jbloggs\nBETFAIR_PASSWORD=secret-pw-123\n")
        assert doctor(str(env)) == 0
        out = capsys.readouterr().out
        assert "user jbloggs" in out and "live key" in out
        assert "secret-pw-123" not in out and "app-key-secret-1" not in out
