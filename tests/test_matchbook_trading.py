"""Matchbook order entry: offers built from orders, Matchbook's records read on
the YES leg, and the adapter against a scripted venue. Records are the
examples in Matchbook's API reference (betting needs an account); the login
example had a trailing comma removed and the settled-bets example its `...`
placeholder. Every request goes to an `httpx.MockTransport`."""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import httpx
import pytest

from synpath.trading.credentials import MatchbookCredentials
from synpath.trading.errors import CredentialsMissing, InsufficientFunds, InvalidOrder, OrderRejected
from synpath.trading.matchbook import (
    MatchbookTrading, build_offer, fills_of_offer, odds_for, order_of_offer, positions_of_bets, settlements_of,
    stake_for,
)
from synpath.trading.types import (
    EditRequest, OrderRequest, OrderStatus, OrderType, PositionSide, Side, TimeInForce,
)

from conftest import load

S = load("matchbook_trading.json")
MAN_UTD = "matchbook:410343160570009-410343161840009-401525949430009"
OFFER_ID = "413177013410013"


def request(**changes) -> OrderRequest:
    base = {"market_id": MAN_UTD, "side": Side.BUY, "quantity": Decimal("12"), "price": Decimal("0.4166666667")}
    return OrderRequest(**{**base, **changes})


class Venue:
    """Answers requests by `METHOD /path`, and records them."""

    def __init__(self, **answers):
        self.answers = {"POST /bpapi/rest/security/session": S["login"], **answers}
        self.requests: list[httpx.Request] = []

    def __call__(self, req: httpx.Request) -> httpx.Response:
        self.requests.append(req)
        answer = self.answers.get(f"{req.method} {req.url.path}")
        if answer is None:
            return httpx.Response(404, json={"errors": [{"messages": ["Resource not found."], "codes": [None]}]})
        if callable(answer):
            answer = answer(req)
        if isinstance(answer, httpx.Response):
            return answer
        return httpx.Response(200, json=answer)

    def sent(self, method: str, path: str) -> list[httpx.Request]:
        return [r for r in self.requests if r.method == method and r.url.path == path]


class Catalog:
    def fetch_fee_schedule(self, market_id):
        from synpath.matchbook import fee_schedule_of
        return fee_schedule_of(market_id)

    def close(self):
        pass


def adapter(venue: Venue) -> MatchbookTrading:
    creds = MatchbookCredentials(username="jbloggs", password="verysecurepassword")
    return MatchbookTrading(
        creds, catalog=Catalog(), reports_limiter=None,
        client=httpx.AsyncClient(transport=httpx.MockTransport(venue)),
        session_client=httpx.Client(transport=httpx.MockTransport(venue)),
    )


def run(coro):
    return asyncio.run(coro)


def offer(row: dict, **changes) -> dict:
    return {**row, **changes}


class TestOffers:
    def test_a_buy_backs_at_the_odds_of_its_price(self):
        assert build_offer(request()) == {
            "runner-id": 401525949430009, "side": "back", "odds": 2.4, "stake": 5, "keep-in-play": False,
        }

    def test_a_sell_lays_and_stakes_the_backers_stake(self):
        item = build_offer(request(side=Side.SELL, price=Decimal("0.4"), quantity=Decimal("10")))
        assert item["side"] == "lay" and item["odds"] == 2.5 and item["stake"] == 4

    def test_odds_snap_to_the_ladder_on_the_cheaper_side(self):
        # 1/0.41 = 2.439: a buy pays no more (odds 2.44), a sell receives no less (odds 2.42).
        assert odds_for(Side.BUY, Decimal("0.41")) == Decimal("2.44")
        assert odds_for(Side.SELL, Decimal("0.41")) == Decimal("2.42")
        assert odds_for(Side.BUY, Decimal("0.5")) == odds_for(Side.SELL, Decimal("0.5")) == Decimal("2")

    def test_prices_off_the_ladder_are_refused(self):
        with pytest.raises(InvalidOrder):
            odds_for(Side.BUY, Decimal("0.0005"))
        with pytest.raises(InvalidOrder):
            odds_for(Side.SELL, Decimal("0.995"))
        with pytest.raises(InvalidOrder):
            odds_for(Side.BUY, Decimal("1"))

    def test_stakes_round_down_to_the_penny(self):
        assert stake_for(Decimal("10"), Decimal("3")) == Decimal("3.33")
        with pytest.raises(InvalidOrder, match="penny"):
            stake_for(Decimal("0.01"), Decimal("3"))

    def test_keep_in_play_is_passed_through(self):
        assert build_offer(request(params={"keep_in_play": True}))["keep-in-play"] is True

    def test_sell_yes_and_buy_no_are_the_same_lay(self):
        lay = build_offer(request(side=Side.SELL))
        assert build_offer(request(side=Side.SELL, outcome="yes", action="sell")) == lay
        assert build_offer(request(side=Side.SELL, outcome="no", action="buy")) == lay

    @pytest.mark.parametrize("changes", [
        {"time_in_force": TimeInForce.FOK}, {"time_in_force": TimeInForce.GTD, "expires_at": 1},
        {"post_only": True}, {"type": OrderType.ICEBERG},
        {"type": OrderType.MARKET, "price": None}, {"market_id": "matchbook:410343160570009-410343161840009"},
    ])
    def test_what_matchbook_does_not_hold_is_refused(self, changes):
        from synpath.errors import BadRequest

        with pytest.raises(BadRequest):        # InvalidOrder is a BadRequest
            build_offer(request(**changes))


class TestRecords:
    def test_an_unmatched_offer(self):
        order = order_of_offer(S["submit_unmatched"]["offers"][0])
        assert order.id == OFFER_ID and order.market_id == MAN_UTD and order.side == Side.BUY
        assert order.status == OrderStatus.OPEN and order.quantity == Decimal("12.0")
        assert order.price == Decimal("0.4166666667") and order.filled == 0 and order.remaining == Decimal("12.0")
        assert order.created_at == 1487274572771

    def test_a_matched_offer_carries_its_bets(self):
        order = order_of_offer(S["submit_matched"]["offers"][0])
        assert order.status == OrderStatus.CLOSED and order.filled == order.quantity and order.remaining == 0
        assert order.average_price == Decimal("0.4166666667") and order.fee == 0

    def test_a_delayed_offer_is_pending(self):
        order = order_of_offer(S["submit_delayed"]["offers"][0])
        assert order.status == OrderStatus.PENDING and order.filled == 0 and order.info["delay"] == 6

    def test_a_lay_is_a_sell_whose_stake_is_the_backers(self):
        order = order_of_offer(S["offer_lay"])
        assert order.side == Side.SELL and order.status == OrderStatus.CLOSED
        assert order.quantity == Decimal("2") * Decimal("1.51021")
        # The documented liability is the stake times the odds less one.
        assert Decimal(str(S["offer_lay"]["potential-liability"])) == pytest.approx(Decimal("2") * Decimal("0.51021"), abs=Decimal("0.00001"))

    def test_cancelled_and_flushed_offers_end(self):
        assert order_of_offer(S["cancel"]).status == OrderStatus.CANCELED
        flushed = S["offers"]["offers"][1] | {"status": "flushed"}
        assert order_of_offer(flushed).status == OrderStatus.CANCELED

    def test_fills_are_the_matched_bets(self):
        [fill] = fills_of_offer(S["submit_matched"]["offers"][0])
        assert fill.id == "413176315170213" and fill.order_id == "413176315170013"
        assert fill.quantity == Decimal("12.0") and fill.price == Decimal("0.4166666667") and fill.side == Side.BUY

    def test_positions_net_backs_and_lays_per_runner(self):
        [position] = positions_of_bets(S["current_bets"])
        assert position.market_id == "matchbook:919619528170041-919619528410042-919619528480041"
        assert position.side == PositionSide.LONG and position.contracts == Decimal("5.00000") * Decimal("3.52000")
        assert position.inventory_no == 0 and position.margin == Decimal("5.00000")
        assert position.entry_price == (Decimal("5") / (Decimal("5") * Decimal("3.52"))).quantize(Decimal("1e-10"))
        lay = json.loads(json.dumps(S["current_bets"]))
        lay["markets"][0]["selections"].append({**lay["markets"][0]["selections"][0], "side": "lay"})
        [flat] = positions_of_bets(lay)
        assert flat.side == PositionSide.FLAT and flat.contracts == 0

    def test_settlements_read_the_bets_result_as_the_runners(self):
        [settled] = settlements_of(S["settled_bets"])
        assert settled.market_id == "matchbook:881453329700042-881454180790042-881454181050042"
        assert settled.held == PositionSide.LONG and settled.result == "no" and settled.won is False
        assert settled.pnl == Decimal("-7.10498985") and settled.timestamp is not None


class TestAdapter:
    def test_login_then_the_session_token_on_every_call(self):
        venue = Venue(**{"POST /edge/rest/v2/offers": S["submit_unmatched"]})

        async def go():
            async with adapter(venue) as mb:
                return await mb.create_order(request(client_order_id="c-1"))

        order = run(go())
        login, submit = venue.requests
        assert json.loads(login.content) == {"username": "jbloggs", "password": "verysecurepassword"}
        assert submit.headers["session-token"] == S["login"]["session-token"]
        assert json.loads(submit.content) == {"odds-type": "DECIMAL", "exchange-type": "back-lay", "offers": [
            {"runner-id": 401525949430009, "side": "back", "odds": 2.4, "stake": 5, "keep-in-play": False}]}
        assert order.status == OrderStatus.OPEN and order.client_order_id == "c-1"

    def test_a_refused_session_logs_in_again_once(self):
        answers = iter([httpx.Response(401, json={"errors": [{"messages": ["Please login to continue"]}]}),
                        httpx.Response(200, json=S["balance"])])
        venue = Venue(**{"GET /edge/rest/account/balance": lambda req: next(answers), "GET /edge/rest/account": S["account"]})

        async def go():
            async with adapter(venue) as mb:
                return await mb.fetch_balance()

        balance = run(go())
        assert len(venue.sent("POST", "/bpapi/rest/security/session")) == 2
        assert balance.currency == "GBP"           # from the login's account

    def test_a_market_order_cancels_its_unmatched_rest(self):
        venue = Venue(**{"POST /edge/rest/v2/offers": S["submit_unmatched"],
                         "DELETE /edge/rest/v2/offers": {"offers": [offer(S["submit_unmatched"]["offers"][0],
                                                                         status="cancelled")]}})

        async def go():
            async with adapter(venue) as mb:
                return await mb.create_order(request(type=OrderType.MARKET))

        order = run(go())
        [cancel] = venue.sent("DELETE", "/edge/rest/v2/offers")
        assert cancel.url.params["offer-ids"] == OFFER_ID
        assert order.status == OrderStatus.CANCELED and order.type == OrderType.MARKET

    def test_an_ioc_in_play_waits_out_the_delay_before_cancelling(self, monkeypatch):
        slept: list[float] = []

        async def sleep(seconds):
            slept.append(seconds)

        monkeypatch.setattr("synpath.trading.matchbook.asyncio.sleep", sleep)
        venue = Venue(**{"POST /edge/rest/v2/offers": S["submit_delayed"],
                         "DELETE /edge/rest/v2/offers": {"offers": []},
                         f"GET /edge/rest/v2/offers/{OFFER_ID}": S["submit_matched"]["offers"][0]})

        async def go():
            async with adapter(venue) as mb:
                return await mb.create_order(request(time_in_force=TimeInForce.IOC))

        order = run(go())
        assert slept == [6.5] and order.status == OrderStatus.CLOSED

    def test_a_matched_order_is_not_cancelled(self):
        venue = Venue(**{"POST /edge/rest/v2/offers": S["submit_matched"]})

        async def go():
            async with adapter(venue) as mb:
                return await mb.create_order(request(type=OrderType.MARKET))

        assert run(go()).status == OrderStatus.CLOSED and venue.sent("DELETE", "/edge/rest/v2/offers") == []

    def test_create_orders_returns_one_result_each(self):
        def submit(req):
            sent = json.loads(req.content)["offers"]
            row = S["submit_unmatched"]["offers"][0]
            return {"offers": [row, {"runner-id": sent[1]["runner-id"], "status": "failed",
                                     "errors": [{"messages": ["Insufficient funds."]}]}]}

        venue = Venue(**{"POST /edge/rest/v2/offers": submit})

        async def go():
            async with adapter(venue) as mb:
                return await mb.create_orders([request(), request(), request(post_only=True)])

        placed, failed, invalid = run(go())
        assert placed.status == OrderStatus.OPEN
        assert isinstance(failed, OrderRejected) and "Insufficient funds" in str(failed)
        assert isinstance(invalid, InvalidOrder)
        assert len(json.loads(venue.sent("POST", "/edge/rest/v2/offers")[0].content)["offers"]) == 2

    def test_a_refused_submit_is_typed(self):
        refused = {"errors": [{"messages": ["Insufficient funds to place the offer."]}]}
        venue = Venue(**{"POST /edge/rest/v2/offers": httpx.Response(400, json=refused)})

        async def go():
            async with adapter(venue) as mb:
                return await mb.create_order(request())

        with pytest.raises(InsufficientFunds):
            run(go())

    def test_cancel_and_a_refused_cancel_read_back(self):
        venue = Venue(**{
            f"DELETE /edge/rest/v2/offers/{OFFER_ID}": S["cancel"],
            "DELETE /edge/rest/v2/offers/1": httpx.Response(400, json=S["cancel_refused"]),
            "GET /edge/rest/v2/offers/1": offer(S["cancel"], id=1),
        })

        async def go():
            async with adapter(venue) as mb:
                return await mb.cancel_order(OFFER_ID), await mb.cancel_order("1")

        cancelled, already = run(go())
        assert cancelled.status == OrderStatus.CANCELED and already.status == OrderStatus.CANCELED

    def test_cancel_all_on_a_runner_a_market_or_everywhere(self):
        venue = Venue(**{"DELETE /edge/rest/v2/offers": S["cancel_many"]})

        async def go():
            async with adapter(venue) as mb:
                return (await mb.cancel_all_orders(market_id=MAN_UTD),
                        await mb.cancel_all_orders(market_id="matchbook:410343160570009-410343161840009"),
                        await mb.cancel_all_orders())

        assert run(go()) == (1, 1, 1)
        runner, market, everything = venue.sent("DELETE", "/edge/rest/v2/offers")
        assert runner.url.params["runner-ids"] == "401525949430009"
        assert market.url.params["market-ids"] == "410343161840009" and not everything.url.params

    def test_edit_sends_the_current_and_new_odds_and_stake(self):
        current = order_of_offer(offer(S["submit_unmatched"]["offers"][0]))
        venue = Venue(**{f"PUT /edge/rest/v2/offers/{OFFER_ID}": offer(S["edit"], id=int(OFFER_ID))})

        async def go():
            async with adapter(venue) as mb:
                return await mb.edit_order(EditRequest(order_id=OFFER_ID, price=Decimal("0.3333333333"),
                                                       quantity=Decimal("300")), current=current)

        order = run(go())
        [sent] = venue.sent("PUT", f"/edge/rest/v2/offers/{OFFER_ID}")
        assert json.loads(sent.content) == {"current-odds": 2.4, "new-odds": 3, "current-stake": 5, "new-stake": 100}
        assert order.status == OrderStatus.OPEN and order.price == Decimal("0.3333333333")

    def test_a_refused_edit_says_why(self):
        current = order_of_offer(S["submit_unmatched"]["offers"][0])
        venue = Venue(**{f"PUT /edge/rest/v2/offers/{OFFER_ID}": httpx.Response(400, json=S["edit_refused"])})

        async def go():
            async with adapter(venue) as mb:
                return await mb.edit_order(EditRequest(order_id=OFFER_ID, price=Decimal("0.5")), current=current)

        with pytest.raises(InvalidOrder, match="Wrong current odds"):
            run(go())

    def test_open_orders_page_through(self):
        pages = iter([{**S["offers"], "total": 3}, {"offers": [S["offers"]["offers"][1]], "total": 3}])
        venue = Venue(**{"GET /edge/rest/v2/offers": lambda req: next(pages)})

        async def go():
            async with adapter(venue) as mb:
                return await mb.fetch_open_orders(market_id=MAN_UTD)

        assert len(run(go())) == 3
        first, second = venue.sent("GET", "/edge/rest/v2/offers")
        assert first.url.params["status"] == "open,edited" and first.url.params["runner-ids"] == "401525949430009"
        assert second.url.params["offset"] == "2"

    def test_my_trades_are_the_matched_bets(self):
        venue = Venue(**{"GET /edge/rest/v2/offers": S["offers"]})

        async def go():
            async with adapter(venue) as mb:
                return await mb.fetch_my_trades()

        fills = run(go())
        assert [f.id for f in fills] == ["413176315170213"] and fills.next_cursor is None
        assert venue.sent("GET", "/edge/rest/v2/offers")[0].url.params["status"] == "matched"

    def test_fetch_orders_pages_by_offset(self):
        venue = Venue(**{"GET /edge/rest/v2/offers": {**S["offers"], "total": 5}})

        async def go():
            async with adapter(venue) as mb:
                return await mb.fetch_orders(status="canceled", limit=2)

        page = run(go())
        assert page.next_cursor == "2"
        assert venue.sent("GET", "/edge/rest/v2/offers")[0].url.params["status"] == "cancelled,flushed"

    def test_balance_positions_and_settlements(self):
        venue = Venue(**{
            "GET /edge/rest/account/balance": {**S["balance"], "balance": 100.5, "exposure": -5, "free-funds": 95.5},
            "GET /edge/rest/reports/v2/bets/current": S["current_bets"],
            "GET /edge/rest/reports/v2/bets/settled": S["settled_bets"],
        })

        async def go():
            async with adapter(venue) as mb:
                return await mb.fetch_balance(), await mb.fetch_positions(), await mb.fetch_settlements()

        balance, positions, settlements = run(go())
        assert (balance.total, balance.available, balance.locked) == (Decimal("100.5"), Decimal("95.5"), Decimal("5"))
        assert positions[0].contracts == Decimal("17.6") and len(settlements) == 1
        assert venue.sent("GET", "/edge/rest/reports/v2/bets/current")[0].url.params["odds-type"] == "DECIMAL"

    def test_the_fee_is_commission_on_what_the_bet_wins(self):
        venue = Venue(**{"GET /edge/rest/account": S["account"]})

        async def go():
            async with adapter(venue) as mb:
                return (await mb.fetch_fee_estimate(MAN_UTD, Side.BUY, Decimal("0.4"), Decimal("100")),
                        await mb.fetch_fee_estimate(MAN_UTD, Side.SELL, Decimal("0.4"), Decimal("100")))

        buy, sell = run(go())
        assert buy.taker_fee == Decimal("1.2") and sell.taker_fee == Decimal("0.8") and buy.maker_fee == buy.taker_fee

    def test_heartbeat_on_and_off(self):
        venue = Venue(**{"POST /edge/rest/v1/heartbeat": S["heartbeat"], "DELETE /edge/rest/v1/heartbeat": S["heartbeat_off"]})

        async def go():
            async with adapter(venue) as mb:
                return await mb.heartbeat(20), await mb.heartbeat(0)

        on, off = run(go())
        assert on["actual-timeout"] == 20 and off["action-performed"] == "HEARTBEAT_TERMINATED"
        assert json.loads(venue.sent("POST", "/edge/rest/v1/heartbeat")[0].content) == {"timeout": 20}

    def test_an_unknown_offer_is_not_found(self):
        from synpath.trading.errors import OrderNotFound

        async def go():
            async with adapter(Venue()) as mb:
                return await mb.fetch_order("999")

        with pytest.raises(OrderNotFound):
            run(go())


class TestWiring:
    def test_credentials_are_needed(self):
        with pytest.raises(CredentialsMissing):
            MatchbookTrading(None)

    def test_the_client_and_the_engine_build_it(self):
        import synpath
        from synpath.engine.__main__ import ADAPTERS

        creds = MatchbookCredentials(username="u", password="p")
        trading = synpath.Client({"matchbook": creds}).trading("matchbook")
        try:
            assert isinstance(trading, MatchbookTrading)
        finally:
            run(trading.close())
        assert ADAPTERS["matchbook"] == ("..trading.matchbook", "MatchbookTrading")

    def test_doctor_names_the_login(self, tmp_path, monkeypatch, capsys):
        from synpath.trading.__main__ import doctor

        for name in ("MATCHBOOK_USERNAME", "MATCHBOOK_PASSWORD"):
            monkeypatch.delenv(name, raising=False)
        env = tmp_path / ".env"
        env.write_text("MATCHBOOK_USERNAME=jbloggs\nMATCHBOOK_PASSWORD=secret-pw-123\n")
        assert doctor(str(env)) == 0
        out = capsys.readouterr().out
        assert "user jbloggs" in out and "secret-pw-123" not in out
