"""SX Bet order entry: signing checked against an independent EIP-712 encoder,
order bodies, REST records, the account stream, and the adapter against a
scripted venue. Records follow the venue's documented examples (they need an
account)."""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import httpx
import pytest
from eth_account import Account as EthAccount
from eth_account.messages import _hash_eip191_message, encode_typed_data

from synpath.trading import sx_bet_signing as sig
from synpath.trading.credentials import CredentialsMissing, SxBetCredentials, load_sx_bet
from synpath.trading.errors import InsufficientFunds, InvalidOrder, OrderRejected
from synpath.trading.sx_bet import (
    SxBetTrading, build_signed_order, fill_of_row, order_after_placing, order_of_row, position_of,
)
from synpath.trading.polymarket_signing import WalletSigner
from synpath.trading.types import (
    Account, Liquidity, OrderRequest, OrderStatus, OrderType, PositionSide, SettlementState, Side, TimeInForce,
)

KEY = "0x" + "11" * 32
OWNER = EthAccount.from_key(KEY)
HASH = "0x" + "ab" * 32
MARKET_ID = f"sx_bet:{HASH}"
META = {
    "chainId": 4162,
    "domain": {"name": "OBv3 Escrow", "version": "1", "chainId": 4162,
               "verifyingContract": "0x890482680C3a0116aBB003B17e7D694D13a6c1eB"},
    "activeAsset": {"symbol": "USDC", "baseToken": "0x6629Ce1Cf35Cc1329ebB4F63202F3f197b3F050B",
                    "escrowAddress": "0x890482680C3a0116aBB003B17e7D694D13a6c1eB", "decimals": 6},
    "oddsLadderStepSize": 125,
    "limits": {"orderSizeMinimumBaseUnits": "1000000", "maxCreateOrders": 10, "maxCancelOrders": 100},
}
ORDER_ROW = {
    "id": "0xd9b693a1075ab77c0638fe98fcd3144dbb8f91d0f7ee454d442bf25ca7cbcb66", "marketHash": HASH,
    "userAddress": OWNER.address, "wallet": "0x4361123dbdc1D812fdf7D27045aF358C9C8AA70A",
    "isBettingOutcomeOne": True, "percentageOdds": "40000000000000000000", "totalBetSize": "2000000",
    "remainingSize": "1000000", "expiry": "2026-07-31T19:45:07.000Z", "status": "ACTIVE", "inactiveReason": None,
    "eventId": "L12952568", "createdAt": "2026-07-31T18:45:07.437Z", "updatedAt": "2026-07-31T18:45:07.456Z",
}
FILL_ROW = {
    "id": "0xec8e:0x02d2:" + HASH, "matchId": "0xec8e", "orderId": "0x02d2", "tradeId": "0xac6b",
    "marketHash": HASH, "isParlay": False, "fillAmount": "333334", "fillOdds": "40000000000000000000",
    "returnAmount": "833335", "netReturnAmount": "829335", "isBettingOutcomeOne": False, "isMaker": False,
    "status": "LOCKED", "createdAt": "2026-07-31T18:47:09.586Z", "settlement": None,
}
POSITION_ROW = {
    "marketHash": HASH, "market": {"status": "ACTIVE"}, "totalStake": "300000000", "maxWin": "200000000",
    "netMaxWin": "198400000", "maxLoss": "-100000000", "isOutcomeOneMaxWin": True, "pnl": None,
    "updatedAt": "2026-03-13T22:03:41.502Z",
}


def request(**changes) -> OrderRequest:
    base = {"market_id": MARKET_ID, "side": Side.BUY, "quantity": Decimal("10"), "price": Decimal("0.4")}
    return OrderRequest(**{**base, **changes})


def built(req: OrderRequest):
    return build_signed_order(req, signer=WalletSigner(KEY), meta=META, salt=7, now=1_800_000_000)


class TestSigning:
    def test_the_digest_and_signature_are_eip712(self):
        order = sig.build_order(market_hash=HASH, base_token=META["activeAsset"]["baseToken"], stake=4_000_000,
                                odds=40 * 10 ** 18, expiry=1_800_000_000, maker=OWNER.address, outcome_one=True, salt=12345)
        typed = sig.order_typed_data({**order, "marketHash": bytes.fromhex(HASH[2:])}, domain=META["domain"])
        encoded = encode_typed_data(full_message=typed)
        assert sig.order_digest(order, domain=META["domain"]) == _hash_eip191_message(encoded)
        assert sig.sign_order(WalletSigner(KEY), order, domain=META["domain"]) == \
            "0x" + OWNER.sign_message(encoded).signature.hex().removeprefix("0x")

    def test_the_docs_ladder(self):
        assert sig.on_ladder(40_125 * 10 ** 15, 125) and not sig.on_ladder(40_100 * 10 ** 15, 125)
        assert sig.on_ladder(10 ** 17, 125) and sig.on_ladder(999 * 10 ** 17, 125)
        assert not sig.on_ladder(0, 125) and not sig.on_ladder(10 ** 20, 125)
        assert sig.round_down(40_100 * 10 ** 15, 125) == 40 * 10 ** 18

    def test_a_buy_stakes_contracts_times_its_price(self):
        assert sig.bet(True, Decimal("0.4"), Decimal("10"), step_size=125, min_stake=1_000_000) == (40 * 10 ** 18, 4_000_000)

    def test_a_sell_bets_outcome_two_at_one_minus_the_price(self):
        assert sig.bet(False, Decimal("0.4"), Decimal("10"), step_size=125, min_stake=1_000_000) == (60 * 10 ** 18, 6_000_000)

    def test_odds_round_to_the_cheaper_side(self):
        buy_odds, _ = sig.bet(True, Decimal("0.4013"), Decimal("10"), step_size=125, min_stake=1)
        sell_odds, _ = sig.bet(False, Decimal("0.4013"), Decimal("10"), step_size=125, min_stake=1)
        assert buy_odds == 40_125 * 10 ** 15  # pays no more than 40.13%
        assert sell_odds == 59_750 * 10 ** 15  # pays no more than 59.87% for outcome two

    def test_the_minimum_stake(self):
        with pytest.raises(InvalidOrder, match="minimum"):
            sig.bet(True, Decimal("0.4"), Decimal("2"), step_size=125, min_stake=1_000_000)
        with pytest.raises(InvalidOrder):
            sig.bet(True, Decimal("1"), Decimal("10"), step_size=125, min_stake=1)

    def test_contracts_are_the_stake_over_the_odds(self):
        assert sig.contracts_of(2_000_000, 40 * 10 ** 18) == Decimal("5")


class TestOrders:
    def test_a_limit_rests_gtc_for_thirty_days(self):
        item, order_id, odds, stake = built(request(client_order_id="c-1"))
        assert item["timeInForce"] == "GTC" and item["clientOrderId"] == "c-1"
        assert item["expiry"] == 1_800_000_000 + 30 * 86400 and item["maker"] == OWNER.address
        assert (item["percentageOdds"], item["totalBetSize"]) == (str(40 * 10 ** 18), "4000000")
        assert item["isMakerBettingOutcomeOne"] is True and order_id.startswith("0x") and len(order_id) == 66

    def test_the_signature_recovers_the_wallet(self):
        item, _, _, _ = built(request())
        fields = {k: item[k] for k in ("marketHash", "baseToken", "totalBetSize", "percentageOdds", "salt", "expiry",
                                       "maker", "isMakerBettingOutcomeOne")}
        fields.update(marketHash=bytes.fromhex(item["marketHash"][2:]), totalBetSize=int(fields["totalBetSize"]),
                      percentageOdds=int(fields["percentageOdds"]), salt=int(fields["salt"]))
        typed = sig.order_typed_data(fields, domain=META["domain"])
        assert EthAccount.recover_message(encode_typed_data(full_message=typed), signature=item["orderSignature"]) == OWNER.address

    @pytest.mark.parametrize("changes, tif", [
        ({"type": OrderType.MARKET}, "IOC"),
        ({"time_in_force": TimeInForce.IOC}, "IOC"),
        ({"time_in_force": TimeInForce.FOK}, "FOK"),
        ({"time_in_force": TimeInForce.GTD, "expires_at": 1_800_003_600_000}, "GTC"),
    ])
    def test_time_in_force(self, changes, tif):
        item, _, _, _ = built(request(**changes))
        assert item["timeInForce"] == tif
        if "expires_at" in changes:
            assert item["expiry"] == 1_800_003_600

    def test_post_only_rests_only(self):
        item, _, _, _ = built(request(post_only=True))
        assert item["postOnly"] is True
        with pytest.raises(InvalidOrder):
            built(request(post_only=True, time_in_force=TimeInForce.IOC))

    @pytest.mark.parametrize("changes", [
        {"type": OrderType.ICEBERG},
        {"reduce_only": True},
        {"time_in_force": TimeInForce.DAY},
        {"time_in_force": TimeInForce.GTD, "expires_at": 1_000},
    ])
    def test_refused_before_signing(self, changes):
        with pytest.raises(InvalidOrder):
            built(request(**changes))

    @pytest.mark.parametrize("state, status, filled", [
        ("RESTED", OrderStatus.OPEN, Decimal("0")),
        ("FULLY_FILLED", OrderStatus.CLOSED, Decimal("10")),
        ("PARTIAL_FILL_DONE", OrderStatus.CANCELED, Decimal("5")),
        ("PARTIAL_FILL_RESTED", OrderStatus.OPEN, Decimal("5")),
        ("CANCELLED", OrderStatus.CANCELED, Decimal("0")),
        ("TIMEOUT", OrderStatus.PENDING, Decimal("0")),
    ])
    def test_what_became_of_it_on_arrival(self, state, status, filled):
        outcome = {"state": state}
        if filled:
            outcome.update(fillAmount=str(int(filled * Decimal("0.4") * 1_000_000)), blendedOdds=str(40 * 10 ** 18))
        if state == "PARTIAL_FILL_RESTED":
            outcome["remainingAmount"] = "2000000"
        order = order_after_placing(request(), {"orderId": "0xabc", "status": "SUBMITTED", "outcome": outcome},
                                    order_id="0xabc", odds=40 * 10 ** 18, stake=4_000_000, account=Account(venue="sx_bet"))
        assert (order.status, order.filled) == (status, filled)
        if state == "PARTIAL_FILL_RESTED":
            assert order.remaining == Decimal("5")
        if filled:
            assert order.average_price == Decimal("0.4")


class TestRecords:
    def test_an_order_row(self):
        order = order_of_row(ORDER_ROW)
        assert (order.side, order.price, order.quantity, order.filled, order.remaining, order.status) == \
            (Side.BUY, Decimal("0.4"), Decimal("5"), Decimal("2.5"), Decimal("2.5"), OrderStatus.OPEN)

    @pytest.mark.parametrize("reason, status", [
        ("FILLED", OrderStatus.CLOSED), ("EXPIRED", OrderStatus.EXPIRED), ("USER_REQUESTED", OrderStatus.CANCELED),
        ("EVENT_LIFECYCLE", OrderStatus.CANCELED), ("HEARTBEAT_TIMEOUT", OrderStatus.CANCELED),
    ])
    def test_inactive_reasons(self, reason, status):
        order = order_of_row({**ORDER_ROW, "status": "INACTIVE", "inactiveReason": reason})
        assert order.status == status and order.remaining == 0

    def test_an_outcome_two_order_is_a_sell_at_one_minus_its_odds(self):
        order = order_of_row({**ORDER_ROW, "isBettingOutcomeOne": False})
        assert (order.side, order.price) == (Side.SELL, Decimal("0.6"))

    def test_a_fill(self):
        fill = fill_of_row(FILL_ROW)
        assert (fill.side, fill.price, fill.quantity, fill.liquidity) == (Side.SELL, Decimal("0.6"), Decimal("0.833335"), Liquidity.TAKER)
        assert fill.settlement == SettlementState.CONFIRMED
        assert fill_of_row({**FILL_ROW, "status": "MATCHED"}).settlement == SettlementState.MATCHED
        assert fill_of_row({**FILL_ROW, "isParlay": True}) is None

    def test_a_position_nets_on_yes(self):
        position = position_of(POSITION_ROW)
        assert (position.side, position.contracts, position.margin) == (PositionSide.LONG, Decimal("300"), Decimal("300"))
        short = position_of({**POSITION_ROW, "isOutcomeOneMaxWin": False})
        assert (short.side, short.contracts) == (PositionSide.SHORT, Decimal("300"))


class TestCredentials:
    def test_a_key_and_an_api_key(self):
        creds = load_sx_bet({"SX_BET_PRIVATE_KEY": KEY[2:], "SX_BET_API_KEY": "k", "SX_BET_TESTNET": "1"})
        assert creds.private_key == KEY and creds.testnet and creds.address == OWNER.address
        assert "k" not in repr(creds) and KEY not in repr(creds)

    def test_the_api_key_is_required(self):
        with pytest.raises(CredentialsMissing):
            load_sx_bet({"SX_BET_PRIVATE_KEY": KEY})
        assert load_sx_bet({}) is None


class Venue:
    """Answers requests by `METHOD /path`, and records them."""

    def __init__(self, **answers):
        self.answers = {"GET /metadata/obv3": {"status": "success", "data": META}, **answers}
        self.requests: list[httpx.Request] = []

    def __call__(self, req: httpx.Request) -> httpx.Response:
        self.requests.append(req)
        key = f"{req.method} {req.url.path}"
        answer = self.answers.get(key)
        if answer is None:
            return httpx.Response(404, json={"message": "not found"})
        if callable(answer):
            answer = answer(req)
        if isinstance(answer, httpx.Response):
            return answer
        return httpx.Response(200, json=answer)

    def sent(self, method: str, path: str) -> list[httpx.Request]:
        return [r for r in self.requests if r.method == method and r.url.path == path]


class Catalog:
    def fetch_fee_schedule(self, market_id):
        from synpath.sx_bet import fee_schedule_of
        return fee_schedule_of(market_id)

    def close(self):
        pass


def adapter(venue: Venue) -> SxBetTrading:
    creds = SxBetCredentials(private_key=KEY, api_key="api-key")
    return SxBetTrading(creds, catalog=Catalog(), client=httpx.AsyncClient(transport=httpx.MockTransport(venue)))


def run(coro):
    return asyncio.run(coro)


def submitted(outcome: dict):
    def answer(req):
        body = json.loads(req.content)
        return {"status": "success", "data": {"orders": [
            {"orderId": "0xAAA", "status": "SUBMITTED", "commandId": "c", "outcome": outcome} for _ in body["orders"]]}}
    return answer


class TestAdapter:
    def test_an_order_is_signed_sent_with_the_key_and_read_from_the_answer(self):
        venue = Venue(**{"POST /orders-v3": submitted({"state": "FULLY_FILLED", "fillAmount": "4000000",
                                                       "blendedOdds": str(40 * 10 ** 18), "tradeId": "t"})})

        async def go():
            async with adapter(venue) as sx:
                return await sx.create_order(request())

        order = run(go())
        [sent] = venue.sent("POST", "/orders-v3")
        body = json.loads(sent.content)
        assert sent.headers["x-sx-api-key"] == "api-key" and body["waitForOutcome"] is True
        assert len(body["orders"]) == 1 and body["orders"][0]["orderSignature"].startswith("0x")
        assert (order.id, order.status, order.filled) == ("0xaaa", OrderStatus.CLOSED, Decimal("10"))

    def test_a_batch_keeps_a_bad_request_as_its_error(self):
        venue = Venue(**{"POST /orders-v3": submitted({"state": "RESTED"})})

        async def go():
            async with adapter(venue) as sx:
                return await sx.create_orders([request(), request(quantity=Decimal("1")), request(side=Side.SELL)])

        good, bad, sell = run(go())
        assert good.status == OrderStatus.OPEN and isinstance(bad, InvalidOrder) and sell.side == Side.SELL
        [sent] = venue.sent("POST", "/orders-v3")
        assert len(json.loads(sent.content)["orders"]) == 2

    def test_a_failed_result_is_a_rejection(self):
        venue = Venue(**{"POST /orders-v3": {"status": "success", "data": {"orders": [
            {"orderId": "0x1", "status": "FAILED", "commandId": "c"}]}}})

        async def go():
            async with adapter(venue) as sx:
                return await sx.create_order(request())

        with pytest.raises(OrderRejected):
            run(go())

    @pytest.mark.parametrize("message, error", [
        ("PROXY_NOT_DEPLOYED", CredentialsMissing),
        ("INSUFFICIENT_BALANCE", InsufficientFunds),
        ("ODDS_NOT_ON_LADDER", InvalidOrder),
    ])
    def test_refusals_are_typed(self, message, error):
        venue = Venue(**{"POST /orders-v3": httpx.Response(400, json={"message": message, "marketHash": HASH})})

        async def go():
            async with adapter(venue) as sx:
                return await sx.create_order(request())

        with pytest.raises(error):
            run(go())

    def test_a_cancel_is_read_back(self):
        venue = Venue(**{
            "DELETE /orders-v3": {"status": "success", "data": {"cancelled": [{"orderId": ORDER_ROW["id"]}]}},
            f"GET /orders-v3/{ORDER_ROW['id']}": {"status": "success", "data": {
                **ORDER_ROW, "status": "INACTIVE", "inactiveReason": "USER_REQUESTED"}},
        })

        async def go():
            async with adapter(venue) as sx:
                return await sx.cancel_order(ORDER_ROW["id"])

        order = run(go())
        [sent] = venue.sent("DELETE", "/orders-v3")
        assert json.loads(sent.content) == {"orders": [{"orderId": ORDER_ROW["id"]}]}
        assert order.status == OrderStatus.CANCELED and order.filled == Decimal("2.5")

    def test_a_refused_cancel_of_a_live_order_is_an_error(self):
        venue = Venue(**{
            "DELETE /orders-v3": {"status": "success", "data": {"notCancelled": [{"orderId": ORDER_ROW["id"], "reason": "X"}]}},
            f"GET /orders-v3/{ORDER_ROW['id']}": {"status": "success", "data": ORDER_ROW},
        })

        async def go():
            async with adapter(venue) as sx:
                return await sx.cancel_orders([ORDER_ROW["id"]])

        assert isinstance(run(go())[0], OrderRejected)

    def test_cancel_all_on_a_market_or_everywhere(self):
        answer = {"status": "success", "data": {"cancelledSubmitted": [{"orderId": "0x1"}, {"orderId": "0x2"}]}}
        venue = Venue(**{"DELETE /orders-v3/market": answer, "DELETE /orders-v3/all": answer})

        async def go():
            async with adapter(venue) as sx:
                return await sx.cancel_all_orders(market_id=MARKET_ID), await sx.cancel_all_orders()

        assert run(go()) == (2, 2)
        [sent] = venue.sent("DELETE", "/orders-v3/market")
        assert sent.url.params["marketHash"] == HASH

    def test_open_orders_page_through(self):
        pages = iter([{"orders": [ORDER_ROW], "nextKey": "k"}, {"orders": [ORDER_ROW]}])
        venue = Venue(**{"GET /orders-v3": lambda req: {"status": "success", "data": next(pages)}})

        async def go():
            async with adapter(venue) as sx:
                return await sx.fetch_open_orders(market_id=MARKET_ID)

        assert len(run(go())) == 2
        assert venue.sent("GET", "/orders-v3")[1].url.params["nextKey"] == "k"

    def test_my_trades_are_filtered_to_the_market(self):
        other = {**FILL_ROW, "marketHash": "0x" + "cd" * 32}
        venue = Venue(**{"GET /fills-v3": {"status": "success", "data": {"fills": [FILL_ROW, other], "nextKey": "n"}}})

        async def go():
            async with adapter(venue) as sx:
                return await sx.fetch_my_trades(market_id=MARKET_ID)

        fills = run(go())
        assert [f.market_id for f in fills] == [MARKET_ID] and fills.next_cursor == "n"

    def test_balance_and_positions(self):
        venue = Venue(**{
            "GET /user/balance-v3": {"status": "success", "data": {"balances": [{
                "tokenAddress": META["activeAsset"]["baseToken"], "availableAmount": "1028000014",
                "pendingAvailableAmount": "0", "escrowedAmount": "5000000", "pendingEscrowAmount": "0"}]}},
            "GET /positions-v3": {"status": "success", "data": {"positions": [POSITION_ROW]}},
        })

        async def go():
            async with adapter(venue) as sx:
                return await sx.fetch_balance(), await sx.fetch_positions()

        balance, positions = run(go())
        assert (balance.available, balance.locked, balance.total) == (Decimal("1028.000014"), Decimal("5"), Decimal("1033.000014"))
        assert positions[0].contracts == Decimal("300")
        assert venue.sent("GET", "/positions-v3")[0].url.params["status"] == "MATCHED,LOCKED"

    def test_the_fee_is_on_the_profit_of_the_outcome_bet(self):
        async def go():
            async with adapter(Venue()) as sx:
                return (await sx.fetch_fee_estimate(MARKET_ID, Side.BUY, Decimal("0.4"), Decimal("100")),
                        await sx.fetch_fee_estimate(MARKET_ID, Side.SELL, Decimal("0.4"), Decimal("100")))

        buy, sell = run(go())
        assert buy.taker_fee == Decimal("0.6") and sell.taker_fee == Decimal("0.4") and buy.maker_fee == 0

    def test_heartbeat(self):
        venue = Venue(**{"POST /heartbeat/v3": {"status": "success", "data": {"timeoutSeconds": 300}}})

        async def go():
            async with adapter(venue) as sx:
                return await sx.heartbeat(300)

        assert run(go())["timeoutSeconds"] == 300
        assert json.loads(venue.sent("POST", "/heartbeat/v3")[0].content) == {"timeoutSeconds": 300}


class TestUserStream:
    def test_orders_and_fills_arrive_on_the_accounts_channels(self):
        from synpath import SxBetUserStream
        from synpath.ws.base import FillEvent, OrderEvent

        stream = SxBetUserStream(token="tok", address=OWNER.address.lower())
        assert stream.address == OWNER.address

        async def go():
            await stream.watch_orders()
            await stream.watch_my_trades()
            order = stream.on_publication(f"account:orders_v3_#{OWNER.address}", {"order": ORDER_ROW})
            fill = stream.on_publication(f"account:fills_v3_#{OWNER.address}", {"fill": FILL_ROW})
            return stream.channels, order, fill

        channels, [order], [fill] = asyncio.run(go())
        assert channels == {f"account:orders_v3_#{OWNER.address}", f"account:fills_v3_#{OWNER.address}"}
        assert isinstance(order, OrderEvent) and order.order.status == OrderStatus.OPEN
        assert isinstance(fill, FillEvent) and fill.fill.side == Side.SELL

    def test_the_engine_gets_both_streams(self):
        from synpath.engine.feeds import default_streams

        creds = SxBetCredentials(private_key=KEY, api_key="api-key", testnet=True)
        streams = default_streams({"sx_bet": object()}, {"sx_bet": creds})["sx_bet"]
        assert streams.market.url.startswith("wss://realtime.toronto") and streams.private.address == OWNER.address
