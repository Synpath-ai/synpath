"""Opinion order entry, offline.

Signing is checked byte for byte against vectors produced by the venue's own
SDK code (`opinion_clob_sdk` 0.7, its order builder and amount rounding) with
the public Hardhat test key. Request and response shapes follow the SDK's
generated client (`opinion_api` 0.4). Not yet run against the live venue.
"""
from __future__ import annotations

from decimal import Decimal

import pytest
from eth_account import Account as EthAccount
from eth_account.messages import encode_typed_data

from synpath.errors import BadRequest
from synpath.opinion import fee_schedule_of, normalize_market
from synpath.trading import opinion_signing as sig
from synpath.trading.credentials import OpinionCredentials
from synpath.trading.errors import CredentialsMissing, InsufficientFunds, InvalidOrder, OrderRejected
from synpath.trading.opinion import (
    OpinionTrading, api_key_typed_data, build_signed_order, create_api_key, fill_of, order_of, position_of,
    MarketInfo,
)
from synpath.trading.polymarket_signing import WalletSigner
from synpath.trading.types import (
    OrderRequest, OrderStatus, OrderType, PositionSide, SettlementState, Side, TimeInForce,
)

from conftest import load

D = Decimal
pytestmark = pytest.mark.anyio
VECTORS = load("opinion_signing_vectors.json")
KEY = VECTORS["private_key"]
SAFE = VECTORS["safe"]
EXCHANGE = VECTORS["exchange"]
USDT = "0x55d398326f99059fF775485246999027B3197955"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def signer():
    return WalletSigner(KEY)


@pytest.fixture
def info(opinion_market):
    return MarketInfo(
        native="8453", yes_token=opinion_market["yesTokenId"], no_token=opinion_market["noTokenId"],
        quote_token=USDT, exchange=EXCHANGE,
    )


def request(**kwargs):
    base = {"market_id": "opinion:8453", "side": Side.BUY, "amount": D("100"), "price": D("0.45")}
    return OrderRequest(**{**base, **kwargs})


class TestSigning:
    @pytest.mark.parametrize("case", VECTORS["cases"], ids=lambda c: c["name"])
    def test_amounts_and_signature_match_the_venue_sdk(self, case, signer):
        side = "BUY" if case["side"] == 0 else "SELL"
        wei = sig.to_wei(D(case["maker_input"]))
        maker, taker = sig.limit_amounts(side, D(case["price"]), wei) if case["trading_method"] == 2 else (wei, 0)
        assert (str(maker), str(taker)) == (case["order"]["makerAmount"], case["order"]["takerAmount"])
        order = sig.build_order(maker=SAFE, signer=signer.address, token_id=case["order"]["tokenId"],
                                maker_amount=maker, taker_amount=taker, side=side, salt=int(case["order"]["salt"]))
        assert sig.sign_order(signer, order, exchange=EXCHANGE) == case["signature"]

    def test_the_amounts_state_the_price_exactly(self):
        maker, taker = sig.limit_amounts("BUY", D("0.071"), sig.to_wei(D("6.2")))
        assert D(maker) / D(taker) == D("0.071")
        maker, taker = sig.limit_amounts("SELL", D("0.723"), sig.to_wei(D("37.5")))
        assert D(taker) / D(maker) == D("0.723")

    @pytest.mark.parametrize("price", ["0", "0.0005", "0.9995", "1", "0.1234567"])
    def test_prices_outside_the_venue_rules_are_refused(self, price):
        with pytest.raises(InvalidOrder):
            sig.check_price(D(price))


class TestBuild:
    def test_a_buy_buys_the_yes_token(self, signer, info):
        body, contracts = build_signed_order(request(), info, signer=signer, maker=SAFE, now_s=1700000000, salt=5)
        assert body["tokenId"] == info.yes_token and body["side"] == "0" and body["price"] == "0.45"
        assert body["makerAmount"] == str(45 * 10**18) and body["takerAmount"] == str(100 * 10**18)
        assert body["topicId"] == 8453 and body["maker"] == SAFE and body["signer"] == signer.address
        assert body["signatureType"] == "2" and body["tradingMethod"] == 2 and body["currencyAddress"] == USDT
        assert body["timestamp"] == 1700000000 and body["sign"] == body["signature"]
        assert contracts == D("100")

    def test_the_signature_is_the_wallets_over_the_market_exchange(self, signer, info):
        body, _ = build_signed_order(request(), info, signer=signer, maker=SAFE, now_s=1, salt=5)
        order = sig.build_order(maker=SAFE, signer=signer.address, token_id=info.yes_token,
                                maker_amount=int(body["makerAmount"]), taker_amount=int(body["takerAmount"]),
                                side="BUY", salt=5)
        message = encode_typed_data(full_message=sig.order_typed_data(order, exchange=EXCHANGE))
        assert EthAccount.recover_message(message, signature=body["signature"]) == signer.address

    def test_a_sell_buys_the_no_token_at_the_complement(self, signer, info):
        body, contracts = build_signed_order(request(side=Side.SELL, price=D("0.55")), info, signer=signer,
                                             maker=SAFE, now_s=1, salt=5)
        assert body["tokenId"] == info.no_token and body["side"] == "0" and body["price"] == "0.45"
        assert contracts == D("100")

    def test_reduce_only_sells_the_tokens_held(self, signer, info):
        body, contracts = build_signed_order(request(side=Side.SELL, price=D("0.6"), reduce_only=True), info,
                                             signer=signer, maker=SAFE, now_s=1, salt=5)
        assert body["tokenId"] == info.yes_token and body["side"] == "1"
        assert body["makerAmount"] == str(100 * 10**18) and contracts == D("100")

    def test_market_orders_are_limits_at_the_worst_price(self, signer, info):
        body, _ = build_signed_order(request(type=OrderType.MARKET, price=D("0.5")), info, signer=signer,
                                     maker=SAFE, now_s=1, salt=5)
        assert body["tradingMethod"] == 2 and body["price"] == "0.5"

    def test_the_size_is_trimmed_as_the_venue_needs(self, signer, info):
        _, contracts = build_signed_order(request(amount=D("123.456"), price=D("0.5")), info, signer=signer,
                                          maker=SAFE, now_s=1, salt=5)
        assert contracts == D("123.46")

    @pytest.mark.parametrize("change, reason", [
        ({"amount": D("10"), "price": D("0.3")}, "minimum order"),
        ({"time_in_force": TimeInForce.FOK}, "not available"),
        ({"time_in_force": TimeInForce.GTD, "expires_at": 4102444800000}, "not available"),
        ({"post_only": True}, "post-only"),
        ({"type": OrderType.STOP_LIMIT, "stop_price": D("0.4")}, "engine"),
        ({"market_id": "kalshi:KXFOO"}, "belongs to kalshi"),
    ])
    def test_what_the_venue_cannot_take_is_refused_before_sending(self, signer, info, change, reason):
        with pytest.raises(InvalidOrder, match=reason):
            build_signed_order(request(**change), info, signer=signer, maker=SAFE, now_s=1, salt=5)


class TestRecords:
    def test_a_rest_order_row(self):
        row = {"orderId": "o-1", "marketId": 8453, "side": 2, "outcomeSide": 1, "price": "0.6",
               "orderShares": "50", "filledShares": "20", "filledAmount": "12", "status": 1,
               "tradingMethod": 2, "createdAt": 1766735464}
        order = order_of(row)
        assert order.side == Side.SELL and order.price == D("0.6")
        assert (order.amount, order.filled, order.remaining, order.cost) == (D("50"), D("20"), D("30"), D("12"))

    def test_a_rest_trade_row(self):
        row = {"tradeNo": "t-1", "orderNo": "o-1", "marketId": 8453, "side": "Sell", "outcomeSide": 2,
               "price": "0.2", "shares": "10", "feeFormatted": "0.04", "status": 2, "createdAt": 1766735571}
        fill = fill_of(row)
        assert fill.order_id == "o-1" and fill.side == Side.BUY and fill.price == D("0.8")
        assert fill.fee == D("0.04") and fill.settlement == SettlementState.CONFIRMED

    def test_positions_net_the_two_tokens(self):
        rows = [
            {"marketId": 8453, "outcomeSide": 1, "sharesOwned": "30", "avgEntryPrice": "0.4",
             "currentValueInQuoteToken": "15", "unrealizedPnl": "3", "marketStatus": 2},
            {"marketId": 8453, "outcomeSide": 2, "sharesOwned": "10", "avgEntryPrice": "0.5",
             "currentValueInQuoteToken": "5", "unrealizedPnl": "-1", "marketStatus": 2},
        ]
        position = position_of(rows, market_id="opinion:8453")
        assert position.side == PositionSide.LONG and position.contracts == D("20")
        assert (position.inventory_yes, position.inventory_no) == (D("30"), D("10"))
        assert position.entry_price == D("0.4") and position.mark_price == D("0.5")
        assert position.unrealized_pnl == D("2") and not position.resolved

    def test_a_no_position_is_short_on_the_yes_leg(self):
        rows = [{"marketId": 1, "outcomeSide": 2, "sharesOwned": "8", "avgEntryPrice": "0.3",
                 "currentValueInQuoteToken": "4", "marketStatus": 4, "claimStatusEnum": "WaitClaim"}]
        position = position_of(rows, market_id="opinion:1")
        assert position.side == PositionSide.SHORT and position.entry_price == D("0.7")
        assert position.resolved and position.redeemable == D("4")


# ---------------------------------------------------------------------------
# The adapter, against a scripted venue
# ---------------------------------------------------------------------------

def ok(result):
    return {"errno": 0, "errmsg": "", "result": result}


class FakeHttp:
    """Answers each `(method, path)` from a script and records every call."""

    def __init__(self, answers):
        self.answers = answers
        self.calls: list[tuple[str, str, dict]] = []

    async def request(self, method, path, **kwargs):
        self.calls.append((method, path, kwargs))
        answer = self.answers.get((method, path))
        if answer is None:
            raise AssertionError(f"unexpected {method} {path}")
        if isinstance(answer, list):
            answer = answer.pop(0)
        return answer(kwargs) if callable(answer) else answer

    async def close(self):
        pass


class FakeCatalog:
    def __init__(self, market, fee=None):
        self.market, self.fee = market, fee

    def fetch_market(self, native):
        return self.market

    def fetch_fee_schedule(self, market_id):
        return self.fee

    def close(self):
        pass


@pytest.fixture
def venue(opinion_market):
    def make(answers, *, safe=SAFE):
        catalog = FakeCatalog(
            normalize_market(opinion_market),
            fee_schedule_of(load("opinion_fee_rate_call.json")["result"], market_id="8453", token_id="1"),
        )
        adapter = OpinionTrading(OpinionCredentials(private_key=KEY, api_key="k", multisig_address=safe), catalog=catalog)
        adapter.http = FakeHttp({
            ("GET", "/quoteToken"): ok({"list": [{"quoteTokenAddress": USDT, "ctfExchangeAddress": EXCHANGE}]}),
            **answers,
        })
        return adapter
    return make


ORDER_ROW = {"orderId": "o-1", "marketId": 8453, "side": 1, "outcomeSide": 1, "price": "0.45",
             "orderShares": "100", "filledShares": "40", "status": 3, "tradingMethod": 2, "createdAt": 1766735464}


class TestAdapter:
    async def test_a_resting_limit_is_placed_once(self, venue):
        adapter = venue({("POST", "/order"): ok({"orderData": {"orderId": "o-1", "status": 1}})})
        order = await adapter.create_order(request(client_order_id="c-1"))
        assert order.id == "o-1" and order.status == OrderStatus.OPEN and order.client_order_id == "c-1"
        assert order.amount == D("100") and order.price == D("0.45")
        [post] = [c for c in adapter.http.calls if c[1] == "/order"]
        assert post[2]["json"]["maker"] == SAFE and "signature" not in order.info["request"]

    async def test_a_market_order_cancels_what_did_not_match(self, venue):
        adapter = venue({
            ("POST", "/order"): ok({"orderData": {"orderId": "o-1", "status": 1}}),
            ("POST", "/order/cancel"): ok({"result": True}),
            ("GET", "/order/o-1"): ok({"orderData": ORDER_ROW}),
        })
        order = await adapter.create_order(request(type=OrderType.MARKET))
        assert [c[1] for c in adapter.http.calls][-3:] == ["/order", "/order/cancel", "/order/o-1"]
        assert order.status == OrderStatus.CANCELED and order.filled == D("40")
        assert order.type == OrderType.MARKET

    async def test_the_safe_is_read_from_the_venue_when_not_configured(self, venue):
        adapter = venue({
            ("GET", "/user/balance"): ok({"multiSignAddress": SAFE, "balances": []}),
            ("POST", "/order"): ok({"orderData": {"orderId": "o-1"}}),
        }, safe=None)
        await adapter.create_order(request())
        assert adapter.wallet == SAFE

    async def test_an_account_without_a_trading_wallet_is_told_so(self, venue):
        adapter = venue({("GET", "/user/balance"): ok({"multiSignAddress": "", "balances": []})}, safe=None)
        with pytest.raises(CredentialsMissing, match="enable trading"):
            await adapter.create_order(request())

    async def test_a_refusal_in_the_envelope_is_typed(self, venue):
        adapter = venue({("POST", "/order"): {"errno": 10500, "errmsg": "insufficient balance", "result": None}})
        with pytest.raises(InsufficientFunds):
            await adapter.create_order(request())

    async def test_a_refused_cancel_on_a_live_order_raises(self, venue):
        adapter = venue({
            ("POST", "/order/cancel"): ok({"result": False}),
            ("GET", "/order/o-1"): ok({"orderData": {**ORDER_ROW, "status": 1}}),
        })
        with pytest.raises(OrderRejected, match="refused"):
            await adapter.cancel_order("o-1")

    async def test_open_orders_walk_every_page(self, venue):
        pages = [ok({"list": [ORDER_ROW] * 20, "total": 21}), ok({"list": [ORDER_ROW], "total": 21})]
        adapter = venue({("GET", "/order"): pages})
        orders = await adapter.fetch_open_orders(market_id="opinion:8453")
        assert len(orders) == 21
        params = [c[2]["params"] for c in adapter.http.calls if c[1] == "/order"]
        assert [p["page"] for p in params] == [1, 2] and params[0]["status"] == 1 and params[0]["marketId"] == 8453

    async def test_an_unknown_status_is_refused(self, venue):
        with pytest.raises(BadRequest):
            await venue({}).fetch_orders(status="sideways")

    async def test_trades_leave_out_splits(self, venue):
        rows = [{"tradeNo": "t-1", "orderNo": "o-1", "marketId": 8453, "side": "Buy", "outcomeSide": 1,
                 "price": "0.4", "shares": "10", "status": 2, "createdAt": 1},
                {"tradeNo": "t-2", "marketId": 8453, "side": "Split", "outcomeSide": 1, "price": "0.5",
                 "shares": "10", "status": 2, "createdAt": 2}]
        adapter = venue({("GET", "/trade"): ok({"list": rows, "total": 2})})
        fills = await adapter.fetch_my_trades()
        assert [f.id for f in fills] == ["t-1"] and fills.next_cursor is None

    async def test_balance_is_the_safes_usdt(self, venue):
        adapter = venue({("GET", "/user/balance"): ok({
            "multiSignAddress": SAFE,
            "balances": [{"quoteToken": USDT, "totalBalance": "120.5", "availableBalance": "100", "frozenBalance": "20.5"}],
        })})
        balance = await adapter.fetch_balance()
        assert (balance.total, balance.available, balance.locked) == (D("120.5"), D("100"), D("20.5"))
        assert balance.currency == "USDT"

    async def test_positions_are_grouped_by_market_and_filtered_by_topic(self, venue):
        rows = [{"marketId": 5342, "rootMarketId": 337, "outcomeSide": 1, "sharesOwned": "5"},
                {"marketId": 8453, "outcomeSide": 2, "sharesOwned": "7"}]
        adapter = venue({("GET", "/positions"): ok({"list": rows})})
        positions = await adapter.fetch_positions()
        assert {p.market_id for p in positions} == {"opinion:5342", "opinion:8453"}
        adapter.http.answers[("GET", "/positions")] = ok({"list": rows})
        assert [p.market_id for p in await adapter.fetch_positions(event_id="opinion:337")] == ["opinion:5342"]

    async def test_the_fee_is_charged_on_the_token_bought(self, venue):
        adapter = venue({})
        buy = await adapter.fetch_fee_estimate("opinion:8453", Side.BUY, D("0.8"), D("1000"))
        sell = await adapter.fetch_fee_estimate("opinion:8453", Side.SELL, D("0.8"), D("1000"))
        assert buy.taker_fee == D(str(round(0.04 * 0.8 * 1000 * 0.8 * 0.2, 6)))
        assert sell.taker_fee == D(str(round(0.04 * 0.2 * 1000 * 0.2 * 0.8, 6)))
        small = await adapter.fetch_fee_estimate("opinion:8453", Side.BUY, D("0.5"), D("1"))
        assert small.taker_fee == D("0.25") and small.min_fee == D("0.25")


class FakeAuth:
    def __init__(self, answers):
        self.answers = answers
        self.sent: list[tuple[str, dict]] = []

    class Answer:
        def __init__(self, body):
            self.body = body

        def json(self):
            return self.body

    def post(self, url, headers):
        self.sent.append(("create", headers))
        return self.Answer(self.answers.pop(0))

    def get(self, url, headers):
        self.sent.append(("get", headers))
        return self.Answer(self.answers.pop(0))


class TestApiKey:
    def test_the_key_is_created_by_signing(self):
        auth = FakeAuth([ok({"apiKey": "new-key", "walletAddress": "x"})])
        assert create_api_key(KEY, client=auth) == "new-key"
        [(action, headers)] = auth.sent
        typed = api_key_typed_data(headers["OPINION_ADDRESS"], "create", headers["OPINION_TIMESTAMP"])
        recovered = EthAccount.recover_message(encode_typed_data(full_message=typed), signature=headers["OPINION_SIGNATURE"])
        assert recovered == headers["OPINION_ADDRESS"] == WalletSigner(KEY).address

    def test_an_existing_key_is_read_back(self):
        auth = FakeAuth([{"errno": 11009, "errmsg": "exists"}, ok({"apiKey": "old-key"})])
        assert create_api_key(KEY, client=auth) == "old-key"
        assert [a for a, _ in auth.sent] == ["create", "get"]

    def test_an_unregistered_wallet_is_told_what_to_do(self):
        with pytest.raises(CredentialsMissing, match="finish onboarding"):
            create_api_key(KEY, client=FakeAuth([{"errno": 11005, "errmsg": "not registered"}]))



def test_doctor_names_the_wallet_and_prints_no_part_of_the_api_key(monkeypatch, tmp_path, capsys):
    from synpath.trading.__main__ import doctor

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OPINION_PRIVATE_KEY", KEY)
    monkeypatch.setenv("OPINION_API_KEY", "secret-opinion-key")
    doctor(None)
    line = next(l for l in capsys.readouterr().out.splitlines() if l.startswith("opinion"))
    assert WalletSigner(KEY).address in line
    assert "secret" not in line


async def test_the_engine_streams_opinion_with_the_api_key(venue):
    from synpath.engine.feeds import default_streams
    from synpath.ws.opinion import OpinionMarketStream, OpinionUserStream

    adapter = venue({})
    creds = OpinionCredentials(private_key=KEY, api_key="the-key")
    streams = default_streams({"opinion": adapter}, {"opinion": creds})["opinion"]
    assert isinstance(streams.market, OpinionMarketStream) and isinstance(streams.private, OpinionUserStream)
    assert streams.market.catalog is adapter.catalog and "apikey=the-key" in streams.private.url


async def test_the_client_routes_opinion_orders_to_this_adapter():
    import synpath

    client = synpath.Client(credentials={"opinion": OpinionCredentials(private_key=KEY, api_key="k")})
    adapter = client.trading("opinion")
    assert isinstance(adapter, OpinionTrading)
    await adapter.close()


def test_the_public_names_are_exported_at_the_top_level():
    """As every other venue's: `from synpath import OpinionTrading, OpinionMarketStream`."""
    import synpath

    assert synpath.OpinionTrading is OpinionTrading
    assert synpath.OpinionMarketStream.__name__ == "OpinionMarketStream"
    assert synpath.OpinionUserStream.__name__ == "OpinionUserStream"
    assert {"OpinionTrading", "OpinionMarketStream", "OpinionUserStream", "Opinion"} <= set(synpath.__all__)
