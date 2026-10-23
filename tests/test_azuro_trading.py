"""Azuro bets: signing checked against `@azuro-org/toolkit` and viem
(`tests/samples/azuro_signing_vectors.json`, made with the well-known Hardhat
test key), bet bodies, bet records, and the adapter against a scripted venue.
The venue's bet routes need a wallet and refuse this test machine's region, so
the bet order records follow the toolkit's `BetOrderData` type."""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import httpx
import pytest
from eth_account import Account as EthAccount
from eth_account.messages import _hash_eip191_message, encode_typed_data

import synpath
from synpath.azuro import CHAINS
from synpath.trading import azuro_signing as sig
from synpath.trading.azuro import (
    AzuroTrading, build_bet, check_request, fill_of_row, order_of_row, positions_of, relayer_fee_of, settlement_of_row,
)
from synpath.trading.credentials import AzuroCredentials, CredentialsMissing, load_azuro, load_credentials
from synpath.trading.errors import InsufficientFunds, InvalidOrder, OrderNotFound, OrderRejected
from synpath.trading.polymarket_signing import WalletSigner
from synpath.trading.types import OrderRequest, OrderStatus, OrderType, PositionSide, Side, TimeInForce

from conftest import load

VECTORS = load("azuro_signing_vectors.json")
KEY = VECTORS["private_key"]
OWNER = EthAccount.from_key(KEY)
POLYGON = CHAINS["polygon"]
CORNERS = "600610060000000000943019940000000000001802657415"
THREE_WAY = "600610060000000000943019940000000000001406934671"
OUTCOMES = {CORNERS: ["1", "2"], THREE_WAY: ["1", "2", "3"]}


def request(**changes) -> OrderRequest:
    base = {"market_id": f"azuro:{CORNERS}", "side": Side.BUY, "quantity": Decimal("10"), "price": Decimal("0.5"),
            "type": OrderType.MARKET}
    return OrderRequest(**{**base, **changes})


def built(req: OrderRequest, outcomes=None):
    return build_bet(req, chain=POLYGON, signer=WalletSigner(KEY), outcomes=outcomes or OUTCOMES[CORNERS],
                     relayer_fee=12345, nonce=7, now=1_800_000_000)


def row(**changes) -> dict:
    """A bet order as the toolkit's `BetOrderData` describes it."""
    base = {
        "id": f"{OWNER.address.lower()}_1792780000000", "state": "Accepted", "environment": "PolygonUSDT",
        "betType": "ORDINARY", "core": POLYGON.core, "bettor": OWNER.address, "owner": OWNER.address,
        "affiliate": OWNER.address, "amount": 5, "payout": None, "odds": 2.04, "bonusId": None, "isFreebet": False,
        "betId": 472100, "txHash": "0x" + "ab" * 32, "margin": "0.05", "settledMargin": None, "result": None,
        "createdAt": "2026-10-23T10:00:00.000Z", "updatedAt": "2026-10-23T10:00:05.000Z", "settledAt": None,
        "redeemedAt": None, "meta": None, "error": None, "errorMessage": None,
        "conditions": [{"conditionId": CORNERS, "outcomeId": 1, "orderId": "x", "gameId": "1006000000000094301994",
                        "gameState": "Prematch", "conditionMargin": "0.05", "selectionMargin": "0.05",
                        "settledSelectionMargin": None, "result": None, "price": "2.04", "potentialAmount": "10.2",
                        "potentialLoss": "5.2"}],
    }
    return {**base, **changes}


class TestSigning:
    @pytest.mark.parametrize("case", VECTORS["cases"], ids=lambda c: c["name"])
    def test_the_digest_and_signature_match_the_toolkit(self, case):
        data, bet = case["clientData"], case["bet"]
        assert "0x" + sig.bet_digest(data, bet).hex() == case["digest"]
        assert sig.sign_bet(WalletSigner(KEY), data, bet) == case["signature"]
        assert sig.bet_domain(data) == case["domain"]

    @pytest.mark.parametrize("case", VECTORS["cases"], ids=lambda c: c["name"])
    def test_the_typed_data_is_what_a_wallet_signs(self, case):
        typed = sig.bet_typed_data(case["clientData"], case["bet"])
        encoded = encode_typed_data(full_message=typed)
        assert "0x" + _hash_eip191_message(encoded).hex() == case["digest"]
        assert "0x" + OWNER.sign_message(encoded).signature.hex().removeprefix("0x") == case["signature"]

    def test_the_cashout_matches_the_toolkit(self):
        case = VECTORS["cashout"]
        message = sig.cashout_message(POLYGON, bet_id=case["tokenId"], cashout_odds=case["cashoutOdds"],
                                      expires_at=case["expiredAt"], attention=case["attention"])
        assert "0x" + sig.cashout_digest(POLYGON, message).hex() == case["digest"]
        assert sig.sign_cashout(WalletSigner(KEY), POLYGON, message) == case["signature"]
        assert sig.cashout_domain(POLYGON) == case["domain"]
        assert message["items"][0]["minOdds"] == int(case["message"]["items"][0]["minOdds"])

    def test_the_chains_match_the_toolkit(self):
        listed = {int(k): v for k, v in VECTORS["chains"].items()}
        for chain in CHAINS.values():
            entry = listed[chain.chain_id]
            assert (chain.environment, chain.core, chain.relayer, chain.lp, chain.cashout) == \
                (entry["environment"], entry["core"], entry["relayer"], entry["lp"], entry["cashout"])
            assert (chain.token.lower(), chain.decimals, chain.symbol) == \
                (entry["betToken"]["address"].lower(), entry["betToken"]["decimals"], entry["betToken"]["symbol"])
            assert (chain.api_url, chain.socket_url, chain.subgraph_url) == (entry["api"], entry["socket"], entry["bets"])


class TestAmounts:
    def test_minimum_odds_are_one_over_the_price_rounded_up(self):
        assert sig.min_odds(Decimal("0.5")) == 2 * 10 ** 12
        assert sig.min_odds(Decimal("0.74")) == 1_351_351_351_352
        assert Decimal(10 ** 12) / sig.min_odds(Decimal("0.74")) <= Decimal("0.74")
        for bad in ("0", "1", "1.2"):
            with pytest.raises(InvalidOrder):
                sig.min_odds(Decimal(bad))

    def test_the_stake_is_contracts_times_price_rounded_down(self):
        assert sig.stake(Decimal("10"), Decimal("0.5"), 6) == 5_000_000
        assert sig.stake(Decimal("3"), Decimal("0.333333"), 6) == 999_999
        assert sig.stake(Decimal("0.01"), Decimal("0.5"), 18) == 5 * 10 ** 15
        with pytest.raises(InvalidOrder):
            sig.stake(Decimal("0.000001"), Decimal("0.1"), 6)

    def test_odds_round_trip(self):
        assert sig.odds_raw("1.85") == 1_850_000_000_000 and sig.odds_of_raw(1_850_000_000_000) == Decimal("1.85")


class TestBets:
    def test_a_buy_backs_the_yes_outcome(self):
        body, bet = built(request())
        assert bet == {"conditionId": CORNERS, "outcomeId": "1", "minOdds": str(2 * 10 ** 12), "amount": "5000000",
                       "nonce": "7"}
        assert body["environment"] == "PolygonUSDT" and body["bettor"] == body["betOwner"] == OWNER.address.lower()
        data = body["clientBetData"]["clientData"]
        assert data["core"] == POLYGON.core and data["chainId"] == 137 and data["relayerFeeAmount"] == "12345"
        assert data["expiresAt"] == 1_800_000_000 + 300 and data["affiliate"] == OWNER.address
        assert data["attention"] == sig.BET_ATTENTION and not data["isBetSponsored"]
        assert body["clientBetData"]["bet"]["outcomeId"] == 1, "the API takes the outcome as a number"

    def test_the_signature_recovers_the_bettor(self):
        body, bet = built(request())
        data = body["clientBetData"]["clientData"]
        encoded = encode_typed_data(full_message=sig.bet_typed_data(data, bet))
        assert EthAccount.recover_message(encoded, signature=body["bettorSignature"]) == OWNER.address

    def test_a_sell_backs_the_other_outcome_at_one_minus_the_price(self):
        _, bet = built(request(side=Side.SELL, price=Decimal("0.6")))
        assert bet["outcomeId"] == "2" and bet["minOdds"] == str(2_500_000_000_000) and bet["amount"] == "4000000"

    def test_one_outcome_of_a_larger_condition(self):
        _, bet = built(request(market_id=f"azuro:{THREE_WAY}-3", price=Decimal("0.2")), OUTCOMES[THREE_WAY])
        assert bet["conditionId"] == THREE_WAY and bet["outcomeId"] == "3" and bet["minOdds"] == str(5 * 10 ** 12)
        with pytest.raises(InvalidOrder, match="only YES"):
            built(request(market_id=f"azuro:{THREE_WAY}-3", side=Side.SELL), OUTCOMES[THREE_WAY])
        with pytest.raises(InvalidOrder, match="name one"):
            built(request(market_id=f"azuro:{THREE_WAY}"), OUTCOMES[THREE_WAY])

    def test_an_affiliate_can_be_named(self):
        body, _ = build_bet(request(), chain=POLYGON, signer=WalletSigner(KEY), outcomes=["1", "2"], relayer_fee=1,
                            affiliate="0x" + "11" * 20, nonce=1, now=0)
        assert body["clientBetData"]["clientData"]["affiliate"] == "0x" + "11" * 20

    @pytest.mark.parametrize("changes", [
        {"type": OrderType.LIMIT},                                           # gtc: would rest
        {"type": OrderType.LIMIT, "time_in_force": TimeInForce.GTD, "expires_at": 1},
        {"post_only": True},
        {"reduce_only": True},
        {"type": OrderType.STOP_MARKET, "stop_price": Decimal("0.4")},
        {"price": None},
    ])
    def test_refused_before_signing(self, changes):
        with pytest.raises(InvalidOrder):
            check_request(request(**changes))

    def test_an_immediate_limit_is_a_bet_too(self):
        check_request(request(type=OrderType.LIMIT, time_in_force=TimeInForce.IOC))
        check_request(request(type=OrderType.LIMIT, time_in_force=TimeInForce.FOK))

    @pytest.mark.parametrize("info", [{"relayerFeeAmount": "123"}, [{"relayerFeeAmount": "123"}]])
    def test_the_relayer_fee_from_either_shape(self, info):
        assert relayer_fee_of(info) == 123


class TestRecords:
    def test_an_accepted_bet_is_a_filled_order_on_yes(self):
        order = order_of_row(row(), outcomes=["1", "2"])
        assert order.market_id == f"azuro:{CORNERS}" and order.side == Side.BUY
        assert order.status == OrderStatus.CLOSED and order.quantity == order.filled == Decimal("10.2")
        assert order.price == Decimal(1) / Decimal("2.04") and order.cost == Decimal("5")
        assert order.type == OrderType.MARKET and order.time_in_force == TimeInForce.FOK

    @pytest.mark.parametrize("state, status", [
        ("Created", OrderStatus.PENDING), ("Placed", OrderStatus.PENDING), ("Sent", OrderStatus.PENDING),
        ("Rejected", OrderStatus.REJECTED), ("Canceled", OrderStatus.CANCELED), ("Settled", OrderStatus.CLOSED),
        ("PendingCancel", OrderStatus.PENDING_CANCEL), ("CancelFailed", OrderStatus.CLOSED),
    ])
    def test_states(self, state, status):
        order = order_of_row(row(state=state), outcomes=["1", "2"])
        assert order.status == status
        assert order.filled == (order.quantity if state in ("Settled", "CancelFailed") else 0)

    def test_a_bet_on_the_other_outcome_is_a_sell(self):
        legs = [{**row()["conditions"][0], "outcomeId": 2}]
        order = order_of_row(row(conditions=legs, odds=1.65), outcomes=["1", "2"])
        assert order.side == Side.SELL and order.price == 1 - Decimal(1) / Decimal("1.65")

    def test_a_larger_conditions_outcome_is_its_own_market(self):
        legs = [{**row()["conditions"][0], "conditionId": THREE_WAY, "outcomeId": 3}]
        order = order_of_row(row(conditions=legs), outcomes=["1", "2", "3"])
        assert order.market_id == f"azuro:{THREE_WAY}-3" and order.side == Side.BUY

    def test_a_fill_and_a_settlement(self):
        fill = fill_of_row(row(), outcomes=["1", "2"])
        assert fill.quantity == Decimal("10.2") and fill.id == "0x" + "ab" * 32
        assert fill_of_row(row(state="Created"), outcomes=["1", "2"]) is None
        won = settlement_of_row(row(state="Settled", result="Won", payout=10.2), outcomes=["1", "2"])
        assert won.won and won.payout == Decimal("10.2") and won.pnl == Decimal("5.2")
        lost = settlement_of_row(row(state="Settled", result="Lost"), outcomes=["1", "2"])
        assert lost.won is False and lost.pnl == Decimal("-5")
        void = settlement_of_row(row(state="Settled", result="Canceled"), outcomes=["1", "2"])
        assert void.won is None and void.pnl == 0
        assert settlement_of_row(row(), outcomes=["1", "2"]) is None

    def test_positions_net_on_yes(self):
        other = {**row()["conditions"][0], "outcomeId": 2, "potentialAmount": "4"}
        rows = [row(), row(id="b", conditions=[other], amount=2.5), row(id="c", result="Won")]
        [position] = positions_of(rows, outcomes={CORNERS: ["1", "2"]})
        assert position.side == PositionSide.LONG and position.contracts == Decimal("6.2")
        assert (position.inventory_yes, position.inventory_no, position.margin) == (Decimal("10.2"), Decimal("4"), Decimal("7.5"))

    def test_a_combo_is_marked(self):
        order = order_of_row(row(betType="COMBO"), outcomes=["1", "2"])
        assert order.info["combo"] and order.quantity == Decimal("5") * Decimal("2.04")
        assert fill_of_row(row(betType="COMBO"), outcomes=["1", "2"]) is None


class Venue:
    def __init__(self, **answers):
        self.answers = answers
        self.requests: list[httpx.Request] = []

    def __call__(self, req: httpx.Request) -> httpx.Response:
        self.requests.append(req)
        key = f"{req.method} {req.url.path}"
        if req.url.host == "rpc.test":
            body = json.loads(req.content)
            key = f"RPC {body['method']}"
        answer = self.answers.get(key)
        if answer is None:
            return httpx.Response(404, json={"message": "not found"})
        if callable(answer):
            answer = answer(req)
        if isinstance(answer, httpx.Response):
            return answer
        return httpx.Response(200, json=answer)

    def sent(self, key: str) -> list[httpx.Request]:
        return [r for r in self.requests if f"{r.method} {r.url.path}" == key]


class Catalog:
    def __init__(self):
        self.asked: list[list[str]] = []

    def condition_outcomes(self, condition_ids):
        self.asked.append(list(condition_ids))
        return {c: OUTCOMES[c] for c in condition_ids if c in OUTCOMES}

    def close(self):
        pass


GAS = "GET /api/v1/public/bet/gas-info"
PLACE = "POST /api/v1/public/bet/orders/ordinar"
BETTOR = f"GET /api/v1/public/bet/orders/bettor/{OWNER.address}"


def adapter(venue: Venue, **creds) -> AzuroTrading:
    credentials = AzuroCredentials(private_key=KEY, rpc_url="https://rpc.test/", **creds)
    return AzuroTrading(credentials, catalog=Catalog(), client=httpx.AsyncClient(transport=httpx.MockTransport(venue)))


def run(coro):
    return asyncio.run(coro)


class TestAdapter:
    def test_a_bet_is_signed_and_handed_to_the_relayer(self):
        venue = Venue(**{GAS: {"relayerFeeAmount": "20000", "symbol": "USDT", "decimals": 6},
                         PLACE: {"id": "0xf39f_1", "state": "Created"}})

        async def go():
            async with adapter(venue) as azuro:
                return await azuro.create_order(request())

        order = run(go())
        [fee] = venue.sent(GAS)
        assert fee.url.params["environment"] == "PolygonUSDT"
        [sent] = venue.sent(PLACE)
        body = json.loads(sent.content)
        assert body["clientBetData"]["clientData"]["relayerFeeAmount"] == "20000"
        assert body["clientBetData"]["bet"]["amount"] == "5000000" and body["bettorSignature"].startswith("0x")
        assert (order.id, order.status, order.filled, order.quantity) == ("0xf39f_1", OrderStatus.PENDING, 0, Decimal("10"))
        assert order.price == Decimal("0.5") and order.market_id == f"azuro:{CORNERS}"

    def test_nonces_never_repeat(self):
        venue = Venue(**{GAS: {"relayerFeeAmount": "1"}, PLACE: {"id": "x", "state": "Created"}})

        async def go():
            async with adapter(venue) as azuro:
                await azuro.create_orders([request(), request(), request()])

        run(go())
        nonces = [json.loads(r.content)["clientBetData"]["bet"]["nonce"] for r in venue.sent(PLACE)]
        assert len(set(nonces)) == 3

    def test_a_refusal_is_typed(self):
        venue = Venue(**{GAS: {"relayerFeeAmount": "1"},
                         PLACE: httpx.Response(400, json={"errorMessage": "Insufficient allowance", "error": "X"})})

        async def go():
            async with adapter(venue) as azuro:
                return await azuro.create_orders([request(), request(side=Side.SELL, market_id=f"azuro:{THREE_WAY}-1")])

        allowance, refused = run(go())
        assert isinstance(allowance, InsufficientFunds) and isinstance(refused, InvalidOrder)
        assert len(venue.sent(PLACE)) == 1, "the refused request was never sent"

    def test_a_bet_not_created_is_rejected(self):
        venue = Venue(**{GAS: {"relayerFeeAmount": "1"},
                         PLACE: {"id": "x", "state": "Rejected", "errorMessage": "condition stopped"}})

        async def go():
            async with adapter(venue) as azuro:
                await azuro.create_order(request())

        with pytest.raises(OrderRejected, match="condition stopped"):
            run(go())

    def test_an_order_is_read_back_and_awaited(self):
        states = iter(["Created", "Sent", "Accepted"])
        venue = Venue(**{"GET /api/v1/public/bet/orders/abc": lambda _r: row(id="abc", state=next(states))})

        async def go():
            async with adapter(venue) as azuro:
                return await azuro.wait_for_order("abc", poll_s=0)

        order = run(go())
        assert order.status == OrderStatus.CLOSED and len(venue.requests) == 3

    def test_an_unknown_order(self):
        async def go():
            async with adapter(Venue()) as azuro:
                await azuro.fetch_order("nope")

        with pytest.raises(OrderNotFound):
            run(go())

    def test_nothing_can_be_cancelled(self):
        async def go():
            async with adapter(Venue()) as azuro:
                await azuro.cancel_order("abc")

        with pytest.raises(synpath.NotSupported):
            run(go())
        assert AzuroTrading.has["cancel_order"] is False

    def test_the_bettors_lists(self):
        pending = row(id="p", state="Sent")
        settled = row(id="s", state="Settled", result="Won", payout=10.2, settledAt="2026-10-23T12:00:00.000Z")
        venue = Venue(**{BETTOR: {"orders": [row(), pending, settled]}})

        async def go():
            async with adapter(venue) as azuro:
                return (await azuro.fetch_orders(), await azuro.fetch_open_orders(), await azuro.fetch_my_trades(),
                        await azuro.fetch_settlements(), await azuro.fetch_positions())

        orders, open_orders, fills, settlements, positions = run(go())
        assert len(orders) == 3 and [o.id for o in open_orders] == [row()["id"], "p", "s"]
        params = venue.sent(BETTOR)[1].url.params
        assert params.get_list("states") == ["Created", "Placed", "Sent", "PendingCancel"]
        assert params["environment"] == "PolygonUSDT" and params["offset"] == "0"
        assert len(fills) == 2 and settlements[0].won and positions[0].contracts == Decimal("10.2")

    def test_balance_and_allowance_are_read_on_chain(self):
        def call(req):
            body = json.loads(req.content)
            target = body["params"][0]
            assert target["to"] == POLYGON.token
            if target["data"].startswith("0x70a08231"):
                return {"jsonrpc": "2.0", "id": 1, "result": hex(12_500_000)}
            assert target["data"][34:74].lower() == OWNER.address[2:].lower()
            assert target["data"][-40:].lower() == POLYGON.relayer[2:].lower()
            return {"jsonrpc": "2.0", "id": 1, "result": hex(2 ** 256 - 1)}

        venue = Venue(**{"RPC eth_call": call})

        async def go():
            async with adapter(venue) as azuro:
                return await azuro.fetch_balance(), await azuro.fetch_allowance()

        balance, allowance = run(go())
        assert balance.total == Decimal("12.5") and balance.currency == "USDT" and balance.locked is None
        assert allowance > Decimal(10) ** 60

    def test_the_fee_is_the_relayers(self):
        venue = Venue(**{GAS: [{"relayerFeeAmount": "15000"}]})

        async def go():
            async with adapter(venue) as azuro:
                return await azuro.fetch_fee_estimate(f"azuro:{CORNERS}", Side.BUY, Decimal("0.5"), Decimal("10"))

        fee = run(go())
        assert fee.taker_fee == Decimal("0.015") and fee.currency == "USDT"

    def test_a_cash_out_is_quoted_signed_and_sent(self):
        quote = {"calculationId": "PolygonUSDT_0xf39f_472100", "owner": OWNER.address, "environment": "PolygonUSDT",
                 "betId": "472100", "cashoutAmount": "8.1", "cashoutOdds": "1.62", "expiredAt": 1_800_000_000,
                 "approveExpiredAt": 1_800_000_000, "isLive": False}
        venue = Venue(**{"POST /api/v1/public/cashout/get-calculation": quote,
                         "POST /api/v1/public/cashout/create": {"id": "c1", "state": "PROCESSING"}})

        async def go():
            async with adapter(venue) as azuro:
                return await azuro.cash_out(472100)

        assert run(go())["state"] == "PROCESSING"
        asked = json.loads(venue.sent("POST /api/v1/public/cashout/get-calculation")[0].content)
        assert asked == {"environment": "PolygonUSDT", "owner": OWNER.address, "betId": "472100"}
        body = json.loads(venue.sent("POST /api/v1/public/cashout/create")[0].content)
        signature = body["signature"]
        assert body["calculationId"] == quote["calculationId"]
        assert signature["verifyingContract"] == POLYGON.cashout and signature["bettingContract"] == POLYGON.core
        message = sig.cashout_message(POLYGON, bet_id=472100, cashout_odds="1.62", expires_at=1_800_000_000)
        encoded = encode_typed_data(full_message=sig.cashout_typed_data(POLYGON, message))
        assert EthAccount.recover_message(encoded, signature=signature["ownerSignature"]) == OWNER.address

    def test_no_cash_out_offered(self):
        async def go():
            async with adapter(Venue()) as azuro:
                assert await azuro.fetch_cashout_quote(1) is None
                await azuro.cash_out(1)

        with pytest.raises(OrderRejected, match="no cash-out"):
            run(go())

    def test_a_discontinued_chain_takes_no_bets(self):
        with pytest.raises(synpath.BadRequest, match="discontinued"):
            AzuroTrading(AzuroCredentials(private_key=KEY, chain="gnosis"), catalog=Catalog())

    def test_base_bets_in_weth(self):
        venue = Venue(**{GAS: {"relayerFeeAmount": "1"}, PLACE: {"id": "x", "state": "Created"}})

        async def go():
            async with adapter(venue, chain="base") as azuro:
                return await azuro.create_order(request(quantity=Decimal("0.01")))

        run(go())
        body = json.loads(venue.sent(PLACE)[0].content)
        assert body["environment"] == "BaseWETH" and body["clientBetData"]["bet"]["amount"] == str(5 * 10 ** 15)
        assert body["clientBetData"]["clientData"]["core"] == CHAINS["base"].core


class TestCredentials:
    def test_a_key_alone_bets_on_polygon(self):
        creds = load_azuro({"AZURO_PRIVATE_KEY": KEY[2:]})
        assert creds.private_key == KEY and creds.chain == "polygon" and creds.affiliate is None
        assert creds.address == OWNER.address and KEY not in repr(creds) and creds.secrets == [KEY]

    def test_nothing_configured(self):
        assert load_azuro({}) is None

    @pytest.mark.parametrize("env, match", [
        ({"AZURO_PRIVATE_KEY": "0x1234"}, "64 hex"),
        ({"AZURO_PRIVATE_KEY": KEY, "AZURO_CHAIN": "gnosis"}, "discontinued"),
        ({"AZURO_PRIVATE_KEY": KEY, "AZURO_AFFILIATE": KEY}, "private key"),
    ])
    def test_bad_values(self, env, match):
        with pytest.raises(CredentialsMissing, match=match) as raised:
            load_azuro(env)
        assert KEY[2:] not in str(raised.value)

    def test_the_chain_affiliate_and_rpc(self):
        creds = load_azuro({"AZURO_PRIVATE_KEY": KEY, "AZURO_CHAIN": "base-sepolia", "AZURO_AFFILIATE": "0x" + "11" * 20,
                            "AZURO_RPC_URL": "https://rpc.example"})
        assert (creds.chain, creds.affiliate, creds.rpc_url) == ("base_sepolia", "0x" + "11" * 20, "https://rpc.example")

    def test_loaded_with_every_venue_and_routed_by_the_client(self):
        loaded = load_credentials({"AZURO_PRIVATE_KEY": KEY}, redact_logs=False)
        assert isinstance(loaded["azuro"], AzuroCredentials)
        trading = synpath.Client(loaded).trading("azuro")
        assert isinstance(trading, AzuroTrading) and trading.chain.name == "polygon"
        run(trading.close())

    def test_doctor_names_the_wallet_and_chain(self, tmp_path, monkeypatch, capsys):
        from synpath.trading.__main__ import doctor

        monkeypatch.chdir(tmp_path)
        for name in [n for n in __import__("os").environ if n.startswith(("AZURO_", "SX_BET_"))]:
            monkeypatch.delenv(name, raising=False)
        (tmp_path / ".env").write_text(f"AZURO_PRIVATE_KEY={KEY}\nAZURO_CHAIN=base\n")
        assert doctor(str(tmp_path / ".env")) == 0
        out = capsys.readouterr().out
        line = next(l for l in out.splitlines() if l.startswith("azuro"))
        assert "configured (base)" in line and OWNER.address in line and KEY[2:] not in out
