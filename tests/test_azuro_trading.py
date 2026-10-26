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
from eth_account.typed_transactions import TypedTransaction
from hexbytes import HexBytes

import synpath
from synpath.azuro import CHAINS
from synpath.trading import azuro_signing as sig
from synpath.trading.azuro import (
    AzuroTrading, ComboLeg, build_bet, build_combo, check_request, claim_calls, claimable_positions_of,
    combo_quote_of, fill_of_row, freebet_share, order_of_row, positions_of, redeemable_of_bet, relayer_fee_of,
    settlement_of_row,
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
OTHER_GAME = "600610060000000000028666020000000000001624778125"
OUTCOMES = {CORNERS: ["1", "2"], THREE_WAY: ["1", "2", "3"], OTHER_GAME: ["1", "2"]}


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
        {"side": Side.SELL, "outcome": "yes", "action": "sell"},
        {"side": Side.BUY, "outcome": "no", "action": "sell"},
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
    def __init__(self, conditions=None, outcomes=None):
        self.asked: list[list[str]] = []
        self.conditions = conditions or {}
        self.outcomes = {**OUTCOMES, **(outcomes or {})}

    def condition_outcomes(self, condition_ids):
        self.asked.append(list(condition_ids))
        return {c: self.outcomes[c] for c in condition_ids if c in self.outcomes}

    def fetch_conditions(self, condition_ids):
        return {c: self.conditions[c] for c in condition_ids if c in self.conditions}

    def close(self):
        pass


GAS = "GET /api/v1/public/bet/gas-info"
PLACE = "POST /api/v1/public/bet/orders/ordinar"
BETTOR = f"GET /api/v1/public/bet/orders/bettor/{OWNER.address}"


SUBGRAPH = "POST /subgraphs/name/azuro-protocol/azuro-api-polygon-v3"


def adapter(venue: Venue, *, catalog: Catalog | None = None, **creds) -> AzuroTrading:
    credentials = AzuroCredentials(private_key=KEY, rpc_url="https://rpc.test/", **creds)
    return AzuroTrading(credentials, catalog=catalog or Catalog(),
                        client=httpx.AsyncClient(transport=httpx.MockTransport(venue)))


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
        venue = Venue(**{BETTOR: {"orders": [row(), pending, settled]}, SUBGRAPH: {"data": {"v3Bets": []}}})

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


# ---------------------------------------------------------------------------
# Combos and claims
# ---------------------------------------------------------------------------

BETS = load("azuro_redeemable_bets.json")["data"]["v3Bets"]
"""Real bets from the Polygon bets subgraph: two won singles waiting to be
claimed (467993, 467650), a won combo claimed (472005), a cancelled single
claimed (472056) and a 33-leg combo lost (472057)."""
BET = {b["tokenId"]: b for b in BETS}
CALLS = VECTORS["calls"]


def condition(condition_id: str, outcomes=("1", "2"), *, odds=("1.58", "2.3"), state="Active", forbidden=False,
              hidden=False) -> dict:
    """A condition as `conditions-by-game-ids` lists it."""
    return {"conditionId": condition_id, "state": state, "isExpressForbidden": forbidden,
            "game": {"gameId": condition_id[4:26]},
            "outcomes": [{"outcomeId": o, "odds": p, "state": "Active", "hidden": hidden} for o, p in zip(outcomes, odds)]}


CONDITIONS = {CORNERS: condition(CORNERS), OTHER_GAME: condition(OTHER_GAME, odds=("1.51", "1.55")),
              THREE_WAY: condition(THREE_WAY, ("1", "2", "3"), odds=("6.83", "4.95", "1.32"))}


class TestComboSigning:
    @pytest.mark.parametrize("case", VECTORS["combos"], ids=lambda c: c["name"])
    def test_the_digest_and_signature_match_the_toolkit(self, case):
        combo = sig.combo_bet(legs=[(b["conditionId"], b["outcomeId"]) for b in case["bets"]],
                              min_odds=int(case["minOdds"]), amount=int(case["amount"]), nonce=int(case["nonce"]))
        assert "0x" + sig.combo_digest(case["clientData"], combo).hex() == case["digest"]
        assert sig.sign_combo(WalletSigner(KEY), case["clientData"], combo) == case["signature"]
        assert sig.combo_typed_data(case["clientData"], combo)["domain"] == case["domain"]

    @pytest.mark.parametrize("case", VECTORS["combos"], ids=lambda c: c["name"])
    def test_the_typed_data_is_what_a_wallet_signs(self, case):
        combo = sig.combo_bet(legs=[(b["conditionId"], b["outcomeId"]) for b in case["bets"]],
                              min_odds=int(case["minOdds"]), amount=int(case["amount"]), nonce=int(case["nonce"]))
        encoded = encode_typed_data(full_message=sig.combo_typed_data(case["clientData"], combo))
        assert "0x" + _hash_eip191_message(encoded).hex() == case["digest"]
        assert EthAccount.recover_message(encoded, signature=case["signature"]) == OWNER.address

    def test_the_wire_body_is_create_combo_bets(self):
        case = VECTORS["combos"][0]
        combo = sig.combo_bet(legs=[(b["conditionId"], b["outcomeId"]) for b in case["bets"]],
                              min_odds=int(case["minOdds"]), amount=int(case["amount"]), nonce=int(case["nonce"]))
        body = sig.wire_combo(POLYGON, bettor=OWNER.address, data=case["clientData"], combo=combo,
                              signature=case["signature"])
        assert body == {
            "environment": "PolygonUSDT", "bettor": OWNER.address.lower(), "betOwner": OWNER.address.lower(),
            "clientBetData": {"clientData": case["clientData"], "amount": case["amount"], "minOdds": case["minOdds"],
                              "nonce": case["nonce"],
                              "bets": [{"conditionId": b["conditionId"], "outcomeId": int(b["outcomeId"])}
                                       for b in case["bets"]]},
            "bettorSignature": case["signature"],
        }


class TestComboOdds:
    @pytest.mark.parametrize("bet", [b for b in BETS if b["type"] == "Express"], ids=lambda b: b["tokenId"])
    def test_the_combo_rule_gives_the_odds_the_protocol_recorded(self, bet):
        assert sig.combo_odds([s["odds"] for s in bet["selections"]]) == Decimal(bet["odds"])

    @pytest.mark.parametrize("case", [c for c in VECTORS["combo_odds"] if c["slippage"] == 0], ids=str)
    def test_cut_to_two_decimals_it_is_the_toolkits_calc_min_odds(self, case):
        odds = sig.combo_odds([str(o) for o in case["odds"]])
        assert odds.quantize(Decimal("0.01"), rounding="ROUND_DOWN") == Decimal(case["minOdds"])

    def test_odds_that_take_no_bets(self):
        for bad in ([], ["1"], ["1.5", "0.9"]):
            with pytest.raises(InvalidOrder):
                sig.combo_odds(bad)


class TestClaimCalldata:
    def test_withdraw_payouts_is_the_toolkits_and_the_chains(self):
        data = sig.withdraw_payouts_calldata(CALLS["core"], [472056, 472055, 472050, 472049, 472048])
        assert data == CALLS["withdrawPayouts"]
        # The same five bets were claimed on Polygon by this exact input,
        # transaction 0x034542780bd4527a8e118e6d469498201efb55f930c496a326540de0585fcd28.
        assert data.startswith("0xe8e3e87b")

    def test_the_other_calls(self):
        assert sig.withdraw_payout_calldata(CALLS["core"], 467993) == CALLS["withdrawPayout"]
        assert sig.view_payout_calldata(CALLS["core"], "467993") == CALLS["viewPayout"]
        assert sig.paymaster_withdraw_payouts_calldata([9001]) == CALLS["paymasterWithdrawPayouts"]

    def test_the_pools_are_the_toolkits(self):
        for chain in CHAINS.values():
            assert chain.lp == CALLS["chains"][str(chain.chain_id)]["lp"]


class TestCombos:
    LEGS = [ComboLeg(f"azuro:{CORNERS}"), ComboLeg(f"azuro:{OTHER_GAME}", Side.SELL)]
    SELECTIONS = [(CORNERS, "1"), (OTHER_GAME, "2")]

    def test_a_combo_is_signed_with_a_worst_price_for_the_whole(self):
        body, combo = build_combo(self.SELECTIONS, chain=POLYGON, signer=WalletSigner(KEY), quantity=Decimal("10"),
                                  price=Decimal("0.4"), relayer_fee=12345, nonce=9, now=1_800_000_000)
        assert combo == {"minOdds": str(25 * 10 ** 11), "amount": "4000000", "nonce": "9",
                         "bets": [{"conditionId": CORNERS, "outcomeId": "1"}, {"conditionId": OTHER_GAME, "outcomeId": "2"}]}
        data = body["clientBetData"]["clientData"]
        assert data["expiresAt"] == 1_800_000_300 and data["relayerFeeAmount"] == "12345"
        assert body["clientBetData"]["bets"][1] == {"conditionId": OTHER_GAME, "outcomeId": 2}
        encoded = encode_typed_data(full_message=sig.combo_typed_data(data, combo))
        assert EthAccount.recover_message(encoded, signature=body["bettorSignature"]) == OWNER.address

    def test_one_leg_is_no_combo(self):
        with pytest.raises(InvalidOrder, match="at least 2"):
            build_combo(self.SELECTIONS[:1], chain=POLYGON, signer=WalletSigner(KEY), quantity=Decimal("1"),
                        price=Decimal("0.5"), relayer_fee=1, nonce=1)

    def test_an_eligible_quote(self):
        quote = combo_quote_of(self.LEGS, self.SELECTIONS, CONDITIONS, {"minBet": 1, "maxBet": 200, "maxPayout": "900"},
                               currency="USDT", stake=Decimal("5"))
        # 1.58 / 0.99 -> 1.60, 1.55 / 0.99 -> 1.57; 1.60 * 1.57 * 0.99
        assert quote.eligible and quote.odds == Decimal("2.48688") and quote.price == 1 / Decimal("2.48688")
        assert (quote.min_bet, quote.max_bet, quote.max_payout) == (1, 200, 900)
        assert [leg["odds"] for leg in quote.legs] == [Decimal("1.58"), Decimal("1.55")]
        assert quote.legs[1]["market_id"] == f"azuro:{OTHER_GAME}" and quote.legs[1]["side"] == Side.SELL

    @pytest.mark.parametrize("change, problem", [
        (lambda c: {**c, CORNERS: condition(CORNERS, forbidden=True)}, "not offered in combos"),
        (lambda c: {**c, CORNERS: condition(CORNERS, state="Stopped")}, "is Stopped"),
        (lambda c: {**c, CORNERS: condition(CORNERS, hidden=True)}, "not taking bets"),
        (lambda c: {k: v for k, v in c.items() if k != CORNERS}, "no condition"),
    ])
    def test_what_stops_a_combo(self, change, problem):
        quote = combo_quote_of(self.LEGS, self.SELECTIONS, change(CONDITIONS), {"maxBet": 10}, currency="USDT")
        assert not quote.eligible and any(problem in p for p in quote.problems)

    def test_legs_on_one_game(self):
        legs = [ComboLeg(f"azuro:{CORNERS}"), ComboLeg(f"azuro:{THREE_WAY}-3")]
        quote = combo_quote_of(legs, [(CORNERS, "1"), (THREE_WAY, "3")], CONDITIONS, None, currency="USDT")
        assert quote.problems == ["game 1006000000000094301994 is in the combo twice; legs must be on different games"]
        twice = combo_quote_of(self.LEGS[:1] * 2, [(CORNERS, "1"), (CORNERS, "2")], CONDITIONS, None, currency="USDT")
        assert any("in the combo twice" in p for p in twice.problems)
        alone = combo_quote_of(self.LEGS[:1], self.SELECTIONS[:1], CONDITIONS, None, currency="USDT")
        assert alone.problems == ["a combo needs at least 2 legs"]

    def test_the_stake_against_the_limits(self):
        def problems(limits, stake):
            return combo_quote_of(self.LEGS, self.SELECTIONS, CONDITIONS, limits, currency="USDT",
                                  stake=Decimal(stake)).problems
        assert problems({"maxBet": 0}, "1") == ["the pool takes nothing on this combo now"]
        assert problems({"maxBet": 3}, "5") == ["a stake of 5 is above the pool's maximum of 3"]
        assert problems({"minBet": 1, "maxBet": 3}, "0.5") == ["a stake of 0.5 is below the pool's minimum of 1"]


class TestComboRecords:
    def combo(self, **changes):
        legs = [row()["conditions"][0], {**row()["conditions"][0], "conditionId": OTHER_GAME, "outcomeId": 2,
                                          "price": "1.55", "potentialAmount": None}]
        return row(betType="COMBO", odds=2.48688, conditions=legs, **changes)

    def test_a_combo_is_a_buy_of_the_whole_with_its_legs(self):
        order = order_of_row(self.combo(), outcomes=OUTCOMES[CORNERS], legs_outcomes=OUTCOMES)
        assert order.info["combo"] and order.side == Side.BUY and order.market_id == f"azuro:{CORNERS}"
        assert order.price == 1 / Decimal("2.48688") and order.quantity == Decimal("5") * Decimal("2.48688")
        assert [(leg["market_id"], leg["side"]) for leg in order.info["legs"]] == [
            (f"azuro:{CORNERS}", Side.BUY), (f"azuro:{OTHER_GAME}", Side.SELL)]

    def test_a_settled_combo_and_its_claim(self):
        won = settlement_of_row(self.combo(state="Settled", result="Won", payout=12.4344), outcomes=OUTCOMES[CORNERS],
                                legs_outcomes=OUTCOMES)
        assert won.info["combo"] and won.won and won.payout == Decimal("12.4344") and won.pnl == Decimal("7.4344")
        assert won.info["claimed"] is False and won.info["claimable"] == Decimal("12.4344")
        claimed = settlement_of_row(self.combo(state="Settled", result="Won", payout=12.4344,
                                               redeemedAt="2026-10-24T00:00:00.000Z",
                                               meta={"isRedeemed": True, "redeemedTxHash": "0x" + "cd" * 32}),
                                    outcomes=OUTCOMES[CORNERS])
        assert claimed.info["claimed"] is True and claimed.info["claimable"] == 0
        assert claimed.info["claim_transaction"] == "0x" + "cd" * 32
        lost = settlement_of_row(self.combo(state="Settled", result="Lost"), outcomes=OUTCOMES[CORNERS])
        assert lost.won is False and lost.info["claimed"] is None and lost.info["claimable"] == 0

    def test_a_cancelled_single_returns_its_stake_to_claim(self):
        void = settlement_of_row(row(state="Settled", result="Canceled"), outcomes=OUTCOMES[CORNERS])
        assert void.payout == Decimal("5") and void.info["claimable"] == Decimal("5")
        cashed = settlement_of_row(row(state="Settled", result="Won", payout=10.2, meta={"isCashedOut": True}),
                                   outcomes=OUTCOMES[CORNERS])
        assert cashed.info["claimable"] == 0


class TestRedeemable:
    def test_won_bets_waiting_are_claims(self):
        claim = redeemable_of_bet(BET["467993"])
        assert claim["bet_id"] == "467993" and claim["result"] == "won" and not claim["combo"]
        assert (claim["amount"], claim["raw_amount"], claim["stake"]) == (Decimal("5.84"), 5_840_000, Decimal("2"))
        assert (claim["core"], claim["lp"]) == (POLYGON.core.lower(), POLYGON.lp.lower())
        assert claim["legs"] == [("600610060000000000901243060000000000000131378013", "3")]

    def test_claimed_and_lost_bets_are_not(self):
        assert [redeemable_of_bet(BET[i]) for i in ("472005", "472056", "472057")] == [None, None, None]

    def test_a_cancelled_bet_returns_its_stake(self):
        waiting = {**BET["472056"], "isRedeemable": True, "isRedeemed": False, "rawPayout": None}
        claim = redeemable_of_bet(waiting)
        assert claim["result"] == "canceled" and claim["amount"] == Decimal("10")

    def test_a_won_combo(self):
        waiting = {**BET["472005"], "isRedeemable": True, "isRedeemed": False}
        claim = redeemable_of_bet(waiting)
        assert claim["combo"] and claim["amount"] == Decimal("17.48736") and len(claim["legs"]) == 2

    def test_a_free_bets_share(self):
        assert freebet_share(30, 10, None) == 20 and freebet_share(30, 10, False) == 30
        assert freebet_share(10, 10, True) == 0
        free = {**BET["467993"], "freebetId": "77", "paymasterContractAddress": CALLS["paymaster"],
                "isFreebetAmountReturnable": True}
        assert redeemable_of_bet(free)["amount"] == Decimal("3.84")

    def test_claims_are_batched_per_pool(self):
        claims = [redeemable_of_bet(BET["467993"]), redeemable_of_bet(BET["467650"])]
        [call] = claim_calls(claims, chain=POLYGON)
        assert call["to"] == POLYGON.lp and call["bet_ids"] == ["467993", "467650"]
        assert call["data"] == sig.withdraw_payouts_calldata(POLYGON.core, [467993, 467650])
        assert call["raw_amount"] == 5_840_000 + 1_530_000
        assert [c["bet_ids"] for c in claim_calls(claims, chain=POLYGON, batch=1)] == [["467993"], ["467650"]]

    def test_free_bets_and_legacy_pools_claim_one_each(self):
        free = redeemable_of_bet({**BET["467993"], "freebetId": "9001", "paymasterContractAddress": CALLS["paymaster"]})
        legacy = {**redeemable_of_bet(BET["467650"]), "lp": "0x" + "12" * 20}
        paid, old = claim_calls([free, legacy], chain=POLYGON)
        assert (paid["to"], paid["data"]) == (CALLS["paymaster"], CALLS["paymasterWithdrawPayouts"])
        assert old["to"] == "0x" + "12" * 20 and old["data"] == sig.withdraw_payout_calldata(POLYGON.core, 467650)
        with pytest.raises(InvalidOrder, match="paymaster"):
            claim_calls([{**free, "paymaster": None}], chain=POLYGON)

    def test_claimable_positions(self):
        claims = [redeemable_of_bet(BET["467993"])]
        three_way = "600610060000000000901243060000000000000131378013"
        [position] = claimable_positions_of(claims, outcomes={three_way: ["1", "2", "3"]}, decimals=6)
        assert position.market_id == f"azuro:{three_way}-3" and position.resolved and position.won
        assert position.redeemable == position.payout == Decimal("5.84") and position.contracts == Decimal("5.84")
        assert position.margin == Decimal("2") and position.info["bet_ids"] == ["467993"]


def rpc(result):
    return {"jsonrpc": "2.0", "id": 1, "result": result}


def sent_transactions(venue: Venue) -> list[dict]:
    raws = [json.loads(r.content)["params"][0] for r in venue.requests
            if r.url.host == "rpc.test" and json.loads(r.content)["method"] == "eth_sendRawTransaction"]
    return [TypedTransaction.from_bytes(HexBytes(raw)).as_dict() for raw in raws]


class TestComboAdapter:
    LEGS = [f"azuro:{CORNERS}", (f"azuro:{OTHER_GAME}", "sell")]
    CALC = "POST /api/v1/public/bet/calculation"
    COMBO = "POST /api/v1/public/bet/orders/combo"

    def venue(self, **more):
        return Venue(**{GAS: {"relayerFeeAmount": "20000"}, self.CALC: {"response": {"minBet": 1, "maxBet": 100,
                                                                                    "maxPayout": "1000"}},
                        self.COMBO: {"id": "0xf39f_2", "state": "Created"}, **more})

    def test_a_combo_is_quoted(self):
        venue = self.venue()

        async def go():
            async with adapter(venue, catalog=Catalog(CONDITIONS)) as azuro:
                return await azuro.fetch_combo_quote(self.LEGS, stake=Decimal("4"))

        quote = run(go())
        assert quote.eligible and quote.odds == Decimal("2.48688") and quote.currency == "USDT"
        asked = json.loads(venue.sent(self.CALC)[0].content)
        assert asked == {"bets": [{"conditionId": CORNERS, "outcomeId": 1}, {"conditionId": OTHER_GAME, "outcomeId": 2}],
                         "environment": "PolygonUSDT", "wallet": OWNER.address}

    def test_a_combo_is_signed_and_handed_to_the_relayer(self):
        venue = self.venue()

        async def go():
            async with adapter(venue, catalog=Catalog(CONDITIONS)) as azuro:
                return await azuro.create_combo_order(self.LEGS, quantity=Decimal("10"), price=Decimal("0.42"))

        order = run(go())
        body = json.loads(venue.sent(self.COMBO)[0].content)
        bet = body["clientBetData"]
        assert bet["amount"] == "4200000" and bet["minOdds"] == str(sig.min_odds(Decimal("0.42")))
        assert bet["bets"] == [{"conditionId": CORNERS, "outcomeId": 1}, {"conditionId": OTHER_GAME, "outcomeId": 2}]
        assert bet["clientData"]["relayerFeeAmount"] == "20000"
        combo = sig.combo_bet(legs=[(CORNERS, "1"), (OTHER_GAME, "2")], min_odds=int(bet["minOdds"]),
                              amount=int(bet["amount"]), nonce=int(bet["nonce"]))
        encoded = encode_typed_data(full_message=sig.combo_typed_data(bet["clientData"], combo))
        assert EthAccount.recover_message(encoded, signature=body["bettorSignature"]) == OWNER.address
        assert (order.id, order.status, order.side, order.cost) == ("0xf39f_2", OrderStatus.PENDING, Side.BUY, Decimal("4.2"))
        assert order.info["combo"] and [leg["side"] for leg in order.info["legs"]] == [Side.BUY, Side.SELL]
        assert order.quantity == Decimal("10")

    def test_a_combo_that_would_be_refused_is_never_signed(self):
        venue = self.venue()

        async def go():
            async with adapter(venue, catalog=Catalog(CONDITIONS)) as azuro:
                await azuro.create_combo_order([f"azuro:{CORNERS}", f"azuro:{THREE_WAY}-3"], quantity=Decimal("10"),
                                               price=Decimal("0.1"))

        with pytest.raises(InvalidOrder, match="different games"):
            run(go())
        assert not venue.sent(self.COMBO)

    def test_a_refused_combo_is_typed(self):
        venue = self.venue(**{self.COMBO: httpx.Response(400, json={"errorMessage": "Insufficient balance"})})

        async def go():
            async with adapter(venue, catalog=Catalog(CONDITIONS)) as azuro:
                await azuro.create_combo_order(self.LEGS, quantity=Decimal("10"), price=Decimal("0.42"), check=False)

        with pytest.raises(InsufficientFunds):
            run(go())
        assert not venue.sent(self.CALC), "check=False reads no quote"


class TestClaimAdapter:
    def venue(self, bets, **rpcs):
        answers = {
            SUBGRAPH: {"data": {"v3Bets": bets}},
            "RPC eth_estimateGas": rpc(hex(150_000)), "RPC eth_getTransactionCount": rpc("0x7"),
            "RPC eth_gasPrice": rpc(hex(30 * 10 ** 9)), "RPC eth_maxPriorityFeePerGas": rpc(hex(10 ** 9)),
            "RPC eth_sendRawTransaction": lambda req: rpc("0x" + "%064x" % len(req.content)),
            "RPC eth_getTransactionReceipt": rpc({"status": "0x1"}),
        }
        return Venue(**{**answers, **{f"RPC {k}": v for k, v in rpcs.items()}})

    def test_the_redeemable_bets_are_read_from_the_subgraph(self):
        venue = self.venue(BETS)

        async def go():
            async with adapter(venue) as azuro:
                return await azuro.fetch_redeemable_bets()

        bets = run(go())
        assert [b["bet_id"] for b in bets] == ["467993", "467650"]
        asked = json.loads(venue.sent(SUBGRAPH)[0].content)["variables"]
        assert asked["where"] == {"actor": OWNER.address.lower(), "isRedeemable": True, "isCashedOut": False}
        assert asked["skip"] == 0

    def test_claims_go_in_one_transaction_from_the_wallet(self):
        venue = self.venue(BETS)

        async def go():
            async with adapter(venue) as azuro:
                return await azuro.claim_winnings()

        result = run(go())
        assert result["bet_ids"] == ["467993", "467650"] and result["amount"] == Decimal("7.37")
        [entry] = result["transactions"]
        assert entry["to"] == POLYGON.lp and entry["receipt"] == {"status": "0x1"}
        [estimate] = [json.loads(r.content)["params"][0] for r in venue.requests
                      if r.url.host == "rpc.test" and json.loads(r.content)["method"] == "eth_estimateGas"]
        assert estimate == {"from": OWNER.address, "to": POLYGON.lp,
                            "data": sig.withdraw_payouts_calldata(POLYGON.core, [467993, 467650])}
        [tx] = sent_transactions(venue)
        assert tx["nonce"] == 7 and tx["to"] == HexBytes(POLYGON.lp) and tx["chainId"] == 137
        assert "0x" + bytes(tx["data"]).hex() == estimate["data"] and tx["gas"] == 180_000

    def test_one_bet_and_batches(self):
        venue = self.venue(BETS)

        async def go():
            async with adapter(venue) as azuro:
                return await azuro.redeem(["467650"], wait=False), await azuro.redeem(batch=1, wait=False)

        one, both = run(go())
        assert one["bet_ids"] == ["467650"] and "receipt" not in one["transactions"][0]
        assert [t["bet_ids"] for t in both["transactions"]] == [["467993"], ["467650"]]
        assert [t["nonce"] for t in sent_transactions(venue)] == [7, 7, 8]

    def test_a_bet_with_nothing_to_claim(self):
        async def go():
            async with adapter(self.venue(BETS)) as azuro:
                await azuro.redeem(["472005"])

        with pytest.raises(InvalidOrder, match="nothing to claim on bet 472005"):
            run(go())

    def test_nothing_to_claim_sends_nothing(self):
        venue = self.venue([])

        async def go():
            async with adapter(venue) as azuro:
                return await azuro.redeem()

        assert run(go()) == {"transactions": [], "bet_ids": [], "amount": 0, "currency": "USDT"}
        assert not [r for r in venue.requests if r.url.host == "rpc.test"]

    def test_a_claim_that_would_revert_is_refused_before_sending(self):
        paid = {"jsonrpc": "2.0", "id": 1, "error": {"code": 3, "message": "execution reverted", "data": "0xd70a0e30"}}
        venue = self.venue(BETS, eth_estimateGas=paid)

        async def go():
            async with adapter(venue) as azuro:
                await azuro.redeem()

        with pytest.raises(OrderRejected, match="AlreadyPaid") as caught:
            run(go())
        assert caught.value.reason == "alreadypaid" and not sent_transactions(venue)

    def test_a_reverted_claim(self):
        venue = self.venue(BETS, eth_getTransactionReceipt=rpc({"status": "0x0"}))

        async def go():
            async with adapter(venue) as azuro:
                await azuro.redeem()

        with pytest.raises(OrderRejected, match="reverted"):
            run(go())

    def test_what_a_claim_pays_now_is_read_on_chain(self):
        def call(req):
            target = json.loads(req.content)["params"][0]
            assert target == {"to": POLYGON.lp, "data": sig.view_payout_calldata(POLYGON.core, 467993)}
            return rpc(hex(5_840_000))

        async def go():
            async with adapter(self.venue([], eth_call=call)) as azuro:
                return await azuro.fetch_claimable(467993)

        assert run(go()) == Decimal("5.84")

    def test_positions_carry_what_waits_to_be_claimed(self):
        three_way = "600610060000000000901243060000000000000131378013"
        venue = self.venue(BETS[:1])
        venue.answers[BETTOR] = {"orders": [row()]}

        async def go():
            async with adapter(venue, catalog=Catalog(outcomes={three_way: ["1", "2", "3"]})) as azuro:
                return await azuro.fetch_positions()

        open_position, waiting = run(go())
        assert open_position.market_id == f"azuro:{CORNERS}" and not open_position.resolved
        assert waiting.market_id == f"azuro:{three_way}-3" and waiting.resolved and waiting.redeemable == Decimal("5.84")


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
