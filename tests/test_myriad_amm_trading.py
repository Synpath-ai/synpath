"""Myriad AMM orders: the call encoding against the venue's own quote
calldata, the pool maths against its quotes, the slippage bound, and the
adapter against a scripted chain (nothing is sent anywhere)."""
from __future__ import annotations

import asyncio
import copy
import json
import math
from decimal import Decimal

import httpx
import pytest
from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from eth_account import Account as EthAccount
from eth_account.typed_transactions import TypedTransaction
from hexbytes import HexBytes

from synpath.myriad import parse_id
from synpath.trading import myriad_signing as sig
from synpath.trading.credentials import MyriadCredentials
from synpath.trading.errors import InsufficientFunds, InvalidOrder, OrderRejected
from synpath.trading.myriad import (
    MyriadTrading, amm_buy_cost, amm_buy_shares, amm_leg, amm_sell_shares, amm_sell_value, amm_time_in_force,
    fill_of_action, plan_amm_trade, positions_of,
)
from synpath.trading.types import OrderRequest, OrderStatus, OrderType, Side, TimeInForce

from conftest import load

KEY = "0x" + "11" * 32
OWNER = EthAccount.from_key(KEY)
RAW = load("myriad_market_amm.json")
"""Market 120473 on BNB Chain: two outcomes, no fees, USD1."""
POOLS = [o["shares"] for o in RAW["outcomes"]]
MARKET_ID = "myriad:56-amm-120473"
CONTRACT = sig.AMM_CONTRACTS[56]

ABSTRACT_POOLS = [1381.597397, 2368.81549]
"""Market 768 on Abstract (USDC.e, 6 decimals; 3% on buys), read with the two quotes below."""


def request(**changes) -> OrderRequest:
    base = {"market_id": MARKET_ID, "side": Side.BUY, "quantity": Decimal("10"), "price": Decimal("0.7"),
            "type": OrderType.MARKET}
    return OrderRequest(**{**base, **changes})


def args_of(calldata: str) -> list[int]:
    return list(abi_decode(["uint256"] * 4, bytes.fromhex(calldata[10:])))


# ---------------------------------------------------------------------------
# Encoding and maths, against the venue's quotes
# ---------------------------------------------------------------------------

class TestCalldata:
    @pytest.mark.parametrize("name", ["myriad_quote_buy.json", "myriad_quote_abstract_buy.json"])
    def test_a_buy_is_the_call_the_venue_quotes(self, name):
        quote = load(name)
        market, outcome, min_shares, value = args_of(quote["calldata"])
        assert sig.amm_buy_calldata(market, outcome, min_shares, value) == quote["calldata"]
        assert quote["calldata"].startswith("0x1281311d"), "buy(uint256,uint256,uint256,uint256)"

    @pytest.mark.parametrize("name", ["myriad_quote_sell.json", "myriad_quote_abstract_sell.json"])
    def test_a_sell_is_the_call_the_venue_quotes(self, name):
        quote = load(name)
        market, outcome, value, max_shares = args_of(quote["calldata"])
        assert sig.amm_sell_calldata(market, outcome, value, max_shares) == quote["calldata"]
        assert quote["calldata"].startswith("0x3620875e"), "sell(uint256,uint256,uint256,uint256)"

    def test_the_quote_bounds_the_shares_not_the_value(self):
        """The quote's slippage is already in its calldata: minimum shares on a
        buy, maximum shares on a sell, the value exact. The token's decimals
        count the shares too (6 on Abstract's USDC.e)."""
        buy, sell = load("myriad_quote_abstract_buy.json"), load("myriad_quote_abstract_sell.json")
        _, outcome, min_shares, value = args_of(buy["calldata"])
        assert (outcome, value) == (1, 10 * 10**6) and min_shares == round(buy["shares_threshold"] * 10**6)
        _, _, value, max_shares = args_of(sell["calldata"])
        assert value == round(sell["value"] * 10**6) and max_shares == round(5 * 1.02 * 10**6)

    def test_claims_and_reads(self):
        assert sig.amm_claim_calldata(120473) == "0x677bd9ff" + abi_encode(["uint256"], [120473]).hex()
        assert sig.amm_claim_voided_calldata(7, 1).startswith("0xec93f0f5")
        assert sig.amm_pools_calldata(1).startswith("0xbb986173")
        assert sig.amm_holdings_calldata(1, OWNER.address).startswith("0xb31eb895")
        array = "0x" + abi_encode(["uint256[]"], [[5, 7]]).hex()
        assert sig.decode_uint_array(array) == [5, 7]
        assert sig.decode_holdings("0x" + abi_encode(["uint256", "uint256[]"], [9, [3, 0]]).hex()) == [3, 0]


class TestPoolMaths:
    def test_buys_match_the_venues_quotes(self):
        assert amm_buy_shares(POOLS, 0, 25, 0.0) == pytest.approx(load("myriad_quote_buy.json")["shares"], rel=1e-9)
        # 3% on buys (fee, treasury, distributor), out of the value spent.
        assert amm_buy_shares(ABSTRACT_POOLS, 1, 10, 0.03) == pytest.approx(
            load("myriad_quote_abstract_buy.json")["shares"], rel=1e-12)

    def test_sells_match_the_venues_quotes(self):
        assert amm_sell_value(POOLS, 0, 20, 0.0) == pytest.approx(load("myriad_quote_sell.json")["value"], rel=1e-9)
        assert amm_sell_value(ABSTRACT_POOLS, 1, 5, 0.0) == pytest.approx(
            load("myriad_quote_abstract_sell.json")["value"], rel=1e-9)

    def test_inverses(self):
        for fee in (0.0, 0.02):
            assert amm_buy_shares(POOLS, 1, amm_buy_cost(POOLS, 1, 40, fee), fee) == pytest.approx(40, rel=1e-12)
            assert amm_sell_shares(POOLS, 0, amm_sell_value(POOLS, 0, 40, fee), fee) == pytest.approx(40, rel=1e-12)

    def test_a_sell_fee_is_on_top_of_the_payout(self):
        """With a fee the pool pays out `value / (1 - fee)`, so the same shares fetch less."""
        assert amm_sell_value(POOLS, 0, 20, 0.01) < amm_sell_value(POOLS, 0, 20, 0.0)
        assert amm_sell_shares(POOLS, 0, 9.9, 0.01) == pytest.approx(amm_sell_shares(POOLS, 0, 10, 0.0), rel=1e-12)


class TestSlippageBound:
    def test_a_buy_spends_for_at_least_value_over_the_worst_price(self):
        plan = plan_amm_trade(POOLS, index=0, action="buy", shares=Decimal("10"), worst=Decimal("0.7"),
                              buy_fee=0.0, sell_fee=0.0, decimals=18, partial=True)
        assert plan["bound_units"] == math.ceil(Decimal(plan["value_units"]) / Decimal("0.7"))
        assert plan["shares"] == pytest.approx(Decimal("10"), rel=Decimal("1e-9"))
        assert plan["average"] < Decimal("0.7") and plan["bound_units"] <= plan["shares"] * 10**18

    def test_a_sell_is_paid_for_at_most_value_over_the_worst_price(self):
        plan = plan_amm_trade(POOLS, index=0, action="sell", shares=Decimal("10"), worst=Decimal("0.6"),
                              buy_fee=0.0, sell_fee=0.0, decimals=18, partial=True)
        assert plan["bound_units"] == int(Decimal(plan["value_units"]) / Decimal("0.6"))
        assert plan["bound_units"] >= plan["shares"] * 10**18 and plan["average"] > Decimal("0.6")
        held = plan_amm_trade(POOLS, index=0, action="sell", shares=Decimal("10"), worst=Decimal("0.6"),
                              buy_fee=0.0, sell_fee=0.0, decimals=18, partial=True, held=Decimal("10.5"))
        assert held["bound_units"] == 105 * 10**17, "never more than the wallet holds"

    def test_known_answer(self):
        """Fixed pool, fixed order: the call may never drift."""
        plan = plan_amm_trade(ABSTRACT_POOLS, index=1, action="buy", shares=Decimal("20"), worst=Decimal("0.45"),
                              buy_fee=0.03, sell_fee=0.0, decimals=6, partial=False)
        assert (plan["value_units"], plan["bound_units"]) == (7621195, 16935989)
        assert sig.amm_buy_calldata(768, 1, plan["bound_units"], plan["value_units"]) == (
            "0x1281311d" + abi_encode(["uint256"] * 4, [768, 1, 16935989, 7621195]).hex())
        assert plan["fee"] == Decimal("0.22863585")

    def test_ioc_trades_what_fits_within_the_worst_price(self):
        whole = plan_amm_trade(POOLS, index=0, action="buy", shares=Decimal("500"), worst=Decimal("0.7"),
                               buy_fee=0.0, sell_fee=0.0, decimals=18, partial=False)
        assert "reason" in whole and "worse than 0.7" in whole["reason"]
        part = plan_amm_trade(POOLS, index=0, action="buy", shares=Decimal("500"), worst=Decimal("0.7"),
                              buy_fee=0.0, sell_fee=0.0, decimals=18, partial=True)
        assert Decimal("0") < part["shares"] < Decimal("500")
        assert part["average"] == pytest.approx(Decimal("0.7"), rel=Decimal("1e-5")) and part["average"] <= Decimal("0.7")
        assert part["bound_units"] <= part["shares"] * 10**18, "the bound holds at the pool as read"

    def test_nothing_fits(self):
        plan = plan_amm_trade(POOLS, index=0, action="buy", shares=Decimal("5"), worst=Decimal("0.5"),
                              buy_fee=0.0, sell_fee=0.0, decimals=18, partial=True)
        assert plan == {"reason": "the pool fills nothing at 0.5 or better"}


class TestOrderShape:
    @pytest.mark.parametrize("changes, partial", [
        ({}, True),
        ({"type": OrderType.LIMIT, "time_in_force": TimeInForce.IOC}, True),
        ({"type": OrderType.LIMIT, "time_in_force": TimeInForce.FOK}, False),
        ({"time_in_force": TimeInForce.FOK}, False),
    ])
    def test_market_ioc_and_fok(self, changes, partial):
        assert amm_time_in_force(request(**changes)) is partial

    @pytest.mark.parametrize("changes, match", [
        ({"type": OrderType.LIMIT}, "taker-only"),
        ({"type": OrderType.LIMIT, "time_in_force": TimeInForce.GTD, "expires_at": 1}, "taker-only"),
        ({"type": OrderType.LIMIT, "post_only": True}, "post-only"),
        ({"price": None}, "worst"),
        ({"price": Decimal("1")}, "outside"),
    ])
    def test_resting_orders_are_refused(self, changes, match):
        with pytest.raises(InvalidOrder, match=match):
            amm_time_in_force(request(**changes))

    def test_two_outcomes_are_yes_and_no(self):
        ref = parse_id("56-amm-120473")
        assert amm_leg(request(), ref, 2) == (0, "buy", Decimal("0.7"))
        assert amm_leg(request(side=Side.SELL), ref, 2) == (1, "buy", Decimal("0.3"))
        assert amm_leg(request(side=Side.SELL, outcome="yes", action="sell"), ref, 2) == (0, "sell", Decimal("0.7"))
        assert amm_leg(request(side=Side.BUY, outcome="no", action="sell"), ref, 2) == (1, "sell", Decimal("0.3"))
        with pytest.raises(InvalidOrder, match="3 outcomes"):
            amm_leg(request(), ref, 3)

    def test_one_outcome_of_many(self):
        ref = parse_id("2741-amm-412-2")
        assert amm_leg(request(), ref, 4) == (2, "buy", Decimal("0.7"))
        assert amm_leg(request(side=Side.SELL, outcome="yes", action="sell"), ref, 4) == (2, "sell", Decimal("0.7"))
        with pytest.raises(InvalidOrder, match="rest of the pool"):
            amm_leg(request(side=Side.SELL), ref, 4)
        with pytest.raises(InvalidOrder, match="names outcome 2"):
            amm_leg(request(), ref, 2)


# ---------------------------------------------------------------------------
# The adapter against a scripted chain
# ---------------------------------------------------------------------------

def word(n: int) -> str:
    return "0x" + n.to_bytes(32, "big").hex()


class Chain:
    """Answers the node's calls for one pool and the wallet, decodes and
    keeps every signed transaction, and mines it with a scripted receipt."""

    def __init__(self, *, pools=POOLS, decimals=18, balance=10**24, allowance=2**256 - 1, held=(0, 0),
                 estimate_error=None, receipt=None):
        self.scale = 10**decimals
        self.pools = [int(Decimal(str(p)) * self.scale) for p in pools]
        self.balance, self.allowance, self.held = balance, allowance, list(held)
        self.estimate_error = estimate_error
        self.receipt = receipt
        self.sent: list[dict] = []
        self.calls: list[dict] = []
        self.mined = True

    def __call__(self, req: httpx.Request) -> httpx.Response:
        body = json.loads(req.content)
        method, params = body["method"], body["params"]
        result = self.answer(method, params)
        if isinstance(result, dict) and "error" in result:
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, **result})
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": result})

    def answer(self, method, params):
        if method == "eth_call":
            call = params[0]
            self.calls.append(call)
            selector = call["data"][:10]
            if selector == "0xbb986173":
                return "0x" + abi_encode(["uint256[]"], [self.pools]).hex()
            if selector == "0xb31eb895":
                return "0x" + abi_encode(["uint256", "uint256[]"], [0, self.held]).hex()
            if selector == "0x70a08231":
                return word(self.balance)
            if selector == "0xdd62ed3e":
                return word(self.allowance)
            raise AssertionError(f"unexpected call {selector}")
        if method == "eth_getTransactionCount":
            return hex(5)
        if method in ("eth_gasPrice", "eth_maxPriorityFeePerGas"):
            return hex(10**8)
        if method == "eth_estimateGas":
            if self.estimate_error:
                return {"error": {"code": 3, "message": self.estimate_error}}
            return hex(200_000)
        if method == "eth_sendRawTransaction":
            raw = HexBytes(params[0])
            tx = TypedTransaction.from_bytes(raw).as_dict()
            tx["from"] = EthAccount.recover_transaction(raw)
            self.sent.append(tx)
            return "0x" + "ab" * 32
        if method == "eth_getTransactionReceipt":
            if not self.mined:
                return None
            return self.receipt(self.sent[-1]) if self.receipt else {"status": "0x1", "logs": []}
        raise AssertionError(f"unexpected rpc {method}")


def action_log(*, action: int, market: int, outcome: int, shares: int, value: int, user=OWNER.address,
               contract=CONTRACT, timestamp=1792900000) -> dict:
    return {
        "address": contract.lower(),
        "topics": [sig.MARKET_ACTION_TX, "0x" + "00" * 12 + user[2:].lower(), word(action), word(market)],
        "data": "0x" + abi_encode(["uint256"] * 4, [outcome, shares, value, timestamp]).hex(),
    }


def filled_as_planned(shares_factor=Decimal("1")):
    """A receipt that fills what the call asked: the value exact, the shares
    the pool gives (a buy) or the shares a sell burns."""
    def receipt(tx):
        data = "0x" + bytes(tx["data"]).hex()
        market, outcome, a, b = args_of(data)
        buying = data.startswith("0x1281311d")
        value = b if buying else a
        pools = [p / 10**18 for p in Chain().pools]
        shares = (amm_buy_shares(pools, outcome, value / 10**18, 0.0) if buying
                  else amm_sell_shares(pools, outcome, value / 10**18, 0.0))
        units = int(Decimal(str(shares)) * shares_factor * 10**18)
        return {"status": "0x1", "blockNumber": "0x10", "gasUsed": "0x30d40", "logs": [
            action_log(action=0 if buying else 1, market=market, outcome=outcome, shares=units, value=value)]}
    return receipt


class Catalog:
    def __init__(self, raw=RAW):
        self.raw = raw

    def _resolve(self, market_id):
        return parse_id(market_id), None

    def raw_market(self, market_id):
        return self.raw

    def fetch_fee_schedule(self, market_id):  # pragma: no cover - unused here
        raise AssertionError

    def close(self):
        pass


def adapter(chain: Chain, *, raw=RAW, venue=None) -> MyriadTrading:
    creds = MyriadCredentials(private_key=KEY, api_key="key-1", api_secret="secret-1")
    return MyriadTrading(
        creds, catalog=Catalog(raw),
        client=httpx.AsyncClient(transport=httpx.MockTransport(venue or (lambda r: httpx.Response(404)))),
        rpc_client=httpx.AsyncClient(transport=httpx.MockTransport(chain)),
    )


def run(coro):
    return asyncio.run(coro)


async def trade(chain: Chain, req: OrderRequest, *, raw=RAW):
    async with adapter(chain, raw=raw) as myriad:
        return await myriad.create_order(req)


class TestAdapter:
    def test_a_buy_is_one_signed_call_and_its_fill_comes_from_the_receipt(self):
        chain = Chain(receipt=filled_as_planned())
        order = run(trade(chain, request(client_order_id="c-1")))
        [tx] = chain.sent
        assert tx["from"] == OWNER.address and tx["to"] == HexBytes(CONTRACT) and tx["chainId"] == 56
        assert tx["nonce"] == 5 and tx["gas"] == 240_000 and tx["value"] == 0
        market, outcome, min_shares, value = args_of("0x" + bytes(tx["data"]).hex())
        assert (market, outcome) == (120473, 0) and min_shares == math.ceil(Decimal(value) / Decimal("0.7"))
        assert {c["data"][:10] for c in chain.calls} == {"0xbb986173", "0x70a08231", "0xdd62ed3e"}
        assert order.id == "0x" + "ab" * 32 and order.status == OrderStatus.CLOSED
        assert order.market_id == MARKET_ID and order.client_order_id == "c-1"
        assert order.filled == pytest.approx(Decimal("10"), rel=Decimal("1e-9"))
        assert order.average_price == pytest.approx(Decimal(value) / 10**18 / order.filled)
        assert order.average_price < Decimal("0.7") and order.fee == 0 and order.fee_currency == "USD1"
        [fill] = order.info["fills"]
        assert fill.id == f"{order.id}:0:buy" and fill.order_id == order.id and fill.side == Side.BUY

    def test_buying_no_buys_outcome_one(self):
        chain = Chain(receipt=filled_as_planned())
        order = run(trade(chain, request(side=Side.SELL, price=Decimal("0.6"))))
        market, outcome, min_shares, value = args_of("0x" + bytes(chain.sent[0]["data"]).hex())
        assert outcome == 1 and min_shares == math.ceil(Decimal(value) / Decimal("0.4"))
        assert order.side == Side.SELL and order.average_price > Decimal("0.6"), "the YES price of a NO buy"
        assert order.info["fills"][0].side == Side.SELL

    def test_a_sell_checks_the_shares_held_and_bounds_what_it_burns(self):
        chain = Chain(held=[15 * 10**18, 0], receipt=filled_as_planned())
        order = run(trade(chain, request(side=Side.SELL, outcome="yes", action="sell", price=Decimal("0.6"))))
        market, outcome, value, max_shares = args_of("0x" + bytes(chain.sent[0]["data"]).hex())
        assert chain.sent[0]["data"][:4] == bytes.fromhex("3620875e") and outcome == 0
        assert max_shares == min(int(Decimal(value) / Decimal("0.6")), 15 * 10**18)
        assert order.status == OrderStatus.CLOSED and order.average_price > Decimal("0.6")
        assert order.info["fills"][0].id.endswith(":0:sell")
        with pytest.raises(InsufficientFunds, match="holds 5"):
            run(trade(Chain(held=[5 * 10**18, 0]), request(side=Side.SELL, outcome="yes", action="sell", price=Decimal("0.6"))))

    def test_fees_are_reported(self):
        raw = copy.deepcopy(RAW)
        raw["fees"]["buy"] = {"fee": 0.01, "treasury_fee": 0, "distributor_fee": 0.01}
        raw["fees"]["sell"] = {"fee": 0.01, "treasury_fee": 0, "distributor_fee": 0}
        chain = Chain(receipt=filled_as_planned())
        order = run(trade(chain, request(), raw=raw))
        value = Decimal(args_of("0x" + bytes(chain.sent[0]["data"]).hex())[3]) / 10**18
        assert order.fee == value * Decimal("0.02") and order.info["plan"]["fee"] == str(value * Decimal("0.02"))
        chain = Chain(held=[15 * 10**18, 0], receipt=filled_as_planned())
        order = run(trade(chain, request(side=Side.SELL, outcome="yes", action="sell", price=Decimal("0.6")), raw=raw))
        paid = Decimal(args_of("0x" + bytes(chain.sent[0]["data"]).hex())[2]) / 10**18
        assert order.fee == pytest.approx(paid / Decimal("0.99") - paid)

    def test_without_approval_nothing_is_sent(self):
        chain = Chain(allowance=0)
        with pytest.raises(InsufficientFunds, match="approve_amm"):
            run(trade(chain, request()))
        with pytest.raises(InsufficientFunds, match="holds"):
            run(trade(Chain(balance=10**18), request()))
        assert chain.sent == []

    def test_a_simulated_revert_is_refused_before_sending(self):
        chain = Chain(estimate_error="execution reverted: minimum shares not met")
        with pytest.raises(OrderRejected, match="would revert") as caught:
            run(trade(chain, request()))
        assert caught.value.reason == "simulation" and chain.sent == []

    def test_a_reverted_trade_is_rejected(self):
        chain = Chain(receipt=lambda tx: {"status": "0x0", "logs": []})
        order = run(trade(chain, request()))
        assert order.status == OrderStatus.REJECTED and order.filled == 0 and "reverted" in order.info["reason"]

    def test_nothing_within_the_worst_price_sends_nothing(self):
        chain = Chain()
        order = run(trade(chain, request(price=Decimal("0.5"))))
        assert order.status == OrderStatus.CANCELED and order.filled == 0 and chain.sent == []
        assert "nothing" in order.info["reason"]
        assert run(trade(Chain(), request(quantity=Decimal("500"), time_in_force=TimeInForce.FOK))).status == \
            OrderStatus.CANCELED

    def test_ioc_fills_part(self):
        chain = Chain(receipt=filled_as_planned())
        order = run(trade(chain, request(quantity=Decimal("500"))))
        assert order.status == OrderStatus.CANCELED and Decimal("0") < order.filled < Decimal("500")
        assert order.average_price <= Decimal("0.7")

    def test_a_pending_trade_is_read_again(self):
        chain = Chain(receipt=filled_as_planned())
        chain.mined = False

        async def go():
            async with adapter(chain) as myriad:
                order = await myriad.create_order(request(params={"wait": False}))
                opened = await myriad.fetch_open_orders(market_id=MARKET_ID)
                with pytest.raises(InvalidOrder, match="nothing to cancel"):
                    await myriad.cancel_order(order.id)
                chain.mined = True
                return order, opened, await myriad.fetch_order(order.id), await myriad.fetch_orders(market_id=MARKET_ID)

        pending, opened, done, page = run(go())
        assert pending.status == OrderStatus.PENDING and [o.id for o in opened] == [pending.id]
        assert done.status == OrderStatus.CLOSED and done.filled > 0 and [o.status for o in page] == [OrderStatus.CLOSED]

    def test_a_closed_market_is_refused(self):
        raw = {**RAW, "state": "closed"}
        with pytest.raises(InvalidOrder, match="closed"):
            run(trade(Chain(), request(), raw=raw))

    def test_a_resting_order_is_refused_before_any_read(self):
        chain = Chain()
        with pytest.raises(InvalidOrder, match="taker-only"):
            run(trade(chain, request(type=OrderType.LIMIT)))
        assert chain.calls == []

    def test_approve_and_redeem(self):
        resolved = {**RAW, "state": "resolved", "resolvedOutcomeId": 1, "voided": False}
        chain = Chain(held=[0, 4 * 10**18], receipt=lambda tx: {"status": "0x1", "logs": [
            action_log(action=4, market=120473, outcome=1, shares=4 * 10**18, value=4 * 10**18)]})

        async def go():
            async with adapter(chain, raw=resolved) as myriad:
                approved = await myriad.approve_amm(MARKET_ID)
                redeemed = await myriad.redeem_amm(MARKET_ID)
                return approved, redeemed

        approved, redeemed = run(go())
        approve, claim = chain.sent
        assert approve["to"] == HexBytes(RAW["token"]["address"])
        assert "0x" + bytes(approve["data"]).hex() == sig.approve_calldata(CONTRACT)
        assert claim["to"] == HexBytes(CONTRACT) and "0x" + bytes(claim["data"]).hex() == sig.amm_claim_calldata(120473)
        assert redeemed["paid"] == Decimal("4") and approved["receipt"]["status"] == "0x1"
        voided = {**resolved, "voided": True}
        chain = Chain(held=[2, 3])

        async def void():
            async with adapter(chain, raw=voided) as myriad:
                return await myriad.redeem_amm(MARKET_ID, wait=False)

        assert len(run(void())["transaction_hashes"]) == 2
        assert ["0x" + bytes(tx["data"]).hex() for tx in chain.sent] == [
            sig.amm_claim_voided_calldata(120473, 0), sig.amm_claim_voided_calldata(120473, 1)]


class TestConsistency:
    """The fill an AMM order reports, the account's history of the same trade
    and the position it makes all name the same market, and the fill and the
    history row the same id."""

    def test_two_outcomes(self):
        order = run(trade(Chain(receipt=filled_as_planned()), request()))
        [fill] = order.info["fills"]
        row = {"action": "buy", "marketId": 120473, "networkId": 56, "outcomeId": 0, "shares": str(fill.quantity),
               "value": fill.info["value"], "timestamp": 1792900000, "txId": order.id}
        history = fill_of_action(row, amm=True)
        [position] = positions_of([{"marketId": 120473, "networkId": 56, "executionMode": 0, "outcomeId": 0,
                                    "shares": str(fill.quantity), "price": 0.65, "status": "ongoing"}])
        assert history.id == fill.id and history.market_id == fill.market_id == position.market_id == MARKET_ID
        assert history.price == fill.price and history.side == fill.side

    def test_one_outcome_of_many(self):
        raw = copy.deepcopy(RAW)
        raw["id"], raw["networkId"] = 412, 2741
        raw["outcomes"] = [{**raw["outcomes"][0], "id": k} for k in range(3)]
        raw["token"] = {**raw["token"], "decimals": 6}
        chain = Chain(pools=[1000, 1000, 1000], decimals=6, receipt=lambda tx: {"status": "0x1", "logs": [
            action_log(action=0, market=412, outcome=1, shares=12 * 10**6, value=4 * 10**6,
                       contract=sig.AMM_CONTRACTS[2741])]})
        order = run(trade(chain, request(market_id="myriad:2741-amm-412-1", quantity=Decimal("12"),
                                         price=Decimal("0.5")), raw=raw))
        assert chain.sent[0]["chainId"] == 2741 and chain.sent[0]["to"] == HexBytes(sig.AMM_CONTRACTS[2741])
        [fill] = order.info["fills"]
        assert fill.market_id == order.market_id == "myriad:2741-amm-412-1"
        assert (fill.side, fill.quantity, fill.price) == (Side.BUY, Decimal("12"), Decimal("4") / Decimal("12"))
        row = {"action": "buy", "marketId": 412, "networkId": 2741, "outcomeId": 1, "shares": "12", "value": "4",
               "timestamp": 1792900000, "txId": order.id}
        assert fill_of_action(row, amm=True, many=True).market_id == fill.market_id
        rows = [{"marketId": 412, "networkId": 2741, "executionMode": 0, "outcomeId": 1, "shares": 12, "status": "ongoing"}]
        assert positions_of(rows, many={(2741, 412)})[0].market_id == fill.market_id
        assert positions_of(rows)[0].market_id == "myriad:2741-amm-412", "without knowing, outcome 1 reads binary"

    def test_history_and_positions_ask_for_both_kinds(self):
        address = OWNER.address
        many_raw = {**RAW, "id": 412, "networkId": 2741, "outcomes": [RAW["outcomes"][0]] * 3}

        def events(req):
            model = req.url.params["trading_model"]
            row = ({"action": "buy", "marketId": 3722, "networkId": 56, "outcomeId": 0, "shares": 2, "value": 1,
                    "timestamp": 1792772000, "txId": "0xob"} if model == "ob" else
                   {"action": "sell", "marketId": 412, "networkId": 2741, "outcomeId": 0, "shares": 2, "value": 1,
                    "timestamp": 1792773000, "txId": "0xamm"})
            return httpx.Response(200, json={"data": [row], "pagination": {"hasNext": model == "amm"}})

        portfolio = {"data": [{"marketId": 412, "networkId": 2741, "executionMode": 0, "outcomeId": 0, "shares": 2,
                               "status": "ongoing"}], "pagination": {"hasNext": False}}

        def venue(req):
            if req.url.path == f"/users/{address}/events":
                return events(req)
            if req.url.path == f"/users/{address}/portfolio":
                return httpx.Response(200, json=portfolio)
            return httpx.Response(404)

        async def go():
            async with adapter(Chain(), raw=many_raw, venue=venue) as myriad:
                return await myriad.fetch_my_trades(), await myriad.fetch_positions()

        trades, [position] = run(go())
        assert [(f.id, f.market_id) for f in trades] == [("0xamm:0:sell", "myriad:2741-amm-412-0"),
                                                         ("0xob:0:buy", "myriad:56-3722")]
        assert trades.next_cursor == "2" and position.market_id == "myriad:2741-amm-412-0"
