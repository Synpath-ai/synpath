"""Hyperliquid order entry: signing against the venue SDK's own vectors, the
order wire, and the adapter against recorded and scripted responses."""
from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from synpath.trading.credentials import HyperliquidCredentials, load_credentials
from synpath.trading.errors import InsufficientFunds, InvalidOrder, OrderNotFound, PermissionDenied
from synpath.trading.hyperliquid import (
    HyperliquidTrading, asset_of, build_order_wire, cloid_of, error_of, position_of,
)
from synpath.trading.hyperliquid_signing import action_hash, packb, sign_l1_action, to_wire
from synpath.trading.polymarket_signing import WalletSigner
from synpath.trading.types import OrderRequest, OrderStatus, OrderType, PositionSide, Side, TimeInForce

from conftest import load

SDK_KEY = "0x0123456789012345678901234567890123456789012345678901234567890123"
"""The private key the venue's Python SDK signs its test vectors with."""


def sdk_order(**extra):
    wire = {"a": 1, "b": True, "p": "100", "s": "100", "r": False, "t": {"limit": {"tif": "Gtc"}}, **extra}
    return {"type": "order", "orders": [wire], "grouping": "na"}


class TestSigning:
    """Every vector here is copied from the SDK's `tests/signing_test.py`."""

    signer = WalletSigner(SDK_KEY)

    def test_the_action_hash(self):
        action = {"type": "order", "orders": [{"a": 4, "b": True, "p": "1670.1", "s": "0.0147", "r": False,
                                               "t": {"limit": {"tif": "Ioc"}}}], "grouping": "na"}
        assert action_hash(action, None, 1677777606040).hex() == \
            "0fcbeda5ae3c4950a548021552a4fea2226858c4453571bf3f24ba017eac2908"

    @pytest.mark.parametrize("mainnet, r, s, v", [
        (True, "0x53749d5b30552aeb2fca34b530185976545bb22d0b3ce6f62e31be961a59298",
         "0x755c40ba9bf05223521753995abb2f73ab3229be8ec921f350cb447e384d8ed8", 27),
        (False, "0x542af61ef1f429707e3c76c5293c80d01f74ef853e34b76efffcb57e574f9510",
         "0x17b8b32f086e8cdede991f1e2c529f5dd5297cbe8128500e00cbaf766204a613", 28),
    ])
    def test_a_plain_action(self, mainnet, r, s, v):
        action = {"type": "dummy", "num": 100000000000}
        assert sign_l1_action(self.signer, action, nonce=0, mainnet=mainnet) == {"r": r, "s": s, "v": v}

    @pytest.mark.parametrize("mainnet, r, s, v", [
        (True, "0xd65369825a9df5d80099e513cce430311d7d26ddf477f5b3a33d2806b100d78e",
         "0x2b54116ff64054968aa237c20ca9ff68000f977c93289157748a3162b6ea940e", 28),
        (False, "0x82b2ba28e76b3d761093aaded1b1cdad4960b3af30212b343fb2e6cdfa4e3d54",
         "0x6b53878fc99d26047f4d7e8c90eb98955a109f44209163f52d8dc4278cbbd9f5", 27),
    ])
    def test_an_order(self, mainnet, r, s, v):
        assert sign_l1_action(self.signer, sdk_order(), nonce=0, mainnet=mainnet) == {"r": r, "s": s, "v": v}

    def test_an_order_with_a_cloid(self):
        signed = sign_l1_action(self.signer, sdk_order(c="0x00000000000000000000000000000001"), nonce=0, mainnet=True)
        assert signed == {"r": "0x41ae18e8239a56cacbc5dad94d45d0b747e5da11ad564077fcac71277a946e3",
                          "s": "0x3c61f667e747404fe7eea8f90ab0e76cc12ce60270438b2058324681a00116da", "v": 27}

    def test_a_vault(self):
        signed = sign_l1_action(self.signer, {"type": "dummy", "num": 100000000000}, nonce=0, mainnet=True,
                                vault_address="0x1719884eb866cb12b2287399b15f7db5e7d775ea")
        assert signed == {"r": "0x3c548db75e479f8012acf3000ca3a6b05606bc2ec0c29c50c515066a326239",
                          "s": "0x4d402be7396ce74fbba3795769cda45aec00dc3125a984f2a9f23177b190da2c", "v": 28}

    @pytest.mark.parametrize("value, packed", [
        (0, "00"), (127, "7f"), (128, "cc80"), (65535, "cdffff"), (100_075_440, "ce05f707b0"),
        (2**40, "cf0000010000000000"), (-1, "ff"), (-200, "d1ff38"),
        (True, "c3"), (None, "c0"), ("a" * 31, "bf" + "61" * 31), ("a" * 32, "d920" + "61" * 32),
        ([1, 2], "920102"), ({"a": 1}, "81a16101"),
    ])
    def test_messagepack(self, value, packed):
        assert packb(value).hex() == packed

    def test_wire_numbers(self):
        assert to_wire(Decimal("0.50000")) == "0.5" and to_wire(Decimal("100")) == "100"
        assert to_wire(Decimal("0.00001")) == "0.00001"
        with pytest.raises(ValueError):
            to_wire(Decimal("0.123456789"))


def request(**kwargs) -> OrderRequest:
    base = {"market_id": "hyperliquid:7544", "side": Side.BUY, "amount": Decimal("100"), "price": Decimal("0.5")}
    return OrderRequest(**{**base, **kwargs})


class TestOrderWire:
    def test_buy_takes_the_yes_coin(self):
        wire, native, outcome, price = build_order_wire(request())
        assert (native, outcome, price) == ("7544", "yes", Decimal("0.5"))
        assert wire == {"a": 100_075_440, "b": True, "p": "0.5", "s": "100", "r": False, "t": {"limit": {"tif": "Gtc"}}}

    def test_sell_buys_the_no_coin_at_the_complement(self):
        wire, _, outcome, price = build_order_wire(request(side=Side.SELL, price=Decimal("0.7")))
        assert outcome == "no" and price == Decimal("0.3")
        assert wire["a"] == asset_of("7544", "no") == 100_075_441 and wire["b"] is True and wire["p"] == "0.3"

    def test_reduce_only_sells_the_token_held(self):
        wire, _, outcome, _ = build_order_wire(request(side=Side.SELL, reduce_only=True))
        assert outcome == "yes" and wire["b"] is False

    def test_order_kinds(self):
        assert build_order_wire(request(type=OrderType.MARKET))[0]["t"] == {"limit": {"tif": "Ioc"}}
        assert build_order_wire(request(time_in_force=TimeInForce.IOC))[0]["t"] == {"limit": {"tif": "Ioc"}}
        assert build_order_wire(request(post_only=True))[0]["t"] == {"limit": {"tif": "Alo"}}

    def test_a_client_order_id_becomes_a_stable_cloid(self):
        wire = build_order_wire(request(client_order_id="bucket-17"))[0]
        assert wire["c"] == cloid_of("bucket-17") and len(wire["c"]) == 34
        assert cloid_of("0x00000000000000000000000000000001") == "0x00000000000000000000000000000001"

    @pytest.mark.parametrize("change, reason", [
        ({"price": Decimal("0.123456")}, "significant figures"),
        ({"amount": Decimal("100.5")}, "whole contracts"),
        ({"amount": Decimal("10"), "price": Decimal("0.5")}, "minimum order"),
        ({"price": Decimal("1")}, "between 0 and 1"),
        ({"time_in_force": TimeInForce.FOK}, "not available"),
        ({"type": OrderType.STOP_MARKET, "stop_price": Decimal("0.4")}, "execution engine"),
        ({"market_id": "hyperliquid:q198"}, "question"),
        ({"post_only": True, "type": OrderType.MARKET}, "post-only"),
    ])
    def test_refusals_before_signing(self, change, reason):
        with pytest.raises(InvalidOrder, match=reason):
            build_order_wire(request(**change))

    def test_the_minimum_counts_the_token_bought(self):
        """Selling at 0.95 buys NO at 0.05: 100 contracts is 5 USDC, under 10."""
        with pytest.raises(InvalidOrder, match="minimum"):
            build_order_wire(request(side=Side.SELL, price=Decimal("0.95")))


class TestErrors:
    def test_mapping(self):
        assert isinstance(error_of("User or API Wallet 0xabc does not exist."), PermissionDenied)
        assert isinstance(error_of("Insufficient spot balance asset=100075440"), InsufficientFunds)
        assert isinstance(error_of("Order was never placed, already canceled, or filled."), OrderNotFound)
        assert isinstance(error_of("Order must have minimum value of $10."), InvalidOrder)


class TestPositions:
    def test_balances_net_on_the_yes_leg(self):
        rows = [row for row in load("hyperliquid_spot_state.json")["balances"] if row["coin"].startswith("+")]
        held = [row for row in rows if Decimal(row["total"]) > 0]
        row = held[0]
        native = str(int(row["coin"][1:]) // 10)
        position = position_of([row], market_id=f"hyperliquid:{native}")
        side_no = row["coin"].endswith("1")
        assert position.side == (PositionSide.SHORT if side_no else PositionSide.LONG)
        assert position.contracts == Decimal(row["total"])
        entry = Decimal(row["entryNtl"]) / Decimal(row["total"])
        assert position.entry_price == (1 - entry if side_no else entry).quantize(Decimal("1e-8"))

    def test_both_tokens_held(self):
        rows = [{"coin": "+75440", "total": "30", "entryNtl": "15"}, {"coin": "+75441", "total": "10", "entryNtl": "4"}]
        position = position_of(rows, market_id="hyperliquid:7544")
        assert (position.side, position.contracts) == (PositionSide.LONG, Decimal("20"))
        assert (position.inventory_yes, position.inventory_no) == (Decimal("30"), Decimal("10"))


class FakeHttp:
    def __init__(self, answers):
        self.answers = answers
        self.calls: list[tuple[str, dict]] = []

    async def post(self, path, json=None, **kwargs):
        self.calls.append((path, json))
        key = json["action"]["type"] if path == "/exchange" else json["type"]
        answer = self.answers[key]
        return answer(json) if callable(answer) else answer

    async def close(self):
        pass


class Catalog:
    def close(self):
        pass


def trading(answers, *, testnet=True) -> HyperliquidTrading:
    venue = HyperliquidTrading(HyperliquidCredentials(private_key=SDK_KEY, testnet=testnet), catalog=Catalog())
    venue.http = FakeHttp(answers)
    return venue


def ok(kind, statuses):
    return {"status": "ok", "response": {"type": kind, "data": {"statuses": statuses}}}


class TestAdapter:
    def test_an_order_is_signed_for_the_network_and_rests(self):
        venue = trading({"order": ok("order", [{"resting": {"oid": 77738308}}])})
        order = asyncio.run(venue.create_order(request(client_order_id="c1")))
        path, body = venue.http.calls[0]
        assert path == "/exchange" and body["vaultAddress"] is None
        assert body["signature"] == sign_l1_action(venue.signer, body["action"], nonce=body["nonce"], mainnet=False)
        assert order.id == "77738308" and order.status == OrderStatus.OPEN and order.remaining == Decimal("100")
        assert order.client_order_id == "c1" and order.info["cloid"] == cloid_of("c1")

    def test_a_filled_order(self):
        venue = trading({"order": ok("order", [{"filled": {"totalSz": "100", "avgPx": "0.3", "oid": 5}}])})
        order = asyncio.run(venue.create_order(request(side=Side.SELL, price=Decimal("0.7"))))
        assert order.status == OrderStatus.CLOSED and order.filled == Decimal("100")
        assert order.average_price == Decimal("0.7")  # bought NO at 0.3

    def test_an_ioc_rest_is_cancelled(self):
        venue = trading({"order": ok("order", [{"filled": {"totalSz": "40", "avgPx": "0.5", "oid": 6}}])})
        order = asyncio.run(venue.create_order(request(type=OrderType.MARKET)))
        assert order.status == OrderStatus.CANCELED and order.filled == Decimal("40")

    def test_a_batch_reports_each_order(self):
        venue = trading({"order": ok("order", [{"resting": {"oid": 1}}, {"error": "Order must have minimum value of $10."}])})
        results = asyncio.run(venue.create_orders([request(), request(price=Decimal("0.6")), request(amount=Decimal("1.5"))]))
        assert results[0].id == "1"
        assert isinstance(results[1], InvalidOrder) and isinstance(results[2], InvalidOrder)
        assert len(venue.http.calls[0][1]["action"]["orders"]) == 2

    def test_a_refused_action(self):
        venue = trading({"order": {"status": "err", "response": "User or API Wallet 0xabc does not exist."}})
        with pytest.raises(PermissionDenied):
            asyncio.run(venue.create_order(request()))

    def test_nonces_only_go_up(self):
        venue = trading({"order": ok("order", [{"resting": {"oid": 1}}])})
        for _ in range(3):
            asyncio.run(venue.create_order(request()))
        nonces = [body["nonce"] for _, body in venue.http.calls]
        assert nonces == sorted(set(nonces))

    def test_a_cancel_names_the_coin_read_from_the_order(self):
        record = load("hyperliquid_order_status.json")
        venue = trading({"orderStatus": record, "cancel": ok("cancel", ["success"])})
        order = asyncio.run(venue.cancel_order(str(record["order"]["order"]["oid"])))
        cancel = [body for path, body in venue.http.calls if path == "/exchange"][0]["action"]
        assert cancel == {"type": "cancel", "cancels": [{"a": 100_075_440, "o": record["order"]["order"]["oid"]}]}
        assert order.info["cancel"] == "success"

    def test_an_unknown_order(self):
        venue = trading({"orderStatus": load("hyperliquid_order_unknown.json")})
        with pytest.raises(OrderNotFound):
            asyncio.run(venue.fetch_order("1"))

    def test_balance_is_the_spot_usdc(self):
        state = load("hyperliquid_spot_state.json")
        venue = trading({"spotClearinghouseState": state})
        balance = asyncio.run(venue.fetch_balance())
        usdc = next(row for row in state["balances"] if row["coin"] == "USDC")
        assert balance.currency == "USDC" and balance.total == Decimal(usdc["total"])
        assert balance.locked == Decimal(usdc["hold"]) and balance.available == balance.total - balance.locked

    def test_positions_skip_empty_balances(self):
        state = load("hyperliquid_spot_state.json")
        venue = trading({"spotClearinghouseState": state})
        positions = asyncio.run(venue.fetch_positions())
        held = {str(int(r["coin"][1:]) // 10) for r in state["balances"]
                if r["coin"].startswith("+") and Decimal(r["total"]) > 0}
        assert {p.market_id.split(":")[1] for p in positions} == held

    def test_fills_and_orders_come_from_the_records(self):
        venue = trading({"userFills": load("hyperliquid_fills.json"),
                         "historicalOrders": load("hyperliquid_orders.json")})
        fills = asyncio.run(venue.fetch_my_trades())
        assert len(fills) == 4 and fills[0].timestamp >= fills[-1].timestamp
        orders = asyncio.run(venue.fetch_orders())
        assert {o.id for o in orders} == {str(r["order"]["oid"]) for r in load("hyperliquid_orders.json")}


class TestCredentials:
    def test_an_api_wallet_trades_for_its_account(self):
        loaded = load_credentials({"HYPERLIQUID_PRIVATE_KEY": SDK_KEY[2:],
                                   "HYPERLIQUID_ACCOUNT_ADDRESS": "0x1719884eB866cb12b2287399B15F7db5e7D775EA",
                                   "HYPERLIQUID_TESTNET": "1"}, dotenv="/nonexistent", redact_logs=False)["hyperliquid"]
        assert loaded.private_key == SDK_KEY and loaded.testnet
        assert loaded.address == "0x1719884eb866cb12b2287399b15f7db5e7d775ea"
        assert SDK_KEY not in repr(loaded)

    def test_the_keys_own_address_otherwise(self):
        creds = HyperliquidCredentials(private_key=SDK_KEY)
        assert creds.address == WalletSigner(SDK_KEY).address.lower() and not creds.testnet
