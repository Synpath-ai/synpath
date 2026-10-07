"""Limitless order entry: signing checked against an independent EIP-712
encoder and the docs' worked examples, order bodies, REST records, and the
adapter against a scripted venue."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
from datetime import datetime, timezone
from decimal import Decimal

import httpx
import pytest
from eth_account import Account as EthAccount
from eth_account.messages import _hash_eip191_message, encode_typed_data

from synpath.limitless import fee_schedule_of
from synpath.trading import limitless_signing as sig
from synpath.trading.credentials import CredentialsMissing, LimitlessCredentials, load_limitless
from synpath.trading.errors import InsufficientFunds, InvalidOrder, OrderRejected
from synpath.trading.limitless import (
    LimitlessTrading, build_signed_order, fill_of_history, market_info_of, order_of_row, position_of,
    status_after_placing,
)
from synpath.trading.polymarket_signing import WalletSigner
from synpath.trading.types import Liquidity, OrderRequest, OrderStatus, OrderType, PositionSide, Side, TimeInForce

from conftest import load

KEY = "0x" + "11" * 32
OWNER = EthAccount.from_key(KEY)
SECRET = base64.b64encode(b"token-secret").decode()
MARKET = load("limitless_market.json")
INFO = market_info_of(MARKET)
SLUG = MARKET["slug"]
MARKET_ID = f"limitless:{SLUG}"
PROFILE = {"id": 4242, "account": OWNER.address, "tradeWalletOption": "eoa", "rank": {"feeRateBps": 300}}


def request(**changes) -> OrderRequest:
    base = {"market_id": MARKET_ID, "side": Side.BUY, "amount": Decimal("10"), "price": Decimal("0.5")}
    return OrderRequest(**{**base, **changes})


def signed(req: OrderRequest, info=INFO, fee=300):
    return build_signed_order(req, info, signer=WalletSigner(KEY), owner_id=4242, fee_rate_bps=fee, salt=7)


def recover(order: dict) -> str:
    fields = {k: (int(v) if k in ("salt", "tokenId", "makerAmount", "takerAmount", "expiration") else v)
              for k, v in order.items() if k not in ("signature", "price")}
    fields["expiration"] = int(fields["expiration"])
    typed = sig.order_typed_data(fields, exchange=INFO.exchange)
    return EthAccount.recover_message(encode_typed_data(full_message=typed), signature=order["signature"])


class TestSigning:
    def test_the_digest_and_signature_are_eip712(self):
        order = sig.build_order(maker=OWNER.address, token_id=INFO.yes_token, maker_amount=5_000_000,
                                taker_amount=10_000_000, side="BUY", fee_rate_bps=300, salt=12345)
        encoded = encode_typed_data(full_message=sig.order_typed_data(order, exchange=INFO.exchange))
        assert sig.order_digest(order, exchange=INFO.exchange) == _hash_eip191_message(encoded)
        assert sig.sign_order(WalletSigner(KEY), order, exchange=INFO.exchange) == \
            "0x" + OWNER.sign_message(encoded).signature.hex().removeprefix("0x")

    def test_the_docs_examples(self):
        """BUY 10 shares at $0.50: 5,000,000 for 10,000,000; SELL the reverse."""
        assert sig.limit_amounts("BUY", Decimal("0.5"), Decimal("10"))[:2] == (5_000_000, 10_000_000)
        assert sig.limit_amounts("SELL", Decimal("0.5"), Decimal("10"))[:2] == (10_000_000, 5_000_000)

    def test_sizes_are_cut_to_thousandths_so_collateral_is_exact(self):
        maker, taker, units = sig.limit_amounts("SELL", Decimal("0.537"), Decimal("12.3456789"))
        assert units == 12_345_000 and maker == units and taker == 6_629_265

    def test_price_and_size_limits(self):
        with pytest.raises(InvalidOrder):
            sig.limit_amounts("BUY", Decimal("0.995"), Decimal("10"))
        with pytest.raises(InvalidOrder, match="decimal"):
            sig.limit_amounts("BUY", Decimal("0.5005"), Decimal("10"))
        with pytest.raises(InvalidOrder, match="minimum"):
            sig.limit_amounts("BUY", Decimal("0.01"), Decimal("0.001"))

    def test_requests_are_signed_with_the_token(self):
        now = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)
        headers = sig.request_headers("tok", SECRET, "post", "/orders", '{"a":1}', now=now)
        message = "2026-10-07T12:00:00.000Z\nPOST\n/orders\n{\"a\":1}"
        expected = base64.b64encode(hmac.new(b"token-secret", message.encode(), hashlib.sha256).digest()).decode()
        assert headers == {"lmts-api-key": "tok", "lmts-timestamp": "2026-10-07T12:00:00.000Z", "lmts-signature": expected}


class TestOrderBody:
    def test_a_buy_is_a_resting_limit_on_yes(self):
        body, contracts = signed(request())
        order = body["order"]
        assert body["orderType"] == "GTC" and body["ownerId"] == 4242 and body["marketSlug"] == SLUG
        assert order["tokenId"] == INFO.yes_token and order["side"] == 0 and order["feeRateBps"] == 300
        assert order["expiration"] == "0" and order["nonce"] == 0 and order["price"] == 0.5
        assert order["maker"] == order["signer"] == OWNER.address and contracts == Decimal("10")
        assert recover(order) == OWNER.address

    def test_a_sell_buys_no_at_the_complement(self):
        order = signed(request(side=Side.SELL, price=Decimal("0.3")))[0]["order"]
        assert order["tokenId"] == INFO.no_token and order["side"] == 0 and order["price"] == 0.7

    def test_reduce_only_sells_the_token_held(self):
        order = signed(request(side=Side.SELL, reduce_only=True))[0]["order"]
        assert order["tokenId"] == INFO.yes_token and order["side"] == 1

    def test_market_and_ioc_are_fill_and_kill_and_post_only_rests(self):
        assert signed(request(type=OrderType.MARKET, time_in_force=TimeInForce.IOC))[0]["orderType"] == "FAK"
        assert signed(request(time_in_force=TimeInForce.IOC))[0]["orderType"] == "FAK"
        body = signed(request(post_only=True, client_order_id="c-1"))[0]
        assert body["postOnly"] is True and body["clientOrderId"] == "c-1"

    def test_a_fee_free_market_signs_no_fee(self):
        free = market_info_of({**MARKET, "metadata": {**MARKET["metadata"], "fee": False}})
        assert signed(request(), info=free)[0]["order"]["feeRateBps"] == 0

    @pytest.mark.parametrize("changes, match", [
        ({"time_in_force": TimeInForce.FOK}, "fill-or-kill"),
        ({"time_in_force": TimeInForce.GTD, "expires_at": 1_900_000_000_000}, "expire"),
        ({"post_only": True, "time_in_force": TimeInForce.IOC}, "post-only"),
    ])
    def test_refusals(self, changes, match):
        with pytest.raises(InvalidOrder, match=match):
            signed(request(**changes))

    def test_an_amm_market_is_not_tradable_here(self):
        with pytest.raises(InvalidOrder):
            market_info_of({**MARKET, "tradeType": "amm"})


class TestRecords:
    def test_an_order_row_on_no_reads_on_yes(self):
        row = {"id": "o-1", "token": INFO.no_token, "type": "GTC", "status": "LIVE", "side": "BUY", "price": "0.3",
               "originalSize": "10000000", "remainingSize": "4000000", "createdAt": "2026-10-07T10:00:00.000Z",
               "clientOrderId": "c-1"}
        order = order_of_row(row, INFO)
        assert (order.side, order.price, order.amount, order.filled, order.remaining) == (
            Side.SELL, Decimal("0.7"), Decimal("10"), Decimal("6"), Decimal("4"))
        assert order.status == OrderStatus.OPEN and order.client_order_id == "c-1"
        assert order_of_row({**row, "status": "UNMATCHED", "type": "FAK"}, INFO).status == OrderStatus.CANCELED

    def test_history_rows_are_settled_fills(self):
        rows = load("limitless_history_account.json")["data"]
        trades = [r for r in rows if r.get("orderId")]
        assert trades and all(fill_of_history(r) is None for r in rows if not r.get("orderId"))
        for row in trades:
            fill = fill_of_history(row)
            assert fill is not None and fill.id == f"{row['tradeEventId']}:{row['orderId']}"
            on_no = row["outcomeIndex"] == 1
            assert fill.price == pytest.approx(Decimal(1) - Decimal(str(row["outcomeTokenPrice"])) if on_no
                                               else Decimal(str(row["outcomeTokenPrice"])))
            buying = "Buy" in row["strategy"]
            assert fill.side == (Side.BUY if buying != on_no else Side.SELL)
        assert fill_of_history({"strategy": "Split", "orderId": None}) is None

    def test_positions_net_on_yes(self):
        entry = load("limitless_positions.json")["clob"][0]
        position = position_of(entry)
        yes = int(entry["tokensBalance"]["yes"]) / 1_000_000
        assert position.inventory_yes == pytest.approx(Decimal(str(yes)))
        assert position.side == PositionSide.LONG and position.resolved

    @pytest.mark.parametrize("settlement, taking, filled, status", [
        ("UNMATCHED", False, "0", OrderStatus.OPEN),
        ("MATCHED", False, "4", OrderStatus.OPEN),
        ("MINED", False, "10", OrderStatus.CLOSED),
        ("UNMATCHED", True, "0", OrderStatus.CANCELED),
        ("DELAYED", True, "0", OrderStatus.OPEN),
        ("FAILED", False, "0", OrderStatus.REJECTED),
    ])
    def test_status_after_placing(self, settlement, taking, filled, status):
        assert status_after_placing(settlement, taking=taking, filled=Decimal(filled), amount=Decimal("10")) == status


class TestCredentials:
    def test_a_token_is_required(self):
        with pytest.raises(CredentialsMissing, match="TOKEN"):
            load_limitless({"LIMITLESS_PRIVATE_KEY": KEY})
        with pytest.raises(CredentialsMissing, match="base64"):
            load_limitless({"LIMITLESS_PRIVATE_KEY": KEY, "LIMITLESS_API_TOKEN_ID": "t", "LIMITLESS_API_SECRET": "not base64!"})

    def test_secrets_stay_out_of_repr(self):
        creds = load_limitless({"LIMITLESS_PRIVATE_KEY": KEY[2:], "LIMITLESS_API_TOKEN_ID": "t", "LIMITLESS_API_SECRET": SECRET})
        assert creds.private_key == KEY and creds.address == OWNER.address
        assert KEY not in repr(creds) and SECRET not in repr(creds) and creds.secrets == [KEY, SECRET]


# ---------------------------------------------------------------------------
# The adapter against a scripted venue
# ---------------------------------------------------------------------------

class Catalog:
    def _raw(self, slug):
        return MARKET

    def fetch_fee_schedule(self, market_id):
        return fee_schedule_of(MARKET, market_id=market_id)

    def close(self):
        pass


class Venue:
    """Answers the venue's routes, checks every request's signature, and records them."""

    def __init__(self, **answers):
        self.answers = {"GET /profiles/me": PROFILE, **answers}
        self.requests: list[httpx.Request] = []

    def __call__(self, req: httpx.Request) -> httpx.Response:
        self.requests.append(req)
        target = req.url.raw_path.decode()
        message = f"{req.headers['lmts-timestamp']}\n{req.method}\n{target}\n{req.content.decode()}"
        expected = base64.b64encode(hmac.new(b"token-secret", message.encode(), hashlib.sha256).digest()).decode()
        assert req.headers["lmts-signature"] == expected and req.headers["lmts-api-key"] == "tok"
        answer = self.answers.get(f"{req.method} {req.url.path}")
        if answer is None:
            return httpx.Response(404, json={"message": "not found"})
        return answer(req) if callable(answer) else httpx.Response(200, json=answer)

    def sent(self, method: str, path: str) -> list[httpx.Request]:
        return [r for r in self.requests if r.method == method and r.url.path == path]


def adapter(venue: Venue, rpc=None) -> LimitlessTrading:
    creds = LimitlessCredentials(private_key=KEY, token_id="tok", secret=SECRET)
    return LimitlessTrading(
        creds, catalog=Catalog(), client=httpx.AsyncClient(transport=httpx.MockTransport(venue)),
        rpc_client=rpc or httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(500))),
    )


def run(coro):
    return asyncio.run(coro)


def placed(execution: dict) -> callable:
    def answer(req):
        body = json.loads(req.content)
        return httpx.Response(201, json={"order": {"id": "o-1", **{k: v for k, v in body["order"].items() if k != "signature"}},
                                         "execution": execution})
    return answer


class TestAdapter:
    def test_an_order_is_signed_sent_compact_and_read_back(self):
        venue = Venue(**{"POST /orders": placed({"matched": True, "settlementStatus": "MATCHED", "feeRateBps": 300,
                                                 "effectiveFeeBps": 300, "totalsRaw": {"contractsGross": "4000000"}})})

        async def go():
            async with adapter(venue) as lm:
                return await lm.create_order(request(client_order_id="c-1"))

        order = run(go())
        [sent] = venue.sent("POST", "/orders")
        assert b": " not in sent.content and json.loads(sent.content)["ownerId"] == 4242
        assert order.id == "o-1" and order.filled == Decimal("4") and order.status == OrderStatus.OPEN
        assert "signature" not in order.info["request"]["order"]

    def test_a_smart_wallet_profile_is_refused_before_anything_is_signed(self):
        venue = Venue(**{"GET /profiles/me": {**PROFILE, "tradeWalletOption": "smartWallet"}})

        async def go():
            async with adapter(venue) as lm:
                await lm.create_order(request())

        with pytest.raises(CredentialsMissing, match="use_eoa_trading_mode"):
            run(go())
        assert venue.sent("POST", "/orders") == []

    def test_another_wallets_token_is_refused(self):
        venue = Venue(**{"GET /profiles/me": {**PROFILE, "account": "0x" + "22" * 20}})

        async def go():
            async with adapter(venue) as lm:
                await lm.create_order(request())

        with pytest.raises(CredentialsMissing, match="belongs to"):
            run(go())

    def test_switching_to_eoa_mode_is_explicit(self):
        venue = Venue(**{"PUT /profiles": {"ok": True}})

        async def go():
            async with adapter(venue) as lm:
                await lm.use_eoa_trading_mode()

        run(go())
        [sent] = venue.sent("PUT", "/profiles")
        assert json.loads(sent.content) == {"tradeWalletOption": "eoa"}

    def test_the_venue_s_refusal_is_typed(self):
        venue = Venue(**{"POST /orders": lambda req: httpx.Response(400, json={"message": "Insufficient USDC balance"})})

        async def go():
            async with adapter(venue) as lm:
                await lm.create_order(request())

        with pytest.raises(InsufficientFunds):
            run(go())

    def test_fetch_order_finds_its_market_then_its_row(self):
        row = {"id": "o-1", "token": INFO.yes_token, "type": "GTC", "status": "LIVE", "side": "BUY", "price": "0.5",
               "originalSize": "10000000", "remainingSize": "10000000"}
        venue = Venue(**{
            "POST /orders/status/batch": {"results": [{"index": 0, "status": "found", "orderId": "o-1",
                                                       "data": {"order": {"id": "o-1", "market": {"slug": SLUG}}}}]},
            f"GET /markets/{SLUG}/user-orders": [row],
        })

        async def go():
            async with adapter(venue) as lm:
                return await lm.fetch_order("o-1")

        order = run(go())
        assert order.status == OrderStatus.OPEN and order.market_id == MARKET_ID
        [rows] = venue.sent("GET", f"/markets/{SLUG}/user-orders")
        assert rows.url.params["limit"] == "200"

    def test_open_orders_span_the_markets_the_positions_name(self):
        positions = {"clob": [{"market": {"slug": SLUG}, "orders": {"liveOrders": [{"id": "o-1"}], "totalCollateralLocked": "5000000"}},
                              {"market": {"slug": "quiet-market"}, "orders": {"liveOrders": [], "totalCollateralLocked": "0"}}]}
        row = {"id": "o-1", "token": INFO.yes_token, "type": "GTC", "status": "LIVE", "side": "BUY", "price": "0.5",
               "originalSize": "10000000", "remainingSize": "10000000"}
        venue = Venue(**{"GET /portfolio/positions": positions, f"GET /markets/{SLUG}/user-orders": [row]})

        async def go():
            async with adapter(venue) as lm:
                return await lm.fetch_open_orders()

        orders = run(go())
        assert [o.id for o in orders] == ["o-1"]
        [sent] = venue.sent("GET", f"/markets/{SLUG}/user-orders")
        assert sent.url.params.get_list("statuses") == ["LIVE"]

    def test_batch_cancel_reports_refusals(self):
        rows = [{"id": i, "token": INFO.yes_token, "type": "GTC", "status": s, "side": "BUY", "price": "0.5",
                 "originalSize": "10000000", "remainingSize": "10000000"} for i, s in (("o-1", "CANCELED"), ("o-2", "LIVE"))]

        def status(req):
            order_id = json.loads(req.content)["items"][0]["orderId"]
            return httpx.Response(200, json={"results": [{"index": 0, "status": "found", "orderId": order_id,
                                                           "data": {"order": {"id": order_id, "market": {"slug": SLUG}}}}]})

        venue = Venue(**{
            "POST /orders/batch-cancel": {"message": "ok", "canceled": ["o-1"], "failed": [{"orderId": "o-2", "message": "locked"}]},
            "POST /orders/status/batch": status, f"GET /markets/{SLUG}/user-orders": rows,
        })

        async def go():
            async with adapter(venue) as lm:
                return await lm.cancel_orders(["o-1", "o-2"])

        first, second = run(go())
        assert first.status == OrderStatus.CANCELED and isinstance(second, OrderRejected)

    def test_the_balance_is_on_chain_less_what_orders_hold(self):
        def rpc(req):
            call = json.loads(req.content)["params"][0]
            assert call["to"] == sig.USDC and call["data"].endswith(OWNER.address[2:].lower())
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": hex(25_000_000)})

        venue = Venue(**{"GET /portfolio/positions": {"clob": [{"orders": {"totalCollateralLocked": "4000000"}}]}})

        async def go():
            async with adapter(venue, rpc=httpx.AsyncClient(transport=httpx.MockTransport(rpc))) as lm:
                return await lm.fetch_balance()

        balance = run(go())
        assert (balance.total, balance.locked, balance.available) == (Decimal("25"), Decimal("4"), Decimal("21"))

    def test_my_trades_come_from_the_history(self):
        venue = Venue(**{"GET /portfolio/history": load("limitless_history_account.json")})

        async def go():
            async with adapter(venue) as lm:
                return await lm.fetch_my_trades(market_id=MARKET_ID, limit=5)

        fills = run(go())
        [sent] = venue.sent("GET", "/portfolio/history")
        assert sent.url.params["market"] == SLUG and fills and fills.next_cursor

    def test_the_fee_estimate_is_on_the_token_bought(self):
        async def go():
            async with adapter(Venue()) as lm:
                return (await lm.fetch_fee_estimate(MARKET_ID, Side.BUY, Decimal("0.3"), Decimal("100")),
                        await lm.fetch_fee_estimate(MARKET_ID, Side.SELL, Decimal("0.3"), Decimal("100")))

        buy, sell = run(go())
        assert buy.taker_fee == Decimal("0.9") and sell.taker_fee == pytest.approx(Decimal("0.0151") * Decimal("0.7") * 100)
        assert buy.maker_fee == 0
