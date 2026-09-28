"""The trading foundations: money, types, credentials, the budget, the async client.

No venue is reached. These pin the rules every adapter will lean on.
"""
from __future__ import annotations

import asyncio
import logging
from decimal import Decimal

import httpx
import pytest

import synpath
from synpath.base import CAPABILITIES, AsyncHttpClient, RateLimiter
from synpath.errors import MarketNotFound, RateLimitExceeded
from synpath.trading import (
    Account, Balance, EditRequest, Fill, InvalidOrder, Order, OrderRequest, OrderStatus,
    OrderType, Position, PositionSide, Precision, RateBudgetExceeded, SettlementState, Side, TimeInForce,
)
from synpath.trading import money
from synpath.trading.credentials import (
    CredentialsMissing, SecretFilter, load_credentials, read_dotenv, require,
)
from synpath.trading.instruments import MarketMaster, precision_for, spec_from_market
from synpath.trading.limiter import BudgetLimiter, Priority

D = Decimal


class TestCapabilityKeys:
    def test_every_read_adapter_answers_every_trading_key_with_false(self):
        trading = [key for key in CAPABILITIES if key in (
            "create_order", "cancel_order", "edit_order", "fetch_balance", "fetch_positions",
            "fetch_open_orders", "watch_orders", "rfq", "split_merge",
        )]
        assert len(trading) == 9
        for venue in synpath.exchanges.values():
            for key in trading:
                assert venue.has[key] is False, (venue.id, key)


class TestMoney:
    def test_floats_are_read_through_their_repr(self):
        assert money.D(0.1) == D("0.1")
        assert money.D("0.1065") == D("0.1065")
        assert money.D(3) == D("3")

    def test_none_and_bool_are_refused(self):
        with pytest.raises(InvalidOrder):
            money.D(None)
        with pytest.raises(InvalidOrder):
            money.D(True)

    def test_rounding_modes(self):
        assert money.round_to(D("0.1065"), D("0.001"), "nearest") == D("0.107")
        assert money.round_to(D("0.1065"), D("0.001"), "down") == D("0.106")
        assert money.round_to(D("0.1065"), D("0.001"), "up") == D("0.107")
        assert money.round_to(D("0.125"), D("0.01"), "bankers") == D("0.12")
        assert money.round_to(D("0.135"), D("0.01"), "bankers") == D("0.14")

    def test_price_is_checked_not_rounded(self):
        precision = Precision(tick=D("0.001"))
        assert money.validate_price(D("0.107"), precision) == D("0.107")
        with pytest.raises(InvalidOrder, match="not on the 0.001 tick"):
            money.validate_price(D("0.1065"), precision)
        for bad in (D("0"), D("1"), D("1.5"), D("-0.1")):
            with pytest.raises(InvalidOrder, match="outside"):
                money.validate_price(bad, precision)

    def test_amount_rules_per_venue(self):
        whole = Precision(tick=D("0.001"), min_amount=D("1"), amount_step=D("1"), whole_contracts=True)
        with pytest.raises(InvalidOrder, match="whole contracts"):
            money.validate_amount(D("1.5"), whole)
        five = Precision(tick=D("0.01"), min_amount=D("5"))
        with pytest.raises(InvalidOrder, match="below the minimum"):
            money.validate_amount(D("4.9"), five)
        assert money.validate_amount(D("5"), five) == D("5")
        cents = Precision(tick=D("0.01"), min_amount=D("0.01"), amount_step=D("0.01"))
        with pytest.raises(InvalidOrder, match="multiple"):
            money.validate_amount(D("1.005"), cents)

    def test_kalshi_wire_formats(self):
        assert money.kalshi_dollars(D("0.56")) == "0.5600"
        assert money.kalshi_count(D("10")) == "10.00"
        assert money.from_kalshi_cents(56) == D("0.5600")
        assert money.from_kalshi_dollars("0.5600") == D("0.5600")

    def test_polymarket_raw_amounts_round_trip_and_round_down(self):
        assert money.poly_raw_amount(D("1.5")) == 1_500_000
        assert money.poly_raw_amount(D("1.9999999")) == 1_999_999, "never round a maker up"
        assert money.from_poly_raw_amount(1_500_000) == D("1.500000")
        assert money.poly_price(D("0.5"), D("0.01")) == "0.50"

    def test_polymarket_us_wire_format(self):
        assert money.polyus_amount(D("0.106")) == "0.1060"
        assert money.from_polyus_amount({"value": "0.1060", "currency": "USD"}) == D("0.1060")

    def test_complement(self):
        assert money.complement(D("0.106")) == D("0.894")


class TestTypes:
    def test_limit_needs_a_price_and_gtd_needs_an_expiry(self):
        with pytest.raises(ValueError, match="needs a price"):
            OrderRequest(market_id="kalshi:m", side=Side.BUY, amount=D("1"))
        with pytest.raises(ValueError, match="expires_at"):
            OrderRequest(market_id="kalshi:m", side=Side.BUY, amount=D("1"), price=D("0.5"),
                         time_in_force=TimeInForce.GTD)
        ok = OrderRequest(market_id="kalshi:m", side=Side.BUY, amount=D("1"), price=D("0.5"))
        assert ok.type == OrderType.LIMIT and ok.time_in_force == TimeInForce.GTC

    def test_stop_orders_need_their_prices(self):
        with pytest.raises(ValueError, match="stop_price"):
            OrderRequest(market_id="kalshi:m", side=Side.SELL, amount=D("1"), type=OrderType.STOP_MARKET)
        with pytest.raises(ValueError, match="limit price"):
            OrderRequest(market_id="kalshi:m", side=Side.SELL, amount=D("1"), type=OrderType.STOP_LIMIT,
                         stop_price=D("0.3"))

    def test_remaining_is_derived_and_partial_fill_is_a_number_not_a_state(self):
        order = Order(
            id="1", venue="kalshi", market_id="kalshi:m", side=Side.BUY,
            type=OrderType.LIMIT, time_in_force=TimeInForce.GTC, status=OrderStatus.OPEN,
            amount=D("10"), filled=D("4"),
        )
        assert order.remaining == D("6")
        assert order.status == OrderStatus.OPEN and not order.is_terminal

    def test_pending_cancel_is_a_named_state(self):
        assert OrderStatus("pending_cancel") is OrderStatus.PENDING_CANCEL
        assert OrderStatus("pending_replace") is OrderStatus.PENDING_REPLACE

    def test_decimals_survive_json(self):
        fill = Fill(
            id="f", order_id="o", venue="polymarket", market_id="polymarket:1",
            side=Side.BUY, price=D("0.4200"), amount=D("12.5"), timestamp=1_700_000_000_000,
            settlement=SettlementState.MATCHED,
        )
        back = Fill.model_validate_json(fill.model_dump_json())
        assert back == fill and isinstance(back.price, Decimal) and back.price == D("0.4200")

    def test_account_key_and_balance_are_per_account(self):
        account = Account(venue="kalshi", name="main", subaccount="3")
        assert account.key == "kalshi:main:3"
        balance = Balance(venue="kalshi", account=account, currency="USD",
                          total=D("100"), available=D("60"), locked=D("40"))
        assert balance.buying_power is None, "a venue that defines none reports none, not zero"

    def test_position_carries_exposure_and_inventories_separately(self):
        position = Position(venue="polymarket", market_id="polymarket:1", side=PositionSide.LONG,
                            contracts=D("3"), inventory_yes=D("5"), inventory_no=D("2"), resolved=True, final=False)
        assert position.resolved and not position.final
        assert position.contracts == position.inventory_yes - position.inventory_no

    def test_edit_request_carries_its_own_idempotency_key(self):
        edit = EditRequest(order_id="o", price=D("0.5"), client_order_id="edit-1")
        assert edit.amount is None and edit.client_order_id == "edit-1"


class TestCredentials:
    PEM = (
        "-----BEGIN RSA PRIVATE KEY-----\n"
        "MIIEowIBAAKCAQEAsecretsecretsecretsecretsecretsecret\n"
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
        "-----END RSA PRIVATE KEY-----\n"
    )

    def _pem(self, tmp_path):
        path = tmp_path / "demo.pem"
        path.write_text(self.PEM)
        return path

    def test_kalshi_trades_on_the_real_exchange_unless_demo_is_asked_for(self, tmp_path):
        env = {"KALSHI_KEY_ID": "bd89-id", "KALSHI_PRIVATE_KEY_PATH": str(self._pem(tmp_path))}
        assert load_credentials(env, dotenv=tmp_path / "absent", redact_logs=False)["kalshi"].env == "prod"
        env["KALSHI_ENV"] = "demo"
        assert load_credentials(env, dotenv=tmp_path / "absent", redact_logs=False)["kalshi"].env == "demo"

    def test_loads_kalshi_from_env_and_keeps_the_key_out_of_repr(self, tmp_path):
        env = {"KALSHI_KEY_ID": "bd89-id", "KALSHI_PRIVATE_KEY_PATH": str(self._pem(tmp_path)), "KALSHI_ENV": "demo"}
        loaded = load_credentials(env, dotenv=tmp_path / "absent", redact_logs=False)
        creds = loaded["kalshi"]
        assert creds is not None and creds.key_id == "bd89-id" and creds.env == "demo"
        assert "secretsecret" not in repr(creds) and "bd89-id" in repr(creds)
        assert loaded["polymarket"] is None and loaded["polymarket_us"] is None

    def test_a_text_file_with_an_id_line_is_refused_with_advice(self, tmp_path):
        path = tmp_path / "synpath.txt"
        path.write_text("API key id: abc\n")
        env = {"KALSHI_KEY_ID": "abc", "KALSHI_PRIVATE_KEY_PATH": str(path)}
        with pytest.raises(CredentialsMissing, match="BEGIN"):
            load_credentials(env, dotenv=tmp_path / "absent", redact_logs=False)

    def test_require_names_the_variables(self):
        with pytest.raises(CredentialsMissing, match="KALSHI_KEY_ID"):
            require("kalshi", {"kalshi": None})

    def test_dotenv_is_read_and_the_environment_wins(self, tmp_path):
        dotenv = tmp_path / ".env"
        dotenv.write_text('POLYMARKET_PRIVATE_KEY="0xfromfile"\nPOLYMARKET_SIGNATURE_TYPE=2\nPOLYMARKET_FUNDER=0xabc\n# comment\n')
        assert read_dotenv(dotenv)["POLYMARKET_PRIVATE_KEY"] == "0xfromfile"
        loaded = load_credentials({"POLYMARKET_PRIVATE_KEY": "0xfromenv"}, dotenv=dotenv, redact_logs=False)
        assert loaded["polymarket"].private_key == "0xfromenv"
        assert loaded["polymarket"].signature_type == 2

    def test_secrets_are_scrubbed_from_log_records(self, tmp_path, caplog):
        env = {"KALSHI_KEY_ID": "k", "KALSHI_PRIVATE_KEY_PATH": str(self._pem(tmp_path))}
        load_credentials(env, dotenv=tmp_path / "absent")   # installs the filter
        logger = logging.getLogger("synpath.test.secrets")
        logger.addFilter(SecretFilter._instance)
        with caplog.at_level(logging.INFO, logger="synpath.test.secrets"):
            logger.info("request body: %s", self.PEM)
        assert "secretsecret" not in caplog.text
        assert "***" in caplog.text


class TestBudgetLimiter:
    def _run(self, coro):
        return asyncio.run(coro)

    def test_reads_and_writes_are_separate_buckets(self):
        limiter = BudgetLimiter(read_per_second=200, write_per_second=100, burst_seconds=1)
        for _ in range(10):
            assert limiter.reserve(10, "write").wait == 0.0
        assert limiter.reserve(10, "write").wait > 0, "write budget spent"
        assert limiter.reserve(10, "read").wait == 0.0, "read budget untouched"

    def test_normal_calls_queue_in_order_by_reservation(self):
        limiter = BudgetLimiter(read_per_second=10, write_per_second=10, burst_seconds=1)
        first = limiter.reserve(10, "write").wait
        second = limiter.reserve(10, "write").wait
        third = limiter.reserve(10, "write").wait
        assert first == 0.0 and 0 < second < third

    def test_high_priority_goes_ahead_and_pushes_the_queue_back(self):
        limiter = BudgetLimiter(read_per_second=10, write_per_second=10, burst_seconds=1, borrow_seconds=1)
        limiter.reserve(10, "write")                                   # bucket now empty
        queued = limiter.reserve(10, "write", Priority.NORMAL)
        assert queued.wait > 0
        fast = limiter.reserve(10, "write", Priority.HIGH)
        assert fast.wait == 0.0, "a cancel does not wait"
        assert limiter.shift_since(queued) == pytest.approx(1.0), "the sleeper owes the cancel's cost"

    def test_the_fast_lane_is_finite(self):
        limiter = BudgetLimiter(read_per_second=10, write_per_second=10, burst_seconds=1, borrow_seconds=1)
        limiter.reserve(10, "write")
        assert limiter.reserve(10, "write", Priority.HIGH).wait == 0.0
        assert limiter.reserve(10, "write", Priority.HIGH).wait > 0, "the borrow bucket is spent; it waits like anyone"

    def test_a_wait_past_the_deadline_is_refused_and_nothing_is_kept(self):
        limiter = BudgetLimiter(read_per_second=10, write_per_second=1, burst_seconds=1, max_wait_s=0.5)
        limiter.reserve(1, "write")
        before = limiter.snapshot()["write"]["tokens"]
        with pytest.raises(RateBudgetExceeded) as caught:
            self._run(limiter.acquire(cost=5, kind="write"))
        assert caught.value.wait_s > 0.5
        assert limiter.snapshot()["write"]["tokens"] == pytest.approx(before, abs=0.05), "the refused draw is given back"

    def test_acquire_sleeps_for_the_reservation_and_then_for_any_shift(self, monkeypatch):
        slept: list[float] = []

        async def fake_sleep(seconds):
            slept.append(seconds)

        monkeypatch.setattr("synpath.trading.limiter.asyncio.sleep", fake_sleep)
        limiter = BudgetLimiter(read_per_second=10, write_per_second=10, burst_seconds=1, max_wait_s=10)
        self._run(limiter.acquire(cost=10))
        self._run(limiter.acquire(cost=10))
        assert slept == [pytest.approx(1.0, abs=0.05)]

        async def normal_then_cancel():
            reservation = limiter.reserve(10, "write")           # queued behind everything
            limiter.reserve(10, "write", Priority.HIGH)          # a cancel jumps in
            wait = reservation.wait
            slept.clear()
            while wait > 0:
                await asyncio.sleep(wait)
                wait = limiter.shift_since(reservation)
                reservation = type(reservation)("write", 10, 0.0, reservation.shift_then + wait)
        self._run(normal_then_cancel())
        assert len(slept) == 2 and slept[1] == pytest.approx(1.0, abs=0.05)

    def test_configure_adopts_the_venues_budget(self):
        limiter = BudgetLimiter(read_per_second=200, write_per_second=100, burst_seconds=1)
        limiter.configure(write_per_second=1000)
        assert limiter.snapshot()["write"]["rate"] == 1000
        assert limiter.snapshot()["write"]["capacity"] == 1000


class TestAsyncHttpClient:
    def _client(self, handler, **kwargs):
        return AsyncHttpClient(
            "https://venue.test", limiter=None, attempts=2,
            client=httpx.AsyncClient(transport=httpx.MockTransport(handler)), **kwargs,
        )

    def test_error_mapping_matches_the_sync_client(self):
        async def run():
            with pytest.raises(MarketNotFound):
                await self._client(lambda r: httpx.Response(404, text="nope")).get("/x")

        asyncio.run(run())

    def test_429_is_retried_then_raised(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            return httpx.Response(429, headers={"retry-after": "0"})

        async def run():
            with pytest.raises(RateLimitExceeded):
                await self._client(handler).get("/x")

        asyncio.run(run())
        assert calls["n"] == 2

    def test_an_empty_body_is_none_not_an_error(self):
        async def run():
            return await self._client(lambda r: httpx.Response(204)).delete("/orders/1")

        assert asyncio.run(run()) is None

    def test_the_shared_limiter_paces_without_blocking(self, monkeypatch):
        slept: list[float] = []

        async def fake_sleep(seconds):
            slept.append(seconds)

        monkeypatch.setattr("asyncio.sleep", fake_sleep)
        limiter = RateLimiter(10.0, burst=1)

        async def run():
            await limiter.acquire_async()
            await limiter.acquire_async()

        asyncio.run(run())
        assert slept and slept[0] == pytest.approx(0.1, abs=0.02)


class TestMarketMaster:
    def test_precision_rules_per_venue(self, kalshi_market, kalshi_event, poly_market, polyus_market):
        from synpath.kalshi import normalize_market as k
        from synpath.polymarket import normalize_market as p
        from synpath.polymarket_us import normalize_market as u

        kalshi = precision_for(k(kalshi_market, kalshi_event))
        assert kalshi.amount_step == D("0.01") and not kalshi.whole_contracts
        poly = precision_for(p(poly_market))
        assert poly.min_amount == D("5") and poly.tick == D("0.01")
        polyus = precision_for(u(polyus_market))
        assert polyus.whole_contracts and polyus.tick == D("0.001")

    def test_spec_carries_tradability_per_side(self, polyus_market):
        from synpath.polymarket_us import normalize_market as u

        spec = spec_from_market(u(polyus_market))
        assert spec.market_id == f"polymarket_us:{polyus_market['slug']}"
        assert spec.tradable and spec.tradable_yes and spec.tradable_no

    def test_stale_reports_missing_and_old(self, poly_market):
        from synpath.polymarket import normalize_market as p

        master = MarketMaster(ttl_s=60)
        market = p(poly_market)
        master.put_market(market)
        assert master.get(market.id).venue == "polymarket"
        assert master.get(market.id).yes_token == market.yes.venue_token_id
        assert master.stale([market.id, "unknown"]) == ["unknown"]
        later = master.get(market.id).read_at + 61_000
        assert master.stale([market.id], now_ms=later) == [market.id]

    def test_refresh_goes_through_the_callers_loader(self, poly_market):
        from synpath.polymarket import normalize_market as p

        market = p(poly_market)
        asked: list[tuple[str, list[str]]] = []

        async def loader(venue, market_ids):
            asked.append((venue, market_ids))
            return [market]

        master = MarketMaster()
        asyncio.run(master.refresh(loader, [market.id, market.id]))
        assert asked == [("polymarket", [market.id])], "the venue is read off the id, and duplicates collapse"
        assert len(master) == 1

    def test_refresh_refuses_a_bare_id_with_no_venue(self):
        from synpath import BadRequest

        async def loader(venue, market_ids):  # pragma: no cover - never reached
            return []

        with pytest.raises(BadRequest, match="not a Synpath id"):
            asyncio.run(MarketMaster().refresh(loader, ["5615282760"]))
