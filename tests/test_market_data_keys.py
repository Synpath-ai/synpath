"""Market data from an API key alone: Opinion, predict.fun and SX Bet serve
books to an API key, so a user who only reads prices never sets a private key."""
from __future__ import annotations

import logging
from pathlib import Path

import httpx
import pytest
import respx

from synpath.engine.feeds import default_streams, market_stream
from synpath.trading.errors import CredentialsMissing
from synpath.server import serve
from synpath.server.api import VenueRegistry
from synpath.trading.credentials import (
    MarketDataKey, SecretFilter, load_credentials, load_market_data, require,
)

from conftest import load

KEY = "0x" + "ab" * 32
API_KEYS = {"OPINION_API_KEY": "op-key-123456", "PREDICT_FUN_API_KEY": "pred_sk_123456", "SX_BET_API_KEY": "sx-key-123456"}


class TestLoading:
    def test_an_api_key_alone_loads_for_market_data_and_not_for_trading(self):
        market_data = load_market_data(API_KEYS, redact_logs=False)
        assert {venue: key.api_key for venue, key in market_data.items()} == {
            "opinion": "op-key-123456", "predict_fun": "pred_sk_123456", "sx_bet": "sx-key-123456"}
        trading = load_credentials(API_KEYS, redact_logs=False)
        assert trading["opinion"] is None and trading["predict_fun"] is None
        assert trading["sx_bet"].private_key is None, "SX Bet's key reads the account and cancels: see test_sx_bet_key_only"

    def test_trading_still_needs_the_private_key_and_says_so(self):
        with pytest.raises(CredentialsMissing,
                           match="PREDICT_FUN_API_KEY reads market data; trading also needs the private key"):
            require("predict_fun", load_credentials(API_KEYS, redact_logs=False))

    def test_with_the_private_key_both_load(self):
        env = {**API_KEYS, "SX_BET_PRIVATE_KEY": KEY, "SX_BET_TESTNET": "1"}
        assert load_credentials(env, redact_logs=False)["sx_bet"].api_key == "sx-key-123456"
        assert load_market_data(env, redact_logs=False)["sx_bet"].testnet

    def test_the_dotenv_given_is_read_and_the_environment_wins(self, tmp_path: Path):
        env_file = tmp_path / "keys.env"
        env_file.write_text("PREDICT_FUN_API_KEY=from-file\nSX_BET_API_KEY=sx-from-file\n")
        loaded = load_market_data({"SX_BET_API_KEY": "sx-exported"}, dotenv=env_file, redact_logs=False)
        assert loaded["predict_fun"].api_key == "from-file" and loaded["sx_bet"].api_key == "sx-exported"

    def test_the_key_is_neither_shown_nor_logged(self, caplog):
        key = load_market_data({"PREDICT_FUN_API_KEY": "pred_sk_secret_value"})["predict_fun"]
        assert "pred_sk_secret_value" not in repr(key)
        record = logging.LogRecord("t", logging.INFO, __file__, 1, "key is %s", ("pred_sk_secret_value",), None)
        SecretFilter._instance.filter(record)
        assert "pred_sk_secret_value" not in record.getMessage()


class TestStreams:
    def test_each_api_key_venue_gets_a_market_stream_and_no_account_stream(self):
        streams = default_streams({}, {}, load_market_data(API_KEYS, redact_logs=False))
        assert set(streams) == {"opinion", "predict_fun", "sx_bet"}
        for venue, pair in streams.items():
            assert pair.market is not None and pair.market.venue == venue
            assert pair.private is None, "an account stream needs the trading credentials"
        assert streams["sx_bet"].market._api_key == "sx-key-123456"
        assert streams["predict_fun"].market._key == "pred_sk_123456"

    def test_predict_fun_on_the_test_network_has_no_stream(self):
        assert market_stream("predict_fun", MarketDataKey(venue="predict_fun", api_key="k", testnet=True)) is None

    def test_public_venues_need_no_key_and_get_none_this_way(self):
        assert market_stream("polymarket", MarketDataKey(venue="polymarket", api_key="k")) is None

    def test_a_traded_venue_keeps_its_own_streams(self):
        adapter = type("Adapter", (), {"catalog": None, "jwt": None})()
        trading = load_credentials({"PREDICT_FUN_PRIVATE_KEY": KEY, "PREDICT_FUN_API_KEY": "pred_sk_t"}, redact_logs=False)
        streams = default_streams({"predict_fun": adapter}, trading, load_market_data(API_KEYS, redact_logs=False))
        assert streams["predict_fun"].market._key == "pred_sk_t", "the trading adapter's stream, not a second one"


class TestReadApp:
    def test_the_registry_builds_adapters_with_the_options_given(self):
        registry = VenueRegistry(options={"predict_fun": {"api_key": "pred_sk_given"}})
        try:
            assert registry.get("predict_fun").has_key
        finally:
            registry.close()

    @pytest.mark.anyio
    async def test_serve_hands_the_read_app_the_keys_from_its_dotenv(self, tmp_path: Path, monkeypatch):
        """Run from a folder with no .env, keys only in --dotenv: search still
        reaches predict.fun with the key, rather than answering 502."""
        for name in API_KEYS:
            monkeypatch.delenv(name, raising=False)
        elsewhere = tmp_path / "keys.env"
        elsewhere.write_text("PREDICT_FUN_API_KEY=pred_sk_from_dotenv\n")
        workdir = tmp_path / "work"
        workdir.mkdir()
        monkeypatch.chdir(workdir)
        assert serve.read_options(load_market_data(dotenv=elsewhere, redact_logs=False)) == {
            "predict_fun": {"api_key": "pred_sk_from_dotenv"}}

        with respx.mock(assert_all_called=True) as venue:
            search = venue.get("https://api.predict.fun/v1/search").mock(
                return_value=httpx.Response(200, json=load("predict_fun_search.json")))
            stack = await serve.build(
                journal=str(tmp_path / "s.db"), control=str(tmp_path / "c.db"), dotenv=str(elsewhere),
                streams=False, adapters={}, home_dir=str(tmp_path / "home"))
            try:
                # serve's read routes take its access token, as /trading does.
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=stack.app), base_url="http://synpath",
                                             headers={"Authorization": f"Bearer {stack.owner_key}"}) as client:
                    answer = await client.get("/venues/predict_fun/markets", params={"query": "bitcoin", "limit": 5})
            finally:
                await serve.shutdown(stack)
        assert answer.status_code == 200, answer.text
        assert search.calls.last.request.headers["x-api-key"] == "pred_sk_from_dotenv"


@pytest.fixture
def anyio_backend():
    return "asyncio"


class TestSxBetReadKey:
    def test_a_key_given_is_sent_and_none_is_needed(self):
        from synpath import SxBet

        with respx.mock(assert_all_called=True) as venue:
            book = venue.get("https://api.sx.bet/orderbook-v3/snapshot").mock(
                return_value=httpx.Response(200, json={"status": "success", "data": {"outcomeOne": [], "outcomeTwo": []}}))
            market = "sx_bet:0x" + "12" * 32
            with SxBet(api_key="sx-key-123456", limiter=None) as keyed:
                keyed.fetch_order_book(market)
            assert book.calls.last.request.headers["x-sx-api-key"] == "sx-key-123456"
            with SxBet(limiter=None) as keyless:
                keyless.fetch_order_book(market)
            assert "x-sx-api-key" not in book.calls.last.request.headers


def test_doctor_reports_a_venue_set_up_for_market_data_only(tmp_path: Path, monkeypatch, capsys):
    from synpath.trading.__main__ import doctor

    for name in API_KEYS:
        monkeypatch.delenv(name, raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("PREDICT_FUN_API_KEY=pred_sk_123456\n")
    assert doctor(str(env_file)) == 0
    out = capsys.readouterr().out
    assert "market data (mainnet)" in out and "set PREDICT_FUN_PRIVATE_KEY to trade" in out
    assert "1 for market data only" in out and "pred_sk_123456" not in out
