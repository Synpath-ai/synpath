"""`synpath init`: asks, checks, writes .env readable by its owner only, keeps what was there."""
from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

from synpath.trading.credentials import load_credentials, load_market_data, read_dotenv
from synpath.trading.init import Prompter, merge, run

KEY = "0x" + "ab" * 32
WALLET = "0x" + "12" * 20
PEM = "-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n"


def scripted(answers: list[str], secrets: list[str] | None = None) -> tuple[Prompter, list[str]]:
    said: list[str] = []
    asked = iter(answers)
    hidden = iter(secrets or [])
    return Prompter(ask=lambda _q: next(asked), ask_secret=lambda _q: next(hidden), say=said.append), said


def mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def test_it_writes_every_venue_asked_for_readable_by_the_owner_only(tmp_path: Path):
    pem = tmp_path / "kalshi.pem"
    pem.write_text(PEM)
    env = tmp_path / ".env"
    p, _ = scripted(
        ["y", "kid-1", str(pem), "",            # Kalshi: prod by default
         "y", WALLET, "",                        # Polymarket: signature type 3 by default
         "y", "us-key",                           # Polymarket US
         "n",                                     # Opinion
         "n",                                     # Hyperliquid
         "n",                                     # predict.fun
         "n",                                     # Limitless
         "n",                                     # SX Bet
         "n",                                     # Betfair
         "n",                                     # Smarkets
         "n"],                                    # Novig
        secrets=[KEY, "us-secret"],
    )
    assert run(env, p) == 0
    values = read_dotenv(env)
    assert values["KALSHI_KEY_ID"] == "kid-1" and values["KALSHI_ENV"] == "prod"
    assert values["KALSHI_PRIVATE_KEY_PATH"] == str(pem.resolve())
    assert values["POLYMARKET_PRIVATE_KEY"] == KEY and values["POLYMARKET_SIGNATURE_TYPE"] == "3"
    assert values["POLYMARKET_US_SECRET_KEY"] == "us-secret"
    assert mode(env) == 0o600
    loaded = load_credentials({}, dotenv=env, redact_logs=False)
    assert loaded["kalshi"] is not None and loaded["polymarket"] is not None and loaded["polymarket_us"] is not None


def test_wrong_values_are_asked_again_with_the_reason(tmp_path: Path):
    pem = tmp_path / "kalshi.pem"
    pem.write_text(PEM)
    not_pem = tmp_path / "notes.txt"
    not_pem.write_text("id: 123\n")
    p, said = scripted(
        ["y", "", "kid-1", str(tmp_path / "missing.pem"), str(not_pem), str(pem), "staging", "demo",
         "y", "0x1234", WALLET, "7", "1",
         "n", "n", "n", "n", "n", "n", "n", "n", "n"],
        secrets=["nothex", KEY[2:]],
    )
    assert run(tmp_path / ".env", p) == 0
    text = " ".join(said)
    for reason in ("Key ID is required", "No file at", "not a PEM key", "Enter one of: prod, demo",
                   "not a private key", "not a wallet address", "Enter one of: 0, 1, 2, 3"):
        assert reason in text, reason
    values = read_dotenv(tmp_path / ".env")
    assert values["KALSHI_ENV"] == "demo" and values["POLYMARKET_SIGNATURE_TYPE"] == "1"
    assert values["POLYMARKET_PRIVATE_KEY"] == KEY, "a key typed without 0x is written with it"


def test_an_existing_file_keeps_its_other_lines_and_is_tightened(tmp_path: Path):
    env = tmp_path / ".env"
    env.write_text("# mine\nSOMETHING_ELSE=keep\nPOLYMARKET_US_KEY_ID=old\n")
    os.chmod(env, 0o644)
    p, _ = scripted(["n", "n", "y", "new-key", "n", "n", "n", "n", "n", "n", "n", "n"], secrets=["new-secret"])
    assert run(env, p) == 0
    text = env.read_text()
    assert "# mine" in text and "SOMETHING_ELSE=keep" in text
    assert "POLYMARKET_US_KEY_ID=new-key" in text and "old" not in text
    assert text.count("POLYMARKET_US_KEY_ID=") == 1
    assert mode(env) == 0o600


def test_saying_no_to_everything_writes_nothing(tmp_path: Path):
    p, said = scripted(["n", "n", "n", "n", "n", "n", "n", "n", "n", "n", "n"])
    assert run(tmp_path / ".env", p) == 0
    assert not (tmp_path / ".env").exists() and any("Nothing changed." in line for line in said)


def test_merge_quotes_values_with_spaces_and_updates_in_place():
    out = merge("A=1\nexport B=2\n", {"B": "two words", "C": "3"})
    assert out.splitlines() == ["A=1", 'B="two words"', "", "# written by synpath init", "C=3"]


@pytest.mark.skipif(shutil.which("git") is None, reason="git is not installed")
def test_inside_a_git_repository_it_offers_to_ignore_the_file(tmp_path: Path):
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    p, said = scripted(["n", "n", "y", "k", "n", "n", "n", "n", "n", "n", "n", "n", "y"], secrets=["s"])
    assert run(tmp_path / ".env", p) == 0
    assert (tmp_path / ".gitignore").read_text() == ".env\n"
    ignored = subprocess.run(["git", "check-ignore", "-q", ".env"], cwd=tmp_path)
    assert ignored.returncode == 0


def test_opinion_takes_a_key_it_is_given(tmp_path: Path):
    env = tmp_path / ".env"
    p, _ = scripted(["n", "n", "n", "y", "n", "n", "n", "n", "n", "n", "n"], secrets=["opinion-api-key", KEY[2:]])
    assert run(env, p) == 0
    values = read_dotenv(env)
    assert values["OPINION_PRIVATE_KEY"] == KEY and values["OPINION_API_KEY"] == "opinion-api-key"
    assert load_credentials({}, dotenv=env, redact_logs=False)["opinion"] is not None


def test_opinion_with_an_api_key_needs_no_private_key(tmp_path: Path):
    env = tmp_path / ".env"
    p, said = scripted(["n", "n", "n", "y", "n", "n", "n", "n", "n", "n", "n"], secrets=["opinion-api-key", ""])
    assert run(env, p) == 0
    assert read_dotenv(env) == {"OPINION_API_KEY": "opinion-api-key"}
    assert load_credentials({}, dotenv=env, redact_logs=False)["opinion"] is None, "not enough to trade"
    assert load_market_data({}, dotenv=env, redact_logs=False)["opinion"].api_key == "opinion-api-key"
    assert any("needed only for trading" in line for line in said)


def test_opinion_creates_the_api_key_when_left_empty(tmp_path: Path, monkeypatch):
    import synpath.trading.opinion as opinion

    signed_with: list[str] = []
    monkeypatch.setattr(opinion, "create_api_key", lambda key: signed_with.append(key) or "created-key")
    p, said = scripted(["n", "n", "n", "y", "n", "n", "n", "n", "n", "n", "n"], secrets=["", "", KEY])
    assert run(tmp_path / ".env", p) == 0
    assert signed_with == [KEY], "creating a key needs the private key: an empty one is asked again"
    assert read_dotenv(tmp_path / ".env")["OPINION_API_KEY"] == "created-key"
    assert any("Created the API key" in line for line in said)


def test_hyperliquid_takes_an_api_wallet_and_its_account(tmp_path: Path):
    env = tmp_path / ".env"
    p, said = scripted(["n", "n", "n", "n", "y", "0x1234", WALLET, "maybe", "y", "n", "n", "n", "n", "n", "n"], secrets=[KEY[2:]])
    assert run(env, p) == 0
    values = read_dotenv(env)
    assert values["HYPERLIQUID_PRIVATE_KEY"] == KEY and values["HYPERLIQUID_ACCOUNT_ADDRESS"] == WALLET
    assert values["HYPERLIQUID_TESTNET"] == "1"
    loaded = load_credentials({}, dotenv=env, redact_logs=False)["hyperliquid"]
    assert loaded is not None and loaded.testnet and loaded.address == WALLET.lower()
    assert any("not a wallet address" in line for line in said)


def test_hyperliquid_with_the_accounts_own_key(tmp_path: Path):
    env = tmp_path / ".env"
    p, _ = scripted(["n", "n", "n", "n", "y", "", "", "n", "n", "n", "n", "n", "n"], secrets=[KEY])
    assert run(env, p) == 0
    values = read_dotenv(env)
    assert values["HYPERLIQUID_PRIVATE_KEY"] == KEY
    assert "HYPERLIQUID_ACCOUNT_ADDRESS" not in values and "HYPERLIQUID_TESTNET" not in values


def test_predict_fun_takes_a_predict_account_and_an_api_key(tmp_path: Path):
    env = tmp_path / ".env"
    p, said = scripted(["n", "n", "n", "n", "n", "y", "", "0x1234", WALLET, "n", "n", "n", "n", "n"], secrets=["pred_sk_test", KEY[2:]])
    assert run(env, p) == 0
    values = read_dotenv(env)
    assert values["PREDICT_FUN_PRIVATE_KEY"] == KEY and values["PREDICT_FUN_ACCOUNT_ADDRESS"] == WALLET
    assert values["PREDICT_FUN_API_KEY"] == "pred_sk_test" and "PREDICT_FUN_TESTNET" not in values
    loaded = load_credentials({}, dotenv=env, redact_logs=False)["predict_fun"]
    assert loaded is not None and loaded.address == WALLET and not loaded.testnet
    assert any("not a wallet address" in line for line in said)
    assert any("Privy wallet" in line for line in said)


def test_predict_fun_with_only_an_api_key_writes_just_the_key(tmp_path: Path):
    env = tmp_path / ".env"
    p, said = scripted(["n", "n", "n", "n", "n", "y", "", "n", "n", "n", "n", "n"], secrets=["", "pred_sk_test", WALLET, ""])
    assert run(env, p) == 0
    assert read_dotenv(env) == {"PREDICT_FUN_API_KEY": "pred_sk_test"}
    text = " ".join(said)
    assert "The API key is required" in text, "the API key is the one thing mainnet cannot do without"
    assert "That is a wallet address" in text, "a pasted 0x address is told apart from a private key"
    assert load_credentials({}, dotenv=env, redact_logs=False)["predict_fun"] is None
    assert load_market_data({}, dotenv=env, redact_logs=False)["predict_fun"].api_key == "pred_sk_test"


def test_predict_fun_on_the_test_network_needs_no_api_key(tmp_path: Path):
    env = tmp_path / ".env"
    p, _ = scripted(["n", "n", "n", "n", "n", "y", "y", "", "n", "n", "n", "n", "n"], secrets=[KEY])
    assert run(env, p) == 0
    values = read_dotenv(env)
    assert values["PREDICT_FUN_TESTNET"] == "1" and "PREDICT_FUN_API_KEY" not in values
    loaded = load_credentials({}, dotenv=env, redact_logs=False)["predict_fun"]
    assert loaded is not None and loaded.testnet and loaded.api_key is None


def test_limitless_takes_a_wallet_and_an_api_token(tmp_path: Path):
    env = tmp_path / ".env"
    secret = "c2VjcmV0LWJ5dGVz"
    p, _ = scripted(["n", "n", "n", "n", "n", "n", "y", "token-123", "n", "n", "n", "n"], secrets=[KEY[2:], secret])
    assert run(env, p) == 0
    values = read_dotenv(env)
    assert values["LIMITLESS_PRIVATE_KEY"] == KEY and values["LIMITLESS_API_TOKEN_ID"] == "token-123"
    assert values["LIMITLESS_API_SECRET"] == secret
    loaded = load_credentials({}, dotenv=env, redact_logs=False)["limitless"]
    assert loaded is not None and loaded.token_id == "token-123"


def test_sx_bet_takes_a_wallet_and_an_api_key(tmp_path: Path):
    env = tmp_path / ".env"
    p, _ = scripted(["n", "n", "n", "n", "n", "n", "n", "y", "y", "n", "n", "n"], secrets=["sx-api-key", KEY[2:]])
    assert run(env, p) == 0
    values = read_dotenv(env)
    assert values["SX_BET_PRIVATE_KEY"] == KEY and values["SX_BET_API_KEY"] == "sx-api-key"
    assert values["SX_BET_TESTNET"] == "1"
    loaded = load_credentials({}, dotenv=env, redact_logs=False)["sx_bet"]
    assert loaded is not None and loaded.api_key == "sx-api-key" and loaded.testnet


def test_sx_bet_with_only_an_api_key_and_the_wallet_address(tmp_path: Path):
    env = tmp_path / ".env"
    p, said = scripted(["n", "n", "n", "n", "n", "n", "n", "y", KEY, WALLET, "", "n", "n", "n"], secrets=["sx-api-key", ""])
    assert run(env, p) == 0
    assert read_dotenv(env) == {"SX_BET_API_KEY": "sx-api-key", "SX_BET_ACCOUNT_ADDRESS": WALLET}
    assert any("That looks like a private key" in line for line in said), "a key pasted as the address is refused"
    assert any("needed only for placing orders" in line for line in said)
    loaded = load_credentials({}, dotenv=env, redact_logs=False)["sx_bet"]
    assert loaded is not None and loaded.private_key is None and loaded.address == WALLET and not loaded.testnet


def test_sx_bet_with_only_an_api_key_writes_just_the_key(tmp_path: Path):
    env = tmp_path / ".env"
    p, _ = scripted(["n", "n", "n", "n", "n", "n", "n", "y", "", "", "n", "n", "n"], secrets=["sx-api-key", ""])
    assert run(env, p) == 0
    assert read_dotenv(env) == {"SX_BET_API_KEY": "sx-api-key"}
    loaded = load_credentials({}, dotenv=env, redact_logs=False)["sx_bet"]
    assert loaded is not None and loaded.private_key is None and loaded.address is None


def test_an_api_key_alone_counts_as_set_up_already(tmp_path: Path):
    env = tmp_path / ".env"
    env.write_text("SX_BET_API_KEY=sx-api-key\n")
    asked: list[str] = []
    p = Prompter(ask=lambda question: asked.append(question) or "n", ask_secret=lambda _q: "", say=lambda _: None)
    assert run(env, p) == 0
    assert any("Set up SX Bet (already set)?" in question for question in asked)
