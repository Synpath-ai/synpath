"""`synpath init`: asks, checks, writes .env readable by its owner only, keeps what was there."""
from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

from synpath.trading.credentials import load_credentials, read_dotenv
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
         "n"],                                    # Opinion
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
         "n", "n"],
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
    p, _ = scripted(["n", "n", "y", "new-key", "n"], secrets=["new-secret"])
    assert run(env, p) == 0
    text = env.read_text()
    assert "# mine" in text and "SOMETHING_ELSE=keep" in text
    assert "POLYMARKET_US_KEY_ID=new-key" in text and "old" not in text
    assert text.count("POLYMARKET_US_KEY_ID=") == 1
    assert mode(env) == 0o600


def test_saying_no_to_everything_writes_nothing(tmp_path: Path):
    p, said = scripted(["n", "n", "n", "n"])
    assert run(tmp_path / ".env", p) == 0
    assert not (tmp_path / ".env").exists() and any("Nothing changed." in line for line in said)


def test_merge_quotes_values_with_spaces_and_updates_in_place():
    out = merge("A=1\nexport B=2\n", {"B": "two words", "C": "3"})
    assert out.splitlines() == ["A=1", 'B="two words"', "", "# written by synpath init", "C=3"]


@pytest.mark.skipif(shutil.which("git") is None, reason="git is not installed")
def test_inside_a_git_repository_it_offers_to_ignore_the_file(tmp_path: Path):
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    p, said = scripted(["n", "n", "y", "k", "n", "y"], secrets=["s"])
    assert run(tmp_path / ".env", p) == 0
    assert (tmp_path / ".gitignore").read_text() == ".env\n"
    ignored = subprocess.run(["git", "check-ignore", "-q", ".env"], cwd=tmp_path)
    assert ignored.returncode == 0


def test_opinion_takes_a_key_it_is_given(tmp_path: Path):
    env = tmp_path / ".env"
    p, _ = scripted(["n", "n", "n", "y"], secrets=[KEY[2:], "opinion-api-key"])
    assert run(env, p) == 0
    values = read_dotenv(env)
    assert values["OPINION_PRIVATE_KEY"] == KEY and values["OPINION_API_KEY"] == "opinion-api-key"
    assert load_credentials({}, dotenv=env, redact_logs=False)["opinion"] is not None


def test_opinion_creates_the_api_key_when_left_empty(tmp_path: Path, monkeypatch):
    import synpath.trading.opinion as opinion

    signed_with: list[str] = []
    monkeypatch.setattr(opinion, "create_api_key", lambda key: signed_with.append(key) or "created-key")
    p, said = scripted(["n", "n", "n", "y"], secrets=[KEY, ""])
    assert run(tmp_path / ".env", p) == 0
    assert signed_with == [KEY]
    assert read_dotenv(tmp_path / ".env")["OPINION_API_KEY"] == "created-key"
    assert any("Created the API key" in line for line in said)
