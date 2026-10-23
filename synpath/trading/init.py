"""`synpath init`: ask for each venue's keys and write them to a `.env` file.

For someone who has never made a `.env` file: it asks venue by venue, checks
each value as it is typed (a Kalshi key file that exists and is PEM, a
Polymarket key and wallet address of the right shape), types secrets without
echoing them, and writes the file readable by its owner only. An existing
`.env` is updated in place: the settings asked about are replaced, every other
line is kept. Nothing leaves the machine; at the end it runs `synpath doctor`
on the file it wrote.
"""
from __future__ import annotations

import argparse
import getpass
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .credentials import read_dotenv

PRIVATE_KEY = re.compile(r"^(0x)?[0-9a-fA-F]{64}$")
ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")


@dataclass
class Prompter:
    """How the command talks to a person; tests pass scripted answers."""

    ask: Callable[[str], str] = input
    ask_secret: Callable[[str], str] = getpass.getpass
    say: Callable[[str], None] = print

    def confirm(self, question: str, *, default: bool) -> bool:
        hint = "[Y/n]" if default else "[y/N]"
        while True:
            answer = self.ask(f"{question} {hint} ").strip().lower()
            if not answer:
                return default
            if answer in ("y", "yes"):
                return True
            if answer in ("n", "no"):
                return False
            self.say("  Please answer y or n.")

    def value(self, question: str, *, check: Callable[[str], str | None], secret: bool = False,
              default: str | None = None) -> str:
        """Ask until `check` returns no complaint. `check` gets the stripped
        answer and returns an error message, or None when it is fine."""
        suffix = f" [{default}]" if default else ""
        while True:
            raw = (self.ask_secret if secret else self.ask)(f"  {question}{suffix}: ").strip()
            if not raw and default is not None:
                raw = default
            problem = check(raw)
            if problem is None:
                return raw
            self.say(f"  {problem}")


def _required(what: str) -> Callable[[str], str | None]:
    return lambda raw: None if raw else f"{what} is required."


def _pem_file(raw: str) -> str | None:
    if not raw:
        return "The path to the private key file is required."
    path = Path(raw).expanduser()
    if not path.is_file():
        return f"No file at {path}. Give the full path to the key file the venue gave you."
    try:
        head = path.read_bytes()[:4096]
    except OSError as exc:
        return f"{path} cannot be read: {exc}"
    if b"-----BEGIN" not in head:
        return f"{path} is not a PEM key (no '-----BEGIN' line). Keep only the BEGIN...END block."
    return None


def _private_key(raw: str) -> str | None:
    if PRIVATE_KEY.match(raw):
        return None
    if ADDRESS.match(raw):
        # The most common mistake: the address is public and shown everywhere,
        # the key that controls it is not.
        return ("That is a wallet address (0x and 40 hex characters), not its private key. The private key is "
                "64 hex characters, optionally starting with 0x, and is exported from the wallet.")
    return "That is not a private key: 64 hex characters, optionally starting with 0x."


def _not_an_address(raw: str) -> str:
    if PRIVATE_KEY.match(raw):
        return "That looks like a private key. Enter the wallet's public address instead: 0x and 40 hex characters."
    return "That is not a wallet address: 0x and 40 hex characters."


def _optional_private_key(p: Prompter, *, purpose: str = "trading") -> str:
    """A private key, or empty: what the venue's API key does works without one."""
    p.say(f"  The private key is needed only for {purpose}: leave it empty to go without.")
    p.say("  It is not your wallet address (0x and 40 hex characters).")
    return p.value(f"Private key (64 hex characters, typed hidden; empty to skip {purpose})", secret=True,
                   default="", check=lambda raw: None if not raw else _private_key(raw))


def _with_0x(key: str) -> str:
    return key if key.startswith("0x") else "0x" + key


def _choice(*options: str) -> Callable[[str], str | None]:
    return lambda raw: None if raw in options else f"Enter one of: {', '.join(options)}."


def ask_kalshi(p: Prompter) -> dict[str, str]:
    p.say("\nKalshi: market data over REST is public; its WebSocket (books) and trading both need an API key.")
    p.say("Create one at kalshi.com/account/profile -> API Keys -> Create New API Key (demo.kalshi.co for")
    p.say("demo): a Key ID and a private key file (PEM, Ed25519 or RSA), shown once.")
    key_id = p.value("Key ID", check=_required("The Key ID"))
    path = p.value("Path to the private key file (.pem) it downloaded", check=_pem_file)
    env = p.value("Environment, prod (real money) or demo", check=_choice("prod", "demo"), default="prod")
    if env == "prod":
        p.say("  prod trades real money.")
    return {"KALSHI_KEY_ID": key_id, "KALSHI_PRIVATE_KEY_PATH": str(Path(path).expanduser().resolve()),
            "KALSHI_ENV": env}


def ask_polymarket(p: Prompter) -> dict[str, str]:
    p.say("\nPolymarket: market data is public and needs nothing; this is for trading. Export your private key")
    p.say("(64 hex characters) on polymarket.com under Settings -> Private Key -> Start Export, and copy your")
    p.say("trading wallet address (0x and 40 hex: the funder, not the key) from the profile menu.")
    key = p.value("Private key (64 hex characters, typed hidden)", secret=True,
                  check=_private_key)
    funder = p.value("Trading wallet address (0x...)",
                     check=lambda raw: None if ADDRESS.match(raw) else "That is not a wallet address: 0x and 40 hex "
                                                                       "characters.")
    signature = p.value("Signature type: 3 for accounts made since May 2026, 1 or 2 for older ones",
                        check=_choice("0", "1", "2", "3"), default="3")
    return {"POLYMARKET_PRIVATE_KEY": _with_0x(key),
            "POLYMARKET_FUNDER": funder, "POLYMARKET_SIGNATURE_TYPE": signature}


def ask_polymarket_us(p: Prompter) -> dict[str, str]:
    p.say("\nPolymarket US: REST market data is public; its WebSocket (books) and trading need an API key, made")
    p.say("at polymarket.us/developer after identity verification: a Key ID and a base64 Secret Key, shown once.")
    p.say("No wallet or private key is involved.")
    key_id = p.value("Key ID", check=_required("The key ID"))
    secret = p.value("Secret key (base64, typed hidden)", secret=True, check=_required("The secret key"))
    return {"POLYMARKET_US_KEY_ID": key_id, "POLYMARKET_US_SECRET_KEY": secret}


def ask_opinion(p: Prompter) -> dict[str, str]:
    p.say("\nOpinion: market data needs only the account's Open API key. Trading also needs the private key of")
    p.say("the wallet you connected on opinion.trade (after enabling trading there). A key you already have is")
    p.say("enough for market data; without one, leave it empty and the private key creates it by signing.")
    api_key = p.value("API key (typed hidden; empty to create one with your private key)", secret=True,
                      default="", check=lambda raw: None)
    if api_key:
        key = _optional_private_key(p)
    else:
        key = p.value("Private key (64 hex characters, typed hidden)", secret=True, check=_private_key)
    values = {"OPINION_API_KEY": api_key}
    if key:
        values["OPINION_PRIVATE_KEY"] = _with_0x(key)
    if not api_key:
        from .opinion import create_api_key

        values["OPINION_API_KEY"] = create_api_key(values["OPINION_PRIVATE_KEY"])
        p.say("  Created the API key.")
    return values


def ask_hyperliquid(p: Prompter) -> dict[str, str]:
    p.say("\nHyperliquid: market data is public and needs nothing; this is for trading. An API wallet is safest:")
    p.say("create one at app.hyperliquid.xyz/API (More -> API); it signs orders but cannot withdraw. Its private")
    p.say("key is 64 hex characters; the account it trades for is a 0x address of 40.")
    key = p.value("Private key (64 hex characters, typed hidden)", secret=True,
                  check=_private_key)
    account = p.value("Account address the API wallet trades for (empty if the key is the account's own)",
                      default="", check=lambda raw: None if not raw or ADDRESS.match(raw) else
                      "That is not a wallet address: 0x and 40 hex characters.")
    testnet = p.value("Use the test network? (y/n)", check=_choice("y", "n"), default="n")
    values = {"HYPERLIQUID_PRIVATE_KEY": _with_0x(key)}
    if account:
        values["HYPERLIQUID_ACCOUNT_ADDRESS"] = account
    if testnet == "y":
        values["HYPERLIQUID_TESTNET"] = "1"
    return values


def ask_predict_fun(p: Prompter) -> dict[str, str]:
    p.say("\npredict.fun: market data on mainnet needs only an API key (pred_sk_...), created at")
    p.say("developers.predict.fun. Trading also needs a private key; the test network needs no API key.")
    testnet = p.value("Use the test network? (y/n)", check=_choice("y", "n"), default="n")
    values: dict[str, str] = {}
    if testnet == "y":
        values["PREDICT_FUN_TESTNET"] = "1"
        key = p.value("Private key (64 hex characters, typed hidden)", secret=True, check=_private_key)
    else:
        values["PREDICT_FUN_API_KEY"] = p.value("API key (pred_sk_..., typed hidden)", secret=True,
                                                check=_required("The API key"))
        p.say("  A Predict account made by the web app exports its owner key under Account -> Settings ->")
        p.say("  Privy wallet; a plain wallet uses its own key.")
        key = _optional_private_key(p)
    if key:
        values["PREDICT_FUN_PRIVATE_KEY"] = _with_0x(key)
        account = p.value("Predict account (deposit) address, 0x and 40 hex (empty for a plain wallet)",
                          default="", check=lambda raw: None if not raw or ADDRESS.match(raw) else
                          "That is not a wallet address: 0x and 40 hex characters.")
        if account:
            values["PREDICT_FUN_ACCOUNT_ADDRESS"] = account
    return values


def ask_limitless(p: Prompter) -> dict[str, str]:
    p.say("\nLimitless: market data is public and needs nothing; this is for trading. It needs the private key")
    p.say("(64 hex characters) of the wallet connected on limitless.exchange, in EOA trading mode, and a scoped")
    p.say("API token: profile menu -> Api keys -> API Tokens -> Derive, giving a token id and a base64 secret")
    p.say("shown once.")
    key = p.value("Private key (64 hex characters, typed hidden)", secret=True,
                  check=_private_key)
    token_id = p.value("API token id", check=_required("The token id"))
    secret = p.value("API token secret (typed hidden)", secret=True, check=_required("The token secret"))
    return {"LIMITLESS_PRIVATE_KEY": _with_0x(key),
            "LIMITLESS_API_TOKEN_ID": token_id, "LIMITLESS_API_SECRET": secret}


def ask_sx_bet(p: Prompter) -> dict[str, str]:
    p.say("\nSX Bet: the API key alone serves market data, reads your account (orders, fills, positions, balance)")
    p.say("and cancels orders. Get it at sx.bet -> Account -> Overview -> API Credentials -> Generate API key")
    p.say("(toronto.sx.bet for the test network); it is shown once. The private key is needed only to place")
    p.say("orders, which SX Bet has the wallet sign: your account's own key, not its proxy wallet's.")
    api_key = p.value("API key (typed hidden)", secret=True, check=_required("The API key"))
    key = _optional_private_key(p, purpose="placing orders")
    values = {"SX_BET_API_KEY": api_key}
    if key:
        values["SX_BET_PRIVATE_KEY"] = _with_0x(key)
    else:
        p.say("  Without the private key the account stream needs your wallet address to name its channels.")
        account = p.value("Wallet address, 0x and 40 hex (public, not the private key; empty to poll instead)",
                          default="", check=lambda raw: None if not raw or ADDRESS.match(raw) else
                          _not_an_address(raw))
        if account:
            values["SX_BET_ACCOUNT_ADDRESS"] = account
    testnet = p.value("Use the test network? (y/n)", check=_choice("y", "n"), default="n")
    if testnet == "y":
        values["SX_BET_TESTNET"] = "1"
    return values


def ask_betfair(p: Prompter) -> dict[str, str]:
    p.say("\nBetfair: your application key (the delayed key is free: developer.betfair.com, or the Accounts API")
    p.say("visualiser) and your Betfair login. Every Betfair call, market data included, logs in. The key and the")
    p.say("password are typed hidden. A client certificate (optional) switches to Betfair's certificate login.")
    app_key = p.value("Application key (typed hidden)", secret=True, check=_required("The application key"))
    username = p.value("Betfair username", check=_required("The username"))
    password = p.value("Betfair password (typed hidden)", secret=True, check=_required("The password"))
    cert = p.value("Client certificate file (.crt, empty for interactive login)", default="", check=lambda raw: None)
    key = p.value("Its private key file (.key)", default="", check=lambda raw: None) if cert else ""
    jurisdiction = p.value("Betfair site the account is on (com, it, es, ro, com.au)", default="com",
                           check=_choice("com", "it", "es", "ro", "com.au"))
    values = {"BETFAIR_APP_KEY": app_key, "BETFAIR_USERNAME": username, "BETFAIR_PASSWORD": password}
    if cert and key:
        values["BETFAIR_CERT_PATH"], values["BETFAIR_KEY_PATH"] = cert, key
    if jurisdiction != "com":
        values["BETFAIR_JURISDICTION"] = jurisdiction
    return values


def ask_smarkets(p: Prompter) -> dict[str, str]:
    p.say("\nSmarkets: market data needs no login at all. A login (optional) sends a session token for")
    p.say("undelayed quotes and your account's own rate limits. Betting needs Smarkets' approved API access")
    p.say("(a one-off £150 fee), which this library does not support yet. The password is typed hidden.")
    username = p.value("Smarkets username (your email)", check=_required("The username"))
    password = p.value("Smarkets password (typed hidden)", secret=True, check=_required("The password"))
    return {"SMARKETS_USERNAME": username, "SMARKETS_PASSWORD": password}


def ask_novig(p: Prompter) -> dict[str, str]:
    p.say("\nNovig: market data is public and needs nothing; this is for trading. Trading uses a subaccount's")
    p.say("trading key: make the management key in the web app (Profile -> Settings -> Novig API; paper.novig.com")
    p.say("for Paper), then open a subaccount with it, which issues the trading key. Each key is a key id and a")
    p.say("PEM private key file (Ed25519 or P-256). Production needs US identity and location checks.")
    env = p.value("Environment, production (real money) or paper", check=_choice("production", "paper"),
                  default="production")
    values = {"NOVIG_ENV": env}
    key_id = p.value("Trading key id (empty if you have only the management key so far)", default="",
                     check=lambda raw: None)
    if key_id:
        values["NOVIG_KEY_ID"] = key_id
        values["NOVIG_PRIVATE_KEY_PATH"] = str(Path(p.value("Path to its private key file (.pem)",
                                                            check=_pem_file)).expanduser().resolve())
    management = p.value("Management key id (optional: opens and funds subaccounts; empty to skip)", default="",
                         check=lambda raw: None if raw or key_id else "Give a trading key or the management key.")
    if management:
        values["NOVIG_MANAGEMENT_KEY_ID"] = management
        values["NOVIG_MANAGEMENT_PRIVATE_KEY_PATH"] = str(Path(p.value(
            "Path to the management key file (novig-api-key-<name>.pem)", check=_pem_file)).expanduser().resolve())
    if env == "production":
        p.say("  production trades real money.")
    return values


def ask_matchbook(p: Prompter) -> dict[str, str]:
    p.say("\nMatchbook: your Matchbook login, for betting. Market data needs no login. An account with")
    p.say("two-factor login cannot log in from here. The password is typed hidden.")
    username = p.value("Matchbook username", check=_required("The username"))
    password = p.value("Matchbook password (typed hidden)", secret=True, check=_required("The password"))
    return {"MATCHBOOK_USERNAME": username, "MATCHBOOK_PASSWORD": password}


def ask_prophetx(p: Prompter) -> dict[str, str]:
    p.say("\nProphetX: the Trading API key pair reads market data, streams it and places orders. Any KYC-approved")
    p.say("account makes one at prophetx.co -> Menu -> API Integration (sandbox.prophetx.dev for the sandbox);")
    p.say("the secret key is shown once. A Market Data API key (optional) is issued by ProphetX's market data")
    p.say("team on request and reads market data only. Every key is typed hidden.")
    access = p.value("Access key (typed hidden)", secret=True, check=_required("The access key"))
    secret = p.value("Secret key (typed hidden)", secret=True, check=_required("The secret key"))
    affiliate = p.value("Market Data API key (typed hidden, empty for none)", secret=True, default="",
                        check=lambda raw: None)
    values = {"PROPHETX_ACCESS_KEY": access, "PROPHETX_SECRET_KEY": secret}
    if affiliate:
        values["PROPHETX_AFFILIATE_KEY"] = affiliate
    if p.value("Use the sandbox? (y/n)", check=_choice("y", "n"), default="n") == "y":
        values["PROPHETX_SANDBOX"] = "1"
    return values


def ask_azuro(p: Prompter) -> dict[str, str]:
    p.say("\nAzuro: market data is public and needs nothing; this is for betting. It needs the private key")
    p.say("(64 hex characters) of the wallet that signs each bet and holds the bet token: USDT on Polygon, WETH")
    p.say("on Base. The wallet also approves Azuro's relayer to spend the token once, on chain (approve()).")
    key = p.value("Private key (64 hex characters, typed hidden)", secret=True, check=_private_key)
    chain = p.value("Chain (polygon, base, polygon_amoy, base_sepolia)", default="polygon",
                    check=_choice("polygon", "base", "polygon_amoy", "base_sepolia"))
    values = {"AZURO_PRIVATE_KEY": _with_0x(key)}
    if chain != "polygon":
        values["AZURO_CHAIN"] = chain
    return values


VENUES: list[tuple[str, tuple[str, ...], Callable[[Prompter], dict[str, str]]]] = [
    ("Kalshi", ("KALSHI_KEY_ID",), ask_kalshi),
    ("Polymarket", ("POLYMARKET_PRIVATE_KEY",), ask_polymarket),
    ("Polymarket US", ("POLYMARKET_US_KEY_ID",), ask_polymarket_us),
    ("Opinion", ("OPINION_API_KEY", "OPINION_PRIVATE_KEY"), ask_opinion),
    ("Hyperliquid", ("HYPERLIQUID_PRIVATE_KEY",), ask_hyperliquid),
    ("predict.fun", ("PREDICT_FUN_API_KEY", "PREDICT_FUN_PRIVATE_KEY"), ask_predict_fun),
    ("Limitless", ("LIMITLESS_PRIVATE_KEY",), ask_limitless),
    ("SX Bet", ("SX_BET_API_KEY", "SX_BET_PRIVATE_KEY"), ask_sx_bet),
    ("Betfair", ("BETFAIR_APP_KEY",), ask_betfair),
    ("Smarkets (optional login; market data needs none)", ("SMARKETS_USERNAME",), ask_smarkets),
    ("Novig", ("NOVIG_KEY_ID", "NOVIG_MANAGEMENT_KEY_ID"), ask_novig),
    ("Matchbook", ("MATCHBOOK_USERNAME",), ask_matchbook),
    ("ProphetX", ("PROPHETX_ACCESS_KEY", "PROPHETX_AFFILIATE_KEY"), ask_prophetx),
    ("Azuro", ("AZURO_PRIVATE_KEY",), ask_azuro),
]
"""Each venue, the settings any of which means it is set up already (an API
key alone is, for market data), and the questions."""


def _quote(value: str) -> str:
    return f'"{value}"' if any(ch.isspace() for ch in value) or "#" in value else value


def merge(existing_text: str, values: dict[str, str]) -> str:
    """The file with `values` set: a line already naming a key is rewritten in
    place, new keys are appended, every other line is kept as it was."""
    lines = existing_text.splitlines()
    done: set[str] = set()
    out: list[str] = []
    for line in lines:
        name = line.split("=", 1)[0].strip().removeprefix("export ").strip() if "=" in line else ""
        if name in values and not line.lstrip().startswith("#"):
            if name not in done:
                out.append(f"{name}={_quote(values[name])}")
                done.add(name)
            continue
        out.append(line)
    new = [name for name in values if name not in done]
    if new:
        if out and out[-1].strip():
            out.append("")
        out.append("# written by synpath init")
        out.extend(f"{name}={_quote(values[name])}" for name in new)
    return "\n".join(out) + "\n"


def write_private(path: Path, text: str) -> None:
    """Write the file readable and writable by its owner only (0600), also
    when it already existed with looser permissions."""
    if path.exists():
        os.chmod(path, 0o600)       # tighten before the secrets go in, not after
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(text)
    os.chmod(path, 0o600)


def _git_ignores(path: Path) -> bool | None:
    """True or False inside a git work tree; None when there is no git or no repository."""
    if shutil.which("git") is None:
        return None
    folder = path.parent
    inside = subprocess.run(["git", "rev-parse", "--is-inside-work-tree"], cwd=folder,
                            capture_output=True, text=True)
    if inside.returncode != 0:
        return None
    ignored = subprocess.run(["git", "check-ignore", "-q", path.name], cwd=folder)
    return ignored.returncode == 0


def run(path: Path, p: Prompter) -> int:
    existing = read_dotenv(path) if path.exists() else {}
    p.say(f"This sets up your venue keys in {path}. They stay on this machine; secrets are typed hidden.")
    if path.exists():
        p.say("That file exists: the settings you enter replace the ones there, everything else is kept.")
    values: dict[str, str] = {}
    for name, markers, ask in VENUES:
        already = " (already set)" if any(existing.get(marker) for marker in markers) else ""
        if p.confirm(f"\nSet up {name}{already}?", default=False):
            values.update(ask(p))
    if not values:
        p.say("\nNothing changed.")
        return 0
    text = merge(path.read_text(encoding="utf-8") if path.exists() else "", values)
    write_private(path, text)
    p.say(f"\nWrote {path}, readable by you only.")
    try:
        add = _git_ignores(path) is False and p.confirm(f"{path.name} is not ignored by git. Add it to .gitignore?",
                                                        default=True)
    except (KeyboardInterrupt, EOFError):
        p.say("")
        add = False
    if add:
        ignore = path.parent / ".gitignore"
        current = ignore.read_text(encoding="utf-8") if ignore.exists() else ""
        ignore.write_text(current + ("" if not current or current.endswith("\n") else "\n") + f"{path.name}\n",
                          encoding="utf-8")
        p.say(f"Added {path.name} to {ignore}.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="synpath init", description=__doc__.split("\n\n")[0])
    parser.add_argument("--path", default=".env", help="the file to write (default: .env in this folder)")
    args = parser.parse_args(argv)
    if not sys.stdin.isatty():
        print("synpath init asks questions: run it in a terminal.", file=sys.stderr)
        return 2
    path = Path(args.path).expanduser().resolve()
    try:
        result = run(path, Prompter())
    except (KeyboardInterrupt, EOFError):
        print("\nStopped; nothing was written.")
        return 130
    if result == 0 and path.exists():
        from .__main__ import doctor

        print("\nChecking what loads:\n")
        doctor(str(path))
        print("\nNext: synpath serve")
    return result


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
