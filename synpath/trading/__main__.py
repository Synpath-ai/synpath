"""`synpath doctor`: what credentials loaded, without printing one.

Reads the environment and a `.env` in the working directory, loads each
venue's credentials the way the adapters will, and reports per venue whether
anything is configured, which environment (demo, preprod, prod) it points
at, and the public half of the identity (a key id, an API key prefix, a
funder address). Secrets are never printed; the logging filter that scrubs
them is installed first.

The venue round trip -- calling one harmless authenticated endpoint per
venue -- arrives with each trading adapter.
"""
from __future__ import annotations

import argparse
import logging
import sys
from typing import Any

from .credentials import ENV_NAMES, load_credentials, load_market_data
from .errors import CredentialsMissing


def sx_bet_identity(creds: Any) -> str:
    """What SX Bet's credentials allow: the API key reads market data and the
    account and cancels; only placing orders needs the private key."""
    wallet = f"wallet {creds.address}" if creds.address else "wallet address unknown (set SX_BET_ACCOUNT_ADDRESS for the account stream)"
    placing = "private key loaded: places orders" if creds.private_key else "no private key: set SX_BET_PRIVATE_KEY to place orders"
    return f"{wallet}; API key: market data, account, cancels; {placing}"


def novig_identity(creds: Any) -> str:
    """Which Novig keys loaded: the subaccount's trading key trades; the
    management key opens and funds subaccounts."""
    trading = f"trading key {creds.key_id}" if creds.key_id else "no trading key: set NOVIG_KEY_ID to trade"
    management = f", management key {creds.management_key_id}" if creds.management_key_id else ""
    return trading + management


def myriad_identity(creds: Any) -> str:
    """What Myriad's credentials allow: the private key signs orders and
    cancels (and signs in without an API key); the API key and secret sign
    requests, and alone read the account."""
    wallet = f"wallet {creds.address}" if creds.address else "wallet address unknown (set MYRIAD_ACCOUNT_ADDRESS)"
    requests = (f"API key {creds.api_key[:8]}…: HMAC-signed requests" if creds.api_key
                else "no API key: the wallet signs in for a session")
    orders = ("private key loaded: places and cancels orders" if creds.private_key
              else "no private key: reads the account only; set MYRIAD_PRIVATE_KEY to place or cancel")
    return f"{wallet}; {requests}; {orders}"


def doctor(dotenv: str | None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        loaded = load_credentials(dotenv=dotenv)
        market_data = load_market_data(dotenv=dotenv)
    except CredentialsMissing as exc:
        print(f"credentials: {exc}")
        return 2
    rows = []
    for venue, creds in loaded.items():
        if creds is None and venue == "smarkets":
            # Optional: Smarkets serves its market data without a login.
            rows.append((venue, "not configured", "market data needs no login; set SMARKETS_USERNAME / "
                         "SMARKETS_PASSWORD to bet and for undelayed quotes, or run synpath init. Placing orders "
                         "needs Smarkets' approved API access"))
            continue
        if creds is None and venue == "matchbook":
            # Market data is public; the login is for betting.
            rows.append((venue, "not configured", "market data needs no login; set MATCHBOOK_USERNAME / "
                         "MATCHBOOK_PASSWORD to bet, or run synpath init"))
            continue
        if creds is not None and venue == "prophetx" and not creds.trades:
            # The affiliate key alone: ProphetX's Market Data API.
            stage = "sandbox" if creds.sandbox else "production"
            rows.append((venue, f"market data ({stage})", "affiliate key loaded; set PROPHETX_ACCESS_KEY / "
                         "PROPHETX_SECRET_KEY to trade and stream"))
            continue
        if creds is None and venue == "gemini":
            rows.append((venue, "no key (optional)", "market data needs no key; set GEMINI_API_KEY / "
                         "GEMINI_API_SECRET to trade (a US account with Prediction Markets access), or run synpath init"))
            continue
        if creds is None and venue in market_data:
            stage = "testnet" if market_data[venue].testnet else "mainnet"
            private = ENV_NAMES[venue][0]
            rows.append((venue, f"market data ({stage})", f"API key loaded; set {private} to trade"))
            continue
        if creds is None:
            names = ENV_NAMES[venue]
            hint = ", or run synpath init" if venue in ("kalshi", "polymarket", "polymarket_us", "opinion", "hyperliquid", "predict_fun", "limitless", "sx_bet", "betfair", "novig", "prophetx", "azuro", "myriad") else ""
            rows.append((venue, "not configured", f"set {names[0]}{hint}"))
            continue
        stage = getattr(creds, "env", "-")
        if venue == "novig":
            identity = novig_identity(creds)
        elif getattr(creds, "key_id", None):
            identity = f"key id {creds.key_id}"
        elif getattr(creds, "participant_id", None):
            identity = f"participant {creds.participant_id}"
        elif getattr(creds, "funder", None):
            identity = f"funder {creds.funder} (signature type {creds.signature_type})"
        elif venue == "opinion":
            # Opinion's API key is itself the secret, so the wallet names the account.
            from .polymarket_signing import WalletSigner

            safe = f", safe {creds.multisig_address}" if creds.multisig_address else ""
            identity = f"wallet {WalletSigner(creds.private_key).address}{safe}"
        elif venue == "hyperliquid":
            from .polymarket_signing import WalletSigner

            signer = WalletSigner(creds.private_key).address.lower()
            stage = "testnet" if creds.testnet else "mainnet"
            identity = f"account {creds.address}" + ("" if creds.address == signer else f", API wallet {signer}")
        elif venue == "predict_fun":
            from .polymarket_signing import WalletSigner

            signer = WalletSigner(creds.private_key).address
            stage = "testnet" if creds.testnet else "mainnet"
            identity = (f"Predict account {creds.account_address}, owner {signer}" if creds.account_address
                        else f"wallet {signer}")
        elif venue == "limitless":
            identity = f"wallet {creds.address}, API token {creds.token_id}"
        elif venue == "sx_bet":
            stage = "testnet" if creds.testnet else "mainnet"
            identity = sx_bet_identity(creds)
        elif venue == "myriad":
            stage = "mainnet"
            identity = myriad_identity(creds)
        elif venue == "betfair":
            # The application key is a secret too; the username names the account.
            stage = creds.jurisdiction
            login = "certificate" if creds.cert_path else "interactive"
            identity = f"user {creds.username}, app key loaded, {login} login"
        elif venue == "prophetx":
            stage = "sandbox" if creds.sandbox else "production"
            affiliate = "; affiliate key loaded: market data from the Market Data API" if creds.affiliate_key else ""
            identity = f"Trading API key pair loaded: market data, streams, orders{affiliate}"
        elif venue == "azuro":
            stage = creds.chain
            affiliate = f", affiliate {creds.affiliate}" if creds.affiliate else ""
            identity = f"wallet {creds.address}{affiliate}"
        elif venue == "gemini":
            sub = f", subaccount {creds.account}" if creds.account else ""
            identity = f"api key {creds.api_key[:12]}…{sub} (account-scoped, time-based nonce for streams)"
        elif venue == "smarkets":
            stage = "login"
            identity = f"user {creds.username}: undelayed quotes and the account; placing orders needs approved API access"
        elif venue == "matchbook":
            stage = "login"
            identity = f"user {creds.username}"
        elif getattr(creds, "api_key", None):
            identity = f"api key {creds.api_key[:8]}…"
        else:
            identity = "wallet key loaded"
        rows.append((venue, f"configured ({stage})", identity))
    width = max(len(r[0]) for r in rows)
    for venue, state, detail in rows:
        print(f"{venue:<{width}}  {state:<22}  {detail}")
    configured = sum(1 for _, state, _ in rows if state.startswith("configured"))
    reading = sum(1 for _, state, _ in rows if state.startswith("market data"))
    also = f", {reading} for market data only" if reading else ""
    print(f"\n{configured} of {len(rows)} venues configured for trading{also}. No secret was printed.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="synpath")
    sub = parser.add_subparsers(dest="command", required=True)
    doc = sub.add_parser("doctor", help="report which venue credentials load")
    doc.add_argument("--dotenv", default=None, help="path to a .env file (default: ./.env if present)")
    args = parser.parse_args(argv)
    if args.command == "doctor":
        return doctor(args.dotenv)
    return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
