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

from .credentials import ENV_NAMES, load_credentials
from .errors import CredentialsMissing


def doctor(dotenv: str | None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        loaded = load_credentials(dotenv=dotenv)
    except CredentialsMissing as exc:
        print(f"credentials: {exc}")
        return 2
    rows = []
    for venue, creds in loaded.items():
        if creds is None:
            names = ENV_NAMES[venue]
            hint = ", or run synpath init" if venue in ("kalshi", "polymarket", "polymarket_us", "opinion", "hyperliquid") else ""
            rows.append((venue, "not configured", f"set {names[0]}{hint}"))
            continue
        stage = getattr(creds, "env", "-")
        if getattr(creds, "key_id", None):
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
        elif getattr(creds, "api_key", None):
            identity = f"api key {creds.api_key[:8]}…"
        else:
            identity = "wallet key loaded"
        rows.append((venue, f"configured ({stage})", identity))
    width = max(len(r[0]) for r in rows)
    for venue, state, detail in rows:
        print(f"{venue:<{width}}  {state:<22}  {detail}")
    configured = sum(1 for _, state, _ in rows if state.startswith("configured"))
    print(f"\n{configured} of {len(rows)} venues configured. No secret was printed.")
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
