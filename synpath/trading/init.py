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
        return f"No file at {path}. Give the full path to the key file Kalshi downloaded."
    try:
        head = path.read_bytes()[:4096]
    except OSError as exc:
        return f"{path} cannot be read: {exc}"
    if b"-----BEGIN" not in head:
        return f"{path} is not a PEM key (no '-----BEGIN' line). Keep only the BEGIN...END block."
    return None


def _choice(*options: str) -> Callable[[str], str | None]:
    return lambda raw: None if raw in options else f"Enter one of: {', '.join(options)}."


def ask_kalshi(p: Prompter) -> dict[str, str]:
    p.say("\nKalshi: Profile Settings -> API Keys -> Create New API Key on kalshi.com (or demo.kalshi.co).")
    key_id = p.value("Key ID", check=_required("The Key ID"))
    path = p.value("Path to the private key file it downloaded", check=_pem_file)
    env = p.value("Environment, prod (real money) or demo", check=_choice("prod", "demo"), default="prod")
    if env == "prod":
        p.say("  prod trades real money.")
    return {"KALSHI_KEY_ID": key_id, "KALSHI_PRIVATE_KEY_PATH": str(Path(path).expanduser().resolve()),
            "KALSHI_ENV": env}


def ask_polymarket(p: Prompter) -> dict[str, str]:
    p.say("\nPolymarket: export your key on polymarket.com under Settings -> Private Key -> Start Export,")
    p.say("and copy your trading wallet address from the profile menu. The key is typed hidden.")
    key = p.value("Private key (0x...)", secret=True,
                  check=lambda raw: None if PRIVATE_KEY.match(raw) else "That is not a private key: 64 hex characters, "
                                                                        "optionally starting with 0x.")
    funder = p.value("Trading wallet address (0x...)",
                     check=lambda raw: None if ADDRESS.match(raw) else "That is not a wallet address: 0x and 40 hex "
                                                                       "characters.")
    signature = p.value("Signature type: 3 for accounts made since May 2026, 1 or 2 for older ones",
                        check=_choice("0", "1", "2", "3"), default="3")
    return {"POLYMARKET_PRIVATE_KEY": key if key.startswith("0x") else "0x" + key,
            "POLYMARKET_FUNDER": funder, "POLYMARKET_SIGNATURE_TYPE": signature}


def ask_polymarket_us(p: Prompter) -> dict[str, str]:
    p.say("\nPolymarket US: create an API key at polymarket.us/developer after identity verification.")
    key_id = p.value("Key ID", check=_required("The key ID"))
    secret = p.value("Secret key (typed hidden)", secret=True, check=_required("The secret key"))
    return {"POLYMARKET_US_KEY_ID": key_id, "POLYMARKET_US_SECRET_KEY": secret}


def ask_opinion(p: Prompter) -> dict[str, str]:
    p.say("\nOpinion: the private key of the wallet you connected on opinion.trade, after enabling trading there.")
    p.say("The key is typed hidden. Leave the API key empty to create one by signing with this wallet.")
    key = p.value("Private key (0x...)", secret=True,
                  check=lambda raw: None if PRIVATE_KEY.match(raw) else "That is not a private key: 64 hex characters, "
                                                                        "optionally starting with 0x.")
    key = key if key.startswith("0x") else "0x" + key
    api_key = p.value("API key (empty to create one)", secret=True, default="", check=lambda raw: None)
    if not api_key:
        from .opinion import create_api_key

        api_key = create_api_key(key)
        p.say("  Created the API key.")
    return {"OPINION_PRIVATE_KEY": key, "OPINION_API_KEY": api_key}


VENUES: list[tuple[str, str, Callable[[Prompter], dict[str, str]]]] = [
    ("Kalshi", "KALSHI_KEY_ID", ask_kalshi),
    ("Polymarket", "POLYMARKET_PRIVATE_KEY", ask_polymarket),
    ("Polymarket US", "POLYMARKET_US_KEY_ID", ask_polymarket_us),
    ("Opinion", "OPINION_PRIVATE_KEY", ask_opinion),
]


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
    for name, marker, ask in VENUES:
        already = " (already set)" if existing.get(marker) else ""
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
