"""Venue credentials: where they come from, and where they never go.

They come from the environment, or from a `.env` file next to the caller
that is git-ignored; the environment wins. They are
loaded once into frozen objects whose `repr` shows nothing secret, and every
secret value is registered with a logging filter so that a stray `%r` in a
log line prints `***` rather than a private key.

Nothing here talks to a venue. `synpath doctor` reports what
loaded; the adapters are what use it.

  Kalshi              KALSHI_KEY_ID, KALSHI_PRIVATE_KEY_PATH, KALSHI_ENV (prod, the default, or demo)
  Polymarket          POLYMARKET_PRIVATE_KEY, POLYMARKET_SIGNATURE_TYPE (0-3),
                      POLYMARKET_FUNDER, and optionally POLYMARKET_API_KEY /
                      _API_SECRET / _API_PASSPHRASE, POLYMARKET_BUILDER_CODE,
                      POLYMARKET_RELAYER_API_KEY / _RELAYER_API_KEY_ADDRESS,
                      POLYMARKET_RPC_URL
  Polymarket US       POLYMARKET_US_KEY_ID, POLYMARKET_US_SECRET_KEY
    (retail API)
  Polymarket US       POLYMARKET_US_CLIENT_ID, POLYMARKET_US_PRIVATE_KEY_PATH,
    (exchange API)    POLYMARKET_US_PARTICIPANT_ID, POLYMARKET_US_ACCOUNT,
                      POLYMARKET_US_ENV (preprod|prod)
  Opinion             OPINION_PRIVATE_KEY, OPINION_API_KEY, and optionally
                      OPINION_MULTISIG_ADDRESS
  Hyperliquid         HYPERLIQUID_PRIVATE_KEY, and optionally
                      HYPERLIQUID_ACCOUNT_ADDRESS (when the key is an API
                      wallet) and HYPERLIQUID_TESTNET
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Mapping

from .errors import CredentialsMissing

REDACTED = "***"


def _redacted_repr(self: Any) -> str:
    public = {k: v for k, v in self.__dict__.items() if k in self._public}
    return f"{type(self).__name__}({', '.join(f'{k}={v!r}' for k, v in public.items())}, secrets={REDACTED})"


@dataclass(frozen=True, repr=False)
class KalshiCredentials:
    key_id: str
    private_key_pem: bytes = field(repr=False)
    """RSA private key, PEM. Signs `timestamp + method + path` per request."""
    env: Literal["demo", "prod"] = "prod"
    """`prod` trades on Kalshi itself; `demo` on its practice exchange."""
    _public = ("key_id", "env")
    __repr__ = _redacted_repr

    @property
    def secrets(self) -> list[str]:
        return [self.private_key_pem.decode(errors="ignore")]


SYNPATH_BUILDER_CODE = "0x3373b88f438d41079965d0742b16c602aa932cbc3b7ee589db36cbf85778c869"
"""Synpath's Polymarket builder code, attached to orders unless the account
sets its own or opts out (`POLYMARKET_BUILDER_CODE=none`). Its builder fee
rate is 0: it adds nothing to what an order costs."""

BUILDER_CODE = re.compile(r"^0x[0-9a-fA-F]{64}$")


@dataclass(frozen=True, repr=False)
class PolymarketCredentials:
    private_key: str = field(repr=False)
    """The signer's private key, hex. Signs every order (EIP-712) and, when
    no API credentials are given, derives them."""
    signature_type: int = 0
    """Which wallet the orders spend from: 0 an EOA (allowlisted accounts
    only), 1 a legacy Magic/Google proxy wallet, 2 a legacy Safe, 3 a
    Deposit Wallet -- the default for every account created since
    2026-05-04."""
    funder: str | None = None
    """The account wallet that holds the pUSD and tokens. Required for types
    1-3; for an EOA it is the signer's own address."""
    api_key: str | None = None
    api_secret: str | None = field(default=None, repr=False)
    api_passphrase: str | None = field(default=None, repr=False)
    """CLOB API credentials, when already created. Derived from the wallet
    key on first use otherwise."""
    builder_code: str | None = SYNPATH_BUILDER_CODE
    """A bytes32 builder code attached to every order for attribution. Public,
    not a secret. Synpath's by default; None attaches none."""
    relayer_api_key: str | None = field(default=None, repr=False)
    relayer_api_key_address: str | None = None
    """A Relayer API key (polymarket.com -> Settings -> API Keys) for gasless
    wallet transactions: approvals, split, merge, redeem."""
    rpc_url: str | None = None
    """A Polygon JSON-RPC endpoint, for an EOA's own on-chain transactions."""
    _public = ("signature_type", "funder", "api_key", "builder_code", "relayer_api_key_address")
    __repr__ = _redacted_repr

    @property
    def secrets(self) -> list[str]:
        return [v for v in (self.private_key, self.api_secret, self.api_passphrase, self.relayer_api_key) if v]


@dataclass(frozen=True, repr=False)
class PolymarketUSCredentials:
    """The retail API at `api.polymarket.us`: a key from polymarket.us/developer
    after identity verification in the app."""

    key_id: str
    secret_key: str = field(repr=False)
    """Base64 Ed25519 private key, shown once at creation. Signs
    `timestamp + METHOD + path` on every request."""
    _public = ("key_id",)
    __repr__ = _redacted_repr

    @property
    def secrets(self) -> list[str]:
        return [self.secret_key]


@dataclass(frozen=True, repr=False)
class PolymarketUSExchangeCredentials:
    """The exchange API at `api.{preprod,prod}.polymarketexchange.com`: a
    firm onboarded by Polymarket US, with a client id, an RSA key pair and a
    participant id per user."""

    client_id: str
    private_key_pem: bytes = field(repr=False)
    """RSA private key, PEM. Signs the client-assertion JWT exchanged for a
    three-minute access token."""
    participant_id: str
    """`firms/<firm>/users/<user>`, exactly as issued at onboarding."""
    account: str | None = None
    """The trading account orders are placed on. The first account the user
    may trade when not given."""
    env: Literal["preprod", "prod"] = "preprod"
    _public = ("client_id", "participant_id", "account", "env")
    __repr__ = _redacted_repr

    @property
    def secrets(self) -> list[str]:
        return [self.private_key_pem.decode(errors="ignore")]


@dataclass(frozen=True, repr=False)
class OpinionCredentials:
    """Opinion: the wallet that signs orders, and the account's API key."""

    private_key: str = field(repr=False)
    """The signer's private key, hex: the wallet connected on opinion.trade.
    Signs every order (EIP-712) for the account's Safe."""
    api_key: str = field(repr=False)
    """The Open API key, from `POST /auth/api-key` signed by the same wallet,
    or the venue's application form."""
    multisig_address: str | None = None
    """The account's Safe, which holds the USDT and tokens ("MyProfile" on
    opinion.trade). Read from the venue on first use when not given."""
    _public = ("multisig_address",)
    __repr__ = _redacted_repr

    @property
    def secrets(self) -> list[str]:
        return [self.private_key, self.api_key]


@dataclass(frozen=True, repr=False)
class HyperliquidCredentials:
    """Hyperliquid: the key that signs, and the account it signs for."""

    private_key: str = field(repr=False)
    """The signer's private key, hex: the account's own wallet, or an API
    wallet the account approved on app.hyperliquid.xyz (More -> API), which
    can trade but never withdraw."""
    account_address: str | None = None
    """The account the orders are for. Needed when `private_key` is an API
    wallet; with the account's own key it is that key's address."""
    testnet: bool = False
    """Sign for and talk to the test network, where outcomes trade for test funds."""
    _public = ("account_address", "testnet")
    __repr__ = _redacted_repr

    @property
    def address(self) -> str:
        """The account's address, lowercase: given, or the key's own."""
        if self.account_address:
            return self.account_address.lower()
        from .polymarket_signing import WalletSigner

        return WalletSigner(self.private_key).address.lower()

    @property
    def secrets(self) -> list[str]:
        return [self.private_key]


Credentials = (
    KalshiCredentials | PolymarketCredentials | PolymarketUSCredentials | PolymarketUSExchangeCredentials
    | OpinionCredentials | HyperliquidCredentials
)

ENV_NAMES: dict[str, tuple[str, ...]] = {
    "kalshi": ("KALSHI_KEY_ID", "KALSHI_PRIVATE_KEY_PATH", "KALSHI_ENV"),
    "polymarket": (
        "POLYMARKET_PRIVATE_KEY", "POLYMARKET_SIGNATURE_TYPE", "POLYMARKET_FUNDER",
        "POLYMARKET_API_KEY", "POLYMARKET_API_SECRET", "POLYMARKET_API_PASSPHRASE",
        "POLYMARKET_BUILDER_CODE", "POLYMARKET_RELAYER_API_KEY", "POLYMARKET_RELAYER_API_KEY_ADDRESS",
        "POLYMARKET_RPC_URL",
    ),
    "polymarket_us": ("POLYMARKET_US_KEY_ID", "POLYMARKET_US_SECRET_KEY"),
    "polymarket_us_exchange": (
        "POLYMARKET_US_CLIENT_ID", "POLYMARKET_US_PRIVATE_KEY_PATH", "POLYMARKET_US_PARTICIPANT_ID",
        "POLYMARKET_US_ACCOUNT", "POLYMARKET_US_ENV",
    ),
    "opinion": ("OPINION_PRIVATE_KEY", "OPINION_API_KEY", "OPINION_MULTISIG_ADDRESS"),
    "hyperliquid": ("HYPERLIQUID_PRIVATE_KEY", "HYPERLIQUID_ACCOUNT_ADDRESS", "HYPERLIQUID_TESTNET"),
}
"""What each venue reads. The first entry is the one whose absence means
"not configured"; the rest are optional or have defaults."""


def read_dotenv(path: Path | str) -> dict[str, str]:
    """A `.env` file as a dict. `KEY=value`, `#` comments, optional quotes.

    Deliberately small: it exists so a user can keep credentials in a
    git-ignored file without another dependency, not to be a shell.
    """
    values: dict[str, str] = {}
    text = Path(path).read_text(encoding="utf-8")
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if key.startswith("export "):
            key = key[7:].strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key] = value
    return values


def _read_pem(path: str, *, var: str) -> bytes:
    try:
        data = Path(path).expanduser().read_bytes()
    except OSError as exc:
        raise CredentialsMissing(f"{var} points at {path!r}, which cannot be read: {exc}") from None
    if b"-----BEGIN" not in data:
        raise CredentialsMissing(
            f"{var} points at {path!r}, which is not a PEM key (no '-----BEGIN' line). "
            f"If the venue gave you a text file with an id line on top, keep only the "
            f"BEGIN...END block."
        )
    return data


def load_kalshi(env: Mapping[str, str]) -> KalshiCredentials | None:
    key_id = env.get("KALSHI_KEY_ID")
    if not key_id:
        return None
    path = env.get("KALSHI_PRIVATE_KEY_PATH")
    if not path:
        raise CredentialsMissing("KALSHI_KEY_ID is set but KALSHI_PRIVATE_KEY_PATH is not")
    stage = (env.get("KALSHI_ENV") or "prod").lower()
    if stage not in ("demo", "prod"):
        raise CredentialsMissing(f"KALSHI_ENV must be demo or prod, got {stage!r}")
    return KalshiCredentials(key_id=key_id, private_key_pem=_read_pem(path, var="KALSHI_PRIVATE_KEY_PATH"), env=stage)  # type: ignore[arg-type]


def load_polymarket(env: Mapping[str, str]) -> PolymarketCredentials | None:
    key = env.get("POLYMARKET_PRIVATE_KEY")
    if not key:
        return None
    raw_type = env.get("POLYMARKET_SIGNATURE_TYPE") or "0"
    try:
        signature_type = int(raw_type)
    except ValueError:
        signature_type = -1
    if signature_type not in (0, 1, 2, 3):
        raise CredentialsMissing(f"POLYMARKET_SIGNATURE_TYPE must be 0, 1, 2 or 3, got {raw_type!r}")
    funder = env.get("POLYMARKET_FUNDER") or None
    if signature_type != 0 and not funder:
        raise CredentialsMissing(
            f"POLYMARKET_SIGNATURE_TYPE={signature_type} spends from a smart wallet; set POLYMARKET_FUNDER "
            f"to its address (polymarket.com -> profile menu)"
        )
    api = [env.get(name) or None for name in ("POLYMARKET_API_KEY", "POLYMARKET_API_SECRET", "POLYMARKET_API_PASSPHRASE")]
    if any(api) and not all(api):
        raise CredentialsMissing(
            "POLYMARKET_API_KEY, POLYMARKET_API_SECRET and POLYMARKET_API_PASSPHRASE go together; "
            "set all three, or none to derive them from the wallet key"
        )
    relayer = env.get("POLYMARKET_RELAYER_API_KEY") or None
    relayer_address = env.get("POLYMARKET_RELAYER_API_KEY_ADDRESS") or None
    if relayer and not relayer_address:
        raise CredentialsMissing("POLYMARKET_RELAYER_API_KEY is set but POLYMARKET_RELAYER_API_KEY_ADDRESS is not")
    return PolymarketCredentials(
        private_key=key, signature_type=signature_type, funder=funder,
        api_key=api[0], api_secret=api[1], api_passphrase=api[2],
        builder_code=_builder_code(env.get("POLYMARKET_BUILDER_CODE")),
        relayer_api_key=relayer, relayer_api_key_address=relayer_address,
        rpc_url=env.get("POLYMARKET_RPC_URL") or None,
    )


def _builder_code(raw: str | None) -> str | None:
    """`POLYMARKET_BUILDER_CODE`: unset or empty is Synpath's code, `none`
    attaches none, anything else must be a bytes32 hex code."""
    value = (raw or "").strip()
    if not value:
        return SYNPATH_BUILDER_CODE
    if value.lower() == "none":
        return None
    if not BUILDER_CODE.match(value):
        raise CredentialsMissing(
            f"POLYMARKET_BUILDER_CODE must be a bytes32 hex code (0x and 64 hex characters) or 'none', got {value!r}"
        )
    return value


def load_polymarket_us(env: Mapping[str, str]) -> PolymarketUSCredentials | None:
    key_id = env.get("POLYMARKET_US_KEY_ID")
    if not key_id:
        return None
    secret = env.get("POLYMARKET_US_SECRET_KEY")
    if not secret:
        raise CredentialsMissing("POLYMARKET_US_KEY_ID is set but POLYMARKET_US_SECRET_KEY is not")
    return PolymarketUSCredentials(key_id=key_id, secret_key=secret)


def load_polymarket_us_exchange(env: Mapping[str, str]) -> PolymarketUSExchangeCredentials | None:
    client_id = env.get("POLYMARKET_US_CLIENT_ID")
    if not client_id:
        return None
    path = env.get("POLYMARKET_US_PRIVATE_KEY_PATH")
    if not path:
        raise CredentialsMissing("POLYMARKET_US_CLIENT_ID is set but POLYMARKET_US_PRIVATE_KEY_PATH is not")
    participant = env.get("POLYMARKET_US_PARTICIPANT_ID")
    if not participant:
        raise CredentialsMissing(
            "POLYMARKET_US_CLIENT_ID is set but POLYMARKET_US_PARTICIPANT_ID is not; it is issued at "
            "onboarding as firms/<firm>/users/<user>"
        )
    stage = (env.get("POLYMARKET_US_ENV") or "preprod").lower()
    if stage not in ("preprod", "prod"):
        raise CredentialsMissing(f"POLYMARKET_US_ENV must be preprod or prod, got {stage!r}")
    return PolymarketUSExchangeCredentials(
        client_id=client_id,
        private_key_pem=_read_pem(path, var="POLYMARKET_US_PRIVATE_KEY_PATH"),
        participant_id=participant,
        account=env.get("POLYMARKET_US_ACCOUNT") or None,
        env=stage,  # type: ignore[arg-type]
    )


def load_opinion(env: Mapping[str, str]) -> OpinionCredentials | None:
    key = env.get("OPINION_PRIVATE_KEY")
    if not key:
        return None
    api_key = env.get("OPINION_API_KEY")
    if not api_key:
        raise CredentialsMissing(
            "OPINION_PRIVATE_KEY is set but OPINION_API_KEY is not; create one by signing with the same wallet "
            "(https://docs.opinion.trade/developer-guide/opinion-open-api/authentication)"
        )
    safe = env.get("OPINION_MULTISIG_ADDRESS") or None
    if safe and not re.match(r"^0x[0-9a-fA-F]{40}$", safe):
        raise CredentialsMissing(f"OPINION_MULTISIG_ADDRESS must be a 0x address, got {safe!r}")
    return OpinionCredentials(private_key=key, api_key=api_key, multisig_address=safe)


def load_hyperliquid(env: Mapping[str, str]) -> HyperliquidCredentials | None:
    key = env.get("HYPERLIQUID_PRIVATE_KEY")
    if not key:
        return None
    if not re.match(r"^(0x)?[0-9a-fA-F]{64}$", key):
        raise CredentialsMissing("HYPERLIQUID_PRIVATE_KEY must be 64 hex characters, optionally starting with 0x")
    account = env.get("HYPERLIQUID_ACCOUNT_ADDRESS") or None
    if account and not re.match(r"^0x[0-9a-fA-F]{40}$", account):
        raise CredentialsMissing(f"HYPERLIQUID_ACCOUNT_ADDRESS must be a 0x address, got {account!r}")
    testnet = (env.get("HYPERLIQUID_TESTNET") or "").strip().lower() in ("1", "true", "yes")
    return HyperliquidCredentials(
        private_key=key if key.startswith("0x") else "0x" + key, account_address=account, testnet=testnet,
    )


LOADERS = {
    "kalshi": load_kalshi,
    "polymarket": load_polymarket,
    "polymarket_us": load_polymarket_us,
    "polymarket_us_exchange": load_polymarket_us_exchange,
    "opinion": load_opinion,
    "hyperliquid": load_hyperliquid,
}


def load_credentials(
    env: Mapping[str, str] | None = None, *, dotenv: Path | str | None = None,
    redact_logs: bool = True,
) -> dict[str, Credentials | None]:
    """Every venue's credentials, or `None` where none are configured.

    The process environment wins over the `.env` file, so a variable exported
    for one run overrides what the file says. With `redact_logs`, every secret
    loaded is registered with the logging filter before anything else can
    print it.
    """
    merged: dict[str, str] = {}
    if dotenv is not None and Path(dotenv).exists():
        merged.update(read_dotenv(dotenv))
    elif dotenv is None and Path(".env").exists():
        merged.update(read_dotenv(".env"))
    merged.update(env if env is not None else os.environ)
    loaded = {venue: loader(merged) for venue, loader in LOADERS.items()}
    if redact_logs:
        for creds in loaded.values():
            if creds is not None:
                SecretFilter.install(creds.secrets)
    return loaded


def require(venue: str, loaded: Mapping[str, Credentials | None]) -> Credentials:
    """The venue's credentials, or `CredentialsMissing` naming what to set."""
    creds = loaded.get(venue)
    if creds is None:
        names = ", ".join(ENV_NAMES.get(venue, ()))
        raise CredentialsMissing(f"no {venue} credentials configured; set {names}")
    return creds


class SecretFilter(logging.Filter):
    """Replaces registered secret values with `***` in every log record.

    Attached to the root logger once, so every logger in the process is
    covered, including third-party ones that might echo a request body.
    Secrets are matched as substrings, so a PEM's base64 lines are scrubbed
    even when only part of the key is printed.
    """

    _instance: "SecretFilter | None" = None

    def __init__(self) -> None:
        super().__init__("synpath-secrets")
        self._secrets: list[str] = []

    @classmethod
    def install(cls, secrets: list[str]) -> "SecretFilter":
        if cls._instance is None:
            cls._instance = cls()
            logging.getLogger().addFilter(cls._instance)
            for handler in logging.getLogger().handlers:
                handler.addFilter(cls._instance)
        cls._instance.add(secrets)
        return cls._instance

    def add(self, secrets: list[str]) -> None:
        for secret in secrets:
            for piece in _pieces(secret):
                if piece and piece not in self._secrets:
                    self._secrets.append(piece)
        self._secrets.sort(key=len, reverse=True)

    def scrub(self, text: str) -> str:
        for secret in self._secrets:
            if secret in text:
                text = text.replace(secret, REDACTED)
        return text

    def filter(self, record: logging.LogRecord) -> bool:
        if not self._secrets:
            return True
        try:
            message = record.getMessage()
        except Exception:  # pragma: no cover - a broken format string is not ours to fix
            return True
        scrubbed = self.scrub(message)
        if scrubbed != message:
            record.msg = scrubbed
            record.args = ()
        return True


def _pieces(secret: str) -> list[str]:
    """A secret and, for a PEM, each of its base64 lines."""
    pieces = [secret.strip()]
    if "-----BEGIN" in secret:
        pieces += [
            line.strip() for line in secret.splitlines()
            if line.strip() and not line.startswith("-----")
        ]
    return [p for p in pieces if len(p) >= 8]
