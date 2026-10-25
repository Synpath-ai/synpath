"""Venue credentials: where they come from, and where they never go.

They come from the environment, or from a `.env` file next to the caller
that is git-ignored; the environment wins. They are
loaded once into frozen objects whose `repr` shows nothing secret, and every
secret value is registered with a logging filter so that a stray `%r` in a
log line prints `***` rather than a private key.

Nothing here talks to a venue. `synpath doctor` reports what
loaded; the adapters are what use it.

Two sets per venue. `load_credentials` is what a trading adapter needs: the
private key included, except for SX Bet, whose API key alone reads the
account and cancels and whose private key only places orders, and Myriad,
whose API key alone reads the account.
`load_market_data` is the API key alone, for the three venues whose market
data wants one and no wallet: Opinion, predict.fun and SX Bet.

  Kalshi              KALSHI_KEY_ID, KALSHI_PRIVATE_KEY_PATH (PEM, Ed25519 or RSA),
                      KALSHI_ENV (prod, the default, or demo)
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
  Opinion             OPINION_API_KEY (market data alone), OPINION_PRIVATE_KEY, and optionally
                      OPINION_MULTISIG_ADDRESS
  Hyperliquid         HYPERLIQUID_PRIVATE_KEY, and optionally
                      HYPERLIQUID_ACCOUNT_ADDRESS (when the key is an API
                      wallet) and HYPERLIQUID_TESTNET
  predict.fun         PREDICT_FUN_API_KEY (mainnet; market data alone), PREDICT_FUN_PRIVATE_KEY,
                      and optionally PREDICT_FUN_ACCOUNT_ADDRESS (a Predict
                      account), PREDICT_FUN_TESTNET, PREDICT_FUN_RPC_URL
  Limitless           LIMITLESS_PRIVATE_KEY, LIMITLESS_API_TOKEN_ID,
                      LIMITLESS_API_SECRET, and optionally LIMITLESS_RPC_URL
  SX Bet              SX_BET_API_KEY (market data, the account, cancels), and
                      optionally SX_BET_PRIVATE_KEY (placing orders),
                      SX_BET_ACCOUNT_ADDRESS, SX_BET_TESTNET
  Myriad              MYRIAD_PRIVATE_KEY (signs orders and cancels; signs in
                      when there is no API key), and optionally
                      MYRIAD_API_KEY with MYRIAD_API_SECRET (HMAC-signed
                      requests), MYRIAD_ACCOUNT_ADDRESS, MYRIAD_RPC_URL
  Betfair             BETFAIR_APP_KEY (a live key bets; a delayed one reads),
                      BETFAIR_USERNAME, BETFAIR_PASSWORD, and optionally
                      BETFAIR_CERT_PATH / BETFAIR_KEY_PATH (certificate
                      login) and BETFAIR_JURISDICTION
  Smarkets            SMARKETS_USERNAME, SMARKETS_PASSWORD: betting and the
                      account log in (placing orders needs Smarkets' approved
                      API access); market data needs no login, and one gives
                      undelayed quotes
  Novig               NOVIG_KEY_ID, NOVIG_PRIVATE_KEY_PATH (a subaccount's trading
                      key, PEM, Ed25519 or P-256), NOVIG_ENV (production, the
                      default, or paper), and optionally NOVIG_MANAGEMENT_KEY_ID /
                      NOVIG_MANAGEMENT_PRIVATE_KEY_PATH (subaccounts and funding)
  Matchbook           MATCHBOOK_USERNAME, MATCHBOOK_PASSWORD: betting logs in;
                      market data needs no login
  ProphetX            PROPHETX_ACCESS_KEY, PROPHETX_SECRET_KEY (the Trading API:
                      market data, streams and orders), and optionally
                      PROPHETX_AFFILIATE_KEY (the Market Data API, market data
                      alone) and PROPHETX_SANDBOX
  Azuro               AZURO_PRIVATE_KEY, and optionally AZURO_CHAIN (polygon,
                      the default, base, polygon_amoy, base_sepolia),
                      AZURO_AFFILIATE, AZURO_RPC_URL
  Gemini              GEMINI_API_KEY, GEMINI_API_SECRET, and optionally
                      GEMINI_ACCOUNT (a master key's subaccount) and GEMINI_ENV
                      (prod, the default, or sandbox); market data needs neither
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


@dataclass(frozen=True, repr=False)
class PredictFunCredentials:
    """predict.fun: the key that signs, the account it signs for, and the
    API key mainnet needs."""

    private_key: str = field(repr=False)
    """The signer's private key, hex: a plain wallet's own, or, for a
    Predict account, its owner key (the Privy wallet, exported from
    predict.fun's account settings)."""
    api_key: str | None = field(default=None, repr=False)
    """The mainnet API key (developers.predict.fun). The test network needs none."""
    account_address: str | None = None
    """The Predict account (the deposit address) the key signs for. Unset,
    the key's own wallet trades."""
    testnet: bool = False
    """Trade on BNB Chain's test network, for test USDT."""
    rpc_url: str | None = None
    """A BNB Chain node for the balance read. A public one when unset."""
    _public = ("account_address", "testnet", "rpc_url")
    __repr__ = _redacted_repr

    @property
    def address(self) -> str:
        """The address that makes the orders and holds the funds."""
        if self.account_address:
            return self.account_address
        from .polymarket_signing import WalletSigner

        return WalletSigner(self.private_key).address

    @property
    def secrets(self) -> list[str]:
        return [self.private_key] + ([self.api_key] if self.api_key else [])


@dataclass(frozen=True, repr=False)
class LimitlessCredentials:
    """Limitless: the wallet that signs orders, and the scoped API token that
    signs every request."""

    private_key: str = field(repr=False)
    """The wallet's private key, hex: the wallet connected on limitless.exchange,
    in EOA trading mode. Signs every order (EIP-712)."""
    token_id: str
    """The scoped API token's id (limitless.exchange, profile, API tokens, Derive)."""
    secret: str = field(repr=False)
    """The token's secret, base64, shown once when the token is derived."""
    rpc_url: str | None = None
    """A Base node for the balance read. A public one when unset."""
    _public = ("token_id", "rpc_url")
    __repr__ = _redacted_repr

    @property
    def address(self) -> str:
        from .polymarket_signing import WalletSigner

        return WalletSigner(self.private_key).address

    @property
    def secrets(self) -> list[str]:
        return [self.private_key, self.secret]


@dataclass(frozen=True, repr=False)
class SxBetCredentials:
    """SX Bet: the API key every request (and every realtime channel)
    carries, and, to place orders, the wallet that signs them.

    The venue authorizes everything but placing by the key alone: the
    account's orders, fills, positions and balance, and every cancel. Only a
    new order is also an EIP-712 message signed by the wallet."""

    api_key: str = field(repr=False)
    """The SX Bet API key, generated on sx.bet under Account -> Overview ->
    API Credentials. It belongs to the account's own address."""
    private_key: str | None = field(default=None, repr=False)
    """The wallet's private key, hex: the account's own address, never the
    proxy wallet. Signs every order (EIP-712); unset, orders cannot be placed."""
    account_address: str | None = None
    """The account's wallet address, for the account stream's channels when
    no private key names it. Public: the 0x address, not the key."""
    testnet: bool = False
    """The Toronto test network (api.toronto.sx.bet) instead of mainnet."""
    _public = ("account_address", "testnet")
    __repr__ = _redacted_repr

    @property
    def address(self) -> str | None:
        """The account's address: the signer's, else the one given, else
        `None` (the venue documents no route that names a key's owner)."""
        if self.private_key:
            from .polymarket_signing import WalletSigner

            return WalletSigner(self.private_key).address
        return self.account_address

    @property
    def secrets(self) -> list[str]:
        return [self.api_key] + ([self.private_key] if self.private_key else [])


@dataclass(frozen=True, repr=False)
class MyriadCredentials:
    """Myriad: the wallet that signs orders, and how requests prove whose they are.

    Every order and every cancel is an EIP-712 message signed by the wallet.
    Requests to the account's endpoints are signed with an API key and its
    secret (HMAC); without a key, the wallet signs in (SIWE) for a session
    token instead. The venue refuses a bare key. A key acts for the wallet
    that minted it, so a key with no private key reads the account but
    cannot place or cancel."""

    private_key: str | None = field(default=None, repr=False)
    """The wallet's private key, hex: the wallet the Myriad account is
    connected to. Signs orders, cancels and the sign-in."""
    api_key: str | None = None
    api_secret: str | None = field(default=None, repr=False)
    """An API key and its secret (Myriad Account settings, or `POST
    /auth/api-keys` from a signed-in session; the secret is shown once)."""
    account_address: str | None = None
    """The wallet's address, when no private key names it. Public."""
    rpc_url: str | None = None
    """A BNB Chain node for the balance read. A public one when unset."""
    _public = ("api_key", "account_address", "rpc_url")
    __repr__ = _redacted_repr

    @property
    def address(self) -> str | None:
        """The trading wallet: the signer's address, else the one given."""
        if self.private_key:
            from .polymarket_signing import WalletSigner

            return WalletSigner(self.private_key).address
        return self.account_address

    @property
    def secrets(self) -> list[str]:
        return [v for v in (self.private_key, self.api_secret) if v]


@dataclass(frozen=True, repr=False)
class BetfairCredentials:
    """Betfair: the account's application key and its login. Every Betfair
    call, market data included, needs both: the key names the application,
    the login's session token the account. Betting needs a live key; a
    delayed key reads market data and the account only."""

    app_key: str = field(repr=False)
    """The application key (delayed or live) from the account's developer
    settings. Sent on every request as `X-Application`."""
    username: str
    password: str = field(repr=False)
    cert_path: str | None = None
    key_path: str | None = None
    """A self-signed client certificate and its key, for Betfair's
    non-interactive (certificate) login. Without them the login is interactive."""
    jurisdiction: str = "com"
    """The Betfair site the account is registered on: com, it, es, ro or com.au."""
    _public = ("username", "cert_path", "jurisdiction")
    __repr__ = _redacted_repr

    @property
    def secrets(self) -> list[str]:
        return [self.app_key, self.password]


@dataclass(frozen=True, repr=False)
class SmarketsCredentials:
    """Smarkets: the account's login. Betting and the account's reads (orders,
    statement, balance) log in with it; market data needs none, and a session
    from it gives undelayed quotes and the account's own rate limits. Placing
    orders needs Smarkets' approved API access. An account with two-factor
    login cannot log in from here (pass a session token instead)."""

    username: str
    password: str = field(repr=False)
    _public = ("username",)
    __repr__ = _redacted_repr

    @property
    def secrets(self) -> list[str]:
        return [self.password]


@dataclass(frozen=True, repr=False)
class NovigCredentials:
    """Novig: a subaccount's trading key, and optionally the account's
    management key. Each is a key id and a PKCS#8 PEM private key, Ed25519
    or P-256, that signs every request (NOVIG-V3).

    The trading key places and cancels orders on its one subaccount, reads
    its orders, fills, positions and balance, and opens the websocket. The
    management key (Profile -> Settings -> Novig API in the web app) opens,
    funds and labels subaccounts and never places an order. A key works only
    in the environment it was made in."""

    key_id: str | None = None
    """The trading key's id: also the address of its subaccount."""
    private_key_pem: bytes | None = field(default=None, repr=False)
    management_key_id: str | None = None
    management_private_key_pem: bytes | None = field(default=None, repr=False)
    env: Literal["production", "paper"] = "production"
    """`production` trades real money at api.novig.com (US identity and
    location checks); `paper` play money at api.paper.novig.com."""
    _public = ("key_id", "management_key_id", "env")
    __repr__ = _redacted_repr

    @property
    def secrets(self) -> list[str]:
        return [pem.decode(errors="ignore") for pem in (self.private_key_pem, self.management_private_key_pem) if pem]


@dataclass(frozen=True, repr=False)
class MatchbookCredentials:
    """Matchbook: the account's login. Betting and the account's reads (offers,
    bets, balance) log in with it; market data needs none. An account with
    two-factor login cannot log in from here (pass a session token instead)."""

    username: str
    password: str = field(repr=False)
    _public = ("username",)
    __repr__ = _redacted_repr

    @property
    def secrets(self) -> list[str]:
        return [self.password]


@dataclass(frozen=True, repr=False)
class ProphetXCredentials:
    """ProphetX: the Trading API's key pair, and optionally the Market Data
    API's affiliate key. Either reads market data; only the key pair trades
    and streams."""

    access_key: str | None = field(default=None, repr=False)
    secret_key: str | None = field(default=None, repr=False)
    """The Trading API key pair, generated by any KYC-approved account under
    API Integration on prophetx.co (the secret is shown once). Exchanged at
    `/auth/login` for a ten-minute bearer token."""
    affiliate_key: str | None = field(default=None, repr=False)
    """A Market Data API key, issued on request by ProphetX's market data
    team. Sent as it is in `Authorization`; reads market data only."""
    sandbox: bool = False
    """ProphetX's sandbox (api.sandbox.prophetx.dev, demo funds) instead of
    production. Sandbox keys are made on sandbox.prophetx.dev."""
    _public = ("sandbox",)
    __repr__ = _redacted_repr

    @property
    def trades(self) -> bool:
        return bool(self.access_key and self.secret_key)

    @property
    def secrets(self) -> list[str]:
        return [v for v in (self.access_key, self.secret_key, self.affiliate_key) if v]


@dataclass(frozen=True, repr=False)
class AzuroCredentials:
    """Azuro: the wallet that signs every bet (EIP-712) and holds the bet
    token, and the chain whose pool it bets with. Market data needs nothing."""

    private_key: str = field(repr=False)
    """The wallet's private key, hex. Signs every bet and cash-out; the
    relayer places them on chain and is paid from this wallet's tokens."""
    chain: str = "polygon"
    """The deployment bet with: `polygon` (USDT), `base` (WETH), or the
    testnets `polygon_amoy` and `base_sepolia`."""
    affiliate: str | None = None
    """The affiliate address every bet names; the wallet's own when unset."""
    rpc_url: str | None = None
    """A JSON-RPC endpoint on the chain, for the balance, the allowance and
    the approval; the chain's public endpoint when unset."""
    _public = ("chain", "affiliate", "rpc_url")
    __repr__ = _redacted_repr

    @property
    def address(self) -> str:
        from .polymarket_signing import WalletSigner

        return WalletSigner(self.private_key).address

    @property
    def secrets(self) -> list[str]:
        return [self.private_key]


@dataclass(frozen=True, repr=False)
class GeminiCredentials:
    """Gemini Predictions: an API key pair from exchange.gemini.com/settings/api.

    Account-scoped, with the Trader role and a time-based nonce: the
    WebSocket takes no other key, and the REST nonces here are Unix seconds.
    The account needs Prediction Markets access (US) and the current
    Prediction Markets terms accepted."""

    api_key: str
    """The key id, `account-...` (or `master-...` for a master key)."""
    api_secret: str = field(repr=False)
    """The secret shown once at creation. Signs every private request (HMAC-SHA384)."""
    account: str | None = None
    """The subaccount a master key acts for (its short name). Unset for an
    account-scoped key."""
    env: Literal["prod", "sandbox"] = "prod"
    """`prod` trades on Gemini itself; `sandbox` on api.sandbox.gemini.com with a sandbox key."""
    _public = ("api_key", "account", "env")
    __repr__ = _redacted_repr

    @property
    def secrets(self) -> list[str]:
        return [self.api_secret]


Credentials = (
    KalshiCredentials | PolymarketCredentials | PolymarketUSCredentials | PolymarketUSExchangeCredentials
    | OpinionCredentials | HyperliquidCredentials | PredictFunCredentials | LimitlessCredentials | SxBetCredentials
    | BetfairCredentials | SmarketsCredentials | NovigCredentials | MatchbookCredentials | ProphetXCredentials | AzuroCredentials | MyriadCredentials | GeminiCredentials
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
    "predict_fun": (
        "PREDICT_FUN_PRIVATE_KEY", "PREDICT_FUN_API_KEY", "PREDICT_FUN_ACCOUNT_ADDRESS", "PREDICT_FUN_TESTNET",
        "PREDICT_FUN_RPC_URL",
    ),
    "limitless": ("LIMITLESS_PRIVATE_KEY", "LIMITLESS_API_TOKEN_ID", "LIMITLESS_API_SECRET", "LIMITLESS_RPC_URL"),
    "sx_bet": ("SX_BET_API_KEY", "SX_BET_PRIVATE_KEY", "SX_BET_ACCOUNT_ADDRESS", "SX_BET_TESTNET"),
    "betfair": (
        "BETFAIR_APP_KEY", "BETFAIR_USERNAME", "BETFAIR_PASSWORD", "BETFAIR_CERT_PATH", "BETFAIR_KEY_PATH",
        "BETFAIR_JURISDICTION",
    ),
    "smarkets": ("SMARKETS_USERNAME", "SMARKETS_PASSWORD"),
    "novig": (
        "NOVIG_KEY_ID", "NOVIG_PRIVATE_KEY_PATH", "NOVIG_ENV", "NOVIG_MANAGEMENT_KEY_ID",
        "NOVIG_MANAGEMENT_PRIVATE_KEY_PATH",
    ),
    "matchbook": ("MATCHBOOK_USERNAME", "MATCHBOOK_PASSWORD"),
    "prophetx": ("PROPHETX_ACCESS_KEY", "PROPHETX_SECRET_KEY", "PROPHETX_AFFILIATE_KEY", "PROPHETX_SANDBOX"),
    "azuro": ("AZURO_PRIVATE_KEY", "AZURO_CHAIN", "AZURO_AFFILIATE", "AZURO_RPC_URL"),
    "myriad": ("MYRIAD_PRIVATE_KEY", "MYRIAD_API_KEY", "MYRIAD_API_SECRET", "MYRIAD_ACCOUNT_ADDRESS", "MYRIAD_RPC_URL"),
    "gemini": ("GEMINI_API_KEY", "GEMINI_API_SECRET", "GEMINI_ACCOUNT", "GEMINI_ENV"),
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


def load_predict_fun(env: Mapping[str, str]) -> PredictFunCredentials | None:
    key = env.get("PREDICT_FUN_PRIVATE_KEY")
    if not key:
        return None
    if not re.match(r"^(0x)?[0-9a-fA-F]{64}$", key):
        raise CredentialsMissing("PREDICT_FUN_PRIVATE_KEY must be 64 hex characters, optionally starting with 0x")
    testnet = (env.get("PREDICT_FUN_TESTNET") or "").strip().lower() in ("1", "true", "yes")
    api_key = env.get("PREDICT_FUN_API_KEY") or None
    if not api_key and not testnet:
        raise CredentialsMissing(
            "PREDICT_FUN_PRIVATE_KEY is set but PREDICT_FUN_API_KEY is not; mainnet needs one "
            "(https://developers.predict.fun), or set PREDICT_FUN_TESTNET=1"
        )
    account = env.get("PREDICT_FUN_ACCOUNT_ADDRESS") or None
    if account and not re.match(r"^0x[0-9a-fA-F]{40}$", account):
        raise CredentialsMissing(f"PREDICT_FUN_ACCOUNT_ADDRESS must be a 0x address, got {account!r}")
    return PredictFunCredentials(
        private_key=key if key.startswith("0x") else "0x" + key, api_key=api_key, account_address=account,
        testnet=testnet, rpc_url=env.get("PREDICT_FUN_RPC_URL") or None,
    )


def load_limitless(env: Mapping[str, str]) -> LimitlessCredentials | None:
    key = env.get("LIMITLESS_PRIVATE_KEY")
    if not key:
        return None
    if not re.match(r"^(0x)?[0-9a-fA-F]{64}$", key):
        raise CredentialsMissing("LIMITLESS_PRIVATE_KEY must be 64 hex characters, optionally starting with 0x")
    token_id, secret = env.get("LIMITLESS_API_TOKEN_ID"), env.get("LIMITLESS_API_SECRET")
    if not token_id or not secret:
        raise CredentialsMissing(
            "LIMITLESS_PRIVATE_KEY is set but LIMITLESS_API_TOKEN_ID / LIMITLESS_API_SECRET are not; derive a "
            "scoped API token on limitless.exchange (profile, API tokens, Derive)"
        )
    import base64
    import binascii

    try:
        base64.b64decode(secret, validate=True)
    except (binascii.Error, ValueError):
        raise CredentialsMissing("LIMITLESS_API_SECRET must be the base64 secret shown when the token was derived") from None
    return LimitlessCredentials(
        private_key=key if key.startswith("0x") else "0x" + key, token_id=token_id, secret=secret,
        rpc_url=env.get("LIMITLESS_RPC_URL") or None,
    )


def load_sx_bet(env: Mapping[str, str]) -> SxBetCredentials | None:
    api_key = env.get("SX_BET_API_KEY")
    key = env.get("SX_BET_PRIVATE_KEY") or None
    if not api_key:
        if key:
            raise CredentialsMissing(
                "SX_BET_PRIVATE_KEY is set but SX_BET_API_KEY is not; generate an API key on sx.bet "
                "(Account -> Overview -> API Credentials)"
            )
        return None
    if key and not re.match(r"^(0x)?[0-9a-fA-F]{64}$", key):
        raise CredentialsMissing("SX_BET_PRIVATE_KEY must be 64 hex characters, optionally starting with 0x")
    key = None if key is None else key if key.startswith("0x") else "0x" + key
    account = env.get("SX_BET_ACCOUNT_ADDRESS") or None
    if account and not re.match(r"^0x[0-9a-fA-F]{40}$", account):
        raise CredentialsMissing(
            f"SX_BET_ACCOUNT_ADDRESS must be the wallet's 0x address (40 hex characters), not its private key; "
            f"got {_shape_of(account)}"
        )
    if account and key:
        from .polymarket_signing import WalletSigner

        # The key signs as its own address; a different account address would
        # watch one account's channels while placing for another.
        if WalletSigner(key).address.lower() != account.lower():
            raise CredentialsMissing("SX_BET_ACCOUNT_ADDRESS is not the address of SX_BET_PRIVATE_KEY")
    testnet = str(env.get("SX_BET_TESTNET") or "").strip().lower() in ("1", "true", "yes")
    return SxBetCredentials(api_key=api_key, private_key=key, account_address=account, testnet=testnet)


def load_myriad(env: Mapping[str, str]) -> MyriadCredentials | None:
    key = env.get("MYRIAD_PRIVATE_KEY") or None
    api_key, secret = env.get("MYRIAD_API_KEY") or None, env.get("MYRIAD_API_SECRET") or None
    if api_key and not secret:
        raise CredentialsMissing(
            "MYRIAD_API_KEY is set but MYRIAD_API_SECRET is not; the venue refuses a key without its signature "
            "since 2026-08-15. A key made before secrets existed cannot be upgraded: mint a new one in Myriad "
            "Account settings")
    if secret and not api_key:
        raise CredentialsMissing("MYRIAD_API_SECRET is set but MYRIAD_API_KEY is not")
    if not key and not api_key:
        return None
    if key and not re.match(r"^(0x)?[0-9a-fA-F]{64}$", key):
        raise CredentialsMissing("MYRIAD_PRIVATE_KEY must be 64 hex characters, optionally starting with 0x")
    key = None if key is None else key if key.startswith("0x") else "0x" + key
    account = env.get("MYRIAD_ACCOUNT_ADDRESS") or None
    if account and not re.match(r"^0x[0-9a-fA-F]{40}$", account):
        raise CredentialsMissing(
            f"MYRIAD_ACCOUNT_ADDRESS must be the wallet's 0x address (40 hex characters), not its private key; "
            f"got {_shape_of(account)}")
    if account and key:
        from .polymarket_signing import WalletSigner

        if WalletSigner(key).address.lower() != account.lower():
            raise CredentialsMissing("MYRIAD_ACCOUNT_ADDRESS is not the address of MYRIAD_PRIVATE_KEY")
    return MyriadCredentials(private_key=key, api_key=api_key, api_secret=secret, account_address=account,
                             rpc_url=env.get("MYRIAD_RPC_URL") or None)


def _shape_of(value: str) -> str:
    """What a malformed value looks like, without echoing it: it may be a
    private key pasted into the wrong variable."""
    return f"{len(value)} characters" + (" that look like a private key" if re.match(r"^(0x)?[0-9a-fA-F]{64}$", value) else "")


BETFAIR_JURISDICTIONS = ("com", "it", "es", "ro", "com.au")


def load_betfair(env: Mapping[str, str]) -> BetfairCredentials | None:
    app_key = env.get("BETFAIR_APP_KEY")
    if not app_key:
        return None
    username, password = env.get("BETFAIR_USERNAME"), env.get("BETFAIR_PASSWORD")
    if not username or not password:
        raise CredentialsMissing(
            "BETFAIR_APP_KEY is set but BETFAIR_USERNAME / BETFAIR_PASSWORD are not; every Betfair call, "
            "market data included, needs the account's login"
        )
    cert, key = env.get("BETFAIR_CERT_PATH") or None, env.get("BETFAIR_KEY_PATH") or None
    if bool(cert) != bool(key):
        raise CredentialsMissing("certificate login needs both BETFAIR_CERT_PATH and BETFAIR_KEY_PATH")
    for path in (cert, key):
        if path and not Path(path).expanduser().exists():
            raise CredentialsMissing(f"no file at {path} (BETFAIR_CERT_PATH / BETFAIR_KEY_PATH)")
    jurisdiction = (env.get("BETFAIR_JURISDICTION") or "com").strip().lower().lstrip(".")
    if jurisdiction not in BETFAIR_JURISDICTIONS:
        raise CredentialsMissing(f"BETFAIR_JURISDICTION must be one of {', '.join(BETFAIR_JURISDICTIONS)}")
    return BetfairCredentials(
        app_key=app_key, username=username, password=password,
        cert_path=str(Path(cert).expanduser()) if cert else None, key_path=str(Path(key).expanduser()) if key else None,
        jurisdiction=jurisdiction,
    )


def load_smarkets(env: Mapping[str, str]) -> SmarketsCredentials | None:
    username, password = env.get("SMARKETS_USERNAME"), env.get("SMARKETS_PASSWORD")
    if not username and not password:
        return None
    if not username or not password:
        raise CredentialsMissing(
            "a Smarkets login needs both SMARKETS_USERNAME and SMARKETS_PASSWORD (or neither: market data "
            "needs no login)"
        )
    return SmarketsCredentials(username=username, password=password)


NOVIG_ENVS = ("production", "paper")


def load_novig(env: Mapping[str, str]) -> NovigCredentials | None:
    key_id, management_id = env.get("NOVIG_KEY_ID") or None, env.get("NOVIG_MANAGEMENT_KEY_ID") or None
    if not key_id and not management_id:
        return None
    pems: list[bytes | None] = []
    for name, path_var in ((key_id, "NOVIG_PRIVATE_KEY_PATH"), (management_id, "NOVIG_MANAGEMENT_PRIVATE_KEY_PATH")):
        path = env.get(path_var) or None
        if name and not path:
            id_var = "NOVIG_KEY_ID" if path_var == "NOVIG_PRIVATE_KEY_PATH" else "NOVIG_MANAGEMENT_KEY_ID"
            raise CredentialsMissing(f"{id_var} is set but {path_var} is not")
        pem = _read_pem(path, var=path_var) if name and path else None
        if pem is not None:
            from .novig_signing import load_private_key

            try:
                load_private_key(pem)
            except (ValueError, TypeError) as exc:
                raise CredentialsMissing(f"{path_var} points at {path!r}, which is not an unencrypted "
                                         f"Ed25519 or P-256 PKCS#8 key: {exc}") from None
        pems.append(pem)
    stage = (env.get("NOVIG_ENV") or "production").strip().lower()
    if stage not in NOVIG_ENVS:
        raise CredentialsMissing(f"NOVIG_ENV must be production or paper, got {stage!r}")
    return NovigCredentials(
        key_id=key_id, private_key_pem=pems[0], management_key_id=management_id,
        management_private_key_pem=pems[1], env=stage,  # type: ignore[arg-type]
    )


def load_matchbook(env: Mapping[str, str]) -> MatchbookCredentials | None:
    username, password = env.get("MATCHBOOK_USERNAME"), env.get("MATCHBOOK_PASSWORD")
    if not username and not password:
        return None
    if not username or not password:
        raise CredentialsMissing("a Matchbook login needs both MATCHBOOK_USERNAME and MATCHBOOK_PASSWORD")
    return MatchbookCredentials(username=username, password=password)


def load_prophetx(env: Mapping[str, str]) -> ProphetXCredentials | None:
    access, secret = env.get("PROPHETX_ACCESS_KEY") or None, env.get("PROPHETX_SECRET_KEY") or None
    affiliate = env.get("PROPHETX_AFFILIATE_KEY") or None
    if not access and not secret and not affiliate:
        return None
    if bool(access) != bool(secret):
        raise CredentialsMissing(
            "ProphetX's Trading API key is a pair: set both PROPHETX_ACCESS_KEY and PROPHETX_SECRET_KEY "
            "(prophetx.co, API Integration; the secret is shown once)"
        )
    sandbox = str(env.get("PROPHETX_SANDBOX") or "").strip().lower() in ("1", "true", "yes")
    return ProphetXCredentials(access_key=access, secret_key=secret, affiliate_key=affiliate, sandbox=sandbox)


AZURO_CHAINS = ("polygon", "base", "polygon_amoy", "base_sepolia")
"""The deployments that take bets; Gnosis and Chiliz are discontinued."""


def load_azuro(env: Mapping[str, str]) -> AzuroCredentials | None:
    key = env.get("AZURO_PRIVATE_KEY")
    if not key:
        return None
    if not re.match(r"^(0x)?[0-9a-fA-F]{64}$", key):
        raise CredentialsMissing("AZURO_PRIVATE_KEY must be 64 hex characters, optionally starting with 0x")
    chain = (env.get("AZURO_CHAIN") or "polygon").strip().lower().replace("-", "_")
    if chain not in AZURO_CHAINS:
        raise CredentialsMissing(f"AZURO_CHAIN must be one of {', '.join(AZURO_CHAINS)} (Gnosis and Chiliz are discontinued)")
    affiliate = env.get("AZURO_AFFILIATE") or None
    if affiliate and not re.match(r"^0x[0-9a-fA-F]{40}$", affiliate):
        raise CredentialsMissing(f"AZURO_AFFILIATE must be a 0x address (40 hex characters); got {_shape_of(affiliate)}")
    return AzuroCredentials(
        private_key=key if key.startswith("0x") else "0x" + key, chain=chain, affiliate=affiliate,
        rpc_url=env.get("AZURO_RPC_URL") or None,
    )


def load_gemini(env: Mapping[str, str]) -> GeminiCredentials | None:
    api_key = (env.get("GEMINI_API_KEY") or "").strip()
    secret = (env.get("GEMINI_API_SECRET") or "").strip()
    if not api_key:
        if secret:
            raise CredentialsMissing("GEMINI_API_SECRET is set but GEMINI_API_KEY is not")
        return None
    if not secret:
        raise CredentialsMissing("GEMINI_API_KEY is set but GEMINI_API_SECRET is not; it is shown once when the key "
                                 "is created (exchange.gemini.com/settings/api)")
    if secret == api_key:
        raise CredentialsMissing("GEMINI_API_SECRET is the same as GEMINI_API_KEY; the secret is the other half of the pair")
    stage = (env.get("GEMINI_ENV") or "prod").strip().lower()
    if stage not in ("prod", "sandbox"):
        raise CredentialsMissing(f"GEMINI_ENV must be prod or sandbox, got {stage!r}")
    account = (env.get("GEMINI_ACCOUNT") or "").strip() or None
    return GeminiCredentials(api_key=api_key, api_secret=secret, account=account, env=stage)  # type: ignore[arg-type]


LOADERS = {
    "kalshi": load_kalshi,
    "polymarket": load_polymarket,
    "polymarket_us": load_polymarket_us,
    "polymarket_us_exchange": load_polymarket_us_exchange,
    "opinion": load_opinion,
    "hyperliquid": load_hyperliquid,
    "predict_fun": load_predict_fun,
    "limitless": load_limitless,
    "sx_bet": load_sx_bet,
    "betfair": load_betfair,
    "smarkets": load_smarkets,
    "novig": load_novig,
    "matchbook": load_matchbook,
    "prophetx": load_prophetx,
    "azuro": load_azuro,
    "myriad": load_myriad,
    "gemini": load_gemini,
}


def merged_env(env: Mapping[str, str] | None = None, *, dotenv: Path | str | None = None) -> dict[str, str]:
    """The `.env` file overlaid by the environment: `dotenv` when given, else
    `./.env` when there is one. The environment wins, so a variable exported
    for one run overrides what the file says."""
    merged: dict[str, str] = {}
    if dotenv is not None and Path(dotenv).exists():
        merged.update(read_dotenv(dotenv))
    elif dotenv is None and Path(".env").exists():
        merged.update(read_dotenv(".env"))
    merged.update(env if env is not None else os.environ)
    return merged


def load_credentials(
    env: Mapping[str, str] | None = None, *, dotenv: Path | str | None = None,
    redact_logs: bool = True,
) -> dict[str, Credentials | None]:
    """Every venue's trading credentials, or `None` where none are configured.

    These are what an order needs, so a venue configured with only the API
    key its market data needs is `None` here; `load_market_data` has it.
    With `redact_logs`, every secret loaded is registered with the logging
    filter before anything else can print it.
    """
    merged = merged_env(env, dotenv=dotenv)
    loaded = {venue: loader(merged) for venue, loader in LOADERS.items()}
    if redact_logs:
        for creds in loaded.values():
            if creds is not None:
                SecretFilter.install(creds.secrets)
    return loaded


@dataclass(frozen=True, repr=False)
class MarketDataKey:
    """The API key a venue wants before it serves market data, with no wallet
    behind it. Reading books needs nothing more, so a user who only watches
    prices never has to put a private key on disk."""

    venue: str
    api_key: str = field(repr=False)
    testnet: bool = False
    _public = ("venue", "testnet")
    __repr__ = _redacted_repr

    @property
    def secrets(self) -> list[str]:
        return [self.api_key]


MARKET_DATA_KEYS: dict[str, tuple[str, str | None]] = {
    "opinion": ("OPINION_API_KEY", None),
    "predict_fun": ("PREDICT_FUN_API_KEY", "PREDICT_FUN_TESTNET"),
    "sx_bet": ("SX_BET_API_KEY", "SX_BET_TESTNET"),
}
"""The venues whose market data needs an API key but no private key: the
variable holding the key, and the one choosing the test network. Kalshi and
Polymarket US sign even their market streams with the full key pair, so
their trading credentials are their market-data credentials; Polymarket,
Hyperliquid and Limitless serve market data to anyone."""


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes")


def load_market_data(
    env: Mapping[str, str] | None = None, *, dotenv: Path | str | None = None,
    redact_logs: bool = True,
) -> dict[str, MarketDataKey]:
    """The market-data API key of every venue that has one set, whether or
    not its private key is. Read from the same places as `load_credentials`."""
    merged = merged_env(env, dotenv=dotenv)
    keys: dict[str, MarketDataKey] = {}
    for venue, (key_name, testnet_name) in MARKET_DATA_KEYS.items():
        api_key = (merged.get(key_name) or "").strip()
        if api_key:
            keys[venue] = MarketDataKey(venue=venue, api_key=api_key,
                                        testnet=_truthy(merged.get(testnet_name)) if testnet_name else False)
    if redact_logs:
        for key in keys.values():
            SecretFilter.install(key.secrets)
    return keys


def require(venue: str, loaded: Mapping[str, Credentials | None]) -> Credentials:
    """The venue's credentials, or `CredentialsMissing` naming what to set."""
    creds = loaded.get(venue)
    if creds is None:
        names = ", ".join(ENV_NAMES.get(venue, ()))
        # An API key alone is a configured venue for market data, so say why
        # it is not enough here rather than telling the user to set it again.
        partial = (f" ({MARKET_DATA_KEYS[venue][0]} reads market data; trading also needs the private key)"
                   if venue in MARKET_DATA_KEYS else "")
        raise CredentialsMissing(f"no {venue} trading credentials configured; set {names}{partial}")
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
