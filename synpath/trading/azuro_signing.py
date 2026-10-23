"""Azuro bet and cash-out signing, and the amounts in them, with no network in sight.

Built to `@azuro-org/toolkit` 7.2.0 (`getBetTypedData`, `createBet`,
`getCashoutTypedData`, `createCashout`), and checked against digests and
signatures that toolkit and viem produce (`tests/samples/azuro_signing_vectors.json`).

**A bet** is the EIP-712 message `ClientBetData`: the `ClientData` (a consent
text, the affiliate, the pool's core contract, an expiry, the chain id, the
relayer's fee and three sponsorship flags) and a one-element `SubBet[]` (the
condition, the outcome, the minimum odds, the stake and a nonce), under the
domain `Live Betting` version `1.0.0`, the chain's id, and the core contract as
the verifying contract. The bettor's wallet signs it; the relayer places the
bet on chain and takes its fee from the bettor's tokens, which needs the
bettor's allowance to the relayer for the stake plus the fee.

**Odds** are fixed point with 12 decimals (1.5 is `1_500_000_000_000`). A bet
names its worst acceptable odds; the YES price given is turned into them as
`1 / price`, rounded up, so a bet never pays more per contract than asked.

**A cash-out** is `CashOutOrder` (a consent text, the chain id, one
`CashOutItem` -- the bet's id, the core contract, the minimum odds -- and an
expiry) under the domain `Cash Out` version `1.0.0`, the chain's id and the
cash-out contract.
"""
from __future__ import annotations

from decimal import ROUND_CEILING, ROUND_DOWN, Decimal
from typing import Any

from ..azuro import Chain
from .errors import InvalidOrder
from .polymarket_signing import WalletSigner, _eth

ODDS_DECIMALS = 12
ODDS_SCALE = 10 ** ODDS_DECIMALS

BET_DOMAIN_NAME = "Live Betting"
BET_DOMAIN_VERSION = "1.0.0"
CASHOUT_DOMAIN_NAME = "Cash Out"
CASHOUT_DOMAIN_VERSION = "1.0.0"

BET_ATTENTION = "By signing this transaction, I agree to place a bet for an event on Azuro Protocol"
CASHOUT_ATTENTION = "By signing this transaction, I agree to cash out on Azuro"
"""The consent texts the venue's SDK signs by default."""

DOMAIN_FIELDS = [
    {"name": "name", "type": "string"}, {"name": "version", "type": "string"},
    {"name": "chainId", "type": "uint256"}, {"name": "verifyingContract", "type": "address"},
]
CLIENT_DATA_FIELDS = [
    {"name": "attention", "type": "string"},
    {"name": "affiliate", "type": "address"},
    {"name": "core", "type": "address"},
    {"name": "expiresAt", "type": "uint256"},
    {"name": "chainId", "type": "uint256"},
    {"name": "relayerFeeAmount", "type": "uint256"},
    {"name": "isFeeSponsored", "type": "bool"},
    {"name": "isBetSponsored", "type": "bool"},
    {"name": "isSponsoredBetReturnable", "type": "bool"},
]
BET_TYPES: dict[str, list[dict[str, str]]] = {
    "ClientBetData": [{"name": "clientData", "type": "ClientData"}, {"name": "bets", "type": "SubBet[]"}],
    "ClientData": CLIENT_DATA_FIELDS,
    "SubBet": [
        {"name": "conditionId", "type": "uint256"},
        {"name": "outcomeId", "type": "uint128"},
        {"name": "minOdds", "type": "uint64"},
        {"name": "amount", "type": "uint128"},
        {"name": "nonce", "type": "uint256"},
    ],
}
CASHOUT_TYPES: dict[str, list[dict[str, str]]] = {
    "CashOutItem": [
        {"name": "betId", "type": "uint256"},
        {"name": "bettingContract", "type": "address"},
        {"name": "minOdds", "type": "uint64"},
    ],
    "CashOutOrder": [
        {"name": "attention", "type": "string"},
        {"name": "chainId", "type": "uint256"},
        {"name": "items", "type": "CashOutItem[]"},
        {"name": "expiresAt", "type": "uint64"},
    ],
}


# ---------------------------------------------------------------------------
# Amounts
# ---------------------------------------------------------------------------

def odds_raw(odds: Decimal | str | float) -> int:
    """Decimal odds as the protocol's 12-decimal fixed point, exactly."""
    return int((Decimal(str(odds)) * ODDS_SCALE).to_integral_value(rounding=ROUND_DOWN))


def odds_of_raw(raw: int | str) -> Decimal:
    return Decimal(int(raw)) / ODDS_SCALE


def min_odds(price: Decimal) -> int:
    """The worst odds a bet at this price accepts: `1 / price`, rounded up
    to the protocol's 12 decimals, so the price paid is never above `price`."""
    price = Decimal(str(price))
    if not Decimal("0") < price < Decimal("1"):
        raise InvalidOrder(f"azuro: price {price} must be strictly between 0 and 1")
    return int((Decimal(ODDS_SCALE) / price).to_integral_value(rounding=ROUND_CEILING))


def stake(contracts: Decimal, price: Decimal, decimals: int) -> int:
    """The stake, in the bet token's base units, that buys `contracts` at
    `price`: `contracts * price`, rounded down. At better odds than `price`
    the same stake returns more contracts."""
    amount = int((Decimal(str(contracts)) * Decimal(str(price)) * (Decimal(10) ** decimals))
                 .to_integral_value(rounding=ROUND_DOWN))
    if amount <= 0:
        raise InvalidOrder(f"azuro: {contracts} contracts at {price} stakes nothing")
    return amount


def token_amount(raw: int | str, decimals: int) -> Decimal:
    return Decimal(int(raw)) / (Decimal(10) ** decimals)


# ---------------------------------------------------------------------------
# EIP-712
# ---------------------------------------------------------------------------

def _encode_type(primary: str, types: dict[str, list[dict[str, str]]]) -> str:
    """`Primary(fields)` followed by every referenced struct, sorted by name."""
    found: set[str] = set()

    def walk(name: str) -> None:
        if name in found or name not in types:
            return
        found.add(name)
        for field in types[name]:
            walk(field["type"].split("[")[0])

    walk(primary)
    order = [primary] + sorted(found - {primary})
    return "".join(f"{name}({','.join(f['type'] + ' ' + f['name'] for f in types[name])})" for name in order)


def _encode_value(kind: str, value: Any, types: dict[str, list[dict[str, str]]]) -> bytes:
    abi_encode, _, _, keccak, to_checksum_address = _eth()
    if kind.endswith("]"):
        inner = kind[:kind.rindex("[")]
        return keccak(b"".join(_encode_value(inner, item, types) for item in value))
    if kind in types:
        return hash_struct(kind, types, value)
    if kind == "string":
        return keccak(text=str(value))
    if kind == "bytes":
        return keccak(bytes.fromhex(str(value).removeprefix("0x")) if isinstance(value, str) else bytes(value))
    if kind == "address":
        return abi_encode(["address"], [to_checksum_address(value)])
    if kind == "bool":
        return abi_encode(["bool"], [bool(value)])
    return abi_encode([kind], [int(value)])


def hash_struct(primary: str, types: dict[str, list[dict[str, str]]], data: dict[str, Any]) -> bytes:
    _, _, _, keccak, _ = _eth()
    type_hash = keccak(text=_encode_type(primary, types))
    return keccak(type_hash + b"".join(_encode_value(f["type"], data[f["name"]], types) for f in types[primary]))


def domain_separator(name: str, version: str, chain_id: int, contract: str) -> bytes:
    return hash_struct("EIP712Domain", {"EIP712Domain": DOMAIN_FIELDS},
                       {"name": name, "version": version, "chainId": chain_id, "verifyingContract": contract})


def digest(domain: dict[str, Any], primary: str, types: dict[str, list[dict[str, str]]], message: dict[str, Any]) -> bytes:
    _, _, _, keccak, _ = _eth()
    separator = domain_separator(domain["name"], domain["version"], int(domain["chainId"]), domain["verifyingContract"])
    return keccak(b"\x19\x01" + separator + hash_struct(primary, types, message))


# ---------------------------------------------------------------------------
# Bets
# ---------------------------------------------------------------------------

def client_data(
    chain: Chain, *, affiliate: str, expires_at: int, relayer_fee: int | str, attention: str = BET_ATTENTION,
) -> dict[str, Any]:
    """The `ClientData` of a bet, in the shape the API takes it back:
    numbers for the expiry and the chain id, a decimal string for the fee."""
    _, _, _, _, to_checksum_address = _eth()
    return {
        "attention": attention,
        "affiliate": to_checksum_address(affiliate),
        "core": to_checksum_address(chain.core),
        "expiresAt": int(expires_at),
        "chainId": chain.chain_id,
        "relayerFeeAmount": str(int(relayer_fee)),
        "isBetSponsored": False,
        "isFeeSponsored": False,
        "isSponsoredBetReturnable": False,
    }


def sub_bet(*, condition_id: str, outcome_id: str | int, min_odds: int, amount: int, nonce: int) -> dict[str, Any]:
    return {"conditionId": str(condition_id), "outcomeId": str(outcome_id), "minOdds": str(int(min_odds)),
            "amount": str(int(amount)), "nonce": str(int(nonce))}


def bet_domain(data: dict[str, Any]) -> dict[str, Any]:
    return {"name": BET_DOMAIN_NAME, "version": BET_DOMAIN_VERSION, "chainId": int(data["chainId"]),
            "verifyingContract": data["core"]}


def bet_message(data: dict[str, Any], bet: dict[str, Any]) -> dict[str, Any]:
    return {
        "clientData": {
            **{k: data[k] for k in ("attention", "affiliate", "core", "isFeeSponsored", "isBetSponsored",
                                    "isSponsoredBetReturnable")},
            "expiresAt": int(data["expiresAt"]), "chainId": int(data["chainId"]),
            "relayerFeeAmount": int(data["relayerFeeAmount"]),
        },
        "bets": [{k: int(bet[k]) for k in ("conditionId", "outcomeId", "minOdds", "amount", "nonce")}],
    }


def bet_typed_data(data: dict[str, Any], bet: dict[str, Any]) -> dict[str, Any]:
    """The bet as EIP-712 typed data, for wallets that sign that."""
    return {
        "primaryType": "ClientBetData",
        "types": {"EIP712Domain": DOMAIN_FIELDS, **BET_TYPES},
        "domain": bet_domain(data),
        "message": bet_message(data, bet),
    }


def bet_digest(data: dict[str, Any], bet: dict[str, Any]) -> bytes:
    return digest(bet_domain(data), "ClientBetData", BET_TYPES, bet_message(data, bet))


def sign_bet(signer: WalletSigner, data: dict[str, Any], bet: dict[str, Any]) -> str:
    """The bettor's signature over the bet, `0x`-hex."""
    return "0x" + bytes(signer.sign_digest(bet_digest(data, bet))).hex()


def wire_bet(chain: Chain, *, bettor: str, data: dict[str, Any], bet: dict[str, Any], signature: str) -> dict[str, Any]:
    """The signed bet as `POST /bet/orders/ordinar` takes it."""
    return {
        "environment": chain.environment,
        "bettor": bettor.lower(),
        "betOwner": bettor.lower(),
        "clientBetData": {
            "clientData": data,
            "bet": {**bet, "outcomeId": int(bet["outcomeId"])},
        },
        "bettorSignature": signature,
    }


# ---------------------------------------------------------------------------
# Cash-out
# ---------------------------------------------------------------------------

def cashout_domain(chain: Chain) -> dict[str, Any]:
    return {"name": CASHOUT_DOMAIN_NAME, "version": CASHOUT_DOMAIN_VERSION, "chainId": chain.chain_id,
            "verifyingContract": chain.cashout}


def cashout_message(chain: Chain, *, bet_id: int | str, cashout_odds: str | Decimal, expires_at: int,
                    attention: str = CASHOUT_ATTENTION) -> dict[str, Any]:
    return {
        "attention": attention,
        "chainId": chain.chain_id,
        "items": [{"betId": int(bet_id), "bettingContract": chain.core, "minOdds": odds_raw(cashout_odds)}],
        "expiresAt": int(expires_at),
    }


def cashout_typed_data(chain: Chain, message: dict[str, Any]) -> dict[str, Any]:
    return {"primaryType": "CashOutOrder", "types": {"EIP712Domain": DOMAIN_FIELDS, **CASHOUT_TYPES},
            "domain": cashout_domain(chain), "message": message}


def cashout_digest(chain: Chain, message: dict[str, Any]) -> bytes:
    return digest(cashout_domain(chain), "CashOutOrder", CASHOUT_TYPES, message)


def sign_cashout(signer: WalletSigner, chain: Chain, message: dict[str, Any]) -> str:
    return "0x" + bytes(signer.sign_digest(cashout_digest(chain, message))).hex()


def wire_cashout(chain: Chain, *, calculation_id: str, attention: str, signature: str) -> dict[str, Any]:
    """The signed cash-out as `POST /cashout/create` takes it."""
    return {
        "calculationId": calculation_id,
        "signature": {
            "verifyingContract": chain.cashout, "bettingContract": chain.core, "attention": attention,
            "chainId": chain.chain_id, "ownerSignature": signature,
        },
    }


# ---------------------------------------------------------------------------
# Wallet calls
# ---------------------------------------------------------------------------

def approve_calldata(spender: str, amount: int) -> str:
    abi_encode, _, _, keccak, _ = _eth()
    return "0x" + (keccak(text="approve(address,uint256)")[:4] + abi_encode(["address", "uint256"], [spender, amount])).hex()


def allowance_calldata(owner: str, spender: str) -> str:
    abi_encode, _, _, keccak, _ = _eth()
    return "0x" + (keccak(text="allowance(address,address)")[:4] + abi_encode(["address", "address"], [owner, spender])).hex()


def balance_calldata(owner: str) -> str:
    abi_encode, _, _, keccak, _ = _eth()
    return "0x" + (keccak(text="balanceOf(address)")[:4] + abi_encode(["address"], [owner])).hex()
