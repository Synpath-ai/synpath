"""SX Bet order signing and amounts, with no network in sight.

**Orders.** An order is the eight-field `Order` struct (marketHash, baseToken,
totalBetSize, percentageOdds, salt, expiry, maker, isMakerBettingOutcomeOne)
under the domain the venue publishes at `GET /metadata/obv3` (`OBv3 Escrow`,
version `1`, the chain's id, the escrow as the verifying contract). The wallet
signs it; the order's id is the EIP-712 digest itself, lowercased, so it is
known before the order is sent. The maker is the wallet, never the proxy that
holds the funds.

**Amounts.** An order bets one outcome: `percentageOdds` is the implied
probability it pays for that outcome (1e20 = 100%) and `totalBetSize` the
USDC it stakes (6 decimals). One contract pays $1, so a stake buys
`stake / odds` contracts. Odds must sit on the venue's ladder (0.125% steps
today, read from the metadata); they are rounded towards the cheaper price, so
an order never pays more than the price it was given.
"""
from __future__ import annotations

import secrets
from decimal import ROUND_DOWN, Decimal
from typing import Any

from .errors import InvalidOrder
from .polymarket_signing import WalletSigner, _eth
from .predict_fun_signing import _domain_separator

ODDS_SCALE = 10 ** 20
USDC = 10 ** 6
LADDER_UNIT = 10 ** 15
"""`oddsLadderStepSize` is counted in these: 125 is 0.125%."""
LADDER_EXCEPTIONS = frozenset({10 ** 17, 999 * 10 ** 17})
"""0.1% and 99.9% are always on the ladder, whatever its step."""

ORDER_TYPE = (
    "Order(bytes32 marketHash,address baseToken,uint256 totalBetSize,uint256 percentageOdds,uint256 salt,"
    "uint256 expiry,address maker,bool isMakerBettingOutcomeOne)"
)


def odds_units(price: Decimal) -> int:
    """A probability in [0, 1] as 1e20-scaled odds, exactly."""
    return int((Decimal(str(price)) * ODDS_SCALE).to_integral_value(rounding=ROUND_DOWN))


def on_ladder(odds: int, step_size: int) -> bool:
    if not 0 < odds < ODDS_SCALE:
        return False
    return odds in LADDER_EXCEPTIONS or odds % (step_size * LADDER_UNIT) == 0


def round_down(odds: int, step_size: int) -> int:
    """Onto the ladder, the cheaper way: an order never pays more than asked."""
    step = step_size * LADDER_UNIT
    return odds // step * step


def bet(outcome_one: bool, yes_price: Decimal, contracts: Decimal, *, step_size: int, min_stake: int) -> tuple[int, int]:
    """`(percentageOdds, totalBetSize)` for `contracts` on one outcome at the
    YES price given: outcome one pays the YES price, outcome two one minus it.
    The odds are snapped down onto the ladder and the stake cut to whole base
    units, so the contracts bought are never more than asked."""
    yes_price = Decimal(str(yes_price))
    if not Decimal("0") < yes_price < Decimal("1"):
        raise InvalidOrder(f"sx_bet: price {yes_price} must be strictly between 0 and 1")
    paid = yes_price if outcome_one else Decimal("1") - yes_price
    odds = round_down(odds_units(paid), step_size)
    if not on_ladder(odds, step_size):
        raise InvalidOrder(f"sx_bet: price {yes_price} rounds to no price on the venue's ladder")
    stake = int((Decimal(str(contracts)) * odds * USDC / ODDS_SCALE).to_integral_value(rounding=ROUND_DOWN))
    if stake < min_stake:
        raise InvalidOrder(
            f"sx_bet: {contracts} contracts at {paid} stakes {Decimal(stake) / USDC} USDC; the venue's minimum is "
            f"{Decimal(min_stake) / USDC} USDC"
        )
    return odds, stake


def contracts_of(stake: int, odds: int) -> Decimal:
    """The contracts a stake buys at its odds: `stake / odds`, in whole dollars."""
    if not odds:
        return Decimal("0")
    return (Decimal(stake) * ODDS_SCALE / Decimal(odds) / USDC).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)


def new_salt() -> int:
    return int.from_bytes(secrets.token_bytes(32), "big")


def build_order(
    *, market_hash: str, base_token: str, stake: int, odds: int, expiry: int, maker: str,
    outcome_one: bool, salt: int,
) -> dict[str, Any]:
    """The order struct's values, before the signature."""
    _, _, _, _, to_checksum_address = _eth()
    return {
        "marketHash": market_hash.lower(),
        "baseToken": to_checksum_address(base_token),
        "totalBetSize": int(stake),
        "percentageOdds": int(odds),
        "salt": int(salt),
        "expiry": int(expiry),
        "maker": to_checksum_address(maker),
        "isMakerBettingOutcomeOne": bool(outcome_one),
    }


def _struct_hash(order: dict[str, Any]) -> bytes:
    abi_encode, _, _, keccak, to_checksum_address = _eth()
    return keccak(abi_encode(
        ["bytes32", "bytes32", "address", "uint256", "uint256", "uint256", "uint256", "address", "bool"],
        [
            keccak(text=ORDER_TYPE), bytes.fromhex(order["marketHash"][2:].rjust(64, "0")),
            to_checksum_address(order["baseToken"]), int(order["totalBetSize"]), int(order["percentageOdds"]),
            int(order["salt"]), int(order["expiry"]), to_checksum_address(order["maker"]),
            bool(order["isMakerBettingOutcomeOne"]),
        ],
    ))


def order_digest(order: dict[str, Any], *, domain: dict[str, Any]) -> bytes:
    _, _, _, keccak, _ = _eth()
    separator = _domain_separator(str(domain["name"]), str(domain["version"]), int(domain["chainId"]),
                                  str(domain["verifyingContract"]))
    return keccak(b"\x19\x01" + separator + _struct_hash(order))


def order_id(order: dict[str, Any], *, domain: dict[str, Any]) -> str:
    """The id the venue gives the order: its digest, lowercased."""
    return "0x" + order_digest(order, domain=domain).hex()


def order_typed_data(order: dict[str, Any], *, domain: dict[str, Any]) -> dict[str, Any]:
    """The same order as EIP-712 typed data, for wallets that sign that."""
    from eth_utils import to_checksum_address

    fields = [p.split(" ") for p in ORDER_TYPE[len("Order("):-1].split(",")]
    return {
        "primaryType": "Order",
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"}, {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"}, {"name": "verifyingContract", "type": "address"},
            ],
            "Order": [{"name": name, "type": kind} for kind, name in fields],
        },
        "domain": {"name": domain["name"], "version": domain["version"], "chainId": int(domain["chainId"]),
                   "verifyingContract": to_checksum_address(domain["verifyingContract"])},
        "message": dict(order),
    }


def sign_order(signer: WalletSigner, order: dict[str, Any], *, domain: dict[str, Any]) -> str:
    """The wallet's signature over the order, `0x`-hex."""
    return "0x" + bytes(signer.sign_digest(order_digest(order, domain=domain))).hex()


def wire_order(order: dict[str, Any], *, signature: str, time_in_force: str) -> dict[str, Any]:
    """The signed order as `POST /orders-v3` takes it: the three amounts and
    the salt as decimal strings, the expiry a number."""
    return {
        "marketHash": order["marketHash"],
        "maker": order["maker"],
        "totalBetSize": str(order["totalBetSize"]),
        "percentageOdds": str(order["percentageOdds"]),
        "salt": str(order["salt"]),
        "expiry": int(order["expiry"]),
        "baseToken": order["baseToken"],
        "isMakerBettingOutcomeOne": bool(order["isMakerBettingOutcomeOne"]),
        "timeInForce": time_in_force,
        "orderSignature": signature,
    }
