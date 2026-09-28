"""Money as `Decimal`, and the venues' wire formats for it.

The read API carries prices as `float`, the ccxt convention, and a float is
fine to look at. It is not fine to sign: `0.1 + 0.2` is not `0.3`, a tick of
`0.001` cannot be represented exactly, and a price that rounds to the wrong
side of a tick is an order the venue rejects at best and fills at the wrong
level at worst. Everything that will be signed goes through here as
`Decimal`, is checked against the instrument's precision, and is rendered
into the venue's own string format only at the boundary.

Wire formats seen on the three venues:

  Kalshi          fixed-point dollar strings, four decimals ("0.5600"), and
                  fixed-point contract counts, two decimals ("10.00")
  Polymarket      token and collateral amounts as integers scaled by 10^6;
                  prices as decimal strings on the instrument's tick
  Polymarket US   decimal strings, four decimals, whole contracts only
"""
from __future__ import annotations

from decimal import ROUND_DOWN, ROUND_HALF_EVEN, ROUND_HALF_UP, ROUND_UP, Decimal, InvalidOperation
from typing import Any, Literal

from .errors import InvalidOrder
from .types import Precision

Rounding = Literal["nearest", "down", "up", "bankers"]
_MODES = {
    "nearest": ROUND_HALF_UP,
    "down": ROUND_DOWN,
    "up": ROUND_UP,
    "bankers": ROUND_HALF_EVEN,
}

ONE = Decimal("1")


def D(value: Any) -> Decimal:
    """A `Decimal` from anything a caller might reasonably hand over.

    Floats go through `str` first, so `D(0.1)` is `0.1` and not
    `0.1000000000000000055511151231257827`. `None` is refused rather than
    read as zero: an absent price is not a free order.
    """
    if isinstance(value, Decimal):
        return value
    if value is None or value == "":
        raise InvalidOrder("a Decimal is required, got None")
    if isinstance(value, bool):
        raise InvalidOrder(f"a Decimal is required, got {value!r}")
    if isinstance(value, float):
        value = repr(value)
    try:
        return Decimal(str(value))
    except InvalidOperation:
        raise InvalidOrder(f"not a number: {value!r}") from None


def round_to(value: Decimal, step: Decimal, mode: Rounding = "nearest") -> Decimal:
    """`value` on the grid of `step`, rounded as asked.

    `nearest` is what a human expects; `down` is what a buyer wants (never
    pay more than typed) and `up` what a seller wants; `bankers` is how
    Polymarket US rounds fees.
    """
    if step <= 0:
        raise InvalidOrder(f"step must be positive, got {step}")
    quotient = (value / step).quantize(ONE, rounding=_MODES[mode])
    return (quotient * step).quantize(step)


def complement(price: Decimal, *, face_value: Decimal = ONE) -> Decimal:
    """The same order seen from the other side: a YES bid at p is a NO ask at
    `face_value - p`."""
    return face_value - price


def validate_price(price: Decimal, precision: Precision) -> Decimal:
    """`price`, or `InvalidOrder` saying exactly why the venue would refuse it.

    A price is checked, never silently rounded: an order typed at 0.1065 on a
    0.001 tick is a mistake to point out, not a 0.107 to place.
    """
    tick = precision.tick
    if price <= 0 or price >= precision.face_value:
        raise InvalidOrder(
            f"price {price} is outside (0, {precision.face_value}); the ends are "
            f"placeholders, not prices"
        )
    if (price / tick) % 1 != 0:
        raise InvalidOrder(f"price {price} is not on the {tick} tick; nearest are "
                           f"{round_to(price, tick, 'down')} and {round_to(price, tick, 'up')}")
    return price


def validate_amount(amount: Decimal, precision: Precision) -> Decimal:
    """`amount`, or `InvalidOrder` naming the rule it breaks."""
    if amount <= 0:
        raise InvalidOrder(f"amount must be positive, got {amount}")
    if precision.whole_contracts and amount % 1 != 0:
        raise InvalidOrder(f"amount {amount}: this venue trades whole contracts only")
    if amount < precision.min_amount:
        raise InvalidOrder(f"amount {amount} is below the minimum {precision.min_amount}")
    step = precision.amount_step
    if step and (amount / step) % 1 != 0:
        raise InvalidOrder(f"amount {amount} is not a multiple of {step}")
    return amount


# -- Kalshi --------------------------------------------------------------------

KALSHI_PRICE_PLACES = Decimal("0.0001")
KALSHI_COUNT_PLACES = Decimal("0.01")


def kalshi_dollars(price: Decimal) -> str:
    """A price as Kalshi V2's fixed-point dollar string: `Decimal("0.56")` -> `"0.5600"`."""
    return str(price.quantize(KALSHI_PRICE_PLACES))


def kalshi_count(amount: Decimal) -> str:
    """A contract count as Kalshi's `count` string: `Decimal("10")` -> `"10.00"`."""
    return str(amount.quantize(KALSHI_COUNT_PLACES))


def from_kalshi_cents(cents: Any) -> Decimal:
    """The legacy integer-cents price as a dollar `Decimal`: `56` -> `0.56`."""
    return (D(cents) / 100).quantize(KALSHI_PRICE_PLACES)


def from_kalshi_dollars(text: Any) -> Decimal:
    return D(text)


# -- Polymarket ----------------------------------------------------------------

POLY_SCALE = Decimal(10) ** 6


def poly_raw_amount(amount: Decimal) -> int:
    """A share or USDC amount as the on-chain integer, six decimals: `1.5` -> `1500000`.

    Rounded down: a maker amount rounded up is an order for more than the
    caller has.
    """
    return int((amount * POLY_SCALE).quantize(ONE, rounding=ROUND_DOWN))


def from_poly_raw_amount(raw: Any) -> Decimal:
    return (D(raw) / POLY_SCALE).quantize(Decimal("0.000001"))


def poly_price(price: Decimal, tick: Decimal) -> str:
    """A price as the CLOB's decimal string, on the instrument's tick."""
    return str(price.quantize(tick))


# -- Polymarket US -------------------------------------------------------------

POLYUS_PLACES = Decimal("0.0001")


def polyus_amount(value: Decimal) -> str:
    """A price or quantity as the gateway's four-place decimal string."""
    return str(value.quantize(POLYUS_PLACES))


def from_polyus_amount(value: Any) -> Decimal:
    """The gateway's `{"value": "0.1060", "currency": "USD"}` or a bare string."""
    if isinstance(value, dict):
        value = value.get("value")
    return D(value)
