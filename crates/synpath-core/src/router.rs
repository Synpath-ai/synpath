//! The router's merged book and its walk, as `synpath.engine.router` runs them.
//!
//! `merge` lays the members' levels side by side in bucket terms and sorts
//! them by net price; `walk` takes them best first up to the limit and
//! reports, per member, what it took. Turning that into legs -- rounding to
//! the venue's step and tick, fee floors, the per-leg division -- stays in
//! Python, where every division rounds as Python's `Decimal` rounds it.
//!
//! The arithmetic here is only additions, subtractions and multiplications,
//! and each is checked to be exact the way Python's default `Decimal`
//! context sees it: a result needing more than 28 significant digits is
//! rounded there, so here it is `None`, and the caller plans in Python
//! instead. On every book a venue actually publishes the numbers are short
//! and nothing falls back.

use rust_decimal::Decimal;

/// Python's default `Decimal` precision.
const PRECISION: u32 = 28;

fn digits(value: &Decimal) -> u32 {
    let mantissa = value.mantissa().unsigned_abs();
    if mantissa == 0 {
        1
    } else {
        mantissa.ilog10() + 1
    }
}

fn fits(value: Decimal) -> Option<Decimal> {
    (digits(&value) <= PRECISION).then_some(value)
}

/// A zero with Python's exponent and sign for an exact result.
fn zero(scale: u32, negative: bool) -> Option<Decimal> {
    let mut value = Decimal::try_new(0, scale).ok()?;
    value.set_sign_negative(negative);
    Some(value)
}

/// `a + b`, if Python computes it exactly (and so gets the same digits).
/// Python keeps the smaller exponent, also on a zero result, which is
/// negative only when both operands are.
pub fn add(a: Decimal, b: Decimal) -> Option<Decimal> {
    let scale = a.scale().max(b.scale());
    let sum = a.checked_add(b)?;
    if sum.is_zero() {
        return zero(scale, a.is_sign_negative() && b.is_sign_negative());
    }
    (sum.scale() == scale).then_some(sum).and_then(fits)
}

/// `a - b`, under the same condition.
pub fn sub(a: Decimal, b: Decimal) -> Option<Decimal> {
    add(a, -b)
}

/// `a * b`, under the same condition. Python's exponent is the sum of the
/// operands', a zero product included; its sign is negative when theirs differ.
pub fn mul(a: Decimal, b: Decimal) -> Option<Decimal> {
    let scale = a.scale() + b.scale();
    let product = a.checked_mul(b)?;
    if product.is_zero() {
        return zero(scale, a.is_sign_negative() != b.is_sign_negative());
    }
    (product.scale() == scale).then_some(product).and_then(fits)
}

#[derive(Debug, Clone, PartialEq)]
pub struct Level {
    /// Index of the member in the bucket's order.
    pub member: usize,
    pub price: Decimal,
    /// `price` plus the per-contract taker fee on a buy, minus it on a sell.
    pub net: Decimal,
    pub size: Decimal,
}

/// Best first: asks cheapest-net first, bids richest-net first; ties keep
/// the order they came in, as Python's stable sort does.
pub fn sort(levels: &mut [Level], buy: bool) {
    if buy {
        levels.sort_by_key(|level| (level.net, level.price));
    } else {
        levels.sort_by_key(|level| std::cmp::Reverse((level.net, level.price)));
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Slot {
    pub member: usize,
    pub amount: Decimal,
    /// Sum of net price times contracts.
    pub cost: Decimal,
    /// Sum of the per-contract fee times contracts.
    pub fee: Decimal,
    /// The worst bucket-terms price reached, before fees.
    pub worst: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    Filled,
    WorstPrice,
    Liquidity,
}

impl Reason {
    /// The word `Plan.reason` uses.
    pub fn word(self) -> &'static str {
        match self {
            Reason::Filled => "",
            Reason::WorstPrice => "worst_price",
            Reason::Liquidity => "liquidity",
        }
    }
}

/// `_walk`'s loop: what each member's levels give, best first, up to
/// `amount` and no worse than `limit` net. Slots come in the order members
/// were first taken from. `None` when the arithmetic would not be exact.
pub fn walk(
    levels: &[Level],
    buy: bool,
    amount: Decimal,
    limit: Decimal,
    excluded: &[bool],
) -> Option<(Vec<Slot>, Decimal, Reason)> {
    let mut slots: Vec<Slot> = Vec::new();
    let mut remaining = amount;
    let mut reason = Reason::Liquidity;
    let mut stopped = false;
    for level in levels {
        if remaining <= Decimal::ZERO {
            reason = Reason::Filled;
            stopped = true;
            break;
        }
        if excluded.get(level.member).copied().unwrap_or(false) {
            continue;
        }
        if (buy && level.net > limit) || (!buy && level.net < limit) {
            reason = Reason::WorstPrice;
            stopped = true;
            break;
        }
        // `min(level.size, remaining)`: the size when they are equal.
        let take = if remaining < level.size {
            remaining
        } else {
            level.size
        };
        let index = match slots.iter().position(|slot| slot.member == level.member) {
            Some(index) => index,
            None => {
                slots.push(Slot {
                    member: level.member,
                    amount: Decimal::ZERO,
                    cost: Decimal::ZERO,
                    fee: Decimal::ZERO,
                    worst: level.price,
                });
                slots.len() - 1
            }
        };
        let slot = &mut slots[index];
        slot.amount = add(slot.amount, take)?;
        slot.cost = add(slot.cost, mul(level.net, take)?)?;
        slot.fee = add(slot.fee, mul(sub(level.net, level.price)?.abs(), take)?)?;
        // `max`/`min` keep the first argument on a tie, as Python's do.
        if (buy && level.price > slot.worst) || (!buy && level.price < slot.worst) {
            slot.worst = level.price;
        }
        remaining = sub(remaining, take)?;
    }
    if !stopped && remaining <= Decimal::ZERO {
        reason = Reason::Filled;
    }
    Some((slots, remaining, reason))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(text: &str) -> Decimal {
        Decimal::from_str(text).unwrap()
    }

    fn level(member: usize, price: &str, fee: &str, size: &str) -> Level {
        Level {
            member,
            price: d(price),
            net: d(price) + d(fee),
            size: d(size),
        }
    }

    #[test]
    fn exact_arithmetic_keeps_python_digits() {
        assert_eq!(add(d("0.40"), d("0.1")).unwrap().to_string(), "0.50");
        assert_eq!(mul(d("0.42"), d("10.0")).unwrap().to_string(), "4.200");
        // 29 significant digits: Python would round, so it is refused here.
        assert_eq!(mul(d("1.23456789012345"), d("1.23456789012345")), None);
        assert_eq!(mul(d("0"), d("0.10")).unwrap().to_string(), "0.00");
        assert_eq!(sub(d("0.10"), d("0.1")).unwrap().to_string(), "0.00");
    }

    #[test]
    fn walks_best_first_and_stops_at_the_limit() {
        let mut levels = vec![
            level(0, "0.42", "0.01", "10"),
            level(1, "0.41", "0", "5"),
            level(1, "0.50", "0", "100"),
        ];
        sort(&mut levels, true);
        assert_eq!(levels[0].member, 1);
        let (slots, remaining, reason) =
            walk(&levels, true, d("30"), d("0.45"), &[false, false]).unwrap();
        assert_eq!(reason, Reason::WorstPrice);
        assert_eq!(remaining, d("15"));
        assert_eq!(slots[0].member, 1);
        assert_eq!(slots[1].amount, d("10"));
        assert_eq!(slots[1].fee, d("0.10"));
    }

    #[test]
    fn an_excluded_member_is_skipped() {
        let levels = vec![level(0, "0.40", "0", "10"), level(1, "0.41", "0", "10")];
        let (slots, remaining, reason) =
            walk(&levels, true, d("10"), d("1"), &[true, false]).unwrap();
        assert_eq!(
            (slots.len(), slots[0].member, remaining, reason),
            (1, 1, d("0"), Reason::Filled)
        );
    }
}
