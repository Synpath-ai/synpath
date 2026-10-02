//! One side of a market's order book, kept up to date from a stream.
//!
//! The Rust twin of `synpath.ws.base.LocalBook`, with the same rules:
//!
//! * a level with size zero or less is not a level: it is removed, never stored;
//! * prices are exact decimals, never floats, so `0.42` from the venue is the
//!   key `0.42` and a delta to it lands on the same level;
//! * `ready` is false until a snapshot has been applied, and a stream sets it
//!   false again when it loses confidence in the book (a gap, a reconnect).
//!
//! Levels are kept sorted (`BTreeMap`), so the best bid and ask are read
//! without scanning every level, and a full depth walk needs no sort.

use std::collections::BTreeMap;

use rust_decimal::Decimal;

/// One price level: `(price, size)`.
pub type Level = (Decimal, Decimal);

/// Which side of the book a level rests on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    /// `"bid"` or `"ask"`, the words the Python API uses.
    pub fn parse(word: &str) -> Option<Side> {
        match word {
            "bid" => Some(Side::Bid),
            "ask" => Some(Side::Ask),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Book {
    pub bids: BTreeMap<Decimal, Decimal>,
    pub asks: BTreeMap<Decimal, Decimal>,
    pub ready: bool,
}

impl Book {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn levels_mut(&mut self, side: Side) -> &mut BTreeMap<Decimal, Decimal> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }

    pub fn levels_of(&self, side: Side) -> &BTreeMap<Decimal, Decimal> {
        match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        }
    }

    /// Replace both sides with a snapshot and mark the book ready. Levels of
    /// size zero or less in the snapshot are dropped; a price listed twice
    /// keeps its last size.
    pub fn replace<I, J>(&mut self, bids: I, asks: J)
    where
        I: IntoIterator<Item = (Decimal, Decimal)>,
        J: IntoIterator<Item = (Decimal, Decimal)>,
    {
        self.bids = positive(bids);
        self.asks = positive(asks);
        self.ready = true;
    }

    /// Set a level to `size`, removing it when `size` is not positive.
    /// Returns the level's size after the change: zero when removed.
    pub fn set(&mut self, side: Side, price: Decimal, size: Decimal) -> Decimal {
        let levels = self.levels_mut(side);
        if size > Decimal::ZERO {
            levels.insert(price, size);
            size
        } else {
            levels.remove(&price);
            Decimal::ZERO
        }
    }

    /// Change a level by `delta`; the result is `set` with the new size.
    pub fn add(&mut self, side: Side, price: Decimal, delta: Decimal) -> Decimal {
        let current = self
            .levels_of(side)
            .get(&price)
            .copied()
            .unwrap_or(Decimal::ZERO);
        self.set(side, price, current + delta)
    }

    pub fn invalidate(&mut self) {
        self.ready = false;
    }

    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.keys().next().copied()
    }

    /// Bids best first (descending), asks best first (ascending), at most
    /// `depth` of each.
    pub fn levels(&self, depth: Option<usize>) -> (Vec<Level>, Vec<Level>) {
        let take = depth.unwrap_or(usize::MAX);
        let bids = self
            .bids
            .iter()
            .rev()
            .take(take)
            .map(|(p, s)| (*p, *s))
            .collect();
        let asks = self.asks.iter().take(take).map(|(p, s)| (*p, *s)).collect();
        (bids, asks)
    }

    /// The other side's view of the same book: each ask at `p` is a bid at
    /// `face_value - p`, and each bid an ask. Readiness carries over.
    pub fn mirrored(&self, face_value: Decimal) -> Book {
        Book {
            bids: self
                .asks
                .iter()
                .map(|(p, s)| (face_value - *p, *s))
                .collect(),
            asks: self
                .bids
                .iter()
                .map(|(p, s)| (face_value - *p, *s))
                .collect(),
            ready: self.ready,
        }
    }
}

fn positive<I: IntoIterator<Item = (Decimal, Decimal)>>(levels: I) -> BTreeMap<Decimal, Decimal> {
    let mut out = BTreeMap::new();
    for (price, size) in levels {
        if size > Decimal::ZERO {
            out.insert(price, size);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(text: &str) -> Decimal {
        Decimal::from_str(text).unwrap()
    }

    #[test]
    fn levels_best_and_removal() {
        let mut book = Book::new();
        assert!(!book.ready && book.best_bid().is_none());
        book.replace(
            [
                (d("0.40"), d("10")),
                (d("0.42"), d("5")),
                (d("0.30"), d("0")),
            ],
            [(d("0.45"), d("7"))],
        );
        assert!(book.ready);
        assert_eq!(book.best_bid(), Some(d("0.42")));
        assert_eq!(book.best_ask(), Some(d("0.45")));
        assert!(!book.bids.contains_key(&d("0.30")));
        assert_eq!(book.add(Side::Bid, d("0.42"), d("-5")), Decimal::ZERO);
        assert_eq!(book.best_bid(), Some(d("0.40")));
        assert_eq!(book.set(Side::Ask, d("0.44"), d("3")), d("3"));
        let (bids, asks) = book.levels(None);
        assert_eq!(bids, vec![(d("0.40"), d("10"))]);
        assert_eq!(asks, vec![(d("0.44"), d("3")), (d("0.45"), d("7"))]);
    }

    #[test]
    fn mirrored_view() {
        let mut book = Book::new();
        book.replace([(d("0.40"), d("10"))], [(d("0.45"), d("7"))]);
        let no = book.mirrored(Decimal::ONE);
        assert_eq!(no.best_bid(), Some(d("0.55")));
        assert_eq!(no.best_ask(), Some(d("0.60")));
        assert_eq!(no.bids.get(&d("0.55")), Some(&d("7")));
        assert!(no.ready);
    }

    #[test]
    fn equal_prices_at_different_scales_are_one_level() {
        // The venue may send 0.4 in a snapshot and 0.40 in a delta.
        let mut book = Book::new();
        book.replace([(d("0.4"), d("10"))], []);
        book.add(Side::Bid, d("0.40"), d("-4"));
        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.bids.get(&d("0.4")), Some(&d("6")));
    }

    #[test]
    fn depth_limits_each_side() {
        let mut book = Book::new();
        book.replace(
            [(d("0.1"), d("1")), (d("0.2"), d("1")), (d("0.3"), d("1"))],
            [(d("0.7"), d("1")), (d("0.8"), d("1"))],
        );
        let (bids, asks) = book.levels(Some(2));
        assert_eq!(
            bids.iter().map(|l| l.0).collect::<Vec<_>>(),
            vec![d("0.3"), d("0.2")]
        );
        assert_eq!(
            asks.iter().map(|l| l.0).collect::<Vec<_>>(),
            vec![d("0.7"), d("0.8")]
        );
    }

    #[test]
    fn a_duplicate_price_in_a_snapshot_keeps_its_last_size() {
        let mut book = Book::new();
        book.replace([(d("0.4"), d("1")), (d("0.4"), d("9"))], []);
        assert_eq!(book.bids.get(&d("0.4")), Some(&d("9")));
    }
}
