//! Polymarket's market channel, book messages only, applied in Rust.
//!
//! `polymarket_market` does what `PolymarketMarketStream._on_book` and
//! `_on_price_change` do -- parse, apply to the stream's books, check the
//! venue's top of book -- on the same `LocalBook` objects and the same
//! `_unverified` dict, so the Python handler can take over at any message
//! and find the state exactly as it would have left it.
//!
//! It returns what the Python side still has to do, in order, as tuples:
//!
//! * `("gap", token, local_bid, local_ask, venue_bid, venue_ask)` -- the book
//!   is already invalidated; report the gap and resubscribe the token;
//! * `("resynced", token)` -- a book that had been lost has a fresh snapshot;
//! * `("book", token, condition, kind, bids, asks, best_bid, best_ask,
//!   timestamp, info)` -- one `BookEvent` to build (`info` is `None` for
//!   deltas).
//!
//! It returns `None`, having changed nothing, whenever the Python handler
//! should read the frame instead: the frame holds other messages, would make
//! the Python handler raise, or the state holds something only Python wrote.

use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use rust_decimal::Decimal;
use serde_json::Value;
use synpath_core::polymarket::{self, Message};
use synpath_core::wire;
use synpath_core::Level;

use crate::pyjson::to_py;
use crate::{level_tuple, optional_decimal, LocalBook};

/// How long a timestamp's changes wait for a later one before their top of
/// book is checked anyway (`_verify_older_than`'s window).
const WINDOW_MS: i64 = 500;

#[pyfunction]
pub(crate) fn polymarket_market<'py>(
    py: Python<'py>,
    raw: &str,
    books: &Bound<'py, PyDict>,
    unverified: &Bound<'py, PyDict>,
    markets: &Bound<'py, PyDict>,
) -> PyResult<Option<Bound<'py, PyList>>> {
    let messages = match polymarket::decode(raw) {
        Ok(Some(messages)) => messages,
        Ok(None) | Err(_) => return Ok(None),
    };
    if !state_is_ours(py, &messages, books, unverified)? {
        return Ok(None);
    }
    let mut run = Run {
        py,
        books,
        unverified,
        out: PyList::empty(py),
    };
    for message in &messages {
        match message {
            Message::Book(snapshot) => run.snapshot(snapshot, markets)?,
            Message::PriceChange(changes) => run.price_changes(changes)?,
        }
    }
    Ok(Some(run.out))
}

/// Whether every book this frame can touch is a Rust book, and every pending
/// top of book (old or arriving) is one `_agrees` can read. Checked before
/// anything changes, so a frame is applied whole or not at all.
fn state_is_ours(
    py: Python<'_>,
    messages: &[Message],
    books: &Bound<'_, PyDict>,
    unverified: &Bound<'_, PyDict>,
) -> PyResult<bool> {
    let mut tokens: Vec<Bound<'_, PyAny>> = Vec::new();
    for message in messages {
        match message {
            Message::Book(snapshot) => {
                tokens.push(PyString::new(py, &snapshot.asset_id).into_any())
            }
            Message::PriceChange(changes) => {
                for change in &changes.changes {
                    if venue_value(&change.best_bid).is_err()
                        || venue_value(&change.best_ask).is_err()
                    {
                        return Ok(false);
                    }
                    tokens.push(PyString::new(py, &change.asset_id).into_any());
                }
            }
        }
    }
    // Every pending entry is read for its timestamp; only those this frame
    // can check -- a token it changes, or one old enough for the window --
    // have their prices read too.
    let changed: std::collections::HashSet<&str> = messages
        .iter()
        .flat_map(|message| match message {
            Message::PriceChange(changes) => changes
                .changes
                .iter()
                .map(|c| c.asset_id.as_str())
                .collect(),
            Message::Book(_) => Vec::new(),
        })
        .collect();
    let newest = messages
        .iter()
        .filter_map(|message| match message {
            Message::PriceChange(changes) => changes.timestamp,
            Message::Book(_) => None,
        })
        .max();
    for (token, pending) in unverified.iter() {
        let Ok((stamp, bid, ask)) =
            pending.extract::<(Option<i64>, Bound<'_, PyAny>, Bound<'_, PyAny>)>()
        else {
            return Ok(false);
        };
        let Ok(name) = token.cast::<PyString>() else {
            return Ok(false);
        };
        let due = matches!((newest, stamp), (Some(now), Some(then)) if now - then > WINDOW_MS);
        if due || changed.contains(name.to_cow()?.as_ref()) {
            if venue_object(&bid).is_err() || venue_object(&ask).is_err() {
                return Ok(false);
            }
            tokens.push(token);
        }
    }
    for token in tokens {
        if let Some(book) = books.get_item(&token)? {
            if book.cast::<LocalBook>().is_err() {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// A venue top of book as `_agrees` reads it: `None` for absent (null or
/// `""`), else the price. `Err` where `Decimal(str(venue))` would raise.
fn venue_value(value: &Value) -> Result<Option<Decimal>, String> {
    match value {
        Value::Null => Ok(None),
        Value::String(text) if text.is_empty() => Ok(None),
        Value::String(text) => wire::parse_decimal(text).map(Some),
        Value::Number(number) => wire::parse_decimal(&number.to_string()).map(Some),
        other => Err(format!("not a price: {other}")),
    }
}

fn venue_object(value: &Bound<'_, PyAny>) -> Result<Option<Decimal>, String> {
    if value.is_none() {
        return Ok(None);
    }
    if let Ok(text) = value.cast::<PyString>() {
        let text = text.to_cow().map_err(|e| e.to_string())?;
        if text.is_empty() {
            return Ok(None);
        }
        return wire::parse_decimal(&text).map(Some);
    }
    if value.is_instance_of::<pyo3::types::PyBool>() {
        return Err("a bool is not a price".to_string());
    }
    let text = value.str().map_err(|e| e.to_string())?;
    wire::parse_decimal(&text.to_cow().map_err(|e| e.to_string())?).map(Some)
}

/// `_agrees`: a venue top of book of `0`, `1` or empty means no level.
fn agrees(local: Option<Decimal>, venue: Option<Decimal>) -> bool {
    match (venue, local) {
        (None, _) => true,
        (Some(price), None) => price == Decimal::ZERO || price == Decimal::ONE,
        (Some(price), Some(local)) => local == price,
    }
}

struct Run<'py, 'a> {
    py: Python<'py>,
    books: &'a Bound<'py, PyDict>,
    unverified: &'a Bound<'py, PyDict>,
    out: Bound<'py, PyList>,
}

impl<'py> Run<'py, '_> {
    fn book(&self, token: &Bound<'py, PyString>) -> PyResult<Option<Bound<'py, LocalBook>>> {
        match self.books.get_item(token)? {
            Some(book) => Ok(Some(book.cast_into::<LocalBook>()?)),
            None => Ok(None),
        }
    }

    fn snapshot(
        &mut self,
        snapshot: &polymarket::Snapshot,
        markets: &Bound<'py, PyDict>,
    ) -> PyResult<()> {
        let py = self.py;
        let token = PyString::new(py, &snapshot.asset_id);
        markets.set_item(&token, &snapshot.market)?;
        let book = match self.book(&token)? {
            Some(book) => book,
            None => {
                let book = Bound::new(py, LocalBook::new())?;
                self.books.set_item(&token, &book)?;
                book
            }
        };
        let (recovering, bids, asks, best_bid, best_ask) = {
            let mut local = book.borrow_mut();
            let recovering = !local.book.ready && local.timestamp.is_some();
            local
                .book
                .replace(snapshot.bids.iter().copied(), snapshot.asks.iter().copied());
            local.timestamp = stamp(py, snapshot.timestamp)?;
            let (bids, asks) = local.book.levels(None);
            (
                recovering,
                bids,
                asks,
                local.book.best_bid(),
                local.book.best_ask(),
            )
        };
        if self.unverified.contains(&token)? {
            self.unverified.del_item(&token)?;
        }
        if recovering {
            self.out.append(("resynced", &token))?;
        }
        let info = PyDict::new(py);
        info.set_item("hash", to_py(py, &snapshot.hash)?)?;
        info.set_item("tick_size", to_py(py, &snapshot.tick_size)?)?;
        info.set_item("last_trade_price", to_py(py, &snapshot.last_trade_price)?)?;
        self.out.append(PyTuple::new(
            py,
            [
                "book".into_pyobject(py)?.into_any(),
                token.into_any(),
                snapshot.market.as_str().into_pyobject(py)?.into_any(),
                "snapshot".into_pyobject(py)?.into_any(),
                level_tuple(py, &bids)?.into_any(),
                level_tuple(py, &asks)?.into_any(),
                optional_decimal(py, best_bid)?,
                optional_decimal(py, best_ask)?,
                snapshot.timestamp.into_pyobject(py)?.into_any(),
                info.into_any(),
            ],
        )?)?;
        Ok(())
    }

    fn price_changes(&mut self, message: &polymarket::PriceChanges) -> PyResult<()> {
        let py = self.py;
        if let Some(now) = message.timestamp {
            self.verify_older_than(now)?;
        }
        let mut order: Vec<String> = Vec::new();
        let mut changed: HashMap<String, (Vec<Level>, Vec<Level>)> = HashMap::new();
        for change in &message.changes {
            let token = PyString::new(py, &change.asset_id);
            let Some(book) = self.book(&token)? else {
                continue;
            };
            if !book.borrow().book.ready {
                continue;
            }
            if let Some(pending) = self.unverified.get_item(&token)? {
                let (pending_stamp, _, _) =
                    pending.extract::<(Option<i64>, Bound<'_, PyAny>, Bound<'_, PyAny>)>()?;
                if pending_stamp != message.timestamp && !self.verify(&token)? {
                    continue;
                }
            }
            let new = {
                let mut local = book.borrow_mut();
                let new = local.book.set(change.side, change.price, change.size);
                local.timestamp = stamp(py, message.timestamp)?;
                new
            };
            let entry = changed.entry(change.asset_id.clone()).or_insert_with(|| {
                order.push(change.asset_id.clone());
                (Vec::new(), Vec::new())
            });
            match change.side {
                synpath_core::Side::Bid => entry.0.push((change.price, new)),
                synpath_core::Side::Ask => entry.1.push((change.price, new)),
            }
            self.unverified.set_item(
                &token,
                (
                    message.timestamp,
                    to_py(py, &change.best_bid)?,
                    to_py(py, &change.best_ask)?,
                ),
            )?;
        }
        for asset_id in order {
            let (bids, asks) = &changed[&asset_id];
            let token = PyString::new(py, &asset_id);
            let (best_bid, best_ask) = match self.book(&token)? {
                Some(book) => {
                    let local = book.borrow();
                    (local.book.best_bid(), local.book.best_ask())
                }
                None => (None, None),
            };
            self.out.append(PyTuple::new(
                py,
                [
                    "book".into_pyobject(py)?.into_any(),
                    token.into_any(),
                    message.market.as_str().into_pyobject(py)?.into_any(),
                    "delta".into_pyobject(py)?.into_any(),
                    level_tuple(py, bids)?.into_any(),
                    level_tuple(py, asks)?.into_any(),
                    optional_decimal(py, best_bid)?,
                    optional_decimal(py, best_ask)?,
                    message.timestamp.into_pyobject(py)?.into_any(),
                    py.None().into_bound(py),
                ],
            )?)?;
        }
        Ok(())
    }

    fn verify_older_than(&mut self, now: i64) -> PyResult<()> {
        let mut stale = Vec::new();
        for (token, pending) in self.unverified.iter() {
            let (pending_stamp, _, _) =
                pending.extract::<(Option<i64>, Bound<'_, PyAny>, Bound<'_, PyAny>)>()?;
            if let Some(pending_stamp) = pending_stamp {
                if now - pending_stamp > WINDOW_MS {
                    stale.push(token.cast_into::<PyString>()?);
                }
            }
        }
        for token in stale {
            self.verify(&token)?;
        }
        Ok(())
    }

    /// `_verify`: check the token's book against the venue's last top of
    /// book; on a mismatch, invalidate it and report the gap.
    fn verify(&mut self, token: &Bound<'py, PyString>) -> PyResult<bool> {
        let pending = self.unverified.get_item(token)?;
        if pending.is_some() {
            self.unverified.del_item(token)?;
        }
        let (Some(pending), Some(book)) = (pending, self.book(token)?) else {
            return Ok(true);
        };
        let mut local = book.borrow_mut();
        if !local.book.ready {
            return Ok(true);
        }
        let (_, venue_bid, venue_ask) =
            pending.extract::<(Option<i64>, Bound<'_, PyAny>, Bound<'_, PyAny>)>()?;
        let bid_ok = agrees(
            local.book.best_bid(),
            venue_object(&venue_bid).ok().flatten(),
        );
        let ask_ok = agrees(
            local.book.best_ask(),
            venue_object(&venue_ask).ok().flatten(),
        );
        if bid_ok && ask_ok {
            return Ok(true);
        }
        self.out.append((
            "gap",
            token,
            local.book.best_bid(),
            local.book.best_ask(),
            venue_bid,
            venue_ask,
        ))?;
        local.book.invalidate();
        Ok(false)
    }
}

fn stamp(py: Python<'_>, timestamp: Option<i64>) -> PyResult<Option<Py<PyAny>>> {
    Ok(match timestamp {
        Some(ms) => Some(ms.into_pyobject(py)?.into_any().unbind()),
        None => None,
    })
}
