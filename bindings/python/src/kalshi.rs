//! Kalshi's book messages, decoded and applied in Rust.
//!
//! In two steps, because the stream's sequence check sits between them and
//! stays in Python (it owns the subscription state): `kalshi_book_message`
//! decodes a frame -- or answers `None` for anything the Python handler
//! should read -- and Python runs `_check_sequence` on its `sid` and `seq`
//! before calling `apply`, which changes the book exactly as `_on_snapshot`
//! or `_on_delta` would and returns what the event needs.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString, PyTuple};
use synpath_core::kalshi::{self, Book};

use crate::pyjson::to_py;
use crate::{level_tuple, optional_decimal, LocalBook};

/// One decoded book message, not yet applied.
#[pyclass(module = "synpath._core", name = "KalshiBookMessage", frozen)]
pub struct KalshiBookMessage {
    message: kalshi::Message,
}

/// Decode a frame that is a book snapshot or delta. `None` when it is
/// anything else, when the Python handler would raise on it, or when the
/// book it is for is not a Rust book.
#[pyfunction]
pub(crate) fn kalshi_book_message(
    raw: &str,
    books: &Bound<'_, PyDict>,
) -> PyResult<Option<KalshiBookMessage>> {
    let message = match kalshi::decode(raw) {
        Ok(Some(message)) => message,
        Ok(None) | Err(_) => return Ok(None),
    };
    let ticker = match &message.book {
        Book::Snapshot(snapshot) => &snapshot.ticker,
        Book::Delta(delta) => &delta.ticker,
    };
    if let Some(book) = books.get_item(ticker)? {
        if book.cast::<LocalBook>().is_err() {
            return Ok(None);
        }
    }
    Ok(Some(KalshiBookMessage { message }))
}

#[pymethods]
impl KalshiBookMessage {
    /// The message's `type`: `orderbook_snapshot` or `orderbook_delta`.
    #[getter]
    fn kind(&self) -> &'static str {
        self.message.kind
    }

    #[getter]
    fn sid(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.message.sid)
    }

    /// `seq` as sent, for `book.sequence` and the event.
    #[getter]
    fn seq(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.message.seq)
    }

    /// `int(seq)`, for the sequence check; `None` without one.
    #[getter]
    fn seq_number(&self) -> Option<i64> {
        self.message.seq_number
    }

    /// Apply to `books` (ticker -> LocalBook). Returns
    /// `("snapshot", ticker, recovering, bids, asks, best_bid, best_ask, info)`,
    /// `("delta", ticker, bids, asks, best_bid, best_ask, timestamp, info)`,
    /// or `None` for a delta to a book that is not ready (its sequence is
    /// still recorded, as `_on_delta` does).
    fn apply<'py>(
        &self,
        py: Python<'py>,
        books: &Bound<'py, PyDict>,
    ) -> PyResult<Option<Bound<'py, PyTuple>>> {
        let seq = to_py(py, &self.message.seq)?;
        let seq = if seq.is_none(py) { None } else { Some(seq) };
        match &self.message.book {
            Book::Snapshot(snapshot) => {
                let ticker = PyString::new(py, &snapshot.ticker);
                let book = match books.get_item(&ticker)? {
                    Some(book) => book.cast_into::<LocalBook>()?,
                    None => {
                        let book = Bound::new(py, LocalBook::new())?;
                        books.set_item(&ticker, &book)?;
                        book
                    }
                };
                let mut local = book.borrow_mut();
                let recovering = !local.book.ready && local.sequence.is_some();
                local
                    .book
                    .replace(snapshot.bids.iter().copied(), snapshot.asks.iter().copied());
                local.sequence = seq;
                let (bids, asks) = local.book.levels(None);
                let info = PyDict::new(py);
                info.set_item("market_id", to_py(py, &snapshot.market_id)?)?;
                Ok(Some(PyTuple::new(
                    py,
                    [
                        "snapshot".into_pyobject(py)?.into_any(),
                        ticker.into_any(),
                        recovering.into_pyobject(py)?.to_owned().into_any(),
                        level_tuple(py, &bids)?.into_any(),
                        level_tuple(py, &asks)?.into_any(),
                        optional_decimal(py, local.book.best_bid())?,
                        optional_decimal(py, local.book.best_ask())?,
                        info.into_any(),
                    ],
                )?))
            }
            Book::Delta(delta) => {
                let ticker = PyString::new(py, &delta.ticker);
                let Some(book) = books.get_item(&ticker)? else {
                    return Ok(None);
                };
                let book = book.cast_into::<LocalBook>()?;
                let mut local = book.borrow_mut();
                if !local.book.ready {
                    local.sequence = seq;
                    return Ok(None);
                }
                let size = local.book.add(delta.side, delta.price, delta.delta);
                let level = [(delta.price, size)];
                let (bids, asks): (&[_], &[_]) = match delta.side {
                    synpath_core::Side::Bid => (&level, &[]),
                    synpath_core::Side::Ask => (&[], &level),
                };
                let timestamp = to_py(py, &delta.ts_ms)?;
                local.sequence = seq;
                local.timestamp = if timestamp.is_none(py) {
                    None
                } else {
                    Some(timestamp.clone_ref(py))
                };
                let info = PyDict::new(py);
                if truthy(&delta.client_order_id) {
                    info.set_item("client_order_id", to_py(py, &delta.client_order_id)?)?;
                }
                Ok(Some(PyTuple::new(
                    py,
                    [
                        "delta".into_pyobject(py)?.into_any(),
                        ticker.into_any(),
                        level_tuple(py, bids)?.into_any(),
                        level_tuple(py, asks)?.into_any(),
                        optional_decimal(py, local.book.best_bid())?,
                        optional_decimal(py, local.book.best_ask())?,
                        timestamp.into_bound(py),
                        info.into_any(),
                    ],
                )?))
            }
        }
    }
}

/// Python truthiness of a JSON value.
fn truthy(value: &serde_json::Value) -> bool {
    use serde_json::Value;
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64() != Some(0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
    }
}
