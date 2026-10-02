//! Opinion and Polymarket US book messages, applied in Rust.
//!
//! Smaller than Polymarket's and Kalshi's: Opinion sends one level per
//! message, and Polymarket US the whole book each time, with no sequence or
//! top-of-book check to run in between.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString, PyTuple};
use synpath_core::{opinion, polymarket_us, Side};

use crate::pyjson::to_py;
use crate::{level_tuple, optional_decimal, LocalBook};

/// One decoded Opinion depth change, not yet applied.
#[pyclass(module = "synpath._core", name = "OpinionDepthChange", frozen)]
pub struct OpinionDepthChange {
    change: opinion::DepthChange,
}

/// Decode a `market.depth.diff` frame; `None` for anything else, or for a
/// change the Python handler would raise on.
#[pyfunction]
pub(crate) fn opinion_depth(raw: &str) -> Option<OpinionDepthChange> {
    match opinion::decode(raw) {
        Ok(Some(change)) => Some(OpinionDepthChange { change }),
        _ => None,
    }
}

#[pymethods]
impl OpinionDepthChange {
    #[getter]
    fn token(&self) -> &str {
        &self.change.token
    }

    /// Apply to `book`, as `OpinionMarketStream._apply` does. Returns
    /// `(bids, asks, best_bid, best_ask)` for the delta event: the one
    /// changed level on its side, with its new size.
    fn apply<'py>(
        &self,
        py: Python<'py>,
        book: &Bound<'py, LocalBook>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let mut local = book.borrow_mut();
        let size = local
            .book
            .set(self.change.side, self.change.price, self.change.size);
        let level = [(self.change.price, size)];
        let (bids, asks): (&[_], &[_]) = match self.change.side {
            Side::Bid => (&level, &[]),
            Side::Ask => (&[], &level),
        };
        PyTuple::new(
            py,
            [
                level_tuple(py, bids)?.into_any(),
                level_tuple(py, asks)?.into_any(),
                optional_decimal(py, local.book.best_bid())?,
                optional_decimal(py, local.book.best_ask())?,
            ],
        )
    }
}

/// Full market data, applied: the slug's book is replaced (and created if
/// new). Returns `(slug, bids, asks, best_bid, best_ask, transact_time,
/// state, stats)`, or `None` -- having changed nothing -- for anything else,
/// a message the Python handler would raise on, or a book that is not Rust's.
#[pyfunction]
pub(crate) fn polymarket_us_market_data<'py>(
    py: Python<'py>,
    raw: &str,
    books: &Bound<'py, PyDict>,
) -> PyResult<Option<Bound<'py, PyTuple>>> {
    let data = match polymarket_us::decode(raw) {
        Ok(Some(data)) => data,
        _ => return Ok(None),
    };
    let slug = PyString::new(py, &data.slug);
    let book = match books.get_item(&slug)? {
        Some(book) => match book.cast_into::<LocalBook>() {
            Ok(book) => book,
            Err(_) => return Ok(None),
        },
        None => {
            let book = Bound::new(py, LocalBook::new())?;
            books.set_item(&slug, &book)?;
            book
        }
    };
    let mut local = book.borrow_mut();
    local
        .book
        .replace(data.bids.iter().copied(), data.asks.iter().copied());
    let (bids, asks) = local.book.levels(None);
    Ok(Some(PyTuple::new(
        py,
        [
            slug.into_any(),
            level_tuple(py, &bids)?.into_any(),
            level_tuple(py, &asks)?.into_any(),
            optional_decimal(py, local.book.best_bid())?,
            optional_decimal(py, local.book.best_ask())?,
            to_py(py, &data.transact_time)?.into_bound(py),
            to_py(py, &data.state)?.into_bound(py),
            to_py(py, &data.stats)?.into_bound(py),
        ],
    )?))
}
