//! `synpath._core`: synpath-core for Python.
//!
//! Each class here is a drop-in for a pure-Python class in `synpath`, with the
//! same methods, the same attribute names and the same values, so code written
//! against one runs against the other unchanged. `synpath` picks this one when
//! the extension is installed; `SYNPATH_PURE_PYTHON=1` forces the Python twin.

use pyo3::basic::CompareOp;
use pyo3::exceptions::{PyKeyError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyMapping, PyTuple};
use rust_decimal::Decimal;
use synpath_core::{Book, Side};

mod kalshi;
mod objects;
mod polymarket;
mod pyjson;
mod router;
mod signing;
mod venues;

pub(crate) use objects::{level_tuple, optional_decimal};

/// The Python version treats any side word other than "bid" as the ask side.
fn side_of(word: &str) -> Side {
    if word == "bid" {
        Side::Bid
    } else {
        Side::Ask
    }
}

fn pairs(levels: &Bound<'_, PyAny>) -> PyResult<Vec<(Decimal, Decimal)>> {
    let mut out = Vec::new();
    for item in levels.try_iter()? {
        out.push(item?.extract::<(Decimal, Decimal)>()?);
    }
    Ok(out)
}

/// One side of a market's book: price -> size on each side, plus readiness.
///
/// `ready` is false until a snapshot has been applied and becomes false again
/// when the stream loses confidence in it (a gap, a reconnect); a book that
/// is not ready ignores deltas rather than applying them to a stale base.
#[pyclass(module = "synpath._core", name = "LocalBook", subclass)]
pub struct LocalBook {
    pub(crate) book: Book,
    pub(crate) sequence: Option<Py<PyAny>>,
    pub(crate) timestamp: Option<Py<PyAny>>,
}

#[pymethods]
impl LocalBook {
    #[new]
    pub(crate) fn new() -> Self {
        LocalBook {
            book: Book::new(),
            sequence: None,
            timestamp: None,
        }
    }

    #[getter]
    fn ready(&self) -> bool {
        self.book.ready
    }

    #[setter]
    fn set_ready(&mut self, value: bool) {
        self.book.ready = value;
    }

    #[getter]
    fn sequence(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.sequence.as_ref().map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_sequence(&mut self, value: Option<Py<PyAny>>) {
        self.sequence = value;
    }

    #[getter]
    fn timestamp(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.timestamp.as_ref().map(|value| value.clone_ref(py))
    }

    #[setter]
    fn set_timestamp(&mut self, value: Option<Py<PyAny>>) {
        self.timestamp = value;
    }

    /// The bid levels, as a live price -> size mapping.
    #[getter]
    fn bids(slf: Bound<'_, Self>) -> BookSide {
        BookSide {
            owner: slf.unbind(),
            side: Side::Bid,
        }
    }

    #[setter]
    fn set_bids(&mut self, levels: &Bound<'_, PyAny>) -> PyResult<()> {
        self.book.bids = mapping_levels(levels)?;
        Ok(())
    }

    /// The ask levels, as a live price -> size mapping.
    #[getter]
    fn asks(slf: Bound<'_, Self>) -> BookSide {
        BookSide {
            owner: slf.unbind(),
            side: Side::Ask,
        }
    }

    #[setter]
    fn set_asks(&mut self, levels: &Bound<'_, PyAny>) -> PyResult<()> {
        self.book.asks = mapping_levels(levels)?;
        Ok(())
    }

    fn replace(&mut self, bids: &Bound<'_, PyAny>, asks: &Bound<'_, PyAny>) -> PyResult<()> {
        self.book.replace(pairs(bids)?, pairs(asks)?);
        Ok(())
    }

    fn set(&mut self, side: &str, price: Decimal, size: Decimal) -> Decimal {
        self.book.set(side_of(side), price, size)
    }

    fn add(&mut self, side: &str, price: Decimal, delta: Decimal) -> Decimal {
        self.book.add(side_of(side), price, delta)
    }

    fn invalidate(&mut self) {
        self.book.invalidate();
    }

    #[getter]
    fn best_bid<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        optional_decimal(py, self.book.best_bid())
    }

    #[getter]
    fn best_ask<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        optional_decimal(py, self.book.best_ask())
    }

    /// Bids best first (descending), asks best first (ascending).
    #[pyo3(signature = (depth = None))]
    fn levels<'py>(&self, py: Python<'py>, depth: Option<usize>) -> PyResult<Bound<'py, PyTuple>> {
        let (bids, asks) = self.book.levels(depth);
        PyTuple::new(py, [level_tuple(py, &bids)?, level_tuple(py, &asks)?])
    }

    /// The other side's view: bids become asks at `1 - p`.
    #[pyo3(signature = (face_value = Decimal::ONE))]
    fn mirrored(&self, py: Python<'_>, face_value: Decimal) -> LocalBook {
        LocalBook {
            book: self.book.mirrored(face_value),
            sequence: self.sequence.as_ref().map(|value| value.clone_ref(py)),
            timestamp: self.timestamp.as_ref().map(|value| value.clone_ref(py)),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "LocalBook(ready={}, bids={}, asks={})",
            if self.book.ready { "True" } else { "False" },
            self.book.bids.len(),
            self.book.asks.len()
        )
    }
}

fn mapping_levels(
    levels: &Bound<'_, PyAny>,
) -> PyResult<std::collections::BTreeMap<Decimal, Decimal>> {
    let mut out = std::collections::BTreeMap::new();
    let items = if let Ok(mapping) = levels.cast::<PyMapping>() {
        mapping.items()?.into_any()
    } else {
        levels.clone()
    };
    for (price, size) in pairs(&items)? {
        out.insert(price, size);
    }
    Ok(out)
}

/// One side of a `LocalBook`, behaving as the `dict` the pure-Python book
/// keeps there: `book.bids[price]`, `price in book.bids`, `.get`, `.items()`,
/// `len(...)`, equality with a plain dict. It is a live view: it reads and
/// writes the book it came from. Keys iterate in ascending price order.
#[pyclass(module = "synpath._core", name = "BookSide", mapping)]
pub struct BookSide {
    owner: Py<LocalBook>,
    side: Side,
}

impl BookSide {
    fn with<R>(
        &self,
        py: Python<'_>,
        read: impl FnOnce(&std::collections::BTreeMap<Decimal, Decimal>) -> R,
    ) -> R {
        let owner = self.owner.bind(py).borrow();
        read(owner.book.levels_of(self.side))
    }

    fn with_mut<R>(
        &self,
        py: Python<'_>,
        write: impl FnOnce(&mut std::collections::BTreeMap<Decimal, Decimal>) -> R,
    ) -> R {
        let mut owner = self.owner.bind(py).borrow_mut();
        write(owner.book.levels_mut(self.side))
    }

    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        let levels: Vec<(Decimal, Decimal)> =
            self.with(py, |levels| levels.iter().map(|(p, s)| (*p, *s)).collect());
        for (price, size) in levels {
            dict.set_item(price, size)?;
        }
        Ok(dict)
    }
}

#[pymethods]
impl BookSide {
    fn __len__(&self, py: Python<'_>) -> usize {
        self.with(py, |levels| levels.len())
    }

    fn __getitem__(&self, py: Python<'_>, price: &Bound<'_, PyAny>) -> PyResult<Decimal> {
        let key: Decimal = price
            .extract()
            .map_err(|_| PyKeyError::new_err(price.clone().unbind()))?;
        self.with(py, |levels| levels.get(&key).copied())
            .ok_or_else(|| PyKeyError::new_err(price.clone().unbind()))
    }

    fn __setitem__(&self, py: Python<'_>, price: Decimal, size: Decimal) {
        self.with_mut(py, |levels| {
            levels.insert(price, size);
        });
    }

    fn __delitem__(&self, py: Python<'_>, price: &Bound<'_, PyAny>) -> PyResult<()> {
        let key: Decimal = price
            .extract()
            .map_err(|_| PyKeyError::new_err(price.clone().unbind()))?;
        self.with_mut(py, |levels| levels.remove(&key))
            .map(|_| ())
            .ok_or_else(|| PyKeyError::new_err(price.clone().unbind()))
    }

    fn __contains__(&self, py: Python<'_>, price: &Bound<'_, PyAny>) -> bool {
        match price.extract::<Decimal>() {
            Ok(key) => self.with(py, |levels| levels.contains_key(&key)),
            Err(_) => false,
        }
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Ok(self.keys(py)?.into_any().try_iter()?.into_any())
    }

    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let keys: Vec<Decimal> = self.with(py, |levels| levels.keys().copied().collect());
        PyList::new(py, keys)
    }

    fn values<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let values: Vec<Decimal> = self.with(py, |levels| levels.values().copied().collect());
        PyList::new(py, values)
    }

    fn items<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let items: Vec<(Decimal, Decimal)> =
            self.with(py, |levels| levels.iter().map(|(p, s)| (*p, *s)).collect());
        PyList::new(py, items)
    }

    #[pyo3(signature = (price, default = None))]
    fn get(
        &self,
        py: Python<'_>,
        price: &Bound<'_, PyAny>,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        if let Ok(key) = price.extract::<Decimal>() {
            if let Some(size) = self.with(py, |levels| levels.get(&key).copied()) {
                return Ok(size.into_pyobject(py)?.unbind());
            }
        }
        Ok(default.unwrap_or_else(|| py.None()))
    }

    #[pyo3(signature = (price, *default))]
    fn pop(
        &self,
        py: Python<'_>,
        price: &Bound<'_, PyAny>,
        default: &Bound<'_, PyTuple>,
    ) -> PyResult<Py<PyAny>> {
        if let Ok(key) = price.extract::<Decimal>() {
            if let Some(size) = self.with_mut(py, |levels| levels.remove(&key)) {
                return Ok(size.into_pyobject(py)?.unbind());
            }
        }
        match default.len() {
            0 => Err(PyKeyError::new_err(price.clone().unbind())),
            1 => Ok(default.get_item(0)?.unbind()),
            n => Err(PyTypeError::new_err(format!(
                "pop expected at most 2 arguments, got {}",
                n + 1
            ))),
        }
    }

    /// A plain `dict` copy of these levels.
    fn copy<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.to_dict(py)
    }

    fn __richcmp__(
        &self,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
        op: CompareOp,
    ) -> PyResult<Py<PyAny>> {
        let equal = match other.cast::<BookSide>() {
            Ok(view) => {
                let theirs = view.borrow();
                let mine: Vec<(Decimal, Decimal)> =
                    self.with(py, |l| l.iter().map(|(p, s)| (*p, *s)).collect());
                let other_levels: Vec<(Decimal, Decimal)> =
                    theirs.with(py, |l| l.iter().map(|(p, s)| (*p, *s)).collect());
                mine == other_levels
            }
            Err(_) => match other.cast::<PyMapping>() {
                Ok(mapping) => self.equals_mapping(py, mapping)?,
                Err(_) => return Ok(py.NotImplemented()),
            },
        };
        match op {
            CompareOp::Eq => Ok(equal.into_pyobject(py)?.to_owned().into_any().unbind()),
            CompareOp::Ne => Ok((!equal).into_pyobject(py)?.to_owned().into_any().unbind()),
            _ => Ok(py.NotImplemented()),
        }
    }

    fn __hash__(&self) -> PyResult<isize> {
        Err(PyTypeError::new_err("unhashable type: 'BookSide'"))
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(self.to_dict(py)?.repr()?.to_string())
    }
}

impl BookSide {
    fn equals_mapping(&self, py: Python<'_>, other: &Bound<'_, PyMapping>) -> PyResult<bool> {
        if other.len()? != self.__len__(py) {
            return Ok(false);
        }
        for item in other.items()?.try_iter()? {
            let (price, size) = match item?.extract::<(Decimal, Decimal)>() {
                Ok(pair) => pair,
                Err(_) => return Ok(false),
            };
            if self.with(py, |levels| levels.get(&price).copied()) != Some(size) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<LocalBook>()?;
    m.add_class::<BookSide>()?;
    m.add_class::<kalshi::KalshiBookMessage>()?;
    m.add_function(wrap_pyfunction!(kalshi::kalshi_book_message, m)?)?;
    m.add_function(wrap_pyfunction!(polymarket::polymarket_market, m)?)?;
    m.add_class::<venues::OpinionDepthChange>()?;
    m.add_class::<signing::Secp256k1Signer>()?;
    m.add_class::<router::RouterLevels>()?;
    m.add_function(wrap_pyfunction!(router::router_view, m)?)?;
    m.add_function(wrap_pyfunction!(router::router_levels, m)?)?;
    m.add_function(wrap_pyfunction!(signing::polymarket_order_hash, m)?)?;
    m.add_function(wrap_pyfunction!(venues::opinion_depth, m)?)?;
    m.add_function(wrap_pyfunction!(venues::polymarket_us_market_data, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
