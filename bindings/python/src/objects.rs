//! Building the Python objects events carry, quickly.
//!
//! A book snapshot is a hundred levels or more, and each level surfaces as a
//! `BookLevel` holding two `decimal.Decimal`s. Building those through their
//! Python constructors is most of what handling a snapshot costs, so:
//!
//! * a `Decimal` is made once per distinct value and then shared: book
//!   prices repeat on every message, and a `Decimal` is immutable, so a
//!   shared one is indistinguishable from a fresh one. The cache is keyed on
//!   the exact representation, so `0.40` and `0.4` stay distinct objects with
//!   their own digits;
//! * a `BookLevel` -- a frozen, slotted dataclass -- is allocated and its two
//!   slots filled directly, which is what its generated `__init__` does,
//!   without running that `__init__` in the interpreter. If the class ever
//!   stops being exactly that shape, its constructor is called instead.

use std::cell::RefCell;
use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyString, PyTuple, PyType};
use pyo3::{ffi, intern};
use rust_decimal::Decimal;

/// Distinct values kept before the cache starts over.
const CACHE_LIMIT: usize = 16_384;

thread_local! {
    static DECIMALS: RefCell<HashMap<[u8; 16], Py<PyAny>>> = RefCell::new(HashMap::new());
    /// The way back: each cached object's address -> its value. Only objects
    /// the cache holds are in it, so an address cannot be reused while it is.
    static VALUES: RefCell<HashMap<usize, Decimal>> = RefCell::new(HashMap::new());
}

/// A Python number as a Rust `Decimal`: a `Decimal` this module made is read
/// back without parsing; anything else is read as `Decimal(str(value))`.
pub(crate) fn from_py(value: &Bound<'_, PyAny>) -> PyResult<Decimal> {
    let address = value.as_ptr() as usize;
    if let Some(found) = VALUES.with(|values| values.borrow().get(&address).copied()) {
        return Ok(found);
    }
    value.extract::<Decimal>()
}

/// `decimal.Decimal` for `value`, with its exact digits.
pub(crate) fn decimal<'py>(py: Python<'py>, value: Decimal) -> PyResult<Bound<'py, PyAny>> {
    let key = value.serialize();
    if let Some(found) =
        DECIMALS.with(|cache| cache.borrow().get(&key).map(|obj| obj.clone_ref(py)))
    {
        return Ok(found.into_bound(py));
    }
    let made = value.into_pyobject(py)?;
    DECIMALS.with(|cache| {
        VALUES.with(|values| {
            let (mut cache, mut values) = (cache.borrow_mut(), values.borrow_mut());
            if cache.len() >= CACHE_LIMIT {
                values.clear();
                cache.clear();
            }
            values.insert(made.as_ptr() as usize, value);
            cache.insert(key, made.clone().unbind());
        })
    });
    Ok(made)
}

/// `None` or a `decimal.Decimal`.
pub(crate) fn optional_decimal<'py>(
    py: Python<'py>,
    value: Option<Decimal>,
) -> PyResult<Bound<'py, PyAny>> {
    match value {
        Some(value) => decimal(py, value),
        None => Ok(py.None().into_bound(py)),
    }
}

/// `synpath.ws.base.BookLevel`. Imported on first use: that module imports
/// this one.
static BOOK_LEVEL: PyOnceLock<Py<PyType>> = PyOnceLock::new();
/// Whether `BookLevel` is the slotted `(price, size)` dataclass the direct
/// build relies on.
static DIRECT: PyOnceLock<bool> = PyOnceLock::new();
static OBJECT_NEW: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

pub(crate) fn book_level_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    BOOK_LEVEL.import(py, "synpath.ws.base", "BookLevel")
}

fn direct(py: Python<'_>) -> PyResult<bool> {
    if let Some(known) = DIRECT.get(py) {
        return Ok(*known);
    }
    let cls = book_level_class(py)?;
    let slots: Option<Vec<String>> = cls.getattr("__slots__").ok().and_then(|s| s.extract().ok());
    let fields = cls.getattr("__dataclass_fields__").ok();
    let shaped = slots.as_deref() == Some(&["price".to_string(), "size".to_string()][..])
        && fields.is_some();
    Ok(*DIRECT.get_or_init(py, || shaped))
}

/// One `BookLevel(price, size)`.
pub(crate) fn book_level<'py>(
    py: Python<'py>,
    price: Decimal,
    size: Decimal,
) -> PyResult<Bound<'py, PyAny>> {
    let cls = book_level_class(py)?;
    let price = decimal(py, price)?;
    let size = decimal(py, size)?;
    if !direct(py)? {
        return cls.call1((price, size));
    }
    let object_new = OBJECT_NEW.get_or_try_init(py, || {
        Ok::<_, PyErr>(
            py.import("builtins")?
                .getattr("object")?
                .getattr("__new__")?
                .unbind(),
        )
    })?;
    let level = object_new.bind(py).call1((cls,))?;
    set_slot(&level, intern!(py, "price"), &price)?;
    set_slot(&level, intern!(py, "size"), &size)?;
    Ok(level)
}

/// `object.__setattr__(target, name, value)`: fills a slot past a frozen
/// dataclass's `__setattr__`, as its own `__init__` does.
fn set_slot(
    target: &Bound<'_, PyAny>,
    name: &Bound<'_, PyString>,
    value: &Bound<'_, PyAny>,
) -> PyResult<()> {
    // SAFETY: all three are live, owned references held for the call;
    // PyObject_GenericSetAttr is part of the stable ABI.
    let status =
        unsafe { ffi::PyObject_GenericSetAttr(target.as_ptr(), name.as_ptr(), value.as_ptr()) };
    if status == 0 {
        Ok(())
    } else {
        Err(PyErr::fetch(target.py()))
    }
}

/// Levels as a tuple of `BookLevel`, the shape events and `levels()` carry.
pub(crate) fn level_tuple<'py>(
    py: Python<'py>,
    levels: &[(Decimal, Decimal)],
) -> PyResult<Bound<'py, PyTuple>> {
    let mut out = Vec::with_capacity(levels.len());
    for (price, size) in levels {
        out.push(book_level(py, *price, *size)?);
    }
    PyTuple::new(py, out)
}
