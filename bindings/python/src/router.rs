//! The router's hot loop for Python: `router_view`, `router_levels` and
//! `RouterLevels.walk`.
//!
//! `synpath.engine.router.plan` uses them when the core is installed and
//! keeps everything else itself: the leg sizing and rounding, the fee
//! floors, the re-walks that drop or move a venue. Any number Rust could not
//! carry exactly as Python's `Decimal` would gives `None`, and the plan is
//! made in Python instead.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyList, PyString, PyTuple, PyType};
use rust_decimal::Decimal;
use synpath_core::router::{self, Level};

use crate::objects::{decimal, from_py};
use crate::LocalBook;

static DECIMAL_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();

fn pair<'py>(py: Python<'py>, price: Decimal, size: Decimal) -> PyResult<Bound<'py, PyTuple>> {
    PyTuple::new(py, [decimal(py, price)?, decimal(py, size)?])
}

/// `view(book, member)` for a Rust `LocalBook`: `(asks, bids)` in bucket
/// terms -- the NO book of a flipped member, as `1 - p` of the YES book --
/// as tuples of `(price, size)`. `None` for any other book.
#[pyfunction]
pub(crate) fn router_view<'py>(
    py: Python<'py>,
    book: &Bound<'py, PyAny>,
    flip: bool,
) -> PyResult<Option<(Bound<'py, PyTuple>, Bound<'py, PyTuple>)>> {
    let Ok(local) = book.cast::<LocalBook>() else {
        return Ok(None);
    };
    if book.hasattr("side")? {
        return Ok(None);
    }
    let (bids, asks) = local.borrow().book.levels(None);
    let positive = |levels: Vec<(Decimal, Decimal)>| {
        levels.into_iter().filter(|(_, size)| *size > Decimal::ZERO)
    };
    let mirror = |levels: Vec<(Decimal, Decimal)>| -> Option<Vec<(Decimal, Decimal)>> {
        positive(levels)
            .map(|(price, size)| Some((router::sub(Decimal::ONE, price)?, size)))
            .collect()
    };
    let (asks, bids): (Vec<_>, Vec<_>) = if flip {
        match (mirror(bids), mirror(asks)) {
            (Some(asks), Some(bids)) => (asks, bids),
            _ => return Ok(None),
        }
    } else {
        (positive(asks).collect(), positive(bids).collect())
    };
    let build = |levels: Vec<(Decimal, Decimal)>| -> PyResult<Bound<'py, PyTuple>> {
        let rows: Vec<_> = levels
            .into_iter()
            .map(|(p, s)| pair(py, p, s))
            .collect::<PyResult<_>>()?;
        PyTuple::new(py, rows)
    };
    Ok(Some((build(asks)?, build(bids)?)))
}

/// One side of the members' books merged and sorted best-net first.
#[pyclass(module = "synpath._core", name = "RouterLevels", frozen)]
pub struct RouterLevels {
    members: Vec<Py<PyString>>,
    levels: Vec<Level>,
}

/// `merge` for the side being planned. `members` is `[(market_id, rows)]`
/// in bucket order, `rows` that member's asks (a buy) or bids (a sell) in
/// bucket terms; `fee(market_id, price, 1)` is asked once per level.
/// `None` when a fee is not a `Decimal` or an integer, or a net price would
/// not be exact.
#[pyfunction]
pub(crate) fn router_levels(
    py: Python<'_>,
    members: &Bound<'_, PyList>,
    fee: &Bound<'_, PyAny>,
    buy: bool,
) -> PyResult<Option<RouterLevels>> {
    let decimal_type = DECIMAL_TYPE.import(py, "decimal", "Decimal")?;
    let one = decimal(py, Decimal::ONE)?;
    let mut names = Vec::new();
    let mut levels = Vec::new();
    for (index, item) in members.iter().enumerate() {
        let (market_id, rows): (Bound<'_, PyString>, Bound<'_, PyAny>) = item.extract()?;
        for row in rows.try_iter()? {
            let (price_obj, size_obj): (Bound<'_, PyAny>, Bound<'_, PyAny>) = row?.extract()?;
            let (Ok(price), Ok(size)) = (from_py(&price_obj), from_py(&size_obj)) else {
                return Ok(None);
            };
            let charged = fee.call1((&market_id, &price_obj, &one))?;
            if !(charged.is_instance(decimal_type)?
                || charged.is_instance_of::<pyo3::types::PyInt>())
            {
                return Ok(None);
            }
            let Ok(charged) = from_py(&charged) else {
                return Ok(None);
            };
            let per = if charged > Decimal::ZERO {
                charged
            } else {
                Decimal::ZERO
            };
            let net = if buy {
                router::add(price, per)
            } else {
                router::sub(price, per)
            };
            let Some(net) = net else {
                return Ok(None);
            };
            levels.push(Level {
                member: index,
                price,
                net,
                size,
            });
        }
        names.push(market_id.unbind());
    }
    router::sort(&mut levels, buy);
    Ok(Some(RouterLevels {
        members: names,
        levels,
    }))
}

#[pymethods]
impl RouterLevels {
    /// `_walk`'s loop. Returns `(slots, remaining, reason)`, a slot being
    /// `(market_id, amount, cost, fee, worst)`, or `None` when the arithmetic
    /// would not be exact.
    fn walk<'py>(
        &self,
        py: Python<'py>,
        buy: bool,
        amount: &Bound<'py, PyAny>,
        limit: &Bound<'py, PyAny>,
        excluded: Vec<String>,
    ) -> PyResult<Option<Bound<'py, PyTuple>>> {
        let (Ok(amount), Ok(limit)) = (from_py(amount), from_py(limit)) else {
            return Ok(None);
        };
        let mut skip = vec![false; self.members.len()];
        for (index, name) in self.members.iter().enumerate() {
            let name = name.bind(py).to_cow()?;
            skip[index] = excluded.iter().any(|e| e.as_str() == name.as_ref());
        }
        let Some((slots, remaining, reason)) =
            router::walk(&self.levels, buy, amount, limit, &skip)
        else {
            return Ok(None);
        };
        let rows = PyList::empty(py);
        for slot in slots {
            rows.append((
                self.members[slot.member].bind(py),
                decimal(py, slot.amount)?,
                decimal(py, slot.cost)?,
                decimal(py, slot.fee)?,
                decimal(py, slot.worst)?,
            ))?;
        }
        Ok(Some(PyTuple::new(
            py,
            [
                rows.into_any(),
                decimal(py, remaining)?,
                reason.word().into_pyobject(py)?.into_any(),
            ],
        )?))
    }

    fn __len__(&self) -> usize {
        self.levels.len()
    }
}
