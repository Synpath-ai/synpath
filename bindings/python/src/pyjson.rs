//! JSON values as the Python objects `json.loads` would have produced.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use serde_json::Value;

pub(crate) fn to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    Ok(match value {
        Value::Null => py.None(),
        Value::Bool(flag) => flag.into_pyobject(py)?.to_owned().into_any().unbind(),
        Value::Number(number) => {
            if let Some(n) = number.as_i64() {
                n.into_pyobject(py)?.into_any().unbind()
            } else if let Some(n) = number.as_u64() {
                n.into_pyobject(py)?.into_any().unbind()
            } else {
                number
                    .as_f64()
                    .unwrap_or(f64::NAN)
                    .into_pyobject(py)?
                    .into_any()
                    .unbind()
            }
        }
        Value::String(text) => text.into_pyobject(py)?.into_any().unbind(),
        Value::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(to_py(py, item)?)?;
            }
            list.into_any().unbind()
        }
        Value::Object(map) => {
            let dict = PyDict::new(py);
            for (key, item) in map {
                dict.set_item(key, to_py(py, item)?)?;
            }
            dict.into_any().unbind()
        }
    })
}
