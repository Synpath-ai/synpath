//! Reading venue JSON the way the Python stream handlers read it.
//!
//! Each helper reproduces one Python expression the handlers use, so a
//! message decoded here means what it means there. Anything the Python
//! expression would raise on is an `Err` here, and the caller hands the whole
//! message back to the Python handler rather than half-applying it.

use std::borrow::Cow;
use std::str::FromStr;

use rust_decimal::Decimal;
use serde_json::value::RawValue;
use serde_json::Value;

pub type Result<T> = std::result::Result<T, String>;

/// A field read without building it: its raw JSON, or `None` when the key is
/// absent or `null` (which the Python handlers' `.get` cannot tell apart).
pub type Raw<'a> = Option<&'a RawValue>;

/// A raw field as a JSON value.
pub fn value(raw: Raw<'_>) -> Result<Value> {
    match raw {
        None => Ok(Value::Null),
        Some(raw) => serde_json::from_str(raw.get()).map_err(|e| e.to_string()),
    }
}

/// `D(message[key])` on a raw field; absent is a `KeyError` in Python.
pub fn decimal_raw(raw: Raw<'_>, key: &str) -> Result<Decimal> {
    let raw = raw.ok_or_else(|| format!("missing {key:?}"))?;
    let text = raw.get();
    if text.starts_with('"') {
        let unquoted: Cow<'_, str> = serde_json::from_str(text).map_err(|e| e.to_string())?;
        return parse_decimal(&unquoted);
    }
    decimal(&value(Some(raw))?)
}

/// `str(message.get(key) or "")` on a raw field.
pub fn text_raw(raw: Raw<'_>) -> Result<String> {
    match raw {
        Some(raw) if raw.get().starts_with('"') => {
            let text: Cow<'_, str> = serde_json::from_str(raw.get()).map_err(|e| e.to_string())?;
            Ok(text.into_owned())
        }
        other => text_or_empty(Some(&value(other)?)),
    }
}

/// `message.get(key) or []` on a raw field, as its raw items.
pub fn list_raw<'a>(raw: Raw<'a>) -> Result<Vec<&'a RawValue>> {
    match raw {
        None => Ok(Vec::new()),
        Some(raw) if raw.get().starts_with('[') => {
            serde_json::from_str(raw.get()).map_err(|e| e.to_string())
        }
        Some(raw) => match value(Some(raw))? {
            Value::Bool(false) => Ok(Vec::new()),
            Value::String(text) if text.is_empty() => Ok(Vec::new()),
            Value::Object(map) if map.is_empty() => Ok(Vec::new()),
            Value::Number(number) if number.as_f64() == Some(0.0) => Ok(Vec::new()),
            other => Err(format!("not a list: {other}")),
        },
    }
}

/// Polymarket's `_ms` on a raw field.
pub fn polymarket_ms_raw(raw: Raw<'_>) -> Result<Option<i64>> {
    match raw {
        Some(raw) if raw.get().starts_with('"') => {
            let text: Cow<'_, str> = serde_json::from_str(raw.get()).map_err(|e| e.to_string())?;
            polymarket_ms(Some(&Value::String(text.into_owned())))
        }
        other => polymarket_ms(Some(&value(other)?)),
    }
}

/// `str(value).upper() == word` on a raw field.
pub fn is_word_raw(raw: Raw<'_>, word: &str) -> Result<bool> {
    match raw {
        Some(raw) if raw.get().starts_with('"') => {
            let text: Cow<'_, str> = serde_json::from_str(raw.get()).map_err(|e| e.to_string())?;
            Ok(text.to_uppercase() == word)
        }
        other => Ok(upper_word(Some(&value(other)?)) == word),
    }
}

/// `Decimal(text)` as Python reads it: surrounding whitespace allowed,
/// scientific notation allowed.
pub fn parse_decimal(text: &str) -> Result<Decimal> {
    let text = text.trim();
    Decimal::from_str(text)
        .or_else(|_| Decimal::from_scientific(text))
        .map_err(|e| format!("not a decimal: {text:?} ({e})"))
}

/// `D(value)`: a dict's `"value"`, else `Decimal(str(value))`.
pub fn decimal(value: &Value) -> Result<Decimal> {
    let value = match value {
        Value::Object(map) => map.get("value").unwrap_or(&Value::Null),
        other => other,
    };
    match value {
        Value::String(text) => parse_decimal(text),
        Value::Number(number) => parse_decimal(&number.to_string()),
        other => Err(format!("not a decimal: {other}")),
    }
}

/// `Decimal(str(value))`, without `D`'s reading of a `{"value": ...}` dict.
pub fn plain_decimal(value: &Value) -> Result<Decimal> {
    match value {
        Value::String(text) => parse_decimal(text),
        Value::Number(number) => parse_decimal(&number.to_string()),
        other => Err(format!("not a decimal: {other}")),
    }
}

/// `D(message[key])`: a missing key is a `KeyError` in Python.
pub fn decimal_at(object: &Value, key: &str) -> Result<Decimal> {
    match object.get(key) {
        Some(value) => decimal(value),
        None => Err(format!("missing {key:?}")),
    }
}

/// `str(value or "")`.
pub fn text_or_empty(value: Option<&Value>) -> Result<String> {
    match value {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(text)) => Ok(text.clone()),
        Some(Value::Bool(false)) => Ok(String::new()),
        Some(Value::Bool(true)) => Ok("True".to_string()),
        Some(Value::Number(number)) => {
            if number.as_f64() == Some(0.0) {
                Ok(String::new())
            } else {
                Ok(number.to_string())
            }
        }
        Some(Value::Array(items)) if items.is_empty() => Ok(String::new()),
        Some(Value::Object(map)) if map.is_empty() => Ok(String::new()),
        Some(other) => Err(format!("not text: {other}")),
    }
}

/// `str(value).upper()`, for the side words venues send.
pub fn upper_word(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => "NONE".to_string(),
        Some(Value::String(text)) => text.to_uppercase(),
        Some(Value::Bool(flag)) => if *flag { "TRUE" } else { "FALSE" }.to_string(),
        Some(other) => other.to_string().to_uppercase(),
    }
}

/// Polymarket's `_ms`: `None` for null or `""`, else `int(str(value))`,
/// read as milliseconds when it is past 1e11 and as seconds otherwise.
pub fn polymarket_ms(value: Option<&Value>) -> Result<Option<i64>> {
    let number = match value {
        None | Some(Value::Null) => return Ok(None),
        Some(Value::String(text)) if text.is_empty() => return Ok(None),
        Some(Value::String(text)) => parse_int(text)?,
        Some(Value::Number(number)) => match number.as_i64() {
            Some(n) => n,
            None => return Err(format!("not an integer: {number}")),
        },
        Some(other) => return Err(format!("not an integer: {other}")),
    };
    if number as f64 > 1e11 {
        Ok(Some(number))
    } else {
        number
            .checked_mul(1000)
            .map(Some)
            .ok_or_else(|| format!("timestamp out of range: {number}"))
    }
}

/// `int(text)`: whitespace, a sign and digit-group underscores allowed.
pub fn parse_int(text: &str) -> Result<i64> {
    let cleaned: String = text.trim().chars().filter(|c| *c != '_').collect();
    cleaned
        .parse::<i64>()
        .map_err(|_| format!("not an integer: {text:?}"))
}

/// `message.get(key) or []` for a list of objects.
pub fn list_at<'a>(object: &'a Value, key: &str) -> Result<&'a [Value]> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(&[]),
        Some(Value::Array(items)) => Ok(items),
        Some(Value::String(text)) if text.is_empty() => Ok(&[]),
        Some(other) => Err(format!("{key:?} is not a list: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decimals_read_like_python() {
        assert_eq!(decimal(&json!("0.42")).unwrap().to_string(), "0.42");
        assert_eq!(decimal(&json!(" 0.420 ")).unwrap().to_string(), "0.420");
        assert_eq!(
            decimal(&json!({"value": "1.5"})).unwrap().to_string(),
            "1.5"
        );
        assert_eq!(decimal(&json!(3)).unwrap().to_string(), "3");
        assert!(decimal(&json!(null)).is_err());
        assert!(decimal(&json!("abc")).is_err());
    }

    #[test]
    fn timestamps_read_like_python() {
        assert_eq!(
            polymarket_ms(Some(&json!("1789659362296"))).unwrap(),
            Some(1789659362296)
        );
        assert_eq!(
            polymarket_ms(Some(&json!(1789659362))).unwrap(),
            Some(1789659362000)
        );
        assert_eq!(polymarket_ms(Some(&json!(""))).unwrap(), None);
        assert_eq!(polymarket_ms(None).unwrap(), None);
        assert!(polymarket_ms(Some(&json!("1.5"))).is_err());
    }

    #[test]
    fn text_reads_like_python() {
        assert_eq!(text_or_empty(Some(&json!("0xabc"))).unwrap(), "0xabc");
        assert_eq!(text_or_empty(Some(&json!(null))).unwrap(), "");
        assert_eq!(text_or_empty(None).unwrap(), "");
        assert_eq!(upper_word(Some(&json!("buy"))), "BUY");
        assert_eq!(upper_word(None), "NONE");
    }
}
