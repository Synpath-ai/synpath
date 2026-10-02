//! Kalshi's WebSocket: order book snapshots and deltas.
//!
//! `decode` reads one frame the way `KalshiStream.handle` reads a book
//! message. Kalshi publishes YES bids and NO bids; the book is kept on the
//! YES leg, so a NO bid at `p` becomes a YES ask at `1 - p` here, as there.
//! Any other message is `Ok(None)` and goes to the Python handler; a book
//! message the Python handler would raise on is an `Err`, and goes there too.

use rust_decimal::Decimal;
use serde_json::Value;

use crate::book::Side;
use crate::wire::{self, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub ticker: String,
    /// YES bids.
    pub bids: Vec<(Decimal, Decimal)>,
    /// NO bids, as YES asks at `1 - p`.
    pub asks: Vec<(Decimal, Decimal)>,
    pub market_id: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Delta {
    pub ticker: String,
    /// `Bid` for a YES-side change; `Ask` for a NO-side one, already at `1 - p`.
    pub side: Side,
    pub price: Decimal,
    pub delta: Decimal,
    pub ts_ms: Value,
    pub client_order_id: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Book {
    Snapshot(Snapshot),
    Delta(Delta),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    /// The message's `type`.
    pub kind: &'static str,
    /// The subscription id and sequence number, as sent.
    pub sid: Value,
    pub seq: Value,
    /// `int(seq)`, when there is one.
    pub seq_number: Option<i64>,
    pub book: Book,
}

pub fn decode(raw: &str) -> Result<Option<Message>> {
    let message: Value = serde_json::from_str(raw).map_err(|e| format!("not JSON: {e}"))?;
    let kind = match message.get("type").and_then(Value::as_str) {
        Some("orderbook_snapshot") => "orderbook_snapshot",
        Some("orderbook_delta") => "orderbook_delta",
        _ => return Ok(None),
    };
    let seq = message.get("seq").cloned().unwrap_or(Value::Null);
    let seq_number = match &seq {
        Value::Null => None,
        Value::Number(number) => Some(
            number
                .as_i64()
                .ok_or_else(|| format!("seq is not an integer: {number}"))?,
        ),
        Value::String(text) => Some(wire::parse_int(text)?),
        other => return Err(format!("seq is not an integer: {other}")),
    };
    // `message.get("msg") or {}`: absent or empty is an empty body; a
    // non-empty body that is not an object would raise in Python.
    let empty = Value::Object(Default::default());
    let body = match message.get("msg") {
        Some(body @ Value::Object(_)) => body,
        None | Some(Value::Null) | Some(Value::Bool(false)) => &empty,
        Some(Value::String(text)) if text.is_empty() => &empty,
        Some(Value::Array(items)) if items.is_empty() => &empty,
        Some(other) => return Err(format!("msg is not an object: {other}")),
    };
    let ticker = wire::text_or_empty(body.get("market_ticker"))?;
    let book = if kind == "orderbook_snapshot" {
        Book::Snapshot(Snapshot {
            ticker,
            bids: pairs(body, "yes_dollars_fp", false)?,
            asks: pairs(body, "no_dollars_fp", true)?,
            market_id: body.get("market_id").cloned().unwrap_or(Value::Null),
        })
    } else {
        let price = wire::decimal_at(body, "price_dollars")?;
        let delta = wire::decimal_at(body, "delta_fp")?;
        let yes = body.get("side").and_then(Value::as_str) == Some("yes");
        Book::Delta(Delta {
            ticker,
            side: if yes { Side::Bid } else { Side::Ask },
            price: if yes { price } else { Decimal::ONE - price },
            delta,
            ts_ms: body.get("ts_ms").cloned().unwrap_or(Value::Null),
            client_order_id: body.get("client_order_id").cloned().unwrap_or(Value::Null),
        })
    };
    Ok(Some(Message {
        kind,
        sid: message.get("sid").cloned().unwrap_or(Value::Null),
        seq,
        seq_number,
        book,
    }))
}

/// `[(D(p), D(s)) for p, s in body.get(key) or []]`, mirrored to `1 - p`
/// for the NO side.
fn pairs(body: &Value, key: &str, mirror: bool) -> Result<Vec<(Decimal, Decimal)>> {
    wire::list_at(body, key)?
        .iter()
        .map(|pair| match pair.as_array().map(Vec::as_slice) {
            Some([price, size]) => {
                let price = wire::decimal(price)?;
                Ok((
                    if mirror { Decimal::ONE - price } else { price },
                    wire::decimal(size)?,
                ))
            }
            _ => Err(format!("{key:?} holds a level that is not a pair: {pair}")),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_snapshot_mirrors_the_no_side() {
        let raw = r#"{"type":"orderbook_snapshot","sid":2,"seq":1,"msg":{"market_ticker":"KX-1",
            "market_id":"abc","yes_dollars_fp":[["0.4100","10.00"]],"no_dollars_fp":[["0.5500","3.00"]]}}"#;
        let message = decode(raw).unwrap().unwrap();
        assert_eq!(message.seq_number, Some(1));
        let Book::Snapshot(book) = message.book else {
            panic!()
        };
        assert_eq!(book.ticker, "KX-1");
        assert_eq!(book.bids[0].0.to_string(), "0.4100");
        assert_eq!(book.asks[0].0.to_string(), "0.4500");
    }

    #[test]
    fn a_no_delta_is_a_yes_ask() {
        let raw = r#"{"type":"orderbook_delta","sid":2,"seq":"7","msg":{"market_ticker":"KX-1",
            "price_dollars":"0.30","delta_fp":"-2.00","side":"no","ts_ms":1789}}"#;
        let message = decode(raw).unwrap().unwrap();
        assert_eq!(message.seq_number, Some(7));
        let Book::Delta(delta) = message.book else {
            panic!()
        };
        assert_eq!(delta.side, Side::Ask);
        assert_eq!(delta.price.to_string(), "0.70");
    }

    #[test]
    fn other_messages_are_left_to_python() {
        assert_eq!(decode(r#"{"type":"ticker","msg":{}}"#).unwrap(), None);
        assert!(
            decode(r#"{"type":"orderbook_delta","msg":{"price_dollars":"x","delta_fp":"1"}}"#)
                .is_err()
        );
    }
}
