//! Polymarket's public market channel: book snapshots and price changes.
//!
//! `decode` reads one WebSocket frame into the book messages it carries,
//! exactly as `PolymarketMarketStream.handle` reads them. It answers `None`
//! when the frame carries anything else (a trade, a quote, a lifecycle
//! event): those are left to the Python handler, which keeps the raw payload
//! on the event it builds. A frame that would make the Python handler raise
//! is an `Err`, and is handed back to it untouched, so both paths fail alike.
//!
//! Nothing here touches a book. Applying a decoded frame is the caller's
//! job, and it starts only once the whole frame has decoded.

use std::borrow::Cow;

use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

use crate::book::Side;
use crate::wire::{self, Raw, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub asset_id: String,
    pub market: String,
    pub timestamp: Option<i64>,
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
    pub hash: Value,
    pub tick_size: Value,
    pub last_trade_price: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Change {
    pub asset_id: String,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    /// The venue's top of book after the change, as sent: checked later.
    pub best_bid: Value,
    pub best_ask: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PriceChanges {
    pub market: String,
    pub timestamp: Option<i64>,
    pub changes: Vec<Change>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    Book(Snapshot),
    PriceChange(PriceChanges),
}

/// The fields of one message the handler reads, in one pass; everything
/// else is skipped without being built. A message whose fields have types
/// these do not accept fails to parse, and the frame goes to Python.
#[derive(Deserialize)]
struct RawMessage<'a> {
    #[serde(borrow, default)]
    event_type: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    asset_id: Raw<'a>,
    #[serde(borrow, default)]
    market: Raw<'a>,
    #[serde(borrow, default)]
    timestamp: Raw<'a>,
    #[serde(borrow, default)]
    bids: Option<Vec<RawLevel<'a>>>,
    #[serde(borrow, default)]
    asks: Option<Vec<RawLevel<'a>>>,
    #[serde(borrow, default)]
    hash: Raw<'a>,
    #[serde(borrow, default)]
    tick_size: Raw<'a>,
    #[serde(borrow, default)]
    last_trade_price: Raw<'a>,
    #[serde(borrow, default)]
    price_changes: Option<Vec<RawChange<'a>>>,
}

#[derive(Deserialize)]
struct RawLevel<'a> {
    #[serde(borrow, default)]
    price: Raw<'a>,
    #[serde(borrow, default)]
    size: Raw<'a>,
}

#[derive(Deserialize)]
struct RawChange<'a> {
    #[serde(borrow, default)]
    asset_id: Raw<'a>,
    #[serde(borrow, default)]
    side: Raw<'a>,
    #[serde(borrow, default)]
    price: Raw<'a>,
    #[serde(borrow, default)]
    size: Raw<'a>,
    #[serde(borrow, default)]
    best_bid: Raw<'a>,
    #[serde(borrow, default)]
    best_ask: Raw<'a>,
}

/// The book messages in one frame, in order; `Ok(None)` when the frame holds
/// anything the Python handler should see instead.
pub fn decode(raw: &str) -> Result<Option<Vec<Message>>> {
    let text = raw.trim_start();
    let messages: Vec<RawMessage<'_>> = if text.starts_with('[') {
        serde_json::from_str(raw).map_err(|e| e.to_string())?
    } else if text.starts_with('{') {
        vec![serde_json::from_str(raw).map_err(|e| e.to_string())?]
    } else {
        return Err("not a message".to_string());
    };
    let mut out = Vec::with_capacity(messages.len());
    for message in &messages {
        match message.event_type.as_deref() {
            Some("book") => out.push(Message::Book(snapshot(message)?)),
            Some("price_change") => out.push(Message::PriceChange(price_changes(message)?)),
            _ => return Ok(None),
        }
    }
    Ok(Some(out))
}

fn snapshot(message: &RawMessage<'_>) -> Result<Snapshot> {
    Ok(Snapshot {
        asset_id: wire::text_raw(message.asset_id)?,
        market: wire::text_raw(message.market)?,
        timestamp: wire::polymarket_ms_raw(message.timestamp)?,
        bids: levels(message.bids.as_deref())?,
        asks: levels(message.asks.as_deref())?,
        hash: wire::value(message.hash)?,
        tick_size: wire::value(message.tick_size)?,
        last_trade_price: wire::value(message.last_trade_price)?,
    })
}

fn levels(raw: Option<&[RawLevel<'_>]>) -> Result<Vec<(Decimal, Decimal)>> {
    raw.unwrap_or_default()
        .iter()
        .map(|level| {
            Ok((
                wire::decimal_raw(level.price, "price")?,
                wire::decimal_raw(level.size, "size")?,
            ))
        })
        .collect()
}

fn price_changes(message: &RawMessage<'_>) -> Result<PriceChanges> {
    let changes = message
        .price_changes
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|change| {
            Ok(Change {
                asset_id: wire::text_raw(change.asset_id)?,
                side: if wire::is_word_raw(change.side, "BUY")? {
                    Side::Bid
                } else {
                    Side::Ask
                },
                price: wire::decimal_raw(change.price, "price")?,
                size: wire::decimal_raw(change.size, "size")?,
                best_bid: wire::value(change.best_bid)?,
                best_ask: wire::value(change.best_ask)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(PriceChanges {
        market: wire::text_raw(message.market)?,
        timestamp: wire::polymarket_ms_raw(message.timestamp)?,
        changes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_snapshot_and_its_levels() {
        let raw = r#"[{"event_type":"book","market":"0xabc","asset_id":"123","timestamp":"1789659362296","hash":"h",
            "bids":[{"price":"0.41","size":"10"}],"asks":[{"price":"0.45","size":"7"}],"tick_size":"0.01"}]"#;
        let messages = decode(raw).unwrap().unwrap();
        let Message::Book(book) = &messages[0] else {
            panic!()
        };
        assert_eq!(book.asset_id, "123");
        assert_eq!(book.timestamp, Some(1789659362296));
        assert_eq!(book.bids[0].0.to_string(), "0.41");
        assert_eq!(book.tick_size, Value::String("0.01".into()));
    }

    #[test]
    fn price_changes_keep_their_order_and_sides() {
        let raw = r#"{"event_type":"price_change","market":"0xabc","timestamp":"1789659362296","price_changes":[
            {"asset_id":"1","price":"0.5","size":"0","side":"BUY","best_bid":"0.4","best_ask":"0.6"},
            {"asset_id":"2","price":"0.6","size":"3","side":"SELL"}]}"#;
        let messages = decode(raw).unwrap().unwrap();
        let Message::PriceChange(change) = &messages[0] else {
            panic!()
        };
        assert_eq!(change.changes[0].side, Side::Bid);
        assert_eq!(change.changes[1].side, Side::Ask);
        assert_eq!(change.changes[1].best_bid, Value::Null);
    }

    #[test]
    fn anything_else_is_left_to_python() {
        let raw = r#"[{"event_type":"book","asset_id":"1","bids":[],"asks":[]},
                      {"event_type":"last_trade_price","asset_id":"1","price":"0.5"}]"#;
        assert_eq!(decode(raw).unwrap(), None);
    }

    #[test]
    fn a_malformed_book_is_an_error_not_a_partial_decode() {
        assert!(decode(r#"{"event_type":"book","bids":[{"price":"0.4"}]}"#).is_err());
        assert!(decode("PONG").is_err());
    }

    #[test]
    fn escaped_and_spaced_text_reads_as_python_reads_it() {
        let raw = r#" { "event_type" : "price_change", "market": "0x\u0061", "timestamp": 1789659362,
            "price_changes": [{"asset_id": "1", "price": " 0.50 ", "size": "3", "side": "buy"}] }"#;
        let Message::PriceChange(change) = &decode(raw).unwrap().unwrap()[0] else {
            panic!()
        };
        assert_eq!(change.market, "0xa");
        assert_eq!(change.timestamp, Some(1789659362000));
        assert_eq!(change.changes[0].price.to_string(), "0.50");
        assert_eq!(change.changes[0].side, Side::Bid);
    }
}
