//! Polymarket US market data: every message is the whole book.
//!
//! `decode` reads a frame the way `PolymarketUSMarketStream.handle` reads
//! full market data (`marketData`, or `market_data`). Heartbeats, errors,
//! the lite feed and trades are `Ok(None)` and go to the Python handler.

use rust_decimal::Decimal;
use serde_json::Value;

use crate::wire::{self, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct MarketData {
    pub slug: String,
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
    /// Sent as is: the Python side parses the timestamp and tracks the state.
    pub transact_time: Value,
    pub state: Value,
    pub stats: Value,
}

pub fn decode(raw: &str) -> Result<Option<MarketData>> {
    let message: Value = serde_json::from_str(raw).map_err(|e| format!("not JSON: {e}"))?;
    let Value::Object(map) = &message else {
        return Ok(None);
    };
    if map.contains_key("heartbeat") || map.contains_key("error") {
        return Ok(None);
    }
    // `_first(message, "marketData", "market_data")`: the first key present.
    let data = match map.get("marketData").or_else(|| map.get("market_data")) {
        Some(data @ Value::Object(_)) => data,
        _ => return Ok(None),
    };
    Ok(Some(MarketData {
        slug: wire::text_or_empty(data.get("marketSlug"))?,
        bids: levels(data, "bids")?,
        asks: levels(data, "offers")?,
        transact_time: data.get("transactTime").cloned().unwrap_or(Value::Null),
        state: data.get("state").cloned().unwrap_or(Value::Null),
        stats: data.get("stats").cloned().unwrap_or(Value::Null),
    }))
}

fn levels(data: &Value, key: &str) -> Result<Vec<(Decimal, Decimal)>> {
    wire::list_at(data, key)?
        .iter()
        .map(|level| {
            Ok((
                wire::decimal_at(level, "px")?,
                wire::decimal_at(level, "qty")?,
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_market_data() {
        let raw = r#"{"marketData":{"marketSlug":"tec-1","bids":[{"px":{"value":"0.41"},"qty":"5"}],
            "offers":[{"px":"0.45","qty":"2"}],"state":"MARKET_STATE_OPEN","transactTime":"2026-09-17T12:00:00.123456789Z"}}"#;
        let data = decode(raw).unwrap().unwrap();
        assert_eq!(data.slug, "tec-1");
        assert_eq!(data.bids[0].0.to_string(), "0.41");
        assert_eq!(data.asks[0].1.to_string(), "2");
    }

    #[test]
    fn other_messages_are_left_to_python() {
        assert_eq!(decode(r#"{"heartbeat":{}}"#).unwrap(), None);
        assert_eq!(decode(r#"{"trade":{"price":"0.5"}}"#).unwrap(), None);
        assert_eq!(decode(r#"{"marketData":null}"#).unwrap(), None);
    }
}
