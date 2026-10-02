//! Opinion's `market.depth.diff`: one changed level per message.
//!
//! `decode` reads a frame the way `OpinionMarketStream._apply` reads a
//! change. Anything other than a single depth message is `Ok(None)` and goes
//! to the Python handler, as does a change it would raise on.

use rust_decimal::Decimal;
use serde_json::Value;

use crate::book::Side;
use crate::wire::{self, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct DepthChange {
    pub token: String,
    pub side: Side,
    pub price: Decimal,
    /// The level's whole new size; zero removes it.
    pub size: Decimal,
}

pub fn decode(raw: &str) -> Result<Option<DepthChange>> {
    let message: Value = serde_json::from_str(raw).map_err(|e| format!("not JSON: {e}"))?;
    if message.get("msgType").and_then(Value::as_str) != Some("market.depth.diff") {
        return Ok(None);
    }
    let side = match message.get("side") {
        Some(Value::String(word)) if word.to_lowercase() == "bids" => Side::Bid,
        _ => Side::Ask,
    };
    let price = message.get("price").ok_or("missing \"price\"")?;
    let size = message.get("size").ok_or("missing \"size\"")?;
    Ok(Some(DepthChange {
        token: wire::text_or_empty(message.get("tokenId"))?,
        side,
        price: wire::plain_decimal(price)?,
        size: wire::plain_decimal(size)?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_depth_change() {
        let raw = r#"{"msgType":"market.depth.diff","tokenId":"77","side":"bids","price":"0.5","size":"12"}"#;
        let change = decode(raw).unwrap().unwrap();
        assert_eq!(change.token, "77");
        assert_eq!(change.side, Side::Bid);
        assert_eq!(change.size.to_string(), "12");
    }

    #[test]
    fn anything_else_is_left_to_python() {
        assert_eq!(decode(r#"{"msgType":"market.last.trade"}"#).unwrap(), None);
        assert_eq!(
            decode(r#"[{"msgType":"market.depth.diff"}]"#).unwrap(),
            None
        );
        assert!(decode(r#"{"msgType":"market.depth.diff","price":"x","size":"1"}"#).is_err());
    }
}
