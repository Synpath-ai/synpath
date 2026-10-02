//! The hot paths of Synpath, in Rust.
//!
//! Pure Rust with no I/O and no Python, so it is testable with `cargo test`
//! alone and reusable from any language binding. `bindings/python` exposes it
//! to Python as `synpath._core`; every type here has a pure-Python twin that
//! the Python test suite runs against side by side.

pub mod book;
pub mod kalshi;
pub mod opinion;
pub mod polymarket;
pub mod polymarket_us;
pub mod router;
pub mod signing;
pub mod wire;

pub use book::{Book, Level, Side};
pub use rust_decimal::Decimal;
