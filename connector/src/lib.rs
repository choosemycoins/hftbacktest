//! Exchange connector library.
//!
//! This crate is primarily run as a binary (see `main.rs`), but the modules are exposed as a
//! library so the message types and parsing utilities can be reused by integration tests and
//! benchmarks (e.g. `benches/bybit_json.rs`).

#[cfg(feature = "binancefutures")]
pub mod binancefutures;
#[cfg(feature = "binancespot")]
pub mod binancespot;
#[cfg(feature = "bybit")]
pub mod bybit;

pub mod connector;
pub mod utils;
