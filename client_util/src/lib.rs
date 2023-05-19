//! Shared InfluxDB IOx API client functionality
#![deny(
    rustdoc::broken_intra_doc_links,
    rustdoc::bare_urls,
    rust_2018_idioms,
    missing_debug_implementations,
    unreachable_pub
)]
#![warn(
    missing_docs,
    clippy::todo,
    clippy::dbg_macro,
    clippy::clone_on_ref_ptr,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::todo,
    clippy::dbg_macro
)]
#![allow(clippy::missing_docs_in_private_items)]

/// Builder for constructing connections for use with the various gRPC clients
pub mod connection;

mod tower;

/// Namespace <--> org/bucket utilities
pub mod namespace_translation;
