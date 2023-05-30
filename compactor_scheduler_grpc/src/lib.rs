//! gRPC service for scheduling compactor tasks.

#![deny(
    rustdoc::broken_intra_doc_links,
    rust_2018_idioms,
    missing_debug_implementations,
    unreachable_pub
)]
#![warn(
    missing_docs,
    clippy::todo,
    clippy::dbg_macro,
    clippy::explicit_iter_loop,
    clippy::clone_on_ref_ptr,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    unused_crate_dependencies
)]
#![allow(clippy::missing_docs_in_private_items)]

mod local_scheduler;
pub use local_scheduler::LocalScheduler;
mod scheduler;
pub use scheduler::Scheduler;
mod service;
pub use service::*;

// temporary mod, for this commit only.
// code still being consumed in the compactor, by direct function call.
// TODO: move into LocalScheduler, which is used by the grpc service.
pub mod temp;
