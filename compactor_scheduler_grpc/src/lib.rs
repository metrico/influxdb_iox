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
use std::sync::Arc;

use backoff::BackoffConfig;
use clap_blocks::compactor_scheduler::{CompactorSchedulerConfig, CompactorSchedulerType};
use iox_catalog::interface::Catalog;
pub use local_scheduler::LocalScheduler;
mod scheduler;
pub use scheduler::Scheduler;
mod service;
pub use service::*;

/// Instantiate a compaction scheduler service
pub fn create_compactor_scheduler_service(
    scheduler_config: CompactorSchedulerConfig,
    catalog: Arc<dyn Catalog>,
) -> Arc<dyn Scheduler> {
    match scheduler_config.compactor_scheduler_type {
        CompactorSchedulerType::Local => {
            Arc::new(LocalScheduler::new(catalog, BackoffConfig::default(), None))
        }
        CompactorSchedulerType::Remote => {
            unimplemented!("only 'local' compactor-scheduler is implemented")
        }
    }
}

// temporary mod, for this commit only.
// code still being consumed in the compactor, by direct function call.
// TODO: move into LocalScheduler, which is used by the grpc service.
pub mod temp;
