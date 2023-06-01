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

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

mod local_scheduler;
use std::sync::Arc;

use backoff::BackoffConfig;
use clap_blocks::compactor_scheduler::{CompactorSchedulerConfig, CompactorSchedulerType};
use iox_catalog::interface::Catalog;

use crate::local_scheduler::shard_config::ShardConfig;

pub use local_scheduler::{LocalScheduler, PartitionsSourceConfig};
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
            let shard_config = ShardConfig::from_config(scheduler_config.shard_config);
            let partitions_source_config =
                PartitionsSourceConfig::from_config(scheduler_config.partition_source_config);
            Arc::new(LocalScheduler::new(
                partitions_source_config,
                catalog,
                BackoffConfig::default(),
                None,
                shard_config,
            ))
        }
        CompactorSchedulerType::Remote => {
            unimplemented!("only 'local' compactor-scheduler is implemented")
        }
    }
}
