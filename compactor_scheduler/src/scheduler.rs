use std::fmt::{Debug, Display};

use async_trait::async_trait;
use data_types::PartitionId;
use generated_types::influxdata::iox::compactor_scheduler::v1::{self as proto};

/// Job assignment for a given partition.
#[derive(Debug)]
pub struct CompactionJob {
    /// Leased partition.
    pub partition_id: PartitionId,
}

impl From<proto::CompactionJob> for CompactionJob {
    fn from(value: proto::CompactionJob) -> Self {
        let proto::CompactionJob {
            partition_id,
            doneness_criteria: _,
            timeout: _,
            input_file_limit: _,
        } = value;

        Self {
            partition_id: PartitionId::new(partition_id),
        }
    }
}

/// Core trait used for all schedulers.
#[async_trait]
pub trait Scheduler: Send + Sync + Debug + Display {
    /// Get partitions to be compacted.
    async fn get_job(&self) -> Vec<CompactionJob>;
}
