use std::fmt::{Debug, Display};

use async_trait::async_trait;
use data_types::PartitionId;

use crate::temp::PartitionsSourceConfig;

/// Core trait used for all schedulers.
#[async_trait]
pub trait Scheduler: Send + Sync + Debug + Display {
    /// Get partitions to be compacted.
    async fn get_partitions(&self, config: PartitionsSourceConfig) -> Vec<PartitionId>;
}
