use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use data_types::PartitionId;
use tracker::InstrumentedAsyncSemaphore;

use crate::components::{
    scratchpad::Scratchpad, try_compact_partition_version::TryCompactPartitionVersion, Components,
};

use super::{CompactPartition, Error};

const DEFAULT_COMPACT_VERSION: usize = 0;

#[derive(Debug)]
pub struct TryCompactPartition {
    try_compact_versions: Vec<Arc<dyn TryCompactPartitionVersion>>,
}

impl Display for TryCompactPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "try_compact_partition")
    }
}

impl TryCompactPartition {
    pub fn new(try_compact_versions: Vec<Arc<dyn TryCompactPartitionVersion>>) -> Self {
        Self {
            try_compact_versions,
        }
    }
}

#[async_trait]
impl CompactPartition for TryCompactPartition {
    async fn compact(
        &self,
        compact_version: usize,
        partition_id: PartitionId,
        job_semaphore: Arc<InstrumentedAsyncSemaphore>,
        components: Arc<Components>,
        scratchpad_ctx: &mut dyn Scratchpad,
    ) -> Result<(), Error> {
        let version = if compact_version < self.try_compact_versions.len() {
            compact_version
        } else {
            DEFAULT_COMPACT_VERSION
        };

        self.try_compact_versions[version]
            .try_compact_partition(partition_id, job_semaphore, components, scratchpad_ctx)
            .await
    }
}
