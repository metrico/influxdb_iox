use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::PartitionId;
use tracker::InstrumentedAsyncSemaphore;

use super::{scratchpad::Scratchpad, Components};

pub mod compact_partition;

type Error = Box<dyn std::error::Error + Send + Sync>;

#[async_trait]
pub trait CompactPartition: Debug + Display + Send + Sync {
    /// Use the given version to compact the partition
    async fn compact(
        &self,
        compact_version: usize,
        partition_id: PartitionId,
        job_semaphore: Arc<InstrumentedAsyncSemaphore>,
        components: Arc<Components>,
        scratchpad_ctx: &mut dyn Scratchpad,
    ) -> Result<(), Error>;
}
