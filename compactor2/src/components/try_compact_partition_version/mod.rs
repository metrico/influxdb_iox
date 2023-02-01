use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::PartitionId;
use tracker::InstrumentedAsyncSemaphore;

use super::{scratchpad::Scratchpad, Components};

pub mod try_compact_partition_v0;
mod utils;

type Error = Box<dyn std::error::Error + Send + Sync>;

#[async_trait]
pub trait TryCompactPartitionVersion: Debug + Display + Send + Sync {
    /// Compact files of a partition.
    ///
    /// This function implements `divide and conquer` the files and repeat to
    /// compact their output until the stage that nothing should be compacted
    async fn try_compact_partition(
        &self,
        partition_id: PartitionId,
        job_semaphore: Arc<InstrumentedAsyncSemaphore>,
        components: Arc<Components>,
        scratchpad_ctx: &mut dyn Scratchpad,
    ) -> Result<(), Error>;
}
