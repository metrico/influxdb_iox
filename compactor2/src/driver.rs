use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use data_types::PartitionId;
use futures::StreamExt;
use tracker::InstrumentedAsyncSemaphore;

use crate::components::Components;

// TODO: modify this comments accordingly as we go
// Currently, we only compact files of level_n with level_n+1 and produce level_n+1 files,
// and with the strictly design that:
//    . Level-0 files can overlap with any files.
//    . Level-N files (N > 0) cannot overlap with any files in the same level.
//    . For Level-0 files, we always pick the smaller `created_at` files to compact (with
//      each other and overlapped L1 files) first.
//    . Level-N+1 files are results of compacting Level-N and/or Level-N+1 files, their `created_at`
//      can be after the `created_at` of other Level-N files but they may include data loaded before
//      the other Level-N files. Hence we should never use `created_at` of Level-N+1 files to order
//      them with Level-N files.
//    . We can only compact different sets of files of the same partition concurrently into the same target_level.
pub async fn compact(
    partition_concurrency: NonZeroUsize,
    partition_timeout: Duration,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
    components: &Arc<Components>,
    compact_version: usize,
) {
    let partition_ids = components.partitions_source.fetch().await;

    futures::stream::iter(partition_ids)
        .map(|partition_id| {
            let components = Arc::clone(components);

            compact_partition(
                partition_id,
                partition_timeout,
                Arc::clone(&job_semaphore),
                components,
                compact_version,
            )
        })
        .buffer_unordered(partition_concurrency.get())
        .collect::<()>()
        .await;
}

async fn compact_partition(
    partition_id: PartitionId,
    partition_timeout: Duration,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
    components: Arc<Components>,
    compact_version: usize,
) {
    let mut scratchpad = components.scratchpad_gen.pad();

    let res = tokio::time::timeout(
        partition_timeout,
        components.try_compact_partition_version.compact(
            compact_version,
            partition_id,
            job_semaphore,
            Arc::clone(&components),
            scratchpad.as_mut(),
        ),
    )
    .await;
    let res = match res {
        Ok(res) => res,
        Err(e) => Err(Box::new(e) as _),
    };
    components
        .partition_done_sink
        .record(partition_id, res)
        .await;

    scratchpad.clean().await;
}
