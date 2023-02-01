// First compaction version that compact all files at once

use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFile, ParquetFileParams, PartitionId};
use parquet_file::ParquetFilePath;
use tracker::InstrumentedAsyncSemaphore;

use crate::components::{scratchpad::Scratchpad, Components};

use super::{
    utils::{fetch_partition_info, stream_into_file_sink},
    Error, TryCompactPartitionVersion,
};

#[derive(Debug)]
pub struct TryCompactPartitionV0 {}

impl TryCompactPartitionV0 {
    pub fn new() -> Self {
        Self {}
    }
}

impl Display for TryCompactPartitionV0 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "try_compact_partition_v0")
    }
}

#[async_trait]
impl TryCompactPartitionVersion for TryCompactPartitionV0 {
    async fn try_compact_partition(
        &self,
        partition_id: PartitionId,
        job_semaphore: Arc<InstrumentedAsyncSemaphore>,
        components: Arc<Components>,
        scratchpad_ctx: &mut dyn Scratchpad,
    ) -> Result<(), Error> {
        let mut files = components.partition_files_source.fetch(partition_id).await;

        // fetch partition info only if we need it
        let mut lazy_partition_info = None;

        loop {
            files = components.files_filter.apply(files);

            if !components
                .partition_filter
                .apply(partition_id, &files)
                .await
            {
                return Ok(());
            }

            // fetch partition info
            if lazy_partition_info.is_none() {
                lazy_partition_info = Some(fetch_partition_info(partition_id, &components).await?);
            }
            let partition_info = lazy_partition_info.as_ref().expect("just fetched");

            let (files_now, files_later) = components.round_split.split(files);

            let mut branches = components.divide_initial.divide(files_now);

            let mut files_next = files_later;
            while let Some(branch) = branches.pop() {
                let delete_ids = branch.iter().map(|f| f.id).collect::<Vec<_>>();

                // compute max_l0_created_at
                let max_l0_created_at = branch
                    .iter()
                    .map(|f| f.max_l0_created_at)
                    .max()
                    .expect("max_l0_created_at should have value");

                // stage files
                let input_paths: Vec<ParquetFilePath> = branch.iter().map(|f| f.into()).collect();
                let input_uuids_inpad = scratchpad_ctx.load_to_scratchpad(&input_paths).await;
                let branch_inpad: Vec<_> = branch
                    .into_iter()
                    .zip(input_uuids_inpad)
                    .map(|(f, uuid)| ParquetFile {
                        object_store_id: uuid,
                        ..f
                    })
                    .collect();

                let create = {
                    // draw semaphore BEFORE creating the DataFusion plan and drop it directly AFTER finishing the
                    // DataFusion computation (but BEFORE doing any additional external IO).
                    //
                    // We guard the DataFusion planning (that doesn't perform any IO) via the semaphore as well in case
                    // DataFusion ever starts to pre-allocate buffers during the physical planning. To the best of our
                    // knowledge, this is currently (2023-01-25) not the case but if this ever changes, then we are prepared.
                    let _permit = job_semaphore
                        .acquire(None)
                        .await
                        .expect("semaphore not closed");

                    // TODO: Need a wraper funtion to:
                    //    . split files into L0, L1 and L2
                    //    . identify right files for hot/cold compaction
                    //    . filter right amount of files
                    //    . compact many steps hot/cold (need more thinking)
                    let target_level = CompactionLevel::FileNonOverlapped;
                    let plan = components
                        .df_planner
                        .plan(branch_inpad, Arc::clone(partition_info), target_level)
                        .await?;
                    let streams = components.df_plan_exec.exec(plan);
                    let job = stream_into_file_sink(
                        streams,
                        Arc::clone(partition_info),
                        target_level,
                        max_l0_created_at.into(),
                        Arc::clone(&components),
                    );

                    // TODO: react to OOM and try to divide branch
                    job.await?
                };

                // upload files to real object store
                let output_files: Vec<ParquetFilePath> = create.iter().map(|p| p.into()).collect();
                let output_uuids = scratchpad_ctx.make_public(&output_files).await;
                let create: Vec<_> = create
                    .into_iter()
                    .zip(output_uuids)
                    .map(|(f, uuid)| ParquetFileParams {
                        object_store_id: uuid,
                        ..f
                    })
                    .collect();

                // clean scratchpad
                scratchpad_ctx.clean_from_scratchpad(&input_paths).await;

                let ids = components
                    .commit
                    .commit(partition_id, &delete_ids, &create)
                    .await;

                files_next.extend(
                    create
                        .into_iter()
                        .zip(ids)
                        .map(|(params, id)| ParquetFile::from_params(params, id)),
                );
            }

            files = files_next;
        }
    }
}
