use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use data_types::{CompactionLevel, ParquetFile, ParquetFileParams, PartitionId};
use futures::StreamExt;
use observability_deps::tracing::info;
use parquet_file::ParquetFilePath;
use tracker::InstrumentedAsyncSemaphore;

use crate::{
    components::{scratchpad::Scratchpad, Components},
    error::DynError,
    file_classification::{FileToSplit, FilesToCompactOrSplit},
    partition_info::PartitionInfo,
    PlanIR,
};

/// Tries to compact all eligible partitions, up to
/// partition_concurrency at a time.
pub async fn compact(
    partition_concurrency: NonZeroUsize,
    partition_timeout: Duration,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
    components: &Arc<Components>,
) {
    components
        .partition_stream
        .stream()
        .map(|partition_id| {
            let components = Arc::clone(components);

            compact_partition(
                partition_id,
                partition_timeout,
                Arc::clone(&job_semaphore),
                components,
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
) {
    info!(partition_id = partition_id.get(), "compact partition",);
    let mut scratchpad = components.scratchpad_gen.pad();

    let res = tokio::time::timeout(
        partition_timeout,
        try_compact_partition(
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
    info!(partition_id = partition_id.get(), "compacted partition",);
}

/// Main function to compact files of a single partition.
///
/// Input: any files in the partitions (L0s, L1s, L2s)
/// Output:
/// 1. No overlapped  L0 files
/// 2. Up to N non-overlapped L1 and L2 files,  subject to  the total size of the files.
///
/// N: config max_number_of_files
///
/// Note that since individual files also have a maximum size limit, the
/// actual number of files can be more than  N.  Also since the Parquet format
/// features high and variable compression (page splits, RLE, zstd compression),
/// splits are based on estimated output file sizes which may deviate from actual file sizes
///
/// Algorithms
///
/// GENERAL IDEA OF THE CODE: DIVIDE & CONQUER  (we have not used all of its power yet)
///
/// The files are split into non-time-overlaped branches, each is compacted in parallel.
/// The output of each branch is then combined and re-branch in next round until
/// they should not be compacted based on defined stop conditions.
///
/// Example: Partition has 7 files: f1, f2, f3, f4, f5, f6, f7
///  Input: shown by their time range
///          |--f1--|               |----f3----|  |-f4-||-f5-||-f7-|
///               |------f2----------|                   |--f6--|
///
/// - Round 1: Assuming 7 files are split into 2 branches:
///  . Branch 1: has 3 files: f1, f2, f3
///  . Branch 2: has 4 files: f4, f5, f6, f7
///          |<------- Branch 1 -------------->|  |<-- Branch 2 -->|
///          |--f1--|               |----f3----|  |-f4-||-f5-||-f7-|
///               |------f2----------|                   |--f6--|
///
///    Output: 3 files f8, f9, f10
///          |<------- Branch 1 -------------->|  |<-- Branch 2--->|
///          |---------f8---------------|--f9--|  |-----f10--------|
///
/// - Round 2: 3 files f8, f9, f10 are in one branch and compacted into 2 files
///    Output: two files f11, f12
///          |-------------------f11---------------------|---f12---|
///
/// - Stop condition meets and the final output is f11 & F12
///
/// The high level flow is:
///
///   . Mutiple rounds, each round process mutltiple branches. Each branch includes at most 200 files
///   . Each branch will compact files lowest level (aka start-level) into its next level (aka target-level), either:
///      - Compact many L0s into fewer and larger L0s. Start-level = target-level = 0
///      - Compact many L1s into fewer and larger L1s. Start-level = target-level = 1
///      - Compact (L0s & L1s) to L1s if there are L0s. Start-level = 0, target-level = 1
///      - Compact (L1s & L2s) to L2s if no L0s. Start-level = 1, target-level = 2
///      - Split L0s each of which overlaps with more than 1 L1s into many L0s, each overlaps with at most one L1 files
///      - Split L1s each of which overlaps with more than 1 L2s into many L1s, each overlaps with at most one L2 files
///   . Each branch does find non-overlaps and upgragde files to avoid unecessary recompacting.
///     The actually split files:
///      1. files_to _keep: do not compact these files because they are already higher than target level
///      2. files_to_upgrade: upgrade this initial-level files to target level because they are not overlap with
///          any target-level and initial-level files and large enough (> desired max size)
///      3. files_to_compact_or_split.: this is either files to compact or split and will be compacted or split accordingly

///
/// Example: 4 files: two L0s, two L1s and one L2
///  Input:
///                                      |-L0.1-||------L0.2-------|
///                  |-----L1.1-----||--L1.2--|
///     |----L2.1-----|
///
///  - Round 1: There are L0s, let compact L0s with L1s. But let split them first:
///    . files_higher_keep: L2.1 (higher leelve than targetlevel) and L1.1 (not overlapped wot any L0s)
///    . files_upgrade: L0.2
///    . files_compact: L0.1, L1.2
///    Output: 4 files
///                                               |------L1.4-------|
///                  |-----L1.1-----||-new L1.3 -|        ^
///     |----L2.1-----|                  ^               |
///                                      |        result of upgrading L0.2
///                            result of compacting L0.1, L1.2
///
///  - Round 2: Compact those 4 files
///    Output: two L2 files
///     |-----------------L2.2---------------------||-----L2.3------|
///
/// Note:
///   . If there are no L0s files in the partition, the first round can just compact L1s and L2s to L2s
///   . Round 2 happens or not depends on the stop condition
async fn try_compact_partition(
    partition_id: PartitionId,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
    components: Arc<Components>,
    scratchpad_ctx: &mut dyn Scratchpad,
) -> Result<(), DynError> {
    let mut files = components.partition_files_source.fetch(partition_id).await;
    let partition_info = components.partition_info_source.fetch(partition_id).await?;

    // loop for each "Round", consider each file in the partition
    loop {
        let round_info = components
            .round_info_source
            .calculate(&partition_info, &files)
            .await?;

        // This is the stop condition which will be different for different version of compaction
        // and describe where the filter is created at version_specific_partition_filters function
        if !components
            .partition_filter
            .apply(&partition_info, &files)
            .await?
        {
            return Ok(());
        }

        let (files_now, files_later) = components.round_split.split(files, round_info);

        // Each branch must not overlap with each other
        let branches = components
            .divide_initial
            .divide(files_now, round_info)
            .into_iter();

        let mut files_next = files_later;
        // loop for each "Branch"
        for branch in branches {
            let input_paths: Vec<ParquetFilePath> =
                branch.iter().map(ParquetFilePath::from).collect();

            // Identify the target level and files that should be
            // compacted together, upgraded, and kept for next round of
            // compaction
            let file_classification =
                components
                    .file_classifier
                    .classify(&partition_info, &round_info, branch);

            // Only skip partition if it has neither files to upgrade nor files to compactor_split
            if !file_classification.has_upgrade_files()
                && !components
                    // todo: rename partition_resource_limit_filter to partition_skip_filter
                    .partition_resource_limit_filter
                    .apply(
                        &partition_info,
                        &file_classification.files_to_compact_or_split.files(),
                    )
                    .await?
            {
                return Ok(());
            }

            // Compact
            let created_file_params = run_plans(
                &file_classification.files_to_compact_or_split,
                &partition_info,
                &components,
                file_classification.target_level,
                Arc::clone(&job_semaphore),
                scratchpad_ctx,
            )
            .await?;

            // upload files to real object store
            let created_file_params =
                upload_files_to_object_store(created_file_params, scratchpad_ctx).await;

            // clean scratchpad
            scratchpad_ctx.clean_from_scratchpad(&input_paths).await;

            // Update the catalog to reflect the newly created files, soft delete the compacted files and
            // update the upgraded files
            let files_to_delete = file_classification.files_to_compact_or_split.files();
            let (created_files, upgraded_files) = update_catalog(
                Arc::clone(&components),
                partition_id,
                files_to_delete,
                file_classification.files_to_upgrade,
                created_file_params,
                file_classification.target_level,
            )
            .await;

            // Extend created files, upgraded files and files_to_keep to files_next
            files_next.extend(created_files);
            files_next.extend(upgraded_files);
            files_next.extend(file_classification.files_to_keep);
        }

        files = files_next;
    }
}

/// Compact of split give files
async fn run_plans(
    files: &FilesToCompactOrSplit,
    partition_info: &Arc<PartitionInfo>,
    components: &Arc<Components>,
    target_level: CompactionLevel,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
    scratchpad_ctx: &mut dyn Scratchpad,
) -> Result<Vec<ParquetFileParams>, DynError> {
    match files {
        FilesToCompactOrSplit::FilesToCompact(files) => {
            run_compaction_plan(
                files,
                partition_info,
                components,
                target_level,
                job_semaphore,
                scratchpad_ctx,
            )
            .await
        }
        FilesToCompactOrSplit::FilesToSplit(files) => {
            run_split_plans(
                files,
                partition_info,
                components,
                target_level,
                job_semaphore,
                scratchpad_ctx,
            )
            .await
        }
        FilesToCompactOrSplit::FilesWithTinyTimeRange(_files) => {
            // TODO: implement this
            Ok(vec![])
        }
    }
}

/// Compact `files` into a new parquet file of the the given target_level
async fn run_compaction_plan(
    files: &[ParquetFile],
    partition_info: &Arc<PartitionInfo>,
    components: &Arc<Components>,
    target_level: CompactionLevel,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
    scratchpad_ctx: &mut dyn Scratchpad,
) -> Result<Vec<ParquetFileParams>, DynError> {
    if files.is_empty() {
        return Ok(vec![]);
    }

    // stage files
    let input_paths: Vec<ParquetFilePath> = files.iter().map(|f| f.into()).collect();
    let input_uuids_inpad = scratchpad_ctx.load_to_scratchpad(&input_paths).await;
    let branch_inpad: Vec<_> = files
        .iter()
        .zip(input_uuids_inpad)
        .map(|(f, uuid)| ParquetFile {
            object_store_id: uuid,
            ..f.clone()
        })
        .collect();

    let plan_ir =
        components
            .ir_planner
            .compact_plan(branch_inpad, Arc::clone(partition_info), target_level);

    execute_plan(
        plan_ir,
        partition_info,
        components,
        target_level,
        job_semaphore,
    )
    .await
}

/// Split each of given files into multiple files
async fn run_split_plans(
    files_to_split: &[FileToSplit],
    partition_info: &Arc<PartitionInfo>,
    components: &Arc<Components>,
    target_level: CompactionLevel,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
    scratchpad_ctx: &mut dyn Scratchpad,
) -> Result<Vec<ParquetFileParams>, DynError> {
    if files_to_split.is_empty() {
        return Ok(vec![]);
    }

    let mut created_file_params = vec![];
    for file_to_split in files_to_split {
        let x = run_split_plan(
            file_to_split,
            partition_info,
            components,
            target_level,
            Arc::clone(&job_semaphore),
            scratchpad_ctx,
        )
        .await?;
        created_file_params.extend(x);
    }

    Ok(created_file_params)
}

// Split a given file into multiple files
async fn run_split_plan(
    file_to_split: &FileToSplit,
    partition_info: &Arc<PartitionInfo>,
    components: &Arc<Components>,
    target_level: CompactionLevel,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
    scratchpad_ctx: &mut dyn Scratchpad,
) -> Result<Vec<ParquetFileParams>, DynError> {
    // stage files
    let input_path = (&file_to_split.file).into();
    let input_uuids_inpad = scratchpad_ctx.load_to_scratchpad(&[input_path]).await;
    let file_inpad = ParquetFile {
        object_store_id: input_uuids_inpad[0],
        ..file_to_split.file.clone()
    };

    let plan_ir = components.ir_planner.split_plan(
        file_inpad,
        file_to_split.split_times.clone(),
        Arc::clone(partition_info),
        target_level,
    );

    execute_plan(
        plan_ir,
        partition_info,
        components,
        target_level,
        job_semaphore,
    )
    .await
}

async fn execute_plan(
    plan_ir: PlanIR,
    partition_info: &Arc<PartitionInfo>,
    components: &Arc<Components>,
    target_level: CompactionLevel,
    job_semaphore: Arc<InstrumentedAsyncSemaphore>,
) -> Result<Vec<ParquetFileParams>, DynError> {
    let create = {
        // draw semaphore BEFORE creating the DataFusion plan and drop it directly AFTER finishing the
        // DataFusion computation (but BEFORE doing any additional external IO).
        //
        // We guard the DataFusion planning (that doesn't perform any IO) via the semaphore as well in case
        // DataFusion ever starts to pre-allocate buffers during the physical planning. To the best of our
        // knowledge, this is currently (2023-01-25) not the case but if this ever changes, then we are prepared.
        let permit = job_semaphore
            .acquire(None)
            .await
            .expect("semaphore not closed");
        info!(
            partition_id = partition_info.partition_id.get(),
            "job semaphore acquired",
        );

        let plan = components
            .df_planner
            .plan(&plan_ir, Arc::clone(partition_info))
            .await?;
        let streams = components.df_plan_exec.exec(plan);
        let job = components.parquet_files_sink.stream_into_file_sink(
            streams,
            Arc::clone(partition_info),
            target_level,
            &plan_ir,
        );

        // TODO: react to OOM and try to divide branch
        let res = job.await;

        drop(permit);
        info!(
            partition_id = partition_info.partition_id.get(),
            "job semaphore released",
        );

        res?
    };

    Ok(create)
}

async fn upload_files_to_object_store(
    created_file_params: Vec<ParquetFileParams>,
    scratchpad_ctx: &mut dyn Scratchpad,
) -> Vec<ParquetFileParams> {
    // Ipload files to real object store
    let output_files: Vec<ParquetFilePath> = created_file_params.iter().map(|p| p.into()).collect();
    let output_uuids = scratchpad_ctx.make_public(&output_files).await;

    // Update file params with object_store_id
    created_file_params
        .into_iter()
        .zip(output_uuids)
        .map(|(f, uuid)| ParquetFileParams {
            object_store_id: uuid,
            ..f
        })
        .collect()
}

/// Update the catalog to create, soft delete and upgrade corresponding given input
/// to provided target level
/// Return created and upgraded files
async fn update_catalog(
    components: Arc<Components>,
    partition_id: PartitionId,
    files_to_delete: Vec<ParquetFile>,
    files_to_upgrade: Vec<ParquetFile>,
    file_params_to_create: Vec<ParquetFileParams>,
    target_level: CompactionLevel,
) -> (Vec<ParquetFile>, Vec<ParquetFile>) {
    let created_ids = components
        .commit
        .commit(
            partition_id,
            &files_to_delete,
            &files_to_upgrade,
            &file_params_to_create,
            target_level,
        )
        .await;

    // Update created ids to their corresponding file params
    let created_file_params = file_params_to_create
        .into_iter()
        .zip(created_ids)
        .map(|(params, id)| ParquetFile::from_params(params, id))
        .collect::<Vec<_>>();

    // Update compaction_level for the files_to_upgrade
    let upgraded_files = files_to_upgrade
        .into_iter()
        .map(|mut f| {
            f.compaction_level = target_level;
            f
        })
        .collect::<Vec<_>>();

    (created_file_params, upgraded_files)
}
