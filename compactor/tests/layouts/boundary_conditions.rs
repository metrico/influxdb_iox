//! layout tests for various boundary condition scenarios for compactor
//!
//! See [crate::layout] module for detailed documentation

use data_types::CompactionLevel;
use iox_time::Time;
use std::time::Duration;

use crate::layouts::{layout_setup_builder, parquet_builder, run_layout_scenario, ONE_MB};

#[tokio::test]
async fn many_small_files_empty_branch() {
    test_helpers::maybe_start_logging();

    const MAX_DESIRED_FILE_SIZE: u64 = 100 * ONE_MB;
    const MAX_FILES_PER_PLAN: i64 = 8;
    const SMALL_FILE_COUNT: i64 = MAX_FILES_PER_PLAN * 2;
    let setup = layout_setup_builder()
        .await
        .with_max_num_files_per_plan(20)
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .with_partition_timeout(Duration::from_millis(100000))
        .with_suppress_run_output() // remove this to debug
        .with_writes_breakdown()
        .with_max_num_files_per_plan(MAX_FILES_PER_PLAN as usize)
        .build()
        .await;

    // We need enough small files to trigger the many small files.
    // This number of files must work out to some number of full branches (i.e. no leftover files to spill into another branch)
    for i in 0..SMALL_FILE_COUNT {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time((i - 1) * 10)
                    .with_max_time(i * 10)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i * 10 - 1))
                    .with_file_size_bytes(100),
            )
            .await;
    }

    // Now files large enough ManySmallFiles will filter them out.
    // Since the prior files didn't spill over into a partially filled branch, these get their own branch.
    // Since they're big enough that ManySmallFiles won't compact them, their branch will be empty.
    for i in SMALL_FILE_COUNT..SMALL_FILE_COUNT + 2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time((i - 1) * 10)
                    .with_max_time(i * 10)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i * 10 - 1))
                    .with_file_size_bytes(MAX_DESIRED_FILE_SIZE),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.1[-10,0] -1ns 100b    |L0.1|                                                                                    "
    - "L0.2[0,10] 9ns 100b           |L0.2|                                                                               "
    - "L0.3[10,20] 19ns 100b              |L0.3|                                                                          "
    - "L0.4[20,30] 29ns 100b                   |L0.4|                                                                     "
    - "L0.5[30,40] 39ns 100b                        |L0.5|                                                                "
    - "L0.6[40,50] 49ns 100b                             |L0.6|                                                           "
    - "L0.7[50,60] 59ns 100b                                  |L0.7|                                                      "
    - "L0.8[60,70] 69ns 100b                                       |L0.8|                                                 "
    - "L0.9[70,80] 79ns 100b                                            |L0.9|                                            "
    - "L0.10[80,90] 89ns 100b                                                |L0.10|                                      "
    - "L0.11[90,100] 99ns 100b                                                    |L0.11|                                 "
    - "L0.12[100,110] 109ns 100b                                                       |L0.12|                            "
    - "L0.13[110,120] 119ns 100b                                                            |L0.13|                       "
    - "L0.14[120,130] 129ns 100b                                                                 |L0.14|                  "
    - "L0.15[130,140] 139ns 100b                                                                      |L0.15|             "
    - "L0.16[140,150] 149ns 100b                                                                           |L0.16|        "
    - "L0.17[150,160] 159ns 100mb                                                                                |L0.17|   "
    - "L0.18[160,170] 169ns 100mb                                                                                     |L0.18|"
    - "**** Final Output Files (200mb written)"
    - "L2                                                                                                                 "
    - "L2.21[-10,80] 169ns 101mb|-------------------L2.21-------------------|                                             "
    - "L2.22[81,170] 169ns 99mb                                              |------------------L2.22-------------------| "
    - "**** Breakdown of where bytes were written"
    - 200mb written by split(CompactAndSplitOutput(TotalSizeLessThanMaxCompactSize))
    - 2kb written by compact(ManySmallFiles)
    "###
    );
}
