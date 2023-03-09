//! layout tests for the pathalogical cases of large amounts of data
//! with a single timestamp.
//!
//! The compactor doesn't necessarily have to handle actually
//! splitting such files, but it shouldn't crash/ panic / etc either.
//!
//! See [crate::layout] module for detailed documentation

use data_types::CompactionLevel;

use crate::layouts::{layout_setup_builder, parquet_builder, run_layout_scenario, ONE_MB};

#[tokio::test]
async fn single_giant_file() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // This test a single file that is too large for the compactor to
    // split with a single timestamp. The compactor should not panic
    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(100)
                .with_max_time(100)
                .with_file_size_bytes(5 * 1000 * ONE_MB)
                .with_compaction_level(CompactionLevel::Initial),
        )
        .await;

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 4.88gb                                                                                "
    - "L0.1[100,100]       |-------------------------------------L0.1-------------------------------------|"
    - "WARNING: file L0.1[100,100] 4.88gb exceeds soft limit 100mb by more than 50%"
    - "Committing partition 1:"
    - "  Upgrading 1 files level to CompactionLevel::L1: L0.1"
    - "Committing partition 1:"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.1"
    - "**** Final Output Files "
    - "L2, all files 4.88gb                                                                                "
    - "L2.1[100,100]       |-------------------------------------L2.1-------------------------------------|"
    - "WARNING: file L2.1[100,100] 4.88gb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

#[tokio::test]
async fn two_giant_files() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // This has two large overlapping files that the compactor can't
    // split as they have a single timestamp. The compactor should not
    // panic
    for _ in 0..2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(100)
                    .with_max_time(100)
                    .with_file_size_bytes(5 * 1000 * ONE_MB)
                    .with_compaction_level(CompactionLevel::Initial),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 4.88gb                                                                                "
    - "L0.1[100,100]       |-------------------------------------L0.1-------------------------------------|"
    - "L0.2[100,100]       |-------------------------------------L0.2-------------------------------------|"
    - "WARNING: file L0.1[100,100] 4.88gb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.2[100,100] 4.88gb exceeds soft limit 100mb by more than 50%"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 268435456. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L0, all files 4.88gb                                                                                "
    - "L0.1[100,100]       |-------------------------------------L0.1-------------------------------------|"
    - "L0.2[100,100]       |-------------------------------------L0.2-------------------------------------|"
    - "WARNING: file L0.1[100,100] 4.88gb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.2[100,100] 4.88gb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

#[tokio::test]
async fn two_giant_files_time_range_1() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // This has two large overlapping files that the compactor can't
    // split as they have a single timestamp. The compactor should not
    // panic
    for _ in 0..2 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(100)
                    .with_max_time(101)
                    .with_file_size_bytes(5 * 1000 * ONE_MB)
                    .with_compaction_level(CompactionLevel::Initial),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 4.88gb                                                                                "
    - "L0.1[100,101]       |-------------------------------------L0.1-------------------------------------|"
    - "L0.2[100,101]       |-------------------------------------L0.2-------------------------------------|"
    - "WARNING: file L0.1[100,101] 4.88gb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.2[100,101] 4.88gb exceeds soft limit 100mb by more than 50%"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 268435456. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L0, all files 4.88gb                                                                                "
    - "L0.1[100,101]       |-------------------------------------L0.1-------------------------------------|"
    - "L0.2[100,101]       |-------------------------------------L0.2-------------------------------------|"
    - "WARNING: file L0.1[100,101] 4.88gb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.2[100,101] 4.88gb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

#[tokio::test]
async fn many_medium_files() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // The compactor has 20 files overlapping files with a single
    // timestamp that indivdually are small enough to be processed,
    // but when compacted together are too large and can't be split by
    // timestamp
    for _ in 0..20 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(100)
                    .with_max_time(100)
                    .with_file_size_bytes(30 * ONE_MB)
                    .with_compaction_level(CompactionLevel::Initial),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 30mb                                                                                  "
    - "L0.1[100,100]       |-------------------------------------L0.1-------------------------------------|"
    - "L0.2[100,100]       |-------------------------------------L0.2-------------------------------------|"
    - "L0.3[100,100]       |-------------------------------------L0.3-------------------------------------|"
    - "L0.4[100,100]       |-------------------------------------L0.4-------------------------------------|"
    - "L0.5[100,100]       |-------------------------------------L0.5-------------------------------------|"
    - "L0.6[100,100]       |-------------------------------------L0.6-------------------------------------|"
    - "L0.7[100,100]       |-------------------------------------L0.7-------------------------------------|"
    - "L0.8[100,100]       |-------------------------------------L0.8-------------------------------------|"
    - "L0.9[100,100]       |-------------------------------------L0.9-------------------------------------|"
    - "L0.10[100,100]      |------------------------------------L0.10-------------------------------------|"
    - "L0.11[100,100]      |------------------------------------L0.11-------------------------------------|"
    - "L0.12[100,100]      |------------------------------------L0.12-------------------------------------|"
    - "L0.13[100,100]      |------------------------------------L0.13-------------------------------------|"
    - "L0.14[100,100]      |------------------------------------L0.14-------------------------------------|"
    - "L0.15[100,100]      |------------------------------------L0.15-------------------------------------|"
    - "L0.16[100,100]      |------------------------------------L0.16-------------------------------------|"
    - "L0.17[100,100]      |------------------------------------L0.17-------------------------------------|"
    - "L0.18[100,100]      |------------------------------------L0.18-------------------------------------|"
    - "L0.19[100,100]      |------------------------------------L0.19-------------------------------------|"
    - "L0.20[100,100]      |------------------------------------L0.20-------------------------------------|"
    - "**** Simulation run 0, type=compact. 8 Input Files, 240mb total:"
    - "L0, all files 30mb                                                                                  "
    - "L0.20[100,100]      |------------------------------------L0.20-------------------------------------|"
    - "L0.19[100,100]      |------------------------------------L0.19-------------------------------------|"
    - "L0.18[100,100]      |------------------------------------L0.18-------------------------------------|"
    - "L0.17[100,100]      |------------------------------------L0.17-------------------------------------|"
    - "L0.16[100,100]      |------------------------------------L0.16-------------------------------------|"
    - "L0.15[100,100]      |------------------------------------L0.15-------------------------------------|"
    - "L0.14[100,100]      |------------------------------------L0.14-------------------------------------|"
    - "L0.13[100,100]      |------------------------------------L0.13-------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 240mb total:"
    - "L1, all files 240mb                                                                                 "
    - "L1.?[100,100]       |-------------------------------------L1.?-------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 8 files: L0.13, L0.14, L0.15, L0.16, L0.17, L0.18, L0.19, L0.20"
    - "  Creating 1 files at level CompactionLevel::L1"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 268435456. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L0                                                                                                  "
    - "L0.1[100,100] 30mb  |-------------------------------------L0.1-------------------------------------|"
    - "L0.2[100,100] 30mb  |-------------------------------------L0.2-------------------------------------|"
    - "L0.3[100,100] 30mb  |-------------------------------------L0.3-------------------------------------|"
    - "L0.4[100,100] 30mb  |-------------------------------------L0.4-------------------------------------|"
    - "L0.5[100,100] 30mb  |-------------------------------------L0.5-------------------------------------|"
    - "L0.6[100,100] 30mb  |-------------------------------------L0.6-------------------------------------|"
    - "L0.7[100,100] 30mb  |-------------------------------------L0.7-------------------------------------|"
    - "L0.8[100,100] 30mb  |-------------------------------------L0.8-------------------------------------|"
    - "L0.9[100,100] 30mb  |-------------------------------------L0.9-------------------------------------|"
    - "L0.10[100,100] 30mb |------------------------------------L0.10-------------------------------------|"
    - "L0.11[100,100] 30mb |------------------------------------L0.11-------------------------------------|"
    - "L0.12[100,100] 30mb |------------------------------------L0.12-------------------------------------|"
    - "L1                                                                                                  "
    - "L1.21[100,100] 240mb|------------------------------------L1.21-------------------------------------|"
    - "WARNING: file L1.21[100,100] 240mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

#[tokio::test]
async fn many_medium_files_time_range_1() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // The compactor has 20 files overlapping files with a single
    // timestamp that indivdually are small enough to be processed,
    // but when compacted together are too large and can't be split by
    // timestamp
    for _ in 0..20 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(100)
                    .with_max_time(101)
                    .with_file_size_bytes(30 * ONE_MB)
                    .with_compaction_level(CompactionLevel::Initial),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 30mb                                                                                  "
    - "L0.1[100,101]       |-------------------------------------L0.1-------------------------------------|"
    - "L0.2[100,101]       |-------------------------------------L0.2-------------------------------------|"
    - "L0.3[100,101]       |-------------------------------------L0.3-------------------------------------|"
    - "L0.4[100,101]       |-------------------------------------L0.4-------------------------------------|"
    - "L0.5[100,101]       |-------------------------------------L0.5-------------------------------------|"
    - "L0.6[100,101]       |-------------------------------------L0.6-------------------------------------|"
    - "L0.7[100,101]       |-------------------------------------L0.7-------------------------------------|"
    - "L0.8[100,101]       |-------------------------------------L0.8-------------------------------------|"
    - "L0.9[100,101]       |-------------------------------------L0.9-------------------------------------|"
    - "L0.10[100,101]      |------------------------------------L0.10-------------------------------------|"
    - "L0.11[100,101]      |------------------------------------L0.11-------------------------------------|"
    - "L0.12[100,101]      |------------------------------------L0.12-------------------------------------|"
    - "L0.13[100,101]      |------------------------------------L0.13-------------------------------------|"
    - "L0.14[100,101]      |------------------------------------L0.14-------------------------------------|"
    - "L0.15[100,101]      |------------------------------------L0.15-------------------------------------|"
    - "L0.16[100,101]      |------------------------------------L0.16-------------------------------------|"
    - "L0.17[100,101]      |------------------------------------L0.17-------------------------------------|"
    - "L0.18[100,101]      |------------------------------------L0.18-------------------------------------|"
    - "L0.19[100,101]      |------------------------------------L0.19-------------------------------------|"
    - "L0.20[100,101]      |------------------------------------L0.20-------------------------------------|"
    - "**** Simulation run 0, type=compact. 8 Input Files, 240mb total:"
    - "L0, all files 30mb                                                                                  "
    - "L0.20[100,101]      |------------------------------------L0.20-------------------------------------|"
    - "L0.19[100,101]      |------------------------------------L0.19-------------------------------------|"
    - "L0.18[100,101]      |------------------------------------L0.18-------------------------------------|"
    - "L0.17[100,101]      |------------------------------------L0.17-------------------------------------|"
    - "L0.16[100,101]      |------------------------------------L0.16-------------------------------------|"
    - "L0.15[100,101]      |------------------------------------L0.15-------------------------------------|"
    - "L0.14[100,101]      |------------------------------------L0.14-------------------------------------|"
    - "L0.13[100,101]      |------------------------------------L0.13-------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 240mb total:"
    - "L1, all files 240mb                                                                                 "
    - "L1.?[100,101]       |-------------------------------------L1.?-------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 8 files: L0.13, L0.14, L0.15, L0.16, L0.17, L0.18, L0.19, L0.20"
    - "  Creating 1 files at level CompactionLevel::L1"
    - "SKIPPED COMPACTION for PartitionId(1): partition 1 has overlapped files that exceed max compact size limit 268435456. The may happen if a large amount of data has the same timestamp"
    - "**** Final Output Files "
    - "L0                                                                                                  "
    - "L0.1[100,101] 30mb  |-------------------------------------L0.1-------------------------------------|"
    - "L0.2[100,101] 30mb  |-------------------------------------L0.2-------------------------------------|"
    - "L0.3[100,101] 30mb  |-------------------------------------L0.3-------------------------------------|"
    - "L0.4[100,101] 30mb  |-------------------------------------L0.4-------------------------------------|"
    - "L0.5[100,101] 30mb  |-------------------------------------L0.5-------------------------------------|"
    - "L0.6[100,101] 30mb  |-------------------------------------L0.6-------------------------------------|"
    - "L0.7[100,101] 30mb  |-------------------------------------L0.7-------------------------------------|"
    - "L0.8[100,101] 30mb  |-------------------------------------L0.8-------------------------------------|"
    - "L0.9[100,101] 30mb  |-------------------------------------L0.9-------------------------------------|"
    - "L0.10[100,101] 30mb |------------------------------------L0.10-------------------------------------|"
    - "L0.11[100,101] 30mb |------------------------------------L0.11-------------------------------------|"
    - "L0.12[100,101] 30mb |------------------------------------L0.12-------------------------------------|"
    - "L1                                                                                                  "
    - "L1.21[100,101] 240mb|------------------------------------L1.21-------------------------------------|"
    - "WARNING: file L1.21[100,101] 240mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}

#[tokio::test]
async fn many_small_files() {
    test_helpers::maybe_start_logging();
    let setup = layout_setup_builder().await.build().await;

    // The compactor has 20 files overlapping files that together are
    // below the partition limit (256MB) but above the target file
    // size limit (100MB) but can't be split by timestamp
    for _ in 0..20 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(100)
                    .with_max_time(100)
                    .with_file_size_bytes(10 * ONE_MB)
                    .with_compaction_level(CompactionLevel::Initial),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0, all files 10mb                                                                                  "
    - "L0.1[100,100]       |-------------------------------------L0.1-------------------------------------|"
    - "L0.2[100,100]       |-------------------------------------L0.2-------------------------------------|"
    - "L0.3[100,100]       |-------------------------------------L0.3-------------------------------------|"
    - "L0.4[100,100]       |-------------------------------------L0.4-------------------------------------|"
    - "L0.5[100,100]       |-------------------------------------L0.5-------------------------------------|"
    - "L0.6[100,100]       |-------------------------------------L0.6-------------------------------------|"
    - "L0.7[100,100]       |-------------------------------------L0.7-------------------------------------|"
    - "L0.8[100,100]       |-------------------------------------L0.8-------------------------------------|"
    - "L0.9[100,100]       |-------------------------------------L0.9-------------------------------------|"
    - "L0.10[100,100]      |------------------------------------L0.10-------------------------------------|"
    - "L0.11[100,100]      |------------------------------------L0.11-------------------------------------|"
    - "L0.12[100,100]      |------------------------------------L0.12-------------------------------------|"
    - "L0.13[100,100]      |------------------------------------L0.13-------------------------------------|"
    - "L0.14[100,100]      |------------------------------------L0.14-------------------------------------|"
    - "L0.15[100,100]      |------------------------------------L0.15-------------------------------------|"
    - "L0.16[100,100]      |------------------------------------L0.16-------------------------------------|"
    - "L0.17[100,100]      |------------------------------------L0.17-------------------------------------|"
    - "L0.18[100,100]      |------------------------------------L0.18-------------------------------------|"
    - "L0.19[100,100]      |------------------------------------L0.19-------------------------------------|"
    - "L0.20[100,100]      |------------------------------------L0.20-------------------------------------|"
    - "**** Simulation run 0, type=compact. 20 Input Files, 200mb total:"
    - "L0, all files 10mb                                                                                  "
    - "L0.20[100,100]      |------------------------------------L0.20-------------------------------------|"
    - "L0.19[100,100]      |------------------------------------L0.19-------------------------------------|"
    - "L0.18[100,100]      |------------------------------------L0.18-------------------------------------|"
    - "L0.17[100,100]      |------------------------------------L0.17-------------------------------------|"
    - "L0.16[100,100]      |------------------------------------L0.16-------------------------------------|"
    - "L0.15[100,100]      |------------------------------------L0.15-------------------------------------|"
    - "L0.14[100,100]      |------------------------------------L0.14-------------------------------------|"
    - "L0.13[100,100]      |------------------------------------L0.13-------------------------------------|"
    - "L0.12[100,100]      |------------------------------------L0.12-------------------------------------|"
    - "L0.11[100,100]      |------------------------------------L0.11-------------------------------------|"
    - "L0.10[100,100]      |------------------------------------L0.10-------------------------------------|"
    - "L0.9[100,100]       |-------------------------------------L0.9-------------------------------------|"
    - "L0.8[100,100]       |-------------------------------------L0.8-------------------------------------|"
    - "L0.7[100,100]       |-------------------------------------L0.7-------------------------------------|"
    - "L0.6[100,100]       |-------------------------------------L0.6-------------------------------------|"
    - "L0.5[100,100]       |-------------------------------------L0.5-------------------------------------|"
    - "L0.4[100,100]       |-------------------------------------L0.4-------------------------------------|"
    - "L0.3[100,100]       |-------------------------------------L0.3-------------------------------------|"
    - "L0.2[100,100]       |-------------------------------------L0.2-------------------------------------|"
    - "L0.1[100,100]       |-------------------------------------L0.1-------------------------------------|"
    - "**** 1 Output Files (parquet_file_id not yet assigned), 200mb total:"
    - "L1, all files 200mb                                                                                 "
    - "L1.?[100,100]       |-------------------------------------L1.?-------------------------------------|"
    - "Committing partition 1:"
    - "  Soft Deleting 20 files: L0.1, L0.2, L0.3, L0.4, L0.5, L0.6, L0.7, L0.8, L0.9, L0.10, L0.11, L0.12, L0.13, L0.14, L0.15, L0.16, L0.17, L0.18, L0.19, L0.20"
    - "  Creating 1 files at level CompactionLevel::L1"
    - "Committing partition 1:"
    - "  Upgrading 1 files level to CompactionLevel::L2: L1.21"
    - "**** Final Output Files "
    - "L2, all files 200mb                                                                                 "
    - "L2.21[100,100]      |------------------------------------L2.21-------------------------------------|"
    - "WARNING: file L2.21[100,100] 200mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}
