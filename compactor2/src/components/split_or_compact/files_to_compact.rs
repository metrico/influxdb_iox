use data_types::{CompactionLevel, ParquetFile, Timestamp};

use crate::components::{
    divide_initial::multiple_branches::order_files,
    files_split::{target_level_split::TargetLevelSplit, FilesSplit},
};

/// Return (`[files_to_compact]`, `[files_to_keep]`) of given files
/// such that `files_to_compact` are files to compact that under max_compact_size limit
/// and `files_to_keep` are the rest of the files that will be considered to compact in next round
///
/// To deduplicate data correctly, we need to select start-level files in their max_l0_created_at order
/// if they are level 0 or in min_time order otherwise, and they must be compacted with overlapped files
/// in target level. See example below for the correlation between start-level order and overlapped time ranges of files
/// 
/// This function is only invoked after after all start_level files are split and each overlaps with at most one target_level file
///
/// Example:
///
///  Input Files: three L0 and threee L1 files. The ID after the dot is the order the files are created
///     |---L0.1---|  |---L0.3---|  |---L0.2---|  Note that L0.2 is created BEFORE L0.3 but has LATER time range
///   |---L1.1---|  |---L1.2---|  |---L1.3---|
///
///  Output of files_to_compact: only 3 possible choices:
///   1. Smallest compacting set: L0.1 + L1.1
///   2. Medium size compacting set: L0.1 + L1.1 + L0.2 + L1.2 + L1.3
///      Note that L1.2 overlaps with the time range of L0.1 + L0.2 and must be included here
///   3. Largest compacting set: All input files
///
/// If the smallest compacting set is still over the limit, they will be return for splitting the smaller sets
///
pub fn limit_files_to_compact(
    max_compact_size: usize,
    files: Vec<ParquetFile>,
    target_level: CompactionLevel,
) -> LimitedFileToCompact { //(Vec<ParquetFile>, Vec<ParquetFile>) {
    // panic if not all files are either in target level or start level
    let start_level = target_level.prev();
    assert!(files
        .iter()
        .all(|f| f.compaction_level == target_level || f.compaction_level == start_level));

    // Get start-level and target-level files
    let len = files.len();
    let split = TargetLevelSplit::new();
    let (start_level_files, mut target_level_files) = split.apply(files, start_level);

    // Order start-level files to group the files to compact them correctly
    let start_level_files = order_files(start_level_files, start_level);
    let mut start_level_files = start_level_files.into_iter();

    // Go over start-level files and find overlapped files in target level
    let mut start_level_files_to_compact = Vec::with_capacity(len);
    let mut target_level_files_to_compact = Vec::with_capacity(len);
    let mut files_to_further_split = Vec::with_capacity(len);
    let mut files_to_keep = Vec::with_capacity(len);
    let mut total_size = 0;

    for file in start_level_files.by_ref() {
        // A start-level file, if compacted, must be compacted with all of its overlapped target-level files.
        // Thus compute the size needed before deciding to compact this file and its overlaps or not

        // Time range of start_level_files_to_compact plus this file
        let (min_time, max_time) = time_range(&file, &start_level_files_to_compact);

        // Get all target-level files that overlaps with the time range and not yet in target_level_files_to_compact
        let overlapped_files: Vec<&ParquetFile> = target_level_files
            .iter()
            .filter(|f| f.overlaps_time_range(min_time, max_time))
            .filter(|f| !target_level_files_to_compact.iter().any(|x| x == *f))
            .collect();

        // Size of the file and its overlapped files
        let size = file.file_size_bytes
            + overlapped_files
                .iter()
                .map(|f| f.file_size_bytes)
                .sum::<i64>();

        // If total size is under limit, add this file and its overlapped files to files_to_compact
        if total_size + size <= max_compact_size as i64 {
            start_level_files_to_compact.push(file);
            target_level_files_to_compact
                .extend(overlapped_files.into_iter().cloned().collect::<Vec<_>>());
            total_size += size;
        } else {
            // Over limit, stop here
            if start_level_files_to_compact.is_empty() {
                // nothing to compact, return this minimum compacting set of further spliting
                files_to_further_split.push(file);
                // since there is only one start_level file, the number of overlapped target_level must <= 1
                assert!(overlapped_files.len() <= 1);
                files_to_further_split.extend(overlapped_files.into_iter().cloned().collect::<Vec<_>>());
            } else {
                files_to_keep.push(file);
            }
            break;
        }
    }

    // Remove all files in target_level_files_to_compact and filess_to_further_split from target_level_files
    target_level_files.retain(|f| !target_level_files_to_compact.iter().any(|x| x == f));
    target_level_files.retain(|f| !files_to_further_split.iter().any(|x| x == f));

    // All files left in start_level_files  and target_level_files are kept for next round
    target_level_files.extend(start_level_files);
    files_to_keep.extend(target_level_files);

    // All files in start_level_files_to_compact and target_level_files_to_compact will be compacted
    let files_to_compact = start_level_files_to_compact
        .into_iter()
        .chain(target_level_files_to_compact.into_iter())
        .collect::<Vec<_>>();

    // Sanity check
    // all files are returned
    assert_eq!(files_to_compact.len() + files_to_further_split.len() + files_to_keep.len(), len);
    // either compact or further split has to be empty
    assert!(files_to_compact.is_empty() || files_to_further_split.is_empty());

    // Return the files to compact and the files to keep
    let files_to_compact_or_further_split = if files_to_compact.is_empty() {
        CompactOrFurtherSplit::FurtherSplit(files_to_further_split)
    } else {
        CompactOrFurtherSplit::Compact(files_to_compact)
    };

    LimitedFileToCompact {
        files_to_compact_or_further_split,
        files_to_keep,
    }
}

/// Return time range of the given file and the list of given files
fn time_range(file: &ParquetFile, files: &[ParquetFile]) -> (Timestamp, Timestamp) {
    let mut min_time = file.min_time;
    let mut max_time = file.max_time;
    files.iter().for_each(|f| {
        min_time = min_time.min(f.min_time);
        max_time = max_time.max(f.max_time);
    });

    (min_time, max_time)
}

/// Struct for the limited files_to_compact_or_further_plit and files_to_keep
pub struct LimitedFileToCompact {
    files_to_compact_or_further_split: CompactOrFurtherSplit,
    files_to_keep: Vec<ParquetFile>,
}

impl LimitedFileToCompact {
    
    pub fn files_to_compact(&self) -> Vec<ParquetFile> {
        match &self.files_to_compact_or_further_split {
            CompactOrFurtherSplit::Compact(files) => files.clone(),
            CompactOrFurtherSplit::FurtherSplit(_) => vec![],
        }
    }

    pub fn files_to_further_split(&self) -> Vec<ParquetFile> {
        match &self.files_to_compact_or_further_split {
            CompactOrFurtherSplit::Compact(_) => vec![],
            CompactOrFurtherSplit::FurtherSplit(files) => files.clone(),
        }
    }

    pub fn files_to_keep(&self) -> Vec<ParquetFile> {
        self.files_to_keep.clone()
    }   
}


/// Files to either compact or to further split
pub enum CompactOrFurtherSplit {
    // These overlapped files are small enough to be compacted
    Compact (Vec<ParquetFile>),
    // These overlapped files are the minimum set to compact but still too large to do so
    FurtherSplit (Vec<ParquetFile>),
}
#[cfg(test)]
mod tests {
    use compactor2_test_utils::{
        create_l1_files, create_overlapped_files, create_overlapped_l0_l1_files_2,
        create_overlapped_start_target_files, format_files, format_files_split,
    };
    use data_types::CompactionLevel;

    use crate::components::split_or_compact::files_to_compact::{limit_files_to_compact};

    const MAX_SIZE: usize = 100;

    #[test]
    fn test_compact_empty() {
        let files = vec![];
        let files_to_compact_or_further_split =
            limit_files_to_compact(MAX_SIZE, files, CompactionLevel::Initial);
        assert!(files_to_compact_or_further_split.files_to_compact().is_empty());
        assert!(files_to_compact_or_further_split.files_to_further_split().is_empty());
        assert!(files_to_compact_or_further_split.files_to_keep().is_empty());
    }

    #[test]
    #[should_panic]
    fn test_compact_wrong_target_level() {
        // all L1 files
        let files = create_l1_files(1);

        // Target is L0 while all files are in L1 --> panic
        let _files_to_compact_or_further_split =
            limit_files_to_compact(MAX_SIZE, files, CompactionLevel::Initial);
    }

    #[test]
    #[should_panic]
    fn test_compact_files_three_level_files() {
        // Three level files
        let files = create_overlapped_files();
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0                                                                                                  "
        - "L0.2[650,750] 1b                                                             |-L0.2-|               "
        - "L0.1[450,620] 1b                                            |----L0.1-----|                         "
        - "L0.3[800,900] 100b                                                                         |-L0.3-| "
        - "L1                                                                                                  "
        - "L1.13[600,700] 100b                                                      |L1.13-|                   "
        - "L1.12[400,500] 1b                                      |L1.12-|                                     "
        - "L1.11[250,350] 1b                         |L1.11-|                                                  "
        - "L2                                                                                                  "
        - "L2.21[0,100] 1b     |L2.21-|                                                                        "
        - "L2.22[200,300] 1b                    |L2.22-|                                                       "
        "###
        );

        // panic because it only handle at most 2 levels next to each other
        let _files_to_compact_or_further_split =
            limit_files_to_compact(MAX_SIZE, files, CompactionLevel::FileNonOverlapped);
    }

    #[test]
    fn test_compact_files_no_limit() {
        let files = create_overlapped_l0_l1_files_2(MAX_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                  "
        - "L0.2[650,750]                                               |-----L0.2-----|                        "
        - "L0.1[450,620]               |----------L0.1-----------|                                             "
        - "L0.3[800,900]                                                                       |-----L0.3-----|"
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]                                      |----L1.13-----|                                "
        - "L1.12[400,500]      |----L1.12-----|                                                                "
        "###
        );

        // size limit > total size --> files to compact = all L0s and overalapped L1s
        let files_to_compact_or_further_split =
            limit_files_to_compact(MAX_SIZE * 5 + 1, files, CompactionLevel::FileNonOverlapped);

        let files_to_compact = files_to_compact_or_further_split.files_to_compact();
        let files_to_keep = files_to_compact_or_further_split.files_to_keep();

        assert_eq!(files_to_compact.len(), 5);
        assert_eq!(files_to_keep.len(), 0);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L0, all files 100b                                                                                  "
        - "L0.1[450,620]               |----------L0.1-----------|                                             "
        - "L0.2[650,750]                                               |-----L0.2-----|                        "
        - "L0.3[800,900]                                                                       |-----L0.3-----|"
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]                                      |----L1.13-----|                                "
        - "L1.12[400,500]      |----L1.12-----|                                                                "
        - "files to keep:"
        "###
        );
    }

    // 
    #[test]
    fn test_compact_files_limit_too_small() {
        let files = create_overlapped_l0_l1_files_2(MAX_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                  "
        - "L0.2[650,750]                                               |-----L0.2-----|                        "
        - "L0.1[450,620]               |----------L0.1-----------|                                             "
        - "L0.3[800,900]                                                                       |-----L0.3-----|"
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]                                      |----L1.13-----|                                "
        - "L1.12[400,500]      |----L1.12-----|                                                                "
        "###
        );

        // size limit too small to compact anything
        let files_to_compact_or_further_split  =
            limit_files_to_compact(MAX_SIZE, files, CompactionLevel::FileNonOverlapped);

        let files_to_compact = files_to_compact_or_further_split.files_to_compact();
        let files_to_further_split = files_to_compact_or_further_split.files_to_further_split();
        let files_to_keep = files_to_compact_or_further_split.files_to_keep();

        assert_eq!(files_to_compact.len(), 0);
        // minimum set nneds to get compacted but too big to do so: L0.1, L1.12, L1.13
        assert_eq!(files_to_further_split.len(), 3);  
        assert_eq!(files_to_keep.len(), 2);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to further split:", &files_to_further_split, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to further split:"
        - "L0, all files 100b                                                                                  "
        - "L0.1[450,620]                    |-------------------L0.1--------------------|                      "
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]                                                           |---------L1.13----------| "
        - "L1.12[400,500]      |---------L1.12----------|                                                      "
        - "files to keep:"
        - "L0, all files 100b                                                                                  "
        - "L0.2[650,750]       |-------------L0.2-------------|                                                "
        - "L0.3[800,900]                                                       |-------------L0.3-------------|"
        "###
        );
    }

    #[test]
    fn test_compact_files_limit() {
        let files = create_overlapped_l0_l1_files_2(MAX_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                  "
        - "L0.2[650,750]                                               |-----L0.2-----|                        "
        - "L0.1[450,620]               |----------L0.1-----------|                                             "
        - "L0.3[800,900]                                                                       |-----L0.3-----|"
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]                                      |----L1.13-----|                                "
        - "L1.12[400,500]      |----L1.12-----|                                                                "
        "###
        );

        // size limit < total size --> only enough to compact L0.1 with L1.12 and L1.13
        let files_to_compact_or_further_split =
            limit_files_to_compact(MAX_SIZE * 3, files, CompactionLevel::FileNonOverlapped);
        
        let files_to_compact = files_to_compact_or_further_split.files_to_compact();
        let files_to_keep = files_to_compact_or_further_split.files_to_keep();

        assert_eq!(files_to_compact.len(), 3);
        assert_eq!(files_to_keep.len(), 2);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L0, all files 100b                                                                                  "
        - "L0.1[450,620]                    |-------------------L0.1--------------------|                      "
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]                                                           |---------L1.13----------| "
        - "L1.12[400,500]      |---------L1.12----------|                                                      "
        - "files to keep:"
        - "L0, all files 100b                                                                                  "
        - "L0.2[650,750]       |-------------L0.2-------------|                                                "
        - "L0.3[800,900]                                                       |-------------L0.3-------------|"
        "###
        );
    }

    #[test]
    fn test_compact_files_limit_2() {
        let files = create_overlapped_l0_l1_files_2(MAX_SIZE as i64);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                  "
        - "L0.2[650,750]                                               |-----L0.2-----|                        "
        - "L0.1[450,620]               |----------L0.1-----------|                                             "
        - "L0.3[800,900]                                                                       |-----L0.3-----|"
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]                                      |----L1.13-----|                                "
        - "L1.12[400,500]      |----L1.12-----|                                                                "
        "###
        );

        // size limit < total size --> only enough to compact L0.1, L0.2 with L1.12 and L1.13
        let files_to_compact_or_further_split =
            limit_files_to_compact(MAX_SIZE * 4, files, CompactionLevel::FileNonOverlapped);

        let files_to_compact = files_to_compact_or_further_split.files_to_compact();
        let files_to_keep = files_to_compact_or_further_split.files_to_keep();

        assert_eq!(files_to_compact.len(), 4);
        assert_eq!(files_to_keep.len(), 1);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L0, all files 100b                                                                                  "
        - "L0.1[450,620]                  |----------------L0.1----------------|                               "
        - "L0.2[650,750]                                                                |--------L0.2--------| "
        - "L1, all files 100b                                                                                  "
        - "L1.13[600,700]                                                   |-------L1.13--------|             "
        - "L1.12[400,500]      |-------L1.12--------|                                                          "
        - "files to keep:"
        - "L0, all files 100b                                                                                  "
        - "L0.3[800,900]       |-------------------------------------L0.3-------------------------------------|"
        "###
        );
    }

    #[test]
    fn test_compact_files_limit_3() {
        let files = create_overlapped_start_target_files(MAX_SIZE as i64, CompactionLevel::Initial);
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L0, all files 100b                                                                                  "
        - "L0.2[550,650]                                                                        |----L0.2----| "
        - "L0.1[150,250]              |----L0.1----|                                                           "
        - "L0.3[350,450]                                           |----L0.3----|                              "
        - "L1, all files 100b                                                                                  "
        - "L1.12[300,400]                                   |---L1.12----|                                     "
        - "L1.13[500,600]                                                                |---L1.13----|        "
        - "L1.11[100,200]      |---L1.11----|                                                                  "
        "###
        );

        // There are only 3 choices for compacting:
        //  1. Smallest set: L0.1 with L1.11
        //  2. Medium size set: L0.1, L0.2 with L1.11, L1.12, L1.13
        //  3. All files: L0.1, L0.2, L0.3 with L1.11, L1.12, L1.13

        // --------------------
        // size limit = MAX_SIZE * 3 to force the first choice, L0.1 with L1.11
        let files_to_compact_or_further_split = limit_files_to_compact(
            MAX_SIZE * 3,
            files.clone(),
            CompactionLevel::FileNonOverlapped,
        );

        let files_to_compact = files_to_compact_or_further_split.files_to_compact();
        let files_to_keep = files_to_compact_or_further_split.files_to_keep();

        assert_eq!(files_to_compact.len(), 2);
        assert_eq!(files_to_keep.len(), 4);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L0, all files 100b                                                                                  "
        - "L0.1[150,250]                                 |-----------------------L0.1------------------------| "
        - "L1, all files 100b                                                                                  "
        - "L1.11[100,200]      |-----------------------L1.11-----------------------|                           "
        - "files to keep:"
        - "L0, all files 100b                                                                                  "
        - "L0.2[550,650]                                                                |--------L0.2--------| "
        - "L0.3[350,450]                  |--------L0.3--------|                                               "
        - "L1, all files 100b                                                                                  "
        - "L1.12[300,400]      |-------L1.12--------|                                                          "
        - "L1.13[500,600]                                                   |-------L1.13--------|             "
        "###
        );

        // --------------------
        // size limit = MAX_SIZE * 4 to force the first choice, L0.1 with L1.11, becasue it still not enough to for second choice
        let files_to_compact_or_further_split = limit_files_to_compact(
            MAX_SIZE * 4,
            files.clone(),
            CompactionLevel::FileNonOverlapped,
        );

        let files_to_compact = files_to_compact_or_further_split.files_to_compact();
        let files_to_keep = files_to_compact_or_further_split.files_to_keep();

        assert_eq!(files_to_compact.len(), 2);
        assert_eq!(files_to_keep.len(), 4);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L0, all files 100b                                                                                  "
        - "L0.1[150,250]                                 |-----------------------L0.1------------------------| "
        - "L1, all files 100b                                                                                  "
        - "L1.11[100,200]      |-----------------------L1.11-----------------------|                           "
        - "files to keep:"
        - "L0, all files 100b                                                                                  "
        - "L0.2[550,650]                                                                |--------L0.2--------| "
        - "L0.3[350,450]                  |--------L0.3--------|                                               "
        - "L1, all files 100b                                                                                  "
        - "L1.12[300,400]      |-------L1.12--------|                                                          "
        - "L1.13[500,600]                                                   |-------L1.13--------|             "
        "###
        );

        // --------------------
        // size limit = MAX_SIZE * 5 to force the second choice, L0.1, L0.2 with L1.11, L1.12, L1.13
        let files_to_compact_or_further_split= limit_files_to_compact(
            MAX_SIZE * 5,
            files.clone(),
            CompactionLevel::FileNonOverlapped,
        );

        let files_to_compact = files_to_compact_or_further_split.files_to_compact();
        let files_to_keep = files_to_compact_or_further_split.files_to_keep();

        assert_eq!(files_to_compact.len(), 5);
        assert_eq!(files_to_keep.len(), 1);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L0, all files 100b                                                                                  "
        - "L0.1[150,250]              |----L0.1----|                                                           "
        - "L0.2[550,650]                                                                        |----L0.2----| "
        - "L1, all files 100b                                                                                  "
        - "L1.11[100,200]      |---L1.11----|                                                                  "
        - "L1.12[300,400]                                   |---L1.12----|                                     "
        - "L1.13[500,600]                                                                |---L1.13----|        "
        - "files to keep:"
        - "L0, all files 100b                                                                                  "
        - "L0.3[350,450]       |-------------------------------------L0.3-------------------------------------|"
        "###
        );

        // --------------------
        // size limit >= total size to force the third choice compacting everything:  L0.1, L0.2, L0.3 with L1.11, L1.12, L1.13
        let files_to_compact_or_further_split =
            limit_files_to_compact(MAX_SIZE * 6, files, CompactionLevel::FileNonOverlapped);

        let files_to_compact = files_to_compact_or_further_split.files_to_compact();
        let files_to_keep = files_to_compact_or_further_split.files_to_keep();

        assert_eq!(files_to_compact.len(), 6);
        assert_eq!(files_to_keep.len(), 0);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L0, all files 100b                                                                                  "
        - "L0.1[150,250]              |----L0.1----|                                                           "
        - "L0.2[550,650]                                                                        |----L0.2----| "
        - "L0.3[350,450]                                           |----L0.3----|                              "
        - "L1, all files 100b                                                                                  "
        - "L1.11[100,200]      |---L1.11----|                                                                  "
        - "L1.12[300,400]                                   |---L1.12----|                                     "
        - "L1.13[500,600]                                                                |---L1.13----|        "
        - "files to keep:"
        "###
        );
    }

    #[test]
    fn test_compact_files_limit_start_level_1() {
        let files = create_overlapped_start_target_files(
            MAX_SIZE as i64,
            CompactionLevel::FileNonOverlapped,
        );
        insta::assert_yaml_snapshot!(
            format_files("initial", &files),
            @r###"
        ---
        - initial
        - "L1, all files 100b                                                                                  "
        - "L1.2[550,650]                                                                        |----L1.2----| "
        - "L1.1[150,250]              |----L1.1----|                                                           "
        - "L1.3[350,450]                                           |----L1.3----|                              "
        - "L2, all files 100b                                                                                  "
        - "L2.12[300,400]                                   |---L2.12----|                                     "
        - "L2.13[500,600]                                                                |---L2.13----|        "
        - "L2.11[100,200]      |---L2.11----|                                                                  "
        "###
        );

        // There are only 3 choices for compacting:
        //  1. Smallest set: L1.1 with L2.11
        //  2. Medium size set: L1.1, L1.3 with L1.11, L1.12,
        //  3. All files: L1.1, L1.2, L1.3 with L2.11, L2.12, L2.13

        // --------------------
        // size limit = MAX_SIZE * 3 to force the first choice, L0.1 with L1.11
        let files_to_compact_or_further_split =
            limit_files_to_compact(MAX_SIZE * 3, files.clone(), CompactionLevel::Final);

        let files_to_compact = files_to_compact_or_further_split.files_to_compact();
        let files_to_keep = files_to_compact_or_further_split.files_to_keep();

        assert_eq!(files_to_compact.len(), 2);
        assert_eq!(files_to_keep.len(), 4);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L1, all files 100b                                                                                  "
        - "L1.1[150,250]                                 |-----------------------L1.1------------------------| "
        - "L2, all files 100b                                                                                  "
        - "L2.11[100,200]      |-----------------------L2.11-----------------------|                           "
        - "files to keep:"
        - "L1, all files 100b                                                                                  "
        - "L1.3[350,450]                  |--------L1.3--------|                                               "
        - "L1.2[550,650]                                                                |--------L1.2--------| "
        - "L2, all files 100b                                                                                  "
        - "L2.12[300,400]      |-------L2.12--------|                                                          "
        - "L2.13[500,600]                                                   |-------L2.13--------|             "
        "###
        );

        // --------------------
        // size limit = MAX_SIZE * 3 to force the first choice, L0.1 with L1.11, becasue it still not enough to for second choice
        let files_to_compact_or_further_split =
            limit_files_to_compact(MAX_SIZE * 3, files.clone(), CompactionLevel::Final);

        let files_to_compact = files_to_compact_or_further_split.files_to_compact();
        let files_to_keep = files_to_compact_or_further_split.files_to_keep();

        assert_eq!(files_to_compact.len(), 2);
        assert_eq!(files_to_keep.len(), 4);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L1, all files 100b                                                                                  "
        - "L1.1[150,250]                                 |-----------------------L1.1------------------------| "
        - "L2, all files 100b                                                                                  "
        - "L2.11[100,200]      |-----------------------L2.11-----------------------|                           "
        - "files to keep:"
        - "L1, all files 100b                                                                                  "
        - "L1.3[350,450]                  |--------L1.3--------|                                               "
        - "L1.2[550,650]                                                                |--------L1.2--------| "
        - "L2, all files 100b                                                                                  "
        - "L2.12[300,400]      |-------L2.12--------|                                                          "
        - "L2.13[500,600]                                                   |-------L2.13--------|             "
        "###
        );

        // --------------------
        // size limit = MAX_SIZE * 5 to force the second choice, L0.1, L0.2 with L1.11, L1.12, L1.13
        let files_to_compact_or_further_split =
            limit_files_to_compact(MAX_SIZE * 5, files.clone(), CompactionLevel::Final);

        let files_to_compact = files_to_compact_or_further_split.files_to_compact();
        let files_to_keep = files_to_compact_or_further_split.files_to_keep();
        
        assert_eq!(files_to_compact.len(), 4);
        assert_eq!(files_to_keep.len(), 2);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L1, all files 100b                                                                                  "
        - "L1.1[150,250]                  |--------L1.1--------|                                               "
        - "L1.3[350,450]                                                                |--------L1.3--------| "
        - "L2, all files 100b                                                                                  "
        - "L2.11[100,200]      |-------L2.11--------|                                                          "
        - "L2.12[300,400]                                                   |-------L2.12--------|             "
        - "files to keep:"
        - "L1, all files 100b                                                                                  "
        - "L1.2[550,650]                                 |-----------------------L1.2------------------------| "
        - "L2, all files 100b                                                                                  "
        - "L2.13[500,600]      |-----------------------L2.13-----------------------|                           "
        "###
        );

        // --------------------
        // size limit >= total size to force the third choice compacting everything:  L0.1, L0.2, L0.3 with L1.11, L1.12, L1.13
        let files_to_compact_or_further_split =
            limit_files_to_compact(MAX_SIZE * 6, files, CompactionLevel::Final);

        let files_to_compact = files_to_compact_or_further_split.files_to_compact();
        let files_to_keep = files_to_compact_or_further_split.files_to_keep();

        assert_eq!(files_to_compact.len(), 6);
        assert_eq!(files_to_keep.len(), 0);

        // See layout of 2 set of files
        insta::assert_yaml_snapshot!(
            format_files_split("files to compact:", &files_to_compact, "files to keep:", &files_to_keep),
            @r###"
        ---
        - "files to compact:"
        - "L1, all files 100b                                                                                  "
        - "L1.1[150,250]              |----L1.1----|                                                           "
        - "L1.3[350,450]                                           |----L1.3----|                              "
        - "L1.2[550,650]                                                                        |----L1.2----| "
        - "L2, all files 100b                                                                                  "
        - "L2.11[100,200]      |---L2.11----|                                                                  "
        - "L2.12[300,400]                                   |---L2.12----|                                     "
        - "L2.13[500,600]                                                                |---L2.13----|        "
        - "files to keep:"
        "###
        );
    }
}
