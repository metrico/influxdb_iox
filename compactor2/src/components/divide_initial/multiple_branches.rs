use std::fmt::Display;

use data_types::{CompactionLevel, ParquetFile};

use crate::RoundInfo;

use super::DivideInitial;

#[derive(Debug, Default)]
pub struct MultipleBranchesDivideInitial;

impl MultipleBranchesDivideInitial {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Display for MultipleBranchesDivideInitial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "multiple_branches")
    }
}

impl DivideInitial for MultipleBranchesDivideInitial {
    fn divide(&self, files: Vec<ParquetFile>, round_info: &RoundInfo) -> Vec<Vec<ParquetFile>> {
        match round_info {
            RoundInfo::ManySmallFiles {
                start_level,
                max_num_files_to_group,
            } => {
                // Sort start_level on max_l0_created_at if start_level is 0 or min_time otherwise
                // to have the right split of groups of files to compact
                let start_level_files = files
                    .into_iter()
                    .filter(|f| f.compaction_level == *start_level)
                    .collect::<Vec<_>>();
                let start_level_files = order_files(start_level_files, start_level);

                // Split files into many small groups, each has at max_num_files_to_group that do
                let branches = start_level_files
                    .chunks(*max_num_files_to_group)
                    .map(|c| c.to_vec())
                    .collect::<Vec<Vec<_>>>();

                branches
            }
            RoundInfo::TargetLevel { .. } => vec![files],
        }
    }
}

// Return a sorted files of the given ones.
// The order is used to split the files and form the right groups of files to compact
// and deduplcate correctly to fewer and larger but same level files
//
// All given files are in the same given start_level.
// They will be sorted on their `max_l0_created_at` if the start_level is 0,
// otherwise on their `min_time`
fn order_files(files: Vec<ParquetFile>, start_level: &CompactionLevel) -> Vec<ParquetFile> {
    let mut files = files;
    if *start_level == CompactionLevel::Initial {
        files.sort_by(|a, b| a.max_l0_created_at.cmp(&b.max_l0_created_at));
    } else {
        files.sort_by(|a, b| a.min_time.cmp(&b.min_time));
    }
    files
}

#[cfg(test)]
mod tests {
    use data_types::CompactionLevel;
    use iox_tests::ParquetFileBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            MultipleBranchesDivideInitial::new().to_string(),
            "multiple_branches"
        );
    }

    #[test]
    fn test_divide() {
        let round_info = RoundInfo::ManySmallFiles {
            start_level: CompactionLevel::Initial,
            max_num_files_to_group: 2,
        };
        let divide = MultipleBranchesDivideInitial::new();

        // empty input
        assert_eq!(divide.divide(vec![], &round_info), Vec::<Vec<_>>::new());

        // not empty
        let f1 = ParquetFileBuilder::new(1)
            .with_compaction_level(CompactionLevel::Initial)
            .with_max_l0_created_at(1)
            .build();
        let f2 = ParquetFileBuilder::new(2)
            .with_compaction_level(CompactionLevel::Initial)
            .with_max_l0_created_at(5)
            .build();
        let f3 = ParquetFileBuilder::new(3)
            .with_compaction_level(CompactionLevel::Initial)
            .with_max_l0_created_at(10)
            .build();

        // files in random order of max_l0_created_at
        let files = vec![f2.clone(), f3.clone(), f1.clone()];

        let branches = divide.divide(files, &round_info);
        // output must be split into their max_l0_created_at
        assert_eq!(branches.len(), 2);
        assert_eq!(branches[0], vec![f1, f2]);
        assert_eq!(branches[1], vec![f3]);
    }
}
