//! Temporary module for the logic performed (currently) for per-compactor scheduling.
//! Will be migrated into the [`LocalScheduler`](crate::LocalScheduler).
use std::{collections::HashSet, fmt::Display, time::Duration};

use data_types::PartitionId;

/*  BELOW HERE IS TEMP CODE, to show work to be moved into the scheduler grpc service. */

/// Create a [`PartitionsSourceConfig`].
pub fn scheduler_create_partition_source_config(
    partition_filter: Option<&[i64]>,
    process_all_partitions: bool,
    compaction_partition_minute_threshold: u64,
) -> PartitionsSourceConfig {
    match (partition_filter, process_all_partitions) {
        (None, false) => PartitionsSourceConfig::CatalogRecentWrites {
            threshold: Duration::from_secs(compaction_partition_minute_threshold * 60),
        },
        (None, true) => PartitionsSourceConfig::CatalogAll,
        (Some(ids), false) => {
            PartitionsSourceConfig::Fixed(ids.iter().cloned().map(PartitionId::new).collect())
        }
        (Some(_), true) => panic!(
            "provided partition ID filter and specific 'process all', this does not make sense"
        ),
    }
}

/// Partitions source config.
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionsSourceConfig {
    /// For "hot" compaction: use the catalog to determine which partitions have recently received
    /// writes, defined as having a new Parquet file created within the last `threshold`.
    CatalogRecentWrites {
        /// The amount of time ago to look for Parquet file creations
        threshold: Duration,
    },

    /// Use all partitions from the catalog.
    ///
    /// This does NOT consider if/when a partition received any writes.
    CatalogAll,

    /// Use a fixed set of partitions.
    ///
    /// This is mostly useful for debugging.
    Fixed(HashSet<PartitionId>),
}

impl Display for PartitionsSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CatalogRecentWrites { threshold } => {
                write!(f, "catalog_recent_writes({threshold:?})")
            }
            Self::CatalogAll => write!(f, "catalog_all"),
            Self::Fixed(p_ids) => {
                let mut p_ids = p_ids.iter().copied().collect::<Vec<_>>();
                p_ids.sort();
                write!(f, "fixed({p_ids:?})")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(
        expected = "provided partition ID filter and specific 'process all', this does not make sense"
    )]
    fn process_all_and_partition_filter_incompatible() {
        scheduler_create_partition_source_config(
            Some(&[1, 7]),
            true,
            10, // arbitrary
        );
    }

    #[test]
    fn fixed_list_of_partitions() {
        let partitions_source_config = scheduler_create_partition_source_config(
            Some(&[1, 7]),
            false,
            10, // arbitrary
        );

        assert_eq!(
            partitions_source_config,
            PartitionsSourceConfig::Fixed([PartitionId::new(1), PartitionId::new(7)].into())
        );
    }

    #[test]
    fn all_in_the_catalog() {
        let partitions_source_config = scheduler_create_partition_source_config(
            None, true, 10, // arbitrary
        );

        assert_eq!(partitions_source_config, PartitionsSourceConfig::CatalogAll,);
    }

    #[test]
    fn hot_compaction() {
        let partitions_source_config = scheduler_create_partition_source_config(None, false, 10);

        assert_eq!(
            partitions_source_config,
            PartitionsSourceConfig::CatalogRecentWrites {
                threshold: Duration::from_secs(600)
            },
        );
    }
}
