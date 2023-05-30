//! Filtering of a [`PartitionsSource`](data_types::PartitionsSource) per PartitionId (no file IO).

use std::fmt::{Debug, Display};

use data_types::PartitionId;

pub(crate) mod and;
pub(crate) mod by_id;
pub(crate) mod shard;

/// Filters partition based on ID.
///
/// This will be used BEFORE any parquet files for the given partition are fetched and hence is a quite
/// efficient filter stage.
pub(crate) trait IdOnlyPartitionFilter: Debug + Display + Send + Sync {
    fn apply(&self, partition_id: PartitionId) -> bool;
}
