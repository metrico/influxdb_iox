mod catalog;
pub(crate) use catalog::*;

use std::fmt::Debug;

use async_trait::async_trait;
use data_types::{ColumnsByName, TableId};
use schema::sort::SortKey;

/// A [`ColumnMapResolver`] offers an infallible lookup mechanism for the set of
/// [`ColumnsByName`] of any given table, verified against the associated
/// [`SortKey`].
#[async_trait]
pub(crate) trait ColumnMapResolver: Debug + Send + Sync {
    /// Loads the mapping of column name to column ID for the given `table_id`
    /// and its `sort_key`.
    ///
    /// # Panics
    ///
    /// If the sort key refers to columns not found in the returned index, a
    /// correctness invariant is broken and this method will panic.
    async fn load_verified_column_map(
        &self,
        table_id: TableId,
        sort_key: Option<&SortKey>,
    ) -> ColumnsByName;
}
