use std::sync::Arc;

use async_trait::async_trait;
use backoff::Backoff;
use data_types::{ColumnsByName, TableId};
use iox_catalog::interface::Catalog;
use schema::sort::SortKey;

use super::ColumnMapResolver;

#[derive(Debug)]
pub(crate) struct CatalogMapResolver {
    catalog: Arc<dyn Catalog>,
}

impl CatalogMapResolver {
    pub(crate) fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl ColumnMapResolver for CatalogMapResolver {
    async fn load_verified_column_map(
        &self,
        table_id: TableId,
        sort_key: Option<&SortKey>,
    ) -> ColumnsByName {
        let column_map = Backoff::new(&Default::default())
            .retry_all_errors("resolve table schema from catalog", || async {
                self.catalog
                    .repositories()
                    .await
                    .columns()
                    .list_by_table_id(table_id)
                    .await
                    .map(ColumnsByName::new)
            })
            .await
            .expect("retry forever");

        if let Some(sort_key) = &sort_key {
            if let Some(sort_key_column) = sort_key
                .to_columns()
                .find(|sort_key_column| !column_map.contains_column_name(sort_key_column))
            {
                panic!(
                    "sort key column {} not present in column map {:?} for table id {}",
                    sort_key_column, column_map, table_id
                )
            }
        }

        column_map
    }
}
