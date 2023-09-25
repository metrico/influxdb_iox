use std::sync::Arc;

use async_trait::async_trait;
use backoff::Backoff;
use data_types::{ColumnsByName, TableId};
use iox_catalog::interface::Catalog;
use schema::sort::SortKey;

use super::ColumnMapResolver;

#[derive(Debug)]
pub(crate) struct CatalogColumnMapResolver {
    catalog: Arc<dyn Catalog>,
}

impl CatalogColumnMapResolver {
    pub(crate) fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl ColumnMapResolver for CatalogColumnMapResolver {
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
                    "sort key column {} not present in column map for table id {}",
                    sort_key_column, table_id
                )
            }
        }

        column_map
    }
}

#[cfg(test)]
mod tests {
    use data_types::Column;
    use iox_catalog::mem::MemCatalog;

    use crate::test_util::{
        populate_catalog_with_table_columns, ARBITRARY_NAMESPACE_NAME, ARBITRARY_TABLE_NAME,
    };

    use super::*;

    #[tokio::test]
    async fn test_load_verified_column_map() {
        let metrics = Arc::new(metric::Registry::default());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));

        let columns = ["bananas", "uno", "dos", "tres"];

        // Populate the catalog with the namespace, table & columns
        let (_ns_id, table_id, col_ids) = populate_catalog_with_table_columns(
            &*catalog,
            &ARBITRARY_NAMESPACE_NAME,
            &ARBITRARY_TABLE_NAME,
            &columns,
        )
        .await;

        let resolver = CatalogColumnMapResolver::new(catalog);

        let got_map = resolver
            .load_verified_column_map(table_id, Some(&SortKey::from_columns(columns)))
            .await;

        assert_eq!(
            got_map,
            ColumnsByName::new(columns.into_iter().zip(col_ids).map(|(name, id)| {
                Column {
                    id,
                    table_id,
                    name: name.to_string(),
                    column_type: data_types::ColumnType::Tag,
                }
            }))
        );
    }

    #[tokio::test]
    #[should_panic(
        expected = r#"sort key column bananas not present in column map for table id 1"#
    )]
    async fn test_load_verified_column_map_panics() {
        let metrics = Arc::new(metric::Registry::default());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));

        let columns = ["uno", "dos", "tres"];

        // Populate the catalog with the namespace, table & columns
        let (_ns_id, table_id, _col_ids) = populate_catalog_with_table_columns(
            &*catalog,
            &ARBITRARY_NAMESPACE_NAME,
            &ARBITRARY_TABLE_NAME,
            &columns,
        )
        .await;

        let resolver = CatalogColumnMapResolver::new(catalog);

        // Load with a on
        resolver
            .load_verified_column_map(table_id, Some(&SortKey::from_columns(["bananas"])))
            .await;
    }
}
