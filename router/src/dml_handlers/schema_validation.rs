use std::{ops::DerefMut, sync::Arc};

use async_trait::async_trait;
use data_types::{
    partition_template::TablePartitionTemplateOverride, NamespaceName, NamespaceSchema, TableId,
};
use hashbrown::HashMap;
use iox_catalog::{interface::Error as CatalogError, validate_or_insert_schema};
use mutable_batch::MutableBatch;
use observability_deps::tracing::*;
use trace::ctx::SpanContext;

use super::DmlHandler;
use crate::{
    namespace_cache::NamespaceCache,
    schema_validator::{SchemaError, SchemaValidator},
};

#[async_trait]
impl<C> DmlHandler for SchemaValidator<C>
where
    C: NamespaceCache<ReadError = iox_catalog::interface::Error>, // The handler expects the cache to read from the catalog if necessary.
{
    type WriteError = SchemaError;

    // Accepts a map of TableName -> MutableBatch
    type WriteInput = HashMap<String, MutableBatch>;
    // And returns a map of TableId -> (TableName, TablePartitionTemplate, MutableBatch)
    type WriteOutput = HashMap<TableId, (String, TablePartitionTemplateOverride, MutableBatch)>;

    /// Validate the schema of all the writes in `batches`.
    ///
    /// # Errors
    ///
    /// If the schema validation fails due to a schema conflict in the request,
    /// [`SchemaError::Conflict`] is returned.
    ///
    /// If the schema validation fails due to a service limit being reached,
    /// [`SchemaError::ServiceLimit`] is returned.
    ///
    /// A request that fails validation on one or more tables fails the request
    /// as a whole - calling this method has "all or nothing" semantics.
    async fn write(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_schema: Arc<NamespaceSchema>,
        batches: Self::WriteInput,
        _span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        let namespace_id = namespace_schema.id;

        let column_names_by_table = batches
            .iter()
            .map(|(table_name, batch)| (table_name.as_str(), batch.column_names()));

        self.validate_service_limits(namespace, &namespace_schema, column_names_by_table)?;

        let mut repos = self.catalog.repositories().await;

        let maybe_new_schema = validate_or_insert_schema(
            batches.iter().map(|(k, v)| (k.as_str(), v)),
            &namespace_schema,
            repos.deref_mut(),
        )
        .await
        .map_err(|e| {
            match e.err() {
                // Schema conflicts
                CatalogError::ColumnTypeMismatch {
                    ref name,
                    ref existing,
                    ref new,
                } => {
                    warn!(
                        %namespace,
                        %namespace_id,
                        column_name=%name,
                        existing_column_type=%existing,
                        request_column_type=%new,
                        table_name=%e.table(),
                        "schema conflict"
                    );
                    self.schema_conflict.inc(1);
                    SchemaError::Conflict(e)
                }
                // Service limits
                CatalogError::ColumnCreateLimitError { table_id, .. } => {
                    warn!(
                        %namespace,
                        %namespace_id,
                        %table_id,
                        error=%e,
                        "service protection limit reached (columns)"
                    );
                    self.service_limit_hit_columns.inc(1);
                    SchemaError::ServiceLimit(Box::new(e.into_err()))
                }
                CatalogError::TableCreateLimitError { .. } => {
                    warn!(
                        %namespace,
                        %namespace_id,
                        error=%e,
                        "service protection limit reached (tables)"
                    );
                    self.service_limit_hit_tables.inc(1);
                    SchemaError::ServiceLimit(Box::new(e.into_err()))
                }
                _ => {
                    error!(
                        %namespace,
                        %namespace_id,
                        error=%e,
                        "schema validation failed"
                    );
                    SchemaError::UnexpectedCatalogError(e.into_err())
                }
            }
        })?;

        trace!(%namespace, "schema validation complete");

        // If the schema has been updated, immediately add it to the cache
        // (before passing through the write) in order to allow subsequent,
        // parallel requests to use it while waiting on this request to
        // complete.
        let latest_schema = match maybe_new_schema {
            Some(v) => {
                let (new_schema, _) = self.cache.put_schema(namespace.clone(), v);
                trace!(%namespace, "schema cache updated");
                new_schema
            }
            None => {
                trace!(%namespace, "schema unchanged");
                namespace_schema
            }
        };

        // Map the "TableName -> Data" into "TableId -> (TableName, TablePartitionTemplate,
        // Data)" for downstream handlers.
        let batches = batches
            .into_iter()
            .map(|(name, data)| {
                let table = latest_schema.tables.get(&name).unwrap();
                let id = table.id;
                let table_partition_template = table.partition_template.clone();

                (id, (name, table_partition_template, data))
            })
            .collect();

        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc};

    use assert_matches::assert_matches;
    use data_types::{ColumnType, MaxColumnsPerTable, MaxTables};
    use iox_tests::{TestCatalog, TestNamespace};
    use once_cell::sync::Lazy;

    use super::*;
    use crate::namespace_cache::{MemoryNamespaceCache, ReadThroughCache};

    static NAMESPACE: Lazy<NamespaceName<'static>> = Lazy::new(|| "bananas".try_into().unwrap());

    #[tokio::test]
    async fn test_validate_column_limits_race() {
        let (catalog, namespace) = test_setup().await;

        namespace.update_column_limit(3).await;

        // Make two schema validator instances each with their own cache
        let cache1 = Arc::new(setup_test_cache(&catalog));
        let handler1 = SchemaValidator::new(
            catalog.catalog(),
            Arc::clone(&cache1),
            &catalog.metric_registry,
        );
        let cache2 = Arc::new(setup_test_cache(&catalog));
        let handler2 = SchemaValidator::new(
            catalog.catalog(),
            Arc::clone(&cache2),
            &catalog.metric_registry,
        );

        // Make a valid write with one column + timestamp through each validator so the
        // namespace schema gets cached
        let writes1_valid = lp_to_writes("dragonfruit val=42i 123456");
        handler1
            .write(
                &NAMESPACE,
                cache1.get_schema(&NAMESPACE).await.unwrap(),
                writes1_valid,
                None,
            )
            .await
            .expect("request should succeed");
        let writes2_valid = lp_to_writes("dragonfruit val=43i 123457");
        handler2
            .write(
                &NAMESPACE,
                cache2.get_schema(&NAMESPACE).await.unwrap(),
                writes2_valid,
                None,
            )
            .await
            .expect("request should succeed");

        // Make "valid" writes through each validator that each add a different column, thus
        // putting the table over the limit
        let writes1_add_column = lp_to_writes("dragonfruit,tag1=A val=42i 123456");
        handler1
            .write(
                &NAMESPACE,
                cache1.get_schema(&NAMESPACE).await.unwrap(),
                writes1_add_column,
                None,
            )
            .await
            .expect("request should succeed");
        let writes2_add_column = lp_to_writes("dragonfruit,tag2=B val=43i 123457");
        handler2
            .write(
                &NAMESPACE,
                cache2.get_schema(&NAMESPACE).await.unwrap(),
                writes2_add_column,
                None,
            )
            .await
            .expect("request should succeed");

        let schema = namespace.schema().await;

        // Columns already existing is allowed
        let column_names_by_table = [("dragonfruit", BTreeSet::from(["val", "time"]))].into_iter();
        assert!(handler1
            .validate_service_limits(&NAMESPACE, &schema, column_names_by_table.clone())
            .is_ok());
        assert!(handler2
            .validate_service_limits(&NAMESPACE, &schema, column_names_by_table)
            .is_ok());

        // Adding more columns over the limit is an error
        let column_names_by_table =
            [("dragonfruit", BTreeSet::from(["i_got_music", "time"]))].into_iter();
        assert_matches!(
            handler1.validate_service_limits(&NAMESPACE, &schema, column_names_by_table.clone()),
            Err(SchemaError::ServiceLimit { .. })
        );
        assert_matches!(
            handler2.validate_service_limits(&NAMESPACE, &schema, column_names_by_table),
            Err(SchemaError::ServiceLimit { .. })
        );
    }

    // Parse `lp` into a table-keyed MutableBatch map.
    fn lp_to_writes(lp: &str) -> HashMap<String, MutableBatch> {
        let (writes, _) = mutable_batch_lp::lines_to_batches_stats(lp, 42)
            .expect("failed to build test writes from LP");
        writes
    }

    /// Initialise an in-memory [`MemCatalog`] and create a single namespace
    /// named [`NAMESPACE`].
    async fn test_setup() -> (Arc<TestCatalog>, Arc<TestNamespace>) {
        let catalog = TestCatalog::new();
        let namespace = catalog.create_namespace_1hr_retention(&NAMESPACE).await;

        (catalog, namespace)
    }

    fn setup_test_cache(catalog: &TestCatalog) -> ReadThroughCache<MemoryNamespaceCache> {
        ReadThroughCache::new(MemoryNamespaceCache::default(), catalog.catalog())
    }

    async fn assert_cache<C>(handler: &SchemaValidator<C>, table: &str, col: &str, want: ColumnType)
    where
        C: NamespaceCache,
    {
        // The cache should be populated.
        let ns = handler
            .cache
            .get_schema(&NAMESPACE)
            .await
            .expect("cache should be populated");
        let table = ns.tables.get(table).expect("table should exist in cache");
        assert_eq!(
            table
                .columns
                .get(col)
                .expect("column not cached")
                .column_type,
            want
        );
    }

    #[tokio::test]
    async fn test_write_ok() {
        let (catalog, namespace) = test_setup().await;

        // Create the table so that there is a known ID that must be returned.
        let want_id = namespace.create_table("bananas").await.table.id;

        let metrics = Arc::new(metric::Registry::default());
        let cache = Arc::new(setup_test_cache(&catalog));
        let handler = SchemaValidator::new(catalog.catalog(), Arc::clone(&cache), &metrics);

        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456");
        let got = handler
            .write(
                &NAMESPACE,
                cache.get_schema(&NAMESPACE).await.unwrap(),
                writes,
                None,
            )
            .await
            .expect("request should succeed");

        // The cache should be populated.
        assert_cache(&handler, "bananas", "tag1", ColumnType::Tag).await;
        assert_cache(&handler, "bananas", "tag2", ColumnType::Tag).await;
        assert_cache(&handler, "bananas", "val", ColumnType::I64).await;
        assert_cache(&handler, "bananas", "time", ColumnType::Time).await;

        // Validate the table ID mapping.
        let (name, _partition_template, _data) = got.get(&want_id).expect("table not in output");
        assert_eq!(name, "bananas");
    }

    #[tokio::test]
    async fn test_write_validation_failure() {
        let (catalog, namespace) = test_setup().await;
        let metrics = Arc::new(metric::Registry::default());
        let handler = SchemaValidator::new(catalog.catalog(), setup_test_cache(&catalog), &metrics);

        // First write sets the schema
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456"); // val=i64
        let got = handler
            .write(
                &NAMESPACE,
                namespace.schema().await.into(),
                writes.clone(),
                None,
            )
            .await
            .expect("request should succeed");
        assert_eq!(writes.len(), got.len());

        // Second write attempts to violate it causing an error
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42.0 123456"); // val=float
        let err = handler
            .write(&NAMESPACE, namespace.schema().await.into(), writes, None)
            .await
            .expect_err("request should fail");

        assert_matches!(err, SchemaError::Conflict(e) => {
            assert_eq!(e.table(), "bananas");
        });

        // The cache should retain the original schema.
        assert_cache(&handler, "bananas", "tag1", ColumnType::Tag).await;
        assert_cache(&handler, "bananas", "tag2", ColumnType::Tag).await;
        assert_cache(&handler, "bananas", "val", ColumnType::I64).await; // original type
        assert_cache(&handler, "bananas", "time", ColumnType::Time).await;

        assert_eq!(1, handler.schema_conflict.fetch());
    }

    #[tokio::test]
    async fn test_write_table_service_limit() {
        let (catalog, namespace) = test_setup().await;
        let metrics = Arc::new(metric::Registry::default());
        let handler = SchemaValidator::new(catalog.catalog(), setup_test_cache(&catalog), &metrics);

        // First write sets the schema
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456");
        let got = handler
            .write(
                &NAMESPACE,
                namespace.schema().await.into(),
                writes.clone(),
                None,
            )
            .await
            .expect("request should succeed");
        assert_eq!(writes.len(), got.len());

        // Configure the service limit to be hit next request
        catalog
            .catalog()
            .repositories()
            .await
            .namespaces()
            .update_table_limit(NAMESPACE.as_str(), MaxTables::try_from(1).unwrap())
            .await
            .expect("failed to set table limit");

        // Second write attempts to violate limits, causing an error
        let writes = lp_to_writes("bananas2,tag1=A,tag2=B val=42i 123456");
        let err = handler
            .write(&NAMESPACE, namespace.schema().await.into(), writes, None)
            .await
            .expect_err("request should fail");

        assert_matches!(err, SchemaError::ServiceLimit(_));
        assert_eq!(1, handler.service_limit_hit_tables.fetch());
    }

    #[tokio::test]
    async fn test_write_column_service_limit() {
        let (catalog, namespace) = test_setup().await;
        let metrics = Arc::new(metric::Registry::default());
        let handler = SchemaValidator::new(catalog.catalog(), setup_test_cache(&catalog), &metrics);

        // First write sets the schema
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456");
        let got = handler
            .write(
                &NAMESPACE,
                namespace.schema().await.into(),
                writes.clone(),
                None,
            )
            .await
            .expect("request should succeed");
        assert_eq!(writes.len(), got.len());

        // Configure the service limit to be hit next request
        namespace.update_column_limit(1).await;
        let handler = SchemaValidator::new(catalog.catalog(), setup_test_cache(&catalog), &metrics);

        // Second write attempts to violate limits, causing an error
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i,val2=42i 123456");
        let err = handler
            .write(&NAMESPACE, namespace.schema().await.into(), writes, None)
            .await
            .expect_err("request should fail");

        assert_matches!(err, SchemaError::ServiceLimit(_));
        assert_eq!(1, handler.service_limit_hit_columns.fetch());
    }

    #[tokio::test]
    async fn test_first_write_many_columns_service_limit() {
        let (catalog, namespace) = test_setup().await;
        let metrics = Arc::new(metric::Registry::default());
        let handler = SchemaValidator::new(catalog.catalog(), setup_test_cache(&catalog), &metrics);

        // Configure the service limit to be hit next request
        catalog
            .catalog()
            .repositories()
            .await
            .namespaces()
            .update_column_limit(NAMESPACE.as_str(), MaxColumnsPerTable::try_from(3).unwrap())
            .await
            .expect("failed to set column limit");

        // First write attempts to add columns over the limit, causing an error
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i,val2=42i 123456");
        let err = handler
            .write(&NAMESPACE, namespace.schema().await.into(), writes, None)
            .await
            .expect_err("request should fail");

        assert_matches!(err, SchemaError::ServiceLimit(_));
        assert_eq!(1, handler.service_limit_hit_columns.fetch());
    }
}
