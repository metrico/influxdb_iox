//! Querier Chunks

use data_types::{ChunkId, ChunkOrder, CompactionLevel, DeletePredicate, PartitionId};
use datafusion::physical_plan::Statistics;
use iox_query::util::create_basic_summary;
use parquet_file::chunk::ParquetChunk;
use schema::sort::SortKey;
use std::sync::Arc;

mod creation;
mod query_access;

pub use creation::ChunkAdapter;

/// Immutable metadata attached to a [`QuerierParquetChunk`].
#[derive(Debug)]
pub struct QuerierParquetChunkMeta {
    /// The ID of the chunk
    chunk_id: ChunkId,

    /// Chunk order.
    order: ChunkOrder,

    /// Sort key.
    sort_key: Option<SortKey>,

    /// Partition ID.
    partition_id: PartitionId,

    /// Compaction level of the parquet file of the chunk
    compaction_level: CompactionLevel,
}

impl QuerierParquetChunkMeta {
    /// Chunk order.
    pub fn order(&self) -> ChunkOrder {
        self.order
    }

    /// Sort key.
    pub fn sort_key(&self) -> Option<&SortKey> {
        self.sort_key.as_ref()
    }

    /// Partition ID.
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    /// Compaction level of the parquet file of the chunk
    pub fn compaction_level(&self) -> CompactionLevel {
        self.compaction_level
    }
}

#[derive(Debug)]
pub struct QuerierParquetChunk {
    /// Immutable chunk metadata
    meta: Arc<QuerierParquetChunkMeta>,

    /// Delete predicates to be combined with the chunk
    delete_predicates: Vec<Arc<DeletePredicate>>,

    /// Chunk of the Parquet file
    parquet_chunk: Arc<ParquetChunk>,

    /// Stats
    stats: Arc<Statistics>,
}

impl QuerierParquetChunk {
    /// Create new parquet-backed chunk (object store data).
    pub fn new(parquet_chunk: Arc<ParquetChunk>, meta: Arc<QuerierParquetChunkMeta>) -> Self {
        let stats = Arc::new(create_basic_summary(
            parquet_chunk.rows() as u64,
            parquet_chunk.schema(),
            parquet_chunk.timestamp_min_max(),
        ));

        Self {
            meta,
            delete_predicates: Vec::new(),
            parquet_chunk,
            stats,
        }
    }

    /// Set delete predicates of the given chunk.
    pub fn with_delete_predicates(self, delete_predicates: Vec<Arc<DeletePredicate>>) -> Self {
        Self {
            delete_predicates,
            ..self
        }
    }

    /// Get metadata attached to the given chunk.
    pub fn meta(&self) -> &QuerierParquetChunkMeta {
        self.meta.as_ref()
    }

    pub fn estimate_size(&self) -> usize {
        self.parquet_chunk.parquet_file().file_size_bytes as usize
    }

    pub fn rows(&self) -> usize {
        self.parquet_chunk.rows()
    }
}

#[cfg(test)]
pub mod tests {
    use crate::{
        cache::{namespace::CachedNamespace, CatalogCache},
        table::MetricPruningObserver,
    };

    use super::*;
    use arrow::{datatypes::DataType, record_batch::RecordBatch};
    use arrow_util::assert_batches_eq;
    use data_types::{ColumnType, NamespaceSchema, ParquetFile};
    use datafusion_util::config::register_iox_object_store;
    use iox_query::{
        exec::{ExecutorType, IOxSessionContext},
        QueryChunk, QueryChunkMeta,
    };
    use iox_tests::{TestCatalog, TestNamespace, TestParquetFileBuilder};
    use metric::{Attributes, Observation, RawReporter};
    use predicate::Predicate;
    use schema::{builder::SchemaBuilder, sort::SortKeyBuilder};
    use test_helpers::maybe_start_logging;
    use tokio::runtime::Handle;

    #[tokio::test]
    async fn test_new_parquet_chunk() {
        maybe_start_logging();
        let test_data = TestData::new().await;
        let namespace_schema = Arc::new(test_data.ns.schema().await);

        // create chunk
        let chunk = test_data.chunk(Arc::clone(&namespace_schema)).await;

        // check state
        assert_eq!(chunk.chunk_type(), "parquet");

        // measure catalog access
        let catalog_metrics1 = test_data.get_catalog_access_metrics();

        // check chunk schema
        assert_schema(&chunk);

        // check sort key
        assert_sort_key(&chunk);

        // back up stats
        let stats_1 = chunk.stats();

        // check if chunk can be queried
        assert_content(&chunk, &test_data).await;

        // check state again
        assert_eq!(chunk.chunk_type(), "parquet");

        // stats have NOT changed
        let stats_2 = chunk.stats();
        assert_eq!(stats_1, stats_2);

        // retrieving the chunk again should not require any catalog requests
        test_data.chunk(namespace_schema).await;
        let catalog_metrics2 = test_data.get_catalog_access_metrics();
        assert_eq!(catalog_metrics1, catalog_metrics2);
    }

    /// collect data for the given chunk
    async fn collect_read_filter(
        chunk: &dyn QueryChunk,
        ctx: IOxSessionContext,
    ) -> Vec<RecordBatch> {
        chunk
            .data()
            .read_to_batches(chunk.schema(), ctx.inner())
            .await
    }

    struct TestData {
        catalog: Arc<TestCatalog>,
        ns: Arc<TestNamespace>,
        parquet_file: Arc<ParquetFile>,
        adapter: ChunkAdapter,
    }

    impl TestData {
        async fn new() -> Self {
            let catalog = TestCatalog::new();

            let lp = vec![
                "table,tag1=WA field_int=1000i 8000",
                "table,tag1=VT field_int=10i 10000",
                "table,tag1=UT field_int=70i 20000",
            ]
            .join("\n");
            let ns = catalog.create_namespace_1hr_retention("ns").await;
            let table = ns.create_table("table").await;
            table.create_column("tag1", ColumnType::Tag).await;
            table.create_column("tag2", ColumnType::Tag).await;
            table.create_column("tag3", ColumnType::Tag).await;
            table.create_column("tag4", ColumnType::Tag).await;
            table.create_column("field_int", ColumnType::I64).await;
            table.create_column("field_float", ColumnType::F64).await;
            table.create_column("time", ColumnType::Time).await;
            let partition = table
                .create_partition("part")
                .await
                .update_sort_key(SortKey::from_columns(["tag1", "tag2", "tag4", "time"]))
                .await;
            let builder = TestParquetFileBuilder::default().with_line_protocol(&lp);
            let parquet_file = Arc::new(partition.create_parquet_file(builder).await.parquet_file);

            let adapter = ChunkAdapter::new(
                Arc::new(CatalogCache::new_testing(
                    catalog.catalog(),
                    catalog.time_provider(),
                    catalog.metric_registry(),
                    catalog.object_store(),
                    &Handle::current(),
                )),
                catalog.metric_registry(),
            );

            Self {
                catalog,
                ns,
                parquet_file,
                adapter,
            }
        }

        async fn chunk(&self, namespace_schema: Arc<NamespaceSchema>) -> QuerierParquetChunk {
            let cached_namespace: CachedNamespace = namespace_schema.as_ref().clone().into();
            let cached_table = cached_namespace.tables.get("table").expect("table exists");
            self.adapter
                .new_chunks(
                    Arc::clone(cached_table),
                    Arc::new(vec![Arc::clone(&self.parquet_file)]),
                    &Predicate::new(),
                    MetricPruningObserver::new_unregistered(),
                    None,
                )
                .await
                .remove(0)
        }

        /// get catalog access metrics from metric registry
        fn get_catalog_access_metrics(&self) -> Vec<(Attributes, u64)> {
            let mut reporter = RawReporter::default();
            self.catalog.metric_registry.report(&mut reporter);

            let mut metrics: Vec<_> = reporter
                .observations()
                .iter()
                .find(|o| o.metric_name == "catalog_op_duration")
                .expect("failed to read metric")
                .observations
                .iter()
                .map(|(a, o)| {
                    if let Observation::DurationHistogram(h) = o {
                        (a.clone(), h.sample_count())
                    } else {
                        panic!("wrong metric type")
                    }
                })
                .collect();
            metrics.sort();
            assert!(!metrics.is_empty());
            metrics
        }
    }

    fn assert_schema(chunk: &QuerierParquetChunk) {
        let expected_schema = SchemaBuilder::new()
            .field("field_int", DataType::Int64)
            .unwrap()
            .tag("tag1")
            .timestamp()
            .build()
            .unwrap();
        let actual_schema = chunk.schema();
        assert_eq!(actual_schema, &expected_schema);
    }

    fn assert_sort_key(chunk: &QuerierParquetChunk) {
        let expected_sort_key = SortKeyBuilder::new()
            .with_col("tag1")
            .with_col("time")
            .build();
        let actual_sort_key = chunk.sort_key().unwrap();
        assert_eq!(actual_sort_key, &expected_sort_key);
    }

    async fn assert_content(chunk: &QuerierParquetChunk, test_data: &TestData) {
        let ctx = test_data.catalog.exec.new_context(ExecutorType::Query);
        let parquet_store = test_data.adapter.catalog_cache().parquet_store();
        register_iox_object_store(
            ctx.inner().runtime_env(),
            parquet_store.id(),
            Arc::clone(parquet_store.object_store()),
        );

        let batches = collect_read_filter(chunk, ctx).await;

        assert_batches_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
                "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
                "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches
        );
    }
}
