use self::test_util::MockIngesterConnection;
use crate::cache::{namespace::CachedTable, CatalogCache};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use data_types::{ChunkId, ChunkOrder, NamespaceId, TimestampMinMax, TransitionPartitionId};
use datafusion::{physical_plan::Statistics, prelude::Expr};
use iox_query::{
    chunk_statistics::{create_chunk_statistics, ColumnRanges},
    QueryChunk, QueryChunkData,
};
use observability_deps::tracing::trace;
use schema::{sort::SortKey, Schema};
use std::{any::Any, sync::Arc};
use trace::span::Span;
use uuid::Uuid;

pub(crate) mod test_util;
mod v1;

/// Create a new set of connections given ingester configurations
pub fn create_ingester_connections(
    ingester_addresses: Vec<Arc<str>>,
    catalog_cache: Arc<CatalogCache>,
    open_circuit_after_n_errors: u64,
    trace_context_header_name: &str,
    use_v2: bool,
) -> Arc<dyn IngesterConnection> {
    if use_v2 {
        unimplemented!("v2 ingester API")
    } else {
        v1::create_ingester_connections(
            ingester_addresses,
            catalog_cache,
            open_circuit_after_n_errors,
            trace_context_header_name,
        )
    }
}

/// Create a new ingester suitable for testing
pub fn create_ingester_connection_for_testing() -> Arc<dyn IngesterConnection> {
    Arc::new(MockIngesterConnection::new())
}

/// Dynamic error type that is used throughout the stack.
pub type DynError = Box<dyn std::error::Error + Send + Sync>;

/// Handles communicating with the ingester(s) to retrieve data that is not yet persisted
#[async_trait]
pub trait IngesterConnection: std::fmt::Debug + Send + Sync + 'static {
    /// Returns all partitions ingester(s) know about for the specified table.
    async fn partitions(
        &self,
        namespace_id: NamespaceId,
        cached_table: Arc<CachedTable>,
        columns: Vec<String>,
        filters: &[Expr],
        span: Option<Span>,
    ) -> Result<Vec<IngesterPartition>, DynError>;

    /// Return backend as [`Any`] which can be used to downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
}

/// A wrapper around the unpersisted data in a partition returned by
/// the ingester that (will) implement the `QueryChunk` interface
///
/// Given the catalog hierarchy:
///
/// ```text
/// (Catalog) Table --> (Catalog) Partition
/// ```
///
/// An IngesterPartition contains the unpersisted data for a catalog partition. Thus, there can be
/// more than one IngesterPartition for each table the ingester knows about.
#[derive(Debug, Clone)]
pub struct IngesterPartition {
    /// The ingester UUID that identifies whether this ingester has restarted since the last time
    /// it was queried or not, which affects whether we can compare the
    /// `completed_persistence_count` with a previous count for this ingester to know if we need
    /// to refresh the catalog cache or not.
    ingester_uuid: Uuid,

    /// The partition identifier.
    partition_id: TransitionPartitionId,

    /// The number of Parquet files this ingester UUID has persisted for this partition.
    completed_persistence_count: u64,

    chunks: Vec<IngesterChunk>,
}

impl IngesterPartition {
    /// Creates a new IngesterPartition, translating the passed `RecordBatches` into the correct
    /// types
    pub fn new(
        ingester_uuid: Uuid,
        partition_id: TransitionPartitionId,
        completed_persistence_count: u64,
    ) -> Self {
        Self {
            ingester_uuid,
            partition_id,
            completed_persistence_count,
            chunks: vec![],
        }
    }

    pub(crate) fn push_chunk(
        mut self,
        chunk_id: ChunkId,
        schema: Schema,
        data: IngesterChunkData,
        ts_min_max: TimestampMinMax,
    ) -> Self {
        let chunk = IngesterChunk {
            chunk_id,
            partition_id: self.partition_id.clone(),
            schema,
            data,
            ts_min_max,
            stats: None,
        };

        self.chunks.push(chunk);

        self
    }

    pub(crate) fn set_partition_column_ranges(&mut self, partition_column_ranges: &ColumnRanges) {
        for chunk in &mut self.chunks {
            let row_count = match &chunk.data {
                IngesterChunkData::Eager(batches) => {
                    Some(batches.iter().map(|batch| batch.num_rows()).sum::<usize>())
                }
            };

            let stats = Arc::new(create_chunk_statistics(
                row_count,
                &chunk.schema,
                Some(chunk.ts_min_max),
                Some(partition_column_ranges),
            ));
            chunk.stats = Some(stats);
        }
    }

    pub(crate) fn partition_id(&self) -> TransitionPartitionId {
        self.partition_id.clone()
    }

    pub(crate) fn ingester_uuid(&self) -> Uuid {
        self.ingester_uuid
    }

    pub(crate) fn completed_persistence_count(&self) -> u64 {
        self.completed_persistence_count
    }

    pub(crate) fn chunks(&self) -> &[IngesterChunk] {
        &self.chunks
    }

    pub(crate) fn into_chunks(self) -> Vec<IngesterChunk> {
        self.chunks
    }
}

#[derive(Debug, Clone)]
pub enum IngesterChunkData {
    /// All batches are fetched already.
    Eager(Vec<RecordBatch>),
}

impl IngesterChunkData {
    #[cfg(test)]
    pub fn eager_ref(&self) -> &[RecordBatch] {
        match self {
            Self::Eager(batches) => batches,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IngesterChunk {
    /// Chunk ID.
    chunk_id: ChunkId,

    /// Partition ID.
    partition_id: TransitionPartitionId,

    /// Chunk schema.
    ///
    /// This may be a subset of the table and partition schema.
    schema: Schema,

    /// Data.
    data: IngesterChunkData,

    /// Timestamp range.
    ts_min_max: TimestampMinMax,

    /// Summary Statistics
    ///
    /// Set to `None` if not calculated yet.
    stats: Option<Arc<Statistics>>,
}

impl QueryChunk for IngesterChunk {
    fn stats(&self) -> Arc<Statistics> {
        Arc::clone(self.stats.as_ref().expect("chunk stats set"))
    }

    fn schema(&self) -> &Schema {
        trace!(schema=?self.schema, "IngesterChunk schema");
        &self.schema
    }

    fn partition_id(&self) -> &TransitionPartitionId {
        &self.partition_id
    }

    fn sort_key(&self) -> Option<&SortKey> {
        // Data is not sorted
        None
    }

    fn id(&self) -> ChunkId {
        self.chunk_id
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        // ingester just dumps data, may contain duplicates!
        true
    }

    fn data(&self) -> QueryChunkData {
        match &self.data {
            IngesterChunkData::Eager(batches) => {
                QueryChunkData::in_mem(batches.clone(), Arc::clone(self.schema.inner()))
            }
        }
    }

    fn chunk_type(&self) -> &str {
        "ingester"
    }

    fn order(&self) -> ChunkOrder {
        // since this is always the 'most recent' chunk for this
        // partition, put it at the end
        ChunkOrder::new(i64::MAX)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
