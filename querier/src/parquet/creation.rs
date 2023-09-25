use std::{collections::HashMap, sync::Arc};

use data_types::{ChunkId, ChunkOrder, ColumnId, ParquetFile, TimestampMinMax};
use datafusion::{physical_plan::Statistics, prelude::Expr};
use futures::StreamExt;
use hashbrown::HashSet;
use iox_catalog::interface::Catalog;
use iox_query::{chunk_statistics::create_chunk_statistics, pruning::prune_summaries};
use parquet_file::chunk::ParquetChunk;
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use schema::{sort::SortKeyBuilder, Schema};
use trace::span::{Span, SpanRecorder};
use uuid::Uuid;

use crate::{
    cache::{namespace::CachedTable, partition::CachedPartition, CatalogCache},
    parquet::QuerierParquetChunkMeta,
    table::PruneMetrics,
    CONCURRENT_CHUNK_CREATION_JOBS,
};

use super::QuerierParquetChunk;

/// Adapter that can create chunks.
#[derive(Debug)]
pub struct ChunkAdapter {
    /// Cache
    catalog_cache: Arc<CatalogCache>,

    /// Prune metrics.
    prune_metrics: Arc<PruneMetrics>,
}

impl ChunkAdapter {
    /// Create new adapter with empty cache.
    pub fn new(catalog_cache: Arc<CatalogCache>, prune_metrics: Arc<PruneMetrics>) -> Self {
        Self {
            catalog_cache,
            prune_metrics,
        }
    }

    /// Get underlying catalog cache.
    pub fn catalog_cache(&self) -> &Arc<CatalogCache> {
        &self.catalog_cache
    }

    /// Get underlying catalog.
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog_cache.catalog()
    }

    pub(crate) async fn new_chunks(
        &self,
        cached_table: Arc<CachedTable>,
        files: impl IntoIterator<Item = (Arc<ParquetFile>, Arc<CachedPartition>)> + Send,
        filters: &[Expr],
        span: Option<Span>,
    ) -> Vec<QuerierParquetChunk> {
        let span_recorder = SpanRecorder::new(span);

        // prepare files
        let files = {
            let _span_recorder = span_recorder.child("prepare files");

            files
                .into_iter()
                .map(|(f, p)| PreparedParquetFile::new(f, &cached_table, p))
                .collect::<Vec<_>>()
        };

        // find all projected schemas
        let projections = {
            let span_recorder = span_recorder.child("get projected schemas");
            let mut projections: HashSet<Box<[ColumnId]>> = HashSet::with_capacity(files.len());
            for f in &files {
                projections.get_or_insert_owned(&f.col_list);
            }

            // de-correlate projections so that subsequent items likely don't block/wait on the same cache lookup
            // (they are likely ordered by partition)
            //
            // Note that we sort before shuffling to achieve a deterministic pseudo-random order
            let mut projections = projections.into_iter().collect::<Vec<_>>();
            projections.sort();
            let mut rng = StdRng::seed_from_u64(cached_table.id.get() as u64);
            projections.shuffle(&mut rng);

            futures::stream::iter(projections)
                .map(|column_ids| {
                    let span_recorder = &span_recorder;
                    let cached_table = Arc::clone(&cached_table);
                    async move {
                        let schema = self
                            .catalog_cache
                            .projected_schema()
                            .get(
                                cached_table,
                                column_ids.clone(),
                                span_recorder.child_span("cache GET projected schema"),
                            )
                            .await;
                        (column_ids, schema)
                    }
                })
                .buffer_unordered(CONCURRENT_CHUNK_CREATION_JOBS)
                .collect::<HashMap<_, _>>()
                .await
        };

        let files = {
            let _span_recorder = span_recorder.child("calculate chunk stats");

            files
                .into_iter()
                .map(|file| {
                    let schema = projections
                        .get(&file.col_list)
                        .expect("looked up all projections")
                        .clone();
                    PreparedParquetFileWithStats::new(file, schema)
                })
                .collect::<Vec<_>>()
        };

        let files = self.prune_chunks(
            files,
            &cached_table,
            filters,
            span_recorder.child_span("prune chunks"),
        );

        {
            let _span_recorder = span_recorder.child("finalize chunks");

            files
                .into_iter()
                .map(|file| {
                    let cached_table = Arc::clone(&cached_table);
                    self.new_chunk(cached_table, file)
                })
                .collect()
        }
    }

    fn prune_chunks(
        &self,
        files: Vec<PreparedParquetFileWithStats>,
        cached_table: &CachedTable,
        filters: &[Expr],
        span: Option<Span>,
    ) -> Vec<PreparedParquetFileWithStats> {
        let _span_recorder = SpanRecorder::new(span);

        let summaries = files
            .iter()
            .map(|f| (Arc::clone(&f.stats), Arc::clone(f.schema.inner())))
            .collect::<Vec<_>>();

        match prune_summaries(&cached_table.schema, &summaries, filters) {
            Ok(keeps) => {
                assert_eq!(files.len(), keeps.len());
                files
                    .into_iter()
                    .zip(keeps.iter())
                    .filter_map(|(f, keep)| {
                        if *keep {
                            self.prune_metrics.was_not_pruned(
                                f.file.row_count as u64,
                                f.file.file_size_bytes as u64,
                            );
                            Some(f)
                        } else {
                            self.prune_metrics.was_pruned_late(
                                f.file.row_count as u64,
                                f.file.file_size_bytes as u64,
                            );
                            None
                        }
                    })
                    .collect()
            }
            Err(reason) => {
                for f in &files {
                    self.prune_metrics.could_not_prune(
                        reason,
                        f.file.row_count as u64,
                        f.file.file_size_bytes as u64,
                    )
                }
                files
            }
        }
    }

    fn new_chunk(
        &self,
        cached_table: Arc<CachedTable>,
        parquet_file: PreparedParquetFileWithStats,
    ) -> QuerierParquetChunk {
        let PreparedParquetFileWithStats {
            file,
            cached_partition,
            col_set,
            schema,
            stats,
        } = parquet_file;

        // NOTE: Because we've looked up the sort key AFTER the namespace schema, it may contain columns for which we
        //       don't have any schema information yet. This is OK because we've ensured that all file columns are known
        //       within the schema and if a column is NOT part of the file, it will also not be part of the chunk sort
        //       key, so we have consistency here.

        // NOTE: The schema that we've projected here may have a different column order than the actual parquet file. This
        //       is OK because the IOx parquet reader can deal with that (see #4921).

        // calculate sort key
        let partition_sort_key = cached_partition
            .sort_key
            .as_ref()
            .expect("partition sort key should be set when a parquet file exists");
        // use the partition sort key as size hint, because `SortKey::from_cols` with the filtered iterator would
        // otherwise resize the internal HashMap too often.
        let mut builder = SortKeyBuilder::with_capacity(partition_sort_key.column_order.len());
        let cols = partition_sort_key
            .column_order
            .iter()
            .filter(|c_id| col_set.contains(*c_id))
            .filter_map(|c_id| cached_table.column_id_map.get(c_id))
            .cloned();
        // cannot use `for_each` because the borrow checker doesn't like that
        for col in cols {
            builder = builder.with_col(col);
        }
        let sort_key = builder.build();
        assert!(
            !sort_key.is_empty(),
            "Sort key can never be empty because there should at least be a time column",
        );

        let chunk_id = ChunkId::from(Uuid::from_u128(file.id.get() as _));

        let order = ChunkOrder::new(file.max_l0_created_at.get());

        let meta = Arc::new(QuerierParquetChunkMeta {
            chunk_id,
            order,
            sort_key: Some(sort_key),
            partition_id: file.partition_id.clone(),
        });

        let parquet_chunk = Arc::new(ParquetChunk::new(
            file,
            schema,
            self.catalog_cache.parquet_store(),
        ));

        QuerierParquetChunk::new(parquet_chunk, meta, stats)
    }
}

/// [`ParquetFile`] with some additional fields.
struct PreparedParquetFile {
    /// The parquet file as received from the catalog.
    file: Arc<ParquetFile>,

    /// Partition
    cached_partition: Arc<CachedPartition>,

    /// The set of columns in this file.
    col_set: HashSet<ColumnId>,

    /// The columns in this file as ordered in the schema.
    col_list: Box<[ColumnId]>,
}

impl PreparedParquetFile {
    fn new(
        file: Arc<ParquetFile>,
        cached_table: &CachedTable,
        cached_partition: Arc<CachedPartition>,
    ) -> Self {
        // be optimistic and assume the the cached table already knows about all columns. Otherwise
        // `.filter(...).collect()` is too pessimistic and resizes the HashSet too often.
        let mut col_set = HashSet::<ColumnId>::with_capacity(file.column_set.len());
        col_set.extend(
            file.column_set
                .iter()
                .filter(|id| cached_table.column_id_map.contains_key(*id))
                .copied(),
        );

        let mut col_list = col_set.iter().copied().collect::<Box<[ColumnId]>>();
        col_list.sort();

        Self {
            file,
            cached_partition,
            col_set,
            col_list,
        }
    }
}

/// [`ParquetFile`] with even more additional fields.
struct PreparedParquetFileWithStats {
    /// The parquet file as received from the catalog.
    file: Arc<ParquetFile>,

    /// Partition
    cached_partition: Arc<CachedPartition>,

    /// The set of columns in this file.
    col_set: HashSet<ColumnId>,

    /// File schema.
    ///
    /// This may be different than the partition or table schema.
    schema: Schema,

    /// Stats.
    stats: Arc<Statistics>,
}

impl PreparedParquetFileWithStats {
    fn new(file: PreparedParquetFile, schema: Schema) -> Self {
        let PreparedParquetFile {
            file,
            cached_partition,
            col_set,
            col_list,
        } = file;

        // col_list was used to look up schemas, not needed for this transform
        drop(col_list);

        let ts_min_max = TimestampMinMax {
            min: file.min_time.get(),
            max: file.max_time.get(),
        };
        let stats = Arc::new(create_chunk_statistics(
            file.row_count as u64,
            &schema,
            Some(ts_min_max),
            &cached_partition.column_ranges,
        ));

        Self {
            file,
            cached_partition,
            col_set,
            schema,
            stats,
        }
    }
}
