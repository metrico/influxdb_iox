//! In memory queryable buffer of data sent from one or more ingesters. It evicts data from the
//! buffer when persist requests are sent in.

use crate::query::partition_response::PartitionResponse;
use crate::query::response::PartitionStream;
use crate::{
    cache::SchemaCache,
    query::{response::QueryResponse, QueryError, QueryExec},
    BufferError, ReplicationBuffer, TableIdToMutableBatch,
};
use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, StringDictionaryBuilder,
    TimestampNanosecondBuilder, UInt64Builder,
};
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;
use arrow_util::bitset::iter_set_positions;
use async_trait::async_trait;
use data_types::{
    sequence_number_set::SequenceNumberSet, ColumnId, ColumnType, NamespaceId, PartitionId,
    PartitionKey, SequenceNumber, TableId, TableSchema, Timestamp,
};
use datafusion_util::MemoryStream;
use iox_query::exec::Executor;
use mutable_batch::{
    column::{Column as MBColumn, ColumnData as MBColumnData},
    MutableBatch,
};
use parking_lot::RwLock;
use schema::{InfluxColumnType, InfluxFieldType, SchemaBuilder};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use trace::span::Span;
use uuid::Uuid;

pub(crate) struct Buffer {
    schema_cache: Arc<SchemaCache>,
    _exec: Arc<Executor>,
    namespaces: RwLock<BTreeMap<NamespaceId, Arc<NamespaceBuffer>>>,
}

impl Buffer {
    pub(crate) fn new(schema_cache: Arc<SchemaCache>, _exec: Arc<Executor>) -> Self {
        Self {
            schema_cache,
            _exec,
            namespaces: Default::default(),
        }
    }

    pub(crate) async fn apply_write(
        &self,
        namespace_id: NamespaceId,
        partition_key: PartitionKey,
        table_batches: TableIdToMutableBatch,
        ingester_id: Uuid,
        sequence_number: SequenceNumber,
    ) -> Result<(), BufferError> {
        // first get all the partition IDs.
        let mut partition_batches = Vec::with_capacity(table_batches.len());

        for (table_id, mb) in table_batches.into_iter() {
            let table_id = TableId::new(table_id);
            let partition_id = self
                .schema_cache
                .get_partition_id(table_id, partition_key.clone())
                .await?;

            partition_batches.push((table_id, partition_id, mb));
        }

        // now that we have all info we need, traverse the buffer structure while holding a lock to
        // get clones of the individual partitions.
        let partition_batches: Vec<_> = {
            let namespace = self.get_namespace(namespace_id);

            partition_batches
                .into_iter()
                .map(|(table_id, partition_id, mb)| {
                    let partition = namespace.get_partition(table_id, partition_id);

                    (partition, mb)
                })
                .collect()
        };

        for (partition_buffer, mb) in partition_batches {
            let mut table_schema = self
                .schema_cache
                .get_table_schema(partition_buffer.table_id)
                .await?;

            // ensure the cached table schema has all the definitions from the mutable batch and
            // that they match.
            if !table_schema_contains_mb_columns(&table_schema, &mb)? {
                table_schema = self
                    .schema_cache
                    .get_table_schema_from_catalog(partition_buffer.table_id)
                    .await?;

                // do another validation or exit because we won't successfully ingest this mb
                if !table_schema_contains_mb_columns(&table_schema, &mb)? {
                    return Err(BufferError::UnresolvableSchema);
                }
            }

            partition_buffer.buffer_batch(ingester_id, sequence_number, table_schema, &mb);
        }

        Ok(())
    }

    async fn query_exec(
        &self,
        namespace_id: NamespaceId,
        table_id: TableId,
        columns: Vec<String>,
        _span: Option<Span>,
    ) -> Result<QueryResponse, QueryError> {
        let partition_buffers = self
            .namespaces
            .read()
            .get(&namespace_id)
            .and_then(|n| {
                n.tables
                    .read()
                    .get(&table_id)
                    .map(|t| t.partitions.values().cloned().collect::<Vec<_>>())
            })
            .unwrap_or_default();
        let mut table_schema = self.schema_cache.get_table_schema(table_id).await?;

        let mut partitions = Vec::with_capacity(partition_buffers.len());

        for p in partition_buffers.into_iter() {
            let (persist_count, batches) =
                match p.get_query_data(Arc::clone(&table_schema), &columns) {
                    Ok(r) => r,
                    Err(_) => {
                        table_schema = match self
                            .schema_cache
                            .get_table_schema_from_catalog(table_id)
                            .await
                        {
                            Ok(t) => t,
                            Err(e) => return Err(QueryError::Cache(e)),
                        };

                        p.get_query_data(Arc::clone(&table_schema), &columns)?
                    }
                };

            let data = Box::pin(MemoryStream::new(batches));

            partitions.push(PartitionResponse::new(
                Some(data),
                p.partition_id,
                persist_count as u64,
            ));
        }

        Ok(QueryResponse::new(PartitionStream::new(
            futures::stream::iter(partitions),
        )))
    }

    fn get_namespace(&self, namespace_id: NamespaceId) -> Arc<NamespaceBuffer> {
        let namespace = self.namespaces.read().get(&namespace_id).cloned();

        match namespace {
            Some(n) => n,
            None => Arc::clone(
                self.namespaces
                    .write()
                    .entry(namespace_id)
                    .or_insert_with(|| {
                        Arc::new(NamespaceBuffer {
                            id: namespace_id,
                            tables: Default::default(),
                        })
                    }),
            ),
        }
    }
}

// returns true if all columns in the mutable batch exist in the passed table schema and match
// type. False if the MB has a column not in the schema. Returns an error if one of the columns
// doesn't match type.
fn table_schema_contains_mb_columns(
    table_schema: &TableSchema,
    mb: &MutableBatch,
) -> Result<bool, BufferError> {
    for (name, col) in mb.columns() {
        match table_schema.columns.get(name) {
            Some(col_schema) => match (col.influx_type(), col_schema.column_type) {
                (InfluxColumnType::Tag, ColumnType::Tag) => (),
                (InfluxColumnType::Timestamp, ColumnType::Time) => (),
                (InfluxColumnType::Field(InfluxFieldType::Integer), ColumnType::I64) => (),
                (InfluxColumnType::Field(InfluxFieldType::Float), ColumnType::F64) => (),
                (InfluxColumnType::Field(InfluxFieldType::UInteger), ColumnType::U64) => (),
                (InfluxColumnType::Field(InfluxFieldType::Boolean), ColumnType::Bool) => (),
                (InfluxColumnType::Field(InfluxFieldType::String), ColumnType::String) => (),
                _ => return Err(BufferError::ColumnTypeMismatch),
            },
            None => return Ok(false),
        }
    }

    Ok(true)
}

struct NamespaceBuffer {
    id: NamespaceId,
    tables: RwLock<BTreeMap<TableId, TableBuffer>>,
}

impl NamespaceBuffer {
    fn get_partition(&self, table_id: TableId, partition_id: PartitionId) -> Arc<PartitionBuffer> {
        let partition = self
            .tables
            .read()
            .get(&table_id)
            .and_then(|t| t.partitions.get(&partition_id).cloned());

        match partition {
            Some(p) => p,
            None => {
                // insert the table and the partition
                let mut tables = self.tables.write();
                Arc::clone(
                    tables
                        .entry(table_id)
                        .or_default()
                        .partitions
                        .entry(partition_id)
                        .or_insert_with(|| {
                            Arc::new(PartitionBuffer {
                                namespace_id: self.id,
                                table_id,
                                partition_id,
                                data: Default::default(),
                            })
                        }),
                )
            }
        }
    }
}

#[derive(Default)]
struct TableBuffer {
    partitions: BTreeMap<PartitionId, Arc<PartitionBuffer>>,
}

pub(crate) struct PartitionBuffer {
    namespace_id: NamespaceId,
    table_id: TableId,
    partition_id: PartitionId,
    data: RwLock<PartitionBufferData>,
}

impl PartitionBuffer {
    // Converts the mutable batch into rows the partition buffer can handle using the provided
    // table schema. The caller must validate the table_schema contains everything in the
    // mutable batch and that there are no conflicts. Appending data is an all or nothing
    // where failing in the middle corrupts the buffer. This should only be called by the Buffer.
    fn buffer_batch(
        &self,
        ingester_id: Uuid,
        sequence_number: SequenceNumber,
        table_schema: Arc<TableSchema>,
        mutable_batch: &MutableBatch,
    ) {
        let mut data = self.data.write();
        data.buffer_batch(
            ingester_id,
            sequence_number,
            self.table_id,
            table_schema,
            mutable_batch,
        );
    }

    // Converts the buffer data into RecordBatch wrapped in a QueryAdaptor. Will error if the
    // passed in schema doesn't match the data in the buffer.
    // TODO: handle projection
    pub(crate) fn get_query_data(
        &self,
        table_schema: Arc<TableSchema>,
        _columns: &[String],
    ) -> Result<(PersistCount, Vec<RecordBatch>), BufferError> {
        let data = self.data.read();

        let mut column_id_to_name = HashMap::new();
        let mut schema_builder = SchemaBuilder::new();
        for (column_name, column_schema) in &table_schema.columns {
            if data.data_columns.contains_key(&column_schema.id) {
                column_id_to_name.insert(column_schema.id, column_name);
                schema_builder.influx_column(column_name, column_schema.column_type.into());
            }
        }
        let schema = schema_builder
            .build()
            .expect("schema to build")
            .sort_fields_by_name();

        let mut cols: Vec<_> = data.data_columns.values().collect();
        cols.sort_by(|a, b| {
            let a_name = column_id_to_name
                .get(&a.column_id)
                .map(|n| n.as_str())
                .unwrap_or("UNKNOWN");
            let b_name = column_id_to_name
                .get(&b.column_id)
                .map(|n| n.as_str())
                .unwrap_or("UNKNOWN");
            Ord::cmp(a_name, b_name)
        });
        let cols: Vec<ArrayRef> = cols.into_iter().map(|c| c.to_arrow()).collect();

        assert_eq!(schema.len(), cols.len());

        let batch = RecordBatch::try_new(schema.as_arrow(), cols)?;

        Ok((data.persist_count, vec![batch]))
    }
}

type PersistCount = usize;

#[derive(Default)]
struct PartitionBufferData {
    persist_count: PersistCount,
    ingester_sequence_column: Vec<IngesterSequence>,
    data_columns: BTreeMap<ColumnId, ColumnData>,
}

impl PartitionBufferData {
    // buffers the mutable batch. By the time we reach this call, all validation must have been done
    // as this function will panic on any error in schema.
    fn buffer_batch(
        &mut self,
        ingester_id: Uuid,
        sequence_number: SequenceNumber,
        _table_id: TableId,
        table_schema: Arc<TableSchema>,
        mutable_batch: &MutableBatch,
    ) {
        let starting_row_count = self.ingester_sequence_column.len();
        let rows_added = mutable_batch.rows();
        let mut columns_added_to = HashSet::new();

        for (name, column) in mutable_batch.columns() {
            let column_schema = table_schema
                .columns
                .get(name)
                .expect("column not found in table schema");
            let column_id = column_schema.id;

            let column_data = self.data_columns.entry(column_id).or_insert_with(|| {
                let mut cd = ColumnData::new(column_id, column_schema.column_type);
                cd.add_null_rows(starting_row_count);
                cd
            });

            column_data
                .append_column(column)
                .expect("schema mismatch with mutable batch");
            columns_added_to.insert(column_id);
        }

        // add nulls for any columns in this schema that didn't have data in the mutable batch
        for (id, col) in &mut self.data_columns {
            if !columns_added_to.contains(id) {
                col.add_null_rows(rows_added);
            }
        }

        // and finally add the ingester and sequence numbers tracking so that data can be pruned
        // when we receive a persist notification.
        for _ in 0..rows_added {
            self.ingester_sequence_column.push(IngesterSequence {
                ingester_id,
                sequence_number,
            });
        }
    }
}

#[derive(Debug, Copy, Clone)]
struct IngesterSequence {
    ingester_id: Uuid,
    sequence_number: SequenceNumber,
}

struct ColumnData {
    column_id: ColumnId,
    builder: Builder,
}

impl ColumnData {
    fn new(column_id: ColumnId, t: ColumnType) -> Self {
        match t {
            ColumnType::I64 => Self {
                column_id,
                builder: Builder::I64(Int64Builder::new()),
            },
            ColumnType::F64 => Self {
                column_id,
                builder: Builder::F64(Float64Builder::new()),
            },
            ColumnType::U64 => Self {
                column_id,
                builder: Builder::U64(UInt64Builder::new()),
            },
            ColumnType::Tag => Self {
                column_id,
                builder: Builder::Tag(StringDictionaryBuilder::new()),
            },
            ColumnType::String => Self {
                column_id,
                builder: Builder::String(StringBuilder::new()),
            },
            ColumnType::Time => Self {
                column_id,
                builder: Builder::Time(TimestampNanosecondBuilder::new()),
            },
            ColumnType::Bool => Self {
                column_id,
                builder: Builder::Bool(BooleanBuilder::new()),
            },
        }
    }

    fn add_null_rows(&mut self, row_count: usize) {
        match &mut self.builder {
            Builder::Bool(b) => {
                for _ in 0..row_count {
                    b.append_null();
                }
            }
            Builder::I64(b) => {
                for _ in 0..row_count {
                    b.append_null();
                }
            }
            Builder::F64(b) => {
                for _ in 0..row_count {
                    b.append_null();
                }
            }
            Builder::U64(b) => {
                for _ in 0..row_count {
                    b.append_null();
                }
            }
            Builder::String(b) => {
                for _ in 0..row_count {
                    b.append_null();
                }
            }
            Builder::Tag(b) => {
                for _ in 0..row_count {
                    b.append_null();
                }
            }
            Builder::Time(b) => {
                for _ in 0..row_count {
                    b.append_null();
                }
            }
        }
    }

    fn into_arrow(self) -> ArrayRef {
        match self.builder {
            Builder::Bool(mut b) => Arc::new(b.finish()),
            Builder::I64(mut b) => Arc::new(b.finish()),
            Builder::F64(mut b) => Arc::new(b.finish()),
            Builder::U64(mut b) => Arc::new(b.finish()),
            Builder::String(mut b) => Arc::new(b.finish()),
            Builder::Tag(mut b) => Arc::new(b.finish()),
            Builder::Time(mut b) => Arc::new(b.finish()),
        }
    }

    fn to_arrow(&self) -> ArrayRef {
        match &self.builder {
            Builder::Bool(b) => Arc::new(b.finish_cloned()),
            Builder::I64(b) => Arc::new(b.finish_cloned()),
            Builder::F64(b) => Arc::new(b.finish_cloned()),
            Builder::U64(b) => Arc::new(b.finish_cloned()),
            Builder::String(b) => Arc::new(b.finish_cloned()),
            Builder::Tag(b) => Arc::new(b.finish_cloned()),
            Builder::Time(b) => Arc::new(b.finish_cloned()),
        }
    }

    fn append_column(&mut self, column: &MBColumn) -> Result<(), BufferError> {
        let valid_mask = column.valid_mask();

        match (column.data(), &mut self.builder) {
            (MBColumnData::I64(vals, _), Builder::I64(b)) => {
                for (i, v) in vals.iter().enumerate() {
                    if valid_mask.get(i) {
                        b.append_value(*v);
                    } else {
                        b.append_null();
                    }
                }
            }
            (MBColumnData::I64(vals, _), Builder::Time(b)) => {
                for (i, v) in vals.iter().enumerate() {
                    if valid_mask.get(i) {
                        b.append_value(*v);
                    } else {
                        b.append_null();
                    }
                }
            }
            (MBColumnData::F64(vals, _), Builder::F64(b)) => {
                for (i, v) in vals.iter().enumerate() {
                    if valid_mask.get(i) {
                        b.append_value(*v);
                    } else {
                        b.append_null();
                    }
                }
            }
            (MBColumnData::U64(vals, _), Builder::U64(b)) => {
                for (i, v) in vals.iter().enumerate() {
                    if valid_mask.get(i) {
                        b.append_value(*v);
                    } else {
                        b.append_null();
                    }
                }
            }
            (MBColumnData::Bool(vals, _), Builder::Bool(b)) => {
                for i in iter_set_positions(vals.bytes()) {
                    if valid_mask.get(i) {
                        b.append_value(vals.get(i));
                    } else {
                        b.append_null();
                    }
                }
            }
            (MBColumnData::String(vals, _), Builder::String(b)) => {
                for (i, val) in vals.iter().enumerate() {
                    if valid_mask.get(i) {
                        b.append_value(val);
                    } else {
                        b.append_null();
                    }
                }
            }
            (MBColumnData::Tag(vals, dict, _), Builder::Tag(b)) => {
                for (i, did) in vals.iter().enumerate() {
                    if valid_mask.get(i) {
                        if let Some(tag_value) = dict.lookup_id(*did) {
                            b.append(tag_value)
                                .expect("should be able to append tag to arrow dictionary");
                        } else {
                            b.append("UNKNOWN")
                                .expect("should be able to append tag to arrow dictionary");
                        }
                    } else {
                        b.append_null();
                    }
                }
            }
            _ => return Err(BufferError::ColumnTypeMismatch),
        }

        Ok(())
    }

    fn append_null(&mut self) {
        match &mut self.builder {
            Builder::Bool(b) => b.append_null(),
            Builder::I64(b) => b.append_null(),
            Builder::F64(b) => b.append_null(),
            Builder::U64(b) => b.append_null(),
            Builder::String(b) => b.append_null(),
            Builder::Tag(b) => b.append_null(),
            Builder::Time(b) => b.append_null(),
        }
    }

    fn append_time(&mut self, time: Timestamp) {
        if let Builder::Time(b) = &mut self.builder {
            b.append_value(time.get())
        }
    }
}

enum Builder {
    Bool(BooleanBuilder),
    I64(Int64Builder),
    F64(Float64Builder),
    U64(UInt64Builder),
    String(StringBuilder),
    Tag(StringDictionaryBuilder<Int32Type>),
    Time(TimestampNanosecondBuilder),
}

#[async_trait]
impl ReplicationBuffer for Buffer {
    async fn apply_write(
        &self,
        namespace_id: NamespaceId,
        partition_key: PartitionKey,
        table_batches: TableIdToMutableBatch,
        ingester_id: Uuid,
        sequence_number: SequenceNumber,
    ) -> Result<(), BufferError> {
        self.apply_write(
            namespace_id,
            partition_key,
            table_batches,
            ingester_id,
            sequence_number,
        )
        .await
    }

    async fn apply_persist(
        &self,
        _ingester_id: Uuid,
        _namespace_id: NamespaceId,
        _table_id: TableId,
        _partition_id: PartitionId,
        _sequence_set: SequenceNumberSet,
    ) -> Result<(), BufferError> {
        panic!("unimplemented")
    }

    async fn append_partition_buffer(
        &self,
        _ingester_id: Uuid,
        _namespace_id: NamespaceId,
        _table_id: TableId,
        _partition_id: PartitionId,
        _sequence_set: SequenceNumberSet,
        _table_batches: TableIdToMutableBatch,
    ) -> Result<(), BufferError> {
        panic!("unimplemented")
    }
}

impl Debug for Buffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Buffer namespaces: {:#?}",
            self.namespaces.read().keys().collect::<Vec<_>>()
        ))
    }
}

#[async_trait]
impl QueryExec for Buffer {
    type Response = QueryResponse;

    async fn query_exec(
        &self,
        namespace_id: NamespaceId,
        table_id: TableId,
        columns: Vec<String>,
        span: Option<Span>,
    ) -> Result<Self::Response, QueryError> {
        self.query_exec(namespace_id, table_id, columns, span).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_util::assert_batches_eq;
    use data_types::{ColumnSchema, ShardIndex};
    use futures::TryStreamExt;
    use iox_catalog::interface::Catalog;
    use iox_catalog::mem::MemCatalog;
    use metric::Registry;
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
    use schema::TIME_COLUMN_NAME;
    use std::ops::Deref;

    #[tokio::test]
    async fn test_partition_buffer() {
        let partition = PartitionBuffer {
            namespace_id: NamespaceId::new(1),
            table_id: TableId::new(1),
            partition_id: PartitionId::new(1),
            data: Default::default(),
        };

        let (_, mb) = lp_to_mutable_batch("cpu,host=a f1=123i 12\ncpu f2=1.1 13");
        let mut table_schema = TableSchema::new(partition.table_id);
        table_schema.columns.insert(
            "host".to_string(),
            ColumnSchema {
                id: ColumnId::new(1),
                column_type: ColumnType::Tag,
            },
        );
        table_schema.columns.insert(
            "f1".to_string(),
            ColumnSchema {
                id: ColumnId::new(2),
                column_type: ColumnType::I64,
            },
        );
        table_schema.columns.insert(
            "f2".to_string(),
            ColumnSchema {
                id: ColumnId::new(3),
                column_type: ColumnType::F64,
            },
        );
        table_schema.columns.insert(
            "f3".to_string(),
            ColumnSchema {
                id: ColumnId::new(4),
                column_type: ColumnType::String,
            },
        );
        table_schema.columns.insert(
            TIME_COLUMN_NAME.to_string(),
            ColumnSchema {
                id: ColumnId::new(5),
                column_type: ColumnType::Time,
            },
        );

        partition.buffer_batch(
            Uuid::new_v4(),
            SequenceNumber::new(1),
            Arc::new(table_schema.clone()),
            &mb,
        );

        let (_, batches) = partition
            .get_query_data(Arc::new(table_schema.clone()), &[])
            .unwrap();

        let expected_data = &[
            "+-----+-----+------+--------------------------------+",
            "| f1  | f2  | host | time                           |",
            "+-----+-----+------+--------------------------------+",
            "| 123 |     | a    | 1970-01-01T00:00:00.000000012Z |",
            "|     | 1.1 |      | 1970-01-01T00:00:00.000000013Z |",
            "+-----+-----+------+--------------------------------+",
        ];

        assert_batches_eq!(expected_data, &batches);

        // ensure we can buffer data with new columns and it works
        table_schema.columns.insert(
            "newtag".to_string(),
            ColumnSchema {
                id: ColumnId::new(6),
                column_type: ColumnType::Tag,
            },
        );
        table_schema.columns.insert(
            "f4".to_string(),
            ColumnSchema {
                id: ColumnId::new(7),
                column_type: ColumnType::Bool,
            },
        );
        table_schema.columns.insert(
            "f5".to_string(),
            ColumnSchema {
                id: ColumnId::new(8),
                column_type: ColumnType::String,
            },
        );

        let (_, mb) = lp_to_mutable_batch("cpu,newtag=b f4=true,f5=\"hi\" 15");
        partition.buffer_batch(
            Uuid::new_v4(),
            SequenceNumber::new(2),
            Arc::new(table_schema.clone()),
            &mb,
        );

        let (_, batches) = partition
            .get_query_data(Arc::new(table_schema), &[])
            .unwrap();

        let expected_data = &[
            "+-----+-----+------+----+------+--------+--------------------------------+",
            "| f1  | f2  | f4   | f5 | host | newtag | time                           |",
            "+-----+-----+------+----+------+--------+--------------------------------+",
            "| 123 |     |      |    | a    |        | 1970-01-01T00:00:00.000000012Z |",
            "|     | 1.1 |      |    |      |        | 1970-01-01T00:00:00.000000013Z |",
            "|     |     | true | hi |      | b      | 1970-01-01T00:00:00.000000015Z |",
            "+-----+-----+------+----+------+--------+--------------------------------+",
        ];

        assert_batches_eq!(expected_data, &batches);
    }

    #[tokio::test]
    async fn buffer_and_query_with_schema_update() {
        let metrics = Arc::new(Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let topic = catalog
            .repositories()
            .await
            .topics()
            .create_or_get("foo")
            .await
            .unwrap();
        let transition_shard = catalog
            .repositories()
            .await
            .shards()
            .create_or_get(&topic, ShardIndex::new(1))
            .await
            .unwrap();
        let query_pool = catalog
            .repositories()
            .await
            .query_pools()
            .create_or_get("whatevs")
            .await
            .unwrap();
        let namespace = catalog
            .repositories()
            .await
            .namespaces()
            .create("asdf", None, topic.id, query_pool.id)
            .await
            .unwrap();
        let table = catalog
            .repositories()
            .await
            .tables()
            .create_or_get("m1", namespace.id)
            .await
            .unwrap();
        let partition_key = PartitionKey::from("1970-01-01");
        catalog
            .repositories()
            .await
            .partitions()
            .create_or_get(partition_key.clone(), transition_shard.id, table.id)
            .await
            .unwrap();

        let cols = HashMap::from([
            ("t1", ColumnType::Tag),
            ("f1", ColumnType::I64),
            (TIME_COLUMN_NAME, ColumnType::Time),
        ]);
        catalog
            .repositories()
            .await
            .columns()
            .create_or_get_many_unchecked(table.id, cols)
            .await
            .unwrap();

        let schema_cache = SchemaCache::new(Arc::clone(&catalog), transition_shard.id);
        let buffer = Buffer::new(Arc::new(schema_cache), Arc::new(Executor::new_testing()));
        let (_, mb) = lp_to_mutable_batch("m1,t1=hi f1=12i 123");
        let mut table_batches = TableIdToMutableBatch::new();
        table_batches.insert(table.id.get(), mb);
        buffer
            .apply_write(
                namespace.id,
                partition_key.clone(),
                table_batches,
                Uuid::new_v4(),
                SequenceNumber::new(1),
            )
            .await
            .unwrap();

        let res = buffer
            .query_exec(
                namespace.id,
                table.id,
                vec![
                    "t1".to_string(),
                    "f1".to_string(),
                    TIME_COLUMN_NAME.to_string(),
                ],
                None,
            )
            .await
            .unwrap();

        let batches = res
            .into_record_batches()
            .try_collect::<Vec<_>>()
            .await
            .expect("query failed")
            .iter()
            .map(|b| b.deref().clone())
            .collect::<Vec<_>>();

        let expected_data = &[
            "+----+----+--------------------------------+",
            "| f1 | t1 | time                           |",
            "+----+----+--------------------------------+",
            "| 12 | hi | 1970-01-01T00:00:00.000000123Z |",
            "+----+----+--------------------------------+",
        ];

        assert_batches_eq!(expected_data, &batches);

        // now update the schema, write some data in and ensure it returns
        let cols = HashMap::from([("t2", ColumnType::Tag), ("f2", ColumnType::F64)]);
        catalog
            .repositories()
            .await
            .columns()
            .create_or_get_many_unchecked(table.id, cols)
            .await
            .unwrap();

        let (_, mb) = lp_to_mutable_batch("m1,t2=world f1=3i,f2=1.2 140");
        let mut table_batches = TableIdToMutableBatch::new();
        table_batches.insert(table.id.get(), mb);
        buffer
            .apply_write(
                namespace.id,
                partition_key,
                table_batches,
                Uuid::new_v4(),
                SequenceNumber::new(2),
            )
            .await
            .unwrap();

        let res = buffer
            .query_exec(
                namespace.id,
                table.id,
                vec![
                    "t1".to_string(),
                    "t2".to_string(),
                    "f1".to_string(),
                    "f2".to_string(),
                    TIME_COLUMN_NAME.to_string(),
                ],
                None,
            )
            .await
            .unwrap();

        let batches = res
            .into_record_batches()
            .try_collect::<Vec<_>>()
            .await
            .expect("query failed")
            .iter()
            .map(|b| b.deref().clone())
            .collect::<Vec<_>>();

        let expected_data = &[
            "+----+-----+----+-------+--------------------------------+",
            "| f1 | f2  | t1 | t2    | time                           |",
            "+----+-----+----+-------+--------------------------------+",
            "| 12 |     | hi |       | 1970-01-01T00:00:00.000000123Z |",
            "| 3  | 1.2 |    | world | 1970-01-01T00:00:00.000000140Z |",
            "+----+-----+----+-------+--------------------------------+",
        ];

        assert_batches_eq!(expected_data, &batches);
    }
}
