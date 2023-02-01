use std::sync::Arc;

use data_types::{CompactionLevel, ParquetFileParams, PartitionId};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{stream::FuturesOrdered, Future, TryFutureExt, TryStreamExt};
use iox_time::Time;

use crate::{components::Components, partition_info::PartitionInfo};

use super::Error;

pub async fn fetch_partition_info(
    partition_id: PartitionId,
    components: &Arc<Components>,
) -> Result<Arc<PartitionInfo>, Error> {
    // TODO: only read partition, table and its schema info the first time and cache them
    // Get info for the partition
    let partition = components
        .partition_source
        .fetch_by_id(partition_id)
        .await
        .ok_or_else::<Error, _>(|| String::from("Cannot find partition info").into())?;

    let table = components
        .tables_source
        .fetch(partition.table_id)
        .await
        .ok_or_else::<Error, _>(|| String::from("Cannot find table").into())?;

    // TODO: after we have catalog funciton to read table schema, we should use it
    // and avoid reading namespace schema
    let namespace = components
        .namespaces_source
        .fetch_by_id(table.namespace_id)
        .await
        .ok_or_else::<Error, _>(|| String::from("Cannot find namespace").into())?;

    let namespace_schema = components
        .namespaces_source
        .fetch_schema_by_id(table.namespace_id)
        .await
        .ok_or_else::<Error, _>(|| String::from("Cannot find namespace schema").into())?;

    let table_schema = namespace_schema
        .tables
        .get(&table.name)
        .ok_or_else::<Error, _>(|| String::from("Cannot find table schema").into())?;

    Ok(Arc::new(PartitionInfo {
        partition_id,
        namespace_id: table.namespace_id,
        namespace_name: namespace.name,
        table: Arc::new(table),
        table_schema: Arc::new(table_schema.clone()),
        sort_key: partition.sort_key(),
        partition_key: partition.partition_key,
    }))
}

pub fn stream_into_file_sink(
    streams: Vec<SendableRecordBatchStream>,
    partition_info: Arc<PartitionInfo>,
    target_level: CompactionLevel,
    max_l0_created_at: Time,
    components: Arc<Components>,
) -> impl Future<Output = Result<Vec<ParquetFileParams>, Error>> {
    streams
        .into_iter()
        .map(move |stream| {
            let components = Arc::clone(&components);
            let partition_info = Arc::clone(&partition_info);
            async move {
                components
                    .parquet_file_sink
                    .store(stream, partition_info, target_level, max_l0_created_at)
                    .await
            }
        })
        // NB: FuturesOrdered allows the futures to run in parallel
        .collect::<FuturesOrdered<_>>()
        // Discard the streams that resulted in empty output / no file uploaded
        // to the object store.
        .try_filter_map(|v| futures::future::ready(Ok(v)))
        // Collect all the persisted parquet files together.
        .try_collect::<Vec<_>>()
        .map_err(|e| Box::new(e) as _)
}
