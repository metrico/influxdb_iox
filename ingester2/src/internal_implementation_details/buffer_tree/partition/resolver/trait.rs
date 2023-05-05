use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceId, PartitionKey, TableId};
use parking_lot::Mutex;

use super::super::{
    super::{namespace::NamespaceName, table::TableName},
    PartitionData,
};
use crate::deferred_load::DeferredLoad;

/// An infallible resolver of [`PartitionData`] for the specified table and
/// partition key, returning an initialised [`PartitionData`] buffer for it.
#[async_trait]
pub(crate) trait PartitionProvider: Send + Sync + Debug {
    /// Return an initialised [`PartitionData`] for a given `(partition_key,
    /// table_id)` tuple.
    ///
    /// NOTE: the constructor for [`PartitionData`] is NOT `pub` and SHOULD NOT
    /// be `pub` so this trait is effectively sealed.
    async fn get_partition(
        &self,
        partition_key: PartitionKey,
        namespace_id: NamespaceId,
        namespace_name: Arc<DeferredLoad<NamespaceName>>,
        table_id: TableId,
        table_name: Arc<DeferredLoad<TableName>>,
    ) -> Arc<Mutex<PartitionData>>;
}

#[async_trait]
impl<T> PartitionProvider for Arc<T>
where
    T: PartitionProvider,
{
    async fn get_partition(
        &self,
        partition_key: PartitionKey,
        namespace_id: NamespaceId,
        namespace_name: Arc<DeferredLoad<NamespaceName>>,
        table_id: TableId,
        table_name: Arc<DeferredLoad<TableName>>,
    ) -> Arc<Mutex<PartitionData>> {
        (**self)
            .get_partition(
                partition_key,
                namespace_id,
                namespace_name,
                table_id,
                table_name,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::{
        super::{super::SortKeyState, mock::MockPartitionProvider},
        *,
    };
    use crate::test_util::{
        defer_namespace_name_1_sec, defer_table_name_1_sec, PartitionDataBuilder,
        ARBITRARY_NAMESPACE_ID, ARBITRARY_PARTITION_ID, ARBITRARY_PARTITION_KEY,
        ARBITRARY_TABLE_ID,
    };

    #[tokio::test]
    async fn test_arc_impl() {
        let namespace_loader = defer_namespace_name_1_sec();
        let table_name_loader = defer_table_name_1_sec();

        let data = PartitionDataBuilder::new()
            .with_table_name_loader(Arc::clone(&table_name_loader))
            .with_namespace_loader(Arc::clone(&namespace_loader))
            .build();

        let mock = Arc::new(MockPartitionProvider::default().with_partition(data));

        let got = mock
            .get_partition(
                ARBITRARY_PARTITION_KEY.clone(),
                ARBITRARY_NAMESPACE_ID,
                Arc::clone(&namespace_loader),
                ARBITRARY_TABLE_ID,
                Arc::clone(&table_name_loader),
            )
            .await;
        assert_eq!(got.lock().partition_id(), ARBITRARY_PARTITION_ID);
        assert_eq!(got.lock().namespace_id(), ARBITRARY_NAMESPACE_ID);
        assert_eq!(
            got.lock().namespace_name().to_string(),
            namespace_loader.to_string()
        );
        assert_eq!(
            got.lock().table_name().to_string(),
            table_name_loader.to_string()
        );
    }
}
