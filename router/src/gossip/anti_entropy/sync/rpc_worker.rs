use std::{fmt::Debug, net::SocketAddr};

use generated_types::influxdata::iox::gossip::v1::schema_message::Event;
use gossip::Identity;
use gossip_schema::dispatcher::SchemaEventHandler;
use observability_deps::tracing::{debug, info, warn};
use thiserror::Error;

use crate::gossip::anti_entropy::mst::handle::AntiEntropyHandle;

use super::traits::{BoxedError, SyncRpcClient};

#[derive(Debug, Error)]
enum SyncError {
    #[error(transparent)]
    Boxed(#[from] BoxedError),

    #[error("sync is missing namespace creation event")]
    NoNamespaceCreate,
}

/// A worker task to perform sync RPC requests to a given peer, and merge the
/// resulting gossip event responses into the local schema cache.
///
/// This task lives for the duration of a single sync round and then exits.
#[derive(Debug)]
pub(crate) struct RpcWorker<T, U> {
    /// The RPC client used to perform sync operations against the peer.
    rpc_client: T,

    /// The [`SchemaEventHandler`] that merges gossip events into the schema
    /// cache.
    schema_event_delegate: U,

    /// The [`MerkleSearchTree`] state actor.
    ///
    /// [`MerkleSearchTree`]: merkle_search_tree::MerkleSearchTree
    mst: AntiEntropyHandle,

    // Fields for log context.
    //
    /// The derived RPC service address for the sync peer.
    peer_addr: SocketAddr,
    /// The [`Identity`] of the sync peer.
    identity: Identity,
}

impl<T, U> RpcWorker<T, U>
where
    T: SyncRpcClient,
    U: SchemaEventHandler,
{
    pub(super) fn new(
        rpc_client: T,
        schema_event_delegate: U,
        mst: AntiEntropyHandle,
        peer_addr: SocketAddr,
        identity: Identity,
    ) -> Self {
        Self {
            rpc_client,
            schema_event_delegate,
            mst,
            peer_addr,
            identity,
        }
    }

    pub(super) async fn run(mut self) {
        debug!(
            peer_addr=%self.peer_addr,
            peer_identity=%self.identity,
            "starting cache sync with peer"
        );

        match self.try_sync().await {
            Ok(()) => info!(
                peer_addr=%self.peer_addr,
                peer_identity=%self.identity,
                "completed cache sync with peer"
            ),
            Err(error) => warn!(
                %error,
                peer_addr=%self.peer_addr,
                peer_identity=%self.identity,
                "failed to cache sync with peer"
            ),
        }
    }

    async fn try_sync(&mut self) -> Result<(), SyncError> {
        // Get the current snapshot of the local MST state.
        let snap = self.mst.snapshot().await;

        // Perform the RPC to the peer to identify inconsistent pages.
        //
        // While this sync round was started because the root hashes of two
        // peers differ (meaning they were inconsistent), the inconsistency may
        // have been resolved by the time this RPC call is made, so this may
        // return "not inconsistent" without issue.
        let inconsistent_ranges = self.rpc_client.find_inconsistent_ranges(snap).await?;

        debug!(
            n_ranges=inconsistent_ranges.len(),
            peer_addr=%self.peer_addr,
            peer_identity=%self.identity,
            "identified inconsistent pages"
        );

        for range in inconsistent_ranges {
            // Fetch the schemas in each inconsistent range
            let events = self.rpc_client.get_schemas_in_range(range).await?;

            // Apply each schema (represented as a composition of gossip events
            // needed to create the schema) to the local cache.
            for event in events {
                // Create the namespace if necessary and block for it to be
                // added to the cache.
                self.schema_event_delegate
                    .handle(Event::NamespaceCreated(
                        event.namespace.ok_or(SyncError::NoNamespaceCreate)?,
                    ))
                    .await;

                // Add merge each table schema in this namespace.
                for table in event.tables {
                    self.schema_event_delegate
                        .handle(Event::TableCreated(table))
                        .await;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        net::{IpAddr, Ipv4Addr},
        sync::Arc,
    };

    use data_types::{
        partition_template::{
            NamespacePartitionTemplateOverride, TablePartitionTemplateOverride,
            PARTITION_BY_DAY_PROTO,
        },
        Column, ColumnId, ColumnType, ColumnsByName, MaxColumnsPerTable, MaxTables, NamespaceId,
        NamespaceName, NamespaceSchema, TableId, TableSchema,
    };
    use generated_types::influxdata::iox::gossip::v1::{
        NamespaceCreated, NamespaceSchemaEntry, TableCreated, TableUpdated,
    };

    use crate::{
        gossip::{
            anti_entropy::{
                mst::actor::AntiEntropyActor, sync::mock_rpc_client::MockSyncRpcClient,
            },
            namespace_cache::NamespaceSchemaGossip,
        },
        namespace_cache::{MemoryNamespaceCache, NamespaceCache},
    };

    use super::*;

    const MOCK_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 42);
    const NAMESPACE_NAME: &str = "bananas_namespace";
    const NAMESPACE_NAME2: &str = "platanos_namespace";
    const DEFAULT_NAMESPACE_PARTITION_TEMPLATE: NamespacePartitionTemplateOverride =
        NamespacePartitionTemplateOverride::const_default();
    const DEFAULT_NAMESPACE: NamespaceSchema = NamespaceSchema {
        id: NamespaceId::new(4242),
        tables: BTreeMap::new(),
        max_columns_per_table: MaxColumnsPerTable::new(1),
        max_tables: MaxTables::new(2),
        retention_period_ns: None,
        partition_template: DEFAULT_NAMESPACE_PARTITION_TEMPLATE,
    };

    /// Assert that a sync worker will request the appropriate gossip events
    /// from the remote peer, and merge them into the schema cache.
    ///
    /// This ensures the RpcWorker is correct, but also that the integration
    /// with the SchemaEventHandler is sufficient to converge the cache given a
    /// set of events.
    #[tokio::test]
    async fn test_synchronises_cache() {
        // Schema cache being synchronised, and the gossip event handler that
        // merges events into the cache.
        let schema_cache = Arc::new(MemoryNamespaceCache::default());
        let schema_event_handler = Arc::new(NamespaceSchemaGossip::new(Arc::clone(&schema_cache)));

        // The MST state actor & handle.
        //
        // The content of the MST does not matter - this test simulates pulling
        // from a populated peer into a completely empty local peer.
        let (actor, mst) = AntiEntropyActor::new(Arc::clone(&schema_cache));
        tokio::spawn(actor.run());

        // Initialise the mock RPC client, simulating a server returning the
        // provided responses.
        let rpc_client = Arc::new(
            MockSyncRpcClient::default()
                .with_find_inconsistent_ranges([Ok(vec![
                    name("a")..=name("d"),
                    name("y")..=name("z"),
                ])])
                .with_get_schemas_in_range([
                    // First call, a->d
                    Ok(vec![NamespaceSchemaEntry {
                        namespace: Some(NamespaceCreated {
                            namespace_name: NAMESPACE_NAME.to_string(),
                            namespace_id: DEFAULT_NAMESPACE.id.get(),
                            partition_template: Some((**PARTITION_BY_DAY_PROTO).clone()),
                            max_columns_per_table: DEFAULT_NAMESPACE.max_columns_per_table.get()
                                as _,
                            max_tables: DEFAULT_NAMESPACE.max_tables.get() as _,
                            retention_period_ns: DEFAULT_NAMESPACE.retention_period_ns,
                        }),
                        tables: vec![
                            TableCreated {
                                table: Some(TableUpdated {
                                    table_name: "bananas1".to_string(),
                                    namespace_name: NAMESPACE_NAME.to_string(),
                                    table_id: 421,
                                    columns: vec![],
                                }),
                                partition_template: Some((**PARTITION_BY_DAY_PROTO).clone()),
                            },
                            TableCreated {
                                table: Some(TableUpdated {
                                    table_name: "bananas2".to_string(),
                                    namespace_name: NAMESPACE_NAME.to_string(),
                                    table_id: 422,
                                    columns: vec![],
                                }),
                                partition_template: None,
                            },
                        ],
                    }]),
                    // Second call, y->z
                    Ok(vec![NamespaceSchemaEntry {
                        namespace: Some(NamespaceCreated {
                            namespace_name: NAMESPACE_NAME2.to_string(),
                            namespace_id: 2424,
                            partition_template: None,
                            max_columns_per_table: 1234,
                            max_tables: 666,
                            retention_period_ns: Some(4321),
                        }),
                        tables: vec![TableCreated {
                            table: Some(TableUpdated {
                                table_name: "platanos".to_string(),
                                namespace_name: NAMESPACE_NAME2.to_string(),
                                table_id: 423,
                                columns: vec![
                                    generated_types::influxdata::iox::gossip::v1::Column {
                                        name: "c1".to_string(),
                                        column_id: 101,
                                        column_type: ColumnType::U64 as _,
                                    },
                                ],
                            }),
                            partition_template: None,
                        }],
                    }]),
                ]),
        );

        // Initialise the worker task.
        let sync_worker = RpcWorker::new(
            Arc::clone(&rpc_client),
            schema_event_handler,
            mst,
            MOCK_ADDR,
            Identity::try_from(b"1234567890123456".to_vec()).unwrap(),
        );

        // Run the sync worker to completion.
        sync_worker.run().await;

        // The cache should now contain the two namespaces pulled from the peer.
        let bananas = schema_cache
            .get_schema(&name(NAMESPACE_NAME))
            .await
            .expect("schema pulled from peer must exist");

        pretty_assertions::assert_eq!(
            *bananas,
            NamespaceSchema {
                tables: [
                    (
                        "bananas1",
                        TableSchema {
                            id: TableId::new(421),
                            partition_template: TablePartitionTemplateOverride::try_new(
                                Some((**PARTITION_BY_DAY_PROTO).clone()),
                                &DEFAULT_NAMESPACE_PARTITION_TEMPLATE,
                            )
                            .unwrap(),
                            columns: ColumnsByName::default(),
                        },
                    ),
                    (
                        "bananas2",
                        TableSchema {
                            id: TableId::new(422),
                            partition_template: TablePartitionTemplateOverride::try_new(
                                None,
                                &NamespacePartitionTemplateOverride::try_from(
                                    (**PARTITION_BY_DAY_PROTO).clone()
                                )
                                .unwrap(),
                            )
                            .unwrap(),
                            columns: ColumnsByName::default(),
                        },
                    ),
                ]
                .map(|(a, b)| (a.to_string(), b))
                .into_iter()
                .collect(),
                partition_template: NamespacePartitionTemplateOverride::try_from(
                    (**PARTITION_BY_DAY_PROTO).clone()
                )
                .unwrap(),
                ..DEFAULT_NAMESPACE.clone()
            }
        );

        // Validate the second namespace (ensuring all were pulled)
        let platanos = schema_cache
            .get_schema(&name(NAMESPACE_NAME2))
            .await
            .expect("schema pulled from peer must exist");

        pretty_assertions::assert_eq!(
            *platanos,
            NamespaceSchema {
                id: NamespaceId::new(2424),
                max_columns_per_table: MaxColumnsPerTable::new(1234),
                max_tables: MaxTables::new(666),
                retention_period_ns: Some(4321),
                partition_template: Default::default(),
                tables: [(
                    "platanos",
                    TableSchema {
                        id: TableId::new(423),
                        partition_template: Default::default(),
                        columns: ColumnsByName::new([Column {
                            id: ColumnId::new(101),
                            table_id: TableId::new(423),
                            name: "c1".to_string(),
                            column_type: ColumnType::U64,
                        }]),
                    },
                ),]
                .map(|(a, b)| (a.to_string(), b))
                .into_iter()
                .collect(),
            }
        );
    }

    fn name(s: &str) -> NamespaceName<'static> {
        NamespaceName::new(s.to_string()).unwrap()
    }
}
