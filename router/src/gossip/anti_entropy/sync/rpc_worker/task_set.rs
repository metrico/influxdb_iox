use std::net::SocketAddr;

use gossip::Identity;
use gossip_schema::dispatcher::SchemaEventHandler;
use hashbrown::{hash_map::Entry, HashMap};
use observability_deps::tracing::warn;
use thiserror::Error;
use tokio::task::JoinHandle;

use crate::gossip::anti_entropy::{mst::handle::AntiEntropyHandle, sync::traits::SyncRpcConnector};

use super::RpcWorker;

/// [`MAX_CONCURRENT_SYNC_TASKS`] defines the maximum number of sync tasks that
/// may be running at any time.
const MAX_CONCURRENT_SYNC_TASKS: usize = 3;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("maximum concurrent sync tasks already running")]
    MaxConcurrentSyncs,

    #[error("existing sync for peer already running")]
    ConcurrentSync,
}

/// A set of ongoing [`RpcWorker`] sync tasks.
///
/// The [`MergeTaskSet`] ensures at most [`MAX_CONCURRENT_SYNC_TASKS`] are
/// running at any time, with at most one task running per [`Identity`].
///
/// This effectively bounds the overhead of sync operations, and prevents
/// redundant sync operations from being started.
#[derive(Debug)]
pub(crate) struct MergeTaskSet<T, U> {
    /// An abstract connector returning a [`SyncRpcClient`] instance for a given
    /// peer RPC address.
    ///
    /// [`SyncRpcClient`]: crate::gossip::anti_entropy::sync::traits::SyncRpcClient
    connector: T,

    /// An abstract [`SchemaEventHandler`] responsible for processing the
    /// incoming stream of gossip events, merging their payloads into the local
    /// [`NamespaceCache`].
    ///
    /// [`NamespaceCache`]: crate::namespace_cache::NamespaceCache
    merge_delegate: U,

    /// A handle to the Merkle Search Tree state actor.
    mst: AntiEntropyHandle,

    /// The set of sync tasks previously started.
    ///
    /// This set may contain terminated tasks.
    task_handles: HashMap<Identity, JoinHandle<()>>,
}

impl<T, U> MergeTaskSet<T, U>
where
    T: SyncRpcConnector + Clone + 'static,
    U: SchemaEventHandler + Clone + 'static,
{
    pub(crate) fn new(connector: T, merge_delegate: U, mst: AntiEntropyHandle) -> Self {
        Self {
            connector,
            merge_delegate,
            mst,
            task_handles: HashMap::with_capacity(MAX_CONCURRENT_SYNC_TASKS),
        }
    }

    /// Spawn a new [`RpcWorker`] sync task, converging any differences between
    /// the local node and `peer_addr`.
    pub(crate) fn start_sync(
        &mut self,
        peer_addr: SocketAddr,
        peer_identity: Identity,
    ) -> Result<(), Error> {
        // Clear out any terminated sync tasks
        self.scrub_terminated();

        // Find an empty slot in the task set or return an error.
        //
        // If an existing sync task is active for `peer_identity`, do not allow
        // starting a new task until it has completed.
        let n_running = self.task_handles.len(); // Number of non-terminated tasks
        let entry = self.task_handles.entry(peer_identity.clone());
        match entry {
            Entry::Vacant(_) => {
                // Do not allow starting a task if the limit has been reached.
                if n_running >= MAX_CONCURRENT_SYNC_TASKS {
                    return Err(Error::MaxConcurrentSyncs);
                }
            }
            Entry::Occupied(ref v) if v.get().is_finished() => {
                // The task has completed - re-use this slot.
            }
            Entry::Occupied(_) => {
                // Prevent multiple syncs to the same peer.
                return Err(Error::ConcurrentSync);
            }
        };

        // Spawn the task and place it into the free slot in the task set
        let handle = tokio::spawn(start_sync(
            self.connector.clone(),
            peer_addr,
            peer_identity,
            self.mst.clone(),
            self.merge_delegate.clone(),
        ));

        entry.insert(handle);

        Ok(())
    }

    /// Remove all terminated sync task handles from the task handle set.
    fn scrub_terminated(&mut self) {
        self.task_handles
            .extract_if(|_k, v| v.is_finished())
            .for_each(drop)
    }
}

/// Connect to the given `peer_addr` and being a sync round.
async fn start_sync<T, U>(
    connector: T,
    peer_addr: SocketAddr,
    peer_identity: Identity,
    mst: AntiEntropyHandle,
    merge_delegate: U,
) where
    T: SyncRpcConnector,
    U: SchemaEventHandler,
{
    let client = match connector.connect(peer_addr).await {
        Ok(v) => v,
        Err(error) => {
            warn!(
                %error,
                %peer_addr,
                %peer_identity,
                "failed to connect to sync rpc peer"
            );
            return;
        }
    };

    RpcWorker::new(client, merge_delegate, mst, peer_addr, peer_identity)
        .run()
        .await
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr},
        sync::Arc,
        time::Duration,
    };

    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use generated_types::influxdata::iox::gossip::v1::schema_message::Event;
    use test_helpers::timeout::FutureTimeout;
    use tokio::sync::mpsc;

    use crate::{
        gossip::anti_entropy::{
            mst::actor::AntiEntropyActor,
            sync::mock_rpc_client::{MockSyncRpcClient, MockSyncRpcConnector},
        },
        namespace_cache::MemoryNamespaceCache,
    };

    const PEER_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4242);

    use super::*;

    #[derive(Debug, Default, Clone)]
    struct MockSchemaEventHandler;

    #[async_trait]
    impl SchemaEventHandler for MockSchemaEventHandler {
        async fn handle(&self, _event: Event) {}
    }

    /// Assert that only one sync job per peer can be running at any time.
    #[tokio::test]
    async fn test_single_in_flight_peer_sync() {
        let ident =
            Identity::try_from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 2, 3, 4, 5, 6, 7]).unwrap();

        let schema_event_handler = MockSchemaEventHandler::default();

        // The MST state actor & handle.
        let (actor, mst) = AntiEntropyActor::new(MemoryNamespaceCache::default());
        tokio::spawn(actor.run());

        // Initialise a notification channel to block the mock RPC client.
        let (tx, mut rx) = mpsc::channel(1);

        // Pre-fill the mpsc channel so the next send blocks
        tx.send(
            crate::gossip::anti_entropy::sync::mock_rpc_client::MockCalls::FindInconsistentRanges(
                mst.snapshot().await,
            ),
        )
        .await
        .unwrap();

        // Configure the RPC client to block on the notification send before
        // returning "fully consistent".
        let rpc_client = Arc::new(
            MockSyncRpcClient::default()
                .with_find_inconsistent_ranges([Ok(vec![])])
                .with_notify(tx),
        );

        // And configure the mock connector to return it.
        let rpc_connector =
            Arc::new(MockSyncRpcConnector::default().with_client(Arc::clone(&rpc_client)));

        // Initialise the worker set.
        let mut task_set = MergeTaskSet::new(rpc_connector, schema_event_handler, mst);

        task_set
            .start_sync(PEER_ADDR, ident.clone())
            .expect("first spawned task should start");

        //
        // The worker will block trying to send into the tx notification
        // channel.
        //

        // A subsequent sync should fail.
        let err = task_set
            .start_sync(PEER_ADDR, ident.clone())
            .expect_err("second spawned task should fail");
        assert_matches!(err, Error::ConcurrentSync);

        // Unblock the first worker
        rx.recv().await.unwrap();

        // The first should complete eventually, at which point the slot should
        // be reclaimed and a new task should be allowed to start.
        async {
            loop {
                if task_set.start_sync(PEER_ADDR, ident.clone()).is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;
    }

    /// Assert that the maximum number of sync tasks does not exceed
    /// MAX_CONCURRENT_SYNC_TASKS, and that completed worker slots are reclaimed
    /// for disjoint peer identities to reuse.
    #[tokio::test]
    async fn test_respects_max_sync_tasks() {
        // Test invariant
        #[allow(clippy::assertions_on_constants)]
        {
            assert!(MAX_CONCURRENT_SYNC_TASKS < 256);
        }

        let schema_event_handler = MockSchemaEventHandler::default();

        // The MST state actor & handle.
        let (actor, mst) = AntiEntropyActor::new(MemoryNamespaceCache::default());
        tokio::spawn(actor.run());

        // Initialise a notification channel to block the mock RPC client.
        let (tx, mut rx) = mpsc::channel(1);

        // Pre-fill the mpsc channel so the next send blocks
        tx.send(
            crate::gossip::anti_entropy::sync::mock_rpc_client::MockCalls::FindInconsistentRanges(
                mst.snapshot().await,
            ),
        )
        .await
        .unwrap();

        // Configure the RPC client to block each worker on the (full) notify
        // channel, and configure it to return MAX_CONCURRENT_SYNC_TASKS + 1 OK responses.
        let rpc_client = Arc::new(
            MockSyncRpcClient::default()
                .with_find_inconsistent_ranges(
                    std::iter::repeat_with(|| Ok(vec![]))
                        .take(MAX_CONCURRENT_SYNC_TASKS + 1)
                        .collect::<Vec<_>>(),
                )
                .with_notify(tx),
        );

        // And configure the mock connector to return it.
        let rpc_connector =
            Arc::new(MockSyncRpcConnector::default().with_client(Arc::clone(&rpc_client)));

        // Initialise the worker set.
        let mut task_set = MergeTaskSet::new(rpc_connector, schema_event_handler, mst);

        // Start MAX_CONCURRENT_SYNC_TASKS number of tasks.
        for i in 0..MAX_CONCURRENT_SYNC_TASKS {
            // Give each worker a unique ident, simulating syncs to distinct
            // peers.
            let ident = Identity::try_from(vec![
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                0,
                2,
                3,
                4,
                5,
                6,
                u8::try_from(i).unwrap(),
            ])
            .unwrap();

            task_set
                .start_sync(PEER_ADDR, ident.clone())
                .expect("first spawned task should start");
        }

        // All the tasks will be stuck on the notification channel, and the next
        // task spawned will hit the max sync error.
        let ident =
            Identity::try_from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 2, 3, 4, 5, 6, 255]).unwrap();

        let err = task_set
            .start_sync(PEER_ADDR, ident.clone())
            .expect_err("next spawned task should fail");
        assert_matches!(err, Error::MaxConcurrentSyncs);

        // Consume one message from rx, unblocking a single worker.
        let _ = rx.recv().await.unwrap();

        // Wait for the unblocked worker to complete, which in turn should allow
        // a new spawn attempt to reclaim the now-complete worker slot, enabling
        // the new worker to be started.

        async {
            loop {
                if task_set.start_sync(PEER_ADDR, ident.clone()).is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;
    }
}
