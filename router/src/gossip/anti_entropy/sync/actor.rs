//! [`NamespaceCache`] anti-entropy background worker.
//!
//! [`NamespaceCache`]: crate::namespace_cache::NamespaceCache

use std::{fmt::Debug, net::SocketAddr, time::Duration};

use generated_types::influxdata::iox::gossip::v1::ConsistencyProbe;
use gossip::Identity;
use gossip_schema::dispatcher::SchemaEventHandler;
use observability_deps::tracing::{debug, warn};
use tokio::sync::mpsc;

use crate::gossip::anti_entropy::mst::handle::AntiEntropyHandle;

use super::{
    consistency_prober::ConsistencyProber, rpc_worker::RpcWorker, traits::SyncRpcConnector,
};

/// How often to perform a consistency check with random peers.
const SYNC_ROUND_INTERVAL: Duration = Duration::from_secs(5);

/// The [`ConvergenceActor`] is responsible for driving peer anti-entropy to
/// achieve eventual consistency of the [`NamespaceCache`] content across peers.
///
/// To converge peers within the cluster, periodic "sync rounds" are performed
/// ([`SYNC_ROUND_INTERVAL`]), using the [`MerkleSearchTree`] state managed by
/// the [`AntiEntropyActor`].
///
///
/// # Sync Round Sequence
///
/// The local node begins a sync round by sending a consistency probe to a
/// random peer (or in this implementation, multiple random peers) over gossip.
///
/// This probe includes the Merkle Search Tree root hash that accurately and
/// compactly describes the content of the local node's [`NamespaceCache`].
///
/// This initial consistency probe is shown as request 1 in the diagram below:
///
/// ```text
///                       ┌─────┐                   ┌────┐
///                       │Local│                   │Peer│
///                       └──┬──┘                   └─┬──┘
///                          │ [1] Send content hash  │
///                          │────────────────────────>
///                          │                        │
///                          │                 ┌───────────────┐
///                          │                 │Compute hash,  │
///                          │                 │stop if equal  │
///                          │                 └───────────────┘
///                          │                        │
///                          │   ╔════════════════╗   │
///   ═══════════════════════╪═══╣ Switch to gRPC ╠═══╪═══════════════════════
///                          │   ╚════════════════╝   │
///                          │                        │
///                          │[2] Serialised MST pages│
///                          │<────────────────────────
///                          │                        │
///                   ┌──────────────┐                │
///                   │Perform diff  │                │
///                   └──────────────┘                │
///                          │ [3] Inconsistent pages │
///                          │────────────────────────>
///                          │                        │
///                          │                        │
///             ╔═══════╤════╪════════════════════════╪════════════╗
///             ║ LOOP  │  For each inconsistent page │            ║
///             ╟───────┘    │                        │            ║
///             ║            │      [4] Schemas       │            ║
///             ║            │<────────────────────────            ║
///             ╚════════════╪════════════════════════╪════════════╝
///                       ┌──┴──┐                   ┌─┴──┐
///                       │Local│                   │Peer│
///                       └─────┘                   └────┘
/// ```
///
/// Upon receipt of a consistency probe, the peer computes its own root hash,
/// and if they are equal, both nodes are consistent - no further action is
/// necessary, and the sync round ends.
///
/// If the root hashes are not equal, the protocol switches to gRPC over TCP (a
/// reliable transport with guaranteed delivery) for further requests, and the
/// peer sends an RPC request describing the peer's [`MerkleSearchTree`] in a
/// form suitable for identification of inconsistent key ranges (the set of
/// [`PageRange`] - request 2).
///
/// The local node computes the tree difference, and responds with a set of key
/// ranges that contain inconsistencies (request 3). The peer then fetches these
/// key ranges, merging the returned schemas into its [`NamespaceCache`] to
/// converge the content.
///
///
/// # Service Discovery
///
/// To switch to gRPC / TCP, the receiver of a consistency probe must know how
/// to connect to the sender's RPC service.
///
/// IOx is expected to run in environments without stable endpoints to address
/// each router across executions - put simply, routers come and go - and
/// frequently!
///
/// To ensure the receiver can connect and complete the sync round without any
/// external dependencies or config, the sender of a consistency probe includes
/// the gRPC port the service is bound to. This port is used along with the
/// sender's IP from the UDP packet header of the consistency probe to derive
/// the gRPC service address.
///
/// This scheme relies on the gRPC bind port matching the externally visible
/// gRPC port (no remapping / NAT) and for the gRPC service to be reachable on
/// the same interface as what is used for the gossip transport.
///
/// If a derived RPC address can't be connected to, a warning is logged -
/// constant connection warnings might indicate a problem as described above.
///
///
/// [`AntiEntropyActor`]:
///     crate::gossip::anti_entropy::mst::actor::AntiEntropyActor
/// [`PageRange`]: merkle_search_tree::diff::PageRange
/// [`MerkleSearchTree`]: merkle_search_tree::MerkleSearchTree
/// [`NamespaceCache`]: crate::namespace_cache::NamespaceCache
#[derive(Debug)]
pub struct ConvergenceActor<T, U, C> {
    /// An abstract connector returning a [`SyncRpcClient`] instance for a given
    /// peer RPC address.
    ///
    /// [`SyncRpcClient`]: super::traits::SyncRpcClient
    connector: T,

    /// An abstract [`SchemaEventHandler`] responsible for processing the
    /// incoming stream of gossip events, merging their payloads into the local
    /// [`NamespaceCache`].
    ///
    /// [`NamespaceCache`]: crate::namespace_cache::NamespaceCache
    schema_event_handler: U,

    /// A handle to the Merkle Search Tree state actor.
    mst: AntiEntropyHandle,

    /// The bind port used by the local node.
    ///
    /// This value is sent to peers in the initial consistency probe request to
    /// enable service discovery.
    grpc_bind_port: u16,

    /// A [`ConsistencyProber`] for sending consistency probe requests to peers.
    prober: C,

    /// An incoming stream of consistency probes from other peers, containing
    /// the sender's [`SocketAddr`].
    probe_rx: mpsc::Receiver<(ConsistencyProbe, Identity, SocketAddr)>,
}

impl<T, U, C> ConvergenceActor<T, U, C>
where
    T: SyncRpcConnector + Clone + 'static,
    U: SchemaEventHandler + Clone + 'static,
    C: ConsistencyProber,
{
    /// Initialise a new [`ConvergenceActor`].
    ///
    /// The actor does not start until [`ConvergenceActor::run()`] is called.
    pub fn new(
        connector: T,
        schema_event_handler: U,
        mst: AntiEntropyHandle,
        grpc_bind_port: u16,
        prober: C,
        probe_rx: mpsc::Receiver<(ConsistencyProbe, Identity, SocketAddr)>,
    ) -> Self {
        Self {
            connector,
            schema_event_handler,
            mst,
            grpc_bind_port,
            prober,
            probe_rx,
        }
    }

    /// Block and run the convergence actor.
    ///
    /// The actor immediately begins probing random peers.
    pub async fn run(mut self) {
        let mut ticker = tokio::time::interval(SYNC_ROUND_INTERVAL);

        // Wait at least SYNC_ROUND_INTERVAL between consistency checks.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = ticker.tick() =>
                    self.solicit_consistency_check().await,

                Some((v, ident, peer_addr)) = self.probe_rx.recv() =>
                    self.perform_consistency_check(v, ident, peer_addr).await,
            }
        }
    }

    /// Generate a root hash and exchange it with some random peers
    ///
    /// (consistency probe, request 1).
    async fn solicit_consistency_check(&mut self) {
        let root_hash = self.mst.content_hash().await;
        self.prober.probe(root_hash, self.grpc_bind_port).await;
    }

    /// Handle a received consistency probe.
    ///
    /// If the local node's MST root hash is equal to the peer root hash, this
    /// is a no-op.
    ///
    /// If the hashes differ, this function switchs the exchange to gRPC/TCP,
    /// and sends a serialised representation of the MST to the sender (request
    /// 2).
    async fn perform_consistency_check(
        &mut self,
        msg: ConsistencyProbe,
        peer_identity: Identity,
        peer_addr: SocketAddr,
    ) {
        // Request the current root hash from the MST
        let root_hash = self.mst.content_hash().await;

        // If the root hashes are equal, the content of the caches are equal.
        if root_hash.as_bytes() == msg.root_hash.as_slice() {
            debug!(
                %root_hash,
                %peer_addr,
                %peer_identity,
                "detected consistent peer"
            );
            return;
        }

        warn!(
            %peer_identity,
            %peer_addr,
            local_root_hash=%root_hash,
            local_root_hash_bytes=?root_hash.as_bytes(),
            peer_root_hash_bytes=?msg.root_hash,
            "peer inconsistency detected - converging"
        );

        // Construct the RPC address for the sender.
        let rpc_addr = SocketAddr::new(peer_addr.ip(), msg.grpc_port as u16);
        debug!(%peer_identity, %peer_addr, ?rpc_addr, "derived peer rpc address");

        // Spawn a task the drive the cache synchronisation.
        tokio::spawn(start_sync(
            self.connector.clone(),
            rpc_addr,
            peer_identity,
            self.mst.clone(),
            self.schema_event_handler.clone(),
        ));
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
    };

    use assert_matches::assert_matches;
    use test_helpers::timeout::FutureTimeout;

    use crate::{
        gossip::{
            anti_entropy::{
                mst::actor::AntiEntropyActor,
                sync::{
                    mock_consistency_prober::MockConsistencyProber,
                    mock_rpc_client::{MockSyncRpcClient, MockSyncRpcConnector},
                },
            },
            namespace_cache::NamespaceSchemaGossip,
        },
        namespace_cache::MemoryNamespaceCache,
    };

    use super::*;

    const RPC_PORT: u16 = 42;

    /// Assert a consistency probe is sent, and advertises the correct state.
    #[tokio::test]
    async fn test_send_consistency_probe() {
        // Schema cache being synchronised, and the gossip event handler that
        // merges events into the cache.
        let schema_cache = Arc::new(MemoryNamespaceCache::default());
        let schema_event_handler = Arc::new(NamespaceSchemaGossip::new(Arc::clone(&schema_cache)));

        // The MST state actor & handle.
        let (actor, mst) = AntiEntropyActor::new(Arc::clone(&schema_cache));
        tokio::spawn(actor.run());

        // A consistency prober recording probe attempts.
        let consistency_prober = Arc::new(MockConsistencyProber::default());

        // A stream of incoming probes.
        let (_probe_tx, probe_rx) = mpsc::channel(10);

        // Spawn the convergence actor and run it.
        tokio::spawn(
            ConvergenceActor::new(
                Arc::new(MockSyncRpcConnector::default()),
                schema_event_handler,
                mst.clone(),
                RPC_PORT,
                Arc::clone(&consistency_prober),
                probe_rx,
            )
            .run(),
        );

        // Wait for a probe to happen.
        let mut calls = async {
            loop {
                let calls = consistency_prober.calls();
                if !calls.is_empty() {
                    return calls;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        // It's technically possible that multiple probes could have been sent
        // before the loop was scheduled and noticed, so only inspect the first.
        let (root, port) = calls.swap_remove(0);

        // Given the absence of MST updates since sending the probe, the
        // advertised root hash must match the MST root hash.
        assert_eq!(root, mst.content_hash().await);

        // The advertised service port must match the configured value.
        assert_eq!(port, RPC_PORT);
    }

    /// Assert that upon receipt of a consistent probe containing an
    /// inconsistent root hash, some resolution mechanism is started such that
    /// an RPC is made to discover inconsistencies.
    ///
    /// This is effectively asserting the RpcWorker is started and provided a
    /// correctly derived RPC address. Behaviours of the worker are tested
    /// independently.
    #[tokio::test]
    async fn test_inconsistent_spawns_sync_worker() {
        const INCONSISTENT_HASH: &[u8; 16] = b"bananas_bananas!";

        // Schema cache being synchronised, and the gossip event handler that
        // merges events into the cache.
        let schema_cache = Arc::new(MemoryNamespaceCache::default());
        let schema_event_handler = Arc::new(NamespaceSchemaGossip::new(Arc::clone(&schema_cache)));

        // The MST state actor & handle.
        let (actor, mst) = AntiEntropyActor::new(Arc::clone(&schema_cache));
        tokio::spawn(actor.run());

        // A consistency prober recording probe attempts.
        let consistency_prober = Arc::new(MockConsistencyProber::default());

        // A stream of incoming probes.
        let (probe_tx, probe_rx) = mpsc::channel(10);

        // Configure the RPC client to reply that the peers are fully
        // consistent.
        //
        // This should stop the rpc worker, allowing the test to assert it
        // started.
        let rpc_client =
            Arc::new(MockSyncRpcClient::default().with_find_inconsistent_ranges([Ok(vec![])]));

        // And configure the mock connector to return it.
        let rpc_connector =
            Arc::new(MockSyncRpcConnector::default().with_client(Arc::clone(&rpc_client)));

        // Spawn the convergence actor and run it.
        tokio::spawn(
            ConvergenceActor::new(
                Arc::clone(&rpc_connector),
                schema_event_handler,
                mst.clone(),
                RPC_PORT,
                Arc::clone(&consistency_prober),
                probe_rx,
            )
            .run(),
        );

        // Send the consistency probe indicating the peers are inconsistent.
        //
        // This starts the RPC worker, which then discovers the peers are
        // actually consistent (due to concurrent updates) and stops.
        let identity = Identity::try_from(b"1234567890123456".to_vec()).unwrap();
        let sender_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8042);
        let probe = ConsistencyProbe {
            root_hash: INCONSISTENT_HASH.to_vec(),
            grpc_port: RPC_PORT as _,
        };

        probe_tx
            .send((probe, identity.clone(), sender_addr))
            .await
            .expect("probe send must succeed");

        // Wait for a RPC call to be made.
        async {
            loop {
                let calls = rpc_client.calls();
                if !calls.is_empty() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        // Inspect the connection parameters.
        assert_matches!(rpc_connector.calls().as_slice(), [addr] => {
            assert_eq!(*addr, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), RPC_PORT));
        });
    }
}
