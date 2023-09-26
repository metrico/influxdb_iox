//! Convergence of [`NamespaceCache`] content across gossip peers.
//!
//! [`NamespaceCache`]: crate::namespace_cache::NamespaceCache

pub mod actor;
pub mod consistency_prober;
pub mod rpc_server;
pub(crate) mod rpc_worker;
pub mod traits;

#[cfg(test)]
pub mod mock_rpc_client;

#[cfg(test)]
pub mod mock_consistency_prober;

#[cfg(test)]
pub mod tests {
    use std::{
        collections::HashSet,
        fmt::Debug,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        ops::RangeInclusive,
        sync::Arc,
        time::Duration,
    };

    use async_trait::async_trait;
    use data_types::NamespaceName;
    use generated_types::influxdata::iox::gossip::v1::{
        self as proto, ConsistencyProbe, NamespaceSchemaEntry,
    };
    use gossip::Identity;
    use merkle_search_tree::digest::RootHash;
    use parking_lot::Mutex;
    use proptest::prelude::*;
    use test_helpers::timeout::FutureTimeout;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use crate::{
        gossip::{
            anti_entropy::{
                mst::{
                    actor::{AntiEntropyActor, MerkleSnapshot},
                    handle::AntiEntropyHandle,
                    merkle::MerkleTree,
                },
                prop_gen::{arbitrary_namespace_schema, deterministic_name_for_schema},
                sync::actor::ConvergenceActor,
            },
            namespace_cache::NamespaceSchemaGossip,
        },
        namespace_cache::{MemoryNamespaceCache, NamespaceCache},
    };

    use super::{
        consistency_prober::ConsistencyProber,
        rpc_server::AntiEntropyService,
        traits::{BoxedError, SyncRpcClient, SyncRpcConnector},
    };

    const N_NAMESPACES: usize = 5;
    const RPC_PORT: u16 = 1313;

    type ProbePayload = (ConsistencyProbe, Identity, SocketAddr);

    /// An adaptor connecting the "consistency prober" of one node, to the
    /// incoming stream of probes of another node.
    #[derive(Debug, Clone)]
    struct MockProbeAdaptor {
        tx: mpsc::Sender<ProbePayload>,
        identity: Identity,
        addr: SocketAddr,
    }

    #[async_trait]
    impl ConsistencyProber for MockProbeAdaptor {
        async fn probe(&self, local_root: RootHash, rpc_bind: u16) {
            let payload = ConsistencyProbe {
                root_hash: local_root.as_bytes().to_vec(),
                grpc_port: rpc_bind as _,
            };
            let _ = self
                .tx
                .send((payload, self.identity.clone(), self.addr))
                .await;
        }
    }

    /// An adaptor connecting the [`SyncRpcClient`] of one node to the
    /// [`AntiEntropyService`] of the other (mocking out the gRPC transport).
    #[derive(Debug)]
    struct MockConnector<T> {
        server: Mutex<Option<Arc<T>>>,
        last_addr: Mutex<Option<SocketAddr>>,
    }

    impl<T> Default for MockConnector<T> {
        fn default() -> Self {
            Self {
                server: Default::default(),
                last_addr: Default::default(),
            }
        }
    }

    impl<T> MockConnector<T>
    where
        T: Send + Sync,
    {
        fn set_server(&self, server: T) {
            *self.server.lock() = Some(Arc::new(server));
        }

        async fn get_server(&self) -> Arc<T> {
            loop {
                let v = self.server.lock().clone();
                match v {
                    Some(v) => return v,
                    None => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
        }

        fn get_addr(&self) -> Option<SocketAddr> {
            *self.last_addr.lock()
        }
    }

    #[async_trait]
    impl<T> SyncRpcConnector for Arc<MockConnector<T>>
    where
        T: proto::anti_entropy_service_server::AntiEntropyService + Debug,
    {
        type Client = Self;
        async fn connect(&self, addr: SocketAddr) -> Result<Self::Client, BoxedError> {
            let now = *self.last_addr.lock().get_or_insert(addr);
            assert_eq!(now, addr); // Addresses should not change

            Ok(Arc::clone(self))
        }
    }

    #[async_trait]
    impl<T> SyncRpcClient for Arc<MockConnector<T>>
    where
        T: proto::anti_entropy_service_server::AntiEntropyService + Debug,
    {
        async fn find_inconsistent_ranges(
            &mut self,
            pages: MerkleSnapshot,
        ) -> Result<Vec<RangeInclusive<NamespaceName<'static>>>, BoxedError> {
            let pages = pages
                .iter()
                .map(|v| proto::PageRange {
                    min: v.start().to_string(),
                    max: v.end().to_string(),
                    page_hash: v.into_hash().as_bytes().to_vec(),
                })
                .collect();

            let server = self.get_server().await;
            let resp = server
                .get_tree_diff(tonic::Request::new(proto::GetTreeDiffRequest { pages }))
                .await
                .expect("RPC call failed")
                .into_inner()
                .ranges
                .into_iter()
                .map(|v| RangeInclusive::new(v.min.try_into().unwrap(), v.max.try_into().unwrap()))
                .collect();

            Ok(resp)
        }

        async fn get_schemas_in_range(
            &mut self,
            range: RangeInclusive<NamespaceName<'static>>,
        ) -> Result<Vec<NamespaceSchemaEntry>, BoxedError> {
            let server = self.get_server().await;
            let resp = server
                .get_range(tonic::Request::new(proto::GetRangeRequest {
                    min: range.start().to_string(),
                    max: range.end().to_string(),
                }))
                .await
                .expect("RPC call failed")
                .into_inner()
                .namespaces;

            Ok(resp)
        }
    }

    /// Initialise a new mock "node" that acts as a gossip/convergence peer.
    fn new_node<C>(
        ip_addr: Ipv4Addr,
        probe_tx: mpsc::Sender<ProbePayload>, // The channel this node sends probes over
        probe_rx: mpsc::Receiver<ProbePayload>, // The incoming stream of probes for this node
        connector: C,
    ) -> (
        impl NamespaceCache,
        AntiEntropyHandle,
        AntiEntropyService<Arc<MerkleTree<Arc<MemoryNamespaceCache>>>>,
        MockProbeAdaptor,
    )
    where
        C: SyncRpcConnector + Clone + 'static,
    {
        let cache = Arc::new(MemoryNamespaceCache::default());

        // Initialise the merkle tree state actor
        let (mst_actor, mst) = AntiEntropyActor::new(Arc::clone(&cache));
        tokio::spawn(mst_actor.run());

        // Initialise the namespace cache decorator that feeds changes
        // into the merkel tree actor
        let cache = Arc::new(MerkleTree::new(cache, mst.clone()));

        // Initialise the gossip event handler, that applies gossip events to
        // the local cache.
        let gossip_apply = Arc::new(NamespaceSchemaGossip::new(Arc::clone(&cache)));

        // For the purposes of these tests, the consistency prober is a direct
        // channel between nodes instead of a gossip link.
        let prober = MockProbeAdaptor {
            tx: probe_tx,
            identity: Identity::try_from(Uuid::new_v4().as_bytes().to_vec()).unwrap(),
            addr: SocketAddr::new(IpAddr::V4(ip_addr), 4242),
        };

        // Initialise the convergence actor
        let conv_actor = ConvergenceActor::new(
            connector,
            gossip_apply,
            mst.clone(),
            RPC_PORT,
            prober.clone(),
            probe_rx,
        );
        tokio::spawn(conv_actor.run());

        let server = AntiEntropyService::new(mst.clone(), Arc::clone(&cache));

        (cache, mst, server, prober)
    }

    proptest! {
        /// Perform an integration test of the anti-entropy subsystem.
        ///
        /// This ensures:
        ///
        ///  * The cache content is accurately tracked in the MST
        ///
        ///  * Inconsistencies within the cache are identified via consistency
        ///    probes
        ///
        ///  * The receiver of the consistency probe derives the RPC address of the
        ///    sender and connects to the sync RPC API
        ///
        ///  * A convergence rpc worker task is started to drive convergence
        ///
        ///  * The caches are successfully converged to an identical state
        ///
        #[test]
        fn prop_peer_convergence(
            a in prop::collection::vec(
                arbitrary_namespace_schema(0..20_i64), // IDs assigned
                0..N_NAMESPACES // N cache entries
            ),
            b in prop::collection::vec(
                arbitrary_namespace_schema(0..20_i64), 0..N_NAMESPACES
            ),
        ){
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move {
                    let (probe_a_tx, probe_a_rx) = mpsc::channel(1);
                    let (probe_b_tx, probe_b_rx) = mpsc::channel(1);

                    let connector_a = Arc::new(MockConnector::default());
                    let connector_b = Arc::new(MockConnector::default());

                    let (cache_a, mst_a, server_a, prober_a) = new_node(
                        Ipv4Addr::new(1, 2, 3, 4),
                        probe_b_tx,
                        probe_a_rx,
                        Arc::clone(&connector_b),
                    );

                    let (cache_b, mst_b, server_b, prober_b) = new_node(
                        Ipv4Addr::new(4, 3, 2, 1),
                        probe_a_tx,
                        probe_b_rx,
                        Arc::clone(&connector_a),
                    );

                    connector_a.set_server(server_a);
                    connector_b.set_server(server_b);

                    // Populate the caches with the random set of schemas per node
                    // and remember the namespace names.
                    let mut names = HashSet::new();
                    for a in a {
                        names.insert(deterministic_name_for_schema(&a));
                        cache_a.put_schema(deterministic_name_for_schema(&a), a);
                    }

                    for b in b {
                        names.insert(deterministic_name_for_schema(&b));
                        cache_b.put_schema(deterministic_name_for_schema(&b), b);
                    }

                    let _ = mst_a.content_hash().await;
                    let _ = mst_b.content_hash().await;

                    // Cause loads of sync attempts, which blocks after 1 in the
                    // queue.
                    async {
                        loop {
                            prober_a.probe(mst_a.content_hash().await, RPC_PORT).await;
                            prober_b.probe(mst_b.content_hash().await, RPC_PORT).await;

                            // After each consistency probe is sent, see if a
                            // previous round converged the caches.
                            if mst_a.content_hash().await == mst_b.content_hash().await {
                                    break
                            }
                        }
                    }
                    .with_timeout_panic(Duration::from_secs(5))
                    .await;

                    // For every namespace placed into either of the caches, the
                    // table schemas must be equal.
                    for name in names {
                        let a = cache_a
                            .get_schema(&name)
                            .await
                            .expect("namespace must exist");
                        let b = cache_b
                            .get_schema(&name)
                            .await
                            .expect("namespace must exist");

                        assert_eq!(a.tables, b.tables);
                    }

                    // If a sync took place, the RPC address must have been
                    // correctly derived.
                    if let Some(addr) = connector_a.get_addr() {
                        assert_eq!(
                            addr,
                            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), RPC_PORT)
                        );
                    }
                    if let Some(addr) = connector_b.get_addr() {
                        assert_eq!(
                            addr,
                            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(4, 3, 2, 1)), RPC_PORT)
                        );
                    }
                }
            );
        }
    }
}
