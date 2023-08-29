//! An abstract transport used to send Merkle Search Tree consistency probes.

use std::sync::Arc;

use async_trait::async_trait;
use generated_types::{
    influxdata::iox::gossip::{v1::ConsistencyProbe, Topic},
    prost::Message,
};
use gossip::GossipHandle;
use merkle_search_tree::digest::RootHash;

/// An abstract mechanism of probing the consistency of cluster peers.
#[async_trait]
pub trait ConsistencyProber: Send + Sync + std::fmt::Debug {
    /// Perform a consistency probe against an arbitrary set of peers.
    ///
    /// Advertise the local node is listening on `rpc_bind` to provide the RPC
    /// sync service, and `local_root` as the MST root hash for the local node.
    async fn probe(&self, local_root: RootHash, rpc_bind: u16);
}

#[async_trait]
impl ConsistencyProber for GossipHandle<Topic> {
    async fn probe(&self, local_root: RootHash, rpc_bind: u16) {
        // Generate the solicitation payload.
        let payload = ConsistencyProbe {
            root_hash: local_root.as_bytes().to_vec(),
            grpc_port: rpc_bind as _,
        };

        // Broadcast the root hash to a random subset of cluster peers.
        self.broadcast_subset(payload.encode_to_vec(), Topic::SchemaCacheConsistency)
            .await
            .expect("gossip message size exceeds max");
    }
}

#[async_trait]
impl<T> ConsistencyProber for Arc<T>
where
    T: ConsistencyProber,
{
    async fn probe(&self, local_root: RootHash, rpc_bind: u16) {
        T::probe(self, local_root, rpc_bind).await
    }
}
