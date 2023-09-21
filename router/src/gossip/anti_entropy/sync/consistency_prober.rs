//! An abstract transport used to send Merkle Search Tree consistency probes.

use std::{net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use generated_types::{
    influxdata::iox::gossip::{v1::ConsistencyProbe, Topic},
    prost::Message,
};
use gossip::{GossipHandle, Identity};
use merkle_search_tree::digest::RootHash;
use observability_deps::tracing::{debug, warn};
use tokio::sync::mpsc;

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

/// A [`gossip::Dispatcher`] enqueuing incoming [`ConsistencyProbe`] messages
/// into a channel.
///
/// Messages are dropped when the probe queue is full.
#[derive(Debug)]
pub struct ProbeDispatcher {
    tx: mpsc::Sender<(ConsistencyProbe, Identity)>,
}

impl ProbeDispatcher {
    /// Initialise a new [`ProbeDispatcher`]and enqueuing the probe into the
    /// returned channel.
    pub fn new() -> (Self, mpsc::Receiver<(ConsistencyProbe, Identity)>) {
        let (tx, rx) = mpsc::channel(5);
        (Self { tx }, rx)
    }
}

#[async_trait]
impl gossip::Dispatcher<Topic> for ProbeDispatcher {
    async fn dispatch(&self, topic: Topic, payload: Bytes, sender: Identity) {
        if topic != Topic::SchemaCacheConsistency {
            return;
        }

        // Don't bother resolving the peer address and deserialising the payload
        // if the probe queue is full.
        let slot = match self.tx.try_reserve() {
            Ok(v) => v,
            Err(e) => {
                warn!(error=%e, "incoming consistency probe dropped");
                return;
            }
        };

        let payload = match ConsistencyProbe::decode(payload) {
            Ok(v) => v,
            Err(e) => {
                warn!(error=%e, "invalid consistency probe payload");
                return;
            }
        };

        slot.send((payload, sender));
    }
}

/// A resolver of peer addresses given a stream of [`ConsistencyProbe`]
/// messages, before forwarding through another channel.
///
/// The sender's address is resolved from the peer list before enqueuing into
/// the provided probe queue.
#[derive(Debug)]
pub struct SenderAddressResolver {
    handle: Arc<GossipHandle<Topic>>,
    rx: mpsc::Receiver<(ConsistencyProbe, Identity)>,
    tx: mpsc::Sender<(ConsistencyProbe, Identity, SocketAddr)>,
}

impl SenderAddressResolver {
    /// Construct a new [`SenderAddressResolver`] using `handle` to resolve the
    /// address of senders of [`ConsistencyProbe`] messages in `incoming`.
    pub fn new(
        handle: Arc<GossipHandle<Topic>>,
        incoming: mpsc::Receiver<(ConsistencyProbe, Identity)>,
    ) -> (
        Self,
        mpsc::Receiver<(ConsistencyProbe, Identity, SocketAddr)>,
    ) {
        let (tx, rx) = mpsc::channel(5);
        (
            Self {
                handle,
                rx: incoming,
                tx,
            },
            rx,
        )
    }

    /// Run the resolver task and block until the incoming channel is closed.
    pub async fn run(mut self) {
        while let Some((probe, sender)) = self.rx.recv().await {
            match self.handle.get_peer_addr(sender.clone()).await {
                Some(v) => {
                    let _ = self.tx.send((probe, sender, v)).await;
                }
                None => {
                    // This can happen if the sender was removed from the peer list
                    // after the local node received the message, but before this
                    // dispatcher attempted to resolve the peer address.
                    debug!("peer address not in peer list");
                }
            };
        }
    }
}
