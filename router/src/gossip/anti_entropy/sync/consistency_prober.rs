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
    /// Initialise a new [`ProbeDispatcher`] that enqueues dispatched probes
    /// into the returned channel.
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

#[cfg(test)]
mod tests {

    use std::{sync::Arc, time::Duration};

    use gossip::{Builder, NopDispatcher};
    use merkle_search_tree::MerkleSearchTree;
    use test_helpers::timeout::FutureTimeout;
    use tokio::net::UdpSocket;

    use super::*;

    /// Bind a UDP socket on a random port and return it alongside the socket
    /// address.
    async fn random_udp() -> (UdpSocket, SocketAddr) {
        // Bind a UDP socket to a random port
        let socket = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("failed to bind UDP socket");
        let addr = socket.local_addr().expect("failed to read local addr");

        (socket, addr)
    }

    #[tokio::test]
    async fn test_gossip_consistency_probes() {
        let metrics = Arc::new(metric::Registry::default());

        let (a_socket, a_addr) = random_udp().await;
        let (b_socket, b_addr) = random_udp().await;

        // Initialise the dispatchers for the reactors
        let (dispatcher, rx) = ProbeDispatcher::new();

        // Initialise both reactors
        let addrs = vec![a_addr.to_string(), b_addr.to_string()];
        let a = Builder::<_, Topic>::new(addrs.clone(), NopDispatcher, Arc::clone(&metrics))
            .build(a_socket);
        let b = Arc::new(Builder::new(addrs, dispatcher, Arc::clone(&metrics)).build(b_socket));

        let (resolver, mut rx) = SenderAddressResolver::new(Arc::clone(&b), rx);
        tokio::spawn(resolver.run());

        // Wait for peer discovery to occur
        async {
            loop {
                if a.get_peers().await.len() == 1 {
                    break;
                }
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        let mut mst = MerkleSearchTree::<&[u8], &[u8]>::default();
        let h = mst.root_hash();

        // Send the probe
        a.probe(h.clone(), 4242).await;

        // Wait for it to be received
        let (probe, sender, addr) = rx.recv().await.expect("should wait");

        assert_eq!(sender, a.identity());
        assert_eq!(addr, a_addr);
        assert_eq!(probe.root_hash, h.as_bytes());
        assert_eq!(probe.grpc_port, 4242);
    }
}
