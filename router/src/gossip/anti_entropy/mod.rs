//! Anti-entropy primitives providing eventual consistency over gossip.
//!
//! [`NamespaceCache`] anti-entropy between gossip peers is driven by the
//! following components:
//!
//! ```text
//!                        ┌───────────────┐
//!                ┌──────▶│   MST Actor   │◀──┐
//!                │       └───────────────┘   │
//!             Schema                         │
//!            Updates                    MST Hashes
//!                │                           │
//!                │                           │
//!    ┌ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─                 │
//!         NamespaceCache    │                ▼
//!    │           ▼                   ┌───────────────┐
//!        ┌──────────────┐   │        │  Convergence  │
//!    │   │ MST Observer │◀───────────│     Actor     │◀────────┐
//!        └──────────────┘   │        └───────────────┘         │
//!    │                                       ▲                 │
//!     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘                │                 │
//!                                       Consistency        Diffs &
//!                                         Probes           Schemas
//!                                            │                 │
//!                                            ▼                 ▼
//!                                    ┌───────────────┐ ┌───────────────┐
//!                                    │    Gossip     │ │   Sync RPC    │
//!                                    └───────────────┘ └───────────────┘
//! ```
//!
//! From left to right:
//!
//!   * [`MerkleTree`]: a [`NamespaceCache`] decorator observing any changes
//!         made to the local [`NamespaceCache`], providing diffs to the local
//!         node's [`MerkleSearchTree`].
//!
//!   * [`AntiEntropyActor`]: an actor task maintaining the local node's
//!         [`MerkleSearchTree`] state to accurately reflect the
//!         [`NamespaceCache`] content.
//!
//!   * [`ConvergenceActor`]: an actor task responsible for performing
//!         consistency checks with cluster peers, and driving convergence when
//!         inconsistencies are detected.
//!
//!   * [`ConsistencyProber`]: an abstract mechanism for exchanging MST
//!         consistency proofs / root hashes. Typically using gossip messages.
//!
//!   * [`RpcWorker`]: a reconciliation worker, spawned by the
//!         [`ConvergenceActor`] to perform differential convergence between the
//!         local node and an inconsistent peer. Makes RPC calls to perform MST
//!         diffs and fetch inconsistent schemas.
//!
//! [`NamespaceCache`]: crate::namespace_cache::NamespaceCache
//! [`MerkleTree`]: mst::merkle::MerkleTree
//! [`AntiEntropyActor`]: mst::actor::AntiEntropyActor
//! [`MerkleSearchTree`]: merkle_search_tree::MerkleSearchTree
//! [`ConvergenceActor`]: sync::actor::ConvergenceActor
//! [`ConsistencyProber`]: sync::consistency_prober::ConsistencyProber
//! [`RpcWorker`]: sync::rpc_worker::RpcWorker

pub mod mst;
pub mod sync;
