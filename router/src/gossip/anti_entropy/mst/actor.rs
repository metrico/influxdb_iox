//! An actor task maintaining [`MerkleSearchTree`] state.
use std::{ops::RangeInclusive, sync::Arc};

use data_types::{NamespaceName, NamespaceSchema};
use merkle_search_tree::{diff::PageRangeSnapshot, digest::RootHash, MerkleSearchTree};
use observability_deps::tracing::{debug, info, trace};
use tokio::sync::{mpsc, oneshot};

use crate::namespace_cache::{CacheMissErr, NamespaceCache};

use super::handle::AntiEntropyHandle;

/// An alias for a [`PageRangeSnapshot`] for [`NamespaceName`] keys.
pub(crate) type MerkleSnapshot = PageRangeSnapshot<NamespaceName<'static>>;

/// Requests sent from an [`AntiEntropyHandle`] to an [`AntiEntropyActor`].
#[derive(Debug)]
pub(super) enum Op {
    /// Request the content / merkle tree root hash.
    ContentHash(oneshot::Sender<RootHash>),

    /// Request a [`MerkleSnapshot`] of the current MST state.
    Snapshot(oneshot::Sender<MerkleSnapshot>),

    /// Compute the diff between the local MST state and the provided snapshot,
    /// returning the key ranges that contain inconsistencies.
    Diff(
        MerkleSnapshot,
        oneshot::Sender<Vec<RangeInclusive<NamespaceName<'static>>>>,
    ),

    /// Return all MST keys within the specified inclusive range.
    KeysInRange(
        RangeInclusive<NamespaceName<'static>>,
        oneshot::Sender<Vec<NamespaceName<'static>>>,
    ),
}

impl Op {
    /// Returns `true` if this [`Op`] no longer needs to be processed.
    fn is_cancelled(&self) -> bool {
        match self {
            Op::ContentHash(tx) => tx.is_closed(),
            Op::Snapshot(tx) => tx.is_closed(),
            Op::Diff(_, tx) => tx.is_closed(),
            Op::KeysInRange(_, tx) => tx.is_closed(),
        }
    }
}

/// A [`NamespaceCache`] anti-entropy state tracking primitive.
///
/// This actor maintains a [`MerkleSearchTree`] covering the content of
/// [`NamespaceSchema`] provided to it.
///
/// [`NamespaceCache`]: crate::namespace_cache::NamespaceCache
#[derive(Debug)]
pub struct AntiEntropyActor<T> {
    cache: T,
    schema_rx: mpsc::Receiver<NamespaceName<'static>>,
    op_rx: mpsc::Receiver<Op>,

    mst: MerkleSearchTree<NamespaceName<'static>, NamespaceContentHash>,
}

impl<T> AntiEntropyActor<T>
where
    // A cache lookup in the underlying impl MUST be infallible - it's asking
    // for existing records, and the cache MUST always return them.
    T: NamespaceCache<ReadError = CacheMissErr>,
{
    /// Initialise a new [`AntiEntropyActor`], and return the
    /// [`AntiEntropyHandle`] used to interact with it.
    ///
    /// The provided cache is used to resolve the most up-to-date
    /// [`NamespaceSchema`] for given a [`NamespaceName`], which SHOULD be as
    /// fast as possible and MUST be infallible.
    pub fn new(cache: T) -> (Self, AntiEntropyHandle) {
        // Initialise the queue used to push schema updates.
        //
        // Filling this queue causes dropped updates to the MST, which in turn
        // cause spurious convergence rounds which are expensive relative to a
        // bit of worst-case RAM (network trips, CPU for hashing, gossip
        // traffic).
        //
        // Each queue entry has a 64 byte upper limit (excluding queue & pointer
        // overhead) because the namespace name has a 64 byte upper bound. This
        // means there's an upper (worst case) RAM utilisation bound of 1 MiB
        // for these queue values.
        let (schema_tx, schema_rx) = mpsc::channel(16_000);

        // A "command" channel for non-schema-update messages.
        let (op_tx, op_rx) = mpsc::channel(50);

        (
            Self {
                cache,
                schema_rx,
                op_rx,
                mst: MerkleSearchTree::default(),
            },
            AntiEntropyHandle::new(op_tx, schema_tx),
        )
    }

    /// Block and run the anti-entropy actor until all [`AntiEntropyHandle`]
    /// instances for it have been dropped.
    pub async fn run(mut self) {
        info!("starting anti-entropy state actor");

        loop {
            tokio::select! {
                // Bias towards processing schema updates.
                //
                // This presents the risk of starvation / high latency for
                // "operation" commands (sent over op_rx) under heavy update
                // load (which is unlikely) at the benefit of ensuring all
                // operations occur against an up-to-date MST.
                //
                // This helps avoid spurious convergence rounds by ensuring all
                // updates are always applied as soon as possible.
                biased;

                // Immediately apply the available MST update.
                Some(name) = self.schema_rx.recv() => self.handle_upsert(name).await,

                // Else process an  "operation" command from the actor handle.
                Some(op) = self.op_rx.recv() => self.handle_op(op),

                // And stop if both channels have closed.
                else => {
                    info!("stopping anti-entropy state actor");
                    return
                },
            }
        }
    }

    async fn handle_upsert(&mut self, name: NamespaceName<'static>) {
        let schema = match self.cache.get_schema(&name).await {
            Ok(v) => v,
            Err(CacheMissErr { .. }) => {
                // This is far from ideal as it causes the MST to skip applying
                // an update, effectively causing it to diverge from the actual
                // cache state.
                //
                // This will cause spurious convergence runs between peers that
                // have applied this update to their MST, in turn causing it to
                // converge locally.
                //
                // If no node applied this update, then this value will not be
                // converged between any peers until a subsequent update causes
                // a successful MST update on a peer.
                //
                // Instead the bounds require the only allowable error to be a
                // cache miss error (no I/O error or other problem) - this can't
                // ever happen, because state updates are enqueued for existing
                // schemas, so there MUST always be an entry returned.
                panic!("cache miss for namespace schema {}", name);
            }
        };

        trace!(%name, ?schema, "applying schema");

        self.mst.upsert(name, &NamespaceContentHash(schema));
    }

    fn handle_op(&mut self, op: Op) {
        // Optimisation: avoid doing work for a caller that gave up waiting for
        // a response before the op was read.
        //
        // This might happen if a backlog of operations was enqueued, and the
        // caller subsequently timed-out / disconnected.
        if op.is_cancelled() {
            return;
        }

        match op {
            Op::ContentHash(tx) => {
                // The caller may have stopped listening, so ignore any errors.
                let _ = tx.send(self.generate_root().clone());
            }
            Op::Snapshot(tx) => {
                // The root hash must be generated before serialising the page
                // ranges.
                self.generate_root();

                let snap = PageRangeSnapshot::from(
                    self.mst
                        .serialise_page_ranges()
                        .expect("root hash generated"),
                );

                let _ = tx.send(snap);
            }
            Op::Diff(snap, tx) => {
                self.generate_root();

                // Generate the local MST pages.
                //
                // In theory these pages could be cached and invalidated as
                // necessary, but in practice the MST itself performs lazy hash
                // invalidation when mutated, so generating the pages from an
                // unchanged MST is extremely fast (microseconds) making the
                // additional caching complexity unjustified.
                let local_pages = self
                    .mst
                    .serialise_page_ranges()
                    .expect("root hash generated");

                // Compute the inconsistent key ranges and map them to an owned
                // representation.
                //
                // This is also extremely fast (microseconds) so executing it on
                // an I/O runtime thread is unlikely to cause problems.
                let diff = merkle_search_tree::diff::diff(snap.iter(), local_pages)
                    .into_iter()
                    .map(|v| RangeInclusive::new(v.start().to_owned(), v.end().to_owned()))
                    .collect();

                let _ = tx.send(diff);
            }
            Op::KeysInRange(range, tx) => {
                // Copy all the NamespaceNames within the MST (covered by the
                // content hashes) in the specified range.
                let keys = self
                    .mst
                    .node_iter()
                    .skip_while(|v| v.key() < range.start())
                    .take_while(|v| v.key() <= range.end())
                    .map(|v| v.key().clone())
                    .collect();

                let _ = tx.send(keys);
            }
        }
    }

    /// Generate the MST root hash and log it for debugging purposes.
    fn generate_root(&mut self) -> &RootHash {
        let root_hash = self.mst.root_hash();
        debug!(%root_hash, "generated content hash");
        root_hash
    }
}

/// A [`NamespaceSchema`] decorator that produces a content hash covering fields
/// that SHOULD be converged across gossip peers.
#[derive(Debug)]
struct NamespaceContentHash(Arc<NamespaceSchema>);

impl std::hash::Hash for NamespaceContentHash {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Technically the ID does not need to be covered by the content hash
        // (the namespace name -> namespace ID is immutable and asserted
        // elsewhere) but it's not harmful to include it, and would drive
        // detection of a broken mapping invariant.
        self.0.id.hash(state);

        // The set of tables, and their schemas MUST form part of the content
        // hash as they are part of the content that must be converged.
        self.0.tables.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::hash_map::DefaultHasher, hash::Hasher};

    use crate::{
        gossip::anti_entropy::prop_gen::arbitrary_namespace_schema,
        namespace_cache::MemoryNamespaceCache,
    };

    use super::*;

    use data_types::{
        partition_template::test_table_partition_override, ColumnId, ColumnsByName, NamespaceId,
        TableId, TableSchema,
    };
    use proptest::prelude::*;

    proptest! {
        /// Assert the [`NamespaceContentHash`] decorator results in hashes that
        /// are equal iff the tables and namespace ID match.
        ///
        /// All other fields may vary without affecting the hash.
        #[test]
        fn prop_content_hash_coverage(
            a in arbitrary_namespace_schema(Just(1)),
            b in arbitrary_namespace_schema(Just(1))
        ) {
            assert_eq!(a.id, b.id); // Test invariant

            let a = Arc::new(a);
            let b = Arc::new(b);

            let wrap_a = NamespaceContentHash(Arc::clone(&a));
            let wrap_b = NamespaceContentHash(Arc::clone(&b));

            // Invariant: if the schemas are equal, the content hashes match
            if a == b {
                assert_eq!(hash(&wrap_a), hash(&wrap_b));
            }

            // True if the content hashes of a and b are equal.
            let is_hash_eq = hash(wrap_a) == hash(wrap_b);

            // Invariant: if the tables and ID match, the content hashes match
            assert_eq!(
                ((a.tables == b.tables) && (a.id == b.id)),
                is_hash_eq
            );

            // Invariant: the hashes chaange if the ID is modified
            let new_id = NamespaceId::new(a.id.get().wrapping_add(1));
            let hash_old_a = hash(&a);
            let mut a = NamespaceSchema::clone(&a);
            a.id = new_id;
            assert_ne!(hash_old_a, hash(a));
        }

        /// A fixture test that asserts the content hash of a given
        /// [`NamespaceSchema`] does not change.
        ///
        /// This uses randomised inputs for fields that do not form part of the
        /// content hash, proving they're not used, and fixes fields that do
        /// form the hash to assert a static value given static content hash
        /// inputs.
        #[test]
        fn prop_content_hash_fixture(
            mut ns in arbitrary_namespace_schema(Just(42))
        ) {
            ns.tables = [(
                "bananas".to_string(),
                TableSchema {
                    id: TableId::new(24),
                    columns: ColumnsByName::new([data_types::Column {
                        name: "platanos".to_string(),
                        column_type: data_types::ColumnType::String,
                        id: ColumnId::new(2442),
                        table_id: TableId::new(24),
                    }]),
                    partition_template: test_table_partition_override(vec![
                        data_types::partition_template::TemplatePart::TagValue("bananatastic"),
                    ]),
                },
            )]
            .into_iter()
            .collect();

            let wrap_ns = NamespaceContentHash(Arc::new(ns));

            // If this assert fails, the content hash for a given representation
            // has changed, and this will cause peers to consider each other
            // completely inconsistent regardless of actual content.
            //
            // You need to be careful about doing this!
            assert_eq!(hash(wrap_ns), 13889074233458619864);
        }
    }

    fn hash(v: impl std::hash::Hash) -> u64 {
        let mut hasher = DefaultHasher::default();
        v.hash(&mut hasher);
        hasher.finish()
    }

    #[tokio::test]
    async fn test_empty_content_hash_fixture() {
        let (actor, handle) = AntiEntropyActor::new(MemoryNamespaceCache::default());
        tokio::spawn(actor.run());

        let got = handle.content_hash().await;
        assert_eq!(got.to_string(), "UEnXR4Cj4H1CAqtH1M7y9A==");
    }
}
