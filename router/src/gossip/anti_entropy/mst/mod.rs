//! Decoupled, concurrency safe management of a [`MerkleSearchTree`].
//!
//! [`MerkleSearchTree`]: merkle_search_tree::MerkleSearchTree

pub mod actor;
pub mod handle;
pub mod merkle;

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use crate::{
        gossip::anti_entropy::mst::{actor::AntiEntropyActor, merkle::MerkleTree},
        namespace_cache::{MemoryNamespaceCache, NamespaceCache},
    };

    use data_types::{
        ColumnId, ColumnSchema, ColumnType, ColumnsByName, MaxColumnsPerTable, MaxTables,
        NamespaceId, NamespaceName, NamespaceSchema, TableId, TableSchema,
    };
    use proptest::prelude::*;

    use super::handle::AntiEntropyHandle;

    /// A set of table and column names from which arbitrary names are selected
    /// in prop tests, instead of using random values that have a low
    /// probability of overlap.
    const TEST_TABLE_NAME_SET: &[&str] = &[
        "bananas", "quiero", "un", "platano", "donkey", "goose", "egg", "mr_toro",
    ];

    prop_compose! {
        /// Generate a series of ColumnSchema assigned randomised IDs with a
        /// stable mapping of `id -> data type`.
        ///
        /// This generates at most 255 unique columns.
        pub fn arbitrary_column_schema_stable()(id in 0_i16..255) -> ColumnSchema {
            // Provide a stable mapping of ID to data type to avoid column type
            // conflicts by reducing the ID to the data type discriminant range
            // and using that to assign the data type.
            let col_type = ColumnType::try_from((id % 7) + 1).expect("valid discriminator range");

            ColumnSchema { id: ColumnId::new(id as _), column_type: col_type }
        }
    }

    prop_compose! {
        /// Generate an arbitrary TableSchema with up to 255 columns that
        /// contain stable `column name -> data type` and `column name -> column
        /// id` mappings.
        pub fn arbitrary_table_schema()(
            id in any::<i64>(),
            columns in proptest::collection::hash_set(
                arbitrary_column_schema_stable(),
                (0, 255) // Set size range
            ),
        ) -> TableSchema {
            // Map the column schemas into `name -> schema`, generating a
            // column name derived from the column ID to ensure a consistent
            // mapping of name -> id, and in turn, name -> data type.
            let columns = columns.into_iter()
                .map(|v| (format!("col-{}", v.id.get()), v))
                .collect::<BTreeMap<String, ColumnSchema>>();

            let columns = ColumnsByName::from(columns);
            TableSchema {
                id: TableId::new(id),
                partition_template: Default::default(),
                columns,
            }
        }
    }

    prop_compose! {
        /// Generate an arbitrary NamespaceSchema that contains tables from
        /// [`TEST_TABLE_NAME_SET`], containing up to 255 columns with stable
        /// `name -> (id, data type)` mappings.
        ///
        /// Namespace IDs are allocated from the specified strategy.
        pub fn arbitrary_namespace_schema(namespace_ids: impl Strategy<Value = i64>)(
            namespace_id in namespace_ids,
            tables in proptest::collection::btree_map(
                proptest::sample::select(TEST_TABLE_NAME_SET),
                arbitrary_table_schema(),
                (0, 10) // Set size range
            ),
            max_tables in any::<usize>(),
            max_columns_per_table in any::<usize>(),
            retention_period_ns in any::<Option<i64>>(),
        ) -> NamespaceSchema {
            let tables = tables.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
            NamespaceSchema {
                id: NamespaceId::new(namespace_id),
                tables,
                max_tables: MaxTables::new(max_tables as i32),
                max_columns_per_table: MaxColumnsPerTable::new(max_columns_per_table as i32),
                retention_period_ns,
                partition_template: Default::default(),
            }
        }
    }

    fn name_for_schema(schema: &NamespaceSchema) -> NamespaceName<'static> {
        NamespaceName::try_from(format!("ns-{}", schema.id)).unwrap()
    }

    const N_NAMESPACES: usize = 10;

    proptest! {
        /// Assert that two distinct namespace cache instances return identical
        /// content hashes and snapshots after applying a given set of cache
        /// updates.
        #[test]
        fn prop_content_hash_diverge_converge(
            // A variable number of cache entry updates for 2 namespace IDs
            updates in prop::collection::vec(arbitrary_namespace_schema(
                prop_oneof![Just(1), Just(2)]), // IDs assigned
                0..10 // Number of updates
            ),
            // An arbitrary namespace with an ID that lies outside of `updates`.
            last_update in arbitrary_namespace_schema(42_i64..100),
        ) {
            tokio::runtime::Runtime::new().unwrap().block_on(async move {
                let cache_a = Arc::new(MemoryNamespaceCache::default());
                let cache_b = Arc::new(MemoryNamespaceCache::default());

                let (actor_a, handle_a) = AntiEntropyActor::new(Arc::clone(&cache_a));
                let (actor_b, handle_b) = AntiEntropyActor::new(Arc::clone(&cache_b));

                // Start the MST actors
                tokio::spawn(actor_a.run());
                tokio::spawn(actor_b.run());

                let ns_a = MerkleTree::new(cache_a, handle_a.clone());
                let ns_b = MerkleTree::new(cache_b, handle_b.clone());

                // Invariant: two empty namespace caches have the same content hash.
                assert_eq!(handle_a.content_hash().await, handle_b.content_hash().await);
                // Invariant: and the same serialised snapshot content
                assert_eq!(handle_a.snapshot().await, handle_b.snapshot().await);

                for update in updates {
                    // Generate a unique, deterministic name for this namespace.
                    let name = name_for_schema(&update);

                    // Apply the update (which may be a no-op) to both.
                    ns_a.put_schema(name.clone(), update.clone());
                    ns_b.put_schema(name, update);

                    // Invariant: after applying the same update, the content hashes
                    // MUST match (even if this update was a no-op / not an update)
                    assert_eq!(handle_a.content_hash().await, handle_b.content_hash().await);
                    // Invariant: and the same serialised snapshot content
                    assert_eq!(handle_a.snapshot().await, handle_b.snapshot().await);
                }

                // At this point all updates have been applied to both caches.
                //
                // Add a new cache entry that doesn't yet exist, and assert this
                // causes the caches to diverge, and then once again reconverge.
                let name = name_for_schema(&last_update);
                ns_a.put_schema(name.clone(), last_update.clone());

                // Invariant: last_update definitely added new cache content,
                // therefore the cache content hashes MUST diverge.
                assert_ne!(handle_a.content_hash().await, handle_b.content_hash().await);
                // Invariant: the serialised snapshot content must have diverged
                assert_ne!(handle_a.snapshot().await, handle_b.snapshot().await);

                // Invariant: applying the update to the other cache converges their
                // content hashes.
                ns_b.put_schema(name, last_update);
                assert_eq!(handle_a.content_hash().await, handle_b.content_hash().await);
                // Invariant: and the serialised snapshot content converges
                assert_eq!(handle_a.snapshot().await, handle_b.snapshot().await);
            });
        }

        /// Assert that two peers with arbitrary schema cache content can
        /// identify inconsistencies and effectively converge them using the MST
        /// subsystem.
        #[test]
        fn prop_cache_content_convergence(
            a in prop::collection::vec(arbitrary_namespace_schema(0..20_i64), // IDs assigned
                0..N_NAMESPACES // Number of updates
            ),
            b in prop::collection::vec(arbitrary_namespace_schema(0..20_i64), // IDs assigned
                0..N_NAMESPACES // Number of updates
            ),
        ){
            tokio::runtime::Runtime::new().unwrap().block_on(async move {
                let cache_a = Arc::new(MemoryNamespaceCache::default());
                let cache_b = Arc::new(MemoryNamespaceCache::default());

                let (actor_a, handle_a) = AntiEntropyActor::new(Arc::clone(&cache_a));
                let (actor_b, handle_b) = AntiEntropyActor::new(Arc::clone(&cache_b));

                // Start the MST actors
                tokio::spawn(actor_a.run());
                tokio::spawn(actor_b.run());

                let ns_a = MerkleTree::new(cache_a, handle_a.clone());
                let ns_b = MerkleTree::new(cache_b, handle_b.clone());

                for a in a {
                    ns_a.put_schema(name_for_schema(&a), a);
                }

                for b in b {
                    ns_b.put_schema(name_for_schema(&b), b);
                }

                // At this point the caches of A and B both contain a set of up
                // to N_NAMESPACES potentially overlapping namespaces each, and
                // may or may not be consistent with each other.
                //
                // After a number of sync rounds both caches MUST converge to
                // the same content (as asserted by the MST root hash, which MAY
                // cover a subset of the NamespaceSchema content).
                //
                // Further to correctness, this test forces the caches to
                // converge within a number of sync rounds to match the number
                // of namespaces, asserting an upper-bound on the convergence
                // overhead.

                for _ in 0..N_NAMESPACES {
                    // Perform a bi-directional sync round, with A pulling from
                    // B, and then B pulling from A.
                    sync_round(&ns_a, &ns_b, &handle_a, &handle_b).await;
                    sync_round(&ns_b, &ns_a, &handle_b, &handle_a).await;

                    // Check if the caches have converged.
                    if handle_a.content_hash().await == handle_b.content_hash().await {
                        break;
                    }
                }

                assert_eq!(handle_a.content_hash().await, handle_b.content_hash().await);
            });
        }
    }

    // Perform a one-way sync between two peer MST's & their caches.
    //
    // A sends a snapshot to B and pulls keys from B, inserting them into A's
    // cache.
    async fn sync_round<T>(
        cache_a: &T,
        cache_b: &T,
        mst_a: &AntiEntropyHandle,
        mst_b: &AntiEntropyHandle,
    ) where
        T: NamespaceCache,
    {
        // A generates a snapshot and sends it to B
        let snap = mst_a.snapshot().await;

        mst_b.snapshot().await;

        // A --snap--> B
        let diff = mst_b.compute_diff(snap).await;
        // A <--diff-- B

        for range in diff {
            // A --range-->B

            // A resolves the keys it needs and B sends the schemas
            let inconsistent_keys = mst_b.get_keys_in_range(range).await;

            for ns in inconsistent_keys {
                // A reads the keys from it's local cache
                let schema = cache_b
                    .get_schema(&ns)
                    .await
                    .expect("must return schema in MST");

                // A <--schema-- B
                cache_a.put_schema(ns, NamespaceSchema::clone(&schema));
            }
        }
    }
}
