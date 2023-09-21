//! Maintain a [Merkle Search Tree] covering the content of a
//! [`NamespaceCache`].
//!
//! [Merkle Search Tree]: https://inria.hal.science/hal-02303490

use std::sync::Arc;

use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};

use crate::namespace_cache::{ChangeStats, NamespaceCache};

use super::handle::AntiEntropyHandle;

/// A [`NamespaceCache`] decorator that maintains a content hash / consistency
/// proof.
///
/// This [`MerkleTree`] tracks the content of the underlying [`NamespaceCache`]
/// delegate, maintaining a compact, serialisable representation in a
/// [`MerkleSearchTree`] that can be used to perform efficient differential
/// convergence (anti-entropy) of peers to provide eventual consistency.
///
/// # Merge Correctness
///
/// The inner [`NamespaceCache`] implementation MUST commutatively &
/// deterministically merge two [`NamespaceSchema`] to converge (monotonically)
/// towards the same result (gossip payloads are CmRDTs).
///
/// # Portability
///
/// This implementation relies on the rust [`Hash`] implementation, which is
/// specifically defined as being allowed to differ across platforms (for
/// example, with differing endianness) and across different Rust complier
/// versions.
///
/// If two nodes are producing differing hashes for the same underlying content,
/// they will appear to never converge.
///
/// [`MerkleSearchTree`]: merkle_search_tree::MerkleSearchTree
#[derive(Debug)]
pub struct MerkleTree<T> {
    inner: T,

    handle: AntiEntropyHandle,
}

impl<T> MerkleTree<T>
where
    T: Send + Sync,
{
    /// Initialise a new [`MerkleTree`] that generates a content hash covering
    /// `inner`.
    pub fn new(inner: T, handle: AntiEntropyHandle) -> Self {
        Self { inner, handle }
    }

    /// Return a 128-bit hash describing the content of the inner `T`.
    ///
    /// This hash only covers a subset of schema fields.
    pub async fn content_hash(&self) -> merkle_search_tree::digest::RootHash {
        self.handle.content_hash().await
    }
}

#[async_trait]
impl<T> NamespaceCache for MerkleTree<T>
where
    T: NamespaceCache,
{
    type ReadError = T::ReadError;

    async fn get_schema(
        &self,
        namespace: &NamespaceName<'static>,
    ) -> Result<Arc<NamespaceSchema>, Self::ReadError> {
        self.inner.get_schema(namespace).await
    }

    fn put_schema(
        &self,
        namespace: NamespaceName<'static>,
        schema: NamespaceSchema,
    ) -> (Arc<NamespaceSchema>, ChangeStats) {
        // Pass the namespace into the inner storage, and evaluate the merged
        // return value (the new content of the cache).
        let (schema, diff) = self.inner.put_schema(namespace.clone(), schema);

        // Attempt to asynchronously update the MST.
        self.handle.observe_update(namespace.clone());

        // And pass through the return value to the caller.
        (schema, diff)
    }
}
