//! Abstractions decoupling the anti-entropy subsystem from other subsystems.

use std::{fmt::Debug, net::SocketAddr, ops::RangeInclusive, sync::Arc};

use async_trait::async_trait;
use data_types::NamespaceName;
use generated_types::influxdata::iox::gossip::v1::NamespaceSchemaEntry;

use crate::gossip::anti_entropy::mst::actor::MerkleSnapshot;

pub(super) type BoxedError = Box<dyn std::error::Error + Send + Sync>;

/// An abstract constructor of [`SyncRpcClient`] instances.
#[async_trait]
pub trait SyncRpcConnector: Send + Sync + Debug {
    /// The concrete client type.
    type Client: SyncRpcClient;

    /// Connect to the specified `addr`, returning an error if unreachable.
    async fn connect(&self, addr: SocketAddr) -> Result<Self::Client, BoxedError>;
}

#[async_trait]
impl<T> SyncRpcConnector for Arc<T>
where
    T: SyncRpcConnector,
{
    type Client = T::Client;

    async fn connect(&self, addr: SocketAddr) -> Result<Self::Client, BoxedError> {
        T::connect(self, addr).await
    }
}

/// An abstract RPC mechanism used to perform cache synchronisation.
#[async_trait]
pub trait SyncRpcClient: Send + Sync + Debug {
    /// Have the remote perform a [`MerkleSearchTree`] diff against the provided
    /// [`MerkleSnapshot`], returning the set of [`NamespaceName`] ranges that
    /// contain inconsistent entries.
    ///
    /// [`MerkleSearchTree`]: merkle_search_tree::MerkleSearchTree
    async fn find_inconsistent_ranges(
        &mut self,
        pages: MerkleSnapshot,
    ) -> Result<Vec<RangeInclusive<NamespaceName<'static>>>, BoxedError>;

    /// Retrieve the set of [`NamespaceSchemaEntry`] for the given range.
    ///
    /// The [`NamespaceSchemaEntry`] is a compound type containing the set of
    /// gossip events necessary to reconstruct peer's full schema entry for a
    /// given [`NamespaceName`].
    async fn get_schemas_in_range(
        &mut self,
        range: RangeInclusive<NamespaceName<'static>>,
    ) -> Result<Vec<NamespaceSchemaEntry>, BoxedError>;
}
