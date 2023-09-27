//! A tonic gRPC handler implementing the server-side sync /
//! [`AntiEntropyService`] protocol.

use std::{ops::RangeInclusive, result::Result};

use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceNameError, NamespaceSchema};
use generated_types::influxdata::iox::gossip::v1::{
    self as proto, NamespaceSchemaEntry, TableCreated,
};
use merkle_search_tree::{diff::OwnedPageRange, digest::PageDigest};
use thiserror::Error;
use tonic::{Request, Response, Status};

use crate::{
    gossip::{
        anti_entropy::mst::{actor::MerkleSnapshot, handle::AntiEntropyHandle},
        namespace_created, table_created,
    },
    namespace_cache::{CacheMissErr, NamespaceCache},
};

/// [`MAX_SYNC_MSG_SIZE`] defines the maximum allowed sync RPC size (over TCP).
pub const MAX_SYNC_MSG_SIZE: usize = 20 * 1024 * 1024; // 20 MiB

#[derive(Debug, Error)]
enum Error {
    /// The sender provided an invalid namespace name.
    #[error("invalid namespace name: {0}")]
    NamespaceName(#[from] NamespaceNameError),

    #[error("invalid page hash")]
    PageHash,
}

impl From<Error> for tonic::Status {
    fn from(v: Error) -> Self {
        match v {
            Error::PageHash | Error::NamespaceName(_) => Status::invalid_argument(v.to_string()),
        }
    }
}

/// The RPC server for the anti-entropy sync service.
///
/// This sync service allows callers to drive [`NamespaceSchema`] cache
/// convergence across nodes by performing the sync protocol over reliable (TCP)
/// transport, and acts as a RPC interface to the Merkle Search Tree maintained
/// by the [`AntiEntropyActor`].
///
/// See the [`ConvergenceActor`] (the caller of this API) for more information.
///
/// [`AntiEntropyActor`]:
///     crate::gossip::anti_entropy::mst::handle::AntiEntropyHandle
/// [`ConvergenceActor`]:
///     crate::gossip::anti_entropy::sync::actor::ConvergenceActor
#[derive(Debug, Clone)]
pub struct AntiEntropyService<T> {
    mst: AntiEntropyHandle,

    /// The [`NamespaceCache`] tracked by the merkle tree above.
    cache: T,
}

impl<T> AntiEntropyService<T> {
    /// Initialise a new [`AntiEntropyService`] that delegates merkle tree
    /// operations to the provided [`AntiEntropyHandle`] and returns schemas
    /// from the given [`NamespaceCache`].
    pub fn new(mst: AntiEntropyHandle, cache: T) -> Self {
        Self { mst, cache }
    }
}

#[async_trait]
impl<T> proto::anti_entropy_service_server::AntiEntropyService for AntiEntropyService<T>
where
    T: NamespaceCache<ReadError = CacheMissErr> + 'static,
{
    /// Return the computed Merkle Search Tree difference between the senders
    /// serialised compact MST representation included in the request, and the
    /// receivers local MST.
    ///
    /// The caller of this RPC sends their serialised MST page ranges, and the
    /// callee performs the tree diff, returning the key ranges identified as
    /// containing inconsistencies.
    async fn get_tree_diff(
        &self,
        request: Request<proto::GetTreeDiffRequest>,
    ) -> Result<Response<proto::GetTreeDiffResponse>, Status> {
        // Convert the API types into a set of MST page ranges.
        let pages = request
            .into_inner()
            .pages
            .into_iter()
            .map(|v| {
                Ok(OwnedPageRange::new(
                    NamespaceName::try_from(v.min)?,
                    NamespaceName::try_from(v.max)?,
                    PageDigest::new(
                        v.page_hash
                            .clone()
                            .try_into()
                            .map_err(|_| Error::PageHash)?,
                    ),
                ))
            })
            .collect::<Result<MerkleSnapshot, Error>>()?;

        // Send the page ranges to the MST actor and have it compute the diff
        // between the provided page ranges, and the local MST pages.
        let ranges = self
            .mst
            .compute_diff(pages)
            .await
            .into_iter()
            .map(|v| {
                let (min, max) = v.into_inner();
                proto::DiffRange {
                    min: min.into_string(),
                    max: max.into_string(),
                }
            })
            .collect();

        Ok(Response::new(proto::GetTreeDiffResponse { ranges }))
    }

    /// Fetch all schemas in the peer cache within the specified inclusive key
    /// range bounds.
    async fn get_range(
        &self,
        request: Request<proto::GetRangeRequest>,
    ) -> Result<Response<proto::GetRangeResponse>, Status> {
        let request = request.into_inner();
        let range = RangeInclusive::new(
            NamespaceName::try_from(request.min).map_err(Error::NamespaceName)?,
            NamespaceName::try_from(request.max).map_err(Error::NamespaceName)?,
        );

        // Expand the range into the set of keys known to the MST in the
        // specified (inclusive) range.
        //
        // To converge state, only keys covered by the MST are relevant -
        // including keys unknown to the MST in a sync (i.e. pulling a range
        // directly from the namespace cache) can only cause further divergence
        // as the receiver MST will add the returned keys, causing the receiver
        // to contain keys that the local MST does not.
        //
        // In practice, the MST content should always reflect the namespace
        // cache content (or in the worse case, eventually converge to it).
        let keys = self.mst.get_keys_in_range(range).await;

        let mut out = Vec::with_capacity(keys.len());
        for v in keys {
            let schema = self
                .cache
                .get_schema(&v)
                .await
                .expect("key in MST does not exist in cache");

            out.push(NamespaceSchemaEntry {
                namespace: Some(namespace_created(&v, &schema)),
                tables: table_events(&v, &schema),
            });
        }

        Ok(Response::new(proto::GetRangeResponse { namespaces: out }))
    }
}

/// Construct the set of [`TableCreated`] events that describe all tables in
/// `schema`.
fn table_events<'a>(
    namespace_name: &'a NamespaceName<'_>,
    schema: &'a NamespaceSchema,
) -> Vec<TableCreated> {
    schema
        .tables
        .iter()
        .map(|(name, t_schema)| table_created(name.to_string(), namespace_name, t_schema))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use data_types::NamespaceId;
    use generated_types::influxdata::iox::gossip::v1::{
        anti_entropy_service_server::AntiEntropyService as _, DiffRange, NamespaceCreated,
        PageRange,
    };

    use crate::{
        gossip::anti_entropy::mst::actor::AntiEntropyActor, namespace_cache::MemoryNamespaceCache,
    };

    use super::*;

    fn empty_namespace(id: i64) -> NamespaceSchema {
        NamespaceSchema {
            id: NamespaceId::new(id),
            tables: Default::default(),
            max_tables: Default::default(),
            max_columns_per_table: Default::default(),
            retention_period_ns: Default::default(),
            partition_template: Default::default(),
        }
    }

    #[tokio::test]
    async fn test_get_tree_diff() {
        let cache = Arc::new(MemoryNamespaceCache::default());

        let (actor, mst) = AntiEntropyActor::new(Arc::clone(&cache));
        tokio::spawn(actor.run());

        // Place some entries in the cache and have the MST update
        {
            let name = NamespaceName::try_from("bananas").unwrap();
            cache.put_schema(name.clone(), empty_namespace(42));
            mst.observe_update_blocking(name).await;
        }
        {
            let name = NamespaceName::try_from("plátanos").unwrap();
            cache.put_schema(name.clone(), empty_namespace(24));
            mst.observe_update_blocking(name).await;
        }

        // Initialise the RPC server handler
        let server = AntiEntropyService::new(mst, cache);

        let response = server
            .get_tree_diff(Request::new(proto::GetTreeDiffRequest {
                pages: vec![PageRange {
                    min: "donkey".to_string(),
                    max: "donkey".to_string(),
                    page_hash: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6],
                }],
            }))
            .await
            .expect("valid request");

        assert_eq!(
            response.into_inner(),
            proto::GetTreeDiffResponse {
                ranges: vec![DiffRange {
                    min: "bananas".to_string(),
                    max: "plátanos".to_string(),
                }]
            }
        )
    }

    #[tokio::test]
    async fn test_get_range() {
        let cache = Arc::new(MemoryNamespaceCache::default());

        let (actor, mst) = AntiEntropyActor::new(Arc::clone(&cache));
        tokio::spawn(actor.run());

        // Place some entries in the cache and have the MST update
        {
            let name = NamespaceName::try_from("bananas").unwrap();
            cache.put_schema(name.clone(), empty_namespace(42));
            mst.observe_update_blocking(name).await;
        }
        {
            let name = NamespaceName::try_from("plátanos").unwrap();
            cache.put_schema(name.clone(), empty_namespace(24));
            mst.observe_update_blocking(name).await;
        }

        // Initialise the RPC server handler
        let server = AntiEntropyService::new(mst, cache);

        let response = server
            .get_range(Request::new(proto::GetRangeRequest {
                min: "apples".to_string(),
                max: "zombies".to_string(),
            }))
            .await
            .expect("valid request");

        pretty_assertions::assert_eq!(
            response.into_inner(),
            proto::GetRangeResponse {
                namespaces: vec![
                    NamespaceSchemaEntry {
                        namespace: Some(NamespaceCreated {
                            namespace_name: "bananas".to_string(),
                            namespace_id: 42,
                            partition_template: None,
                            max_columns_per_table: 200,
                            max_tables: 500,
                            retention_period_ns: None,
                        }),
                        tables: vec![],
                    },
                    NamespaceSchemaEntry {
                        namespace: Some(NamespaceCreated {
                            namespace_name: "plátanos".to_string(),
                            namespace_id: 24,
                            partition_template: None,
                            max_columns_per_table: 200,
                            max_tables: 500,
                            retention_period_ns: None,
                        }),
                        tables: vec![],
                    }
                ]
            }
        )
    }
}
