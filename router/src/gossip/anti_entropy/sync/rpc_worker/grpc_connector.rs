//! gRPC-based [`SyncRpcClient`] and it's [`SyncRpcConnector`] implementation.

use std::{net::SocketAddr, ops::RangeInclusive, time::Duration};

use async_trait::async_trait;
use data_types::NamespaceName;
use generated_types::influxdata::iox::gossip::v1::{
    self as proto, anti_entropy_service_client::AntiEntropyServiceClient, NamespaceSchemaEntry,
};
use tonic::transport::Endpoint;

use crate::gossip::anti_entropy::{
    mst::actor::MerkleSnapshot,
    sync::traits::{BoxedError, SyncRpcClient, SyncRpcConnector},
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// A [`SyncRpcConnector`] using tonic/gRPC.
#[derive(Debug, Default, Clone)]
pub struct GrpcConnector;

#[async_trait]
impl SyncRpcConnector for GrpcConnector {
    type Client = RpcClient;

    async fn connect(&self, addr: SocketAddr) -> Result<Self::Client, BoxedError> {
        let endpoint = Endpoint::from_shared(format!("http://{}", addr))?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT);

        let c = endpoint.connect().await?;

        Ok(RpcClient(AntiEntropyServiceClient::new(c)))
    }
}

/// A gRPC implementation of the [`SyncRpcClient`].
///
/// Constructed by [`GrpcConnector`].
#[derive(Debug)]
pub struct RpcClient(AntiEntropyServiceClient<tonic::transport::Channel>);

#[async_trait]
impl SyncRpcClient for RpcClient {
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

        self.0
            .get_tree_diff(tonic::Request::new(proto::GetTreeDiffRequest { pages }))
            .await?
            .into_inner()
            .ranges
            .into_iter()
            .map(|v| Ok(RangeInclusive::new(v.min.try_into()?, v.max.try_into()?)))
            .collect()
    }

    async fn get_schemas_in_range(
        &mut self,
        range: RangeInclusive<NamespaceName<'static>>,
    ) -> Result<Vec<NamespaceSchemaEntry>, BoxedError> {
        let resp = self
            .0
            .get_range(tonic::Request::new(proto::GetRangeRequest {
                min: range.start().to_string(),
                max: range.end().to_string(),
            }))
            .await?
            .into_inner()
            .namespaces;

        Ok(resp)
    }
}
