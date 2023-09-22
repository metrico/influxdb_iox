//! A mock [`SyncRpcClient`] and [`SyncRpcConnector`] factory.

use std::{collections::VecDeque, net::SocketAddr, ops::RangeInclusive, sync::Arc};

use async_trait::async_trait;
use data_types::NamespaceName;
use generated_types::influxdata::iox::gossip::v1::NamespaceSchemaEntry;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::gossip::anti_entropy::mst::actor::MerkleSnapshot;

use super::traits::{BoxedError, SyncRpcClient, SyncRpcConnector};

#[derive(Debug)]
pub struct MockSyncRpcConnector<T> {
    client: T,
    calls: Mutex<Vec<SocketAddr>>,
}

impl Default for MockSyncRpcConnector<Arc<MockSyncRpcClient>> {
    fn default() -> Self {
        Self {
            client: Arc::new(MockSyncRpcClient::default()),
            calls: Default::default(),
        }
    }
}

#[async_trait]
impl<T> SyncRpcConnector for MockSyncRpcConnector<T>
where
    T: SyncRpcClient + Clone,
{
    type Client = T;

    async fn connect(&self, addr: SocketAddr) -> Result<Self::Client, BoxedError> {
        self.calls.lock().push(addr);
        Ok(self.client.clone())
    }
}

impl<T> MockSyncRpcConnector<T> {
    pub fn with_client<U>(self, client: U) -> MockSyncRpcConnector<U> {
        MockSyncRpcConnector {
            client,
            calls: Default::default(),
        }
    }

    pub fn calls(&self) -> Vec<SocketAddr> {
        self.calls.lock().clone()
    }
}

#[derive(Debug, Clone)]
pub enum MockCalls {
    FindInconsistentRanges(MerkleSnapshot),
    GetSchemasInRange(RangeInclusive<NamespaceName<'static>>),
}

#[derive(Debug, Default)]
pub struct MockSyncRpcClient {
    calls: Mutex<Vec<MockCalls>>,

    /// An optional [`MockCalls`] notification blocking sent within each call.
    notify: Option<mpsc::Sender<MockCalls>>,

    ret_find_inconsistent_ranges:
        Mutex<VecDeque<Result<Vec<RangeInclusive<NamespaceName<'static>>>, BoxedError>>>,

    ret_get_schemas_in_range: Mutex<VecDeque<Result<Vec<NamespaceSchemaEntry>, BoxedError>>>,
}

impl MockSyncRpcClient {
    pub fn with_find_inconsistent_ranges(
        self,
        ret: impl Into<VecDeque<Result<Vec<RangeInclusive<NamespaceName<'static>>>, BoxedError>>>,
    ) -> Self {
        *self.ret_find_inconsistent_ranges.lock() = ret.into();
        self
    }

    pub fn with_get_schemas_in_range(
        self,
        ret: impl Into<VecDeque<Result<Vec<NamespaceSchemaEntry>, BoxedError>>>,
    ) -> Self {
        *self.ret_get_schemas_in_range.lock() = ret.into();
        self
    }

    pub fn with_notify(mut self, notify: mpsc::Sender<MockCalls>) -> Self {
        self.notify = Some(notify);
        self
    }

    pub fn calls(&self) -> Vec<MockCalls> {
        self.calls.lock().clone()
    }
}

#[async_trait]
impl SyncRpcClient for Arc<MockSyncRpcClient> {
    async fn find_inconsistent_ranges(
        &mut self,
        pages: MerkleSnapshot,
    ) -> Result<Vec<RangeInclusive<NamespaceName<'static>>>, BoxedError> {
        // Send on the notification channel, if any.
        if let Some(n) = &self.notify {
            n.send(MockCalls::FindInconsistentRanges(pages.clone()))
                .await
                .unwrap();
        }

        self.calls
            .lock()
            .push(MockCalls::FindInconsistentRanges(pages));

        self.ret_find_inconsistent_ranges
            .lock()
            .pop_front()
            .expect("no mock value configured for find_inconsistent_ranges()")
    }

    async fn get_schemas_in_range(
        &mut self,
        range: RangeInclusive<NamespaceName<'static>>,
    ) -> Result<Vec<NamespaceSchemaEntry>, BoxedError> {
        // Send on the notification channel, if any.
        if let Some(n) = &self.notify {
            n.send(MockCalls::GetSchemasInRange(range.clone()))
                .await
                .unwrap();
        }

        self.calls.lock().push(MockCalls::GetSchemasInRange(range));

        self.ret_get_schemas_in_range
            .lock()
            .pop_front()
            .expect("no mock value configured for get_schemas_in_range()")
    }
}
