use std::sync::Arc;

use async_trait::async_trait;
use generated_types::influxdata::iox::ingester::v1::{
    replication_service_client::ReplicationServiceClient, PersistCompleteRequest,
};
use observability_deps::tracing::{debug, warn};
use tonic::transport::Channel;

use crate::{
    ingester_id::IngesterId,
    persist::completion_observer::{CompletedPersist, PersistCompletionObserver},
};

/// An [`PersistCompletionObserver`] implementation that synchronously
/// replicates persistence completion events to an upstream replica.
#[derive(Debug)]
pub(crate) struct PersistReplicationObserver {
    ingester_id: IngesterId,
    client: ReplicationServiceClient<Channel>,
}

impl PersistReplicationObserver {
    /// Construct a new [`PersistReplicationObserver`], pushing persist
    /// notifications to `client`.
    pub(crate) fn new(ingester_id: IngesterId, client: ReplicationServiceClient<Channel>) -> Self {
        Self {
            ingester_id,
            client,
        }
    }
}

#[async_trait]
impl PersistCompletionObserver for PersistReplicationObserver {
    async fn persist_complete(&self, note: Arc<CompletedPersist>) {
        let resp = self
            .client
            .clone()
            .persist_complete(PersistCompleteRequest {
                ingester_uuid: self.ingester_id.to_string(),
                namespace_id: note.namespace_id().get(),
                table_id: note.table_id().get(),
                partition_id: note.partition_id().get(),
                croaring_sequence_number_bitmap: note.sequence_numbers().to_bytes(),
            })
            .await;

        let namespace_id = note.namespace_id();
        let table_id = note.table_id();
        let partition_id = note.partition_id();
        let n_writes = note.sequence_numbers().len();

        match resp {
            Ok(_) => debug!(
                %partition_id,
                %table_id,
                %namespace_id,
                %n_writes,
                "replicated persist completion event"
            ),
            Err(error) => warn!(
                %error,
                %partition_id,
                %table_id,
                %namespace_id,
                %n_writes,
                "failed to replicate persist completion event"
            ),
        };
    }
}
