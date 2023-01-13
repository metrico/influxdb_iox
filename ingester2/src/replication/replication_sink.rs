use async_trait::async_trait;
use dml::DmlOperation;
use futures::TryFutureExt;
use generated_types::influxdata::iox::ingester::v1::{
    replication_service_client::ReplicationServiceClient, ReplicateRequest,
};
use mutable_batch_pb::encode::encode_write;
use thiserror::Error;
use tokio::try_join;
use tonic::transport::Channel;

use crate::{
    dml_sink::{DmlError, DmlSink},
    ingester_id::IngesterId,
};

/// Errors returned when replicating a write to a upstream replica.
#[derive(Debug, Error)]
pub enum ReplicationError {
    #[error("upstream replica returned error: {0}")]
    UpstreamReplica(#[from] tonic::Status),

    #[error(transparent)]
    Inner(#[from] Box<DmlError>),
}

/// A [`DmlSink`] layer that pushes writes to a configured replica, while
/// concurrently applying them to `T`.
///
/// Blocks for the write to be applied to `T`, and a successful ACK from the
/// replica.
#[derive(Debug)]
pub(crate) struct ReplicationSink<T> {
    inner: T,

    ingester_id: IngesterId,
    client: ReplicationServiceClient<Channel>,
}

impl<T> ReplicationSink<T> {
    /// Construct a new [`ReplicationSink`], pushing writes to `client`.
    pub(crate) fn new(
        inner: T,
        ingester_id: IngesterId,
        client: ReplicationServiceClient<Channel>,
    ) -> Self {
        Self {
            inner,
            ingester_id,
            client,
        }
    }
}

#[async_trait]
impl<T> DmlSink for ReplicationSink<T>
where
    T: DmlSink,
{
    type Error = ReplicationError;

    async fn apply(&self, op: DmlOperation) -> Result<(), Self::Error> {
        match &op {
            DmlOperation::Write(w) => {
                // Generate the future that replicates the write, while op is in
                // scope.
                let mut client = self.client.clone();
                let fut = client
                    .replicate(ReplicateRequest {
                        ingester_uuid: self.ingester_id.to_string(),
                        sequence_number: op
                            .meta()
                            .sequence()
                            .expect("replicating unsequenced write")
                            .sequence_number
                            .get(),
                        payload: Some(encode_write(op.namespace_id().get(), w)),
                    })
                    .map_err(ReplicationError::from);

                // Pass op into the inner handler, obtaining the future to
                // execute it.
                let inner_fut = self
                    .inner
                    .apply(op)
                    .map_err(|e| ReplicationError::Inner(Box::new(Into::<DmlError>::into(e))));

                // Execute both the apply to the inner handler, and the
                // replication push to the replica at the same time.
                try_join!(fut, inner_fut)?;

                Ok(())
            }
            DmlOperation::Delete(_) => unreachable!(),
        }
    }
}
