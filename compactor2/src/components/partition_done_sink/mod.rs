use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::PartitionId;

use crate::error::DynError;

pub mod catalog;
pub mod error_kind;
pub mod logging;
pub mod metrics;
pub mod mock;

#[async_trait]
pub trait PartitionDoneSink: Debug + Display + Send + Sync {
    /// Record "partition is done" status for given partition.
    ///
    /// This method should retry.
    async fn record(&self, partition: PartitionId, res: Result<(), DynError>);
}

#[async_trait]
impl<T> PartitionDoneSink for Arc<T>
where
    T: PartitionDoneSink + ?Sized,
{
    async fn record(&self, partition: PartitionId, res: Result<(), DynError>) {
        self.as_ref().record(partition, res).await
    }
}
