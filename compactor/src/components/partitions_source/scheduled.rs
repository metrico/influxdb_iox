use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use compactor_scheduler_grpc::Scheduler;
use data_types::{PartitionId, PartitionsSource};

#[derive(Debug)]
pub struct ScheduledPartitionsSource {
    scheduler: Arc<dyn Scheduler>,
}

impl ScheduledPartitionsSource {
    pub fn new(scheduler: Arc<dyn Scheduler>) -> Self {
        Self { scheduler }
    }
}

impl Display for ScheduledPartitionsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "not_empty({})", self.scheduler)
    }
}

#[async_trait]
impl PartitionsSource for ScheduledPartitionsSource {
    async fn fetch(&self) -> Vec<PartitionId> {
        self.scheduler.get_partitions().await
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_fetch() {
        // FIXME TODO:
        // move this abstraction to use the grpc service
        // after this service is defined
    }
}
