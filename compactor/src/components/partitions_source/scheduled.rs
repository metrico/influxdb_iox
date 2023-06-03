use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use compactor_scheduler::{CompactionJob, Scheduler};
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
        let job: Vec<CompactionJob> = self.scheduler.get_job().await;
        job.into_iter().map(|job| job.partition_id).collect()
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
