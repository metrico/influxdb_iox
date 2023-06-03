//! Internals used by [`RemoteScheduler`].
use std::fmt::Display;

use async_trait::async_trait;
use generated_types::influxdata::iox::compactor_scheduler::v1::*;
use tonic::{transport::Channel, Response, Status};

use crate::scheduler::{CompactionJob, Scheduler};

/// Implementation of the [`Scheduler`] for remote coordinated scheduling.
#[derive(Debug)]
pub(crate) struct RemoteScheduler {
    client: compactor_scheduler_service_client::CompactorSchedulerServiceClient<Channel>,
}

impl RemoteScheduler {
    /// Create new LocalScheduler.
    pub(crate) fn new() -> Self {
        unimplemented!("'remote' compactor-scheduler is not yet implemented")
    }

    #[allow(dead_code)]
    /// Attempt to connect to remote scheduler.
    pub(crate) async fn connect(&self) -> Result<Self, Box<dyn std::error::Error>> {
        unimplemented!("'remote' compactor-scheduler is not yet implemented");
    }
}

#[async_trait]
impl Scheduler for RemoteScheduler {
    async fn get_job(&self) -> Vec<CompactionJob> {
        let mut client = self.client.clone();
        let response: Result<Response<GetJobResponse>, Status> =
            client.get_job(GetJobRequest {}).await;
        let grpc_jobs = response
            .expect("need to add error handling")
            .into_inner()
            .compaction_jobs;
        grpc_jobs.into_iter().map(CompactionJob::from).collect()
    }
}

impl Display for RemoteScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "remote_compaction_scheduler")
    }
}
