//! gRPC service for scheduling compactor tasks.
use std::{fmt::Debug, sync::Arc};

use generated_types::influxdata::iox::compactor_scheduler::v1::*;
use tonic::{Request, Response, Status};

use crate::Scheduler;

// Implementation of the compactor scheduler gRPC service
#[derive(Debug)]
pub struct CompactorSchedulerService {
    /// Scheduler
    _scheduler: Arc<dyn Scheduler>,
}

impl CompactorSchedulerService {
    /// Create a new Scheduler service
    pub fn new(_scheduler: Arc<dyn Scheduler>) -> Self {
        Self { _scheduler }
    }
}

#[tonic::async_trait]
impl compactor_scheduler_service_server::CompactorSchedulerService for CompactorSchedulerService {
    async fn get_job(
        &self,
        _request: Request<GetCompactionJobRequest>,
    ) -> Result<Response<ListCompactionJobResponse>, Status> {
        let response = ListCompactionJobResponse {
            compaction_jobs: Vec::new(),
        };
        Ok(Response::new(response))
    }

    async fn report_job_status(
        &self,
        _request: Request<ReportJobStatusRequest>,
    ) -> Result<Response<ReportJobStatusResponse>, Status> {
        let response = ReportJobStatusResponse {};
        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use generated_types::influxdata::iox::compactor_scheduler::v1::compactor_scheduler_service_server::CompactorSchedulerService;

    use crate::LocalScheduler;

    use super::*;

    #[tokio::test]
    async fn get_job() {
        let scheduler = Arc::new(LocalScheduler::default());
        let grpc = super::CompactorSchedulerService::new(scheduler);

        let request = GetCompactionJobRequest {};
        let tonic_response = grpc.get_job(Request::new(request)).await;
        assert!(tonic_response.is_ok());
    }
}
