// use client_util::connection::{self, Connection};
// use crate::{connection::Connection, error::Error};
// use influxdb_iox_client::connection::Connection;
use observability_deps::tracing::warn;
use generated_types::influxdata::iox::compactor::v1::*;

use generated_types::influxdata::iox::compactor::v1::{GetCompactionJobRequest, ListCompactionJobResponse, CompactionJob,  ReportJobStatusRequest, ReportJobStatusResponse};
//use generated_types::influxdata::iox::compactor::v1::compaction_scheduler_service_server::CompactionSchedulerService;

use generated_types::influxdata::iox::compactor::v1::compaction_scheduler_service_server::CompactionSchedulerServiceServer;

use tonic::{Request, Response, Status};


// Implementation of the compactor scheduler gRPC service
#[derive(Debug)]
pub struct CompactionSchedulerService {
}

impl CompactionSchedulerService {
    pub fn new() -> Self {
        Self { }
    }
}

#[tonic::async_trait]
//impl CompactionSchedulerServiceServer for CompactionSchedulerService {
impl compaction_scheduler_service_server::CompactionSchedulerService for CompactionSchedulerService {

    async fn get_job(
        &self,
        //request: Request<GetCompactionJobRequest>,
        //request: tonic::request::Request<GetCompactionJobRequest>
        //request: tonic::IntoRequest<super::GetCompactionJobRequest>
        request: Request<GetCompactionJobRequest>,
    ) -> Result<Response<ListCompactionJobResponse>, Status> {
        let request = request.into_inner();
        let mut client = self.connection.client();
        let response = client
            .get_compaction_job(request)
            .await
            .map_err(|e| {
                warn!(error=%e, "TODO: improve this error message");
                Status::not_found(e.to_string())
            })?;
        Ok(Response::new(response))
    }

    async fn report_job_status(
        &self,
        request: Request<ReportJobStatusRequest>,
    ) -> Result<Response<ReportJobStatusResponse>, Status> {
        let request = request.into_inner();
        let mut client = self.connection.client();
        let response = client
            .report_job_status(request)
            .await
            .map_err(|e| {
                warn!(error=%e, "TODO: improve this error message");
                Status::not_found(e.to_string())
            })?;
        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() {
    // let addr = "[::1]:50051".parse().unwrap();
    // let compactor_scheduler = CompactorSchedulerServer::new(CompactorSchedulerImpl::default());

    // Server::builder()
    //     .add_service(compactor_scheduler)
    //     .serve(addr)
    //     .await?;

    // Ok(())
}