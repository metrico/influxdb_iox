//! MultiTenantRequestParser
/// This is a [`WriteInfoExtractor`] implementation that populates [`WriteInfo`]
/// structs from [`Request`] that conform to the [V1 Write API] or [V2 Write API]
/// when this router is configured in multiple-tenant mode.
///
/// [V1 Write API]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint
/// [V2 Write API]: https://docs.influxdata.com/influxdb/v2.6/api/#operation/PostWrite
use hyper::{Body, Request};

use super::{
    write_dml::WriteInfo,
    write_v2::{OrgBucketError, WriteParamsV2},
    WriteInfoExtractor,
};
use crate::server::http::Error;
use data_types::org_and_bucket_to_namespace;

#[allow(missing_docs)]
#[derive(Debug, Default)]
pub struct MultiTenantRequestParser;

impl WriteInfoExtractor for MultiTenantRequestParser {
    fn extract_v1_dml_info(&self, _req: &Request<Body>) -> Result<WriteInfo, Error> {
        println!("mt::extract_v1_dml_info");

        Err(Error::NoHandler)
    }

    fn extract_v2_dml_info(&self, req: &Request<Body>) -> Result<WriteInfo, Error> {
        let write_params = WriteParamsV2::try_from(req)?;
        let namespace = org_and_bucket_to_namespace(&write_params.org, &write_params.bucket)
            .map_err(OrgBucketError::MappingFail)?;
        let skip_database_creation = None;

        Ok(WriteInfo {
            namespace,
            precision: write_params.precision,
            skip_database_creation,
        })
    }
}
