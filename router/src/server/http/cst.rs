//! SingleTenantRequestParser
///  This is a [`WriteInfoExtractor`] implementation that populates [`WriteInfo`]
/// structs from [`Request`] that conform to the [V1 Write API] or [V2 Write API]
/// when this router is configured in single-tenancy mode.
///
/// [V1 Write API]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint
/// [V2 Write API]: https://docs.influxdata.com/influxdb/v2.6/api/#operation/PostWrite
/// [CST-specific namespace name construction]: docs TBD
use hyper::{Body, Request};

use super::{
    write_dml::WriteInfo,
    write_v1::{DatabaseRpError, RetentionPolicy, WriteParamsV1},
    write_v2::{OrgBucketError, WriteParamsV2},
    WriteInfoExtractor,
};
use crate::server::http::Error::{self, InvalidOrgBucket};
use data_types::{rp_and_database_to_namespace, NamespaceName};

#[allow(missing_docs)]
#[derive(Debug, Default)]
pub struct SingleTenantRequestParser;

impl WriteInfoExtractor for SingleTenantRequestParser {
    fn extract_v1_dml_info(&self, req: &Request<Body>) -> Result<WriteInfo, Error> {
        let write_params = WriteParamsV1::try_from(req)?;

        let namespace = match write_params.rp {
            RetentionPolicy::Unspecified => NamespaceName::try_from(&write_params.db),
            RetentionPolicy::Autogen => NamespaceName::try_from(&write_params.db),
            RetentionPolicy::Named(rp) => rp_and_database_to_namespace(&rp, &write_params.db),
        }
        .map_err(DatabaseRpError::MappingFail)?;

        let skip_database_creation = Some(false); // Only in v1 telegraf. Ignored for now.

        Ok(WriteInfo {
            namespace,
            precision: write_params.precision,
            skip_database_creation,
        })
    }

    fn extract_v2_dml_info(&self, req: &Request<Body>) -> Result<WriteInfo, Error> {
        let write_params = WriteParamsV2::try_from(req)?;
        if write_params.bucket.is_empty() {
            return Err(InvalidOrgBucket(OrgBucketError::NotSpecified));
        }
        let namespace =
            NamespaceName::try_from(&write_params.bucket).map_err(OrgBucketError::MappingFail)?;
        let skip_database_creation = None;

        Ok(WriteInfo {
            namespace,
            precision: write_params.precision,
            skip_database_creation,
        })
    }
}
