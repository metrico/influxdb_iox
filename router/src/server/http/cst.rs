//! SingleTenantRequestParser

use hyper::{Body, Request};

use super::{
    write_dml::WriteInfo,
    write_v1::{DatabaseRpError, RetentionPolicy, WriteParamsV1},
    write_v2::{OrgBucketError, WriteParamsV2},
    WriteInfoExtractor,
};
use crate::server::http::Error::{self, InvalidOrgBucket};
use data_types::NamespaceName;

#[allow(missing_docs)]
#[derive(Debug, Default)]
pub struct SingleTenantRequestParser;

///  This is a [`WriteInfoExtractor`] implementation that populates [`WriteInfo`]
/// structs from [`Request`] that conform to the [V1 Write API] or [V2 Write API]
/// when this router is configured in single-tenancy mode.
///
/// [V1 Write API]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint
/// [V2 Write API]: https://docs.influxdata.com/influxdb/v2.6/api/#operation/PostWrite
/// [CST-specific namespace name construction]: docs TBD
impl WriteInfoExtractor for SingleTenantRequestParser {
    fn extract_v1_dml_info(&self, req: &Request<Body>) -> Result<WriteInfo, Error> {
        let write_params = WriteParamsV1::try_from(req)?;

        const SEPARATOR: char = '/';
        let namespace = match write_params.rp {
            RetentionPolicy::Unspecified | RetentionPolicy::Autogen => {
                NamespaceName::convert_namespace_name(&write_params.db)
            }
            RetentionPolicy::Named(rp) => {
                if write_params.db.is_empty() {
                    return Err(Error::InvalidDatabaseRp(DatabaseRpError::NotSpecified));
                }
                NamespaceName::generate_namespace_name(&write_params.db, &rp, SEPARATOR)
            }
        }
        .map_err(DatabaseRpError::MappingFail)?;

        Ok(WriteInfo {
            namespace,
            precision: write_params.precision,
        })
    }

    fn extract_v2_dml_info(&self, req: &Request<Body>) -> Result<WriteInfo, Error> {
        let write_params = WriteParamsV2::try_from(req)?;
        if write_params.bucket.is_empty() {
            return Err(InvalidOrgBucket(OrgBucketError::NotSpecified));
        }
        let namespace = NamespaceName::convert_namespace_name(&write_params.bucket)
            .map_err(OrgBucketError::MappingFail)?;

        Ok(WriteInfo {
            namespace,
            precision: write_params.precision,
        })
    }
}

#[cfg(test)]
#[macro_export]
/// Wrapper over test_http_handler specifically for CST.
macro_rules! test_cst_handler {
    // Match based on named arg `dml_write_handler`.
    (
        $name:ident,
        route_string = $route_string:expr,   // v1 versus v2 routes
        query_string = $query_string:expr,   // Request URI query string
        body = $body:expr,                   // Request body content
        dml_write_handler = $dml_handler:expr,     // DML write handler response (if called)
        want_result = $want_result:pat,
        want_dml_calls = $($want_dml_calls:tt )+
    ) => {
        paste::paste! {
            $crate::test_http_handler!(
                [<cst_write_ $name>],
                uri = format!("https://bananas.example{}{}", $route_string, $query_string),
                body = $body,
                auth_handler = None, // TODO: add auth handler for cst only.
                dml_info_handler = Box::<$crate::server::http::cst::SingleTenantRequestParser>::default(),
                dml_write_handler = $dml_handler,
                dml_delete_handler = [],
                want_result = $want_result,
                want_dml_calls = $($want_dml_calls)+
            );
        }
    };
    // Match based on named arg `dml_delete_handler`.
    (
        $name:ident,
        route_string = $route_string:expr,   // v1 versus v2 routes
        query_string = $query_string:expr,   // Request URI query string
        body = $body:expr,                   // Request body content
        dml_delete_handler = $dml_handler:expr,     // DML write handler response (if called)
        want_result = $want_result:pat,
        want_dml_calls = $($want_dml_calls:tt )+
    ) => {
        paste::paste! {
            $crate::test_http_handler!(
                [<cst_delete_ $name>],
                uri = format!("https://bananas.example{}{}", $route_string, $query_string),
                body = $body,
                auth_handler = None, // TODO: add auth handler for cst only.
                dml_info_handler = Box::<$crate::server::http::cst::SingleTenantRequestParser>::default(),
                dml_write_handler = [],
                dml_delete_handler = $dml_handler,
                want_result = $want_result,
                want_dml_calls = $($want_dml_calls)+
            );
        }
    };
}
