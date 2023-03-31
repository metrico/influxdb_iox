//! MultiTenantRequestParser

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

/// This is a [`WriteInfoExtractor`] implementation that populates [`WriteInfo`]
/// structs from [`Request`] that conform to the [V1 Write API] or [V2 Write API]
/// when this router is configured in multiple-tenant mode.
///
/// [V1 Write API]: https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint
/// [V2 Write API]: https://docs.influxdata.com/influxdb/v2.6/api/#operation/PostWrite
impl WriteInfoExtractor for MultiTenantRequestParser {
    fn extract_v1_dml_info(&self, _req: &Request<Body>) -> Result<WriteInfo, Error> {
        Err(Error::NoHandler)
    }

    fn extract_v2_dml_info(&self, req: &Request<Body>) -> Result<WriteInfo, Error> {
        let write_params = WriteParamsV2::try_from(req)?;
        let namespace = org_and_bucket_to_namespace(&write_params.org, &write_params.bucket)
            .map_err(OrgBucketError::MappingFail)?;

        Ok(WriteInfo {
            namespace,
            precision: write_params.precision,
        })
    }
}

#[cfg(test)]
#[macro_export]
/// Wrapper over test_http_handler specifically for MT.
macro_rules! test_mt_handler {
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
                [<mt_write_ $name>],
                uri = format!("https://bananas.example{}{}", $route_string, $query_string),
                body = $body,
                auth_handler = None, // handled at gateway
                dml_info_handler = Box::<$crate::server::http::mt::MultiTenantRequestParser>::default(),
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
                [<mt_delete_ $name>],
                uri = format!("https://bananas.example{}{}", $route_string, $query_string),
                body = $body,
                auth_handler = None, // handled at gateway
                dml_info_handler = Box::<$crate::server::http::mt::MultiTenantRequestParser>::default(),
                dml_write_handler = [],
                dml_delete_handler = $dml_handler,
                want_result = $want_result,
                want_dml_calls = $($want_dml_calls)+
            );
        }
    };
}
