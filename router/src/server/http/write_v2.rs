use data_types::NamespaceMappingError;
use hyper::Request;
use serde::Deserialize;

use crate::server::http::{write_dml::Precision, Error};

/// v2 DmlErrors returned when decoding the organisation / bucket information from a
/// HTTP request and deriving the namespace name from it.
#[derive(Debug, Error)]
pub enum OrgBucketError {
    /// The request contains no org/bucket destination information.
    #[error("no org/bucket destination provided")]
    NotSpecified,

    /// The request contains invalid parameters.
    #[error("failed to deserialize org/bucket/precision in request: {0}")]
    DecodeFail(#[from] serde::de::value::Error),

    /// The provided org/bucket could not be converted into a namespace name.
    #[error(transparent)]
    MappingFail(#[from] NamespaceMappingError),
}

#[derive(Debug, Deserialize)]
/// Query Parameters for v2 DML operation.
pub(crate) struct WriteParamsV2 {
    pub(crate) org: String,
    pub(crate) bucket: String,

    #[serde(default)]
    pub(crate) precision: Precision,
}

impl<T> TryFrom<&Request<T>> for WriteParamsV2 {
    type Error = OrgBucketError;

    fn try_from(req: &Request<T>) -> Result<Self, Self::Error> {
        let query = req.uri().query().ok_or(OrgBucketError::NotSpecified)?;
        let got: WriteParamsV2 = serde_urlencoded::from_str(query)?;

        // An empty org or bucket is not acceptable.
        if got.org.is_empty() || got.bucket.is_empty() {
            return Err(OrgBucketError::NotSpecified);
        }

        Ok(got)
    }
}

#[cfg(test)]
#[macro_export]
// ensure v2 WRITE contract stays the same
// only namespace_name is different
macro_rules! run_v2_write_test_in_env {
    (
        $name:ident,
        query_string = $query_string:expr,   // Request URI query string
        body = $body:expr,                   // Request body content
        dml_handler = $dml_handler:expr,     // DML write handler response (if called)
        want_result = $want_result:pat,
        want_dml_calls = $($want_dml_calls:tt )+
    ) => {
        paste::paste! {
            mod [<mt_write_ $name>] {
                #[allow(unused)]
                use super::*;
                use $crate::test_mt_handler;
                #[allow(unused)]
                static EXPECTED_NAMESPACE: &str = "bananas_test";

                test_mt_handler!(
                    $name,
                    route_string = "/api/v2/write",
                    query_string = $query_string,
                    body = $body,
                    dml_write_handler = $dml_handler,
                    want_result = $want_result,
                    want_dml_calls = $($want_dml_calls)+
                );
            }

            mod [<cst_write_ $name>] {
                #[allow(unused)]
                use super::*;
                use $crate::test_cst_handler;
                #[allow(unused)]
                static EXPECTED_NAMESPACE: &str = "test";

                test_cst_handler!(
                    $name,
                    route_string = "/api/v2/write",
                    query_string = $query_string,
                    body = $body,
                    dml_write_handler = $dml_handler,
                    want_result = $want_result,
                    want_dml_calls = $($want_dml_calls)+
                );
            }
        }
    };
}

#[cfg(test)]
#[macro_export]
// ensure v2 DELETE contract stays the same
// only namespace_name is different
macro_rules! run_v2_delete_test_in_env {
    (
        $name:ident,
        query_string = $query_string:expr,   // Request URI query string
        body = $body:expr,                   // Request body content
        dml_handler = $dml_handler:expr,     // DML write handler response (if called)
        want_result = $want_result:pat,
        want_dml_calls = $($want_dml_calls:tt )+
    ) => {
        paste::paste! {
            mod [<mt_delete_ $name>] {
                use super::*;
                use $crate::test_mt_handler;
                #[allow(unused)]
                static EXPECTED_NAMESPACE: &str = "bananas_test";

                test_mt_handler!(
                    $name,
                    route_string = "/api/v2/delete",
                    query_string = $query_string,
                    body = $body,
                    dml_delete_handler = $dml_handler,
                    want_result = $want_result,
                    want_dml_calls = $($want_dml_calls)+
                );
            }

            mod [<cst_delete_ $name>] {
                use super::*;
                use $crate::test_cst_handler;
                #[allow(unused)]
                static EXPECTED_NAMESPACE: &str = "test";

                test_cst_handler!(
                    $name,
                    route_string = "/api/v2/delete",
                    query_string = $query_string,
                    body = $body,
                    dml_delete_handler = $dml_handler,
                    want_result = $want_result,
                    want_dml_calls = $($want_dml_calls)+
                );
            }
        }
    };
}
