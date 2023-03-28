use data_types::NamespaceMappingError;
use hyper::Request;
use serde::{Deserialize, Deserializer};

use crate::server::http::{write_dml::Precision, Error};

/// v1 DmlErrors returned when decoding the database / rp information from a
/// HTTP request and deriving the namespace name from it.
#[derive(Debug, Error)]
pub enum DatabaseRpError {
    /// The request contains no org/bucket destination information.
    #[error("no db destination provided")]
    NotSpecified,

    /// The request contains invalid parameters.
    #[error("failed to deserialize db/rp/precision in request: {0}")]
    DecodeFail(#[from] serde::de::value::Error),

    /// The provided db (and optional rp) could not be converted into a namespace name.
    #[error(transparent)]
    MappingFail(#[from] NamespaceMappingError),
}

#[derive(Debug, Deserialize)]
enum Consistency {
    #[serde(rename = "any")]
    Any,
    #[serde(rename = "one")]
    One,
    #[serde(rename = "quorum")]
    Quorum,
    #[serde(rename = "all")]
    All,
}

impl Default for Consistency {
    fn default() -> Self {
        Self::One
    }
}

/// May be empty string, explicit rp name, or `autogen`. As provided at the write API.
/// Handling is described in context of the construction of the `NamespaceName`,
/// and not an explicit honoring for retention duration.
#[derive(Debug)]
pub(crate) enum RetentionPolicy {
    /// The user did not specify a retention policy (at the write API).
    // #[serde(deserialize_with = "deserialize_empty")]
    Unspecified,
    /// Default on v1 database creation, if no rp was provided.
    Autogen,
    /// The user specified the name of the retention policy to be used.
    Named(String),
}

impl<'de> Deserialize<'de> for RetentionPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?.to_lowercase();
        let rp = match s.to_lowercase().as_str() {
            "" => RetentionPolicy::Unspecified,
            "''" => RetentionPolicy::Unspecified,
            "autogen" => RetentionPolicy::Autogen,
            _ => RetentionPolicy::Named(s),
        };
        Ok(rp)
    }
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self::Unspecified
    }
}

#[derive(Debug, Deserialize)]
/// Query Parameters for v1 DML operation.
pub(crate) struct WriteParamsV1 {
    pub(crate) db: String,

    #[allow(dead_code)]
    u: Option<String>,
    #[allow(dead_code)]
    p: Option<String>,

    #[allow(dead_code)]
    #[serde(default)]
    consistency: Consistency,
    #[serde(default)]
    pub(crate) precision: Precision,
    #[serde(default)]
    pub(crate) rp: RetentionPolicy,
}

impl<T> TryFrom<&Request<T>> for WriteParamsV1 {
    type Error = DatabaseRpError;

    fn try_from(req: &Request<T>) -> Result<Self, Self::Error> {
        let query = req.uri().query().ok_or(DatabaseRpError::NotSpecified)?;
        let got: WriteParamsV1 = serde_urlencoded::from_str(query)?;

        // An empty database name is not acceptable.
        if got.db.is_empty() {
            return Err(DatabaseRpError::NotSpecified);
        }

        Ok(got)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dml_handlers::mock::MockDmlHandlerCall,
        server::http::{
            write_test_helpers::{summary, MAX_BYTES, NAMESPACE_ID},
            Error,
        },
        test_write_handler,
    };

    mod mt {
        use super::*;
        use crate::server::http::mt::MultiTenantRequestParser;

        test_write_handler!(
            mt_v1_no_handler,
            route_string = "/write",
            query_string = "?db=database",
            body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
            dml_info_handler = &MultiTenantRequestParser,
            dml_handler = [],
            want_result = Err(Error::NoHandler),
            want_dml_calls = []
        );
    }

    mod cst {
        use super::*;
        use crate::server::http::cst::SingleTenantRequestParser;

        mod v1 {
            use super::*;
            static EXPECTED_NAMESPACE: &str = "database";

            test_write_handler!(
                cst_v1_ok,
                route_string = "/write",
                query_string = "?db=database",
                body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                dml_info_handler = &SingleTenantRequestParser,
                dml_handler = [Ok(summary())],
                want_result = Ok(_),
                want_dml_calls = [MockDmlHandlerCall::Write{namespace, ..}] => {
                    assert_eq!(namespace, EXPECTED_NAMESPACE);
                }
            );

            test_write_handler!(
                cst_v1_no_query_params,
                route_string = "/write",
                query_string = "",
                body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                dml_info_handler = &SingleTenantRequestParser,
                dml_handler = [Ok(summary())],
                want_result = Err(Error::InvalidDatabaseRp(DatabaseRpError::NotSpecified)),
                want_dml_calls = [] // None
            );

            test_write_handler!(
                cst_v1_no_db,
                route_string = "/write",
                query_string = "?",
                body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                dml_info_handler = &SingleTenantRequestParser,
                dml_handler = [Ok(summary())],
                want_result = Err(Error::InvalidDatabaseRp(DatabaseRpError::DecodeFail(_))),
                want_dml_calls = [] // None
            );

            test_write_handler!(
                cst_v1_empty_db,
                route_string = "/write",
                query_string = "?db=",
                body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                dml_info_handler = &SingleTenantRequestParser,
                dml_handler = [Ok(summary())],
                want_result = Err(Error::InvalidDatabaseRp(DatabaseRpError::NotSpecified)),
                want_dml_calls = [] // None
            );

            test_write_handler!(
                cst_v1_invalid_db,
                route_string = "/write",
                query_string = format!("?db={}", "A".repeat(1000)),
                body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                dml_info_handler = &SingleTenantRequestParser,
                dml_handler = [Ok(summary())],
                want_result = Err(Error::InvalidDatabaseRp(DatabaseRpError::MappingFail(_))),
                want_dml_calls = [] // None
            );

            test_write_handler!(
                cst_v1_ok_with_consistency,
                route_string = "/write",
                query_string = "?db=database&consistency=any",
                body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                dml_info_handler = &SingleTenantRequestParser,
                dml_handler = [Ok(summary())],
                want_result = Ok(_),
                want_dml_calls = [MockDmlHandlerCall::Write{namespace, ..}] => {
                    assert_eq!(namespace, EXPECTED_NAMESPACE);
                }
            );

            test_write_handler!(
                cst_v1_invalid_consistency,
                route_string = "/write",
                query_string = "?db=database&consistency=wrong",
                body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                dml_info_handler = &SingleTenantRequestParser,
                dml_handler = [Ok(summary())],
                want_result = Err(Error::InvalidDatabaseRp(DatabaseRpError::DecodeFail(_))),
                want_dml_calls = [] // None
            );

            mod with_rp {
                use super::*;
                static EXPECTED_NAMESPACE: &str = "database/myrp";

                test_write_handler!(
                    cst_v1_rp_ok,
                    route_string = "/write",
                    query_string = "?db=database&rp=myrp",
                    body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                    dml_info_handler = &SingleTenantRequestParser,
                    dml_handler = [Ok(summary())],
                    want_result = Ok(_),
                    want_dml_calls = [MockDmlHandlerCall::Write{namespace, ..}] => {
                        assert_eq!(namespace, EXPECTED_NAMESPACE);
                    }
                );
            }

            mod with_rp_ignored {
                use super::*;
                static EXPECTED_NAMESPACE: &str = "database";

                test_write_handler!(
                    cst_v1_rp_empty,
                    route_string = "/write",
                    query_string = "?db=database&rp=",
                    body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                    dml_info_handler = &SingleTenantRequestParser,
                    dml_handler = [Ok(summary())],
                    want_result = Ok(_),
                    want_dml_calls = [MockDmlHandlerCall::Write{namespace, ..}] => {
                        assert_eq!(namespace, EXPECTED_NAMESPACE);
                    }
                );

                test_write_handler!(
                    cst_v1_rp_empty_str,
                    route_string = "/write",
                    query_string = "?db=database&rp=''",
                    body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                    dml_info_handler = &SingleTenantRequestParser,
                    dml_handler = [Ok(summary())],
                    want_result = Ok(_),
                    want_dml_calls = [MockDmlHandlerCall::Write{namespace, ..}] => {
                        assert_eq!(namespace, EXPECTED_NAMESPACE);
                    }
                );

                test_write_handler!(
                    cst_v1_rp_ignore_autogen,
                    route_string = "/write",
                    query_string = "?db=database&rp=autogen",
                    body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                    dml_info_handler = &SingleTenantRequestParser,
                    dml_handler = [Ok(summary())],
                    want_result = Ok(_),
                    want_dml_calls = [MockDmlHandlerCall::Write{namespace, ..}] => {
                        assert_eq!(namespace, EXPECTED_NAMESPACE);
                    }
                );
            }
        }
    }
}
