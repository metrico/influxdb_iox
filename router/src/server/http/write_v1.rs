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
#[macro_export]
// v1 runs only in CST
macro_rules! run_v1_write_test_in_env {
    (
        $name:ident,
        query_string = $query_string:expr,   // Request URI query string
        body = $body:expr,                   // Request body content
        dml_handler = $dml_handler:expr,     // DML write handler response (if called)
        want_result = $want_result:pat,
        want_dml_calls = $($want_dml_calls:tt )+
    ) => {
        paste::paste! {
            mod [<cst_write_ $name>] {
                use super::*;
                use $crate::test_cst_handler;
                #[allow(unused)]
                static EXPECTED_NAMESPACE: &str = "database";

                test_cst_handler!(
                    $name,
                    route_string = "/write",
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
mod tests {
    use crate::{server::http::Error, test_mt_handler};

    test_mt_handler!(
        v1_no_handler,
        route_string = "/write",
        query_string = "?db=database",
        body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
        dml_write_handler = [],
        want_result = Err(Error::NoHandler),
        want_dml_calls = []
    );
}
