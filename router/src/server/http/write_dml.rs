use hyper::{Body, Request};
use serde::Deserialize;

use super::write_v1::{DatabaseRpError, WriteParamsV1};
use super::write_v2::{OrgBucketError, WriteParamsV2};
use crate::server::http::{Error, WriteInfoExtractor, CST, MT};
use data_types::{
    org_and_bucket_to_namespace, rp_and_database_to_namespace, string_to_namespace, NamespaceName,
};

#[derive(Clone, Debug, Deserialize)]
pub(crate) enum Precision {
    #[serde(rename = "s")]
    Seconds,
    #[serde(rename = "ms")]
    Milliseconds,
    #[serde(rename = "us")]
    Microseconds,
    #[serde(rename = "ns")]
    Nanoseconds,
}

impl Default for Precision {
    fn default() -> Self {
        Self::Nanoseconds
    }
}

impl Precision {
    /// Returns the multiplier to convert to nanosecond timestamps
    pub(crate) fn timestamp_base(&self) -> i64 {
        match self {
            Precision::Seconds => 1_000_000_000,
            Precision::Milliseconds => 1_000_000,
            Precision::Microseconds => 1_000,
            Precision::Nanoseconds => 1,
        }
    }
}

#[derive(Debug)]
pub(crate) enum ContentEncoding {
    #[allow(dead_code)]
    Gzip,
    Identity,
}

// TODO: what is the default here?
impl Default for ContentEncoding {
    fn default() -> Self {
        Self::Identity
    }
}

#[derive(Debug)]
/// Standardized DML operation parameters
pub struct WriteInfo<'a> {
    pub(crate) namespace: NamespaceName<'a>,
    pub(crate) precision: Precision,
    #[allow(dead_code)]
    pub(crate) skip_database_creation: Option<bool>,
    #[allow(dead_code)]
    pub(crate) content_encoding: ContentEncoding,
}

pub(crate) trait IntoWriteInfo {
    fn into_write_info(self, namespace: NamespaceName<'_>) -> Result<WriteInfo<'_>, Error>;
}

impl WriteInfoExtractor for CST {
    fn extract_v1_dml_info<'a>(&self, req: &Request<Body>) -> Result<WriteInfo<'a>, Error> {
        let write_params = WriteParamsV1::try_from(req)?;
        let namespace = rp_and_database_to_namespace(&write_params.rp, &write_params.db)
            .map_err(DatabaseRpError::MappingFail)?;
        write_params.into_write_info(namespace)
    }

    fn extract_v2_dml_info<'a>(&self, req: &Request<Body>) -> Result<WriteInfo<'a>, Error> {
        let write_params = WriteParamsV2::try_from(req)?;
        let namespace =
            string_to_namespace(&write_params.bucket).map_err(OrgBucketError::MappingFail)?;
        write_params.into_write_info(namespace)
    }
}

impl WriteInfoExtractor for MT {
    fn extract_v1_dml_info<'a>(&self, _req: &Request<Body>) -> Result<WriteInfo<'a>, Error> {
        Err(Error::NoHandler)
    }

    fn extract_v2_dml_info<'a>(&self, req: &Request<Body>) -> Result<WriteInfo<'a>, Error> {
        let write_params = WriteParamsV2::try_from(req)?;
        let namespace = org_and_bucket_to_namespace(&write_params.org, &write_params.bucket)
            .map_err(OrgBucketError::MappingFail)?;
        write_params.into_write_info(namespace)
    }
}
