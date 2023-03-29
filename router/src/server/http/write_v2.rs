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
mod tests {
    use super::*;
    use crate::{
        dml_handlers::{mock::MockDmlHandlerCall, DmlError},
        server::http::{
            write_test_helpers::{summary, MAX_BYTES, NAMESPACE_ID},
            Error,
        },
    };
    use mutable_batch::column::ColumnData;
    use mutable_batch_lp::LineWriteError;
    use std::iter;

    macro_rules! run_v2_test_in_env {
        ($test_scope:ident, $dml_info_handler:expr) => {
            use $crate::{test_http_handler, test_write_handler, test_delete_handler};

            paste::paste! {
                test_write_handler!(
                    [<$test_scope _ ok>],
                    query_string = "?org=bananas&bucket=test",
                    body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(summary())],
                    want_result = Ok(_),
                    want_dml_calls = [MockDmlHandlerCall::Write{namespace, ..}] => {
                        assert_eq!(namespace, EXPECTED_NAMESPACE);
                    }
                );

                test_write_handler!(
                    [<$test_scope _ ok_precision_s>],
                    query_string = "?org=bananas&bucket=test&precision=s",
                    body = "platanos,tag1=A,tag2=B val=42i 1647622847".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(summary())],
                    want_result = Ok(_),
                    want_dml_calls = [MockDmlHandlerCall::Write{namespace, namespace_id, write_input}] => {
                        assert_eq!(namespace, EXPECTED_NAMESPACE);
                        assert_eq!(*namespace_id, NAMESPACE_ID);

                        let table = write_input.get("platanos").expect("table not found");
                        let ts = table.timestamp_summary().expect("no timestamp summary");
                        assert_eq!(Some(1647622847000000000), ts.stats.min);
                    }
                );

                test_write_handler!(
                    [<$test_scope _ ok_precision_ms>],
                    query_string = "?org=bananas&bucket=test&precision=ms",
                    body = "platanos,tag1=A,tag2=B val=42i 1647622847000".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(summary())],
                    want_result = Ok(_),
                    want_dml_calls = [MockDmlHandlerCall::Write{namespace, namespace_id, write_input}] => {
                        assert_eq!(namespace, EXPECTED_NAMESPACE);
                        assert_eq!(*namespace_id, NAMESPACE_ID);

                        let table = write_input.get("platanos").expect("table not found");
                        let ts = table.timestamp_summary().expect("no timestamp summary");
                        assert_eq!(Some(1647622847000000000), ts.stats.min);
                    }
                );

                test_write_handler!(
                    [<$test_scope _ ok_precision_us>],
                    query_string = "?org=bananas&bucket=test&precision=us",
                    body = "platanos,tag1=A,tag2=B val=42i 1647622847000000".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(summary())],
                    want_result = Ok(_),
                    want_dml_calls = [MockDmlHandlerCall::Write{namespace, namespace_id, write_input}] => {
                        assert_eq!(namespace, EXPECTED_NAMESPACE);
                        assert_eq!(*namespace_id, NAMESPACE_ID);

                        let table = write_input.get("platanos").expect("table not found");
                        let ts = table.timestamp_summary().expect("no timestamp summary");
                        assert_eq!(Some(1647622847000000000), ts.stats.min);
                    }
                );

                test_write_handler!(
                    [<$test_scope _ ok_precision_ns>],
                    query_string = "?org=bananas&bucket=test&precision=ns",
                    body = "platanos,tag1=A,tag2=B val=42i 1647622847000000000".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(summary())],
                    want_result = Ok(_),
                    want_dml_calls = [MockDmlHandlerCall::Write{namespace, namespace_id, write_input}] => {
                        assert_eq!(namespace, EXPECTED_NAMESPACE);
                        assert_eq!(*namespace_id, NAMESPACE_ID);

                        let table = write_input.get("platanos").expect("table not found");
                        let ts = table.timestamp_summary().expect("no timestamp summary");
                        assert_eq!(Some(1647622847000000000), ts.stats.min);
                    }
                );

                test_write_handler!(
                    [<$test_scope _ precision_overflow>],
                    // SECONDS, so multiplies the provided timestamp by 1,000,000,000
                    query_string = "?org=bananas&bucket=test&precision=s",
                    body = "platanos,tag1=A,tag2=B val=42i 1647622847000000000".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(summary())],
                    want_result = Err(Error::ParseLineProtocol(_)),
                    want_dml_calls = []
                );

                test_write_handler!(
                    [<$test_scope _ no_query_params>],
                    query_string = "",
                    body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(summary())],
                    want_result = Err(Error::InvalidOrgBucket(OrgBucketError::NotSpecified)),
                    want_dml_calls = [] // None
                );

                test_write_handler!(
                    [<$test_scope _ no_org_bucket>],
                    query_string = "?",
                    body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(summary())],
                    want_result = Err(Error::InvalidOrgBucket(OrgBucketError::DecodeFail(_))),
                    want_dml_calls = [] // None
                );

                test_write_handler!(
                    [<$test_scope _ empty_org_bucket>],
                    query_string = "?org=&bucket=",
                    body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(summary())],
                    want_result = Err(Error::InvalidOrgBucket(OrgBucketError::NotSpecified)),
                    want_dml_calls = [] // None
                );

                test_write_handler!(
                    [<$test_scope _ invalid_org_bucket>],
                    query_string = format!("?org=test&bucket={}", "A".repeat(1000)),
                    body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(summary())],
                    want_result = Err(Error::InvalidOrgBucket(OrgBucketError::MappingFail(_))),
                    want_dml_calls = [] // None
                );

                test_write_handler!(
                    [<$test_scope _ invalid_line_protocol>],
                    query_string = "?org=bananas&bucket=test",
                    body = "not line protocol".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(summary())],
                    want_result = Err(Error::ParseLineProtocol(_)),
                    want_dml_calls = [] // None
                );

                test_write_handler!(
                    [<$test_scope _ non_utf8_body>],
                    query_string = "?org=bananas&bucket=test",
                    body = vec![0xc3, 0x28],
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(summary())],
                    want_result = Err(Error::NonUtf8Body(_)),
                    want_dml_calls = [] // None
                );

                test_write_handler!(
                    [<$test_scope _ max_request_size_truncation>],
                    query_string = "?org=bananas&bucket=test",
                    body = {
                        // Generate a LP string in the form of:
                        //
                        //  bananas,A=AAAAAAAAAA(repeated)... B=42
                        //                                  ^
                        //                                  |
                        //                         MAX_BYTES boundary
                        //
                        // So that reading MAX_BYTES number of bytes produces the string:
                        //
                        //  bananas,A=AAAAAAAAAA(repeated)...
                        //
                        // Effectively trimming off the " B=42" suffix.
                        let body = "bananas,A=";
                        iter::once(body)
                            .chain(iter::repeat("A").take(MAX_BYTES - body.len()))
                            .chain(iter::once(" B=42\n"))
                            .flat_map(|s| s.bytes())
                            .collect::<Vec<u8>>()
                    },
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(summary())],
                    want_result = Err(Error::RequestSizeExceeded(_)),
                    want_dml_calls = [] // None
                );

                test_write_handler!(
                    [<$test_scope _ db_not_found>],
                    query_string = "?org=bananas&bucket=test",
                    body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Err(DmlError::NamespaceNotFound(EXPECTED_NAMESPACE.to_string()))],
                    want_result = Err(Error::DmlHandler(DmlError::NamespaceNotFound(_))),
                    want_dml_calls = [MockDmlHandlerCall::Write{namespace, ..}] => {
                        assert_eq!(namespace, EXPECTED_NAMESPACE);
                    }
                );

                test_write_handler!(
                    [<$test_scope _ dml_handler_error>],
                    query_string = "?org=bananas&bucket=test",
                    body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Err(DmlError::Internal("💣".into()))],
                    want_result = Err(Error::DmlHandler(DmlError::Internal(_))),
                    want_dml_calls = [MockDmlHandlerCall::Write{namespace, ..}] => {
                        assert_eq!(namespace, EXPECTED_NAMESPACE);
                    }
                );

                test_write_handler!(
                    [<$test_scope _ field_upsert_within_batch>],
                    query_string = "?org=bananas&bucket=test",
                    body = "test field=1u 100\ntest field=2u 100".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(summary())],
                    want_result = Ok(_),
                    want_dml_calls = [MockDmlHandlerCall::Write{namespace, namespace_id, write_input}] => {
                        assert_eq!(namespace, EXPECTED_NAMESPACE);
                        assert_eq!(*namespace_id, NAMESPACE_ID);
                        let table = write_input.get("test").expect("table not in write");
                        let col = table.column("field").expect("column missing");
                        assert_matches!(col.data(), ColumnData::U64(data, _) => {
                            // Ensure both values are recorded, in the correct order.
                            assert_eq!(data.as_slice(), [1, 2]);
                        });
                    }
                );

                test_write_handler!(
                    [<$test_scope _ column_named_time>],
                    query_string = "?org=bananas&bucket=test",
                    body = "test field=1u,time=42u 100".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [],
                    want_result = Err(_),
                    want_dml_calls = []
                );

                test_delete_handler!(
                    [<$test_scope _ ok>],
                    query_string = "?org=bananas&bucket=test",
                    body = r#"{"start":"2021-04-01T14:00:00Z","stop":"2021-04-02T14:00:00Z", "predicate":"_measurement=its_a_table and location=Boston"}"#.as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(())],
                    want_result = Ok(_),
                    want_dml_calls = [MockDmlHandlerCall::Delete{namespace, namespace_id, table, predicate}] => {
                        assert_eq!(table, "its_a_table");
                        assert_eq!(namespace, EXPECTED_NAMESPACE);
                        assert_eq!(*namespace_id, NAMESPACE_ID);
                        assert!(!predicate.exprs.is_empty());
                    }
                );

                test_delete_handler!(
                    [<$test_scope _ invalid_delete_body>],
                    query_string = "?org=bananas&bucket=test",
                    body = r#"{wat}"#.as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [],
                    want_result = Err(Error::ParseHttpDelete(_)),
                    want_dml_calls = []
                );

                test_delete_handler!(
                    [<$test_scope _ no_query_params>],
                    query_string = "",
                    body = "".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(())],
                    want_result = Err(Error::InvalidOrgBucket(OrgBucketError::NotSpecified)),
                    want_dml_calls = [] // None
                );

                test_delete_handler!(
                    [<$test_scope _ no_org_bucket>],
                    query_string = "?",
                    body = "".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(())],
                    want_result = Err(Error::InvalidOrgBucket(OrgBucketError::DecodeFail(_))),
                    want_dml_calls = [] // None
                );

                test_delete_handler!(
                    [<$test_scope _ empty_org_bucket>],
                    query_string = "?org=&bucket=",
                    body = "".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(())],
                    want_result = Err(Error::InvalidOrgBucket(OrgBucketError::NotSpecified)),
                    want_dml_calls = [] // None
                );

                test_delete_handler!(
                    [<$test_scope _ invalid_org_bucket>],
                    query_string = format!("?org=test&bucket={}", "A".repeat(1000)),
                    body = "".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(())],
                    want_result = Err(Error::InvalidOrgBucket(OrgBucketError::MappingFail(_))),
                    want_dml_calls = [] // None
                );

                test_delete_handler!(
                    [<$test_scope _ non_utf8_body>],
                    query_string = "?org=bananas&bucket=test",
                    body = vec![0xc3, 0x28],
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Ok(())],
                    want_result = Err(Error::NonUtf8Body(_)),
                    want_dml_calls = [] // None
                );

                test_delete_handler!(
                    [<$test_scope _ db_not_found>],
                    query_string = "?org=bananas&bucket=test",
                    body = r#"{"start":"2021-04-01T14:00:00Z","stop":"2021-04-02T14:00:00Z", "predicate":"_measurement=its_a_table and location=Boston"}"#.as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Err(DmlError::NamespaceNotFound(EXPECTED_NAMESPACE.to_string()))],
                    want_result = Err(Error::DmlHandler(DmlError::NamespaceNotFound(_))),
                    want_dml_calls = [MockDmlHandlerCall::Delete{namespace, namespace_id, table, predicate}] => {
                        assert_eq!(table, "its_a_table");
                        assert_eq!(namespace, EXPECTED_NAMESPACE);
                        assert_eq!(*namespace_id, NAMESPACE_ID);
                        assert!(!predicate.exprs.is_empty());
                    }
                );

                test_delete_handler!(
                    [<$test_scope _ dml_handler_error>],
                    query_string = "?org=bananas&bucket=test",
                    body = r#"{"start":"2021-04-01T14:00:00Z","stop":"2021-04-02T14:00:00Z", "predicate":"_measurement=its_a_table and location=Boston"}"#.as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_handler = [Err(DmlError::Internal("💣".into()))],
                    want_result = Err(Error::DmlHandler(DmlError::Internal(_))),
                    want_dml_calls = [MockDmlHandlerCall::Delete{namespace, namespace_id, table, predicate}] => {
                        assert_eq!(table, "its_a_table");
                        assert_eq!(namespace, EXPECTED_NAMESPACE);
                        assert_eq!(*namespace_id, NAMESPACE_ID);
                        assert!(!predicate.exprs.is_empty());
                    }
                );

                test_http_handler!(
                    [<$test_scope _ not_found>],
                    uri = "https://bananas.example/wat",
                    body = "".as_bytes(),
                    dml_info_handler = $dml_info_handler,
                    dml_write_handler = [],
                    dml_delete_handler = [],
                    want_result = Err(Error::NoHandler),
                    want_dml_calls = []
                );

                // https://github.com/influxdata/influxdb_iox/issues/4326
                mod [<issue4326 _ $test_scope>] {
                    use super::*;

                    test_write_handler!(
                        [<$test_scope _ duplicate_fields_same_value>],
                        query_string = "?org=bananas&bucket=test",
                        body = "whydo InputPower=300i,InputPower=300i".as_bytes(),
                        dml_info_handler = $dml_info_handler,
                        dml_handler = [Ok(summary())],
                        want_result = Ok(_),
                        want_dml_calls = [MockDmlHandlerCall::Write{namespace, write_input, ..}] => {
                            assert_eq!(namespace, EXPECTED_NAMESPACE);
                            let table = write_input.get("whydo").expect("table not in write");
                            let col = table.column("InputPower").expect("column missing");
                            assert_matches!(col.data(), ColumnData::I64(data, _) => {
                                // Ensure the duplicate values are coalesced.
                                assert_eq!(data.as_slice(), [300]);
                            });
                        }
                    );

                    test_write_handler!(
                        [<$test_scope _ duplicate_fields_different_value>],
                        query_string = "?org=bananas&bucket=test",
                        body = "whydo InputPower=300i,InputPower=42i".as_bytes(),
                        dml_info_handler = $dml_info_handler,
                        dml_handler = [Ok(summary())],
                        want_result = Ok(_),
                        want_dml_calls = [MockDmlHandlerCall::Write{namespace, write_input, ..}] => {
                            assert_eq!(namespace, EXPECTED_NAMESPACE);
                            let table = write_input.get("whydo").expect("table not in write");
                            let col = table.column("InputPower").expect("column missing");
                            assert_matches!(col.data(), ColumnData::I64(data, _) => {
                                // Last value wins
                                assert_eq!(data.as_slice(), [42]);
                            });
                        }
                    );

                    test_write_handler!(
                        [<$test_scope _ duplicate_fields_different_type>],
                        query_string = "?org=bananas&bucket=test",
                        body = "whydo InputPower=300i,InputPower=4.2".as_bytes(),
                        dml_info_handler = $dml_info_handler,
                        dml_handler = [],
                        want_result = Err(Error::ParseLineProtocol(mutable_batch_lp::Error::Write {
                            source: LineWriteError::ConflictedFieldTypes { .. },
                            ..
                        })),
                        want_dml_calls = []
                    );

                    test_write_handler!(
                        [<$test_scope _ duplicate_tags_same_value>],
                        query_string = "?org=bananas&bucket=test",
                        body = "whydo,InputPower=300i,InputPower=300i field=42i".as_bytes(),
                        dml_info_handler = $dml_info_handler,
                        dml_handler = [],
                        want_result = Err(Error::ParseLineProtocol(mutable_batch_lp::Error::Write {
                            source: LineWriteError::DuplicateTag { .. },
                            ..
                        })),
                        want_dml_calls = []
                    );

                    test_write_handler!(
                        [<$test_scope _ duplicate_tags_different_value>],
                        query_string = "?org=bananas&bucket=test",
                        body = "whydo,InputPower=300i,InputPower=42i field=42i".as_bytes(),
                        dml_info_handler = $dml_info_handler,
                        dml_handler = [],
                        want_result = Err(Error::ParseLineProtocol(mutable_batch_lp::Error::Write {
                            source: LineWriteError::DuplicateTag { .. },
                            ..
                        })),
                        want_dml_calls = []
                    );

                    test_write_handler!(
                        [<$test_scope _ duplicate_tags_different_type>],
                        query_string = "?org=bananas&bucket=test",
                        body = "whydo,InputPower=300i,InputPower=4.2 field=42i".as_bytes(),
                        dml_info_handler = $dml_info_handler,
                        dml_handler = [],
                        want_result = Err(Error::ParseLineProtocol(mutable_batch_lp::Error::Write {
                            source: LineWriteError::DuplicateTag { .. },
                            ..
                        })),
                        want_dml_calls = []
                    );

                    test_write_handler!(
                        [<$test_scope _ duplicate_is_tag_and_field>],
                        query_string = "?org=bananas&bucket=test",
                        body = "whydo,InputPower=300i InputPower=300i".as_bytes(),
                        dml_info_handler = $dml_info_handler,
                        dml_handler = [],
                        want_result = Err(Error::ParseLineProtocol(mutable_batch_lp::Error::Write {
                            source: LineWriteError::MutableBatch {
                                source: mutable_batch::writer::Error::TypeMismatch { .. }
                            },
                            ..
                        })),
                        want_dml_calls = []
                    );

                    test_write_handler!(
                        [<$test_scope _ duplicate_is_tag_and_field_different_types>],
                        query_string = "?org=bananas&bucket=test",
                        body = "whydo,InputPower=300i InputPower=30.0".as_bytes(),
                        dml_info_handler = $dml_info_handler,
                        dml_handler = [],
                        want_result = Err(Error::ParseLineProtocol(mutable_batch_lp::Error::Write {
                            source: LineWriteError::MutableBatch {
                                source: mutable_batch::writer::Error::TypeMismatch { .. }
                            },
                            ..
                        })),
                        want_dml_calls = []
                    );
                }
            }
        };
    }

    mod mt {
        use super::*;
        use crate::server::http::mt::MultiTenantRequestParser;
        static EXPECTED_NAMESPACE: &str = "bananas_test";
        run_v2_test_in_env!(mt, Box::<MultiTenantRequestParser>::default());
    }

    mod cst {
        use super::*;
        use crate::server::http::cst::SingleTenantRequestParser;
        static EXPECTED_NAMESPACE: &str = "test";
        run_v2_test_in_env!(cst, Box::<SingleTenantRequestParser>::default());
    }
}
