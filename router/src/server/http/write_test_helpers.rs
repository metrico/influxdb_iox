use data_types::NamespaceId;
use metric::{Attributes, Metric, U64Counter};
use write_summary::WriteSummary;

pub(crate) const MAX_BYTES: usize = 1024;
pub(crate) const NAMESPACE_ID: NamespaceId = NamespaceId::new(42);

pub(crate) fn summary() -> WriteSummary {
    WriteSummary::default()
}

pub(crate) fn assert_metric_hit(
    metrics: &metric::Registry,
    name: &'static str,
    value: Option<u64>,
) {
    let counter = metrics
        .get_instrument::<Metric<U64Counter>>(name)
        .expect("failed to read metric")
        .get_observer(&Attributes::from(&[]))
        .expect("failed to get observer")
        .fetch();

    if let Some(want) = value {
        assert_eq!(want, counter, "metric does not have expected value");
    } else {
        assert!(counter > 0, "metric {name} did not record any values");
    }
}

#[macro_export]
/// Generate two HTTP handler tests - one for a plain request and one with a
/// gzip-encoded body (and appropriate header), asserting the handler return
/// value & write op.
macro_rules! test_http_handler {
    (
        $name:ident,
        uri = $uri:expr,                                // Request URI
        body = $body:expr,                              // Request body content
        dml_info_handler = $dml_info_handler:ident,
        dml_write_handler = $dml_write_handler:expr,    // DML write handler response (if called)
        dml_delete_handler = $dml_delete_handler:expr,  // DML delete handler response (if called)
        want_result = $want_result:pat,                 // Expected handler return value (as pattern)
        want_dml_calls = $($want_dml_calls:tt )+        // assert_matches slice pattern for expected DML calls
    ) => {
        // Generate the two test cases by feed the same inputs, but varying
        // the encoding.
        $crate::test_http_handler!(
            $name,
            encoding=plain,
            uri = $uri,
            body = $body,
            dml_info_handler = $dml_info_handler,
            dml_write_handler = $dml_write_handler,
            dml_delete_handler = $dml_delete_handler,
            want_result = $want_result,
            want_dml_calls = $($want_dml_calls)+
        );
        $crate::test_http_handler!(
            $name,
            encoding=gzip,
            uri = $uri,
            body = $body,
            dml_info_handler = $dml_info_handler,
            dml_write_handler = $dml_write_handler,
            dml_delete_handler = $dml_delete_handler,
            want_result = $want_result,
            want_dml_calls = $($want_dml_calls)+
        );
    };
    // Actual test body generator.
    (
        $name:ident,
        encoding = $encoding:tt,
        uri = $uri:expr,
        body = $body:expr,
        dml_info_handler = $dml_info_handler:ident,
        dml_write_handler = $dml_write_handler:expr,
        dml_delete_handler = $dml_delete_handler:expr,
        want_result = $want_result:pat,
        want_dml_calls = $($want_dml_calls:tt )+
    ) => {
        paste::paste! {
            mod [<test_mod_ test_http_handler_ $name _ $encoding>] {
                use super::*;
                use $crate::{
                    test_http_handler,
                    dml_handlers::{
                        mock::{MockDmlHandler},
                    },
                    namespace_resolver::{mock::MockNamespaceResolver},
                    server::http::{
                        self,
                        HttpDelegate,
                        WriteInfoExtractor,
                        write_test_helpers::assert_metric_hit,
                    },
                };
                use assert_matches::assert_matches;
                #[allow(unused_imports)]
                use flate2::{write::GzEncoder, Compression};
                #[allow(unused_imports)]
                use hyper::{header::{CONTENT_ENCODING, HeaderValue}, Body, Request, StatusCode};
                #[allow(unused_imports)]
                use std::{io::Write, sync::Arc};

                #[tokio::test]
                async fn [<test_http_handler_ $name _ $encoding>]() {
                    let body = $body;

                    // Optionally generate a fragment of code to encode the body
                    let body = test_http_handler!(encoding=$encoding, body);

                    #[allow(unused_mut)]
                    let mut request = Request::builder()
                        .uri($uri)
                        .method("POST")
                        .body(Body::from(body))
                        .unwrap();

                    // Optionally modify request to account for the desired
                    // encoding
                    test_http_handler!(encoding_header=$encoding, request);

                    // add all the mappings used in the tests.
                    let mock_namespace_resolver = MockNamespaceResolver::default()
                        .with_mapping("bananas_test", NAMESPACE_ID)
                        .with_mapping("test", NAMESPACE_ID)
                        .with_mapping("database", NAMESPACE_ID)
                        .with_mapping("database/myrp", NAMESPACE_ID);
                    let dml_handler = Arc::new(MockDmlHandler::default()
                        .with_write_return($dml_write_handler)
                        .with_delete_return($dml_delete_handler)
                    );
                    let metrics = Arc::new(metric::Registry::default());
                    let dml_info_extractor: &'static dyn WriteInfoExtractor = &http::[<$dml_info_handler>];

                    let delegate = HttpDelegate::new(
                        MAX_BYTES,
                        100,
                        mock_namespace_resolver,
                        Arc::clone(&dml_handler),
                        None,
                        &metrics,
                        dml_info_extractor,
                    );

                    let got = delegate.route(request).await;
                    assert_matches!(got, $want_result);

                    // All successful responses should have a NO_CONTENT code
                    // and metrics should be recorded.
                    if let Ok(v) = got {
                        assert_eq!(v.status(), StatusCode::NO_CONTENT);
                        if $uri.contains("/write") {
                            assert_metric_hit(&metrics, "http_write_lines", None);
                            assert_metric_hit(&metrics, "http_write_fields", None);
                            assert_metric_hit(&metrics, "http_write_tables", None);
                            assert_metric_hit(&metrics, "http_write_body_bytes", Some($body.len() as _));
                        } else {
                            assert_metric_hit(&metrics, "http_delete_body_bytes", Some($body.len() as _));
                        }
                    }

                    let calls = dml_handler.calls();
                    assert_matches!(calls.as_slice(), $($want_dml_calls)+);
                }
            }
        }
    };
    (encoding=plain, $body:ident) => {
        $body
    };
    (encoding=gzip, $body:ident) => {{
        // Apply gzip compression to the body
        let mut e = GzEncoder::new(Vec::new(), Compression::default());
        e.write_all(&$body).unwrap();
        e.finish().expect("failed to compress test body")
    }};
    (encoding_header=plain, $request:ident) => {};
    (encoding_header=gzip, $request:ident) => {{
        // Set the gzip content encoding
        $request
            .headers_mut()
            .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }};
}

#[macro_export]
/// Wrapper over test_http_handler specifically for write requests.
macro_rules! test_write_handler {
    (
        $name:ident,
        query_string = $query_string:expr,   // Request URI query string
        body = $body:expr,                   // Request body content
        dml_info_handler = $dml_info_handler:ident,
        dml_handler = $dml_handler:expr,     // DML write handler response (if called)
        want_result = $want_result:pat,
        want_dml_calls = $($want_dml_calls:tt )+
    ) => {
        $crate::test_write_handler!(
            $name,
            route_string = "/api/v2/write",
            query_string = $query_string,
            body = $body,
            dml_info_handler = $dml_info_handler,
            dml_handler = $dml_handler,
            want_result = $want_result,
            want_dml_calls = $($want_dml_calls)+
        );
    };
    (
        $name:ident,
        route_string = $route_string:expr,   // v1 versus v2 routes
        query_string = $query_string:expr,   // Request URI query string
        body = $body:expr,                   // Request body content
        dml_info_handler = $dml_info_handler:ident,
        dml_handler = $dml_handler:expr,     // DML write handler response (if called)
        want_result = $want_result:pat,
        want_dml_calls = $($want_dml_calls:tt )+
    ) => {
        paste::paste! {
            $crate::test_http_handler!(
                [<write_ $name>],
                uri = format!("https://bananas.example{}{}", $route_string, $query_string),
                body = $body,
                dml_info_handler = $dml_info_handler,
                dml_write_handler = $dml_handler,
                dml_delete_handler = [],
                want_result = $want_result,
                want_dml_calls = $($want_dml_calls)+
            );
        }
    };
}

#[macro_export]
/// Wrapper over test_http_handler specifically for delete requests.
macro_rules! test_delete_handler {
    (
        $name:ident,
        query_string = $query_string:expr,   // Request URI query string
        body = $body:expr,                   // Request body content
        dml_info_handler = $dml_info_handler:ident,
        dml_handler = $dml_handler:expr,     // DML delete handler response (if called)
        want_result = $want_result:pat,
        want_dml_calls = $($want_dml_calls:tt )+
    ) => {
        paste::paste! {
            $crate::test_http_handler!(
                [<delete_ $name>],
                uri = format!("https://bananas.example/api/v2/delete{}", $query_string),
                body = $body,
                dml_info_handler = $dml_info_handler,
                dml_write_handler = [],
                dml_delete_handler = $dml_handler,
                want_result = $want_result,
                want_dml_calls = $($want_dml_calls)+
            );
        }
    };
}
