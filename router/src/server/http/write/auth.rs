//! Authorization of HTTP requests using the authz service client.

use hyper::{header::HeaderValue, Body, Request};
use server_util::authorization::AuthorizationHeaderExtension;
use std::sync::Arc;

use authz::{self, Action, Authorizer, Error, Permission, Resource};
use data_types::NamespaceName;

fn extract_token<'a>(
    header_value: &'a HeaderValue,
    optional_token: Option<&'a String>,
) -> Option<&'a [u8]> {
    let b = header_value.as_bytes();
    if let Some(token) = b.strip_prefix(b"Token ") {
        (!token.is_empty()).then_some(token)
    } else if let Some(token) = b.strip_prefix(b"Bearer ") {
        (!token.is_empty()).then_some(token)
    } else if let Some(creds) = b.strip_prefix(b"Basic ") {
        let token = creds
            .iter()
            .position(|b| &[*b] == b":")
            .map(|idx| creds[idx + 1..].as_ref());
        token.and_then(|t| (!t.is_empty()).then_some(t))
    } else {
        optional_token.map(|s| s.as_bytes())
    }
}

pub(crate) async fn authorize(
    auth_service: &Arc<dyn Authorizer>,
    req: &Request<Body>,
    namespace: &NamespaceName<'_>,
    query_param_token: Option<String>,
) -> Result<(), Error> {
    let token = req
        .extensions()
        .get::<AuthorizationHeaderExtension>()
        .and_then(|v| v.as_ref())
        .and_then(|v| extract_token(v, query_param_token.as_ref()));

    let perms = [Permission::ResourceAction(
        Resource::Database(namespace.to_string()),
        Action::Write,
    )];

    auth_service.require_any_permission(token, &perms).await?;
    Ok(())
}

#[cfg(test)]
pub mod mock {
    use super::*;
    use async_trait::async_trait;

    #[derive(Debug, Default)]
    pub struct MockAuthorizer {}

    #[async_trait]
    impl Authorizer for MockAuthorizer {
        async fn permissions(
            &self,
            token: Option<&[u8]>,
            perms: &[Permission],
        ) -> Result<Vec<Permission>, authz::Error> {
            match token {
                Some(b"GOOD") => Ok(perms.to_vec()),
                Some(b"UGLY") => Err(authz::Error::verification("test", "test error")),
                Some(_) => Ok(vec![]),
                None => Err(authz::Error::NoToken),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{mock::*, *};
    use crate::{
        dml_handlers::mock::{MockDmlHandler, MockDmlHandlerCall},
        namespace_resolver::mock::MockNamespaceResolver,
        server::http::{
            self,
            write::single_tenant::{SingleTenantExtractError, SingleTenantRequestUnifier},
            HttpDelegate,
        },
    };
    use assert_matches::assert_matches;
    use data_types::NamespaceId;
    use hyper::header::HeaderValue;

    const MAX_BYTES: usize = 1024;

    #[tokio::test]
    async fn test_authz_service_integration() {
        static NAMESPACE_NAME: &str = "test";
        let mock_namespace_resolver =
            MockNamespaceResolver::default().with_mapping(NAMESPACE_NAME, NamespaceId::new(42));

        let dml_handler = Arc::new(
            MockDmlHandler::default()
                .with_write_return([Ok(())])
                .with_delete_return([]),
        );
        let metrics = Arc::new(metric::Registry::default());
        let authz = Arc::new(MockAuthorizer::default());
        let delegate = HttpDelegate::new(
            MAX_BYTES,
            1,
            mock_namespace_resolver,
            Arc::clone(&dml_handler),
            &metrics,
            Box::new(SingleTenantRequestUnifier::new(authz, &metrics)),
        );

        let request = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .extension(AuthorizationHeaderExtension::new(Some(
                HeaderValue::from_str("Token GOOD").unwrap(),
            )))
            .body(Body::from("platanos,tag1=A,tag2=B val=42i 123456"))
            .unwrap();

        let got = delegate.route(request).await;
        assert_matches!(got, Ok(_));

        let request = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .extension(AuthorizationHeaderExtension::new(Some(
                HeaderValue::from_str("Token BAD").unwrap(),
            )))
            .body(Body::from(""))
            .unwrap();

        let got = delegate.route(request).await;
        assert_matches!(
            got,
            Err(http::Error::SingleTenantError(
                SingleTenantExtractError::Authorizer(authz::Error::Forbidden)
            ))
        );

        let request = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .body(Body::from(""))
            .unwrap();

        let got = delegate.route(request).await;
        assert_matches!(
            got,
            Err(http::Error::SingleTenantError(
                SingleTenantExtractError::Authorizer(authz::Error::NoToken)
            ))
        );

        let request = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .extension(AuthorizationHeaderExtension::new(Some(
                HeaderValue::from_str("Token UGLY").unwrap(),
            )))
            .body(Body::from(""))
            .unwrap();

        let got = delegate.route(request).await;
        assert_matches!(
            got,
            Err(http::Error::SingleTenantError(
                SingleTenantExtractError::Authorizer(authz::Error::Verification { .. })
            ))
        );

        let calls = dml_handler.calls();
        assert_matches!(calls.as_slice(), [MockDmlHandlerCall::Write{namespace, ..}] => {
            assert_eq!(namespace, NAMESPACE_NAME);
        })
    }

    macro_rules! test_authorize {
        (
            $name:ident,
            header_value = $header_value:expr,       // If present, set as header
            query_param_token = $query_token:expr,   // Optional token provided as ?q=<token>
            want = $($want:tt)+                      // A pattern match for assert_matches!
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_authorize_ $name>]() {
                    let authz: Arc<dyn Authorizer> = Arc::new(MockAuthorizer::default());
                    let namespace = NamespaceName::new("test").unwrap();

                    let request = Request::builder()
                        .uri(format!("https://any.com/ignored"))
                        .method("POST")
                        .extension(AuthorizationHeaderExtension::new(Some(
                            HeaderValue::from_str($header_value).unwrap(),
                        )))
                        .body(Body::from(""))
                        .unwrap();

                    let got = authorize(&authz, &request, &namespace, $query_token).await;
                    assert_matches!(got, $($want)+);
                }
            }
        };
    }

    test_authorize!(
        token_header_ok,
        header_value = "Token GOOD",
        query_param_token = Some("ignore".to_string()),
        want = Ok(())
    );

    test_authorize!(
        token_header_rejected,
        header_value = "Token UGLY",
        query_param_token = Some("ignore".to_string()),
        want = Err(authz::Error::Verification { .. })
    );

    test_authorize!(
        token_header_forbidden,
        header_value = "Token BAD",
        query_param_token = Some("ignore".to_string()),
        want = Err(authz::Error::Forbidden)
    );

    test_authorize!(
        token_header_missing,
        header_value = "Token ",
        query_param_token = None,
        want = Err(authz::Error::NoToken)
    );

    test_authorize!(
        token_header_missing_whitespace,
        header_value = "Token",
        query_param_token = None,
        want = Err(authz::Error::NoToken)
    );

    test_authorize!(
        token_header_missing_whitespace_match_next,
        header_value = "Token",
        query_param_token = Some("GOOD".to_string()),
        want = Ok(())
    );

    test_authorize!(
        bearer_header_ok,
        header_value = "Bearer GOOD",
        query_param_token = Some("ignore".to_string()),
        want = Ok(())
    );

    test_authorize!(
        bearer_header_missing,
        header_value = "Bearer ",
        query_param_token = None,
        want = Err(authz::Error::NoToken)
    );

    test_authorize!(
        basic_header_ok,
        header_value = "Basic ignore:GOOD",
        query_param_token = Some("ignore".to_string()),
        want = Ok(())
    );

    test_authorize!(
        basic_header_missing,
        header_value = "Basic ",
        query_param_token = Some("ignore".to_string()),
        want = Err(authz::Error::NoToken)
    );

    test_authorize!(
        basic_header_missing_part,
        header_value = "Basic ignore:",
        query_param_token = Some("ignore".to_string()),
        want = Err(authz::Error::NoToken)
    );

    test_authorize!(
        basic_header_rejected,
        header_value = "Basic ignore:UGLY",
        query_param_token = Some("ignore".to_string()),
        want = Err(authz::Error::Verification { .. })
    );

    test_authorize!(
        basic_header_forbidden,
        header_value = "Basic ignore:BAD",
        query_param_token = Some("ignore".to_string()),
        want = Err(authz::Error::Forbidden)
    );

    test_authorize!(
        query_param_token_ok,
        header_value = "",
        query_param_token = Some("GOOD".to_string()),
        want = Ok(())
    );

    test_authorize!(
        query_param_token_rejected,
        header_value = "",
        query_param_token = Some("UGLY".to_string()),
        want = Err(authz::Error::Verification { .. })
    );

    test_authorize!(
        query_param_token_forbidden,
        header_value = "",
        query_param_token = Some("BAD".to_string()),
        want = Err(authz::Error::Forbidden)
    );

    test_authorize!(
        everything_missing,
        header_value = "",
        query_param_token = None,
        want = Err(authz::Error::NoToken)
    );
}
