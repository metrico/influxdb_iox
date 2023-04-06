//! Authorization of HTTP requests using the authz service client.

use hyper::{Body, Request};
use server_util::authorization::AuthorizationHeaderExtension;
use std::sync::Arc;

use authz::{Action, Authorizer, Error, Permission, Resource};
use data_types::NamespaceName;

pub(crate) async fn authorize(
    auth_service: &Arc<dyn Authorizer>,
    req: &Request<Body>,
    namespace: &NamespaceName<'_>,
) -> Result<(), Error> {
    let token = req
        .extensions()
        .get::<AuthorizationHeaderExtension>()
        .and_then(|v| v.as_ref())
        .and_then(|v| {
            let s = v.as_ref();
            if s.len() < b"Token ".len() {
                None
            } else {
                match s.split_at(b"Token ".len()) {
                    (b"Token ", token) => Some(token),
                    _ => None,
                }
            }
        });
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

    #[derive(Debug)]
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
            write::single_tenant::{SingleTenantExtractError, SingleTenantRequestParser},
            Error, HttpDelegate,
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
        let authz = Arc::new(MockAuthorizer {});
        let delegate = HttpDelegate::new(
            MAX_BYTES,
            1,
            mock_namespace_resolver,
            Arc::clone(&dml_handler),
            &metrics,
            Box::new(SingleTenantRequestParser::new(authz)),
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
            Err(Error::SingleTenantError(
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
            Err(Error::SingleTenantError(
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
            Err(Error::SingleTenantError(
                SingleTenantExtractError::Authorizer(authz::Error::Verification { .. })
            ))
        );

        let calls = dml_handler.calls();
        assert_matches!(calls.as_slice(), [MockDmlHandlerCall::Write{namespace, ..}] => {
            assert_eq!(namespace, NAMESPACE_NAME);
        })
    }
}
