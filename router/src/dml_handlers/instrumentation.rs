use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};
use iox_time::{SystemProvider, TimeProvider};
use metric::{DurationHistogram, Metric};
use std::sync::Arc;
use trace::{ctx::SpanContext, span::SpanRecorder};

use super::DmlHandler;

/// An instrumentation decorator recording call latencies for [`DmlHandler`] implementations.
///
/// Metrics are broken down by operation (write/delete) and result (success/error).
#[derive(Debug)]
pub struct InstrumentationDecorator<T, P = SystemProvider> {
    name: &'static str,
    inner: T,
    time_provider: P,

    write_success: DurationHistogram,
    write_error: DurationHistogram,
}

impl<T> InstrumentationDecorator<T> {
    /// Wrap a new [`InstrumentationDecorator`] over `T` exposing metrics
    /// labelled with `handler=name`.
    pub fn new(name: &'static str, registry: &metric::Registry, inner: T) -> Self {
        let write: Metric<DurationHistogram> =
            registry.register_metric("dml_handler_write_duration", "write handler call duration");

        let write_success = write.recorder(&[("handler", name), ("result", "success")]);
        let write_error = write.recorder(&[("handler", name), ("result", "error")]);

        Self {
            name,
            inner,
            time_provider: Default::default(),
            write_success,
            write_error,
        }
    }
}

#[async_trait]
impl<T> DmlHandler for InstrumentationDecorator<T>
where
    T: DmlHandler,
{
    type WriteInput = T::WriteInput;
    type WriteError = T::WriteError;
    type WriteOutput = T::WriteOutput;

    /// Call the inner `write` method and record the call latency.
    async fn write(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_schema: Arc<NamespaceSchema>,
        input: Self::WriteInput,
        span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        let t = self.time_provider.now();

        // Create a tracing span for this handler.
        let mut span_recorder =
            SpanRecorder::new(span_ctx.clone().map(|parent| parent.child(self.name)));

        let res = self
            .inner
            .write(namespace, namespace_schema, input, span_ctx)
            .await;

        // Avoid exploding if time goes backwards - simply drop the measurement
        // if it happens.
        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => {
                    span_recorder.ok("success");
                    self.write_success.record(delta)
                }
                Err(e) => {
                    span_recorder.error(e.to_string());
                    self.write_error.record(delta)
                }
            };
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use data_types::NamespaceId;
    use metric::Attributes;
    use trace::{span::SpanStatus, RingBufferTraceCollector, TraceCollector};

    use super::*;
    use crate::dml_handlers::{mock::MockDmlHandler, DmlError};

    const HANDLER_NAME: &str = "bananas";

    // Start a new `NamespaceSchema` with only the given ID; the rest of the fields are arbitrary.
    fn new_empty_namespace_schema(id: i64) -> Arc<NamespaceSchema> {
        Arc::new(NamespaceSchema {
            id: NamespaceId::new(id),
            tables: Default::default(),
            max_columns_per_table: 500,
            max_tables: 200,
            retention_period_ns: None,
            partition_template: Default::default(),
        })
    }

    fn assert_metric_hit(
        metrics: &metric::Registry,
        metric_name: &'static str,
        result: &'static str,
    ) {
        let histogram = metrics
            .get_instrument::<Metric<DurationHistogram>>(metric_name)
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[
                ("handler", HANDLER_NAME),
                ("result", result),
            ]))
            .expect("failed to get observer")
            .fetch();

        let hit_count = histogram.sample_count();
        assert!(hit_count > 0, "metric did not record any calls");
    }

    fn assert_trace(traces: Arc<dyn TraceCollector>, status: SpanStatus) {
        let traces = traces
            .as_any()
            .downcast_ref::<RingBufferTraceCollector>()
            .expect("unexpected collector impl");

        let span = traces
            .spans()
            .into_iter()
            .find(|s| s.name == HANDLER_NAME)
            .expect("tracing span not found");

        assert_eq!(
            span.status, status,
            "span status does not match expected value"
        );
    }

    #[tokio::test]
    async fn test_write_ok() {
        let ns = "platanos".try_into().unwrap();
        let handler = Arc::new(MockDmlHandler::default().with_write_return([Ok(())]));

        let metrics = Arc::new(metric::Registry::default());
        let traces: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces));

        let decorator = InstrumentationDecorator::new(HANDLER_NAME, &metrics, handler);

        decorator
            .write(&ns, new_empty_namespace_schema(42), (), Some(span))
            .await
            .expect("inner handler configured to succeed");

        assert_metric_hit(&metrics, "dml_handler_write_duration", "success");
        assert_trace(traces, SpanStatus::Ok);
    }

    #[tokio::test]
    async fn test_write_err() {
        let ns = "platanos".try_into().unwrap();
        let handler = Arc::new(
            MockDmlHandler::default()
                .with_write_return([Err(DmlError::NamespaceNotFound("nope".to_owned()))]),
        );

        let metrics = Arc::new(metric::Registry::default());
        let traces: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces));

        let decorator = InstrumentationDecorator::new(HANDLER_NAME, &metrics, handler);

        let err = decorator
            .write(&ns, new_empty_namespace_schema(42), (), Some(span))
            .await
            .expect_err("inner handler configured to fail");

        assert_matches!(err, DmlError::NamespaceNotFound(_));

        assert_metric_hit(&metrics, "dml_handler_write_duration", "error");
        assert_trace(traces, SpanStatus::Err);
    }
}
