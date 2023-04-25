//! Metrics decorator for durations

use iox_time::{SystemProvider, TimeProvider};
use std::future::Future;

use crate::{DurationHistogram, Metric, Registry};

/// An instrumentation decorator for recording [`DurationHistogram`] on a given trait.
///
/// Use by implementing the target trait in [`DurationHistogramDecorator`].
/// ```compile_fail
/// use async_trait::async_trait;
///
/// #[async_trait]
/// impl<T> AnyTrait for DurationHistogramDecorator<T>
/// where
///     T: AnyTrait
/// {
///     async fn any_method(&self, any_args: AnyType) -> Result<O,E> { <body> };
/// }
/// ```
///
/// Then utilize the [`DurationHistogramDecorator::record_duration()`](Self::record_duration).
#[derive(Debug)]
pub struct DurationHistogramDecorator<T, P = SystemProvider> {
    inner: T,
    time_provider: P,

    success: DurationHistogram,
    error: DurationHistogram,
}

impl<T> DurationHistogramDecorator<T> {
    /// Wrap a new [`DurationHistogramDecorator`] over `T` exposing metrics.
    pub fn new(
        title: &'static str,
        description: &'static str,
        registry: &Registry,
        inner: T,
    ) -> Self {
        let metric: Metric<DurationHistogram> = registry.register_metric(title, description);

        let success = metric.recorder(&[("result", "success")]);
        let error = metric.recorder(&[("result", "error")]);

        Self {
            inner,
            time_provider: Default::default(),
            success,
            error,
        }
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Method to call when implementing a given trait for [`DurationHistogramDecorator`],
    /// to have the duration metric be recorded for a given closure.
    ///
    /// Accepts an asynchronous FnOnce. Example:
    /// ```compile_fail
    /// use async_trait::async_trait;
    ///
    /// #[async_trait]
    /// impl<T> AnyTrait for DurationHistogramDecorator<T>
    /// where
    ///     T: AnyTrait
    /// {
    ///     async fn any_method(&self, any_args: AnyType) -> Result<O,E> {
    ///         self.record_duration(|| async move { self.inner().any_method(any_args).await }).await
    ///     }
    /// }
    /// ```
    pub async fn record_duration<F, Fut, E, O>(&self, func: F) -> Result<O, E>
    where
        O: Sized,
        E: ToString + Sized,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<O, E>> + Send,
    {
        let t = self.time_provider.now();
        let res = func().await;

        // Avoid exploding if time goes backwards - simply drop the measurement
        // if it happens.
        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => self.success.record(delta),
                Err(_) => self.error.record(delta),
            };
        }

        res
    }
}
