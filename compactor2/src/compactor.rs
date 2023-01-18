//! Main compactor entry point.
use std::sync::Arc;

use futures::{
    future::{BoxFuture, Shared},
    FutureExt, TryFutureExt,
};
use observability_deps::tracing::warn;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::config::Config;

/// A [`JoinHandle`] that can be cloned
type SharedJoinHandle = Shared<BoxFuture<'static, Result<(), Arc<JoinError>>>>;

/// Convert a [`JoinHandle`] into a [`SharedJoinHandle`].
fn shared_handle(handle: JoinHandle<()>) -> SharedJoinHandle {
    handle.map_err(Arc::new).boxed().shared()
}

/// Main compactor driver.
#[derive(Debug)]
pub struct Compactor2 {
    shutdown: CancellationToken,
    worker: SharedJoinHandle,
}

impl Compactor2 {
    /// Start compactor.
    pub fn start(_config: Config) -> Self {
        let shutdown = CancellationToken::new();
        let shutdown_captured = shutdown.clone();
        let worker = tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_captured.cancelled() => {}
            }
        });
        let worker = shared_handle(worker);

        Self { shutdown, worker }
    }

    /// Trigger shutdown. You should [join](Self::join) afterwards.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Wait until the compactor finishes.
    pub async fn join(&self) -> Result<(), Arc<JoinError>> {
        self.worker.clone().await
    }
}

impl Drop for Compactor2 {
    fn drop(&mut self) {
        if self.worker.clone().now_or_never().is_none() {
            warn!("Compactor was not shut down properly");
        }
    }
}