use backoff::*;
use futures::prelude::*;
use futures::stream::{BoxStream, Take};
use object_store::{DynObjectStore, ObjectMeta};
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::sleep};
use crate::objectstore::checker;

/// Object store implementations will generally list all objects in the bucket/prefix. This limits
/// the total items pulled (assuming lazy streams) to limit impact on the catalog.
/// Consider increasing this is throughput is an issue, or shortening the loop sleep interval.
/// Listing will list all files, including those not to be deleted, which may be a very large number.
const MAX_ITEMS_PROCESSED_PER_LOOP: usize = 1_000;

/// perform a object store list, limiting to 1000 files per loop iteration, waiting sleep interval
/// per loop.
pub(crate) async fn perform(
    object_store: Arc<DynObjectStore>,
    checker: mpsc::Sender<ObjectMeta>,
    sleep_interval_minutes: u64,
) -> Result<()> {

    let mut items = get_items(object_store.clone()).await;
    loop {
        // relist and sleep on an error to allow time for transient errors to dissipate
        // todo(pjb): react differently to different errors
        match process_item_list(&mut items.take(MAX_ITEMS_PROCESSED_PER_LOOP), &checker).await {
            Ok(_) => {
                if let None = items.peekable().peak().await {
                    // finished the first list
                    items = get_items(object_store.clone()).await;
                }
                // else: reuse the same stream with pagination/continuation to avoid relisting the
                // same first MAX files over and over again.
            }
            Err(e) => {
                warn!("error processing items from object store, continuing: {e}");
                items = get_items(object_store.clone()).await;
            }
        };
        sleep(Duration::from_secs(60 * sleep_interval_minutes)).await;
    }
}

async fn get_items(
    object_store: Arc<DynObjectStore>,
) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
    // backoff retry to avoid issues with immediately polling the object store at startup
    let mut backoff = Backoff::new(&BackoffConfig::default());

    backoff
        .retry_all_errors("list_os_files", move || object_store.list(None))
        .await
        .expect("backoff retries forever")
}

// returns a result of true meaning the list has more items but we stopped at the limit. Reuse the
// stream on the next iteration instead of re-listing, which might keep listing the same items over
// and over.
async fn process_item_list(
    items: &mut Take<BoxStream<'_, object_store::Result<ObjectMeta>>>,
    checker: &mpsc::Sender<ObjectMeta>,
) -> Result<()> {
    while let Some(item) = items.next().await {
        let item = item.context(MalformedSnafu)?;
        debug!(location = %item.location, "Object store item");
        checker.send(item).await?;
    }
    debug!("end of object store item list");
    Ok(())
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("The prefix could not be listed"))]
    Listing { source: object_store::Error },

    #[snafu(display("The object could not be listed"))]
    Malformed { source: object_store::Error },

    #[snafu(display("The checker task exited unexpectedly"))]
    #[snafu(context(false))]
    CheckerExited {
        source: tokio::sync::mpsc::error::SendError<ObjectMeta>,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;
