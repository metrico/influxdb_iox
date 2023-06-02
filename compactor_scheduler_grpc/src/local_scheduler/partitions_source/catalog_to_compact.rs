use std::{fmt::Display, sync::Arc, time::Duration};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{PartitionId, PartitionsSource};
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;

#[derive(Debug)]
/// Returns all [`PartitionId`](data_types::PartitionId) that had a new Parquet file written after a lower bound of the current
/// time minus `min_threshold` and optionally limited only to those with Parquet files written
/// before the current time minus `max_threshold`.
///
/// If `max_threshold` is not specified, the upper bound is effectively the current time.
///
/// If `max_threshold` is specified, it must be less than `min_threshold` so that when computing
/// the range endpoints as `(now - min_threshold, now - max_threshold)`, the lower bound is lower
/// than the upper bound.
pub(crate) struct CatalogToCompactPartitionsSource {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
    min_threshold: Duration,
    max_threshold: Option<Duration>,
    time_provider: Arc<dyn TimeProvider>,
}

impl CatalogToCompactPartitionsSource {
    pub(crate) fn new(
        backoff_config: BackoffConfig,
        catalog: Arc<dyn Catalog>,
        min_threshold: Duration,
        max_threshold: Option<Duration>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            backoff_config,
            catalog,
            min_threshold,
            max_threshold,
            time_provider,
        }
    }
}

impl Display for CatalogToCompactPartitionsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog_to_compact")
    }
}

#[async_trait]
impl PartitionsSource for CatalogToCompactPartitionsSource {
    async fn fetch(&self) -> Vec<PartitionId> {
        let minimum_time = self.time_provider.now() - self.min_threshold;
        let maximum_time = self.max_threshold.map(|max| self.time_provider.now() - max);

        Backoff::new(&self.backoff_config)
            .retry_all_errors("partitions_to_compact", || async {
                self.catalog
                    .repositories()
                    .await
                    .partitions()
                    .partitions_new_file_between(minimum_time.into(), maximum_time.map(Into::into))
                    .await
            })
            .await
            .expect("retry forever")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::Timestamp;
    use iox_catalog::mem::MemCatalog;
    use iox_tests::PartitionBuilder;

    fn partition_ids(ids: &[i64]) -> Vec<PartitionId> {
        ids.iter().cloned().map(PartitionId::new).collect()
    }

    async fn fetch_test(
        catalog: Arc<MemCatalog>,
        min_threshold: Duration,
        max_threshold: Option<Duration>,
        expected_ids: &[i64],
    ) {
        let time_provider = catalog.time_provider();

        let partitions_source = CatalogToCompactPartitionsSource::new(
            Default::default(),
            catalog,
            min_threshold,
            max_threshold,
            time_provider,
        );

        let mut actual_partition_ids = partitions_source.fetch().await;
        actual_partition_ids.sort();

        assert_eq!(
            actual_partition_ids,
            partition_ids(expected_ids),
            "CatalogToCompact source with min_threshold {min_threshold:?} and \
            max_threshold {max_threshold:?} failed",
        );
    }

    #[tokio::test]
    async fn no_max_specified() {
        let catalog = Arc::new(MemCatalog::new(Default::default()));
        let time_provider = catalog.time_provider();

        let time_three_hour_ago = Timestamp::from(time_provider.hours_ago(3));
        let time_six_hour_ago = Timestamp::from(time_provider.hours_ago(6));

        for (id, time) in [(1, time_three_hour_ago), (2, time_six_hour_ago)]
            .iter()
            .cloned()
        {
            let partition = PartitionBuilder::new(id as i64)
                .with_new_file_at(time)
                .build();
            catalog.add_partition(partition).await;
        }

        let one_minute = Duration::from_secs(60);
        fetch_test(Arc::clone(&catalog), one_minute, None, &[]).await;

        let four_hours = Duration::from_secs(60 * 60 * 4);
        fetch_test(Arc::clone(&catalog), four_hours, None, &[1]).await;

        let seven_hours = Duration::from_secs(60 * 60 * 7);
        fetch_test(Arc::clone(&catalog), seven_hours, None, &[1, 2]).await;
    }

    #[tokio::test]
    async fn max_specified() {
        let catalog = Arc::new(MemCatalog::new(Default::default()));
        let time_provider = catalog.time_provider();

        let time_now = Timestamp::from(time_provider.now());
        let time_three_hour_ago = Timestamp::from(time_provider.hours_ago(3));
        let time_six_hour_ago = Timestamp::from(time_provider.hours_ago(6));

        for (id, time) in [
            (1, time_now),
            (2, time_three_hour_ago),
            (3, time_six_hour_ago),
        ]
        .iter()
        .cloned()
        {
            let partition = PartitionBuilder::new(id as i64)
                .with_new_file_at(time)
                .build();
            catalog.add_partition(partition).await;
        }

        let one_minute = Duration::from_secs(60);
        let one_hour = Duration::from_secs(60 * 60);
        let four_hours = Duration::from_secs(60 * 60 * 4);
        let seven_hours = Duration::from_secs(60 * 60 * 7);

        fetch_test(Arc::clone(&catalog), seven_hours, Some(four_hours), &[3]).await;

        fetch_test(Arc::clone(&catalog), seven_hours, Some(one_hour), &[2, 3]).await;

        fetch_test(Arc::clone(&catalog), seven_hours, Some(one_minute), &[2, 3]).await;

        fetch_test(Arc::clone(&catalog), four_hours, Some(one_hour), &[2]).await;

        fetch_test(Arc::clone(&catalog), four_hours, Some(one_minute), &[2]).await;

        fetch_test(Arc::clone(&catalog), one_hour, Some(one_minute), &[]).await;
    }
}
