//! Internal modules used by [`LocalScheduler`].
pub(crate) mod id_only_partition_filter;
pub(crate) mod partitions_source;

use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use backoff::BackoffConfig;
use data_types::{MockPartitionsSource, PartitionId, PartitionsSource};
use iox_catalog::interface::Catalog;
use iox_time::{SystemProvider, TimeProvider};
use observability_deps::tracing::info;

use crate::local_scheduler::id_only_partition_filter::shard::ShardPartitionFilter;
use crate::{
    scheduler::Scheduler,
    temp::{PartitionsSourceConfig, ShardConfig},
};

use self::{
    id_only_partition_filter::{and::AndIdOnlyPartitionFilter, IdOnlyPartitionFilter},
    partitions_source::{
        catalog_all::CatalogAllPartitionsSource,
        catalog_to_compact::CatalogToCompactPartitionsSource,
        filter::FilterPartitionsSourceWrapper,
    },
};

/// Implementation of the [`Scheduler`] for local (per compactor) scheduling.
#[derive(Debug)]
pub struct LocalScheduler {
    catalog: Arc<dyn Catalog>,
    time_provider: Arc<dyn TimeProvider>,
    backoff_config: BackoffConfig,
}

impl LocalScheduler {
    /// Create new LocalScheduler.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
        time_provider: Option<Arc<dyn TimeProvider>>,
    ) -> Self {
        let time_provider: Arc<dyn TimeProvider> = match &time_provider {
            Some(t) => Arc::clone(t),
            None => Arc::new(SystemProvider::default()),
        };

        Self {
            catalog,
            time_provider,
            backoff_config,
        }
    }
}

#[async_trait]
impl Scheduler for LocalScheduler {
    async fn get_partitions(
        &self,
        config: PartitionsSourceConfig,
        shard_config: Option<ShardConfig>,
    ) -> Vec<PartitionId> {
        let partitions_source: Arc<dyn PartitionsSource> = match &config {
            PartitionsSourceConfig::CatalogRecentWrites { threshold } => {
                Arc::new(CatalogToCompactPartitionsSource::new(
                    self.backoff_config.clone(),
                    Arc::clone(&self.catalog),
                    *threshold,
                    None, // Recent writes is `threshold` ago to now
                    Arc::clone(&self.time_provider),
                ))
            }
            PartitionsSourceConfig::CatalogAll => Arc::new(CatalogAllPartitionsSource::new(
                self.backoff_config.clone(),
                Arc::clone(&self.catalog),
            )),
            PartitionsSourceConfig::Fixed(ids) => {
                Arc::new(MockPartitionsSource::new(ids.iter().cloned().collect()))
            }
        };

        let mut id_only_partition_filters: Vec<Arc<dyn IdOnlyPartitionFilter>> = vec![];
        if let Some(shard_config) = &shard_config {
            // add shard filter before performing any catalog IO
            info!(
                "starting compactor {} of {}",
                shard_config.shard_id, shard_config.n_shards
            );
            id_only_partition_filters.push(Arc::new(ShardPartitionFilter::new(
                shard_config.n_shards,
                shard_config.shard_id,
            )));
        }

        FilterPartitionsSourceWrapper::new(
            AndIdOnlyPartitionFilter::new(id_only_partition_filters),
            partitions_source,
        )
        .fetch()
        .await
    }
}

impl Display for LocalScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "local_compaction_scheduler")
    }
}
