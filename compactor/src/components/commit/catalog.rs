use std::{fmt::Display, sync::Arc};
use std::time::Duration;

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{CompactionLevel, ParquetFile, ParquetFileId, ParquetFileParams, PartitionId};
use iox_catalog::interface::Catalog;

use super::Commit;

#[derive(Debug)]
pub struct CatalogCommit {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
    deleted_at_adjustment: Duration
}

impl CatalogCommit {
    pub fn new(backoff_config: BackoffConfig, catalog: Arc<dyn Catalog>, deleted_at_adjustment: Duration) -> Self {
        Self {
            backoff_config,
            catalog,
            deleted_at_adjustment
        }
    }
}

impl Display for CatalogCommit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog")
    }
}

#[async_trait]
impl Commit for CatalogCommit {
    async fn commit(
        &self,
        _partition_id: PartitionId,
        delete: &[ParquetFile],
        upgrade: &[ParquetFile],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Vec<ParquetFileId> {
        assert!(!upgrade.is_empty() || (!delete.is_empty() && !create.is_empty()));

        let delete = delete.iter().map(|f| f.id).collect::<Vec<_>>();
        let upgrade = upgrade.iter().map(|f| f.id).collect::<Vec<_>>();

        Backoff::new(&self.backoff_config)
            .retry_all_errors("commit parquet file changes", || async {
                let mut repos = self.catalog.repositories().await;
                let parquet_files = repos.parquet_files();
                let ids = parquet_files
                    .create_upgrade_delete(&delete, &upgrade, create, target_level, Some(self.deleted_at_adjustment))
                    .await?;

                Ok::<_, iox_catalog::interface::Error>(ids)
            })
            .await
            .expect("retry forever")
    }
}
