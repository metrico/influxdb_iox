//! Compactor-Scheduler-related configs.
use std::sync::Arc;

use backoff::BackoffConfig;
use compactor_scheduler_grpc::{LocalScheduler, Scheduler};
use iox_catalog::interface::Catalog;
use snafu::Snafu;

/// Why a specified ingester address might be invalid
#[allow(missing_docs)]
#[derive(Snafu, Copy, Clone, Debug)]
pub enum Error {}

/// Compaction Scheduler type.
#[derive(Debug, Default, Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum CompactorSchedulerType {
    /// Perform scheduling decisions locally.
    #[default]
    Local,

    /// Perform scheduling decisions remotely.
    Remote,
}

/// CLI config for compactor scheduler.
#[derive(Debug, Copy, Clone, Default, clap::Parser)]
pub struct CompactorSchedulerConfig {
    /// Scheduler type to use.
    #[clap(
        value_enum,
        long = "compactor-scheduler",
        env = "INFLUXDB_IOX_COMPACTION_SCHEDULER",
        default_value = "local",
        action
    )]
    pub compactor_scheduler_type: CompactorSchedulerType,
}

impl CompactorSchedulerConfig {
    /// Get config-dependent compactor scheduler.
    pub fn get_scheduler(
        &self,
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
    ) -> Arc<dyn Scheduler> {
        match self.compactor_scheduler_type {
            CompactorSchedulerType::Local => {
                Arc::new(LocalScheduler::new(catalog, backoff_config, None))
            }
            CompactorSchedulerType::Remote => {
                unimplemented!("only 'local' compactor-scheduler is implemented")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use test_helpers::assert_contains;

    #[test]
    fn default_compactor_scheduler_type_is_local() {
        let config = CompactorSchedulerConfig::try_parse_from(["my_binary"]).unwrap();
        assert_eq!(
            config.compactor_scheduler_type,
            CompactorSchedulerType::Local
        );
    }

    #[test]
    fn can_specify_local() {
        let config = CompactorSchedulerConfig::try_parse_from([
            "my_binary",
            "--compactor-scheduler",
            "local",
        ])
        .unwrap();
        assert_eq!(
            config.compactor_scheduler_type,
            CompactorSchedulerType::Local
        );
    }

    #[test]
    fn any_other_scheduler_type_string_is_invalid() {
        let error = CompactorSchedulerConfig::try_parse_from([
            "my_binary",
            "--compactor-scheduler",
            "hello",
        ])
        .unwrap_err()
        .to_string();
        assert_contains!(
            &error,
            "invalid value 'hello' for '--compactor-scheduler <COMPACTOR_SCHEDULER_TYPE>'"
        );
        assert_contains!(&error, "[possible values: local, remote]");
    }
}
