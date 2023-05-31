//! Compactor-Scheduler-related configs.

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
#[derive(Debug, Clone, Default, clap::Parser)]
pub struct ShardConfigForLocalScheduler {
    /// Number of shards.
    ///
    /// If this is set then the shard ID MUST also be set. If both are not provided, sharding is disabled.
    /// (shard ID can be provided by the host name)
    #[clap(
        long = "compaction-shard-count",
        env = "INFLUXDB_IOX_COMPACTION_SHARD_COUNT",
        action
    )]
    pub shard_count: Option<usize>,

    /// Shard ID.
    ///
    /// Starts at 0, must be smaller than the number of shard.
    ///
    /// If this is set then the shard count MUST also be set. If both are not provided, sharding is disabled.
    #[clap(
        long = "compaction-shard-id",
        env = "INFLUXDB_IOX_COMPACTION_SHARD_ID",
        requires("shard_count"),
        action
    )]
    pub shard_id: Option<usize>,

    /// Host Name
    ///
    /// comprised of leading text (e.g. 'iox-shared-compactor-'), ending with shard_id (e.g. '0').
    /// When shard_count is specified, but shard_id is not specified, the id is extracted from hostname.
    #[clap(long = "hostname", env = "HOSTNAME", requires("shard_count"), action)]
    pub hostname: Option<String>,
}

/// CLI config for compactor scheduler.
#[derive(Debug, Clone, Default, clap::Parser)]
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

    /// Shard config used by the local scheduler.
    #[clap(flatten)]
    pub shard_config: ShardConfigForLocalScheduler,
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
