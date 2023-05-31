use clap_blocks::compactor_scheduler::ShardConfigForLocalScheduler;

/// Shard config.
/// configured per LocalScheduler, which equates to per compactor.
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)]
pub struct ShardConfig {
    /// Number of shards.
    pub n_shards: usize,

    /// Shard ID.
    ///
    /// Starts as 0 and must be smaller than the number of shards.
    pub shard_id: usize,
}

impl ShardConfig {
    pub(crate) fn from_config(config: ShardConfigForLocalScheduler) -> Option<Self> {
        // if shard_count is specified, shard_id must be provided also.
        // shard_id may be specified explicitly or extracted from the host name.
        let mut shard_id = config.shard_id;
        if config.shard_id.is_none() && config.shard_count.is_some() && config.hostname.is_some() {
            let parsed_id = config
                .hostname
                .as_ref()
                .expect("already checked")
                .chars()
                .skip_while(|ch| !ch.is_ascii_digit())
                .take_while(|ch| ch.is_ascii_digit())
                .fold(None, |acc, ch| {
                    ch.to_digit(10).map(|b| acc.unwrap_or(0) * 10 + b)
                });
            shard_id = parsed_id.map(|id| id as usize);
        }
        assert!(
            shard_id.is_some() == config.shard_count.is_some(),
            "must provide or not provide shard ID and count"
        );
        shard_id.map(|shard_id| ShardConfig {
            shard_id,
            n_shards: config.shard_count.expect("just checked"),
        })
    }
}
