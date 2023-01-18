use data_types::PartitionId;
use iox_time::TimeProvider;
use metric::{
    Attributes, DurationHistogram, DurationHistogramOptions, Metric, U64Gauge, DURATION_MAX,
};
use observability_deps::tracing::debug;
use std::{sync::Arc, time::Duration};

/// Data points needed to run a compactor
#[derive(Debug)]
pub struct Compactor {
    /// Time provider for all activities in this compactor
    pub time_provider: Arc<dyn TimeProvider>,

    /// Gauge for the number of compaction partition candidates
    pub(crate) compaction_candidate_gauge: Metric<U64Gauge>,

    /// Histogram for tracking time to slect partition candidates
    pub(crate) candidate_selection_duration: Metric<DurationHistogram>,

    /// Histogram for tracking time to compact all selected partitions in a cycle
    pub(crate) compaction_cycle_duration: Metric<DurationHistogram>,

}

impl Compactor {
    pub fn new(
        time_provider: Arc<dyn TimeProvider>,
        registry: Arc<metric::Registry>,
    ) -> Self{
        let compaction_candidate_gauge = registry.register_metric(
            "compactor_candidates",
            "gauge for the number of compaction candidates that are found when checked",
        );

        let duration_histogram_options = DurationHistogramOptions::new([
            Duration::from_millis(500),
            Duration::from_millis(1_000), // 1 second
            Duration::from_millis(5_000),
            Duration::from_millis(15_000),
            Duration::from_millis(30_000),
            Duration::from_millis(60_000), // 1 minute
            Duration::from_millis(5 * 60_000),
            Duration::from_millis(15 * 60_000),
            Duration::from_millis(60 * 60_000),
            DURATION_MAX,
        ]);

        let candidate_selection_duration: Metric<DurationHistogram> = registry
            .register_metric_with_options(
                "compactor_candidate_selection_duration",
                "Duration to select compaction partition candidates",
                || duration_histogram_options.clone(),
            );

        let compaction_cycle_duration: Metric<DurationHistogram> = registry
            .register_metric_with_options(
                "compactor_compaction_cycle_duration",
                "Duration to compact all selected candidates for each cycle",
                || duration_histogram_options,
            );

        Self {
            time_provider,
            compaction_candidate_gauge,
            candidate_selection_duration,
            compaction_cycle_duration,
        }
    }
}

/// A cycle to select partition candidates and compact them
/// This is the main function of  https://github.com/influxdata/idpe/issues/16922
#[allow(dead_code)]
pub async fn compact(compactor: Arc<Compactor>)  {
    
    // Select candidates
    // Partitions with recently created L0 and/or L1 files are candidates
    // todo1
    let candidates: Vec<PartitionId> = vec![];

    let n_candidates = candidates.len();
    debug!(n_candidates, "found compaction candidates");

    // todo2:
    // Hash table of <TableId, StructOfNeededtableInfoForCompaction>
    // This is a mutable and mutex protected hash table
    // We only add more entry when it is not in the hash table
    // This can be moved outside this function and keep it as  a cache
    // let mut table_info = HashMap::new();

    // Loop through each candidate of the candidates and invoke 
    // Fully compact a partition means reading its files and compacting all eligible files.
    // After we finish compact a partition in a cycle, the partition may have new L0 files 
    // which will not compacted in this cycle and will be compacted in the next cycle. However,
    // all existing L0s files will be compacted in L1s, and large enough existing and newly
    //  created L1s in this cycle will be compacted in L2s
    let start_time = compactor.time_provider.now();
    let mut actual_compacted_partitions = 0;
    // todo3: this loop can be parallelized
    for candidate in candidates {
        
        // todo4:
        // compact_partition(partition_id, table_info)
    }

    debug!(n_candidate, actual_compacted_partitions, "Finish compacting all candidates in the cycle");

    // Done compacting all candidates in the cycle, record its time
    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let attributes = Attributes::from([("compaction_cycle", "finished")]);
        let duration = compactor.compaction_cycle_duration.recorder(attributes);
        duration.record(delta);
    }
}