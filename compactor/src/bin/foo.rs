use std::sync::Arc;

use backoff::BackoffConfig;
use compactor::{compact::Compactor, handler::CompactorConfig};
use data_types::{ColumnType, PartitionParam};
use iox_query::exec::Executor;
use iox_tests::util::{TestCatalog, TestParquetFileBuilder};
use iox_time::SystemProvider;
use observability_deps::tracing::info;
use parquet_file::storage::ParquetStorage;

use compactor::compact_partition;

#[tokio::main]
async fn main() {
    test_helpers::maybe_start_logging();
    let catalog = TestCatalog::new();

    let namespace = "namespace_name";
    let table_name = "test_table";

    // n sets of Line Protocols, each has 700 columns: 4 tags, 695 fields and time
    let n = 101;
    // let n = 2;

    // m rows per lp
    let m = 1001;
    // let m = 2;

    let mut lps = Vec::with_capacity(n);

    let field_num = 695;
    // let field_num = 2;

    for i in 1..n {
        let lp = (1..m)
            .into_iter()
            .map(|l| {
                // A lp: with 4 tags
                let mut line: String =
                    format!("{},tag1=MA,tag2=Boston,tag3=30,tag4=abc ", table_name);
                // add 694 field columns
                let fields = (1..field_num)
                    .into_iter()
                    .map(|i| format!("field{}={}i,", i, i))
                    .collect::<Vec<_>>()
                    .join("");
                line.push_str(fields.as_str());
                // 695th field and time
                line.push_str("fieldlast=100i ");
                line.push_str((i * l).to_string().as_str());
                line
            })
            .collect::<Vec<_>>()
            .join("\n");
        lps.push(lp);
        // println!("x: {:#?}", lp);
    }

    let ns = catalog.create_namespace(namespace).await;
    let sequencer = ns.create_sequencer(1).await;
    let table = ns.create_table(table_name).await;
    table.create_column("tag1", ColumnType::Tag).await;
    table.create_column("tag2", ColumnType::Tag).await;
    table.create_column("tag3", ColumnType::Tag).await;
    table.create_column("tag4", ColumnType::Tag).await;
    table.create_column("time", ColumnType::Time).await;
    for i in 1..field_num {
        let field_name = format!("field{}", i);
        table.create_column(&field_name, ColumnType::I64).await;
    }
    table.create_column("fieldlast", ColumnType::I64).await;
    let partition = table
        .with_sequencer(&sequencer)
        .create_partition("part")
        .await;

    for (i, lp) in lps.iter().enumerate() {
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(lp)
            .with_max_seq(i.try_into().unwrap())
            .with_min_time(10)
            .with_max_time(20)
            .with_file_size_bytes(600000) // 600KB
            .with_creation_time((i * m).try_into().unwrap());
        partition.create_parquet_file(builder).await;
    }

    // should have n L0 files before compacting
    let count = catalog.count_level_0_files(sequencer.sequencer.id).await;
    assert_eq!(count, n - 1);

    // Let compact the partition
    let _time = Arc::new(SystemProvider::new());
    let config = make_compactor_config();
    let metrics = Arc::new(metric::Registry::new());
    let compactor = Compactor::new(
        vec![sequencer.sequencer.id],
        Arc::clone(&catalog.catalog),
        ParquetStorage::new(Arc::clone(&catalog.object_store)),
        Arc::new(Executor::new(1)),
        Arc::new(SystemProvider::new()),
        BackoffConfig::default(),
        config,
        Arc::clone(&metrics),
    );

    let p = PartitionParam {
        partition_id: partition.partition.id,
        sequencer_id: sequencer.sequencer.id,
        namespace_id: ns.namespace.id,
        table_id: table.table.id,
    };
    let mut candidates = compactor.add_info_to_partitions(&[p]).await.unwrap();
    let c = candidates.pop().unwrap();

    info!("start compacting");
    compact_partition(&compactor, c).await.unwrap();
}

fn make_compactor_config() -> CompactorConfig {
    let max_desired_file_size_bytes = 10_000;
    let percentage_max_file_size = 30;
    let split_percentage = 80;
    let max_concurrent_size_bytes = 100_000;
    let max_number_partitions_per_sequencer = 1;
    let min_number_recent_ingested_per_partition = 1;
    let input_size_threshold_bytes = 300 * 1024 * 1024;
    let input_file_count_threshold = 100;
    CompactorConfig::new(
        max_desired_file_size_bytes,
        percentage_max_file_size,
        split_percentage,
        max_concurrent_size_bytes,
        max_number_partitions_per_sequencer,
        min_number_recent_ingested_per_partition,
        input_size_threshold_bytes,
        input_file_count_threshold,
    )
}
