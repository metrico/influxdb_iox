use futures_util::TryStreamExt;
use influxdb_iox_client::{
    catalog::{self, generated_types::ParquetFile},
    connection::Connection,
    store,
};
use observability_deps::tracing::{debug, info};
use std::{
    fmt::Debug,
    path::{Path, PathBuf},
};
use thiserror::Error;
use tokio::{
    fs::{self, File, OpenOptions},
    io::{self, AsyncWriteExt},
};
use tokio_util::compat::FuturesAsyncReadCompatExt;

#[derive(Debug, Error)]
pub enum ExportError {
    #[error("JSON Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("IOx request failed: {0}")]
    Client(#[from] influxdb_iox_client::error::Error),

    #[error("Writing file: {0}")]
    File(#[from] std::io::Error),
}

type Result<T, E = ExportError> = std::result::Result<T, E>;

/// Represents a parquet file that is being exported
#[derive(Debug)]
pub struct ExportedParquetFileInfo {
    /// the file is index/num_parquet_files
    pub index: usize,
    /// the total number of parquet files
    pub num_parquet_files: usize,
    /// The filename that the file is being downloaded to
    pub filename: String,
}

/// Used to receive notifications f events during the exporting of data
pub trait ExportObserver: Debug + Send {
    /// Called when some number of parquet files are found to export
    fn files_found(&self, num_parquet_files: usize);

    /// Called when a file is skipped because it already exists
    fn file_exists(&self, file: &ExportedParquetFileInfo);

    /// Called when a file begins downloading
    fn file_downloading(&self, file: &ExportedParquetFileInfo);

    /// Called when the export is complete
    fn done(&self);
}

/// Exports data from a remote IOx instance to local files.
///
/// Data is read using the clients in [`influxdb_iox_client`] (rather
/// than the catalog) so that this can be used to debug remote systems.
#[derive(Debug)]
pub struct RemoteExporter {
    catalog_client: catalog::Client,
    store_client: store::Client,

    /// Optional partition filter. If `Some(partition_id)`, only these
    /// files with that `partition_id` are downloaded.
    partition_filter: Option<i64>,

    /// Observer to report events to
    observer: Box<dyn ExportObserver>,
}

impl RemoteExporter {
    pub fn new(connection: Connection) -> Self {
        Self {
            catalog_client: catalog::Client::new(connection.clone()),
            store_client: store::Client::new(connection),
            partition_filter: None,
            observer: Box::new(NoOpObserver::new()),
        }
    }

    /// Specify that only files and metadata for the specific
    /// partition id should be exported.
    pub fn with_partition_filter(mut self, partition_id: i64) -> Self {
        info!(partition_id, "Filtering by partition");
        self.partition_filter = Some(partition_id);
        self
    }

    /// Register an [`ExportObserver`] to receive events
    pub fn with_observer(mut self, observer: Box<dyn ExportObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Exports all data and metadata for `table_name` in
    /// `namespace` to local files.
    ///
    /// If `output_directory` is specified, all files are written
    /// there otherwise files are exported to a directory named
    /// `table_name`.
    pub async fn export_table(
        &mut self,
        output_directory: Option<PathBuf>,
        namespace_name: String,
        table_name: String,
    ) -> Result<()> {
        let output_directory = output_directory.unwrap_or_else(|| PathBuf::from(&table_name));
        fs::create_dir_all(&output_directory).await?;

        let parquet_files = self
            .catalog_client
            .get_parquet_files_by_namespace_table(namespace_name, table_name)
            .await?;

        // Export the metadata for the table. Since all
        // parquet_files are part of the same table, use the table_id
        // from the first parquet_file
        let table_id = parquet_files
            .get(0)
            .map(|parquet_file| parquet_file.table_id);
        if let Some(table_id) = table_id {
            self.export_table_metadata(&output_directory, table_id)
                .await?;
        }

        let num_parquet_files = parquet_files.len();
        self.observer.files_found(num_parquet_files);
        let indexed_parquet_file_metadata = parquet_files.into_iter().enumerate();

        for (index, parquet_file) in indexed_parquet_file_metadata {
            if self.should_export(parquet_file.partition_id) {
                self.export_parquet_file(
                    &output_directory,
                    index,
                    num_parquet_files,
                    &parquet_file,
                )
                .await?;
            } else {
                debug!(
                    "skipping file {} of {num_parquet_files} ({} does not match request)",
                    index + 1,
                    parquet_file.partition_id
                );
            }
        }
        self.observer.done();

        Ok(())
    }

    /// Return true if this partition should be exported
    fn should_export(&self, partition_id: i64) -> bool {
        self.partition_filter
            .map(|partition_filter| {
                // if a partition filter was specified, only export
                // the file if the partition matches
                partition_filter == partition_id
            })
            // export files if there is no partition
            .unwrap_or(true)
    }

    /// Exports table and partition information for the specified
    /// table. Overwrites existing files, if any, to ensure it has the
    /// latest catalog information.
    ///
    /// 1. `<output_directory>/table.<partition_id>.json`: pbjson
    /// encoded data about the table (minimal now)
    ///
    /// 2. `<output_directory>/partition.<partition_id>.json`: pbjson
    /// encoded data for each partition
    async fn export_table_metadata(
        &mut self,
        output_directory: &Path,
        table_id: i64,
    ) -> Result<()> {
        // write table metadata
        //
        // (Note that since there is way to get table metadata via
        // catalog API yet, make an empty object)
        let table_json = "{}";
        let filename = format!("table.{table_id}.json");
        let file_path = output_directory.join(&filename);
        write_string_to_file(table_json, &file_path).await?;

        // write partition metadata for the table
        let partitions = self
            .catalog_client
            .get_partitions_by_table_id(table_id)
            .await?;

        for partition in partitions {
            let partition_id = partition.id;
            if self.should_export(partition_id) {
                let partition_json = serde_json::to_string_pretty(&partition)?;
                let filename = format!("partition.{partition_id}.json");
                let file_path = output_directory.join(&filename);
                write_string_to_file(&partition_json, &file_path).await?;
            }
        }

        Ok(())
    }

    /// Exports a remote ParquetFile to:
    ///
    /// 1. `<output_directory>/<uuid>.parquet`: The parquet bytes
    ///
    /// 2. `<output_directory>/<uuid>.parquet.json`: pbjson encoded `ParquetFile` metadata
    async fn export_parquet_file(
        &mut self,
        output_directory: &Path,
        index: usize,
        num_parquet_files: usize,
        parquet_file: &ParquetFile,
    ) -> Result<()> {
        let uuid = &parquet_file.object_store_id;
        let partition_id = parquet_file.partition_id;
        let file_size_bytes = parquet_file.file_size_bytes as u64;

        // copy out the metadata as pbjson encoded data always (to
        // ensure we have the most up to date version)
        {
            let filename = format!("{uuid}.{partition_id}.parquet.json");
            let file_path = output_directory.join(&filename);
            let json = serde_json::to_string_pretty(&parquet_file)?;
            write_string_to_file(&json, &file_path).await?;
        }

        let filename = format!("{uuid}.{partition_id}.parquet");
        let file_info = ExportedParquetFileInfo {
            filename,
            index,
            num_parquet_files,
        };
        let file_path = output_directory.join(&file_info.filename);

        if fs::metadata(&file_path)
            .await
            .map_or(false, |metadata| metadata.len() == file_size_bytes)
        {
            self.observer.file_exists(&file_info);
        } else {
            self.observer.file_downloading(&file_info);
            let mut response = self
                .store_client
                .get_parquet_file_by_object_store_id(uuid.clone())
                .await?
                .map_ok(|res| res.data)
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
                .into_async_read()
                .compat();
            let mut file = File::create(&file_path).await?;
            io::copy(&mut response, &mut file).await?;
        }

        Ok(())
    }
}

/// writes the contents of a string to a file, overwriting the previous contents, if any
async fn write_string_to_file(contents: &str, path: &Path) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(path)
        .await?;

    file.write_all(contents.as_bytes()).await?;

    Ok(())
}

#[derive(Default, Debug)]
struct NoOpObserver {}

impl NoOpObserver {
    pub fn new() -> Self {
        Default::default()
    }
}

impl ExportObserver for NoOpObserver {
    fn files_found(&self, _num_parquet_files: usize) {}

    fn file_exists(&self, _file: &ExportedParquetFileInfo) {}

    fn file_downloading(&self, _file: &ExportedParquetFileInfo) {}

    fn done(&self) {}
}
