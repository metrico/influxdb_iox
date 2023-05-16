//! A module providing a CLI command for producing line protocol from a WAL file.
use std::fs;
use std::path::PathBuf;

use observability_deps::tracing::{error, info};
use wal::{ClosedSegmentFileReader, WriteOpEntryDecoder};
use wal_inspect::{LineProtoWriter, NamespacedBatchWriter, WriteError};

use super::Error;

/// A container for the possible arguments & flags of a `write_lp` command.
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// The path to the input WAL file
    #[clap(value_parser)]
    input: PathBuf,

    /// The directory to write line protocol. Creates the directory if it does
    /// not exist.
    ///
    /// When unspecified the line protocol is written to stdout
    #[clap(long, short, value_parser)]
    output_directory: Option<PathBuf>,

    /// When enabled, pre-existing line protocol files will be overwritten
    #[clap(long, short)]
    force: bool,
}

/// Executes the `write_lp` command with the provided configuration, reading
/// write operation entries from a WAL file and mapping them to line protocol.
pub fn command(config: Config) -> Result<(), Error> {
    let decoder = WriteOpEntryDecoder::from(
        ClosedSegmentFileReader::from_path(&config.input)
            .map_err(|wal_err| Error::UnableToOpenWalFile { source: wal_err })?,
    );

    match config.output_directory {
        Some(d) => {
            fs::create_dir_all(d.as_path())?;
            let writer = LineProtoWriter::new(
                |namespace_id| {
                    let file_path = d.as_path().join(format!("namespace_{}.lp", namespace_id));

                    let mut open_options = fs::OpenOptions::new().write(true).to_owned();
                    if config.force {
                        open_options.create(true);
                    } else {
                        open_options.create_new(true);
                    }

                    info!(
                        file_path = ?file_path.to_str(),

                        "Creating new file for namespaced line protocol output",
                    );

                    open_options.open(file_path).map_err(WriteError::IoError)
                },
                None,
            );
            decode_and_write_entries(decoder, writer)
        }
        None => {
            let writer = LineProtoWriter::new(|_namespace_id| Ok(std::io::stdout()), None);
            decode_and_write_entries(decoder, writer)
        }
    }
}

fn decode_and_write_entries<W: NamespacedBatchWriter>(
    mut decoder: WriteOpEntryDecoder,
    mut writer: W,
) -> Result<(), Error> {
    let mut incomplete_write = false;
    while let Some(Ok(decoded_entries)) = decoder.next() {
        for entry in decoded_entries {
            info!(
                "Writing out new WAL entry for namespace ID {}:",
                entry.namespace
            );
            writer
                .write_namespaced_table_batches(entry.namespace, entry.table_batches)
                .unwrap_or_else(|err| {
                    incomplete_write = true;
                    error!(
                        %err,
                        "Encountered an error writing out WAL entry as line protocol",
                    );
                });
        }
    }

    if incomplete_write {
        return Err(Error::UnableToWriteAllLineProto);
    }
    Ok(())
}
