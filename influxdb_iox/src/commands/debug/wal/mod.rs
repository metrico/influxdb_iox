//! This module implements CLI commands for debugging the ingester WAL.

use thiserror::Error;

mod write_lp;

/// A command level error type to decorate WAL errors with some extra
/// "human" context for the user
#[derive(Debug, Error)]
pub enum Error {
    #[error("could not open WAL file: {source}")]
    UnableToOpenWalFile { source: wal::Error },

    #[error("failed to decode write entries from the WAL file: {0}")]
    FailedToDecodeWriteOpEntry(#[from] wal::DecodeError),

    #[error("encountered errors writing out WAL entries as line protocol")]
    UnableToWriteAllLineProto,

    #[error("encountered an error performing i/o: {0}")]
    IoFailure(#[from] std::io::Error),
}

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// Subcommands for debugging the ingester WAL
#[derive(Debug, clap::Parser)]
enum Command {
    /// Write out the contents of a WAL file as line protocol
    WriteLp(write_lp::Config),
}

/// Executes a WAL debugging subcommand as directed by the config
pub fn command(config: Config) -> Result<(), Error> {
    match config.command {
        Command::WriteLp(config) => write_lp::command(config),
    }
}
