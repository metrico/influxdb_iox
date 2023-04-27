//! This module implements the `compact` CLI command
use std::{path::PathBuf, pin::Pin};

use observability_deps::tracing::info;
use snafu::{ResultExt, Snafu};
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio_stream::StreamExt;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Cannot {} output file '{:?}': {}", operation, path, source))]
    File {
        operation: String,
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("No input files specified"))]
    NoFiles {},

    // #[snafu(display("Error converting: {}", source))]
    // Conversion {
    //     source: parquet_to_line_protocol::Error,
    // },

    // #[snafu(display("IO error: {}", source))]
    // IO { source: std::io::Error },

    // #[snafu(display("Cannot flush output: {}", message))]
    // Flush {
    //     // flush error has the W writer in it, all we care about is the error
    //     message: String,
    // },
}

/// Compact multiple IOx Parquet files into a single output file (files must be from the same table in IOx)
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// Input file(s)
    #[clap(value_parser)]
    input: Vec<PathBuf>,

    #[clap(long, short)]
    /// The output path to which to write
    output: PathBuf,
}

pub async fn command(config: Config) -> Result<(), Error> {
    let Config { input, output } = config;
    info!(?input, ?output, "compacting parquet files...");

    if input.is_empty() {
        return Err(Error::NoFiles{});
    }

    // fire up the command
    //let


    Ok(())
}
