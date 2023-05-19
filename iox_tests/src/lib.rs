//! IOx test utils and tests

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro
)]

mod catalog;
pub use catalog::{
    TestCatalog, TestNamespace, TestParquetFile, TestParquetFileBuilder, TestPartition, TestTable,
};

mod builders;
pub use builders::{ParquetFileBuilder, PartitionBuilder, SkippedCompactionBuilder, TableBuilder};
