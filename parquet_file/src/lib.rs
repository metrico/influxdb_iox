//! Parquet file generation, storage, and metadata implementations.

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    unreachable_pub,
    missing_docs,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]
#![allow(clippy::missing_docs_in_private_items)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod chunk;
pub mod metadata;
pub mod serialize;
pub mod storage;
pub mod writer;

use data_types::{NamespaceId, ParquetFile, ParquetFileParams, PartitionId, TableId};
use object_store::path::Path;
use uuid::Uuid;

/// Location of a Parquet file within a namespace's object store.
/// The exact format is an implementation detail and is subject to change.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ParquetFilePath {
    namespace_id: NamespaceId,
    table_id: TableId,
    partition_id: PartitionId,
    object_store_id: Uuid,
}

impl ParquetFilePath {
    /// Create parquet file path relevant for the storage layout.
    pub fn new(
        namespace_id: NamespaceId,
        table_id: TableId,
        partition_id: PartitionId,
        object_store_id: Uuid,
    ) -> Self {
        Self {
            namespace_id,
            table_id,
            partition_id,
            object_store_id,
        }
    }

    /// Get object-store path.
    pub fn object_store_path(&self) -> Path {
        let Self {
            namespace_id,
            table_id,
            partition_id,
            object_store_id,
        } = self;
        Path::from_iter([
            namespace_id.to_string().as_str(),
            table_id.to_string().as_str(),
            partition_id.to_string().as_str(),
            &format!("{object_store_id}.parquet"),
        ])
    }

    /// Get object store ID.
    pub fn objest_store_id(&self) -> Uuid {
        self.object_store_id
    }

    /// Set new object store ID.
    pub fn with_object_store_id(self, object_store_id: Uuid) -> Self {
        Self {
            object_store_id,
            ..self
        }
    }
}

impl From<&Self> for ParquetFilePath {
    fn from(borrowed: &Self) -> Self {
        *borrowed
    }
}

impl From<&crate::metadata::IoxMetadata> for ParquetFilePath {
    fn from(m: &crate::metadata::IoxMetadata) -> Self {
        Self {
            namespace_id: m.namespace_id,
            table_id: m.table_id,
            partition_id: m.partition_id,
            object_store_id: m.object_store_id,
        }
    }
}

impl From<&ParquetFile> for ParquetFilePath {
    fn from(f: &ParquetFile) -> Self {
        Self {
            namespace_id: f.namespace_id,
            table_id: f.table_id,
            partition_id: f.partition_id,
            object_store_id: f.object_store_id,
        }
    }
}

impl From<&ParquetFileParams> for ParquetFilePath {
    fn from(f: &ParquetFileParams) -> Self {
        Self {
            namespace_id: f.namespace_id,
            table_id: f.table_id,
            partition_id: f.partition_id,
            object_store_id: f.object_store_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parquet_file_absolute_dirs_and_file_path() {
        let pfp = ParquetFilePath::new(
            NamespaceId::new(1),
            TableId::new(2),
            PartitionId::new(4),
            Uuid::nil(),
        );
        let path = pfp.object_store_path();
        assert_eq!(
            path.to_string(),
            "1/2/4/00000000-0000-0000-0000-000000000000.parquet".to_string(),
        );
    }
}
