//! A mutable, hierarchical buffer structure of namespace -> table ->
//! partition -> write payloads.

pub(crate) mod namespace;
pub(crate) mod partition;
pub(crate) mod table;

/// The root node of a [`BufferTree`].
mod root;
#[allow(unused_imports)]
pub(crate) use root::*;

pub(crate) mod post_write;

/// Re-export [`PartitionData`] for benchmarking purposes only.
#[cfg(feature = "benches")]
pub mod benches {
    pub use super::partition::PartitionData;
}
