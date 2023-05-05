//! An abstract resolver of [`PartitionData`] for a given table.
//!
//! [`PartitionData`]: super::PartitionData

#![allow(unused_imports)] // Transition time only.

mod cache;
pub(crate) use cache::*;

mod r#trait;
pub(crate) use r#trait::*;

mod catalog;
pub(crate) use catalog::*;

mod sort_key;
pub(crate) use sort_key::*;

mod coalesce;
pub(crate) use coalesce::*;

#[cfg(test)]
pub(crate) mod mock;
