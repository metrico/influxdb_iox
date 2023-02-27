//! DML data types

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro
)]

use std::{collections::BTreeMap, time::Duration};

use croaring::treemap::NativeSerializer;
use data_types::{
    DeletePredicate, NamespaceId, NonEmptyString, PartitionKey, StatValues, Statistics, TableId,
};
use hashbrown::HashMap;
use iox_time::{Time, TimeProvider};
use mutable_batch::MutableBatch;
use trace::ctx::SpanContext;

/// Metadata information about a DML operation
#[derive(Debug, Default, Clone, PartialEq)]
pub struct DmlMeta {
    /// The sequence number associated with this write
    sequence_number: Option<SequenceNumber>,

    /// When this write was ingested into the write buffer
    producer_ts: Option<Time>,

    /// Optional span context associated w/ this write
    span_ctx: Option<SpanContext>,

    /// Bytes read from the wire
    bytes_read: Option<usize>,
}

impl DmlMeta {
    /// Create a new [`DmlMeta`] for a sequenced operation
    pub fn sequenced(
        sequence_number: SequenceNumber,
        producer_ts: Time,
        span_ctx: Option<SpanContext>,
        bytes_read: usize,
    ) -> Self {
        Self {
            sequence_number: Some(sequence_number),
            producer_ts: Some(producer_ts),
            span_ctx,
            bytes_read: Some(bytes_read),
        }
    }

    /// Create a new [`DmlMeta`] for an unsequenced operation
    pub fn unsequenced(span_ctx: Option<SpanContext>) -> Self {
        Self {
            sequence_number: None,
            producer_ts: None,
            span_ctx,
            bytes_read: None,
        }
    }

    /// Gets the sequence number associated with the write if any
    pub fn sequence(&self) -> Option<SequenceNumber> {
        self.sequence_number
    }

    /// Gets the producer timestamp associated with the write if any
    pub fn producer_ts(&self) -> Option<Time> {
        self.producer_ts
    }

    /// returns the Duration since this DmlMeta was produced, if any
    pub fn duration_since_production(&self, time_provider: &dyn TimeProvider) -> Option<Duration> {
        self.producer_ts
            .and_then(|ts| time_provider.now().checked_duration_since(ts))
    }

    /// Gets the span context if any
    pub fn span_context(&self) -> Option<&SpanContext> {
        self.span_ctx.as_ref()
    }

    /// Returns the number of bytes read from the wire if relevant
    pub fn bytes_read(&self) -> Option<usize> {
        self.bytes_read
    }

    /// Return the approximate memory size of the metadata, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .span_ctx
                .as_ref()
                .map(|ctx| ctx.size())
                .unwrap_or_default()
            - self
                .span_ctx
                .as_ref()
                .map(|_| std::mem::size_of::<SpanContext>())
                .unwrap_or_default()
    }
}

/// A DML operation
#[derive(Debug, Clone)]
pub enum DmlOperation {
    /// A write operation
    Write(DmlWrite),

    /// A delete operation
    Delete(DmlDelete),
}

impl DmlOperation {
    /// Gets the metadata associated with this operation
    pub fn meta(&self) -> &DmlMeta {
        match &self {
            Self::Write(w) => w.meta(),
            Self::Delete(d) => d.meta(),
        }
    }

    /// Sets the metadata for this operation
    pub fn set_meta(&mut self, meta: DmlMeta) {
        match self {
            Self::Write(w) => w.set_meta(meta),
            Self::Delete(d) => d.set_meta(meta),
        }
    }

    /// Return the approximate memory size of the operation, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        match self {
            Self::Write(w) => {
                std::mem::size_of::<Self>() - std::mem::size_of::<DmlWrite>() + w.size()
            }
            Self::Delete(d) => {
                std::mem::size_of::<Self>() - std::mem::size_of::<DmlDelete>() + d.size()
            }
        }
    }

    /// Namespace catalog ID associated with this operation
    pub fn namespace_id(&self) -> NamespaceId {
        match self {
            Self::Write(w) => w.namespace_id(),
            Self::Delete(d) => d.namespace_id(),
        }
    }
}

impl From<DmlWrite> for DmlOperation {
    fn from(v: DmlWrite) -> Self {
        Self::Write(v)
    }
}

impl From<DmlDelete> for DmlOperation {
    fn from(v: DmlDelete) -> Self {
        Self::Delete(v)
    }
}

/// A collection of writes to potentially multiple tables within the same namespace
#[derive(Debug, Clone)]
pub struct DmlWrite {
    /// The namespace being written to
    namespace_id: NamespaceId,
    /// Writes to individual tables keyed by table ID
    table_ids: HashMap<TableId, MutableBatch>,
    /// Write metadata
    meta: DmlMeta,
    min_timestamp: i64,
    max_timestamp: i64,
    /// The partition key derived for this write.
    partition_key: PartitionKey,
}

impl DmlWrite {
    /// Create a new [`DmlWrite`]
    ///
    /// # Panic
    ///
    /// Panics if
    ///
    /// - `table_ids` is empty
    /// - a MutableBatch is empty
    /// - a MutableBatch lacks an i64 "time" column
    pub fn new(
        namespace_id: NamespaceId,
        table_ids: HashMap<TableId, MutableBatch>,
        partition_key: PartitionKey,
        meta: DmlMeta,
    ) -> Self {
        assert_ne!(table_ids.len(), 0);

        let mut stats = StatValues::new_empty();
        for (table_id, table) in &table_ids {
            match table
                .column(schema::TIME_COLUMN_NAME)
                .expect("time")
                .stats()
            {
                Statistics::I64(col_stats) => stats.update_from(&col_stats),
                s => unreachable!(
                    "table \"{}\" has unexpected type for time column: {}",
                    table_id,
                    s.type_name()
                ),
            };
        }

        Self {
            table_ids,
            partition_key,
            meta,
            min_timestamp: stats.min.unwrap(),
            max_timestamp: stats.max.unwrap(),
            namespace_id,
        }
    }

    /// Metadata associated with this write
    pub fn meta(&self) -> &DmlMeta {
        &self.meta
    }

    /// Set the metadata
    pub fn set_meta(&mut self, meta: DmlMeta) {
        self.meta = meta
    }

    /// Returns an iterator over the per-table writes within this [`DmlWrite`]
    /// in no particular order
    pub fn tables(&self) -> impl Iterator<Item = (&TableId, &MutableBatch)> + '_ {
        self.table_ids.iter()
    }

    /// Consumes `self`, returning an iterator of the table ID and data contained within it.
    pub fn into_tables(self) -> impl Iterator<Item = (TableId, MutableBatch)> {
        self.table_ids.into_iter()
    }

    /// Gets the write for a given table
    pub fn table(&self, id: &TableId) -> Option<&MutableBatch> {
        self.table_ids.get(id)
    }

    /// Returns the number of tables within this write
    pub fn table_count(&self) -> usize {
        self.table_ids.len()
    }

    /// Returns the minimum timestamp in the write
    pub fn min_timestamp(&self) -> i64 {
        self.min_timestamp
    }

    /// Returns the maximum timestamp in the write
    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }

    /// Return the approximate memory size of the write, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .table_ids
                .values()
                .map(|v| std::mem::size_of::<TableId>() + v.size())
                .sum::<usize>()
            + self.meta.size()
            + std::mem::size_of::<NamespaceId>()
            + std::mem::size_of::<PartitionKey>()
            - std::mem::size_of::<DmlMeta>()
    }

    /// Return the partition key derived for this op.
    pub fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    /// Return the [`NamespaceId`] to which this [`DmlWrite`] should be applied.
    pub fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }
}

/// A delete operation
#[derive(Debug, Clone, PartialEq)]
pub struct DmlDelete {
    namespace_id: NamespaceId,
    predicate: DeletePredicate,
    table_name: Option<NonEmptyString>,
    meta: DmlMeta,
}

impl DmlDelete {
    /// Create a new [`DmlDelete`]
    pub fn new(
        namespace_id: NamespaceId,
        predicate: DeletePredicate,
        table_name: Option<NonEmptyString>,
        meta: DmlMeta,
    ) -> Self {
        Self {
            namespace_id,
            predicate,
            table_name,
            meta,
        }
    }

    /// Returns the table_name for this delete
    pub fn table_name(&self) -> Option<&str> {
        self.table_name.as_deref()
    }

    /// Returns the [`DeletePredicate`]
    pub fn predicate(&self) -> &DeletePredicate {
        &self.predicate
    }

    /// Returns the [`DmlMeta`]
    pub fn meta(&self) -> &DmlMeta {
        &self.meta
    }

    /// Sets the [`DmlMeta`] for this [`DmlDelete`]
    pub fn set_meta(&mut self, meta: DmlMeta) {
        self.meta = meta
    }

    /// Return the approximate memory size of the delete, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.predicate.size() - std::mem::size_of::<DeletePredicate>()
            + self
                .table_name
                .as_ref()
                .map(|s| s.len())
                .unwrap_or_default()
            + self.meta.size()
            - std::mem::size_of::<DmlMeta>()
    }

    /// Return the [`NamespaceId`] to which this operation should be applied.
    pub fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }
}

/// A sequence number for writes from a particular ingester instance
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SequenceNumber(i64);

#[allow(missing_docs)]
impl SequenceNumber {
    pub fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

/// A space-efficient encoded set of [`SequenceNumber`].
#[derive(Debug, Default, Clone, PartialEq)]
pub struct SequenceNumberSet(croaring::Treemap);

impl SequenceNumberSet {
    /// Add the specified [`SequenceNumber`] to the set.
    pub fn add(&mut self, n: SequenceNumber) {
        self.0.add(n.get() as _);
    }

    /// Remove the specified [`SequenceNumber`] to the set, if present.
    ///
    /// This is a no-op if `n` was not part of `self`.
    pub fn remove(&mut self, n: SequenceNumber) {
        self.0.remove(n.get() as _);
    }

    /// Add all the [`SequenceNumber`] in `other` to `self`.
    ///
    /// The result of this operation is the set union of both input sets.
    pub fn add_set(&mut self, other: &Self) {
        self.0.or_inplace(&other.0)
    }

    /// Remove all the [`SequenceNumber`] in `other` from `self`.
    pub fn remove_set(&mut self, other: &Self) {
        self.0.andnot_inplace(&other.0)
    }

    /// Reduce the memory usage of this set (trading off immediate CPU time) by
    /// efficiently re-encoding the set (using run-length encoding).
    pub fn run_optimise(&mut self) {
        self.0.run_optimize();
    }

    /// Serialise `self` into an array of bytes.
    ///
    /// [This document][spec] describes the serialised format.
    ///
    /// [spec]: https://github.com/RoaringBitmap/RoaringFormatSpec/
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0
            .serialize()
            .expect("failed to serialise sequence number set")
    }

    /// Return true if the specified [`SequenceNumber`] has been added to
    /// `self`.
    pub fn contains(&self, n: SequenceNumber) -> bool {
        self.0.contains(n.get() as _)
    }

    /// Returns the number of [`SequenceNumber`] in this set.
    pub fn len(&self) -> u64 {
        self.0.cardinality()
    }

    /// Return `true` if there are no [`SequenceNumber`] in this set.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Return an iterator of all [`SequenceNumber`] in this set.
    pub fn iter(&self) -> impl Iterator<Item = SequenceNumber> + '_ {
        self.0.iter().map(|v| SequenceNumber::new(v as _))
    }

    /// Initialise a [`SequenceNumberSet`] that is pre-allocated to contain up
    /// to `n` elements without reallocating.
    pub fn with_capacity(n: u32) -> Self {
        let mut map = BTreeMap::new();
        map.insert(0, croaring::Bitmap::create_with_capacity(n));
        Self(croaring::Treemap { map })
    }
}

/// Deserialisation method.
impl TryFrom<&[u8]> for SequenceNumberSet {
    type Error = String;

    fn try_from(buffer: &[u8]) -> Result<Self, Self::Error> {
        croaring::Treemap::deserialize(buffer)
            .map(SequenceNumberSet)
            .map_err(|e| format!("failed to deserialise sequence number set: {e}"))
    }
}

impl Extend<SequenceNumber> for SequenceNumberSet {
    fn extend<T: IntoIterator<Item = SequenceNumber>>(&mut self, iter: T) {
        self.0.extend(iter.into_iter().map(|v| v.get() as _))
    }
}

impl FromIterator<SequenceNumber> for SequenceNumberSet {
    fn from_iter<T: IntoIterator<Item = SequenceNumber>>(iter: T) -> Self {
        Self(iter.into_iter().map(|v| v.get() as _).collect())
    }
}

/// Return the intersection of `self` and `other`.
pub fn intersect(a: &SequenceNumberSet, b: &SequenceNumberSet) -> SequenceNumberSet {
    SequenceNumberSet(a.0.and(&b.0))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use proptest::{prelude::prop, proptest, strategy::Strategy};

    use super::*;

    #[test]
    fn test_set_operations() {
        let mut a = SequenceNumberSet::default();
        let mut b = SequenceNumberSet::default();

        // Add an element and check it is readable
        a.add(SequenceNumber::new(1));
        assert!(a.contains(SequenceNumber::new(1)));
        assert_eq!(a.len(), 1);
        assert_eq!(a.iter().collect::<Vec<_>>(), vec![SequenceNumber::new(1)]);
        assert!(!a.contains(SequenceNumber::new(42)));

        // Merging an empty set into a should not change a
        a.add_set(&b);
        assert_eq!(a.len(), 1);
        assert!(a.contains(SequenceNumber::new(1)));

        // Merging a non-empty set should add the new elements
        b.add(SequenceNumber::new(2));
        a.add_set(&b);
        assert_eq!(a.len(), 2);
        assert!(a.contains(SequenceNumber::new(1)));
        assert!(a.contains(SequenceNumber::new(2)));

        // Removing the set should return it to the pre-merged state.
        a.remove_set(&b);
        assert_eq!(a.len(), 1);
        assert!(a.contains(SequenceNumber::new(1)));

        // Removing a non-existant element should be a NOP
        a.remove(SequenceNumber::new(42));
        assert_eq!(a.len(), 1);

        // Removing the last element should result in an empty set.
        a.remove(SequenceNumber::new(1));
        assert_eq!(a.len(), 0);
    }

    #[test]
    fn test_extend() {
        let mut a = SequenceNumberSet::default();
        a.add(SequenceNumber::new(42));

        let extend_set = [SequenceNumber::new(4), SequenceNumber::new(2)];

        assert!(a.contains(SequenceNumber::new(42)));
        assert!(!a.contains(SequenceNumber::new(4)));
        assert!(!a.contains(SequenceNumber::new(2)));

        a.extend(extend_set);

        assert!(a.contains(SequenceNumber::new(42)));
        assert!(a.contains(SequenceNumber::new(4)));
        assert!(a.contains(SequenceNumber::new(2)));
    }

    #[test]
    fn test_collect() {
        let collect_set = [SequenceNumber::new(4), SequenceNumber::new(2)];

        let a = collect_set.into_iter().collect::<SequenceNumberSet>();

        assert!(!a.contains(SequenceNumber::new(42)));
        assert!(a.contains(SequenceNumber::new(4)));
        assert!(a.contains(SequenceNumber::new(2)));
    }

    #[test]
    fn test_partial_eq() {
        let mut a = SequenceNumberSet::default();
        let mut b = SequenceNumberSet::default();

        assert_eq!(a, b);

        a.add(SequenceNumber::new(42));
        assert_ne!(a, b);

        b.add(SequenceNumber::new(42));
        assert_eq!(a, b);

        b.add(SequenceNumber::new(24));
        assert_ne!(a, b);

        a.add(SequenceNumber::new(24));
        assert_eq!(a, b);
    }

    #[test]
    fn test_intersect() {
        let a = [0, i64::MAX, 40, 41, 42, 43, 44, 45]
            .into_iter()
            .map(SequenceNumber::new)
            .collect::<SequenceNumberSet>();

        let b = [1, 5, i64::MAX, 42]
            .into_iter()
            .map(SequenceNumber::new)
            .collect::<SequenceNumberSet>();

        let intersection = intersect(&a, &b);
        let want = [i64::MAX, 42]
            .into_iter()
            .map(SequenceNumber::new)
            .collect::<SequenceNumberSet>();

        assert_eq!(intersection, want);
    }

    /// Yield vec's of [`SequenceNumber`] derived from u64 values and cast to
    /// i64.
    ///
    /// This matches how the ingester allocates [`SequenceNumber`] - from a u64
    /// source.
    fn sequence_number_vec() -> impl Strategy<Value = Vec<SequenceNumber>> {
        prop::collection::vec(0..u64::MAX, 0..1024).prop_map(|vec| {
            vec.into_iter()
                .map(|v| SequenceNumber::new(v as i64))
                .collect()
        })
    }

    // The following tests compare to an order-independent HashSet, as the
    // SequenceNumber uses the PartialOrd impl of the inner i64 for ordering,
    // resulting in incorrect output when compared to an ordered set of cast as
    // u64.
    //
    //      https://github.com/influxdata/influxdb_iox/issues/7260
    //
    // These tests also cover, collect()-ing to a SequenceNumberSet, etc.
    proptest! {
        /// Perform a SequenceNumberSet intersection test comparing the results
        /// to the known-good stdlib HashSet intersection implementation.
        #[test]
        fn prop_set_intersection(
                a in sequence_number_vec(),
                b in sequence_number_vec()
            ) {
            let known_a = a.iter().cloned().collect::<HashSet<_>>();
            let known_b = b.iter().cloned().collect::<HashSet<_>>();
            let set_a = a.into_iter().collect::<SequenceNumberSet>();
            let set_b = b.into_iter().collect::<SequenceNumberSet>();

            // The sets should be equal
            assert_eq!(set_a.iter().collect::<HashSet<_>>(), known_a, "set a does not match");
            assert_eq!(set_b.iter().collect::<HashSet<_>>(), known_b, "set b does not match");

            let known_intersection = known_a.intersection(&known_b).cloned().collect::<HashSet<_>>();
            let set_intersection = intersect(&set_a, &set_b).iter().collect::<HashSet<_>>();

            // The set intersections should be equal.
            assert_eq!(set_intersection, known_intersection);
        }

        /// Perform a SequenceNumberSet remove_set test comparing the results to
        /// the known-good stdlib HashSet difference implementation.
        #[test]
        fn prop_set_difference(
            a in sequence_number_vec(),
            b in sequence_number_vec()
        ) {
            let known_a = a.iter().cloned().collect::<HashSet<_>>();
            let known_b = b.iter().cloned().collect::<HashSet<_>>();
            let mut set_a = a.into_iter().collect::<SequenceNumberSet>();
            let set_b = b.into_iter().collect::<SequenceNumberSet>();

            // The sets should be equal
            assert_eq!(set_a.iter().collect::<HashSet<_>>(), known_a, "set a does not match");
            assert_eq!(set_b.iter().collect::<HashSet<_>>(), known_b, "set b does not match");

            let known_a = known_a.difference(&known_b).cloned().collect::<HashSet<_>>();
            set_a.remove_set(&set_b);
            let set_a = set_a.iter().collect::<HashSet<_>>();

            // The set difference should be equal.
            assert_eq!(set_a, known_a);
        }

        /// Perform a SequenceNumberSet add_set test comparing the results to
        /// the known-good stdlib HashSet or implementation.
        #[test]
        fn prop_set_add(
            a in sequence_number_vec(),
            b in sequence_number_vec()
        ) {
            let known_a = a.iter().cloned().collect::<HashSet<_>>();
            let known_b = b.iter().cloned().collect::<HashSet<_>>();
            let mut set_a = a.into_iter().collect::<SequenceNumberSet>();
            let set_b = b.into_iter().collect::<SequenceNumberSet>();

            // The sets should be equal
            assert_eq!(set_a.iter().collect::<HashSet<_>>(), known_a, "set a does not match");
            assert_eq!(set_b.iter().collect::<HashSet<_>>(), known_b, "set b does not match");

            let known_a = known_a.union(&known_b).cloned().collect::<HashSet<_>>();
            set_a.add_set(&set_b);
            let set_a = set_a.iter().collect::<HashSet<_>>();

            // The sets should be equal.
            assert_eq!(set_a, known_a);
        }

        /// Assert that a SequenceNumberSet deserialised from its serialised
        /// representation is equal (round-trippable).
        #[test]
        fn prop_serialise_deserialise(
            a in sequence_number_vec()
        ) {
            let orig = a.iter().cloned().collect::<SequenceNumberSet>();

            let serialised = orig.to_bytes();
            let got = SequenceNumberSet::try_from(&*serialised).expect("failed to deserialise valid set");

            assert_eq!(got, orig);
        }
    }
}
