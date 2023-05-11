//! Shared data types

// `clippy::use_self` is deliberately excluded from the lints this crate uses.
// See <https://github.com/rust-lang/rust-clippy/issues/6902>.
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro
)]

pub mod sequence_number_set;

mod columns;
pub use columns::*;
mod namespace_name;
pub use namespace_name::*;
mod partition_template;
pub use partition_template::*;

use observability_deps::tracing::warn;
use schema::{sort::SortKey, TIME_COLUMN_NAME};
use std::{
    borrow::Borrow,
    collections::{BTreeMap, BTreeSet, HashMap},
    convert::TryFrom,
    fmt::{Display, Write},
    mem::{self, size_of_val},
    num::{FpCategory, NonZeroU64},
    ops::{Add, Deref, Sub},
    sync::Arc,
};
use uuid::Uuid;

/// Compaction levels
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash, sqlx::Type)]
#[repr(i16)]
pub enum CompactionLevel {
    /// The starting compaction level for parquet files persisted by an Ingester is zero.
    Initial = 0,
    /// Level of files persisted by a Compactor that do not overlap with non-level-0 files.
    FileNonOverlapped = 1,
    /// Level of files persisted by a Compactor that are fully compacted and should not be
    /// recompacted unless a new overlapping Initial level file arrives
    Final = 2,
}

impl Display for CompactionLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initial => write!(f, "CompactionLevel::L0"),
            Self::FileNonOverlapped => write!(f, "CompactionLevel::L1"),
            Self::Final => write!(f, "CompactionLevel::L2"),
        }
    }
}

impl TryFrom<i32> for CompactionLevel {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            x if x == Self::Initial as i32 => Ok(Self::Initial),
            x if x == Self::FileNonOverlapped as i32 => Ok(Self::FileNonOverlapped),
            x if x == Self::Final as i32 => Ok(Self::Final),
            _ => Err("invalid compaction level value".into()),
        }
    }
}

impl CompactionLevel {
    /// When compacting files of this level, provide the level that the resulting file should be.
    /// Does not exceed the maximum available level.
    pub fn next(&self) -> Self {
        match self {
            Self::Initial => Self::FileNonOverlapped,
            Self::FileNonOverlapped => Self::Final,
            Self::Final => Self::Final,
        }
    }

    /// Return previous level
    pub fn prev(&self) -> Self {
        match self {
            Self::Initial => Self::Initial,
            Self::FileNonOverlapped => Self::Initial,
            Self::Final => Self::FileNonOverlapped,
        }
    }

    /// Returns all levels
    pub fn all() -> &'static [Self] {
        &[Self::Initial, Self::FileNonOverlapped, Self::Final]
    }

    /// Static name
    pub fn name(&self) -> &'static str {
        match self {
            Self::Initial => "L0",
            Self::FileNonOverlapped => "L1",
            Self::Final => "L2",
        }
    }
}

/// Unique ID for a `Namespace`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct NamespaceId(i64);

#[allow(missing_docs)]
impl NamespaceId {
    pub const fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique ID for a `Table`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct TableId(i64);

#[allow(missing_docs)]
impl TableId {
    pub const fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique ID for a `Partition`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, sqlx::FromRow)]
#[sqlx(transparent)]
pub struct PartitionId(i64);

#[allow(missing_docs)]
impl PartitionId {
    pub const fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for PartitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A sequence number from a `router::Shard` (kafka partition)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
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

impl Add<i64> for SequenceNumber {
    type Output = Self;

    fn add(self, other: i64) -> Self {
        Self(self.0 + other)
    }
}

impl Sub<i64> for SequenceNumber {
    type Output = Self;

    fn sub(self, other: i64) -> Self {
        Self(self.0 - other)
    }
}

/// A time in nanoseconds from epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct Timestamp(i64);

#[allow(missing_docs)]
impl Timestamp {
    pub fn new(v: i64) -> Self {
        Self(v)
    }

    pub fn get(&self) -> i64 {
        self.0
    }
}

impl From<iox_time::Time> for Timestamp {
    fn from(time: iox_time::Time) -> Self {
        Self::new(time.timestamp_nanos())
    }
}

impl From<Timestamp> for iox_time::Time {
    fn from(time: Timestamp) -> iox_time::Time {
        iox_time::Time::from_timestamp_nanos(time.get())
    }
}

impl Add for Timestamp {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self(self.0.checked_add(other.0).expect("timestamp wraparound"))
    }
}

impl Sub for Timestamp {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self(self.0.checked_sub(other.0).expect("timestamp wraparound"))
    }
}

impl Add<i64> for Timestamp {
    type Output = Self;

    fn add(self, rhs: i64) -> Self::Output {
        self + Self(rhs)
    }
}

impl Sub<i64> for Timestamp {
    type Output = Self;

    fn sub(self, rhs: i64) -> Self::Output {
        self - Self(rhs)
    }
}

/// Unique ID for a `ParquetFile`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct ParquetFileId(i64);

#[allow(missing_docs)]
impl ParquetFileId {
    pub fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for ParquetFileId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // Use `self.number` to refer to each positional data point.
        write!(f, "{}", self.0)
    }
}

/// Data object for a namespace
#[derive(Debug, Clone)]
pub struct Namespace {
    /// The id of the namespace
    pub id: NamespaceId,
    /// The unique name of the namespace
    pub name: String,
    /// The retention period in ns. None represents infinite duration (i.e. never drop data).
    pub retention_period_ns: Option<i64>,
    /// The maximum number of tables that can exist in this namespace
    pub max_tables: i32,
    /// The maximum number of columns per table in this namespace
    pub max_columns_per_table: i32,
    /// When this file was marked for deletion.
    pub deleted_at: Option<Timestamp>,
    /// The partition template to use for writes in this namespace.
    pub partition_template: Arc<sqlx::types::JsonRawValue>,
}

/// [`sqlx::types::JsonRawValue`] (which is really [`serde_json::RawValue`]) does not implement
/// `PartialEq`, so we have to write a custom implementation. This compares the string values
/// exactly, and they should be the same if we've generated both of them the same way.
impl PartialEq for Namespace {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.name == other.name
            && self.retention_period_ns == other.retention_period_ns
            && self.max_tables == other.max_tables
            && self.max_columns_per_table == other.max_columns_per_table
            && self.deleted_at == other.deleted_at
            && self.partition_template.get() == other.partition_template.get()
    }
}
impl Eq for Namespace {}

/// Schema collection for a namespace. This is an in-memory object useful for a schema
/// cache.
#[derive(Debug, Clone)]
pub struct NamespaceSchema {
    /// the namespace id
    pub id: NamespaceId,
    /// the tables in the namespace by name
    pub tables: BTreeMap<String, TableSchema>,
    /// the number of columns per table this namespace allows
    pub max_columns_per_table: usize,
    /// The maximum number of tables permitted in this namespace.
    pub max_tables: usize,
    /// The retention period in ns.
    /// None represents infinite duration (i.e. never drop data).
    pub retention_period_ns: Option<i64>,
    /// The partition template to use for writes in this namespace.
    pub partition_template: Arc<sqlx::types::JsonRawValue>,
}

/// [`sqlx::types::JsonRawValue`] (which is really [`serde_json::RawValue`]) does not implement
/// `PartialEq`, so we have to write a custom implementation. This compares the string values
/// exactly, and they should be the same if we've generated both of them the same way.
impl PartialEq for NamespaceSchema {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.tables == other.tables
            && self.max_columns_per_table == other.max_columns_per_table
            && self.max_tables == other.max_tables
            && self.retention_period_ns == other.retention_period_ns
            && self.partition_template.get() == other.partition_template.get()
    }
}
impl Eq for NamespaceSchema {}

impl NamespaceSchema {
    /// Start a new `NamespaceSchema` with empty `tables` but the rest of the information populated
    /// from the given `Namespace`.
    pub fn new_empty_from(namespace: &Namespace) -> Self {
        let &Namespace {
            id,
            retention_period_ns,
            max_tables,
            max_columns_per_table,
            ref partition_template,
            ..
        } = namespace;

        Self {
            id,
            tables: BTreeMap::new(),
            max_columns_per_table: max_columns_per_table as usize,
            max_tables: max_tables as usize,
            retention_period_ns,
            partition_template: Arc::clone(&partition_template),
        }
    }

    /// Estimated Size in bytes including `self`.
    pub fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .tables
                .iter()
                .map(|(k, v)| size_of_val(k) + k.capacity() + v.size())
                .sum::<usize>()
    }
}

/// Data object for a table
#[derive(Debug, Clone)]
pub struct Table {
    /// The id of the table
    pub id: TableId,
    /// The namespace id that the table is in
    pub namespace_id: NamespaceId,
    /// The name of the table, which is unique within the associated namespace
    pub name: String,
    /// The partition template to use for writes in this table.
    pub partition_template: Arc<sqlx::types::JsonRawValue>,
}

/// [`sqlx::types::JsonRawValue`] (which is really [`serde_json::RawValue`]) does not implement
/// `PartialEq`, so we have to write a custom implementation. This compares the string values
/// exactly, and they should be the same if we've generated both of them the same way.
impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.namespace_id == other.namespace_id
            && self.name == other.name
            && self.partition_template.get() == other.partition_template.get()
    }
}
impl Eq for Table {}

/// Column definitions for a table
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// the table id
    pub id: TableId,

    /// the table's partition template
    partition_template: Arc<sqlx::types::JsonRawValue>,

    /// the table's columns by their name
    pub columns: ColumnsByName,
}

/// [`sqlx::types::JsonRawValue`] (which is really [`serde_json::RawValue`]) does not implement
/// `PartialEq`, so we have to write a custom implementation. This compares the string values
/// exactly, and they should be the same if we've generated both of them the same way.
impl PartialEq for TableSchema {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.partition_template.get() == other.partition_template.get()
            && self.columns == other.columns
    }
}
impl Eq for TableSchema {}

impl TableSchema {
    /// Initialize new `TableSchema` from the information in the given `Table`.
    pub fn new_empty_from(table: &Table) -> Self {
        Self {
            id: table.id,
            partition_template: Arc::clone(&table.partition_template),
            columns: ColumnsByName::new([]),
        }
    }

    /// The partition template override to use when partitioning writes to this table.
    pub fn partition_template(&self) -> TablePartitionTemplateOverride {
        TablePartitionTemplateOverride::new(Arc::clone(&self.partition_template))
    }

    /// Add `col` to this table schema.
    ///
    /// # Panics
    ///
    /// This method panics if a column of the same name already exists in
    /// `self`, or if `col` references a different `table_id`.
    pub fn add_column(&mut self, col: Column) {
        let Column {
            id,
            name,
            column_type,
            table_id,
        } = col;

        assert_eq!(table_id, self.id);

        let column_schema = ColumnSchema { id, column_type };
        self.add_column_schema(name, column_schema);
    }

    /// Add the name and column schema to this table's schema.
    ///
    /// # Panics
    ///
    /// This method panics if a column of the same name already exists in
    /// `self`.
    pub fn add_column_schema(&mut self, column_name: String, column_schema: ColumnSchema) {
        self.columns.add_column(column_name, column_schema);
    }

    /// Estimated Size in bytes including `self`.
    pub fn size(&self) -> usize {
        size_of_val(self)
            + self
                .columns
                .iter()
                .map(|(k, v)| size_of_val(k) + k.capacity() + size_of_val(v))
                .sum::<usize>()
    }

    /// Create `ID->name` map for columns.
    pub fn column_id_map(&self) -> HashMap<ColumnId, &str> {
        self.columns.id_map()
    }

    /// Whether a column with this name is in the schema.
    pub fn contains_column_name(&self, name: &str) -> bool {
        self.columns.contains_column_name(name)
    }

    /// Return the set of column names for this table. Used in combination with a write operation's
    /// column names to determine whether a write would exceed the max allowed columns.
    pub fn column_names(&self) -> BTreeSet<&str> {
        self.columns.names()
    }

    /// Return number of columns of the table
    pub fn column_count(&self) -> usize {
        self.columns.column_count()
    }
}

/// Defines an partition via an arbitrary string within a table within
/// a namespace.
///
/// Implemented as a reference-counted string, serialisable to
/// the Postgres VARCHAR data type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionKey(Arc<str>);

impl PartitionKey {
    /// Returns true if this instance of [`PartitionKey`] is backed by the same
    /// string storage as other.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Display for PartitionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for PartitionKey {
    fn from(s: String) -> Self {
        assert!(!s.is_empty());
        Self(s.into())
    }
}

impl From<&str> for PartitionKey {
    fn from(s: &str) -> Self {
        assert!(!s.is_empty());
        Self(s.into())
    }
}

impl sqlx::Type<sqlx::Postgres> for PartitionKey {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        // Store this type as VARCHAR
        sqlx::postgres::PgTypeInfo::with_name("VARCHAR")
    }
}

impl sqlx::Encode<'_, sqlx::Postgres> for PartitionKey {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::database::HasArguments<'_>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        <&str as sqlx::Encode<sqlx::Postgres>>::encode(&self.0, buf)
    }
}

impl sqlx::Decode<'_, sqlx::Postgres> for PartitionKey {
    fn decode(
        value: <sqlx::Postgres as sqlx::database::HasValueRef<'_>>::ValueRef,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        Ok(Self(
            <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?.into(),
        ))
    }
}

impl sqlx::Type<sqlx::Sqlite> for PartitionKey {
    fn type_info() -> sqlx::sqlite::SqliteTypeInfo {
        <String as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

impl sqlx::Encode<'_, sqlx::Sqlite> for PartitionKey {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Sqlite as sqlx::database::HasArguments<'_>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        <String as sqlx::Encode<sqlx::Sqlite>>::encode(self.0.to_string(), buf)
    }
}

impl sqlx::Decode<'_, sqlx::Sqlite> for PartitionKey {
    fn decode(
        value: <sqlx::Sqlite as sqlx::database::HasValueRef<'_>>::ValueRef,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        Ok(Self(
            <String as sqlx::Decode<sqlx::Sqlite>>::decode(value)?.into(),
        ))
    }
}

/// Data object for a partition. The combination of table and key are unique (i.e. only one record
/// can exist for each combo)
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct Partition {
    /// the id of the partition
    pub id: PartitionId,
    /// the table the partition is under
    pub table_id: TableId,
    /// the string key of the partition
    pub partition_key: PartitionKey,
    /// vector of column names that describes how *every* parquet file
    /// in this [`Partition`] is sorted. The sort_key contains all the
    /// primary key (PK) columns that have been persisted, and nothing
    /// else. The PK columns are all `tag` columns and the `time`
    /// column.
    ///
    /// Even though it is possible for both the unpersisted data
    /// and/or multiple parquet files to contain different subsets of
    /// columns, the partition's sort_key is guaranteed to be
    /// "compatible" across all files. Compatible means that the
    /// parquet file is sorted in the same order as the partition
    /// sort_key after removing any missing columns.
    ///
    /// Partitions are initially created before any data is persisted
    /// with an empty sort_key. The partition sort_key is updated as
    /// needed when data is persisted to parquet files: both on the
    /// first persist when the sort key is empty, as on subsequent
    /// persist operations when new tags occur in newly inserted data.
    ///
    /// Updating inserts new column into the existing order. The order
    /// of the existing columns relative to each other is NOT changed.
    ///
    /// For example, updating `A,B,C` to either `A,D,B,C` or `A,B,C,D`
    /// is legal. However, updating to `A,C,D,B` is not because the
    /// relative order of B and C have been reversed.
    pub sort_key: Vec<String>,

    /// The time at which the newest file of the partition is created
    pub new_file_at: Option<Timestamp>,
}

impl Partition {
    /// The sort key for the partition, if present, structured as a `SortKey`
    pub fn sort_key(&self) -> Option<SortKey> {
        if self.sort_key.is_empty() {
            return None;
        }

        Some(SortKey::from_columns(self.sort_key.iter().map(|s| &**s)))
    }
}

/// Data recorded when compaction skips a partition.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::FromRow)]
pub struct SkippedCompaction {
    /// the partition
    pub partition_id: PartitionId,
    /// the reason compaction was skipped
    pub reason: String,
    /// when compaction was skipped
    pub skipped_at: Timestamp,
    /// estimated memory budget
    pub estimated_bytes: i64,
    /// limit on memory budget
    pub limit_bytes: i64,
    /// num files selected to compact
    pub num_files: i64,
    /// limit on num files
    pub limit_num_files: i64,
    /// limit on num files for the first file in a partition
    pub limit_num_files_first_in_partition: i64,
}

/// Data for a parquet file reference that has been inserted in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct ParquetFile {
    /// the id of the file in the catalog
    pub id: ParquetFileId,
    /// the namespace
    pub namespace_id: NamespaceId,
    /// the table
    pub table_id: TableId,
    /// the partition
    pub partition_id: PartitionId,
    /// the uuid used in the object store path for this file
    pub object_store_id: Uuid,
    /// the min timestamp of data in this file
    pub min_time: Timestamp,
    /// the max timestamp of data in this file
    pub max_time: Timestamp,
    /// When this file was marked for deletion
    pub to_delete: Option<Timestamp>,
    /// file size in bytes
    pub file_size_bytes: i64,
    /// the number of rows of data in this file
    pub row_count: i64,
    /// The compaction level of the file.
    ///
    ///  * 0 (`CompactionLevel::Initial`): represents a level-0 file that is persisted by an
    ///      Ingester. Partitions with level-0 files are usually hot/recent partitions.
    ///  * 1 (`CompactionLevel::FileOverlapped`): represents a level-1 file that is persisted by a
    ///      Compactor and potentially overlaps with other level-1 files. Partitions with level-1
    ///      files are partitions with a lot of or/and large overlapped files that have to go
    ///      through many compaction cycles before they are fully compacted to non-overlapped
    ///      files.
    ///  * 2 (`CompactionLevel::FileNonOverlapped`): represents a level-1 file that is persisted by
    ///      a Compactor and does not overlap with other files except level 0 ones. Eventually,
    ///      cold partitions (partitions that no longer needs to get compacted) will only include
    ///      one or many level-1 files
    pub compaction_level: CompactionLevel,
    /// the creation time of the parquet file
    pub created_at: Timestamp,
    /// Set of columns within this parquet file.
    ///
    /// # Relation to Table-wide Column Set
    /// Columns within this set may or may not be part of the table-wide schema.
    ///
    /// Columns that are NOT part of the table-wide schema must be ignored. It is likely that these
    /// columns were originally part of the table but were later removed.
    ///
    /// # Column Types
    /// Column types are identical to the table-wide types.
    ///
    /// # Column Order & Sort Key
    /// The columns that are present in the table-wide schema are sorted according to the partition
    /// sort key. The occur in the parquet file according to this order.
    pub column_set: ColumnSet,
    /// the max of created_at of all L0 files needed for file/chunk ordering for deduplication
    pub max_l0_created_at: Timestamp,
}

impl ParquetFile {
    /// Create new file from given parameters and ID.
    ///
    /// [`to_delete`](Self::to_delete) will be set to `None`.
    pub fn from_params(params: ParquetFileParams, id: ParquetFileId) -> Self {
        Self {
            id,
            namespace_id: params.namespace_id,
            table_id: params.table_id,
            partition_id: params.partition_id,
            object_store_id: params.object_store_id,
            min_time: params.min_time,
            max_time: params.max_time,
            to_delete: None,
            file_size_bytes: params.file_size_bytes,
            row_count: params.row_count,
            compaction_level: params.compaction_level,
            created_at: params.created_at,
            column_set: params.column_set,
            max_l0_created_at: params.max_l0_created_at,
        }
    }

    /// Estimate the memory consumption of this object and its contents
    pub fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.column_set.size()
            - std::mem::size_of_val(&self.column_set)
    }

    /// Return true if the time range overlaps with the time range of the given file
    pub fn overlaps(&self, other: &Self) -> bool {
        self.min_time <= other.max_time && self.max_time >= other.min_time
    }

    /// Return true if the time range of this file overlaps with the given time range
    pub fn overlaps_time_range(&self, min_time: Timestamp, max_time: Timestamp) -> bool {
        self.min_time <= max_time && self.max_time >= min_time
    }
}

/// Data for a parquet file to be inserted into the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetFileParams {
    /// the namespace
    pub namespace_id: NamespaceId,
    /// the table
    pub table_id: TableId,
    /// the partition
    pub partition_id: PartitionId,
    /// the uuid used in the object store path for this file
    pub object_store_id: Uuid,
    /// the min timestamp of data in this file
    pub min_time: Timestamp,
    /// the max timestamp of data in this file
    pub max_time: Timestamp,
    /// file size in bytes
    pub file_size_bytes: i64,
    /// the number of rows of data in this file
    pub row_count: i64,
    /// the compaction level of the file
    pub compaction_level: CompactionLevel,
    /// the creation time of the parquet file
    pub created_at: Timestamp,
    /// columns in this file.
    pub column_set: ColumnSet,
    /// the max of created_at of all L0 files
    pub max_l0_created_at: Timestamp,
}

impl From<ParquetFile> for ParquetFileParams {
    fn from(value: ParquetFile) -> Self {
        Self {
            namespace_id: value.namespace_id,
            table_id: value.table_id,
            partition_id: value.partition_id,
            object_store_id: value.object_store_id,
            min_time: value.min_time,
            max_time: value.max_time,
            file_size_bytes: value.file_size_bytes,
            row_count: value.row_count,
            compaction_level: value.compaction_level,
            created_at: value.created_at,
            column_set: value.column_set,
            max_l0_created_at: value.max_l0_created_at,
        }
    }
}

/// ID of a chunk.
///
/// This ID is unique within a single partition.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkId(Uuid);

impl ChunkId {
    /// Create new, random ID.
    #[allow(clippy::new_without_default)] // `new` creates non-deterministic result
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// **TESTING ONLY:** Create new ID from integer.
    ///
    /// Since this can easily lead to ID collisions (which in turn can lead to panics), this must
    /// only be used for testing purposes!
    pub fn new_test(id: u128) -> Self {
        Self(Uuid::from_u128(id))
    }

    /// The chunk id is only effective in case the chunk's order is the same with another chunk.
    /// Hence collisions are safe in that context.
    pub fn new_id(id: u128) -> Self {
        Self(Uuid::from_u128(id))
    }

    /// Get inner UUID.
    pub fn get(&self) -> Uuid {
        self.0
    }
}

impl std::fmt::Debug for ChunkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
    }
}

impl std::fmt::Display for ChunkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if (self.0.get_variant() == uuid::Variant::RFC4122)
            && (self.0.get_version() == Some(uuid::Version::Random))
        {
            f.debug_tuple("ChunkId").field(&self.0).finish()
        } else {
            f.debug_tuple("ChunkId").field(&self.0.as_u128()).finish()
        }
    }
}

impl From<Uuid> for ChunkId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

/// Order of a chunk.
///
/// This is used for:
/// 1. **upsert order:** chunks with higher order overwrite data in chunks with lower order
/// 2. **locking order:** chunks must be locked in consistent (ascending) order
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkOrder(i64);

impl ChunkOrder {
    /// The minimum ordering value a chunk could have. Currently only used in testing.
    pub const MIN: Self = Self(0);

    /// The maximum chunk order.
    pub const MAX: Self = Self(i64::MAX);

    /// Create a ChunkOrder from the given value.
    pub fn new(order: i64) -> Self {
        Self(order)
    }

    /// Under underlying order as integer.
    pub fn get(&self) -> i64 {
        self.0
    }
}

/// Represents a parsed delete predicate for evaluation by the InfluxDB IOx
/// query engine.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeletePredicate {
    /// Only rows within this range are included in
    /// results. Other rows are excluded.
    pub range: TimestampRange,

    /// Optional arbitrary predicates, represented as list of
    /// expressions applied a logical conjunction (aka they
    /// are 'AND'ed together). Only rows that evaluate to TRUE for all
    /// these expressions should be returned. Other rows are excluded
    /// from the results.
    pub exprs: Vec<DeleteExpr>,
}

impl DeletePredicate {
    /// Format expr to SQL string.
    pub fn expr_sql_string(&self) -> String {
        let mut out = String::new();
        for expr in &self.exprs {
            if !out.is_empty() {
                write!(&mut out, " AND ").expect("writing to a string shouldn't fail");
            }
            write!(&mut out, "{expr}").expect("writing to a string shouldn't fail");
        }
        out
    }

    /// Return the approximate memory size of the predicate, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.exprs.iter().map(|expr| expr.size()).sum::<usize>()
    }

    /// Return the delete predicate for data outside retention
    /// We need to only retain time >= retention_time.
    /// Thus we only need to set the range to MIN < time < retention_time
    pub fn retention_delete_predicate(retention_time: i64) -> Self {
        let range = TimestampRange {
            start: i64::MIN,
            end: retention_time,
        };
        Self {
            range,
            exprs: vec![],
        }
    }
}

/// Single expression to be used as parts of a predicate.
///
/// Only very simple expression of the type `<column> <op> <scalar>` are supported.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeleteExpr {
    /// Column (w/o table name).
    pub column: String,

    /// Operator.
    pub op: Op,

    /// Scalar value.
    pub scalar: Scalar,
}

impl DeleteExpr {
    /// Create a new [`DeleteExpr`]
    pub fn new(column: String, op: Op, scalar: Scalar) -> Self {
        Self { column, op, scalar }
    }

    /// Column (w/o table name).
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Operator.
    pub fn op(&self) -> Op {
        self.op
    }

    /// Scalar value.
    pub fn scalar(&self) -> &Scalar {
        &self.scalar
    }

    /// Return the approximate memory size of the expression, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.column.capacity() + self.scalar.size()
    }
}

impl std::fmt::Display for DeleteExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            r#""{}"{}{}"#,
            self.column().replace('\\', r#"\\"#).replace('"', r#"\""#),
            self.op(),
            self.scalar(),
        )
    }
}

/// Binary operator that can be evaluated on a column and a scalar value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Op {
    /// Strict equality (`=`).
    Eq,

    /// Inequality (`!=`).
    Ne,
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::Ne => write!(f, "!="),
        }
    }
}

/// Scalar value of a certain type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(missing_docs)]
pub enum Scalar {
    Bool(bool),
    I64(i64),
    F64(ordered_float::OrderedFloat<f64>),
    String(String),
}

impl Scalar {
    /// Return the approximate memory size of the scalar, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + match &self {
                Self::Bool(_) | Self::I64(_) | Self::F64(_) => 0,
                Self::String(s) => s.capacity(),
            }
    }
}

impl std::fmt::Display for Scalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scalar::Bool(value) => value.fmt(f),
            Scalar::I64(value) => value.fmt(f),
            Scalar::F64(value) => match value.classify() {
                FpCategory::Nan => write!(f, "'NaN'"),
                FpCategory::Infinite if *value.as_ref() < 0.0 => write!(f, "'-Infinity'"),
                FpCategory::Infinite => write!(f, "'Infinity'"),
                _ => write!(f, "{:?}", value.as_ref()),
            },
            Scalar::String(value) => {
                write!(
                    f,
                    "'{}'",
                    value.replace('\\', r#"\\"#).replace('\'', r#"\'"#),
                )
            }
        }
    }
}

/// A string that cannot be empty
///
/// This is particularly useful for types that map to/from protobuf, where string fields
/// are not nullable - that is they default to an empty string if not specified
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NonEmptyString(Box<str>);

impl NonEmptyString {
    /// Create a new `NonEmptyString` from the provided `String`
    ///
    /// Returns None if empty
    pub fn new(s: impl Into<String>) -> Option<Self> {
        let s = s.into();
        match s.is_empty() {
            true => None,
            false => Some(Self(s.into_boxed_str())),
        }
    }
}

impl Deref for NonEmptyString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

/// Column name, statistics which encode type information
#[derive(Debug, PartialEq, Clone)]
pub struct ColumnSummary {
    /// Column name
    pub name: String,

    /// Column's Influx data model type
    pub influxdb_type: InfluxDbType,

    /// Per column
    pub stats: Statistics,
}

impl ColumnSummary {
    /// Returns the total number of rows (including nulls) in this column
    pub fn total_count(&self) -> u64 {
        self.stats.total_count()
    }

    /// Updates statistics from other if the same type, otherwise a noop
    pub fn update_from(&mut self, other: &Self) {
        match (&mut self.stats, &other.stats) {
            (Statistics::F64(s), Statistics::F64(o)) => {
                s.update_from(o);
            }
            (Statistics::I64(s), Statistics::I64(o)) => {
                s.update_from(o);
            }
            (Statistics::Bool(s), Statistics::Bool(o)) => {
                s.update_from(o);
            }
            (Statistics::String(s), Statistics::String(o)) => {
                s.update_from(o);
            }
            (Statistics::U64(s), Statistics::U64(o)) => {
                s.update_from(o);
            }
            // do catch alls for the specific types, that way if a new type gets added, the compiler
            // will complain.
            (Statistics::F64(_), _) => unreachable!(),
            (Statistics::I64(_), _) => unreachable!(),
            (Statistics::U64(_), _) => unreachable!(),
            (Statistics::Bool(_), _) => unreachable!(),
            (Statistics::String(_), _) => unreachable!(),
        }
    }

    /// Updates these statistics so that that the total length of this
    /// column is `len` rows, padding it with trailing NULLs if
    /// necessary
    pub fn update_to_total_count(&mut self, len: u64) {
        let total_count = self.total_count();
        assert!(
            total_count <= len,
            "trying to shrink column stats from {total_count} to {len}"
        );
        let delta = len - total_count;
        self.stats.update_for_nulls(delta);
    }

    /// Return size in bytes of this Column metadata (not the underlying column)
    pub fn size(&self) -> usize {
        mem::size_of::<Self>() + self.name.len() + self.stats.size()
    }
}

// Replicate this enum here as it can't be derived from the existing statistics
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[allow(missing_docs)]
pub enum InfluxDbType {
    Tag,
    Field,
    Timestamp,
}

/// Summary statistics for a column.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StatValues<T> {
    /// minimum (non-NaN, non-NULL) value, if any
    pub min: Option<T>,

    /// maximum (non-NaN, non-NULL) value, if any
    pub max: Option<T>,

    /// total number of values in this column, including null values
    pub total_count: u64,

    /// number of null values in this column
    pub null_count: Option<u64>,

    /// number of distinct values in this column if known
    ///
    /// This includes NULLs and NANs
    pub distinct_count: Option<NonZeroU64>,
}

/// Represents the result of comparing the min/max ranges of two [`StatValues`]
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum StatOverlap {
    /// There is at least one value that exists in both ranges
    NonZero,

    /// There are zero values that exists in both ranges
    Zero,

    /// It is not known if there are any intersections (e.g. because
    /// one of the bounds is not Known / is None)
    Unknown,
}

impl<T> StatValues<T>
where
    T: PartialOrd,
{
    /// returns information about the overlap between two `StatValues`
    pub fn overlaps(&self, other: &Self) -> StatOverlap {
        match (&self.min, &self.max, &other.min, &other.max) {
            (Some(self_min), Some(self_max), Some(other_min), Some(other_max)) => {
                if self_min <= other_max && self_max >= other_min {
                    StatOverlap::NonZero
                } else {
                    StatOverlap::Zero
                }
            }
            // At least one of the values was None
            _ => StatOverlap::Unknown,
        }
    }
}

impl<T> Default for StatValues<T> {
    fn default() -> Self {
        Self {
            min: None,
            max: None,
            total_count: 0,
            null_count: None,
            distinct_count: None,
        }
    }
}

impl<T> StatValues<T> {
    /// Create new statistics with no values
    pub fn new_empty() -> Self {
        Self {
            min: None,
            max: None,
            total_count: 0,
            null_count: Some(0),
            distinct_count: None,
        }
    }

    /// Returns true if both the min and max values are None (aka not known)
    pub fn is_none(&self) -> bool {
        self.min.is_none() && self.max.is_none()
    }

    /// Update the statistics values to account for `num_nulls` additional null values
    pub fn update_for_nulls(&mut self, num_nulls: u64) {
        self.total_count += num_nulls;
        self.null_count = self.null_count.map(|x| x + num_nulls);
    }

    /// updates the statistics keeping the min, max and incrementing count.
    ///
    /// The type plumbing exists to allow calling with `&str` on a `StatValues<String>`.
    pub fn update<U: ?Sized>(&mut self, other: &U)
    where
        T: Borrow<U>,
        U: ToOwned<Owned = T> + PartialOrd + IsNan,
    {
        self.total_count += 1;
        self.distinct_count = None;

        if !other.is_nan() {
            match &self.min {
                None => self.min = Some(other.to_owned()),
                Some(s) => {
                    if s.borrow() > other {
                        self.min = Some(other.to_owned());
                    }
                }
            }

            match &self.max {
                None => {
                    self.max = Some(other.to_owned());
                }
                Some(s) => {
                    if other > s.borrow() {
                        self.max = Some(other.to_owned());
                    }
                }
            }
        }
    }
}

impl<T> StatValues<T>
where
    T: Clone + PartialOrd,
{
    /// Updates statistics from other
    pub fn update_from(&mut self, other: &Self) {
        self.total_count += other.total_count;
        self.null_count = self.null_count.zip(other.null_count).map(|(a, b)| a + b);

        // No way to accurately aggregate counts
        self.distinct_count = None;

        match (&self.min, &other.min) {
            (None, None) | (Some(_), None) => {}
            (None, Some(o)) => self.min = Some(o.clone()),
            (Some(s), Some(o)) => {
                if s > o {
                    self.min = Some(o.clone());
                }
            }
        }

        match (&self.max, &other.max) {
            (None, None) | (Some(_), None) => {}
            (None, Some(o)) => self.max = Some(o.clone()),
            (Some(s), Some(o)) => {
                if o > s {
                    self.max = Some(o.clone());
                }
            }
        };
    }
}

impl<T> StatValues<T>
where
    T: IsNan + PartialOrd,
{
    /// Create new statistics with the specified count and null count
    pub fn new(min: Option<T>, max: Option<T>, total_count: u64, null_count: Option<u64>) -> Self {
        let distinct_count = None;
        Self::new_with_distinct(min, max, total_count, null_count, distinct_count)
    }

    /// Create statistics for a column that only has nulls up to now
    pub fn new_all_null(total_count: u64, distinct_count: Option<u64>) -> Self {
        let min = None;
        let max = None;
        let null_count = Some(total_count);

        if let Some(count) = distinct_count {
            assert!(count > 0);
        }
        Self::new_with_distinct(
            min,
            max,
            total_count,
            null_count,
            distinct_count.map(|c| NonZeroU64::new(c).unwrap()),
        )
    }

    /// Create statistics for a column with zero nulls and unknown distinct count
    pub fn new_non_null(min: Option<T>, max: Option<T>, total_count: u64) -> Self {
        let null_count = Some(0);
        let distinct_count = None;
        Self::new_with_distinct(min, max, total_count, null_count, distinct_count)
    }

    /// Create new statistics with the specified count and null count and distinct values
    pub fn new_with_distinct(
        min: Option<T>,
        max: Option<T>,
        total_count: u64,
        null_count: Option<u64>,
        distinct_count: Option<NonZeroU64>,
    ) -> Self {
        if let Some(min) = &min {
            assert!(!min.is_nan());
        }
        if let Some(max) = &max {
            assert!(!max.is_nan());
        }
        if let (Some(min), Some(max)) = (&min, &max) {
            assert!(min <= max);
        }

        Self {
            min,
            max,
            total_count,
            null_count,
            distinct_count,
        }
    }
}

/// Whether a type is NaN or not.
pub trait IsNan {
    /// Test for NaNess.
    fn is_nan(&self) -> bool;
}

impl<T: IsNan> IsNan for &T {
    fn is_nan(&self) -> bool {
        (*self).is_nan()
    }
}

macro_rules! impl_is_nan_false {
    ($t:ty) => {
        impl IsNan for $t {
            fn is_nan(&self) -> bool {
                false
            }
        }
    };
}

impl_is_nan_false!(bool);
impl_is_nan_false!(str);
impl_is_nan_false!(String);
impl_is_nan_false!(i8);
impl_is_nan_false!(i16);
impl_is_nan_false!(i32);
impl_is_nan_false!(i64);
impl_is_nan_false!(u8);
impl_is_nan_false!(u16);
impl_is_nan_false!(u32);
impl_is_nan_false!(u64);

impl IsNan for f64 {
    fn is_nan(&self) -> bool {
        Self::is_nan(*self)
    }
}

/// Statistics and type information for a column.
#[derive(Debug, PartialEq, Clone)]
#[allow(missing_docs)]
pub enum Statistics {
    I64(StatValues<i64>),
    U64(StatValues<u64>),
    F64(StatValues<f64>),
    Bool(StatValues<bool>),
    String(StatValues<String>),
}

impl Statistics {
    /// Returns the total number of rows in this column
    pub fn total_count(&self) -> u64 {
        match self {
            Self::I64(s) => s.total_count,
            Self::U64(s) => s.total_count,
            Self::F64(s) => s.total_count,
            Self::Bool(s) => s.total_count,
            Self::String(s) => s.total_count,
        }
    }

    /// Returns true if both the min and max values are None (aka not known)
    pub fn is_none(&self) -> bool {
        match self {
            Self::I64(v) => v.is_none(),
            Self::U64(v) => v.is_none(),
            Self::F64(v) => v.is_none(),
            Self::Bool(v) => v.is_none(),
            Self::String(v) => v.is_none(),
        }
    }

    /// Returns the number of null rows in this column
    pub fn null_count(&self) -> Option<u64> {
        match self {
            Self::I64(s) => s.null_count,
            Self::U64(s) => s.null_count,
            Self::F64(s) => s.null_count,
            Self::Bool(s) => s.null_count,
            Self::String(s) => s.null_count,
        }
    }

    /// Returns the distinct count if known
    pub fn distinct_count(&self) -> Option<NonZeroU64> {
        match self {
            Self::I64(s) => s.distinct_count,
            Self::U64(s) => s.distinct_count,
            Self::F64(s) => s.distinct_count,
            Self::Bool(s) => s.distinct_count,
            Self::String(s) => s.distinct_count,
        }
    }

    /// Update the statistics values to account for `num_nulls` additional null values
    pub fn update_for_nulls(&mut self, num_nulls: u64) {
        match self {
            Self::I64(v) => v.update_for_nulls(num_nulls),
            Self::U64(v) => v.update_for_nulls(num_nulls),
            Self::F64(v) => v.update_for_nulls(num_nulls),
            Self::Bool(v) => v.update_for_nulls(num_nulls),
            Self::String(v) => v.update_for_nulls(num_nulls),
        }
    }

    /// Return the size in bytes of this stats instance
    pub fn size(&self) -> usize {
        match self {
            Self::String(v) => std::mem::size_of::<Self>() + v.string_size(),
            _ => std::mem::size_of::<Self>(),
        }
    }

    /// Return a human interpretable description of this type
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::I64(_) => "I64",
            Self::U64(_) => "U64",
            Self::F64(_) => "F64",
            Self::Bool(_) => "Bool",
            Self::String(_) => "String",
        }
    }

    /// Extract i64 type.
    pub fn as_i64(&self) -> Option<&StatValues<i64>> {
        match self {
            Self::I64(val) => Some(val),
            _ => None,
        }
    }
}

impl StatValues<String> {
    /// Returns the bytes associated by storing min/max string values
    pub fn string_size(&self) -> usize {
        self.min.as_ref().map(|x| x.len()).unwrap_or(0)
            + self.max.as_ref().map(|x| x.len()).unwrap_or(0)
    }
}

/// Metadata and statistics information for a table. This can be
/// either for the portion of a Table stored within a single chunk or
/// aggregated across chunks.
#[derive(Debug, PartialEq, Clone, Default)]
pub struct TableSummary {
    /// Per column statistics
    pub columns: Vec<ColumnSummary>,
}

impl TableSummary {
    /// Get the column summary by name.
    pub fn column(&self, name: &str) -> Option<&ColumnSummary> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Returns the total number of rows in the columns of this summary
    pub fn total_count(&self) -> u64 {
        // Assumes that all tables have the same number of rows, so
        // pick the first one
        let count = self.columns.get(0).map(|c| c.total_count()).unwrap_or(0);

        // Validate that the counts are consistent across columns
        for c in &self.columns {
            // Restore to assert when https://github.com/influxdata/influxdb_iox/issues/2124 is fixed
            if c.total_count() != count {
                warn!(column_name=%c.name,
                      column_count=c.total_count(), previous_count=count,
                      "Mismatch in statistics count, see #2124");
            }
        }
        count
    }

    /// Updates the table summary with combined stats from the other. Counts are
    /// treated as non-overlapping so they're just added together. If the
    /// type of a column differs between the two tables, no update is done
    /// on that column. Columns that only exist in the other are cloned into
    /// this table summary.
    pub fn update_from(&mut self, other: &Self) {
        let new_total_count = self.total_count() + other.total_count();

        // update all existing columns
        for col in &mut self.columns {
            if let Some(other_col) = other.column(&col.name) {
                col.update_from(other_col);
            } else {
                col.update_to_total_count(new_total_count);
            }
        }

        // Add any columns that were new
        for col in &other.columns {
            if self.column(&col.name).is_none() {
                let mut new_col = col.clone();
                // ensure the count is consistent
                new_col.update_to_total_count(new_total_count);
                self.columns.push(new_col);
            }
        }
    }

    /// Total size of all ColumnSummaries that belong to this table which include
    /// column names and their stats
    pub fn size(&self) -> usize {
        let size: usize = self.columns.iter().map(|c| c.size()).sum();
        size + mem::size_of::<Self>() // Add size of this struct that points to
                                      // table and ColumnSummary
    }

    /// Extracts min/max values of the timestamp column, if possible
    pub fn time_range(&self) -> Option<TimestampMinMax> {
        self.column(TIME_COLUMN_NAME).and_then(|c| {
            if let Statistics::I64(StatValues {
                min: Some(min),
                max: Some(max),
                ..
            }) = &c.stats
            {
                Some(TimestampMinMax::new(*min, *max))
            } else {
                None
            }
        })
    }
}

/// minimum time that can be represented.
///
/// 1677-09-21 00:12:43.145224194 +0000 UTC
///
/// The two lowest minimum integers are used as sentinel values.  The
/// minimum value needs to be used as a value lower than any other value for
/// comparisons and another separate value is needed to act as a sentinel
/// default value that is unusable by the user, but usable internally.
/// Because these two values need to be used for a special purpose, we do
/// not allow users to write points at these two times.
///
/// Source: [influxdb](https://github.com/influxdata/influxdb/blob/540bb66e1381a48a6d1ede4fc3e49c75a7d9f4af/models/time.go#L12-L34)
pub const MIN_NANO_TIME: i64 = i64::MIN + 2;

/// maximum time that can be represented.
///
/// 2262-04-11 23:47:16.854775806 +0000 UTC
///
/// The highest time represented by a nanosecond needs to be used for an exclusive range, so the
/// maximum time needs to be one less than the possible maximum number of nanoseconds representable
/// by an int64 so that we don't lose a point at that one time.
/// Source: [influxdb](https://github.com/influxdata/influxdb/blob/540bb66e1381a48a6d1ede4fc3e49c75a7d9f4af/models/time.go#L12-L34)
pub const MAX_NANO_TIME: i64 = i64::MAX - 1;

/// Specifies a continuous range of nanosecond timestamps. Timestamp
/// predicates are so common and critical to performance of timeseries
/// databases in general, and IOx in particular, that they are handled
/// specially
///
/// Timestamp ranges are defined such that a value `v` is within the
/// range iff:
///
/// ```text
///  range.start <= v < range.end
/// ```
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Copy, Debug, Hash)]
pub struct TimestampRange {
    /// Start defines the inclusive lower bound. Minimum value is [MIN_NANO_TIME]
    start: i64,
    /// End defines the exclusive upper bound. Maximum value is [MAX_NANO_TIME]
    end: i64,
}

impl TimestampRange {
    /// Create a new TimestampRange.
    ///
    /// Takes an inclusive start and an exclusive end. You may create an empty range by setting `start = end`.
    ///
    /// Clamps `start` to [`MIN_NANO_TIME`].
    /// end is unclamped. End may be set to `i64:MAX == MAX_NANO_TIME+1` to indicate that the upper bound is NOT
    /// restricted (this does NOT affect `start` though!).
    ///
    /// If `start > end`, this will be interpreted as an empty time range and `start` will be set to `end`.
    pub fn new(start: i64, end: i64) -> Self {
        let start = start.clamp(MIN_NANO_TIME, end);
        let end = end.max(MIN_NANO_TIME);
        Self { start, end }
    }

    /// Returns true if this range contains all representable timestamps
    pub fn contains_all(&self) -> bool {
        self.start <= MIN_NANO_TIME && self.end > MAX_NANO_TIME
    }

    /// Returns true if this range contains all representable timestamps except possibly MAX_NANO_TIME
    ///
    /// This is required for queries from InfluxQL, which are intended to be
    /// for all time but instead can be for [MIN_NANO_TIME, MAX_NANO_TIME).
    /// When <https://github.com/influxdata/idpe/issues/13094> is fixed,
    /// all uses of contains_nearly_all should be replaced by contains_all
    pub fn contains_nearly_all(&self) -> bool {
        self.start <= MIN_NANO_TIME && self.end >= MAX_NANO_TIME
    }

    #[inline]
    /// Returns true if this range contains the value v
    pub fn contains(&self, v: i64) -> bool {
        self.start <= v && v < self.end
    }

    /// Return the timestamp exclusive range's end.
    pub fn end(&self) -> i64 {
        self.end
    }

    /// Return the timestamp inclusive range's start.
    pub fn start(&self) -> i64 {
        self.start
    }
}

/// Specifies a min/max timestamp value.
///
/// Note this differs subtlety (but critically) from a
/// [`TimestampRange`] as the minimum and maximum values are included ([`TimestampRange`] has an exclusive end).
#[derive(Clone, Debug, Copy)]
pub struct TimestampMinMax {
    /// The minimum timestamp value
    pub min: i64,
    /// the maximum timestamp value
    pub max: i64,
}

impl TimestampMinMax {
    /// Create a new TimestampMinMax. Panics if min > max.
    pub fn new(min: i64, max: i64) -> Self {
        assert!(min <= max, "expected min ({min}) <= max ({max})");
        Self { min, max }
    }

    #[inline]
    /// Returns true if any of the values between min / max
    /// (inclusive) are contained within the specified timestamp range
    pub fn overlaps(&self, range: TimestampRange) -> bool {
        range.contains(self.min)
            || range.contains(self.max)
            || (self.min <= range.start && self.max >= range.end)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;
    use ordered_float::OrderedFloat;

    #[test]
    fn test_chunk_id_new() {
        // `ChunkId::new()` create new random ID
        assert_ne!(ChunkId::new(), ChunkId::new());
    }

    #[test]
    fn test_chunk_id_new_test() {
        // `ChunkId::new_test(...)` creates deterministic ID
        assert_eq!(ChunkId::new_test(1), ChunkId::new_test(1));
        assert_ne!(ChunkId::new_test(1), ChunkId::new_test(2));
    }

    #[test]
    fn test_chunk_id_debug_and_display() {
        // Random chunk IDs use UUID-format
        let id_random = ChunkId::new();
        let inner: Uuid = id_random.get();
        assert_eq!(format!("{id_random:?}"), format!("ChunkId({inner})"));
        assert_eq!(format!("{id_random}"), format!("ChunkId({inner})"));

        // Deterministic IDs use integer format
        let id_test = ChunkId::new_test(42);
        assert_eq!(format!("{id_test:?}"), "ChunkId(42)");
        assert_eq!(format!("{id_test}"), "ChunkId(42)");
    }

    #[test]
    fn test_expr_to_sql_no_expressions() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };
        assert_eq!(&pred.expr_sql_string(), "");
    }

    #[test]
    fn test_expr_to_sql_operators() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col1"),
                    op: Op::Eq,
                    scalar: Scalar::I64(1),
                },
                DeleteExpr {
                    column: String::from("col2"),
                    op: Op::Ne,
                    scalar: Scalar::I64(2),
                },
            ],
        };
        assert_eq!(&pred.expr_sql_string(), r#""col1"=1 AND "col2"!=2"#);
    }

    #[test]
    fn test_expr_to_sql_column_escape() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col 1"),
                    op: Op::Eq,
                    scalar: Scalar::I64(1),
                },
                DeleteExpr {
                    column: String::from(r#"col\2"#),
                    op: Op::Eq,
                    scalar: Scalar::I64(2),
                },
                DeleteExpr {
                    column: String::from(r#"col"3"#),
                    op: Op::Eq,
                    scalar: Scalar::I64(3),
                },
            ],
        };
        assert_eq!(
            &pred.expr_sql_string(),
            r#""col 1"=1 AND "col\\2"=2 AND "col\"3"=3"#
        );
    }

    #[test]
    fn test_expr_to_sql_bool() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col1"),
                    op: Op::Eq,
                    scalar: Scalar::Bool(false),
                },
                DeleteExpr {
                    column: String::from("col2"),
                    op: Op::Eq,
                    scalar: Scalar::Bool(true),
                },
            ],
        };
        assert_eq!(&pred.expr_sql_string(), r#""col1"=false AND "col2"=true"#);
    }

    #[test]
    fn test_expr_to_sql_i64() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col1"),
                    op: Op::Eq,
                    scalar: Scalar::I64(0),
                },
                DeleteExpr {
                    column: String::from("col2"),
                    op: Op::Eq,
                    scalar: Scalar::I64(-1),
                },
                DeleteExpr {
                    column: String::from("col3"),
                    op: Op::Eq,
                    scalar: Scalar::I64(1),
                },
                DeleteExpr {
                    column: String::from("col4"),
                    op: Op::Eq,
                    scalar: Scalar::I64(i64::MIN),
                },
                DeleteExpr {
                    column: String::from("col5"),
                    op: Op::Eq,
                    scalar: Scalar::I64(i64::MAX),
                },
            ],
        };
        assert_eq!(
            &pred.expr_sql_string(),
            r#""col1"=0 AND "col2"=-1 AND "col3"=1 AND "col4"=-9223372036854775808 AND "col5"=9223372036854775807"#
        );
    }

    #[test]
    fn test_expr_to_sql_f64() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col1"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(0.0)),
                },
                DeleteExpr {
                    column: String::from("col2"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(-0.0)),
                },
                DeleteExpr {
                    column: String::from("col3"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(1.0)),
                },
                DeleteExpr {
                    column: String::from("col4"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(f64::INFINITY)),
                },
                DeleteExpr {
                    column: String::from("col5"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(f64::NEG_INFINITY)),
                },
                DeleteExpr {
                    column: String::from("col6"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(f64::NAN)),
                },
            ],
        };
        assert_eq!(
            &pred.expr_sql_string(),
            r#""col1"=0.0 AND "col2"=-0.0 AND "col3"=1.0 AND "col4"='Infinity' AND "col5"='-Infinity' AND "col6"='NaN'"#
        );
    }

    #[test]
    fn test_expr_to_sql_string() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col1"),
                    op: Op::Eq,
                    scalar: Scalar::String(String::from("")),
                },
                DeleteExpr {
                    column: String::from("col2"),
                    op: Op::Eq,
                    scalar: Scalar::String(String::from("foo")),
                },
                DeleteExpr {
                    column: String::from("col3"),
                    op: Op::Eq,
                    scalar: Scalar::String(String::from(r#"fo\o"#)),
                },
                DeleteExpr {
                    column: String::from("col4"),
                    op: Op::Eq,
                    scalar: Scalar::String(String::from(r#"fo'o"#)),
                },
            ],
        };
        assert_eq!(
            &pred.expr_sql_string(),
            r#""col1"='' AND "col2"='foo' AND "col3"='fo\\o' AND "col4"='fo\'o'"#
        );
    }

    #[test]
    fn statistics_new_non_null() {
        let actual = StatValues::new_non_null(Some(-1i64), Some(1i64), 3);
        let expected = StatValues {
            min: Some(-1i64),
            max: Some(1i64),
            total_count: 3,
            null_count: Some(0),
            distinct_count: None,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn statistics_new_all_null() {
        // i64 values do not have a distinct count
        let actual = StatValues::<i64>::new_all_null(3, None);
        let expected = StatValues {
            min: None,
            max: None,
            total_count: 3,
            null_count: Some(3),
            distinct_count: None,
        };
        assert_eq!(actual, expected);

        // string columns can have a distinct count
        let actual = StatValues::<i64>::new_all_null(3, Some(1_u64));
        let expected = StatValues {
            min: None,
            max: None,
            total_count: 3,
            null_count: Some(3),
            distinct_count: Some(NonZeroU64::try_from(1_u64).unwrap()),
        };
        assert_eq!(actual, expected);
    }

    impl<T> StatValues<T>
    where
        T: IsNan + PartialOrd + Clone,
    {
        fn new_with_value(starting_value: T) -> Self {
            let starting_value = if starting_value.is_nan() {
                None
            } else {
                Some(starting_value)
            };

            let min = starting_value.clone();
            let max = starting_value;
            let total_count = 1;
            let null_count = Some(0);
            let distinct_count = None;
            Self::new_with_distinct(min, max, total_count, null_count, distinct_count)
        }
    }

    impl Statistics {
        /// Return the minimum value, if any, formatted as a string
        fn min_as_str(&self) -> Option<Cow<'_, str>> {
            match self {
                Self::I64(v) => v.min.map(|x| Cow::Owned(x.to_string())),
                Self::U64(v) => v.min.map(|x| Cow::Owned(x.to_string())),
                Self::F64(v) => v.min.map(|x| Cow::Owned(x.to_string())),
                Self::Bool(v) => v.min.map(|x| Cow::Owned(x.to_string())),
                Self::String(v) => v.min.as_deref().map(Cow::Borrowed),
            }
        }

        /// Return the maximum value, if any, formatted as a string
        fn max_as_str(&self) -> Option<Cow<'_, str>> {
            match self {
                Self::I64(v) => v.max.map(|x| Cow::Owned(x.to_string())),
                Self::U64(v) => v.max.map(|x| Cow::Owned(x.to_string())),
                Self::F64(v) => v.max.map(|x| Cow::Owned(x.to_string())),
                Self::Bool(v) => v.max.map(|x| Cow::Owned(x.to_string())),
                Self::String(v) => v.max.as_deref().map(Cow::Borrowed),
            }
        }
    }

    #[test]
    fn statistics_update() {
        let mut stat = StatValues::new_with_value(23);
        assert_eq!(stat.min, Some(23));
        assert_eq!(stat.max, Some(23));
        assert_eq!(stat.total_count, 1);

        stat.update(&55);
        assert_eq!(stat.min, Some(23));
        assert_eq!(stat.max, Some(55));
        assert_eq!(stat.total_count, 2);

        stat.update(&6);
        assert_eq!(stat.min, Some(6));
        assert_eq!(stat.max, Some(55));
        assert_eq!(stat.total_count, 3);

        stat.update(&30);
        assert_eq!(stat.min, Some(6));
        assert_eq!(stat.max, Some(55));
        assert_eq!(stat.total_count, 4);
    }

    #[test]
    fn statistics_default() {
        let mut stat = StatValues::default();
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.total_count, 0);

        stat.update(&55);
        assert_eq!(stat.min, Some(55));
        assert_eq!(stat.max, Some(55));
        assert_eq!(stat.total_count, 1);

        let mut stat = StatValues::<String>::default();
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.total_count, 0);

        stat.update("cupcakes");
        assert_eq!(stat.min, Some("cupcakes".to_string()));
        assert_eq!(stat.max, Some("cupcakes".to_string()));
        assert_eq!(stat.total_count, 1);

        stat.update("woo");
        assert_eq!(stat.min, Some("cupcakes".to_string()));
        assert_eq!(stat.max, Some("woo".to_string()));
        assert_eq!(stat.total_count, 2);
    }

    #[test]
    fn statistics_is_none() {
        let mut stat = StatValues::default();
        assert!(stat.is_none());
        stat.min = Some(0);
        assert!(!stat.is_none());
        stat.max = Some(1);
        assert!(!stat.is_none());
    }

    #[test]
    fn statistics_overlaps() {
        let stat1 = StatValues {
            min: Some(10),
            max: Some(20),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat1), StatOverlap::NonZero);

        //    [--stat1--]
        // [--stat2--]
        let stat2 = StatValues {
            min: Some(5),
            max: Some(15),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat2), StatOverlap::NonZero);
        assert_eq!(stat2.overlaps(&stat1), StatOverlap::NonZero);

        //    [--stat1--]
        //        [--stat3--]
        let stat3 = StatValues {
            min: Some(15),
            max: Some(25),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat3), StatOverlap::NonZero);
        assert_eq!(stat3.overlaps(&stat1), StatOverlap::NonZero);

        //    [--stat1--]
        //                [--stat4--]
        let stat4 = StatValues {
            min: Some(25),
            max: Some(35),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat4), StatOverlap::Zero);
        assert_eq!(stat4.overlaps(&stat1), StatOverlap::Zero);

        //              [--stat1--]
        // [--stat5--]
        let stat5 = StatValues {
            min: Some(0),
            max: Some(5),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat5), StatOverlap::Zero);
        assert_eq!(stat5.overlaps(&stat1), StatOverlap::Zero);
    }

    #[test]
    fn statistics_overlaps_none() {
        let stat1 = StatValues {
            min: Some(10),
            max: Some(20),
            ..Default::default()
        };

        let stat2 = StatValues {
            min: None,
            max: Some(20),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat2), StatOverlap::Unknown);
        assert_eq!(stat2.overlaps(&stat1), StatOverlap::Unknown);

        let stat3 = StatValues {
            min: Some(10),
            max: None,
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat3), StatOverlap::Unknown);
        assert_eq!(stat3.overlaps(&stat1), StatOverlap::Unknown);

        let stat4 = StatValues {
            min: None,
            max: None,
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat4), StatOverlap::Unknown);
        assert_eq!(stat4.overlaps(&stat1), StatOverlap::Unknown);
    }

    #[test]
    fn statistics_overlaps_mixed_none() {
        let stat1 = StatValues {
            min: Some(10),
            max: None,
            ..Default::default()
        };

        let stat2 = StatValues {
            min: None,
            max: Some(5),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat2), StatOverlap::Unknown);
        assert_eq!(stat2.overlaps(&stat1), StatOverlap::Unknown);
    }

    #[test]
    fn update_string() {
        let mut stat = StatValues::new_with_value("bbb".to_string());
        assert_eq!(stat.min, Some("bbb".to_string()));
        assert_eq!(stat.max, Some("bbb".to_string()));
        assert_eq!(stat.total_count, 1);

        stat.update("aaa");
        assert_eq!(stat.min, Some("aaa".to_string()));
        assert_eq!(stat.max, Some("bbb".to_string()));
        assert_eq!(stat.total_count, 2);

        stat.update("z");
        assert_eq!(stat.min, Some("aaa".to_string()));
        assert_eq!(stat.max, Some("z".to_string()));
        assert_eq!(stat.total_count, 3);

        stat.update("p");
        assert_eq!(stat.min, Some("aaa".to_string()));
        assert_eq!(stat.max, Some("z".to_string()));
        assert_eq!(stat.total_count, 4);
    }

    #[test]
    fn stats_is_none() {
        let stat = Statistics::I64(StatValues::new_non_null(Some(-1), Some(100), 1));
        assert!(!stat.is_none());

        let stat = Statistics::I64(StatValues::new_non_null(None, Some(100), 1));
        assert!(!stat.is_none());

        let stat = Statistics::I64(StatValues::new_non_null(None, None, 0));
        assert!(stat.is_none());
    }

    #[test]
    fn stats_as_str_i64() {
        let stat = Statistics::I64(StatValues::new_non_null(Some(-1), Some(100), 1));
        assert_eq!(stat.min_as_str(), Some("-1".into()));
        assert_eq!(stat.max_as_str(), Some("100".into()));

        let stat = Statistics::I64(StatValues::new_non_null(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn stats_as_str_u64() {
        let stat = Statistics::U64(StatValues::new_non_null(Some(1), Some(100), 1));
        assert_eq!(stat.min_as_str(), Some("1".into()));
        assert_eq!(stat.max_as_str(), Some("100".into()));

        let stat = Statistics::U64(StatValues::new_non_null(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn stats_as_str_f64() {
        let stat = Statistics::F64(StatValues::new_non_null(Some(99.0), Some(101.0), 1));
        assert_eq!(stat.min_as_str(), Some("99".into()));
        assert_eq!(stat.max_as_str(), Some("101".into()));

        let stat = Statistics::F64(StatValues::new_non_null(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn stats_as_str_bool() {
        let stat = Statistics::Bool(StatValues::new_non_null(Some(false), Some(true), 1));
        assert_eq!(stat.min_as_str(), Some("false".into()));
        assert_eq!(stat.max_as_str(), Some("true".into()));

        let stat = Statistics::Bool(StatValues::new_non_null(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn stats_as_str_str() {
        let stat = Statistics::String(StatValues::new_non_null(
            Some("a".to_string()),
            Some("zz".to_string()),
            1,
        ));
        assert_eq!(stat.min_as_str(), Some("a".into()));
        assert_eq!(stat.max_as_str(), Some("zz".into()));

        let stat = Statistics::String(StatValues::new_non_null(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn table_update_from() {
        let mut string_stats = StatValues::new_with_value("foo".to_string());
        string_stats.update("bar");
        let string_col = ColumnSummary {
            name: "string".to_string(),
            influxdb_type: InfluxDbType::Field,
            stats: Statistics::String(string_stats),
        };

        let mut int_stats = StatValues::new_with_value(1);
        int_stats.update(&5);
        let int_col = ColumnSummary {
            name: "int".to_string(),
            influxdb_type: InfluxDbType::Field,
            stats: Statistics::I64(int_stats),
        };

        let mut float_stats = StatValues::new_with_value(9.1);
        float_stats.update(&1.3);
        let float_col = ColumnSummary {
            name: "float".to_string(),
            influxdb_type: InfluxDbType::Field,
            stats: Statistics::F64(float_stats),
        };

        let mut table_a = TableSummary {
            columns: vec![string_col, int_col, float_col],
        };

        let mut string_stats = StatValues::new_with_value("aaa".to_string());
        string_stats.update("zzz");
        let string_col = ColumnSummary {
            name: "string".to_string(),
            influxdb_type: InfluxDbType::Field,
            stats: Statistics::String(string_stats),
        };

        let mut int_stats = StatValues::new_with_value(3);
        int_stats.update(&9);
        let int_col = ColumnSummary {
            name: "int".to_string(),
            influxdb_type: InfluxDbType::Field,
            stats: Statistics::I64(int_stats),
        };

        let mut table_b = TableSummary {
            columns: vec![int_col, string_col],
        };

        // keep this to test joining the other way
        let table_c = table_a.clone();

        table_a.update_from(&table_b);
        let col = table_a.column("string").unwrap();
        assert_eq!(
            col.stats,
            Statistics::String(StatValues::new_non_null(
                Some("aaa".to_string()),
                Some("zzz".to_string()),
                4,
            ))
        );

        let col = table_a.column("int").unwrap();
        assert_eq!(
            col.stats,
            Statistics::I64(StatValues::new_non_null(Some(1), Some(9), 4))
        );

        let col = table_a.column("float").unwrap();
        assert_eq!(
            col.stats,
            Statistics::F64(StatValues::new(Some(1.3), Some(9.1), 4, Some(2)))
        );

        table_b.update_from(&table_c);
        let col = table_b.column("string").unwrap();
        assert_eq!(
            col.stats,
            Statistics::String(StatValues::new_non_null(
                Some("aaa".to_string()),
                Some("zzz".to_string()),
                4,
            ))
        );

        let col = table_b.column("int").unwrap();
        assert_eq!(
            col.stats,
            Statistics::I64(StatValues::new_non_null(Some(1), Some(9), 4))
        );

        let col = table_b.column("float").unwrap();
        assert_eq!(
            col.stats,
            Statistics::F64(StatValues::new(Some(1.3), Some(9.1), 4, Some(2)))
        );
    }

    #[test]
    fn table_update_from_new_column() {
        let string_stats = StatValues::new_with_value("bar".to_string());
        let string_col = ColumnSummary {
            name: "string".to_string(),
            influxdb_type: InfluxDbType::Tag,
            stats: Statistics::String(string_stats),
        };

        let int_stats = StatValues::new_with_value(5);
        let int_col = ColumnSummary {
            name: "int".to_string(),
            influxdb_type: InfluxDbType::Field,
            stats: Statistics::I64(int_stats),
        };

        // table summary that does not have the "string" col
        let table1 = TableSummary {
            columns: vec![int_col.clone()],
        };

        // table summary that has both columns
        let table2 = TableSummary {
            columns: vec![int_col, string_col],
        };

        // Statistics should be the same regardless of the order we update the stats

        let expected_string_stats = Statistics::String(StatValues::new(
            Some("bar".to_string()),
            Some("bar".to_string()),
            2,       // total count is 2 even though did not appear in the update
            Some(1), // 1 null
        ));

        let expected_int_stats = Statistics::I64(StatValues::new(
            Some(5),
            Some(5),
            2,
            Some(0), // no nulls
        ));

        // update table 1 with table 2
        let mut table = table1.clone();
        table.update_from(&table2);

        assert_eq!(
            &table.column("string").unwrap().stats,
            &expected_string_stats
        );

        assert_eq!(&table.column("int").unwrap().stats, &expected_int_stats);

        // update table 2 with table 1
        let mut table = table2;
        table.update_from(&table1);

        assert_eq!(
            &table.column("string").unwrap().stats,
            &expected_string_stats
        );

        assert_eq!(&table.column("int").unwrap().stats, &expected_int_stats);
    }

    #[test]
    fn column_update_from_boolean() {
        let bool_false = ColumnSummary {
            name: "b".to_string(),
            influxdb_type: InfluxDbType::Field,
            stats: Statistics::Bool(StatValues::new(Some(false), Some(false), 1, Some(1))),
        };
        let bool_true = ColumnSummary {
            name: "b".to_string(),
            influxdb_type: InfluxDbType::Field,
            stats: Statistics::Bool(StatValues::new(Some(true), Some(true), 1, Some(2))),
        };

        let expected_stats = Statistics::Bool(StatValues::new(Some(false), Some(true), 2, Some(3)));

        let mut b = bool_false.clone();
        b.update_from(&bool_true);
        assert_eq!(b.stats, expected_stats);

        let mut b = bool_true;
        b.update_from(&bool_false);
        assert_eq!(b.stats, expected_stats);
    }

    #[test]
    fn column_update_from_u64() {
        let mut min = ColumnSummary {
            name: "foo".to_string(),
            influxdb_type: InfluxDbType::Field,
            stats: Statistics::U64(StatValues::new(Some(5), Some(23), 1, Some(1))),
        };

        let max = ColumnSummary {
            name: "foo".to_string(),
            influxdb_type: InfluxDbType::Field,
            stats: Statistics::U64(StatValues::new(Some(6), Some(506), 43, Some(2))),
        };

        min.update_from(&max);

        let expected = Statistics::U64(StatValues::new(Some(5), Some(506), 44, Some(3)));
        assert_eq!(min.stats, expected);
    }

    #[test]
    fn nans() {
        let mut stat = StatValues::default();
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.total_count, 0);

        stat.update(&f64::NAN);
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.total_count, 1);

        stat.update(&1.0);
        assert_eq!(stat.min, Some(1.0));
        assert_eq!(stat.max, Some(1.0));
        assert_eq!(stat.total_count, 2);

        stat.update(&2.0);
        assert_eq!(stat.min, Some(1.0));
        assert_eq!(stat.max, Some(2.0));
        assert_eq!(stat.total_count, 3);

        stat.update(&f64::INFINITY);
        assert_eq!(stat.min, Some(1.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.total_count, 4);

        stat.update(&-1.0);
        assert_eq!(stat.min, Some(-1.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.total_count, 5);

        // ===========

        let mut stat = StatValues::new_with_value(2.0);
        stat.update(&f64::INFINITY);
        assert_eq!(stat.min, Some(2.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.total_count, 2);

        stat.update(&f64::NAN);
        assert_eq!(stat.min, Some(2.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.total_count, 3);

        // ===========

        let mut stat2 = StatValues::new_with_value(1.0);
        stat2.update_from(&stat);
        assert_eq!(stat2.min, Some(1.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat2.total_count, 4);

        // ===========

        let stat2 = StatValues::new_with_value(1.0);
        stat.update_from(&stat2);
        assert_eq!(stat.min, Some(1.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.total_count, 4);

        // ===========

        let stat = StatValues::new_with_value(f64::NAN);
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.total_count, 1);
    }

    #[test]
    fn test_timestamp_nano_min_max() {
        let cases = vec![
            (
                "MIN / MAX Nanos",
                TimestampRange::new(MIN_NANO_TIME, MAX_NANO_TIME + 1),
            ),
            ("MIN/MAX i64", TimestampRange::new(i64::MIN, i64::MAX)),
        ];

        for (name, range) in cases {
            println!("case: {name}");
            assert!(!range.contains(i64::MIN));
            assert!(!range.contains(i64::MIN + 1));
            assert!(range.contains(MIN_NANO_TIME));
            assert!(range.contains(MIN_NANO_TIME + 1));
            assert!(range.contains(MAX_NANO_TIME - 1));
            assert!(range.contains(MAX_NANO_TIME));
            assert!(!range.contains(i64::MAX));
            assert!(range.contains_all());
            assert!(range.contains_nearly_all());
        }
    }

    #[test]
    fn test_timestamp_i64_min_max_offset() {
        let range = TimestampRange::new(MIN_NANO_TIME + 1, MAX_NANO_TIME - 1);

        assert!(!range.contains(i64::MIN));
        assert!(!range.contains(MIN_NANO_TIME));
        assert!(range.contains(MIN_NANO_TIME + 1));
        assert!(range.contains(MAX_NANO_TIME - 2));
        assert!(!range.contains(MAX_NANO_TIME - 1));
        assert!(!range.contains(MAX_NANO_TIME));
        assert!(!range.contains(i64::MAX));
        assert!(!range.contains_all());
        assert!(!range.contains_nearly_all());
    }

    #[test]
    fn test_timestamp_i64_min_max_offset_max() {
        let range = TimestampRange::new(MIN_NANO_TIME, MAX_NANO_TIME);

        assert!(!range.contains(i64::MIN));
        assert!(range.contains(MIN_NANO_TIME));
        assert!(range.contains(MIN_NANO_TIME + 1));
        assert!(range.contains(MAX_NANO_TIME - 1));
        assert!(!range.contains(MAX_NANO_TIME));
        assert!(!range.contains(i64::MAX));
        assert!(!range.contains_all());
        assert!(range.contains_nearly_all());
    }

    #[test]
    fn test_timestamp_range_contains() {
        let range = TimestampRange::new(100, 200);
        assert!(!range.contains(99));
        assert!(range.contains(100));
        assert!(range.contains(101));
        assert!(range.contains(199));
        assert!(!range.contains(200));
        assert!(!range.contains(201));
    }

    #[test]
    fn test_timestamp_range_overlaps() {
        let range = TimestampRange::new(100, 200);
        assert!(!TimestampMinMax::new(0, 99).overlaps(range));
        assert!(TimestampMinMax::new(0, 100).overlaps(range));
        assert!(TimestampMinMax::new(0, 101).overlaps(range));

        assert!(TimestampMinMax::new(0, 200).overlaps(range));
        assert!(TimestampMinMax::new(0, 201).overlaps(range));
        assert!(TimestampMinMax::new(0, 300).overlaps(range));

        assert!(TimestampMinMax::new(100, 101).overlaps(range));
        assert!(TimestampMinMax::new(100, 200).overlaps(range));
        assert!(TimestampMinMax::new(100, 201).overlaps(range));

        assert!(TimestampMinMax::new(101, 101).overlaps(range));
        assert!(TimestampMinMax::new(101, 200).overlaps(range));
        assert!(TimestampMinMax::new(101, 201).overlaps(range));

        assert!(!TimestampMinMax::new(200, 200).overlaps(range));
        assert!(!TimestampMinMax::new(200, 201).overlaps(range));

        assert!(!TimestampMinMax::new(201, 300).overlaps(range));
    }

    #[test]
    #[should_panic(expected = "expected min (2) <= max (1)")]
    fn test_timestamp_min_max_invalid() {
        TimestampMinMax::new(2, 1);
    }

    #[test]
    #[should_panic = "timestamp wraparound"]
    fn test_timestamp_wraparound_panic_add_i64() {
        let _ = Timestamp::new(i64::MAX) + 1;
    }

    #[test]
    #[should_panic = "timestamp wraparound"]
    fn test_timestamp_wraparound_panic_sub_i64() {
        let _ = Timestamp::new(i64::MIN) - 1;
    }

    #[test]
    #[should_panic = "timestamp wraparound"]
    fn test_timestamp_wraparound_panic_add_timestamp() {
        let _ = Timestamp::new(i64::MAX) + Timestamp::new(1);
    }

    #[test]
    #[should_panic = "timestamp wraparound"]
    fn test_timestamp_wraparound_panic_sub_timestamp() {
        let _ = Timestamp::new(i64::MIN) - Timestamp::new(1);
    }

    #[test]
    fn test_timestamprange_start_after_end() {
        let tr = TimestampRange::new(2, 1);
        assert_eq!(tr.start(), 1);
        assert_eq!(tr.end(), 1);
    }
}
