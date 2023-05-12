//! Traits and data types for the IOx Catalog API.

use async_trait::async_trait;
use data_types::{
    Column, ColumnType, ColumnsByName, CompactionLevel, Namespace, NamespaceId, NamespaceSchema,
    ParquetFile, ParquetFileId, ParquetFileParams, Partition, PartitionId, PartitionKey,
    PartitionTemplate, SkippedCompaction, Table, TableId, TablePartitionTemplateOverride,
    TableSchema, Timestamp,
};
use generated_types::influxdata::iox::namespace::v1 as proto;
use iox_time::TimeProvider;
use once_cell::sync::Lazy;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::{Debug, Display},
    sync::Arc,
};
use uuid::Uuid;

/// Maximum number of files deleted by [`ParquetFileRepo::delete_old_ids_only`] and
/// [`ParquetFileRepo::flag_for_delete_by_retention`] at a time.
pub const MAX_PARQUET_FILES_SELECTED_ONCE: i64 = 1_000;

/// An error wrapper detailing the reason for a compare-and-swap failure.
#[derive(Debug)]
pub enum CasFailure<T> {
    /// The compare-and-swap failed because the current value differers from the
    /// comparator.
    ///
    /// Contains the new current value.
    ValueMismatch(T),
    /// A query error occurred.
    QueryError(Error),
}

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("name {} already exists", name))]
    NameExists { name: String },

    #[snafu(display("A table named {name} already exists in namespace {namespace_id}"))]
    TableNameExists {
        name: String,
        namespace_id: NamespaceId,
    },

    #[snafu(display("unhandled sqlx error: {}", source))]
    SqlxError { source: sqlx::Error },

    #[snafu(display("foreign key violation: {}", source))]
    ForeignKeyViolation { source: sqlx::Error },

    #[snafu(display("column {} is type {} but write has type {}", name, existing, new))]
    ColumnTypeMismatch {
        name: String,
        existing: ColumnType,
        new: ColumnType,
    },

    #[snafu(display(
        "column type {} is in the db for column {}, which is unknown",
        data_type,
        name
    ))]
    UnknownColumnType { data_type: i16, name: String },

    #[snafu(display("namespace {} not found", name))]
    NamespaceNotFoundByName { name: String },

    #[snafu(display("namespace {} not found", id))]
    NamespaceNotFoundById { id: NamespaceId },

    #[snafu(display("table {} not found", id))]
    TableNotFound { id: TableId },

    #[snafu(display("partition {} not found", id))]
    PartitionNotFound { id: PartitionId },

    #[snafu(display(
        "couldn't create column {} in table {}; limit reached on namespace",
        column_name,
        table_id,
    ))]
    ColumnCreateLimitError {
        column_name: String,
        table_id: TableId,
    },

    #[snafu(display(
        "couldn't create table {}; limit reached on namespace {}",
        table_name,
        namespace_id
    ))]
    TableCreateLimitError {
        table_name: String,
        namespace_id: NamespaceId,
    },

    #[snafu(display("parquet file with object_store_id {} already exists", object_store_id))]
    FileExists { object_store_id: Uuid },

    #[snafu(display("parquet file with id {} does not exist. Foreign key violation", id))]
    FileNotFound { id: i64 },

    #[snafu(display("parquet_file record {} not found", id))]
    ParquetRecordNotFound { id: ParquetFileId },

    #[snafu(display("cannot derive valid column schema from column {}: {}", name, source))]
    InvalidColumn {
        source: Box<dyn std::error::Error + Send + Sync>,
        name: String,
    },

    #[snafu(display("cannot start a transaction: {}", source))]
    StartTransaction { source: sqlx::Error },

    #[snafu(display("no transaction provided"))]
    NoTransaction,

    #[snafu(display("error while converting usize {} to i64", value))]
    InvalidValue { value: usize },

    #[snafu(display("database setup error: {}", source))]
    Setup { source: sqlx::Error },

    #[snafu(display(
        "could not record a skipped compaction for partition {partition_id}: {source}"
    ))]
    CouldNotRecordSkippedCompaction {
        source: sqlx::Error,
        partition_id: PartitionId,
    },

    #[snafu(display("could not list skipped compactions: {source}"))]
    CouldNotListSkippedCompactions { source: sqlx::Error },

    #[snafu(display("could not delete skipped compactions: {source}"))]
    CouldNotDeleteSkippedCompactions { source: sqlx::Error },

    #[snafu(display("could not delete namespace: {source}"))]
    CouldNotDeleteNamespace { source: sqlx::Error },

    #[snafu(display("could not serialize partition template to JSON: {source}"))]
    PartitionTemplateSerialization { source: serde_json::Error },

    #[snafu(display("could not deserialize partition template to proto: {source}"))]
    PartitionTemplateDeserialization { source: serde_json::Error },

    #[snafu(display("could not create a partition template raw JSON value: {source}"))]
    PartitionTemplateRawJson { source: serde_json::Error },
}

/// A specialized `Error` for Catalog errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Specify how soft-deleted entities should affect query results.
///
/// ```text
///
///                ExcludeDeleted          OnlyDeleted
///
///                       ┃                     ┃
///                 .─────╋─────.         .─────╋─────.
///              ,─'      ┃      '─.   ,─'      ┃      '─.
///            ,'         ●         `,'         ●         `.
///          ,'                    ,' `.                    `.
///         ;                     ;     :                     :
///         │      No deleted     │     │   Only deleted      │
///         │         rows        │  ●  │       rows          │
///         :                     :  ┃  ;                     ;
///          ╲                     ╲ ┃ ╱                     ╱
///           `.                    `┃'                    ,'
///             `.                 ,'┃`.                 ,'
///               '─.           ,─'  ┃  '─.           ,─'
///                  `─────────'     ┃     `─────────'
///                                  ┃
///
///                               AllRows
///
/// ```
#[derive(Debug, Clone, Copy)]
pub enum SoftDeletedRows {
    /// Return all rows.
    AllRows,

    /// Return all rows, except soft deleted rows.
    ExcludeDeleted,

    /// Return only soft deleted rows.
    OnlyDeleted,
}

impl SoftDeletedRows {
    pub(crate) fn as_sql_predicate(&self) -> &str {
        match self {
            Self::ExcludeDeleted => "deleted_at IS NULL",
            Self::OnlyDeleted => "deleted_at IS NOT NULL",
            Self::AllRows => "1=1",
        }
    }
}

/// Methods for working with the catalog.
#[async_trait]
pub trait Catalog: Send + Sync + Debug + Display {
    /// Setup catalog for usage and apply possible migrations.
    async fn setup(&self) -> Result<(), Error>;

    /// Creates a new [`Transaction`].
    ///
    /// Creating transactions is potentially expensive. Holding one consumes resources. The number
    /// of parallel active transactions might be limited per catalog, so you MUST NOT rely on the
    /// ability to create multiple transactions in parallel for correctness. Parallel transactions
    /// must only be used for scaling.
    async fn start_transaction(&self) -> Result<Box<dyn Transaction>, Error>;

    /// Accesses the repositories without a transaction scope.
    async fn repositories(&self) -> Box<dyn RepoCollection>;

    /// Gets metric registry associated with this catalog.
    fn metrics(&self) -> Arc<metric::Registry>;

    /// Gets the time provider associated with this catalog.
    fn time_provider(&self) -> Arc<dyn TimeProvider>;
}

/// Secret module for [sealed traits].
///
/// [sealed traits]: https://rust-lang.github.io/api-guidelines/future-proofing.html#sealed-traits-protect-against-downstream-implementations-c-sealed
#[doc(hidden)]
pub(crate) mod sealed {
    use super::*;

    /// Helper trait to implement commit and abort of an transaction.
    ///
    /// The problem is that both methods cannot take `self` directly, otherwise the [`Transaction`]
    /// would not be object safe. Therefore we can only take a reference. To avoid that a user uses
    /// a transaction after calling one of the finalizers, we use a tiny trick and take `Box<dyn
    /// Transaction>` in our public interface and use a sealed trait for the actual implementation.
    #[async_trait]
    pub trait TransactionFinalize: Send + Sync + Debug {
        async fn commit_inplace(&mut self) -> Result<(), Error>;
        async fn abort_inplace(&mut self) -> Result<(), Error>;
    }
}

/// Transaction in a [`Catalog`] (similar to a database transaction).
///
/// A transaction provides a consistent view on data and stages writes.
/// To finalize a transaction, call [commit](Self::commit) or [abort](Self::abort).
///
/// Repositories can cheaply be borrowed from it.
///
/// Note that after any method in this transaction (including all repositories derived from it)
/// returns an error, the transaction MIGHT be poisoned and will return errors for all operations,
/// depending on the backend.
///
///
/// # Drop
///
/// Dropping a transaction without calling [`commit`](Self::commit) or [`abort`](Self::abort) will
/// abort the transaction. However resources might not be released immediately, so it is adviced to
/// always call [`abort`](Self::abort) when you want to enforce that. Dropping w/o
/// commiting/aborting will also log a warning.
#[async_trait]
pub trait Transaction: Send + Sync + Debug + sealed::TransactionFinalize + RepoCollection {
    /// Commits a transaction.
    ///
    /// # Error Handling
    ///
    /// If successful, all changes will be visible to other transactions.
    ///
    /// If an error is returned, the transaction may or or not be committed. This might be due to
    /// IO errors after the transaction was finished. However in either case, the transaction is
    /// atomic and can only succeed or fail entirely.
    async fn commit(mut self: Box<Self>) -> Result<(), Error> {
        self.commit_inplace().await
    }

    /// Aborts the transaction, throwing away all changes.
    async fn abort(mut self: Box<Self>) -> Result<(), Error> {
        self.abort_inplace().await
    }
}

impl<T> Transaction for T where T: Send + Sync + Debug + sealed::TransactionFinalize + RepoCollection
{}

/// Methods for working with the catalog's various repositories (collections of entities).
///
/// # Repositories
///
/// The methods (e.g. `create_*` or `get_by_*`) for handling entities (namespaces, partitions,
/// etc.) are grouped into *repositories* with one repository per entity. A repository can be
/// thought of a collection of a single kind of entity. Getting repositories from the transaction
/// is cheap.
///
/// A repository might internally map to a wide range of different storage abstractions, ranging
/// from one or more SQL tables over key-value key spaces to simple in-memory vectors. The user
/// should and must not care how these are implemented.
#[async_trait]
pub trait RepoCollection: Send + Sync + Debug {
    /// Repository for [namespaces](data_types::Namespace).
    fn namespaces(&mut self) -> &mut dyn NamespaceRepo;

    /// Repository for [tables](data_types::Table).
    fn tables(&mut self) -> &mut dyn TableRepo;

    /// Repository for [columns](data_types::Column).
    fn columns(&mut self) -> &mut dyn ColumnRepo;

    /// Repository for [partitions](data_types::Partition).
    fn partitions(&mut self) -> &mut dyn PartitionRepo;

    /// Repository for [Parquet files](data_types::ParquetFile).
    fn parquet_files(&mut self) -> &mut dyn ParquetFileRepo;
}

/// The default partitioning scheme is by each day according to the "time" column.
pub static PARTITION_BY_DAY: Lazy<Arc<sqlx::types::JsonRawValue>> =
    Lazy::new(|| proto_to_json(&*proto::PARTITION_BY_DAY).unwrap());

/// Turn possibly-specified borrowed raw JSON representing a partition template into
/// definitely-specified owned raw JSON to be returned as part of a `Namespace` or `Table` instance.
/// If the database doesn't have a partition template, use the default of partitioning by day.
pub fn partition_template_owned_json(
    partition_template: Option<&sqlx::types::JsonRawValue>,
) -> Result<Arc<sqlx::types::JsonRawValue>> {
    Ok(partition_template
        .map(|pt| serde_json::value::to_raw_value::<sqlx::types::JsonRawValue>(pt))
        .transpose()
        .context(PartitionTemplateRawJsonSnafu)?
        .map(Arc::from)
        .unwrap_or_else(|| Arc::clone(&PARTITION_BY_DAY)))
}

/// Deserialize possibly-specified borrowed raw JSON representing a partition template into
/// the [`data_types::PartitionTemplate`] type to be used when partitioning a table's writes.
/// If the database doesn't have a partition template for this table, use the default of
/// partitioning by day.
pub fn partition_template_deserialized(
    partition_template: Option<&sqlx::types::JsonRawValue>,
) -> Result<PartitionTemplate> {
    let partition_template_proto = partition_template
        .map(|pt| serde_json::from_str::<proto::PartitionTemplate>(pt.get()))
        .transpose()
        .context(PartitionTemplateDeserializationSnafu)?;
    partition_template_proto_deserialized(partition_template_proto)
}

/// Deserialize possibly-specified borrowed proto representing a partition templaet into the
/// [`data_types::PartitionTemplate`] type to be used when partitioning a table's writes.
/// If the database doesn't have a partition template for this table, use the default of
/// partitioning by day.
pub fn partition_template_proto_deserialized(
    partition_template: Option<proto::PartitionTemplate>,
) -> Result<Option<PartitionTemplate>> {

    partition_template
        .map(PartitionTemplate::try_from)
        .map_err(PartitionTemplateDeserializationSnafu)?
        .unwrap_or_else(data_types::PARTITION_BY_DAY)
}

fn proto_to_json(
    partition_template: &proto::PartitionTemplate,
) -> Result<Arc<sqlx::types::JsonRawValue>> {
    sqlx::types::JsonRawValue::from_string(
        serde_json::to_string(&*partition_template).context(PartitionTemplateSerializationSnafu)?,
    )
    .context(PartitionTemplateRawJsonSnafu)
    .map(Arc::from)
}

/// Turn possibly-specified protobuf representing a partition template into
/// definitely-specified owned raw JSON to be returned as part of a `Namespace` or `Table` instance.
/// If the partition template isn't provided, use the default of partitioning by day.
pub fn partition_template_proto_to_owned_json(
    partition_template: Option<&proto::PartitionTemplate>,
) -> Result<Arc<sqlx::types::JsonRawValue>> {
    Ok(partition_template
        .map(proto_to_json)
        .transpose()?
        .unwrap_or_else(|| Arc::clone(&PARTITION_BY_DAY)))
}

/// Turn raw JSON back into protobuf
pub fn proto_from_raw(
    raw_json: &sqlx::types::JsonRawValue,
) -> Result<proto::PartitionTemplate, serde_json::Error> {
    serde_json::from_str(raw_json.get())
}

/// Functions for working with namespaces in the catalog
#[async_trait]
pub trait NamespaceRepo: Send + Sync {
    /// Creates the namespace in the catalog. If one by the same name already exists, an
    /// error is returned.
    /// Specify `None` for `retention_period_ns` to get infinite retention.
    async fn create(
        &mut self,
        name: &str,
        partition_template: Option<&proto::PartitionTemplate>,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace>;

    /// Update retention period for a namespace
    async fn update_retention_period(
        &mut self,
        name: &str,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace>;

    /// List all namespaces.
    async fn list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>>;

    /// Gets the namespace by its ID.
    async fn get_by_id(
        &mut self,
        id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>>;

    /// Gets the namespace by its unique name.
    async fn get_by_name(
        &mut self,
        name: &str,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>>;

    /// Soft-delete a namespace by name
    async fn soft_delete(&mut self, name: &str) -> Result<()>;

    /// Update the limit on the number of tables that can exist per namespace.
    async fn update_table_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace>;

    /// Update the limit on the number of columns that can exist per table in a given namespace.
    async fn update_column_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace>;
}

/// Until the database is backfilled, we'll handle potential null partition templates here by
/// filling in with `proto::PARTITION_BY_DAY`. When the backfilling is done, this can be an `impl
/// sqlx::FromRow` in `data_types` (that will need to be custom to turn the borrowed raw JSON into
/// the owned variant), but we can't `impl sqlx::FromRow for Namespace` in this crate because
/// `Namespace` is defined in `data_types` and the orphan rule prevents that. We can't use the
/// `proto` definitions in `data_types` because `generated_types` depends on `data_types` and that
/// would be a circular dependency. So here it is as a regular function used by the catalog methods
/// that return `Namespace`.
pub fn namespace_from_row<'a, R: ::sqlx::Row>(row: &'a R) -> ::sqlx::Result<Namespace>
where
    &'a ::std::primitive::str: ::sqlx::ColumnIndex<R>,
    NamespaceId: ::sqlx::decode::Decode<'a, R::Database>,
    NamespaceId: ::sqlx::types::Type<R::Database>,
    String: ::sqlx::decode::Decode<'a, R::Database>,
    String: ::sqlx::types::Type<R::Database>,
    Option<i64>: ::sqlx::decode::Decode<'a, R::Database>,
    Option<i64>: ::sqlx::types::Type<R::Database>,
    i32: ::sqlx::decode::Decode<'a, R::Database>,
    i32: ::sqlx::types::Type<R::Database>,
    i32: ::sqlx::decode::Decode<'a, R::Database>,
    i32: ::sqlx::types::Type<R::Database>,
    Option<Timestamp>: ::sqlx::decode::Decode<'a, R::Database>,
    Option<Timestamp>: ::sqlx::types::Type<R::Database>,
    &'a ::sqlx::types::JsonRawValue: ::sqlx::Decode<'a, R::Database>,
    for<'b> ::sqlx::types::Json<&'b ::sqlx::types::JsonRawValue>:
        sqlx::Type<<R as sqlx::Row>::Database>,
{
    let id: NamespaceId = row.try_get("id")?;
    let name: String = row.try_get("name")?;
    let retention_period_ns: Option<i64> =
        row.try_get("retention_period_ns").or_else(|e| match e {
            ::sqlx::Error::ColumnNotFound(_) => ::std::result::Result::Ok(Default::default()),
            e => ::std::result::Result::Err(e),
        })?;
    let max_tables: i32 = row.try_get("max_tables")?;
    let max_columns_per_table: i32 = row.try_get("max_columns_per_table")?;
    let deleted_at: Option<Timestamp> = row.try_get("deleted_at")?;

    // There might not be a partition template until we backfill the database. When the database
    // has been backfilled, the `Option` will go away.
    let partition_template: Option<&sqlx::types::JsonRawValue> =
        row.try_get("partition_template")?;

    // Convert the borrowed raw JSON to the owned variant.
    let partition_template = partition_template_owned_json(partition_template).map_err(|e| {
        sqlx::Error::ColumnDecode {
            index: "partition_template".into(),
            source: Box::new(e),
        }
    })?;

    ::std::result::Result::Ok(Namespace {
        id,
        name,
        retention_period_ns,
        max_tables,
        max_columns_per_table,
        deleted_at,
        partition_template,
    })
}

/// Functions for working with tables in the catalog
#[async_trait]
pub trait TableRepo: Send + Sync {
    /// Creates the table in the catalog. If one in the same namespace with the same name already
    /// exists, an error is returned.
    async fn create(
        &mut self,
        name: &str,
        partition_template: Option<&proto::PartitionTemplate>,
        namespace_id: NamespaceId,
    ) -> Result<Table>;

    /// get table by ID
    async fn get_by_id(&mut self, table_id: TableId) -> Result<Option<Table>>;

    /// get table by namespace ID and name
    async fn get_by_namespace_and_name(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<Option<Table>>;

    /// Lists all tables in the catalog for the given namespace id.
    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>>;

    /// List all tables.
    async fn list(&mut self) -> Result<Vec<Table>>;
}

/// Until the database is backfilled, we'll handle potential null partition templates here by
/// filling in with `proto::PARTITION_BY_DAY`. When the backfilling is done, this can be an `impl
/// sqlx::FromRow` in `data_types` (that will need to be custom to turn the borrowed raw JSON into
/// the owned variant), but we can't `impl sqlx::FromRow for Table` in this crate because
/// `Table` is defined in `data_types` and the orphan rule prevents that. We can't use the
/// `proto` definitions in `data_types` because `generated_types` depends on `data_types` and that
/// would be a circular dependency. So here it is as a regular function used by the catalog methods
/// that return `Table`.
pub fn table_from_row<'a, R: ::sqlx::Row>(row: &'a R) -> ::sqlx::Result<Table>
where
    &'a ::std::primitive::str: ::sqlx::ColumnIndex<R>,
    TableId: ::sqlx::decode::Decode<'a, R::Database>,
    TableId: ::sqlx::types::Type<R::Database>,
    NamespaceId: ::sqlx::decode::Decode<'a, R::Database>,
    NamespaceId: ::sqlx::types::Type<R::Database>,
    String: ::sqlx::decode::Decode<'a, R::Database>,
    String: ::sqlx::types::Type<R::Database>,
    &'a ::sqlx::types::JsonRawValue: ::sqlx::Decode<'a, R::Database>,
    for<'b> ::sqlx::types::Json<&'b ::sqlx::types::JsonRawValue>:
        sqlx::Type<<R as sqlx::Row>::Database>,
{
    let id: TableId = row.try_get("id")?;
    let namespace_id: NamespaceId = row.try_get("namespace_id")?;
    let name: String = row.try_get("name")?;

    // There might not be a partition template until we backfill the database. When the database
    // has been backfilled, the `Option` will go away.
    let partition_template: Option<&sqlx::types::JsonRawValue> =
        row.try_get("partition_template")?;

    // Deserialize the borrowed raw JSON to the application type.
    let partition_template = partition_template_deserialized(partition_template).map_err(|e| {
        sqlx::Error::ColumnDecode {
            index: "partition_template".into(),
            source: Box::new(e),
        }
    })?;

    let partition_template = Arc::new(TablePartitionTemplateOverride::new(partition_template));

    ::std::result::Result::Ok(Table {
        id,
        namespace_id,
        name,
        partition_template,
    })
}

/// Functions for working with columns in the catalog
#[async_trait]
pub trait ColumnRepo: Send + Sync {
    /// Creates the column in the catalog or returns the existing column. Will return a
    /// `Error::ColumnTypeMismatch` if the existing column type doesn't match the type
    /// the caller is attempting to create.
    async fn create_or_get(
        &mut self,
        name: &str,
        table_id: TableId,
        column_type: ColumnType,
    ) -> Result<Column>;

    /// Perform a bulk upsert of columns specified by a map of column name to column type.
    ///
    /// Implementations make no guarantees as to the ordering or atomicity of
    /// the batch of column upsert operations - a batch upsert may partially
    /// commit, in which case an error MUST be returned by the implementation.
    ///
    /// Per-namespace limits on the number of columns allowed per table are explicitly NOT checked
    /// by this function, hence the name containing `unchecked`. It is expected that the caller
    /// will check this first-- and yes, this is racy.
    async fn create_or_get_many_unchecked(
        &mut self,
        table_id: TableId,
        columns: HashMap<&str, ColumnType>,
    ) -> Result<Vec<Column>>;

    /// Lists all columns in the passed in namespace id.
    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>>;

    /// List all columns for the given table ID.
    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Column>>;

    /// List all columns.
    async fn list(&mut self) -> Result<Vec<Column>>;
}

/// Functions for working with IOx partitions in the catalog. These are how IOx splits up
/// data within a namespace.
#[async_trait]
pub trait PartitionRepo: Send + Sync {
    /// create or get a partition record for the given partition key and table
    async fn create_or_get(&mut self, key: PartitionKey, table_id: TableId) -> Result<Partition>;

    /// get partition by ID
    async fn get_by_id(&mut self, partition_id: PartitionId) -> Result<Option<Partition>>;

    /// return the partitions by table id
    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>>;

    /// return all partitions IDs
    async fn list_ids(&mut self) -> Result<Vec<PartitionId>>;

    /// Update the sort key for the partition, setting it to `new_sort_key` iff
    /// the current value matches `old_sort_key`.
    ///
    /// NOTE: it is expected that ONLY the ingesters update sort keys for
    /// existing partitions.
    ///
    /// # Spurious failure
    ///
    /// Implementations are allowed to spuriously return
    /// [`CasFailure::ValueMismatch`] for performance reasons in the presence of
    /// concurrent writers.
    async fn cas_sort_key(
        &mut self,
        partition_id: PartitionId,
        old_sort_key: Option<Vec<String>>,
        new_sort_key: &[&str],
    ) -> Result<Partition, CasFailure<Vec<String>>>;

    /// Record an instance of a partition being selected for compaction but compaction was not
    /// completed for the specified reason.
    #[allow(clippy::too_many_arguments)]
    async fn record_skipped_compaction(
        &mut self,
        partition_id: PartitionId,
        reason: &str,
        num_files: usize,
        limit_num_files: usize,
        limit_num_files_first_in_partition: usize,
        estimated_bytes: u64,
        limit_bytes: u64,
    ) -> Result<()>;

    /// Get the record of a partition being skipped.
    async fn get_in_skipped_compaction(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>>;

    /// List the records of compacting a partition being skipped. This is mostly useful for testing.
    async fn list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>>;

    /// Delete the records of skipping a partition being compacted.
    async fn delete_skipped_compactions(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>>;

    /// Return the N most recently created partitions.
    async fn most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>>;

    /// Select partitions with a `new_file_at` value greater than the minimum time value and, if specified, less than
    /// the maximum time value. Both range ends are exclusive; a timestamp exactly equal to either end will _not_ be
    /// included in the results.
    async fn partitions_new_file_between(
        &mut self,
        minimum_time: Timestamp,
        maximum_time: Option<Timestamp>,
    ) -> Result<Vec<PartitionId>>;
}

/// Functions for working with parquet file pointers in the catalog
#[async_trait]
pub trait ParquetFileRepo: Send + Sync {
    /// create the parquet file
    async fn create(&mut self, parquet_file_params: ParquetFileParams) -> Result<ParquetFile>;

    /// Flag the parquet file for deletion
    async fn flag_for_delete(&mut self, id: ParquetFileId) -> Result<()>;

    /// Flag all parquet files for deletion that are older than their namespace's retention period.
    async fn flag_for_delete_by_retention(&mut self) -> Result<Vec<ParquetFileId>>;

    /// List all parquet files within a given namespace that are NOT marked as
    /// [`to_delete`](ParquetFile::to_delete).
    async fn list_by_namespace_not_to_delete(
        &mut self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<ParquetFile>>;

    /// List all parquet files within a given table that are NOT marked as
    /// [`to_delete`](ParquetFile::to_delete).
    async fn list_by_table_not_to_delete(&mut self, table_id: TableId) -> Result<Vec<ParquetFile>>;

    /// List all parquet files within a given table including those marked as [`to_delete`](ParquetFile::to_delete).
    /// This is for debug purpose
    async fn list_by_table(&mut self, table_id: TableId) -> Result<Vec<ParquetFile>>;

    /// Delete parquet files that were marked to be deleted earlier than the specified time.
    ///
    /// Returns the deleted IDs only.
    ///
    /// This deletion is limited to a certain (backend-specific) number of files to avoid overlarge
    /// changes. The caller MAY call this method again if the result was NOT empty.
    async fn delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ParquetFileId>>;

    /// List parquet files for a given partition that are NOT marked as
    /// [`to_delete`](ParquetFile::to_delete).
    async fn list_by_partition_not_to_delete(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Vec<ParquetFile>>;

    /// Update the compaction level of the specified parquet files to
    /// the specified [`CompactionLevel`].
    /// Returns the IDs of the files that were successfully updated.
    async fn update_compaction_level(
        &mut self,
        parquet_file_ids: &[ParquetFileId],
        compaction_level: CompactionLevel,
    ) -> Result<Vec<ParquetFileId>>;

    /// Verify if the parquet file exists by selecting its id
    async fn exist(&mut self, id: ParquetFileId) -> Result<bool>;

    /// Return count
    async fn count(&mut self) -> Result<i64>;

    /// Return the parquet file with the given object store id
    async fn get_by_object_store_id(
        &mut self,
        object_store_id: Uuid,
    ) -> Result<Option<ParquetFile>>;
}

/// Gets the namespace schema including all tables and columns.
pub async fn get_schema_by_id<R>(
    id: NamespaceId,
    repos: &mut R,
    deleted: SoftDeletedRows,
) -> Result<NamespaceSchema>
where
    R: RepoCollection + ?Sized,
{
    let namespace = repos
        .namespaces()
        .get_by_id(id, deleted)
        .await?
        .context(NamespaceNotFoundByIdSnafu { id })?;

    get_schema_internal(namespace, repos).await
}

/// Gets the namespace schema including all tables and columns.
pub async fn get_schema_by_name<R>(
    name: &str,
    repos: &mut R,
    deleted: SoftDeletedRows,
) -> Result<NamespaceSchema>
where
    R: RepoCollection + ?Sized,
{
    let namespace = repos
        .namespaces()
        .get_by_name(name, deleted)
        .await?
        .context(NamespaceNotFoundByNameSnafu { name })?;

    get_schema_internal(namespace, repos).await
}

async fn get_schema_internal<R>(namespace: Namespace, repos: &mut R) -> Result<NamespaceSchema>
where
    R: RepoCollection + ?Sized,
{
    // get the columns first just in case someone else is creating schema while we're doing this.
    let columns = repos.columns().list_by_namespace_id(namespace.id).await?;
    let tables = repos.tables().list_by_namespace_id(namespace.id).await?;

    let mut namespace = NamespaceSchema::new_empty_from(&namespace);

    let mut table_id_to_schema = BTreeMap::new();
    for t in tables {
        let table_schema = TableSchema::new_empty_from(&t);
        table_id_to_schema.insert(t.id, (t.name, table_schema));
    }

    for c in columns {
        let (_, t) = table_id_to_schema.get_mut(&c.table_id).unwrap();
        t.add_column(c);
    }

    for (_, (table_name, schema)) in table_id_to_schema {
        namespace.tables.insert(table_name, schema);
    }

    Ok(namespace)
}

/// Gets all the table's columns.
pub async fn get_table_columns_by_id<R>(id: TableId, repos: &mut R) -> Result<ColumnsByName>
where
    R: RepoCollection + ?Sized,
{
    let columns = repos.columns().list_by_table_id(id).await?;

    Ok(ColumnsByName::new(columns))
}

/// Fetch all [`NamespaceSchema`] in the catalog.
///
/// This method performs the minimal number of queries needed to build the
/// result set. No table lock is obtained, nor are queries executed within a
/// transaction, but this method does return a point-in-time snapshot of the
/// catalog state.
///
/// # Soft Deletion
///
/// No schemas for soft-deleted namespaces are returned.
pub async fn list_schemas(
    catalog: &dyn Catalog,
) -> Result<impl Iterator<Item = (Namespace, NamespaceSchema)>> {
    let mut repos = catalog.repositories().await;

    // In order to obtain a point-in-time snapshot, first fetch the columns,
    // then the tables, and then resolve the namespace IDs to Namespace in order
    // to construct the schemas.
    //
    // The set of columns returned forms the state snapshot, with the subsequent
    // queries resolving only what is needed to construct schemas for the
    // retrieved columns (ignoring any newly added tables/namespaces since the
    // column snapshot was taken).
    //
    // This approach also tolerates concurrently deleted namespaces, which are
    // simply ignored at the end when joining to the namespace query result.

    // First fetch all the columns - this is the state snapshot of the catalog
    // schemas.
    let columns = repos.columns().list().await?;

    // Construct the set of table IDs these columns belong to.
    let retain_table_ids = columns.iter().map(|c| c.table_id).collect::<HashSet<_>>();

    // Fetch all tables, and filter for those that are needed to construct
    // schemas for "columns" only.
    //
    // Discard any tables that have no columns or have been created since
    // the "columns" snapshot was retrieved, and construct a map of ID->Table.
    let tables = repos
        .tables()
        .list()
        .await?
        .into_iter()
        .filter_map(|t| {
            if !retain_table_ids.contains(&t.id) {
                return None;
            }

            Some((t.id, t))
        })
        .collect::<HashMap<_, _>>();

    // Drop the table ID set as it will not be referenced again.
    drop(retain_table_ids);

    // Do all the I/O to fetch the namespaces in the background, while this
    // thread constructs the NamespaceId->TableSchema map below.
    let namespaces = tokio::spawn(async move {
        repos
            .namespaces()
            .list(SoftDeletedRows::ExcludeDeleted)
            .await
    });

    // A set of tables within a single namespace.
    type NamespaceTables = BTreeMap<String, TableSchema>;

    let mut joined = HashMap::<NamespaceId, NamespaceTables>::default();
    for column in columns {
        // Resolve the table this column references
        let table = tables.get(&column.table_id).expect("no table for column");

        let table_schema = joined
            // Find or create a record in the joined <NamespaceId, Tables> map
            // for this namespace ID.
            .entry(table.namespace_id)
            .or_default()
            // Fetch the schema record for this table, or create an empty one.
            .entry(table.name.clone())
            .or_insert_with(|| TableSchema::new_empty_from(table));

        table_schema.add_column(column);
    }

    // The table map is no longer needed - immediately reclaim the memory.
    drop(tables);

    // Convert the Namespace instances into NamespaceSchema instances.
    let iter = namespaces
        .await
        .expect("namespace list task panicked")?
        .into_iter()
        // Ignore any namespaces that did not exist when the "columns" snapshot
        // was created, or have no tables/columns (and therefore have no entry
        // in "joined").
        .filter_map(move |v| {
            // The catalog call explicitly asked for no soft deleted records.
            assert!(v.deleted_at.is_none());

            let mut ns = NamespaceSchema::new_empty_from(&v);

            ns.tables = joined.remove(&v.id)?;
            Some((v, ns))
        });

    Ok(iter)
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use crate::{validate_or_insert_schema, DEFAULT_MAX_COLUMNS_PER_TABLE, DEFAULT_MAX_TABLES};

    use super::*;
    use ::test_helpers::{assert_contains, assert_error, tracing::TracingCapture};
    use assert_matches::assert_matches;
    use data_types::{ColumnId, ColumnSet, CompactionLevel};
    use futures::Future;
    use metric::{Attributes, DurationHistogram, Metric};
    use std::{collections::BTreeSet, ops::DerefMut, sync::Arc, time::Duration};

    pub(crate) async fn test_catalog<R, F>(clean_state: R)
    where
        R: Fn() -> F + Send + Sync,
        F: Future<Output = Arc<dyn Catalog>> + Send,
    {
        test_setup(clean_state().await).await;
        test_namespace_soft_deletion(clean_state().await).await;
        test_partitions_new_file_between(clean_state().await).await;
        test_column(clean_state().await).await;
        test_partition(clean_state().await).await;
        test_parquet_file(clean_state().await).await;
        test_parquet_file_delete_broken(clean_state().await).await;
        test_update_to_compaction_level_1(clean_state().await).await;
        test_list_by_partiton_not_to_delete(clean_state().await).await;
        test_txn_isolation(clean_state().await).await;
        test_txn_drop(clean_state().await).await;
        test_list_schemas(clean_state().await).await;
        test_list_schemas_soft_deleted_rows(clean_state().await).await;
        test_delete_namespace(clean_state().await).await;

        let catalog = clean_state().await;
        test_namespace(Arc::clone(&catalog)).await;
        assert_metric_hit(&catalog.metrics(), "namespace_create");

        let catalog = clean_state().await;
        test_table(Arc::clone(&catalog)).await;
        assert_metric_hit(&catalog.metrics(), "table_create");

        let catalog = clean_state().await;
        test_column(Arc::clone(&catalog)).await;
        assert_metric_hit(&catalog.metrics(), "column_create_or_get");

        let catalog = clean_state().await;
        test_partition(Arc::clone(&catalog)).await;
        assert_metric_hit(&catalog.metrics(), "partition_create_or_get");

        let catalog = clean_state().await;
        test_parquet_file(Arc::clone(&catalog)).await;
        assert_metric_hit(&catalog.metrics(), "parquet_create");
    }

    async fn test_setup(catalog: Arc<dyn Catalog>) {
        catalog.setup().await.expect("first catalog setup");
        catalog.setup().await.expect("second catalog setup");
    }

    async fn test_namespace(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace_name = "test_namespace";
        let namespace = repos
            .namespaces()
            .create(namespace_name, None, None)
            .await
            .unwrap();
        assert!(namespace.id > NamespaceId::new(0));
        assert_eq!(namespace.name, namespace_name);
        // assert_eq!(
        //     &namespace.partition_template.as_ref().unwrap().0,
        //     the default
        // );

        // Assert default values for service protection limits.
        assert_eq!(namespace.max_tables, DEFAULT_MAX_TABLES);
        assert_eq!(
            namespace.max_columns_per_table,
            DEFAULT_MAX_COLUMNS_PER_TABLE
        );

        let conflict = repos.namespaces().create(namespace_name, None, None).await;
        assert!(matches!(
            conflict.unwrap_err(),
            Error::NameExists { name: _ }
        ));

        let found = repos
            .namespaces()
            .get_by_id(namespace.id, SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap()
            .expect("namespace should be there");
        assert_eq!(namespace, found);

        let not_found = repos
            .namespaces()
            .get_by_id(NamespaceId::new(i64::MAX), SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap();
        assert!(not_found.is_none());

        let found = repos
            .namespaces()
            .get_by_name(namespace_name, SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap()
            .expect("namespace should be there");
        assert_eq!(namespace, found);

        let not_found = repos
            .namespaces()
            .get_by_name("does_not_exist", SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap();
        assert!(not_found.is_none());

        let namespace2_name = "test_namespace2";
        let namespace2 = repos
            .namespaces()
            .create(namespace2_name, None, None)
            .await
            .unwrap();
        let mut namespaces = repos
            .namespaces()
            .list(SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap();
        namespaces.sort_by_key(|ns| ns.name.clone());
        assert_eq!(namespaces, vec![namespace, namespace2]);

        const NEW_TABLE_LIMIT: i32 = 15000;
        let modified = repos
            .namespaces()
            .update_table_limit(namespace_name, NEW_TABLE_LIMIT)
            .await
            .expect("namespace should be updateable");
        assert_eq!(NEW_TABLE_LIMIT, modified.max_tables);

        const NEW_COLUMN_LIMIT: i32 = 1500;
        let modified = repos
            .namespaces()
            .update_column_limit(namespace_name, NEW_COLUMN_LIMIT)
            .await
            .expect("namespace should be updateable");
        assert_eq!(NEW_COLUMN_LIMIT, modified.max_columns_per_table);

        const NEW_RETENTION_PERIOD_NS: i64 = 5 * 60 * 60 * 1000 * 1000 * 1000;
        let modified = repos
            .namespaces()
            .update_retention_period(namespace_name, Some(NEW_RETENTION_PERIOD_NS))
            .await
            .expect("namespace should be updateable");
        assert_eq!(
            NEW_RETENTION_PERIOD_NS,
            modified.retention_period_ns.unwrap()
        );

        let modified = repos
            .namespaces()
            .update_retention_period(namespace_name, None)
            .await
            .expect("namespace should be updateable");
        assert!(modified.retention_period_ns.is_none());

        // create namespace with retention period NULL
        let namespace3_name = "test_namespace3";
        let namespace3 = repos
            .namespaces()
            .create(namespace3_name, None, None)
            .await
            .expect("namespace with NULL retention should be created");
        assert!(namespace3.retention_period_ns.is_none());

        // create namespace with retention period
        let namespace4_name = "test_namespace4";
        let namespace4 = repos
            .namespaces()
            .create(namespace4_name, None, Some(NEW_RETENTION_PERIOD_NS))
            .await
            .expect("namespace with 5-hour retention should be created");
        assert_eq!(
            NEW_RETENTION_PERIOD_NS,
            namespace4.retention_period_ns.unwrap()
        );
        // reset retention period to NULL to avoid affecting later tests
        repos
            .namespaces()
            .update_retention_period(namespace4_name, None)
            .await
            .expect("namespace should be updateable");

        // create a namespace with a PartitionTemplate other than the default
        let tag_partition_template = proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::ColumnValue("tag1".into())),
            }],
        };
        let namespace5_name = "test_namespace5";
        let namespace5 = repos
            .namespaces()
            .create(namespace5_name, Some(&tag_partition_template), None)
            .await
            .unwrap();
        assert_eq!(
            proto_from_raw(&namespace5.partition_template).unwrap(),
            tag_partition_template
        );
        let lookup_namespace5 = repos
            .namespaces()
            .get_by_name(namespace5_name, SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(namespace5, lookup_namespace5);

        // remove namespace to avoid it from affecting later tests
        repos
            .namespaces()
            .soft_delete("test_namespace")
            .await
            .expect("delete namespace should succeed");
        repos
            .namespaces()
            .soft_delete("test_namespace2")
            .await
            .expect("delete namespace should succeed");
        repos
            .namespaces()
            .soft_delete("test_namespace3")
            .await
            .expect("delete namespace should succeed");
        repos
            .namespaces()
            .soft_delete("test_namespace4")
            .await
            .expect("delete namespace should succeed");
    }

    /// Construct a set of two namespaces:
    ///
    ///  * deleted-ns: marked as soft-deleted
    ///  * active-ns: not marked as deleted
    ///
    /// And assert the expected "soft delete" semantics / correctly filter out
    /// the expected rows for all three states of [`SoftDeletedRows`].
    async fn test_namespace_soft_deletion(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;

        let deleted_ns = repos
            .namespaces()
            .create("deleted-ns", None, None)
            .await
            .unwrap();
        let active_ns = repos
            .namespaces()
            .create("active-ns", None, None)
            .await
            .unwrap();

        // Mark "deleted-ns" as soft-deleted.
        repos.namespaces().soft_delete("deleted-ns").await.unwrap();

        // Which should be idempotent (ignoring the timestamp change - when
        // changing this to "soft delete" it was idempotent, so I am preserving
        // that).
        repos.namespaces().soft_delete("deleted-ns").await.unwrap();

        // Listing should respect soft deletion.
        let got = repos
            .namespaces()
            .list(SoftDeletedRows::AllRows)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["deleted-ns", "active-ns"]);

        let got = repos
            .namespaces()
            .list(SoftDeletedRows::OnlyDeleted)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["deleted-ns"]);

        let got = repos
            .namespaces()
            .list(SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["active-ns"]);

        // As should get by ID
        let got = repos
            .namespaces()
            .get_by_id(deleted_ns.id, SoftDeletedRows::AllRows)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["deleted-ns"]);
        let got = repos
            .namespaces()
            .get_by_id(deleted_ns.id, SoftDeletedRows::OnlyDeleted)
            .await
            .unwrap()
            .into_iter()
            .map(|v| {
                assert!(v.deleted_at.is_some());
                v.name
            });
        assert_string_set_eq(got, ["deleted-ns"]);
        let got = repos
            .namespaces()
            .get_by_id(deleted_ns.id, SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap();
        assert!(got.is_none());
        let got = repos
            .namespaces()
            .get_by_id(active_ns.id, SoftDeletedRows::AllRows)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["active-ns"]);
        let got = repos
            .namespaces()
            .get_by_id(active_ns.id, SoftDeletedRows::OnlyDeleted)
            .await
            .unwrap();
        assert!(got.is_none());
        let got = repos
            .namespaces()
            .get_by_id(active_ns.id, SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["active-ns"]);

        // And get by name
        let got = repos
            .namespaces()
            .get_by_name(&deleted_ns.name, SoftDeletedRows::AllRows)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["deleted-ns"]);
        let got = repos
            .namespaces()
            .get_by_name(&deleted_ns.name, SoftDeletedRows::OnlyDeleted)
            .await
            .unwrap()
            .into_iter()
            .map(|v| {
                assert!(v.deleted_at.is_some());
                v.name
            });
        assert_string_set_eq(got, ["deleted-ns"]);
        let got = repos
            .namespaces()
            .get_by_name(&deleted_ns.name, SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap();
        assert!(got.is_none());
        let got = repos
            .namespaces()
            .get_by_name(&active_ns.name, SoftDeletedRows::AllRows)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["active-ns"]);
        let got = repos
            .namespaces()
            .get_by_name(&active_ns.name, SoftDeletedRows::OnlyDeleted)
            .await
            .unwrap();
        assert!(got.is_none());
        let got = repos
            .namespaces()
            .get_by_name(&active_ns.name, SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["active-ns"]);
    }

    // Assert the set of strings "a" is equal to the set "b", tolerating
    // duplicates.
    #[track_caller]
    fn assert_string_set_eq<T, U>(a: impl IntoIterator<Item = T>, b: impl IntoIterator<Item = U>)
    where
        T: Into<String>,
        U: Into<String>,
    {
        let mut a = a.into_iter().map(Into::into).collect::<Vec<String>>();
        a.sort_unstable();
        let mut b = b.into_iter().map(Into::into).collect::<Vec<String>>();
        b.sort_unstable();
        assert_eq!(a, b);
    }

    async fn test_table(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace = repos
            .namespaces()
            .create("namespace_table_test", None, None)
            .await
            .unwrap();

        // test we can create a table
        let t = repos
            .tables()
            .create("test_table", None, namespace.id)
            .await
            .unwrap();
        assert!(t.id > TableId::new(0));

        // test we get an error if we try to create it again
        let err = repos
            .tables()
            .create("test_table", None, namespace.id)
            .await;
        assert_error!(
            err,
            Error::TableNameExists { ref name, namespace_id }
                if name == "test_table" && namespace_id == namespace.id
        );

        // get by id
        assert_eq!(t, repos.tables().get_by_id(t.id).await.unwrap().unwrap());
        assert!(repos
            .tables()
            .get_by_id(TableId::new(i64::MAX))
            .await
            .unwrap()
            .is_none());

        let tables = repos
            .tables()
            .list_by_namespace_id(namespace.id)
            .await
            .unwrap();
        assert_eq!(vec![t.clone()], tables);

        // test we can create a table of the same name in a different namespace
        let namespace2 = repos.namespaces().create("two", None, None).await.unwrap();
        assert_ne!(namespace, namespace2);
        let test_table = repos
            .tables()
            .create("test_table", None, namespace2.id)
            .await
            .unwrap();
        assert_ne!(t.id, test_table.id);
        assert_eq!(test_table.namespace_id, namespace2.id);

        // create a table with a PartitionTemplate other than the default
        let tag_partition_template_proto = proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::ColumnValue("tag1".into())),
            }],
        };
        let tag_partition_template = PartitionTemplate::from(&tag_partition_template_proto);
        let partitioned_table_name = "test_partitioned_table";
        let test_partitioned_table = repos
            .tables()
            .create(
                partitioned_table_name,
                Some(&tag_partition_template_proto),
                namespace2.id,
            )
            .await
            .unwrap();
        assert_eq!(
            test_partitioned_table.partition_template.inner(),
            &tag_partition_template
        );
        let lookup_table = repos
            .tables()
            .get_by_namespace_and_name(namespace2.id, partitioned_table_name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(test_partitioned_table, lookup_table);

        // test get by namespace and name
        let foo_table = repos
            .tables()
            .create("foo", None, namespace2.id)
            .await
            .unwrap();
        assert_eq!(
            repos
                .tables()
                .get_by_namespace_and_name(NamespaceId::new(i64::MAX), "test_table")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            repos
                .tables()
                .get_by_namespace_and_name(namespace.id, "not_existing")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            repos
                .tables()
                .get_by_namespace_and_name(namespace.id, "test_table")
                .await
                .unwrap(),
            Some(t.clone())
        );
        assert_eq!(
            repos
                .tables()
                .get_by_namespace_and_name(namespace2.id, "test_table")
                .await
                .unwrap()
                .as_ref(),
            Some(&test_table)
        );
        assert_eq!(
            repos
                .tables()
                .get_by_namespace_and_name(namespace2.id, "foo")
                .await
                .unwrap()
                .as_ref(),
            Some(&foo_table)
        );

        // All tables should be returned by list(), regardless of namespace
        let mut list = repos.tables().list().await.unwrap();
        list.sort_by_key(|t| t.id);
        let mut expected = [t, test_table, foo_table, test_partitioned_table];
        expected.sort_by_key(|t| t.id);
        assert_eq!(&list, &expected);

        // test per-namespace table limits
        let latest = repos
            .namespaces()
            .update_table_limit("namespace_table_test", 1)
            .await
            .expect("namespace should be updateable");
        let err = repos
            .tables()
            .create("definitely_unique", None, latest.id)
            .await
            .expect_err("should error with table create limit error");
        assert!(matches!(
            err,
            Error::TableCreateLimitError {
                table_name: _,
                namespace_id: _
            }
        ));

        repos
            .namespaces()
            .soft_delete("namespace_table_test")
            .await
            .expect("delete namespace should succeed");
        repos
            .namespaces()
            .soft_delete("two")
            .await
            .expect("delete namespace should succeed");
    }

    async fn test_column(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace = repos
            .namespaces()
            .create("namespace_column_test", None, None)
            .await
            .unwrap();
        let table = repos
            .tables()
            .create("test_table", None, namespace.id)
            .await
            .unwrap();
        assert_eq!(table.namespace_id, namespace.id);

        // test we can create or get a column
        let c = repos
            .columns()
            .create_or_get("column_test", table.id, ColumnType::Tag)
            .await
            .unwrap();
        let cc = repos
            .columns()
            .create_or_get("column_test", table.id, ColumnType::Tag)
            .await
            .unwrap();
        assert!(c.id > ColumnId::new(0));
        assert_eq!(c, cc);

        // test that attempting to create an already defined column of a different type returns
        // error
        let err = repos
            .columns()
            .create_or_get("column_test", table.id, ColumnType::U64)
            .await
            .expect_err("should error with wrong column type");
        assert!(matches!(err, Error::ColumnTypeMismatch { .. }));

        // test that we can create a column of the same name under a different table
        let table2 = repos
            .tables()
            .create("test_table_2", None, namespace.id)
            .await
            .unwrap();
        let ccc = repos
            .columns()
            .create_or_get("column_test", table2.id, ColumnType::U64)
            .await
            .unwrap();
        assert_ne!(c, ccc);

        let columns = repos
            .columns()
            .list_by_namespace_id(namespace.id)
            .await
            .unwrap();

        let mut want = vec![c.clone(), ccc];
        assert_eq!(want, columns);

        let columns = repos.columns().list_by_table_id(table.id).await.unwrap();

        let want2 = vec![c];
        assert_eq!(want2, columns);

        // Add another tag column into table2
        let c3 = repos
            .columns()
            .create_or_get("b", table2.id, ColumnType::Tag)
            .await
            .unwrap();

        // Listing columns should return all columns in the catalog
        let list = repos.columns().list().await.unwrap();
        want.extend([c3]);
        assert_eq!(list, want);

        // test create_or_get_many_unchecked, below column limit
        let mut columns = HashMap::new();
        columns.insert("column_test", ColumnType::Tag);
        columns.insert("new_column", ColumnType::Tag);
        let table1_columns = repos
            .columns()
            .create_or_get_many_unchecked(table.id, columns)
            .await
            .unwrap();
        let mut table1_column_names: Vec<_> = table1_columns.iter().map(|c| &c.name).collect();
        table1_column_names.sort();
        assert_eq!(table1_column_names, vec!["column_test", "new_column"]);

        // test per-namespace column limits
        repos
            .namespaces()
            .update_column_limit("namespace_column_test", 1)
            .await
            .expect("namespace should be updateable");
        let err = repos
            .columns()
            .create_or_get("definitely unique", table.id, ColumnType::Tag)
            .await
            .expect_err("should error with table create limit error");
        assert!(matches!(
            err,
            Error::ColumnCreateLimitError {
                column_name: _,
                table_id: _,
            }
        ));

        // test per-namespace column limits are NOT enforced with create_or_get_many_unchecked
        let table3 = repos
            .tables()
            .create("test_table_3", None, namespace.id)
            .await
            .unwrap();
        let mut columns = HashMap::new();
        columns.insert("apples", ColumnType::Tag);
        columns.insert("oranges", ColumnType::Tag);
        let table3_columns = repos
            .columns()
            .create_or_get_many_unchecked(table3.id, columns)
            .await
            .unwrap();
        let mut table3_column_names: Vec<_> = table3_columns.iter().map(|c| &c.name).collect();
        table3_column_names.sort();
        assert_eq!(table3_column_names, vec!["apples", "oranges"]);

        repos
            .namespaces()
            .soft_delete("namespace_column_test")
            .await
            .expect("delete namespace should succeed");
    }

    async fn test_partition(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace = repos
            .namespaces()
            .create("namespace_partition_test", None, None)
            .await
            .unwrap();
        let table = repos
            .tables()
            .create("test_table", None, namespace.id)
            .await
            .unwrap();

        let mut created = BTreeMap::new();
        for key in ["foo", "bar"] {
            let partition = repos
                .partitions()
                .create_or_get(key.into(), table.id)
                .await
                .expect("failed to create partition");
            created.insert(partition.id, partition);
        }
        let other_partition = repos
            .partitions()
            .create_or_get("asdf".into(), table.id)
            .await
            .unwrap();

        // partitions can be retrieved easily
        assert_eq!(
            other_partition,
            repos
                .partitions()
                .get_by_id(other_partition.id)
                .await
                .unwrap()
                .unwrap()
        );
        assert!(repos
            .partitions()
            .get_by_id(PartitionId::new(i64::MAX))
            .await
            .unwrap()
            .is_none());

        let listed = repos
            .partitions()
            .list_by_table_id(table.id)
            .await
            .expect("failed to list partitions")
            .into_iter()
            .map(|v| (v.id, v))
            .collect::<BTreeMap<_, _>>();

        created.insert(other_partition.id, other_partition.clone());
        assert_eq!(created, listed);

        let listed = repos
            .partitions()
            .list_ids()
            .await
            .expect("failed to list partitions")
            .into_iter()
            .collect::<BTreeSet<_>>();

        assert_eq!(created.keys().copied().collect::<BTreeSet<_>>(), listed);

        // sort_key should be empty on creation
        assert!(other_partition.sort_key.is_empty());

        // test update_sort_key from None to Some
        repos
            .partitions()
            .cas_sort_key(other_partition.id, None, &["tag2", "tag1", "time"])
            .await
            .unwrap();

        // test sort key CAS with an incorrect value
        let err = repos
            .partitions()
            .cas_sort_key(
                other_partition.id,
                Some(["bananas".to_string()].to_vec()),
                &["tag2", "tag1", "tag3 , with comma", "time"],
            )
            .await
            .expect_err("CAS with incorrect value should fail");
        assert_matches!(err, CasFailure::ValueMismatch(old) => {
            assert_eq!(old, &["tag2", "tag1", "time"]);
        });

        // test getting the new sort key
        let updated_other_partition = repos
            .partitions()
            .get_by_id(other_partition.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            updated_other_partition.sort_key,
            vec!["tag2", "tag1", "time"]
        );

        // test sort key CAS with no value
        let err = repos
            .partitions()
            .cas_sort_key(
                other_partition.id,
                None,
                &["tag2", "tag1", "tag3 , with comma", "time"],
            )
            .await
            .expect_err("CAS with incorrect value should fail");
        assert_matches!(err, CasFailure::ValueMismatch(old) => {
            assert_eq!(old, ["tag2", "tag1", "time"]);
        });

        // test sort key CAS with an incorrect value
        let err = repos
            .partitions()
            .cas_sort_key(
                other_partition.id,
                Some(["bananas".to_string()].to_vec()),
                &["tag2", "tag1", "tag3 , with comma", "time"],
            )
            .await
            .expect_err("CAS with incorrect value should fail");
        assert_matches!(err, CasFailure::ValueMismatch(old) => {
            assert_eq!(old, ["tag2", "tag1", "time"]);
        });

        // test update_sort_key from Some value to Some other value
        repos
            .partitions()
            .cas_sort_key(
                other_partition.id,
                Some(
                    ["tag2", "tag1", "time"]
                        .into_iter()
                        .map(ToString::to_string)
                        .collect(),
                ),
                &["tag2", "tag1", "tag3 , with comma", "time"],
            )
            .await
            .unwrap();

        // test getting the new sort key
        let updated_other_partition = repos
            .partitions()
            .get_by_id(other_partition.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            updated_other_partition.sort_key,
            vec!["tag2", "tag1", "tag3 , with comma", "time"]
        );

        // The compactor can log why compaction was skipped
        let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
        assert!(
            skipped_compactions.is_empty(),
            "Expected no skipped compactions, got: {skipped_compactions:?}"
        );
        repos
            .partitions()
            .record_skipped_compaction(other_partition.id, "I am le tired", 1, 2, 4, 10, 20)
            .await
            .unwrap();
        let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
        assert_eq!(skipped_compactions.len(), 1);
        assert_eq!(skipped_compactions[0].partition_id, other_partition.id);
        assert_eq!(skipped_compactions[0].reason, "I am le tired");
        assert_eq!(skipped_compactions[0].num_files, 1);
        assert_eq!(skipped_compactions[0].limit_num_files, 2);
        assert_eq!(skipped_compactions[0].estimated_bytes, 10);
        assert_eq!(skipped_compactions[0].limit_bytes, 20);
        //
        let skipped_partition_record = repos
            .partitions()
            .get_in_skipped_compaction(other_partition.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(skipped_partition_record.partition_id, other_partition.id);
        assert_eq!(skipped_partition_record.reason, "I am le tired");

        // Only save the last reason that any particular partition was skipped (really if the
        // partition appears in the skipped compactions, it shouldn't become a compaction candidate
        // again, but race conditions and all that)
        repos
            .partitions()
            .record_skipped_compaction(other_partition.id, "I'm on fire", 11, 12, 24, 110, 120)
            .await
            .unwrap();
        let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
        assert_eq!(skipped_compactions.len(), 1);
        assert_eq!(skipped_compactions[0].partition_id, other_partition.id);
        assert_eq!(skipped_compactions[0].reason, "I'm on fire");
        assert_eq!(skipped_compactions[0].num_files, 11);
        assert_eq!(skipped_compactions[0].limit_num_files, 12);
        assert_eq!(skipped_compactions[0].estimated_bytes, 110);
        assert_eq!(skipped_compactions[0].limit_bytes, 120);
        //
        let skipped_partition_record = repos
            .partitions()
            .get_in_skipped_compaction(other_partition.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(skipped_partition_record.partition_id, other_partition.id);
        assert_eq!(skipped_partition_record.reason, "I'm on fire");

        // Delete the skipped compaction
        let deleted_skipped_compaction = repos
            .partitions()
            .delete_skipped_compactions(other_partition.id)
            .await
            .unwrap()
            .expect("The skipped compaction should have been returned");

        assert_eq!(deleted_skipped_compaction.partition_id, other_partition.id);
        assert_eq!(deleted_skipped_compaction.reason, "I'm on fire");
        assert_eq!(deleted_skipped_compaction.num_files, 11);
        assert_eq!(deleted_skipped_compaction.limit_num_files, 12);
        assert_eq!(deleted_skipped_compaction.estimated_bytes, 110);
        assert_eq!(deleted_skipped_compaction.limit_bytes, 120);
        //
        let skipped_partition_record = repos
            .partitions()
            .get_in_skipped_compaction(other_partition.id)
            .await
            .unwrap();
        assert!(skipped_partition_record.is_none());

        let not_deleted_skipped_compaction = repos
            .partitions()
            .delete_skipped_compactions(other_partition.id)
            .await
            .unwrap();

        assert!(
            not_deleted_skipped_compaction.is_none(),
            "There should be no skipped compation",
        );

        let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
        assert!(
            skipped_compactions.is_empty(),
            "Expected no skipped compactions, got: {skipped_compactions:?}"
        );

        let recent = repos
            .partitions()
            .most_recent_n(10)
            .await
            .expect("should list most recent");
        assert_eq!(recent.len(), 3);

        let recent = repos
            .partitions()
            .most_recent_n(3)
            .await
            .expect("should list most recent");
        assert_eq!(recent.len(), 3);

        let recent = repos
            .partitions()
            .most_recent_n(2)
            .await
            .expect("should list most recent");
        assert_eq!(recent.len(), 2);

        repos
            .namespaces()
            .soft_delete("namespace_partition_test")
            .await
            .expect("delete namespace should succeed");
    }

    /// tests many interactions with the catalog and parquet files. See the individual conditions herein
    async fn test_parquet_file(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace = repos
            .namespaces()
            .create("namespace_parquet_file_test", None, None)
            .await
            .unwrap();
        let table = repos
            .tables()
            .create("test_table", None, namespace.id)
            .await
            .unwrap();
        let other_table = repos
            .tables()
            .create("other", None, namespace.id)
            .await
            .unwrap();
        let partition = repos
            .partitions()
            .create_or_get("one".into(), table.id)
            .await
            .unwrap();
        let other_partition = repos
            .partitions()
            .create_or_get("one".into(), other_table.id)
            .await
            .unwrap();

        let parquet_file_params = ParquetFileParams {
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(10),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
            max_l0_created_at: Timestamp::new(1),
        };
        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();

        // verify we can get it by its object store id
        let pfg = repos
            .parquet_files()
            .get_by_object_store_id(parquet_file.object_store_id)
            .await
            .unwrap();
        assert_eq!(parquet_file, pfg.unwrap());

        // verify that trying to create a file with the same UUID throws an error
        let err = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::FileExists { object_store_id: _ }));

        let other_params = ParquetFileParams {
            table_id: other_partition.table_id,
            partition_id: other_partition.id,
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(50),
            max_time: Timestamp::new(60),
            ..parquet_file_params.clone()
        };
        let other_file = repos.parquet_files().create(other_params).await.unwrap();

        let exist_id = parquet_file.id;
        let non_exist_id = ParquetFileId::new(other_file.id.get() + 10);
        // make sure exists_id != non_exist_id
        assert_ne!(exist_id, non_exist_id);
        assert!(repos.parquet_files().exist(exist_id).await.unwrap());
        assert!(!repos.parquet_files().exist(non_exist_id).await.unwrap());

        // verify that to_delete is initially set to null and the file does not get deleted
        assert!(parquet_file.to_delete.is_none());
        let older_than = Timestamp::new(
            (catalog.time_provider().now() + Duration::from_secs(100)).timestamp_nanos(),
        );
        let deleted = repos
            .parquet_files()
            .delete_old_ids_only(older_than)
            .await
            .unwrap();
        assert!(deleted.is_empty());
        assert!(repos.parquet_files().exist(parquet_file.id).await.unwrap());

        // test list_by_table that includes soft-deleted file
        // at this time the file is not soft-deleted yet and will be included in the returned list
        let files = repos
            .parquet_files()
            .list_by_table(parquet_file.table_id)
            .await
            .unwrap();
        assert_eq!(files.len(), 1);

        // verify to_delete can be updated to a timestamp
        repos
            .parquet_files()
            .flag_for_delete(parquet_file.id)
            .await
            .unwrap();

        // test list_by_table that includes soft-deleted file
        // at this time the file is soft-deleted and will be included in the returned list
        let files = repos
            .parquet_files()
            .list_by_table(parquet_file.table_id)
            .await
            .unwrap();
        assert_eq!(files.len(), 1);
        let marked_deleted = files.first().unwrap();
        assert!(marked_deleted.to_delete.is_some());

        // File is not deleted if it was marked to be deleted after the specified time
        let before_deleted = Timestamp::new(
            (catalog.time_provider().now() - Duration::from_secs(100)).timestamp_nanos(),
        );
        let deleted = repos
            .parquet_files()
            .delete_old_ids_only(before_deleted)
            .await
            .unwrap();
        assert!(deleted.is_empty());
        assert!(repos.parquet_files().exist(parquet_file.id).await.unwrap());

        // test list_by_table that includes soft-deleted file
        // at this time the file is not actually hard deleted yet and stay as soft deleted
        // and will be returned in the list
        let files = repos
            .parquet_files()
            .list_by_table(parquet_file.table_id)
            .await
            .unwrap();
        assert_eq!(files.len(), 1);

        // File is deleted if it was marked to be deleted before the specified time
        let deleted = repos
            .parquet_files()
            .delete_old_ids_only(older_than)
            .await
            .unwrap();
        assert_eq!(deleted.len(), 1);
        assert_eq!(marked_deleted.id, deleted[0]);
        assert!(!repos.parquet_files().exist(parquet_file.id).await.unwrap());

        // test list_by_table that includes soft-deleted file
        // at this time the file is hard deleted -> the returned list is empty
        let files = repos
            .parquet_files()
            .list_by_table(parquet_file.table_id)
            .await
            .unwrap();
        assert_eq!(files.len(), 0);

        // test list_by_table_not_to_delete
        let files = repos
            .parquet_files()
            .list_by_table_not_to_delete(table.id)
            .await
            .unwrap();
        assert_eq!(files, vec![]);
        let files = repos
            .parquet_files()
            .list_by_table_not_to_delete(other_table.id)
            .await
            .unwrap();
        assert_eq!(files, vec![other_file.clone()]);

        // test list_by_table
        println!("parquet_file.table_id = {}", parquet_file.table_id);
        let files = repos
            .parquet_files()
            // .list_by_table(parquet_file.table_id) // todo: tables of deleted files
            .list_by_table(other_file.table_id)
            .await
            .unwrap();
        assert_eq!(files.len(), 1);

        // test list_by_namespace_not_to_delete
        let namespace2 = repos
            .namespaces()
            .create("namespace_parquet_file_test1", None, None)
            .await
            .unwrap();
        let table2 = repos
            .tables()
            .create("test_table2", None, namespace2.id)
            .await
            .unwrap();
        let partition2 = repos
            .partitions()
            .create_or_get("foo".into(), table2.id)
            .await
            .unwrap();
        let files = repos
            .parquet_files()
            .list_by_namespace_not_to_delete(namespace2.id)
            .await
            .unwrap();
        assert!(files.is_empty());

        let f1_params = ParquetFileParams {
            table_id: partition2.table_id,
            partition_id: partition2.id,
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(10),
            ..parquet_file_params
        };
        let f1 = repos
            .parquet_files()
            .create(f1_params.clone())
            .await
            .unwrap();

        let f2_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(50),
            max_time: Timestamp::new(60),
            ..f1_params.clone()
        };
        let f2 = repos
            .parquet_files()
            .create(f2_params.clone())
            .await
            .unwrap();
        let files = repos
            .parquet_files()
            .list_by_namespace_not_to_delete(namespace2.id)
            .await
            .unwrap();
        assert_eq!(vec![f1.clone(), f2.clone()], files);

        let f3_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(50),
            max_time: Timestamp::new(60),
            ..f2_params
        };
        let f3 = repos
            .parquet_files()
            .create(f3_params.clone())
            .await
            .unwrap();
        let files = repos
            .parquet_files()
            .list_by_namespace_not_to_delete(namespace2.id)
            .await
            .unwrap();
        assert_eq!(vec![f1.clone(), f2.clone(), f3.clone()], files);

        repos.parquet_files().flag_for_delete(f2.id).await.unwrap();
        let files = repos
            .parquet_files()
            .list_by_namespace_not_to_delete(namespace2.id)
            .await
            .unwrap();
        assert_eq!(vec![f1.clone(), f3.clone()], files);

        let files = repos
            .parquet_files()
            .list_by_namespace_not_to_delete(NamespaceId::new(i64::MAX))
            .await
            .unwrap();
        assert!(files.is_empty());

        // test delete_old_ids_only
        let older_than = Timestamp::new(
            (catalog.time_provider().now() + Duration::from_secs(100)).timestamp_nanos(),
        );
        let ids = repos
            .parquet_files()
            .delete_old_ids_only(older_than)
            .await
            .unwrap();
        assert_eq!(ids.len(), 1);

        // test retention-based flagging for deletion
        // Since mem catalog has default retention 1 hour, let us first set it to 0 means infinite
        let namespaces = repos
            .namespaces()
            .list(SoftDeletedRows::AllRows)
            .await
            .expect("listing namespaces");
        for namespace in namespaces {
            repos
                .namespaces()
                .update_retention_period(&namespace.name, None) // infinite
                .await
                .unwrap();
        }

        // 1. with no retention period set on the ns, nothing should get flagged
        let ids = repos
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .unwrap();
        assert!(ids.is_empty());
        // 2. set ns retention period to one hour then create some files before and after and
        //    ensure correct files get deleted
        repos
            .namespaces()
            .update_retention_period(&namespace.name, Some(60 * 60 * 1_000_000_000)) // 1 hour
            .await
            .unwrap();
        let f4_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            max_time: Timestamp::new(
                // a bit over an hour ago
                (catalog.time_provider().now() - Duration::from_secs(60 * 65)).timestamp_nanos(),
            ),
            ..f3_params
        };
        let f4 = repos
            .parquet_files()
            .create(f4_params.clone())
            .await
            .unwrap();
        let f5_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            max_time: Timestamp::new(
                // a bit under an hour ago
                (catalog.time_provider().now() - Duration::from_secs(60 * 55)).timestamp_nanos(),
            ),
            ..f4_params
        };
        let f5 = repos
            .parquet_files()
            .create(f5_params.clone())
            .await
            .unwrap();
        let ids = repos
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .unwrap();
        assert!(ids.len() > 1); // it's also going to flag f1, f2 & f3 because they have low max
                                // timestamps but i don't want this test to be brittle if those
                                // values change so i'm not asserting len == 4
        let f4 = repos
            .parquet_files()
            .get_by_object_store_id(f4.object_store_id)
            .await
            .unwrap()
            .unwrap();
        assert_matches!(f4.to_delete, Some(_)); // f4 is > 1hr old
        let f5 = repos
            .parquet_files()
            .get_by_object_store_id(f5.object_store_id)
            .await
            .unwrap()
            .unwrap();
        assert_matches!(f5.to_delete, None); // f5 is < 1hr old

        // call flag_for_delete_by_retention() again and nothing should be flagged because they've
        // already been flagged
        let ids = repos
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .unwrap();
        assert!(ids.is_empty());

        // test that flag_for_delete_by_retention respects UPDATE LIMIT
        // create limit + the meaning of life parquet files that are all older than the retention (>1hr)
        const LIMIT: usize = 1000;
        const MOL: usize = 42;
        for _ in 0..LIMIT + MOL {
            let params = ParquetFileParams {
                object_store_id: Uuid::new_v4(),
                max_time: Timestamp::new(
                    // a bit over an hour ago
                    (catalog.time_provider().now() - Duration::from_secs(60 * 65))
                        .timestamp_nanos(),
                ),
                ..f1_params.clone()
            };
            repos.parquet_files().create(params.clone()).await.unwrap();
        }
        let ids = repos
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .unwrap();
        assert_eq!(ids.len(), LIMIT);
        let ids = repos
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .unwrap();
        assert_eq!(ids.len(), MOL); // second call took remainder
        let ids = repos
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .unwrap();
        assert_eq!(ids.len(), 0); // none left
    }

    async fn test_parquet_file_delete_broken(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace_1 = repos
            .namespaces()
            .create("retention_broken_1", None, None)
            .await
            .unwrap();
        let namespace_2 = repos
            .namespaces()
            .create("retention_broken_2", None, Some(1))
            .await
            .unwrap();
        let table_1 = repos
            .tables()
            .create("test_table", None, namespace_1.id)
            .await
            .unwrap();
        let table_2 = repos
            .tables()
            .create("test_table", None, namespace_2.id)
            .await
            .unwrap();
        let partition_1 = repos
            .partitions()
            .create_or_get("one".into(), table_1.id)
            .await
            .unwrap();
        let partition_2 = repos
            .partitions()
            .create_or_get("one".into(), table_2.id)
            .await
            .unwrap();

        let parquet_file_params_1 = ParquetFileParams {
            namespace_id: namespace_1.id,
            table_id: table_1.id,
            partition_id: partition_1.id,
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(10),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
            max_l0_created_at: Timestamp::new(1),
        };
        let parquet_file_params_2 = ParquetFileParams {
            namespace_id: namespace_2.id,
            table_id: table_2.id,
            partition_id: partition_2.id,
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(10),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
            max_l0_created_at: Timestamp::new(1),
        };
        let _parquet_file_1 = repos
            .parquet_files()
            .create(parquet_file_params_1.clone())
            .await
            .unwrap();
        let parquet_file_2 = repos
            .parquet_files()
            .create(parquet_file_params_2.clone())
            .await
            .unwrap();

        let ids = repos
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .unwrap();
        assert_eq!(ids, vec![parquet_file_2.id]);
    }

    async fn test_partitions_new_file_between(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace = repos
            .namespaces()
            .create("test_partitions_new_file_between", None, None)
            .await
            .unwrap();
        let table = repos
            .tables()
            .create("test_table_for_new_file_between", None, namespace.id)
            .await
            .unwrap();

        // param for the tests
        let time_now = Timestamp::from(catalog.time_provider().now());
        let time_one_hour_ago = Timestamp::from(catalog.time_provider().hours_ago(1));
        let time_two_hour_ago = Timestamp::from(catalog.time_provider().hours_ago(2));
        let time_three_hour_ago = Timestamp::from(catalog.time_provider().hours_ago(3));
        let time_five_hour_ago = Timestamp::from(catalog.time_provider().hours_ago(5));
        let time_six_hour_ago = Timestamp::from(catalog.time_provider().hours_ago(6));

        // Db has no partitions
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // -----------------
        // PARTITION one
        // The DB has 1 partition but it does not have any file
        let partition1 = repos
            .partitions()
            .create_or_get("one".into(), table.id)
            .await
            .unwrap();
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // create files for partition one
        let parquet_file_params = ParquetFileParams {
            namespace_id: namespace.id,
            table_id: partition1.table_id,
            partition_id: partition1.id,
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(10),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: time_three_hour_ago,
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
            max_l0_created_at: time_now,
        };

        // create a deleted L0 file that was created 3 hours ago
        let delete_l0_file = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();
        repos
            .parquet_files()
            .flag_for_delete(delete_l0_file.id)
            .await
            .unwrap();
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert!(partitions.is_empty());
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, Some(time_one_hour_ago))
            .await
            .unwrap();
        assert!(partitions.is_empty());
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_one_hour_ago))
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // create a deleted L0 file that was created 1 hour ago
        let l0_one_hour_ago_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            created_at: time_one_hour_ago,
            ..parquet_file_params.clone()
        };
        repos
            .parquet_files()
            .create(l0_one_hour_ago_file_params.clone())
            .await
            .unwrap();
        // partition one should be returned
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_two_hour_ago))
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // -----------------
        // PARTITION two
        // Partition two without any file
        let partition2 = repos
            .partitions()
            .create_or_get("two".into(), table.id)
            .await
            .unwrap();
        // should return partition one only
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);

        // Add a L0 file created 5 hours ago
        let l0_five_hour_ago_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            created_at: time_five_hour_ago,
            partition_id: partition2.id,
            ..parquet_file_params.clone()
        };
        repos
            .parquet_files()
            .create(l0_five_hour_ago_file_params.clone())
            .await
            .unwrap();
        // still return partition one only
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        // Between six and three hours ago, return only partition 2
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_six_hour_ago, Some(time_three_hour_ago))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition2.id);

        // Add an L1 file created just now
        let l1_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            created_at: time_now,
            partition_id: partition2.id,
            compaction_level: CompactionLevel::FileNonOverlapped,
            ..parquet_file_params.clone()
        };
        repos
            .parquet_files()
            .create(l1_file_params.clone())
            .await
            .unwrap();
        // should return both partitions
        let mut partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 2);
        partitions.sort();
        assert_eq!(partitions[0], partition1.id);
        assert_eq!(partitions[1], partition2.id);
        // Only return partition1: the creation time must be strictly less than the maximum time,
        // not equal
        let mut partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        partitions.sort();
        assert_eq!(partitions[0], partition1.id);
        // Between six and three hours ago, return none
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_six_hour_ago, Some(time_three_hour_ago))
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // -----------------
        // PARTITION three
        // Partition three without any file
        let partition3 = repos
            .partitions()
            .create_or_get("three".into(), table.id)
            .await
            .unwrap();
        // should return partition one and two only
        let mut partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 2);
        partitions.sort();
        assert_eq!(partitions[0], partition1.id);
        assert_eq!(partitions[1], partition2.id);
        // Only return partition1: the creation time must be strictly less than the maximum time,
        // not equal
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        // When the maximum time is greater than the creation time of partition2, return it
        let mut partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now + 1))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 2);
        partitions.sort();
        assert_eq!(partitions[0], partition1.id);
        assert_eq!(partitions[1], partition2.id);
        // Between six and three hours ago, return none
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_six_hour_ago, Some(time_three_hour_ago))
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // Add an L2 file created just now for partition three
        // Since the file is L2, the partition won't get updated
        let l2_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            created_at: time_now,
            partition_id: partition3.id,
            compaction_level: CompactionLevel::Final,
            ..parquet_file_params.clone()
        };
        repos
            .parquet_files()
            .create(l2_file_params.clone())
            .await
            .unwrap();
        // still should return partition one and two only
        let mut partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 2);
        partitions.sort();
        assert_eq!(partitions[0], partition1.id);
        assert_eq!(partitions[1], partition2.id);
        // Only return partition1: the creation time must be strictly less than the maximum time,
        // not equal
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        // Between six and three hours ago, return none
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_six_hour_ago, Some(time_three_hour_ago))
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // add an L0 file created one hour ago for partition three
        let l0_one_hour_ago_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            created_at: time_one_hour_ago,
            partition_id: partition3.id,
            ..parquet_file_params.clone()
        };
        repos
            .parquet_files()
            .create(l0_one_hour_ago_file_params.clone())
            .await
            .unwrap();
        // should return all partitions
        let mut partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 3);
        partitions.sort();
        assert_eq!(partitions[0], partition1.id);
        assert_eq!(partitions[1], partition2.id);
        assert_eq!(partitions[2], partition3.id);
        // Only return partitions 1 and 3; 2 was created just now
        let mut partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 2);
        partitions.sort();
        assert_eq!(partitions[0], partition1.id);
        assert_eq!(partitions[1], partition3.id);
        // Between six and three hours ago, return none
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_six_hour_ago, Some(time_three_hour_ago))
            .await
            .unwrap();
        assert!(partitions.is_empty());
    }

    async fn test_list_by_partiton_not_to_delete(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace = repos
            .namespaces()
            .create(
                "namespace_parquet_file_test_list_by_partiton_not_to_delete",
                None,
                None,
            )
            .await
            .unwrap();
        let table = repos
            .tables()
            .create("test_table", None, namespace.id)
            .await
            .unwrap();

        let partition = repos
            .partitions()
            .create_or_get("test_list_by_partiton_not_to_delete_one".into(), table.id)
            .await
            .unwrap();
        let partition2 = repos
            .partitions()
            .create_or_get("test_list_by_partiton_not_to_delete_two".into(), table.id)
            .await
            .unwrap();

        let min_time = Timestamp::new(1);
        let max_time = Timestamp::new(10);

        let parquet_file_params = ParquetFileParams {
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            min_time,
            max_time,
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
            max_l0_created_at: Timestamp::new(1),
        };

        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();
        let delete_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };
        let delete_file = repos
            .parquet_files()
            .create(delete_file_params)
            .await
            .unwrap();
        repos
            .parquet_files()
            .flag_for_delete(delete_file.id)
            .await
            .unwrap();
        let level1_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };
        let mut level1_file = repos
            .parquet_files()
            .create(level1_file_params)
            .await
            .unwrap();
        repos
            .parquet_files()
            .update_compaction_level(&[level1_file.id], CompactionLevel::FileNonOverlapped)
            .await
            .unwrap();
        level1_file.compaction_level = CompactionLevel::FileNonOverlapped;

        let other_partition_params = ParquetFileParams {
            partition_id: partition2.id,
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };
        let _partition2_file = repos
            .parquet_files()
            .create(other_partition_params)
            .await
            .unwrap();

        let files = repos
            .parquet_files()
            .list_by_partition_not_to_delete(partition.id)
            .await
            .unwrap();
        // not asserting against a vector literal to guard against flakiness due to uncertain
        // ordering of SQL query in postgres impl
        assert_eq!(files.len(), 2);
        assert_matches!(files.iter().find(|f| f.id == parquet_file.id), Some(_));
        assert_matches!(files.iter().find(|f| f.id == level1_file.id), Some(_));

        // remove namespace to avoid it from affecting later tests
        repos
            .namespaces()
            .soft_delete("namespace_parquet_file_test_list_by_partiton_not_to_delete")
            .await
            .expect("delete namespace should succeed");
    }

    async fn test_update_to_compaction_level_1(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace = repos
            .namespaces()
            .create("namespace_update_to_compaction_level_1_test", None, None)
            .await
            .unwrap();
        let table = repos
            .tables()
            .create("update_table", None, namespace.id)
            .await
            .unwrap();
        let partition = repos
            .partitions()
            .create_or_get("test_update_to_compaction_level_1_one".into(), table.id)
            .await
            .unwrap();

        // Set up the window of times we're interested in level 1 files for
        let query_min_time = Timestamp::new(5);
        let query_max_time = Timestamp::new(10);

        // Create a file with times entirely within the window
        let parquet_file_params = ParquetFileParams {
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            min_time: query_min_time + 1,
            max_time: query_max_time - 1,
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
            max_l0_created_at: Timestamp::new(1),
        };
        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();

        // Create a file that will remain as level 0
        let level_0_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };
        let level_0_file = repos.parquet_files().create(level_0_params).await.unwrap();

        // Create a ParquetFileId that doesn't actually exist in the catalog
        let nonexistent_parquet_file_id = ParquetFileId::new(level_0_file.id.get() + 1);

        // Make parquet_file compaction level 1, attempt to mark the nonexistent file; operation
        // should succeed
        let updated = repos
            .parquet_files()
            .update_compaction_level(
                &[parquet_file.id, nonexistent_parquet_file_id],
                CompactionLevel::FileNonOverlapped,
            )
            .await
            .unwrap();
        assert_eq!(updated, vec![parquet_file.id]);

        // remove namespace to avoid it from affecting later tests
        repos
            .namespaces()
            .soft_delete("namespace_update_to_compaction_level_1_test")
            .await
            .expect("delete namespace should succeed");
    }

    /// Assert that a namespace deletion does NOT cascade to the tables/schema
    /// items/parquet files/etc.
    ///
    /// Removal of this entities breaks the invariant that once created, a row
    /// always exists for the lifetime of an IOx process, and causes the system
    /// to panic in multiple components. It's also ineffective, because most
    /// components maintain a cache of at least one of these entities.
    ///
    /// Instead soft deleted namespaces should have their files GC'd like a
    /// normal parquet file deletion, removing the rows once they're no longer
    /// being actively used by the system. This is done by waiting a long time
    /// before deleting records, and whilst isn't perfect, it is largely
    /// effective.
    async fn test_delete_namespace(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace_1 = repos
            .namespaces()
            .create("namespace_test_delete_namespace_1", None, None)
            .await
            .unwrap();
        let table_1 = repos
            .tables()
            .create("test_table_1", None, namespace_1.id)
            .await
            .unwrap();
        let _c = repos
            .columns()
            .create_or_get("column_test_1", table_1.id, ColumnType::Tag)
            .await
            .unwrap();
        let partition_1 = repos
            .partitions()
            .create_or_get("test_delete_namespace_one".into(), table_1.id)
            .await
            .unwrap();

        // parquet files
        let parquet_file_params = ParquetFileParams {
            namespace_id: namespace_1.id,
            table_id: partition_1.table_id,
            partition_id: partition_1.id,
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(100),
            max_time: Timestamp::new(250),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
            max_l0_created_at: Timestamp::new(1),
        };
        let p1_n1 = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();
        let parquet_file_params_2 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(200),
            max_time: Timestamp::new(300),
            ..parquet_file_params
        };
        let p2_n1 = repos
            .parquet_files()
            .create(parquet_file_params_2.clone())
            .await
            .unwrap();

        // we've now created a namespace with a table and parquet files. before we test deleting
        // it, let's create another so we can ensure that doesn't get deleted.
        let namespace_2 = repos
            .namespaces()
            .create("namespace_test_delete_namespace_2", None, None)
            .await
            .unwrap();
        let table_2 = repos
            .tables()
            .create("test_table_2", None, namespace_2.id)
            .await
            .unwrap();
        let _c = repos
            .columns()
            .create_or_get("column_test_2", table_2.id, ColumnType::Tag)
            .await
            .unwrap();
        let partition_2 = repos
            .partitions()
            .create_or_get("test_delete_namespace_two".into(), table_2.id)
            .await
            .unwrap();

        // parquet files
        let parquet_file_params = ParquetFileParams {
            namespace_id: namespace_2.id,
            table_id: partition_2.table_id,
            partition_id: partition_2.id,
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(100),
            max_time: Timestamp::new(250),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
            max_l0_created_at: Timestamp::new(1),
        };
        let p1_n2 = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();
        let parquet_file_params_2 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(200),
            max_time: Timestamp::new(300),
            ..parquet_file_params
        };
        let p2_n2 = repos
            .parquet_files()
            .create(parquet_file_params_2.clone())
            .await
            .unwrap();

        // now delete namespace_1 and assert it's all gone and none of
        // namespace_2 is gone
        repos
            .namespaces()
            .soft_delete("namespace_test_delete_namespace_1")
            .await
            .expect("delete namespace should succeed");
        // assert that namespace is soft-deleted, but the table, column, and parquet files are all
        // still there.
        assert!(repos
            .namespaces()
            .get_by_id(namespace_1.id, SoftDeletedRows::ExcludeDeleted)
            .await
            .expect("get namespace should succeed")
            .is_none());
        assert_eq!(
            repos
                .namespaces()
                .get_by_id(namespace_1.id, SoftDeletedRows::AllRows)
                .await
                .expect("get namespace should succeed")
                .map(|mut v| {
                    // The only change after soft-deletion should be the deleted_at
                    // field being set - this block normalises that field, so that
                    // the before/after can be asserted as equal.
                    v.deleted_at = None;
                    v
                })
                .expect("should see soft-deleted row"),
            namespace_1
        );
        assert_eq!(
            repos
                .tables()
                .get_by_id(table_1.id)
                .await
                .expect("get table should succeed")
                .expect("should return row"),
            table_1
        );
        assert_eq!(
            repos
                .columns()
                .list_by_namespace_id(namespace_1.id)
                .await
                .expect("listing columns should succeed")
                .len(),
            1
        );
        assert_eq!(
            repos
                .columns()
                .list_by_table_id(table_1.id)
                .await
                .expect("listing columns should succeed")
                .len(),
            1
        );
        assert!(repos
            .partitions()
            .get_by_id(partition_1.id)
            .await
            .expect("fetching partition by id should succeed")
            .is_some());
        assert!(repos
            .parquet_files()
            .exist(p1_n1.id)
            .await
            .expect("parquet file exists check should succeed"));
        assert!(repos
            .parquet_files()
            .exist(p2_n1.id)
            .await
            .expect("parquet file exists check should succeed"));

        // assert that the namespace, table, column, and parquet files for namespace_2 are still
        // there
        assert!(repos
            .namespaces()
            .get_by_id(namespace_2.id, SoftDeletedRows::ExcludeDeleted)
            .await
            .expect("get namespace should succeed")
            .is_some());
        assert!(repos
            .tables()
            .get_by_id(table_2.id)
            .await
            .expect("get table should succeed")
            .is_some());
        assert_eq!(
            repos
                .columns()
                .list_by_namespace_id(namespace_2.id)
                .await
                .expect("listing columns should succeed")
                .len(),
            1
        );
        assert_eq!(
            repos
                .columns()
                .list_by_table_id(table_2.id)
                .await
                .expect("listing columns should succeed")
                .len(),
            1
        );
        assert!(repos
            .partitions()
            .get_by_id(partition_2.id)
            .await
            .expect("fetching partition by id should succeed")
            .is_some());
        assert!(repos
            .parquet_files()
            .exist(p1_n2.id)
            .await
            .expect("parquet file exists check should succeed"));
        assert!(repos
            .parquet_files()
            .exist(p2_n2.id)
            .await
            .expect("parquet file exists check should succeed"));
    }

    async fn test_txn_isolation(catalog: Arc<dyn Catalog>) {
        let barrier = Arc::new(tokio::sync::Barrier::new(2));

        let barrier_captured = Arc::clone(&barrier);
        let catalog_captured = Arc::clone(&catalog);
        let insertion_task = tokio::spawn(async move {
            barrier_captured.wait().await;

            let mut txn = catalog_captured.start_transaction().await.unwrap();
            txn.namespaces()
                .create("test_txn_isolation", None, None)
                .await
                .unwrap();

            tokio::time::sleep(Duration::from_millis(200)).await;
            txn.abort().await.unwrap();
        });

        let mut txn = catalog.start_transaction().await.unwrap();

        barrier.wait().await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let namespace = txn
            .namespaces()
            .get_by_name("test_txn_isolation", SoftDeletedRows::AllRows)
            .await
            .unwrap();
        assert!(namespace.is_none());
        txn.abort().await.unwrap();

        insertion_task.await.unwrap();

        let mut txn = catalog.start_transaction().await.unwrap();
        let namespace = txn
            .namespaces()
            .get_by_name("test_txn_isolation", SoftDeletedRows::AllRows)
            .await
            .unwrap();
        assert!(namespace.is_none());
        txn.abort().await.unwrap();
    }

    async fn test_txn_drop(catalog: Arc<dyn Catalog>) {
        let capture = TracingCapture::new();
        let mut txn = catalog.start_transaction().await.unwrap();
        txn.namespaces()
            .create("test_txn_drop", None, None)
            .await
            .unwrap();
        drop(txn);

        // got a warning
        assert_contains!(capture.to_string(), "Dropping ");
        assert_contains!(capture.to_string(), " w/o finalizing (commit or abort)");

        // data is NOT committed
        let mut txn = catalog.start_transaction().await.unwrap();
        let namespace = txn
            .namespaces()
            .get_by_name("test_txn_drop", SoftDeletedRows::AllRows)
            .await
            .unwrap();
        assert!(namespace.is_none());
        txn.abort().await.unwrap();
    }

    /// Upsert a namespace called `namespace_name` and write `lines` to it.
    async fn populate_namespace<R>(
        repos: &mut R,
        namespace_name: &str,
        lines: &str,
    ) -> (Namespace, NamespaceSchema)
    where
        R: RepoCollection + ?Sized,
    {
        let namespace = repos.namespaces().create(namespace_name, None, None).await;

        let namespace = match namespace {
            Ok(v) => v,
            Err(Error::NameExists { .. }) => repos
                .namespaces()
                .get_by_name(namespace_name, SoftDeletedRows::AllRows)
                .await
                .unwrap()
                .unwrap(),
            e @ Err(_) => e.unwrap(),
        };

        let batches = mutable_batch_lp::lines_to_batches(lines, 42).unwrap();
        let batches = batches.iter().map(|(table, batch)| (table.as_str(), batch));
        let ns = NamespaceSchema::new_empty_from(&namespace);

        let schema = validate_or_insert_schema(batches, &ns, repos)
            .await
            .expect("validate schema failed")
            .unwrap_or(ns);

        (namespace, schema)
    }

    async fn test_list_schemas(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;

        let ns1 = populate_namespace(
            repos.deref_mut(),
            "ns1",
            "cpu,tag=1 field=1i\nanother,tag=1 field=1.0",
        )
        .await;
        let ns2 = populate_namespace(
            repos.deref_mut(),
            "ns2",
            "cpu,tag=1 field=1i\nsomethingelse field=1u",
        )
        .await;

        // Otherwise the in-mem catalog deadlocks.... (but not postgres)
        drop(repos);

        let got = list_schemas(&*catalog)
            .await
            .expect("should be able to list the schemas")
            .collect::<Vec<_>>();

        assert!(got.contains(&ns1), "{:#?}\n\nwant{:#?}", got, &ns1);
        assert!(got.contains(&ns2), "{:#?}\n\nwant{:#?}", got, &ns2);
    }

    async fn test_list_schemas_soft_deleted_rows(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;

        let ns1 = populate_namespace(
            repos.deref_mut(),
            "ns1",
            "cpu,tag=1 field=1i\nanother,tag=1 field=1.0",
        )
        .await;
        let ns2 = populate_namespace(
            repos.deref_mut(),
            "ns2",
            "cpu,tag=1 field=1i\nsomethingelse field=1u",
        )
        .await;

        repos
            .namespaces()
            .soft_delete(&ns2.0.name)
            .await
            .expect("failed to soft delete namespace");

        // Otherwise the in-mem catalog deadlocks.... (but not postgres)
        drop(repos);

        let got = list_schemas(&*catalog)
            .await
            .expect("should be able to list the schemas")
            .collect::<Vec<_>>();

        assert!(got.contains(&ns1), "{:#?}\n\nwant{:#?}", got, &ns1);
        assert!(!got.contains(&ns2), "{:#?}\n\n do not want{:#?}", got, &ns2);
    }

    fn assert_metric_hit(metrics: &metric::Registry, name: &'static str) {
        let histogram = metrics
            .get_instrument::<Metric<DurationHistogram>>("catalog_op_duration")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("op", name), ("result", "success")]))
            .expect("failed to get observer")
            .fetch();

        let hit_count = histogram.sample_count();
        assert!(hit_count > 1, "metric did not record any calls");
    }
}
