//! A Postgres backed implementation of the Catalog

use crate::{
    interface::{
        self, sealed::TransactionFinalize, CasFailure, Catalog, ColumnRepo,
        ColumnTypeMismatchSnafu, Error, NamespaceRepo, ParquetFileRepo, PartitionRepo,
        QueryPoolRepo, RepoCollection, Result, SoftDeletedRows, TableRepo, TopicMetadataRepo,
        Transaction, MAX_PARQUET_FILES_SELECTED_ONCE,
    },
    kafkaless_transition::{TRANSITION_SHARD_ID, TRANSITION_SHARD_INDEX},
    metrics::MetricDecorator,
    DEFAULT_MAX_COLUMNS_PER_TABLE, DEFAULT_MAX_TABLES, SHARED_TOPIC_ID, SHARED_TOPIC_NAME,
};
use async_trait::async_trait;
use data_types::{
    Column, ColumnType, CompactionLevel, Namespace, NamespaceId, ParquetFile, ParquetFileId,
    ParquetFileParams, Partition, PartitionHashId, PartitionId, PartitionKey, QueryPool,
    QueryPoolId, SkippedCompaction, Table, TableId, Timestamp, TopicId, TopicMetadata,
};
use iox_time::{SystemProvider, TimeProvider};
use observability_deps::tracing::{debug, info, warn};
use snafu::prelude::*;
use sqlx::{
    migrate::Migrator,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::Uuid,
    Acquire, ConnectOptions, Executor, Postgres, Row,
};
use sqlx_hotswap_pool::HotSwapPool;
use std::{collections::HashMap, fmt::Display, str::FromStr, sync::Arc, time::Duration};

static MIGRATOR: Migrator = sqlx::migrate!();

/// Postgres connection options.
#[derive(Debug, Clone)]
pub struct PostgresConnectionOptions {
    /// Application name.
    ///
    /// This will be reported to postgres.
    pub app_name: String,

    /// Schema name.
    pub schema_name: String,

    /// DSN.
    pub dsn: String,

    /// Maximum number of concurrent connections.
    pub max_conns: u32,

    /// Set the amount of time to attempt connecting to the database.
    pub connect_timeout: Duration,

    /// Set a maximum idle duration for individual connections.
    pub idle_timeout: Duration,

    /// If the DSN points to a file (i.e. starts with `dsn-file://`), this sets the interval how often the the file
    /// should be polled for updates.
    ///
    /// If an update is encountered, the underlying connection pool will be hot-swapped.
    pub hotswap_poll_interval: Duration,
}

impl PostgresConnectionOptions {
    /// Default value for [`schema_name`](Self::schema_name).
    pub const DEFAULT_SCHEMA_NAME: &'static str = "iox_catalog";

    /// Default value for [`max_conns`](Self::max_conns).
    pub const DEFAULT_MAX_CONNS: u32 = 10;

    /// Default value for [`connect_timeout`](Self::connect_timeout).
    pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

    /// Default value for [`idle_timeout`](Self::idle_timeout).
    pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

    /// Default value for [`hotswap_poll_interval`](Self::hotswap_poll_interval).
    pub const DEFAULT_HOTSWAP_POLL_INTERVAL: Duration = Duration::from_secs(5);
}

impl Default for PostgresConnectionOptions {
    fn default() -> Self {
        Self {
            app_name: String::from("iox"),
            schema_name: String::from(Self::DEFAULT_SCHEMA_NAME),
            dsn: String::new(),
            max_conns: Self::DEFAULT_MAX_CONNS,
            connect_timeout: Self::DEFAULT_CONNECT_TIMEOUT,
            idle_timeout: Self::DEFAULT_IDLE_TIMEOUT,
            hotswap_poll_interval: Self::DEFAULT_HOTSWAP_POLL_INTERVAL,
        }
    }
}

/// PostgreSQL catalog.
#[derive(Debug)]
pub struct PostgresCatalog {
    metrics: Arc<metric::Registry>,
    pool: HotSwapPool<Postgres>,
    time_provider: Arc<dyn TimeProvider>,
    // Connection options for display
    options: PostgresConnectionOptions,
}

// struct to get return value from "select count(id) ..." query
#[derive(sqlx::FromRow)]
struct Count {
    count: i64,
}

impl PostgresCatalog {
    /// Connect to the catalog store.
    pub async fn connect(
        options: PostgresConnectionOptions,
        metrics: Arc<metric::Registry>,
    ) -> Result<Self> {
        let pool = new_pool(&options)
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        Ok(Self {
            pool,
            metrics,
            time_provider: Arc::new(SystemProvider::new()),
            options,
        })
    }

    fn schema_name(&self) -> &str {
        &self.options.schema_name
    }
}

impl Display for PostgresCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            // Do not include dsn in log as it may have credentials
            // that should not end up in the log
            "Postgres(dsn=OMITTED, schema_name='{}')",
            self.schema_name()
        )
    }
}

/// transaction for [`PostgresCatalog`].
#[derive(Debug)]
pub struct PostgresTxn {
    inner: PostgresTxnInner,
    time_provider: Arc<dyn TimeProvider>,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum PostgresTxnInner {
    Txn(Option<sqlx::Transaction<'static, Postgres>>),
    Oneshot(HotSwapPool<Postgres>),
}

impl<'c> Executor<'c> for &'c mut PostgresTxnInner {
    type Database = Postgres;

    #[allow(clippy::type_complexity)]
    fn fetch_many<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> futures::stream::BoxStream<
        'e,
        Result<
            sqlx::Either<
                <Self::Database as sqlx::Database>::QueryResult,
                <Self::Database as sqlx::Database>::Row,
            >,
            sqlx::Error,
        >,
    >
    where
        'c: 'e,
        E: sqlx::Execute<'q, Self::Database>,
    {
        match self {
            PostgresTxnInner::Txn(txn) => {
                txn.as_mut().expect("Not yet finalized").fetch_many(query)
            }
            PostgresTxnInner::Oneshot(pool) => pool.fetch_many(query),
        }
    }

    fn fetch_optional<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> futures::future::BoxFuture<
        'e,
        Result<Option<<Self::Database as sqlx::Database>::Row>, sqlx::Error>,
    >
    where
        'c: 'e,
        E: sqlx::Execute<'q, Self::Database>,
    {
        match self {
            PostgresTxnInner::Txn(txn) => txn
                .as_mut()
                .expect("Not yet finalized")
                .fetch_optional(query),
            PostgresTxnInner::Oneshot(pool) => pool.fetch_optional(query),
        }
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as sqlx::Database>::TypeInfo],
    ) -> futures::future::BoxFuture<
        'e,
        Result<<Self::Database as sqlx::database::HasStatement<'q>>::Statement, sqlx::Error>,
    >
    where
        'c: 'e,
    {
        match self {
            PostgresTxnInner::Txn(txn) => txn
                .as_mut()
                .expect("Not yet finalized")
                .prepare_with(sql, parameters),
            PostgresTxnInner::Oneshot(pool) => pool.prepare_with(sql, parameters),
        }
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures::future::BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
    {
        match self {
            PostgresTxnInner::Txn(txn) => txn.as_mut().expect("Not yet finalized").describe(sql),
            PostgresTxnInner::Oneshot(pool) => pool.describe(sql),
        }
    }
}

impl Drop for PostgresTxn {
    fn drop(&mut self) {
        if let PostgresTxnInner::Txn(Some(_)) = self.inner {
            warn!("Dropping PostgresTxn w/o finalizing (commit or abort)");

            // SQLx ensures that the inner transaction enqueues a rollback when it is dropped, so
            // we don't need to spawn a task here to call `rollback` manually.
        }
    }
}

#[async_trait]
impl TransactionFinalize for PostgresTxn {
    async fn commit_inplace(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            PostgresTxnInner::Txn(txn) => txn
                .take()
                .expect("Not yet finalized")
                .commit()
                .await
                .map_err(|e| Error::SqlxError { source: e }),
            PostgresTxnInner::Oneshot(_) => {
                panic!("cannot commit oneshot");
            }
        }
    }

    async fn abort_inplace(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            PostgresTxnInner::Txn(txn) => txn
                .take()
                .expect("Not yet finalized")
                .rollback()
                .await
                .map_err(|e| Error::SqlxError { source: e }),
            PostgresTxnInner::Oneshot(_) => {
                panic!("cannot abort oneshot");
            }
        }
    }
}

#[async_trait]
impl Catalog for PostgresCatalog {
    async fn setup(&self) -> Result<(), Error> {
        // We need to create the schema if we're going to set it as the first item of the
        // search_path otherwise when we run the sqlx migration scripts for the first time, sqlx
        // will create the `_sqlx_migrations` table in the public namespace (the only namespace
        // that exists), but the second time it will create it in the `<schema_name>` namespace and
        // re-run all the migrations without skipping the ones already applied (see #3893).
        //
        // This makes the migrations/20210217134322_create_schema.sql step unnecessary; we need to
        // keep that file because migration files are immutable.
        let create_schema_query = format!("CREATE SCHEMA IF NOT EXISTS {};", self.schema_name());
        self.pool
            .execute(sqlx::query(&create_schema_query))
            .await
            .map_err(|e| Error::Setup { source: e })?;

        MIGRATOR
            .run(&self.pool)
            .await
            .map_err(|e| Error::Setup { source: e.into() })?;

        // We need to manually insert the topic here so that we can create the transition shard below.
        sqlx::query(
            r#"
INSERT INTO topic (name)
VALUES ($1)
ON CONFLICT ON CONSTRAINT topic_name_unique
DO NOTHING;
        "#,
        )
        .bind(SHARED_TOPIC_NAME)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Setup { source: e })?;

        // The transition shard must exist and must have magic ID and INDEX.
        sqlx::query(
            r#"
INSERT INTO shard (id, topic_id, shard_index, min_unpersisted_sequence_number)
OVERRIDING SYSTEM VALUE
VALUES ($1, $2, $3, 0)
ON CONFLICT ON CONSTRAINT shard_unique
DO NOTHING;
        "#,
        )
        .bind(TRANSITION_SHARD_ID)
        .bind(SHARED_TOPIC_ID)
        .bind(TRANSITION_SHARD_INDEX)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Setup { source: e })?;

        Ok(())
    }

    async fn start_transaction(&self) -> Result<Box<dyn Transaction>, Error> {
        let transaction = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        Ok(Box::new(MetricDecorator::new(
            PostgresTxn {
                inner: PostgresTxnInner::Txn(Some(transaction)),
                time_provider: Arc::clone(&self.time_provider),
            },
            Arc::clone(&self.metrics),
        )))
    }

    async fn repositories(&self) -> Box<dyn RepoCollection> {
        Box::new(MetricDecorator::new(
            PostgresTxn {
                inner: PostgresTxnInner::Oneshot(self.pool.clone()),
                time_provider: Arc::clone(&self.time_provider),
            },
            Arc::clone(&self.metrics),
        ))
    }

    fn metrics(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metrics)
    }

    fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }
}

/// Creates a new [`sqlx::Pool`] from a database config and an explicit DSN.
///
/// This function doesn't support the IDPE specific `dsn-file://` uri scheme.
async fn new_raw_pool(
    options: &PostgresConnectionOptions,
    parsed_dsn: &str,
) -> Result<sqlx::Pool<Postgres>, sqlx::Error> {
    // sqlx exposes some options as pool options, while other options are available as connection options.
    let mut connect_options = PgConnectOptions::from_str(parsed_dsn)?;
    // the default is INFO, which is frankly surprising.
    connect_options.log_statements(log::LevelFilter::Trace);

    let app_name = options.app_name.clone();
    let app_name2 = options.app_name.clone(); // just to log below
    let schema_name = options.schema_name.clone();
    let pool = PgPoolOptions::new()
        .min_connections(1)
        .max_connections(options.max_conns)
        .acquire_timeout(options.connect_timeout)
        .idle_timeout(options.idle_timeout)
        .test_before_acquire(true)
        .after_connect(move |c, _meta| {
            let app_name = app_name.to_owned();
            let schema_name = schema_name.to_owned();
            Box::pin(async move {
                // Tag the connection with the provided application name, while allowing it to
                // be override from the connection string (aka DSN).
                // If current_application_name is empty here it means the application name wasn't
                // set as part of the DSN, and we can set it explicitly.
                // Recall that this block is running on connection, not when creating the pool!
                let current_application_name: String =
                    sqlx::query_scalar("SELECT current_setting('application_name');")
                        .fetch_one(&mut *c)
                        .await?;
                if current_application_name.is_empty() {
                    sqlx::query("SELECT set_config('application_name', $1, false);")
                        .bind(&*app_name)
                        .execute(&mut *c)
                        .await?;
                }
                let search_path_query = format!("SET search_path TO {schema_name},public;");
                c.execute(sqlx::query(&search_path_query)).await?;

                // Ensure explicit timezone selection, instead of deferring to
                // the server value.
                c.execute("SET timezone = 'UTC';").await?;
                Ok(())
            })
        })
        .connect_with(connect_options)
        .await?;

    // Log a connection was successfully established and include the application
    // name for cross-correlation between Conductor logs & database connections.
    info!(application_name=%app_name2, "connected to config store");

    Ok(pool)
}

/// Creates a new HotSwapPool
///
/// This function understands the IDPE specific `dsn-file://` dsn uri scheme
/// and hot swaps the pool with a new sqlx::Pool when the file changes.
/// This is useful because the credentials can be rotated by infrastructure
/// agents while the service is running.
///
/// The file is polled for changes every `polling_interval`.
///
/// The pool is replaced only once the new pool is successfully created.
/// The [`new_raw_pool`] function will return a new pool only if the connection
/// is successfull (see [`sqlx::pool::PoolOptions::test_before_acquire`]).
async fn new_pool(
    options: &PostgresConnectionOptions,
) -> Result<HotSwapPool<Postgres>, sqlx::Error> {
    let parsed_dsn = match get_dsn_file_path(&options.dsn) {
        Some(filename) => std::fs::read_to_string(filename)?,
        None => options.dsn.clone(),
    };
    let pool = HotSwapPool::new(new_raw_pool(options, &parsed_dsn).await?);
    let polling_interval = options.hotswap_poll_interval;

    if let Some(dsn_file) = get_dsn_file_path(&options.dsn) {
        let pool = pool.clone();
        let options = options.clone();

        // TODO(mkm): return a guard that stops this background worker.
        // We create only one pool per process, but it would be cleaner to be
        // able to properly destroy the pool. If we don't kill this worker we
        // effectively keep the pool alive (since it holds a reference to the
        // Pool) and we also potentially pollute the logs with spurious warnings
        // if the dsn file disappears (this may be annoying if they show up in the test
        // logs).
        tokio::spawn(async move {
            let mut current_dsn = parsed_dsn.clone();
            loop {
                tokio::time::sleep(polling_interval).await;

                async fn try_update(
                    options: &PostgresConnectionOptions,
                    current_dsn: &str,
                    dsn_file: &str,
                    pool: &HotSwapPool<Postgres>,
                ) -> Result<Option<String>, sqlx::Error> {
                    let new_dsn = std::fs::read_to_string(dsn_file)?;
                    if new_dsn == current_dsn {
                        Ok(None)
                    } else {
                        let new_pool = new_raw_pool(options, &new_dsn).await?;
                        let old_pool = pool.replace(new_pool);
                        info!("replaced hotswap pool");
                        info!(?old_pool, "closing old DB connection pool");
                        // The pool is not closed on drop. We need to call `close`.
                        // It will close all idle connections, and wait until acquired connections
                        // are returned to the pool or closed.
                        old_pool.close().await;
                        info!(?old_pool, "closed old DB connection pool");
                        Ok(Some(new_dsn))
                    }
                }

                match try_update(&options, &current_dsn, &dsn_file, &pool).await {
                    Ok(None) => {}
                    Ok(Some(new_dsn)) => {
                        current_dsn = new_dsn;
                    }
                    Err(e) => {
                        warn!(
                            error=%e,
                            filename=%dsn_file,
                            "not replacing hotswap pool because of an error \
                            connecting to the new DSN"
                        );
                    }
                }
            }
        });
    }

    Ok(pool)
}

// Parses a `dsn-file://` scheme, according to the rules of the IDPE kit/sql package.
//
// If the dsn matches the `dsn-file://` prefix, the prefix is removed and the rest is interpreted
// as a file name, in which case this function will return `Some(filename)`.
// Otherwise it will return None. No URI decoding is performed on the filename.
fn get_dsn_file_path(dsn: &str) -> Option<String> {
    const DSN_SCHEME: &str = "dsn-file://";
    dsn.starts_with(DSN_SCHEME)
        .then(|| dsn[DSN_SCHEME.len()..].to_owned())
}

#[async_trait]
impl RepoCollection for PostgresTxn {
    fn topics(&mut self) -> &mut dyn TopicMetadataRepo {
        self
    }

    fn query_pools(&mut self) -> &mut dyn QueryPoolRepo {
        self
    }

    fn namespaces(&mut self) -> &mut dyn NamespaceRepo {
        self
    }

    fn tables(&mut self) -> &mut dyn TableRepo {
        self
    }

    fn columns(&mut self) -> &mut dyn ColumnRepo {
        self
    }

    fn partitions(&mut self) -> &mut dyn PartitionRepo {
        self
    }

    fn parquet_files(&mut self) -> &mut dyn ParquetFileRepo {
        self
    }
}

#[async_trait]
impl TopicMetadataRepo for PostgresTxn {
    async fn create_or_get(&mut self, name: &str) -> Result<TopicMetadata> {
        let rec = sqlx::query_as::<_, TopicMetadata>(
            r#"
INSERT INTO topic ( name )
VALUES ( $1 )
ON CONFLICT ON CONSTRAINT topic_name_unique
DO UPDATE SET name = topic.name
RETURNING *;
        "#,
        )
        .bind(name) // $1
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn get_by_name(&mut self, name: &str) -> Result<Option<TopicMetadata>> {
        let rec = sqlx::query_as::<_, TopicMetadata>(
            r#"
SELECT *
FROM topic
WHERE name = $1;
        "#,
        )
        .bind(name) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let topic = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(topic))
    }
}

#[async_trait]
impl QueryPoolRepo for PostgresTxn {
    async fn create_or_get(&mut self, name: &str) -> Result<QueryPool> {
        let rec = sqlx::query_as::<_, QueryPool>(
            r#"
INSERT INTO query_pool ( name )
VALUES ( $1 )
ON CONFLICT ON CONSTRAINT query_pool_name_unique
DO UPDATE SET name = query_pool.name
RETURNING *;
        "#,
        )
        .bind(name) // $1
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }
}

#[async_trait]
impl NamespaceRepo for PostgresTxn {
    async fn create(
        &mut self,
        name: &str,
        retention_period_ns: Option<i64>,
        topic_id: TopicId,
        query_pool_id: QueryPoolId,
    ) -> Result<Namespace> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"
                INSERT INTO namespace ( name, topic_id, query_pool_id, retention_period_ns, max_tables )
                VALUES ( $1, $2, $3, $4, $5 )
                RETURNING *;
            "#,
        )
        .bind(name) // $1
        .bind(topic_id) // $2
        .bind(query_pool_id) // $3
        .bind(retention_period_ns) // $4
        .bind(DEFAULT_MAX_TABLES); // $5

        let rec = rec.fetch_one(&mut self.inner).await.map_err(|e| {
            if is_unique_violation(&e) {
                Error::NameExists {
                    name: name.to_string(),
                }
            } else if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        // Ensure the column default values match the code values.
        debug_assert_eq!(rec.max_tables, DEFAULT_MAX_TABLES);
        debug_assert_eq!(rec.max_columns_per_table, DEFAULT_MAX_COLUMNS_PER_TABLE);

        Ok(rec)
    }

    async fn list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>> {
        let rec = sqlx::query_as::<_, Namespace>(
            format!(
                r#"SELECT * FROM namespace WHERE {v};"#,
                v = deleted.as_sql_predicate()
            )
            .as_str(),
        )
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn get_by_id(
        &mut self,
        id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        let rec = sqlx::query_as::<_, Namespace>(
            format!(
                r#"SELECT * FROM namespace WHERE id=$1 AND {v};"#,
                v = deleted.as_sql_predicate()
            )
            .as_str(),
        )
        .bind(id) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let namespace = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(namespace))
    }

    async fn get_by_name(
        &mut self,
        name: &str,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        let rec = sqlx::query_as::<_, Namespace>(
            format!(
                r#"SELECT * FROM namespace WHERE name=$1 AND {v};"#,
                v = deleted.as_sql_predicate()
            )
            .as_str(),
        )
        .bind(name) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let namespace = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(namespace))
    }

    async fn soft_delete(&mut self, name: &str) -> Result<()> {
        let flagged_at = Timestamp::from(self.time_provider.now());

        // note that there is a uniqueness constraint on the name column in the DB
        sqlx::query(r#"UPDATE namespace SET deleted_at=$1 WHERE name = $2;"#)
            .bind(flagged_at) // $1
            .bind(name) // $2
            .execute(&mut self.inner)
            .await
            .context(interface::CouldNotDeleteNamespaceSnafu)
            .map(|_| ())
    }

    async fn update_table_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"
UPDATE namespace
SET max_tables = $1
WHERE name = $2
RETURNING *;
        "#,
        )
        .bind(new_max)
        .bind(name)
        .fetch_one(&mut self.inner)
        .await;

        let namespace = rec.map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::NamespaceNotFoundByName {
                name: name.to_string(),
            },
            _ => Error::SqlxError { source: e },
        })?;

        Ok(namespace)
    }

    async fn update_column_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"
UPDATE namespace
SET max_columns_per_table = $1
WHERE name = $2
RETURNING *;
        "#,
        )
        .bind(new_max)
        .bind(name)
        .fetch_one(&mut self.inner)
        .await;

        let namespace = rec.map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::NamespaceNotFoundByName {
                name: name.to_string(),
            },
            _ => Error::SqlxError { source: e },
        })?;

        Ok(namespace)
    }

    async fn update_retention_period(
        &mut self,
        name: &str,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"UPDATE namespace SET retention_period_ns = $1 WHERE name = $2 RETURNING *;"#,
        )
        .bind(retention_period_ns) // $1
        .bind(name) // $2
        .fetch_one(&mut self.inner)
        .await;

        let namespace = rec.map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::NamespaceNotFoundByName {
                name: name.to_string(),
            },
            _ => Error::SqlxError { source: e },
        })?;

        Ok(namespace)
    }
}

#[async_trait]
impl TableRepo for PostgresTxn {
    async fn create_or_get(&mut self, name: &str, namespace_id: NamespaceId) -> Result<Table> {
        // A simple insert statement becomes quite complicated in order to avoid checking the table
        // limits in a select and then conditionally inserting (which would be racey).
        //
        // from https://www.postgresql.org/docs/current/sql-insert.html
        //   "INSERT inserts new rows into a table. One can insert one or more rows specified by
        //   value expressions, or zero or more rows resulting from a query."
        // By using SELECT rather than VALUES it will insert zero rows if it finds a null in the
        // subquery, i.e. if count >= max_tables. fetch_one() will return a RowNotFound error if
        // nothing was inserted. Not pretty!
        let rec = sqlx::query_as::<_, Table>(
            r#"
INSERT INTO table_name ( name, namespace_id )
SELECT $1, id FROM (
    SELECT namespace.id AS id, max_tables, COUNT(table_name.*) AS count
    FROM namespace LEFT JOIN table_name ON namespace.id = table_name.namespace_id
    WHERE namespace.id = $2
    GROUP BY namespace.max_tables, table_name.namespace_id, namespace.id
) AS get_count WHERE count < max_tables
ON CONFLICT ON CONSTRAINT table_name_unique
DO UPDATE SET name = table_name.name
RETURNING *;
        "#,
        )
        .bind(name) // $1
        .bind(namespace_id) // $2
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::TableCreateLimitError {
                table_name: name.to_string(),
                namespace_id,
            },
            _ => {
                if is_fk_violation(&e) {
                    Error::ForeignKeyViolation { source: e }
                } else {
                    Error::SqlxError { source: e }
                }
            }
        })?;

        Ok(rec)
    }

    async fn get_by_id(&mut self, table_id: TableId) -> Result<Option<Table>> {
        let rec = sqlx::query_as::<_, Table>(
            r#"
SELECT *
FROM table_name
WHERE id = $1;
            "#,
        )
        .bind(table_id) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let table = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(table))
    }

    async fn get_by_namespace_and_name(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<Option<Table>> {
        let rec = sqlx::query_as::<_, Table>(
            r#"
SELECT *
FROM table_name
WHERE namespace_id = $1 AND name = $2;
            "#,
        )
        .bind(namespace_id) // $1
        .bind(name) // $2
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let table = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(table))
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>> {
        let rec = sqlx::query_as::<_, Table>(
            r#"
SELECT *
FROM table_name
WHERE namespace_id = $1;
            "#,
        )
        .bind(namespace_id)
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn list(&mut self) -> Result<Vec<Table>> {
        let rec = sqlx::query_as::<_, Table>("SELECT * FROM table_name;")
            .fetch_all(&mut self.inner)
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }
}

#[async_trait]
impl ColumnRepo for PostgresTxn {
    async fn create_or_get(
        &mut self,
        name: &str,
        table_id: TableId,
        column_type: ColumnType,
    ) -> Result<Column> {
        let rec = sqlx::query_as::<_, Column>(
            r#"
INSERT INTO column_name ( name, table_id, column_type )
SELECT $1, table_id, $3 FROM (
    SELECT max_columns_per_table, namespace.id, table_name.id as table_id, COUNT(column_name.*) AS count
    FROM namespace LEFT JOIN table_name ON namespace.id = table_name.namespace_id
                   LEFT JOIN column_name ON table_name.id = column_name.table_id
    WHERE table_name.id = $2
    GROUP BY namespace.max_columns_per_table, namespace.id, table_name.id
) AS get_count WHERE count < max_columns_per_table
ON CONFLICT ON CONSTRAINT column_name_unique
DO UPDATE SET name = column_name.name
RETURNING *;
        "#,
        )
        .bind(name) // $1
        .bind(table_id) // $2
        .bind(column_type) // $3
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::ColumnCreateLimitError {
                column_name: name.to_string(),
                table_id,
            },
            _ => {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        }})?;

        ensure!(
            rec.column_type == column_type,
            ColumnTypeMismatchSnafu {
                name,
                existing: rec.column_type,
                new: column_type,
            }
        );

        Ok(rec)
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>> {
        let rec = sqlx::query_as::<_, Column>(
            r#"
SELECT column_name.* FROM table_name
INNER JOIN column_name on column_name.table_id = table_name.id
WHERE table_name.namespace_id = $1;
            "#,
        )
        .bind(namespace_id)
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Column>> {
        let rec = sqlx::query_as::<_, Column>(
            r#"
SELECT * FROM column_name
WHERE table_id = $1;
            "#,
        )
        .bind(table_id)
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn list(&mut self) -> Result<Vec<Column>> {
        let rec = sqlx::query_as::<_, Column>("SELECT * FROM column_name;")
            .fetch_all(&mut self.inner)
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn create_or_get_many_unchecked(
        &mut self,
        table_id: TableId,
        columns: HashMap<&str, ColumnType>,
    ) -> Result<Vec<Column>> {
        let num_columns = columns.len();
        let (v_name, v_column_type): (Vec<&str>, Vec<i16>) = columns
            .iter()
            .map(|(&name, &column_type)| (name, column_type as i16))
            .unzip();

        // The `ORDER BY` in this statement is important to avoid deadlocks during concurrent
        // writes to the same IOx table that each add many new columns. See:
        //
        // - <https://rcoh.svbtle.com/postgres-unique-constraints-can-cause-deadlock>
        // - <https://dba.stackexchange.com/a/195220/27897>
        // - <https://github.com/influxdata/idpe/issues/16298>
        let out = sqlx::query_as::<_, Column>(
            r#"
INSERT INTO column_name ( name, table_id, column_type )
SELECT name, $1, column_type
FROM UNNEST($2, $3) as a(name, column_type)
ORDER BY name
ON CONFLICT ON CONSTRAINT column_name_unique
DO UPDATE SET name = column_name.name
RETURNING *;
            "#,
        )
        .bind(table_id) // $1
        .bind(&v_name) // $2
        .bind(&v_column_type) // $3
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        assert_eq!(num_columns, out.len());

        for existing in &out {
            let want = columns.get(existing.name.as_str()).unwrap();
            ensure!(
                existing.column_type == *want,
                ColumnTypeMismatchSnafu {
                    name: &existing.name,
                    existing: existing.column_type,
                    new: *want,
                }
            );
        }

        Ok(out)
    }
}

#[async_trait]
impl PartitionRepo for PostgresTxn {
    async fn create_or_get(&mut self, key: PartitionKey, table_id: TableId) -> Result<Partition> {
        // Note: since sort_key is now an array, we must explicitly insert '{}' which is an empty
        // array rather than NULL which sqlx will throw `UnexpectedNullError` while is is doing
        // `ColumnDecode`

        let hash_id = PartitionHashId::new(table_id, &key);

        let v = sqlx::query_as::<_, Partition>(
            r#"
INSERT INTO partition
    (partition_key, shard_id, table_id, hash_id, sort_key)
VALUES
    ( $1, $2, $3, $4, '{}')
ON CONFLICT ON CONSTRAINT partition_key_unique
DO UPDATE SET partition_key = partition.partition_key
RETURNING id, hash_id, table_id, partition_key, sort_key, persisted_sequence_number, new_file_at;
        "#,
        )
        .bind(key) // $1
        .bind(TRANSITION_SHARD_ID) // $2
        .bind(table_id) // $3
        .bind(&hash_id) // $4
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        Ok(v)
    }

    async fn get_by_id(&mut self, partition_id: PartitionId) -> Result<Option<Partition>> {
        let rec = sqlx::query_as::<_, Partition>(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key, persisted_sequence_number, new_file_at
FROM partition
WHERE id = $1;
        "#,
        )
        .bind(partition_id) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let partition = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(partition))
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>> {
        sqlx::query_as::<_, Partition>(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key, persisted_sequence_number, new_file_at
FROM partition
WHERE table_id = $1;
            "#,
        )
        .bind(table_id) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn list_ids(&mut self) -> Result<Vec<PartitionId>> {
        sqlx::query_as(
            r#"
            SELECT p.id as partition_id
            FROM partition p
            "#,
        )
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    /// Update the sort key for `partition_id` if and only if `old_sort_key`
    /// matches the current value in the database.
    ///
    /// This compare-and-swap operation is allowed to spuriously return
    /// [`CasFailure::ValueMismatch`] for performance reasons (avoiding multiple
    /// round trips to service a transaction in the happy path).
    async fn cas_sort_key(
        &mut self,
        partition_id: PartitionId,
        old_sort_key: Option<Vec<String>>,
        new_sort_key: &[&str],
    ) -> Result<Partition, CasFailure<Vec<String>>> {
        let old_sort_key = old_sort_key.unwrap_or_default();
        let res = sqlx::query_as::<_, Partition>(
            r#"
UPDATE partition
SET sort_key = $1
WHERE id = $2 AND sort_key = $3
RETURNING id, hash_id, table_id, partition_key, sort_key, persisted_sequence_number, new_file_at;
        "#,
        )
        .bind(new_sort_key) // $1
        .bind(partition_id) // $2
        .bind(&old_sort_key) // $3
        .fetch_one(&mut self.inner)
        .await;

        let partition = match res {
            Ok(v) => v,
            Err(sqlx::Error::RowNotFound) => {
                // This update may have failed either because:
                //
                // * A row with the specified ID did not exist at query time
                //   (but may exist now!)
                // * The sort key does not match.
                //
                // To differentiate, we submit a get partition query, returning
                // the actual sort key if successful.
                //
                // NOTE: this is racy, but documented - this might return "Sort
                // key differs! Old key: <old sort key you provided>"
                return Err(CasFailure::ValueMismatch(
                    PartitionRepo::get_by_id(self, partition_id)
                        .await
                        .map_err(CasFailure::QueryError)?
                        .ok_or(CasFailure::QueryError(Error::PartitionNotFound {
                            id: partition_id,
                        }))?
                        .sort_key,
                ));
            }
            Err(e) => return Err(CasFailure::QueryError(Error::SqlxError { source: e })),
        };

        debug!(
            ?partition_id,
            ?old_sort_key,
            ?new_sort_key,
            "partition sort key cas successful"
        );

        Ok(partition)
    }

    async fn record_skipped_compaction(
        &mut self,
        partition_id: PartitionId,
        reason: &str,
        num_files: usize,
        limit_num_files: usize,
        limit_num_files_first_in_partition: usize,
        estimated_bytes: u64,
        limit_bytes: u64,
    ) -> Result<()> {
        sqlx::query(
            r#"
INSERT INTO skipped_compactions
    ( partition_id, reason, num_files, limit_num_files, limit_num_files_first_in_partition, estimated_bytes, limit_bytes, skipped_at )
VALUES
    ( $1, $2, $3, $4, $5, $6, $7, extract(epoch from NOW()) )
ON CONFLICT ( partition_id )
DO UPDATE
SET
reason = EXCLUDED.reason,
num_files = EXCLUDED.num_files,
limit_num_files = EXCLUDED.limit_num_files,
limit_num_files_first_in_partition = EXCLUDED.limit_num_files_first_in_partition,
estimated_bytes = EXCLUDED.estimated_bytes,
limit_bytes = EXCLUDED.limit_bytes,
skipped_at = EXCLUDED.skipped_at;
        "#,
        )
        .bind(partition_id) // $1
        .bind(reason)
        .bind(num_files as i64)
        .bind(limit_num_files as i64)
        .bind(limit_num_files_first_in_partition as i64)
        .bind(estimated_bytes as i64)
        .bind(limit_bytes as i64)
        .execute(&mut self.inner)
        .await
        .context(interface::CouldNotRecordSkippedCompactionSnafu { partition_id })?;
        Ok(())
    }

    async fn get_in_skipped_compaction(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>> {
        let rec = sqlx::query_as::<_, SkippedCompaction>(
            r#"SELECT * FROM skipped_compactions WHERE partition_id = $1;"#,
        )
        .bind(partition_id) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let skipped_partition_record = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(skipped_partition_record))
    }

    async fn list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>> {
        sqlx::query_as::<_, SkippedCompaction>(
            r#"
SELECT * FROM skipped_compactions
        "#,
        )
        .fetch_all(&mut self.inner)
        .await
        .context(interface::CouldNotListSkippedCompactionsSnafu)
    }

    async fn delete_skipped_compactions(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>> {
        sqlx::query_as::<_, SkippedCompaction>(
            r#"
DELETE FROM skipped_compactions
WHERE partition_id = $1
RETURNING *
        "#,
        )
        .bind(partition_id)
        .fetch_optional(&mut self.inner)
        .await
        .context(interface::CouldNotDeleteSkippedCompactionsSnafu)
    }

    async fn most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>> {
        sqlx::query_as(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key, persisted_sequence_number, new_file_at
FROM partition
ORDER BY id DESC
LIMIT $1;"#,
        )
        .bind(n as i64) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn partitions_new_file_between(
        &mut self,
        minimum_time: Timestamp,
        maximum_time: Option<Timestamp>,
    ) -> Result<Vec<PartitionId>> {
        let sql = format!(
            r#"
            SELECT p.id as partition_id
            FROM partition p
            WHERE p.new_file_at > $1
            {}
            "#,
            maximum_time
                .map(|_| "AND p.new_file_at < $2")
                .unwrap_or_default()
        );

        sqlx::query_as(&sql)
            .bind(minimum_time) // $1
            .bind(maximum_time) // $2
            .fetch_all(&mut self.inner)
            .await
            .map_err(|e| Error::SqlxError { source: e })
    }
}

#[async_trait]
impl ParquetFileRepo for PostgresTxn {
    async fn create(&mut self, parquet_file_params: ParquetFileParams) -> Result<ParquetFile> {
        let ParquetFileParams {
            namespace_id,
            table_id,
            partition_id,
            object_store_id,
            max_sequence_number,
            min_time,
            max_time,
            file_size_bytes,
            row_count,
            compaction_level,
            created_at,
            column_set,
            max_l0_created_at,
        } = parquet_file_params;

        let rec = sqlx::query_as::<_, ParquetFile>(
            r#"
INSERT INTO parquet_file (
    shard_id, table_id, partition_id, object_store_id,
    max_sequence_number, min_time, max_time, file_size_bytes,
    row_count, compaction_level, created_at, namespace_id, column_set, max_l0_created_at )
VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14 )
RETURNING
    id, table_id, partition_id, object_store_id,
    max_sequence_number, min_time, max_time, to_delete, file_size_bytes,
    row_count, compaction_level, created_at, namespace_id, column_set, max_l0_created_at;
        "#,
        )
        .bind(TRANSITION_SHARD_ID) // $1
        .bind(table_id) // $2
        .bind(partition_id) // $3
        .bind(object_store_id) // $4
        .bind(max_sequence_number) // $5
        .bind(min_time) // $6
        .bind(max_time) // $7
        .bind(file_size_bytes) // $8
        .bind(row_count) // $9
        .bind(compaction_level) // $10
        .bind(created_at) // $11
        .bind(namespace_id) // $12
        .bind(column_set) // $13
        .bind(max_l0_created_at) // $14
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| {
            if is_unique_violation(&e) {
                Error::FileExists { object_store_id }
            } else if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        Ok(rec)
    }

    async fn flag_for_delete(&mut self, id: ParquetFileId) -> Result<()> {
        let marked_at = Timestamp::from(self.time_provider.now());

        let _ = sqlx::query(r#"UPDATE parquet_file SET to_delete = $1 WHERE id = $2;"#)
            .bind(marked_at) // $1
            .bind(id) // $2
            .execute(&mut self.inner)
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        Ok(())
    }

    async fn flag_for_delete_by_retention(&mut self) -> Result<Vec<ParquetFileId>> {
        let flagged_at = Timestamp::from(self.time_provider.now());
        // TODO - include check of table retention period once implemented
        let flagged = sqlx::query(
            r#"
WITH parquet_file_ids as (
    SELECT parquet_file.id
    FROM namespace, parquet_file
    WHERE namespace.retention_period_ns IS NOT NULL
    AND parquet_file.to_delete IS NULL
    AND parquet_file.max_time < $1 - namespace.retention_period_ns
    AND namespace.id = parquet_file.namespace_id
    LIMIT $2
)
UPDATE parquet_file
SET to_delete = $1
WHERE id IN (SELECT id FROM parquet_file_ids)
RETURNING id;
            "#,
        )
        .bind(flagged_at) // $1
        .bind(MAX_PARQUET_FILES_SELECTED_ONCE) // $2
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        let flagged = flagged.into_iter().map(|row| row.get("id")).collect();
        Ok(flagged)
    }

    async fn list_by_namespace_not_to_delete(
        &mut self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<ParquetFile>> {
        sqlx::query_as::<_, ParquetFile>(
            r#"
SELECT parquet_file.id, parquet_file.namespace_id,
       parquet_file.table_id, parquet_file.partition_id, parquet_file.object_store_id,
       parquet_file.max_sequence_number, parquet_file.min_time,
       parquet_file.max_time, parquet_file.to_delete, parquet_file.file_size_bytes,
       parquet_file.row_count, parquet_file.compaction_level, parquet_file.created_at,
       parquet_file.column_set, parquet_file.max_l0_created_at
FROM parquet_file
INNER JOIN table_name on table_name.id = parquet_file.table_id
WHERE table_name.namespace_id = $1
  AND parquet_file.to_delete IS NULL;
             "#,
        )
        .bind(namespace_id) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn list_by_table_not_to_delete(&mut self, table_id: TableId) -> Result<Vec<ParquetFile>> {
        sqlx::query_as::<_, ParquetFile>(
            r#"
SELECT id, namespace_id, table_id, partition_id, object_store_id,
       max_sequence_number, min_time, max_time, to_delete, file_size_bytes,
       row_count, compaction_level, created_at, column_set, max_l0_created_at
FROM parquet_file
WHERE table_id = $1 AND to_delete IS NULL;
             "#,
        )
        .bind(table_id) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn list_by_table(&mut self, table_id: TableId) -> Result<Vec<ParquetFile>> {
        // Deliberately doesn't use `SELECT *` to avoid the performance hit of fetching the large
        // `parquet_metadata` column!!
        sqlx::query_as::<_, ParquetFile>(
            r#"
SELECT id, shard_id, namespace_id, table_id, partition_id, object_store_id,
       max_sequence_number, min_time, max_time, to_delete, file_size_bytes,
       row_count, compaction_level, created_at, column_set, max_l0_created_at
FROM parquet_file
WHERE table_id = $1;
             "#,
        )
        .bind(table_id) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ParquetFileId>> {
        // see https://www.crunchydata.com/blog/simulating-update-or-delete-with-limit-in-postgres-ctes-to-the-rescue
        let deleted = sqlx::query(
            r#"
WITH parquet_file_ids as (
    SELECT id
    FROM parquet_file
    WHERE to_delete < $1
    LIMIT $2
)
DELETE FROM parquet_file
WHERE id IN (SELECT id FROM parquet_file_ids)
RETURNING id;
             "#,
        )
        .bind(older_than) // $1
        .bind(MAX_PARQUET_FILES_SELECTED_ONCE) // $2
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        let deleted = deleted.into_iter().map(|row| row.get("id")).collect();
        Ok(deleted)
    }

    async fn list_by_partition_not_to_delete(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Vec<ParquetFile>> {
        sqlx::query_as::<_, ParquetFile>(
            r#"
SELECT id, namespace_id, table_id, partition_id, object_store_id,
       max_sequence_number, min_time, max_time, to_delete, file_size_bytes,
       row_count, compaction_level, created_at, column_set, max_l0_created_at
FROM parquet_file
WHERE parquet_file.partition_id = $1
  AND parquet_file.to_delete IS NULL;
        "#,
        )
        .bind(partition_id) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn update_compaction_level(
        &mut self,
        parquet_file_ids: &[ParquetFileId],
        compaction_level: CompactionLevel,
    ) -> Result<Vec<ParquetFileId>> {
        // If I try to do `.bind(parquet_file_ids)` directly, I get a compile error from sqlx.
        // See https://github.com/launchbadge/sqlx/issues/1744
        let ids: Vec<_> = parquet_file_ids.iter().map(|p| p.get()).collect();
        let updated = sqlx::query(
            r#"
UPDATE parquet_file
SET compaction_level = $1
WHERE id = ANY($2)
RETURNING id;
        "#,
        )
        .bind(compaction_level) // $1
        .bind(&ids[..]) // $2
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        let updated = updated.into_iter().map(|row| row.get("id")).collect();
        Ok(updated)
    }

    async fn exist(&mut self, id: ParquetFileId) -> Result<bool> {
        let read_result = sqlx::query_as::<_, Count>(
            r#"SELECT count(1) as count FROM parquet_file WHERE id = $1;"#,
        )
        .bind(id) // $1
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count > 0)
    }

    async fn count(&mut self) -> Result<i64> {
        let read_result =
            sqlx::query_as::<_, Count>(r#"SELECT count(1) as count FROM parquet_file;"#)
                .fetch_one(&mut self.inner)
                .await
                .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count)
    }

    async fn get_by_object_store_id(
        &mut self,
        object_store_id: Uuid,
    ) -> Result<Option<ParquetFile>> {
        let rec = sqlx::query_as::<_, ParquetFile>(
            r#"
SELECT id, namespace_id, table_id, partition_id, object_store_id,
       max_sequence_number, min_time, max_time, to_delete, file_size_bytes,
       row_count, compaction_level, created_at, column_set, max_l0_created_at
FROM parquet_file
WHERE object_store_id = $1;
             "#,
        )
        .bind(object_store_id) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let parquet_file = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(parquet_file))
    }
}

/// The error code returned by Postgres for a unique constraint violation.
///
/// See <https://www.postgresql.org/docs/9.2/errcodes-appendix.html>
const PG_UNIQUE_VIOLATION: &str = "23505";

/// Returns true if `e` is a unique constraint violation error.
fn is_unique_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(inner) = e {
        if let Some(code) = inner.code() {
            if code == PG_UNIQUE_VIOLATION {
                return true;
            }
        }
    }

    false
}

/// Error code returned by Postgres for a foreign key constraint violation.
const PG_FK_VIOLATION: &str = "23503";

fn is_fk_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(inner) = e {
        if let Some(code) = inner.code() {
            if code == PG_FK_VIOLATION {
                return true;
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_or_get_default_records;
    use assert_matches::assert_matches;
    use data_types::{ColumnId, ColumnSet, SequenceNumber};
    use metric::{Attributes, DurationHistogram, Metric};
    use rand::Rng;
    use sqlx::migrate::MigrateDatabase;
    use std::{env, io::Write, ops::DerefMut, sync::Arc, time::Instant};
    use tempfile::NamedTempFile;

    // Helper macro to skip tests if TEST_INTEGRATION and TEST_INFLUXDB_IOX_CATALOG_DSN environment
    // variables are not set.
    macro_rules! maybe_skip_integration {
        ($panic_msg:expr) => {{
            dotenvy::dotenv().ok();

            let required_vars = ["TEST_INFLUXDB_IOX_CATALOG_DSN"];
            let unset_vars: Vec<_> = required_vars
                .iter()
                .filter_map(|&name| match env::var(name) {
                    Ok(_) => None,
                    Err(_) => Some(name),
                })
                .collect();
            let unset_var_names = unset_vars.join(", ");

            let force = env::var("TEST_INTEGRATION");

            if force.is_ok() && !unset_var_names.is_empty() {
                panic!(
                    "TEST_INTEGRATION is set, \
                            but variable(s) {} need to be set",
                    unset_var_names
                );
            } else if force.is_err() {
                eprintln!(
                    "skipping Postgres integration test - set {}TEST_INTEGRATION to run",
                    if unset_var_names.is_empty() {
                        String::new()
                    } else {
                        format!("{} and ", unset_var_names)
                    }
                );

                let panic_msg: &'static str = $panic_msg;
                if !panic_msg.is_empty() {
                    panic!("{}", panic_msg);
                }

                return;
            }
        }};
        () => {
            maybe_skip_integration!("")
        };
    }

    fn assert_metric_hit(metrics: &metric::Registry, name: &'static str) {
        let histogram = metrics
            .get_instrument::<Metric<DurationHistogram>>("catalog_op_duration")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("op", name), ("result", "success")]))
            .expect("failed to get observer")
            .fetch();

        let hit_count = histogram.sample_count();
        assert!(hit_count > 0, "metric did not record any calls");
    }

    async fn create_db(dsn: &str) {
        // Create the catalog database if it doesn't exist
        if !Postgres::database_exists(dsn).await.unwrap() {
            // Ignore failure if another test has already created the database
            let _ = Postgres::create_database(dsn).await;
        }
    }

    async fn setup_db() -> PostgresCatalog {
        // create a random schema for this particular pool
        let schema_name = {
            // use scope to make it clear to clippy / rust that `rng` is
            // not carried past await points
            let mut rng = rand::thread_rng();
            (&mut rng)
                .sample_iter(rand::distributions::Alphanumeric)
                .filter(|c| c.is_ascii_alphabetic())
                .take(20)
                .map(char::from)
                .collect::<String>()
        };

        let metrics = Arc::new(metric::Registry::default());
        let dsn = std::env::var("TEST_INFLUXDB_IOX_CATALOG_DSN").unwrap();

        create_db(&dsn).await;

        let options = PostgresConnectionOptions {
            app_name: String::from("test"),
            schema_name: schema_name.clone(),
            dsn,
            max_conns: 3,
            ..Default::default()
        };
        let pg = PostgresCatalog::connect(options, metrics)
            .await
            .expect("failed to connect catalog");

        // Create the test schema
        pg.pool
            .execute(format!("CREATE SCHEMA {schema_name};").as_str())
            .await
            .expect("failed to create test schema");

        // Ensure the test user has permission to interact with the test schema.
        pg.pool
            .execute(
                format!(
                    "GRANT USAGE ON SCHEMA {schema_name} TO public; GRANT CREATE ON SCHEMA {schema_name} TO public;"
                )
                .as_str(),
            )
            .await
            .expect("failed to grant privileges to schema");

        // Run the migrations against this random schema.
        pg.setup().await.expect("failed to initialise database");
        pg
    }

    #[tokio::test]
    async fn test_catalog() {
        maybe_skip_integration!();

        let postgres = setup_db().await;

        // Validate the connection time zone is the expected UTC value.
        let tz: String = sqlx::query_scalar("SHOW TIME ZONE;")
            .fetch_one(&postgres.pool)
            .await
            .expect("read time zone");
        assert_eq!(tz, "UTC");

        let pool = postgres.pool.clone();
        let schema_name = postgres.schema_name().to_string();

        let postgres: Arc<dyn Catalog> = Arc::new(postgres);

        crate::interface::test_helpers::test_catalog(|| async {
            // Clean the schema.
            pool
                .execute(format!("DROP SCHEMA {schema_name} CASCADE").as_str())
                .await
                .expect("failed to clean schema between tests");

            // Recreate the test schema
            pool
                .execute(format!("CREATE SCHEMA {schema_name};").as_str())
                .await
                .expect("failed to create test schema");

            // Ensure the test user has permission to interact with the test schema.
            pool
                .execute(
                    format!(
                        "GRANT USAGE ON SCHEMA {schema_name} TO public; GRANT CREATE ON SCHEMA {schema_name} TO public;"
                    )
                    .as_str(),
                )
                .await
                .expect("failed to grant privileges to schema");

            // Run the migrations against this random schema.
            postgres.setup().await.expect("failed to initialise database");

            Arc::clone(&postgres)
        })
        .await;
    }

    #[tokio::test]
    async fn test_partition_create_or_get_idempotent() {
        maybe_skip_integration!();

        let postgres = setup_db().await;

        let postgres: Arc<dyn Catalog> = Arc::new(postgres);
        let mut txn = postgres.start_transaction().await.expect("txn start");
        let (kafka, query) = create_or_get_default_records(txn.deref_mut())
            .await
            .expect("db init failed");
        txn.commit().await.expect("txn commit");

        let namespace_id = postgres
            .repositories()
            .await
            .namespaces()
            .create("ns4", None, kafka.id, query.id)
            .await
            .expect("namespace create failed")
            .id;
        let table_id = postgres
            .repositories()
            .await
            .tables()
            .create_or_get("table", namespace_id)
            .await
            .expect("create table failed")
            .id;

        let key = PartitionKey::from("bananas");

        let hash_id = PartitionHashId::new(table_id, &key);

        let a = postgres
            .repositories()
            .await
            .partitions()
            .create_or_get(key.clone(), table_id)
            .await
            .expect("should create OK");

        assert_eq!(a.hash_id(), hash_id);

        // Call create_or_get for the same (key, table_id) pair, to ensure the write is idempotent.
        let b = postgres
            .repositories()
            .await
            .partitions()
            .create_or_get(key.clone(), table_id)
            .await
            .expect("idempotent write should succeed");

        assert_eq!(a, b);

        // Check that the hash_id is saved in the database and is returned when queried.
        let table_partitions = postgres
            .repositories()
            .await
            .partitions()
            .list_by_table_id(table_id)
            .await
            .unwrap();
        assert_eq!(table_partitions.len(), 1);
        assert_eq!(table_partitions[0].hash_id(), hash_id);
    }

    #[test]
    fn test_parse_dsn_file() {
        assert_eq!(
            get_dsn_file_path("dsn-file:///tmp/my foo.txt"),
            Some("/tmp/my foo.txt".to_owned()),
        );
        assert_eq!(get_dsn_file_path("dsn-file:blah"), None,);
        assert_eq!(get_dsn_file_path("postgres://user:pw@host/db"), None,);
    }

    #[tokio::test]
    async fn test_reload() {
        maybe_skip_integration!();

        const POLLING_INTERVAL: Duration = Duration::from_millis(10);

        // fetch dsn from envvar
        let test_dsn = std::env::var("TEST_INFLUXDB_IOX_CATALOG_DSN").unwrap();
        create_db(&test_dsn).await;
        eprintln!("TEST_DSN={test_dsn}");

        // create a temp file to store the initial dsn
        let mut dsn_file = NamedTempFile::new().expect("create temp file");
        dsn_file
            .write_all(test_dsn.as_bytes())
            .expect("write temp file");

        const TEST_APPLICATION_NAME: &str = "test_application_name";
        let dsn_good = format!("dsn-file://{}", dsn_file.path().display());
        eprintln!("dsn_good={dsn_good}");

        // create a hot swap pool with test application name and dsn file pointing to tmp file.
        // we will later update this file and the pool should be replaced.
        let options = PostgresConnectionOptions {
            app_name: TEST_APPLICATION_NAME.to_owned(),
            schema_name: String::from("test"),
            dsn: dsn_good,
            max_conns: 3,
            hotswap_poll_interval: POLLING_INTERVAL,
            ..Default::default()
        };
        let pool = new_pool(&options).await.expect("connect");
        eprintln!("got a pool");

        // ensure the application name is set as expected
        let application_name: String =
            sqlx::query_scalar("SELECT current_setting('application_name') as application_name;")
                .fetch_one(&pool)
                .await
                .expect("read application_name");
        assert_eq!(application_name, TEST_APPLICATION_NAME);

        // create a new temp file object with updated dsn and overwrite the previous tmp file
        const TEST_APPLICATION_NAME_NEW: &str = "changed_application_name";
        let mut new_dsn_file = NamedTempFile::new().expect("create temp file");
        new_dsn_file
            .write_all(test_dsn.as_bytes())
            .expect("write temp file");
        new_dsn_file
            .write_all(format!("?application_name={TEST_APPLICATION_NAME_NEW}").as_bytes())
            .expect("write temp file");
        new_dsn_file
            .persist(dsn_file.path())
            .expect("overwrite new dsn file");

        // wait until the hotswap machinery has reloaded the updated DSN file and
        // successfully performed a new connection with the new DSN.
        let mut application_name = "".to_string();
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(5)
            && application_name != TEST_APPLICATION_NAME_NEW
        {
            tokio::time::sleep(POLLING_INTERVAL).await;

            application_name = sqlx::query_scalar(
                "SELECT current_setting('application_name') as application_name;",
            )
            .fetch_one(&pool)
            .await
            .expect("read application_name");
        }
        assert_eq!(application_name, TEST_APPLICATION_NAME_NEW);
    }

    macro_rules! test_column_create_or_get_many_unchecked {
        (
            $name:ident,
            calls = {$([$($col_name:literal => $col_type:expr),+ $(,)?]),+},
            want = $($want:tt)+
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_column_create_or_get_many_unchecked_ $name>]() {
                    maybe_skip_integration!();

                    let postgres = setup_db().await;
                    let metrics = Arc::clone(&postgres.metrics);

                    let postgres: Arc<dyn Catalog> = Arc::new(postgres);
                    let mut txn = postgres.start_transaction().await.expect("txn start");
                    let (kafka, query) = create_or_get_default_records(txn.deref_mut())
                        .await
                        .expect("db init failed");
                    txn.commit().await.expect("txn commit");

                    let namespace_id = postgres
                        .repositories()
                        .await
                        .namespaces()
                        .create("ns4", None, kafka.id, query.id)
                        .await
                        .expect("namespace create failed")
                        .id;
                    let table_id = postgres
                        .repositories()
                        .await
                        .tables()
                        .create_or_get("table", namespace_id)
                        .await
                        .expect("create table failed")
                        .id;

                    $(
                        let mut insert = HashMap::new();
                        $(
                            insert.insert($col_name, $col_type);
                        )+

                        let got = postgres
                            .repositories()
                            .await
                            .columns()
                            .create_or_get_many_unchecked(table_id, insert.clone())
                            .await;

                        // The returned columns MUST always match the requested
                        // column values if successful.
                        if let Ok(got) = &got {
                            assert_eq!(insert.len(), got.len());

                            for got in got {
                                assert_eq!(table_id, got.table_id);
                                let requested_column_type = insert
                                    .get(got.name.as_str())
                                    .expect("Should have gotten back a column that was inserted");
                                assert_eq!(
                                    *requested_column_type,
                                    ColumnType::try_from(got.column_type)
                                        .expect("invalid column type")
                                );
                            }

                            assert_metric_hit(&metrics, "column_create_or_get_many_unchecked");
                        }
                    )+

                    assert_matches!(got, $($want)+);
                }
            }
        }
    }

    // Issue a few calls to create_or_get_many that contain distinct columns and
    // covers the full set of column types.
    test_column_create_or_get_many_unchecked!(
        insert,
        calls = {
            [
                "test1" => ColumnType::I64,
                "test2" => ColumnType::U64,
                "test3" => ColumnType::F64,
                "test4" => ColumnType::Bool,
                "test5" => ColumnType::String,
                "test6" => ColumnType::Time,
                "test7" => ColumnType::Tag,
            ],
            [
                "test8" => ColumnType::String,
                "test9" => ColumnType::Bool,
            ]
        },
        want = Ok(_)
    );

    // Issue two calls with overlapping columns - request should succeed (upsert
    // semantics).
    test_column_create_or_get_many_unchecked!(
        partial_upsert,
        calls = {
            [
                "test1" => ColumnType::I64,
                "test2" => ColumnType::U64,
                "test3" => ColumnType::F64,
                "test4" => ColumnType::Bool,
            ],
            [
                "test1" => ColumnType::I64,
                "test2" => ColumnType::U64,
                "test3" => ColumnType::F64,
                "test4" => ColumnType::Bool,
                "test5" => ColumnType::String,
                "test6" => ColumnType::Time,
                "test7" => ColumnType::Tag,
                "test8" => ColumnType::String,
            ]
        },
        want = Ok(_)
    );

    // Issue two calls with the same columns and types.
    test_column_create_or_get_many_unchecked!(
        full_upsert,
        calls = {
            [
                "test1" => ColumnType::I64,
                "test2" => ColumnType::U64,
                "test3" => ColumnType::F64,
                "test4" => ColumnType::Bool,
            ],
            [
                "test1" => ColumnType::I64,
                "test2" => ColumnType::U64,
                "test3" => ColumnType::F64,
                "test4" => ColumnType::Bool,
            ]
        },
        want = Ok(_)
    );

    // Issue two calls with overlapping columns with conflicting types and
    // observe a correctly populated ColumnTypeMismatch error.
    test_column_create_or_get_many_unchecked!(
        partial_type_conflict,
        calls = {
            [
                "test1" => ColumnType::String,
                "test2" => ColumnType::String,
                "test3" => ColumnType::String,
                "test4" => ColumnType::String,
            ],
            [
                "test1" => ColumnType::String,
                "test2" => ColumnType::Bool, // This one differs
                "test3" => ColumnType::String,
                // 4 is missing.
                "test5" => ColumnType::String,
                "test6" => ColumnType::Time,
                "test7" => ColumnType::Tag,
                "test8" => ColumnType::String,
            ]
        },
        want = Err(e) => {
            assert_matches!(e, Error::ColumnTypeMismatch { name, existing, new } => {
                assert_eq!(name, "test2");
                assert_eq!(existing, ColumnType::String);
                assert_eq!(new, ColumnType::Bool);
            })
        }
    );

    #[tokio::test]
    async fn test_billing_summary_on_parqet_file_creation() {
        maybe_skip_integration!();

        let postgres = setup_db().await;
        let pool = postgres.pool.clone();

        let postgres: Arc<dyn Catalog> = Arc::new(postgres);
        let mut txn = postgres.start_transaction().await.expect("txn start");
        let (kafka, query) = create_or_get_default_records(txn.deref_mut())
            .await
            .expect("db init failed");
        txn.commit().await.expect("txn commit");

        let namespace_id = postgres
            .repositories()
            .await
            .namespaces()
            .create("ns4", None, kafka.id, query.id)
            .await
            .expect("namespace create failed")
            .id;
        let table_id = postgres
            .repositories()
            .await
            .tables()
            .create_or_get("table", namespace_id)
            .await
            .expect("create table failed")
            .id;

        let key = "bananas";

        let partition_id = postgres
            .repositories()
            .await
            .partitions()
            .create_or_get(key.into(), table_id)
            .await
            .expect("should create OK")
            .id;

        // parquet file to create- all we care about here is the size, the rest is to satisfy DB
        // constraints
        let time_provider = Arc::new(SystemProvider::new());
        let time_now = Timestamp::from(time_provider.now());
        let mut p1 = ParquetFileParams {
            namespace_id,
            table_id,
            partition_id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(100),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(5),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial, // level of file of new writes
            created_at: time_now,
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
            max_l0_created_at: time_now,
        };
        let f1 = postgres
            .repositories()
            .await
            .parquet_files()
            .create(p1.clone())
            .await
            .expect("create parquet file should succeed");
        // insert the same again with a different size; we should then have 3x1337 as total file size
        p1.object_store_id = Uuid::new_v4();
        p1.file_size_bytes *= 2;
        let _f2 = postgres
            .repositories()
            .await
            .parquet_files()
            .create(p1.clone())
            .await
            .expect("create parquet file should succeed");

        // after adding two files we should have 3x1337 in the summary
        let total_file_size_bytes: i64 =
            sqlx::query_scalar("SELECT total_file_size_bytes FROM billing_summary;")
                .fetch_one(&pool)
                .await
                .expect("fetch total file size failed");
        assert_eq!(total_file_size_bytes, 1337 * 3);

        // flag f1 for deletion and assert that the total file size is reduced accordingly.
        postgres
            .repositories()
            .await
            .parquet_files()
            .flag_for_delete(f1.id)
            .await
            .expect("flag parquet file for deletion should succeed");
        let total_file_size_bytes: i64 =
            sqlx::query_scalar("SELECT total_file_size_bytes FROM billing_summary;")
                .fetch_one(&pool)
                .await
                .expect("fetch total file size failed");
        // we marked the first file of size 1337 for deletion leaving only the second that was 2x that
        assert_eq!(total_file_size_bytes, 1337 * 2);

        // actually deleting shouldn't change the total
        let now = Timestamp::from(time_provider.now());
        postgres
            .repositories()
            .await
            .parquet_files()
            .delete_old_ids_only(now)
            .await
            .expect("parquet file deletion should succeed");
        let total_file_size_bytes: i64 =
            sqlx::query_scalar("SELECT total_file_size_bytes FROM billing_summary;")
                .fetch_one(&pool)
                .await
                .expect("fetch total file size failed");
        assert_eq!(total_file_size_bytes, 1337 * 2);
    }
}
