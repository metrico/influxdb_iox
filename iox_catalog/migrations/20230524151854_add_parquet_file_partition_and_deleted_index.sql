-- Add to help the querier when it searches for undeleted parquet files.
--
--
-- # Transaction Handling
-- From https://www.postgresql.org/docs/current/sql-createindex.html :
--
-- > a standard index build locks out writes (but not reads) on the table until it's done.
--
-- Since the `parquet_file` table in prod is rather large, this write lock would be held quite long. So we want to use
-- the `CONCURRENTLY`.
--
-- However, from https://www.postgresql.org/docs/current/sql-createindex.html :
--
-- > a regular `CREATE INDEX` command can be performed within a transaction block, but `CREATE INDEX CONCURRENTLY`
-- > cannot.
--
-- SQLx however relies on transactions for its migrations:
-- - https://github.com/launchbadge/sqlx/blob/bb064e3789d68ad4e9affe7cba34944abb000f72/sqlx-core/src/postgres/migrate.rs#L225-L230
-- - https://github.com/launchbadge/sqlx/issues/2085
--
-- As a workaround, we kinda hack in a non-transaction phase into the migration script as suggested by
-- https://github.com/launchbadge/sqlx/issues/2085#issuecomment-1499859906 .

-- Stop SQLx migration transaction.
COMMIT TRANSACTION;

-- By default we often only have 5min to finish our statements. The `CREATE INDEX CONCURRENTLY` however takes longer.
-- In our prod test this took about 15min, but better be safe than sorry.
SET statement_timeout TO '60min';

-- While `CONCURRENTLY` means it runs parallel to other writes, this command will only finish after the index was
-- successfully built.
CREATE INDEX CONCURRENTLY IF NOT EXISTS parquet_file_table_delete_idx ON parquet_file (table_id) WHERE to_delete IS NULL;

-- Setup a new transaction so that the SQLx migration runs through.
BEGIN TRANSACTION;
