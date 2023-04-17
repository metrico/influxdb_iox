ALTER TABLE
  partition
  ADD COLUMN hash_id BLOB;

CREATE UNIQUE INDEX IF NOT EXISTS partition_hash_id_unique ON partition (hash_id);
