ALTER TABLE
  IF EXISTS partition
  ADD COLUMN hash_id bytea;

ALTER TABLE
  IF EXISTS partition
  ADD CONSTRAINT partition_hash_id_unique UNIQUE (hash_id);
