-- IOX_SETUP: TwoMeasurementsManyFieldsOneChunk

-- Note this setup only uses a single chunk configuration
-- as the system tables reflect the state of the chunks

-- ensures the tables / plumbing are hooked up (so no need to test timestamp columns, etc)
SELECT partition_key, table_name, storage, memory_bytes, row_count from system.chunks;

-- ensures the tables / plumbing are hooked up (so no need to test timestamp columns, etc)
SELECT * from system.columns;
