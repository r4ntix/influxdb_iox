-- IOX_SETUP: TwoMeasurementsManyFieldsTwoChunks

-- Note this setup only uses a single 2 chunk configuration
-- as the system tables reflect the state of the chunks

-- ensures the tables / plumbing are hooked up (so no need to test timestamp columns, etc)
SELECT partition_key, table_name, column_name, storage, row_count, null_count, min_value, max_value, memory_bytes from system.chunk_columns;
