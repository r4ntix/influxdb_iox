-- IOX_SETUP: TwoMeasurementsManyFields

-- validate we have access to information schema for listing table names
SELECT * from information_schema.tables;

-- validate we have access to information schema for listing column names
SELECT * from information_schema.columns where table_name = 'h2o' OR table_name = 'o2';

-- validate we have access to SHOW SCHEMA for listing columns names
SHOW COLUMNS FROM h2o;
