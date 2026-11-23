-- Run once in a Databricks SQL editor or notebook cell.
-- If CREATE CATALOG is blocked on your account, skip it and use your default
-- workspace catalog name everywhere you see "seattle".
CREATE CATALOG IF NOT EXISTS seattle;
CREATE SCHEMA IF NOT EXISTS seattle.raw;
CREATE SCHEMA IF NOT EXISTS seattle.bronze;
CREATE SCHEMA IF NOT EXISTS seattle.silver;
CREATE SCHEMA IF NOT EXISTS seattle.gold;
CREATE VOLUME IF NOT EXISTS seattle.raw.landing;      -- files from AWS land here
CREATE VOLUME IF NOT EXISTS seattle.raw.checkpoints;  -- streaming state lives here
