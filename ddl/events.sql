```sql
-- ddl/events.sql
-- Canonical "Event Log" table for the event simulator payload/envelope.
-- Works as a reference DDL you can adapt to:
--   (A) Iceberg on S3/local (Spark/Trino/Athena/etc.)
--   (B) Snowflake native table (VARIANT for JSON payload)
--
-- Envelope fields produced by the sim:
-- event_id, event_type, schema_version, event_time, ingest_time,
-- tenant_id, user_id, session_id, source_system, environment,
-- record_source, payload (JSON), checksum

-- ============================================================
-- A) ICEBERG / LAKEHOUSE DDL (Trino/Spark SQL-ish)
-- ============================================================
-- Notes:
-- - Prefer Parquet storage
-- - Partition by event_date (derived from event_time) for time-based access
-- - Keep raw payload for schema evolution; extract stable dimensions as columns

CREATE TABLE IF NOT EXISTS lakehouse.events (
  event_id        STRING      NOT NULL,
  event_type      STRING      NOT NULL,
  schema_version  INT         NOT NULL,

  event_time      TIMESTAMP   NOT NULL,
  ingest_time     TIMESTAMP   NOT NULL,

  tenant_id       STRING,
  user_id         STRING,
  session_id      STRING,

  source_system   STRING,
  environment     STRING,
  record_source   STRING,

  -- Raw evolving data
  payload         STRING,     -- store JSON as STRING (portable); engines can parse JSON from string
  checksum        STRING,

  -- Partition column (derived upstream when writing)
  event_date      DATE        NOT NULL
)
USING ICEBERG
PARTITIONED BY (event_date);

-- Optional: if your engine supports JSON type, change payload to JSON/VARIANT/STRUCT.

-- ============================================================
-- B) SNOWFLAKE DDL (native)
-- ============================================================
-- Notes:
-- - Use VARIANT for payload to preserve evolving schema
-- - Cluster by event_date/event_time if needed (optional)
-- - Keep event_date as a stored column for pruning and retention

CREATE TABLE IF NOT EXISTS ANALYTICS.EVENTS (
  EVENT_ID        STRING       NOT NULL,
  EVENT_TYPE      STRING       NOT NULL,
  SCHEMA_VERSION  INTEGER      NOT NULL,

  EVENT_TIME      TIMESTAMP_TZ NOT NULL,
  INGEST_TIME     TIMESTAMP_TZ NOT NULL,

  TENANT_ID       STRING,
  USER_ID         STRING,
  SESSION_ID      STRING,

  SOURCE_SYSTEM   STRING,
  ENVIRONMENT     STRING,
  RECORD_SOURCE   STRING,

  PAYLOAD         VARIANT,     -- raw JSON
  CHECKSUM        STRING,

  EVENT_DATE      DATE         NOT NULL,

  -- Optional ingestion metadata
  LOADED_AT       TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- Optional (Snowflake): help pruning on time-based queries
-- ALTER TABLE ANALYTICS.EVENTS CLUSTER BY (EVENT_DATE, EVENT_TYPE);

-- ============================================================
-- Suggested constraint/index notes (conceptual)
-- ============================================================
-- - event_id should be unique in practice (idempotency). Enforce if your engine supports it.
-- - In lakehouse systems, uniqueness is usually handled in ETL (dedup by event_id).
-- - For analytics queries, most filters are: event_date range, event_type, tenant_id.
```

