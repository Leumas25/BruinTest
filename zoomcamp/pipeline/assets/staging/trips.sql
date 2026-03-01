/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
name: staging.trips
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table


# TODO: Define output columns, mark primary keys, and add a few checks.
columns:
  - name: pickup_datetime
    type: timestamp
    description: pickup timestamp (event time)
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: dropoff timestamp
  - name: passenger_count
    type: DOUBLE
    description: passenger count
  - name: trip_distance
    type: DOUBLE
    description: trip distance (miles)
  - name: payment_type_id
    type: INTEGER
    description: payment type id (lookup key)
  - name: payment_type_name
    type: string
    description: payment type name from lookup
  - name: extracted_at
    type: TIMESTAMP
    description: ingestion timestamp
  - name: vendor_id
    type: BIGINT
  - name: pu_location_id
    type: BIGINT
  - name: do_location_id
    type: BIGINT
  - name: fare_amount
    type: DOUBLE
  - name: tip_amount
    type: DOUBLE
  - name: total_amount
    type: DOUBLE
  - name: source_taxi_type
    type: VARCHAR
    description: taxi color/type (as provided by ingestion)
  - name: row_hash
    type: string
    description: deterministic hash to detect duplicates
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: unique

# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: row_count_positive
    description: Ensure the query returns at least 1 row.
    query: |
      SELECT Count(*) > 0  FROM staging.trips
    value: 0

@bruin */

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

-- Staging SELECT: normalize, join lookup, and deduplicate.
WITH src AS (
  SELECT
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    passenger_count,
    trip_distance,
    CAST(payment_type AS INTEGER) AS payment_type_id,
    extracted_at,
    vendor_id,
    pu_location_id as pickup_location_id,
    do_location_id as dropoff_location_id,
    fare_amount,
    tip_amount,
    total_amount,
    source_taxi_type
  FROM ingestion.trips
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
),

joined AS (
  SELECT s.*,
         p.payment_type_name
  FROM src s
  LEFT JOIN ingestion.payment_lookup p
    ON s.payment_type_id = p.payment_type_id
),

deduped AS (
  SELECT *,
         md5(
           CONCAT(
             COALESCE(CAST(pickup_datetime AS VARCHAR),''), '|',
             COALESCE(CAST(vendor_id AS VARCHAR),''), '|',
             COALESCE(CAST(pickup_location_id AS VARCHAR),''), '|',
             COALESCE(CAST(dropoff_location_id AS VARCHAR),''), '|',
             COALESCE(CAST(fare_amount AS VARCHAR),''), '|',
             COALESCE(CAST(total_amount AS VARCHAR),'')
           )
         ) AS row_hash,
         row_number() OVER (
           PARTITION BY pickup_datetime, vendor_id, pickup_location_id, dropoff_location_id, passenger_count
           ORDER BY extracted_at DESC
         ) AS rn
  FROM joined
)

SELECT
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  payment_type_id,
  payment_type_name,
  extracted_at,
  vendor_id,
  pickup_location_id,
  dropoff_location_id,
  fare_amount,
  tip_amount,
  total_amount,
  source_taxi_type,
  row_hash
FROM deduped
WHERE rn = 1
