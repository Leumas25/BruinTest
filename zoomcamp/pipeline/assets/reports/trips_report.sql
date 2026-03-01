/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# Asset name for reporting on trips aggregated daily by payment and taxi type
name: reports.trips_report

# Platform type
type: duckdb.sql

# Declare dependency on the staging asset
depends:
  - staging.trips

materialization:
  type: table


columns:
  - name: report_date
    type: DATE
    description: date of the aggregation (derived from pickup_datetime)
    primary_key: true
  - name: payment_type_name
    type: STRING
    description: payment type name from lookup
    primary_key: true
  - name: source_taxi_type
    type: STRING
    description: taxi color/type (from ingestion)
    primary_key: true
  - name: trips_count
    type: BIGINT
    description: number of trips in the group
    checks:
      - name: non_negative
  - name: total_amount
    type: DOUBLE
    description: sum of `total_amount` for the group
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: DOUBLE
    description: average trip distance (miles)
  - name: avg_tip_pct
    type: DOUBLE
    description: average tip as fraction of fare (tip / fare)

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

-- Aggregate staging data by date, payment type and taxi type.
SELECT
  CAST(DATE(pickup_datetime) AS DATE) AS report_date,
  COALESCE(payment_type_name, 'UNKNOWN') AS payment_type_name,
  COALESCE(source_taxi_type, 'UNKNOWN') AS source_taxi_type,
  COUNT(*) AS trips_count,
  SUM(COALESCE(total_amount,0.0)) AS total_amount,
  AVG(COALESCE(trip_distance,0.0)) AS avg_trip_distance,
  AVG(CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount ELSE NULL END) AS avg_tip_pct
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY report_date, payment_type_name, source_taxi_type
ORDER BY report_date, payment_type_name, source_taxi_type
