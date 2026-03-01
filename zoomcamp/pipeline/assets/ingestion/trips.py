"""@bruin

name: ingestion.trips
connection: duckdb-default

materialization:
  type: table
  strategy: append
image: python:3.11

secrets:
  - key: duckdb-default
    inject_as: duckdb-default

columns:
  - name: pickup_datetime
    type: timestamp
    description: pickup timestamp (may vary by taxi type)
  - name: dropoff_datetime
    type: timestamp
    description: dropoff timestamp (may vary by taxi type)
  - name: passenger_count
    type: DOUBLE
    description: passenger count
  - name: trip_distance
    type: DOUBLE
    description: trip distance (miles)
  - name: payment_type
    type: DOUBLE
    description: payment type description or code
  - name: extracted_at
    type: TIMESTAMP
    description: ingestion timestamp
  - name: vendor_id
    type: BIGINT
  - name: tpep_pickup_datetime
    type: TIMESTAMP
  - name: tpep_dropoff_datetime
    type: TIMESTAMP
  - name: ratecode_id
    type: DOUBLE
  - name: store_and_fwd_flag
    type: VARCHAR
  - name: pu_location_id
    type: BIGINT
  - name: do_location_id
    type: BIGINT
  - name: fare_amount
    type: DOUBLE
  - name: extra
    type: DOUBLE
  - name: mta_tax
    type: DOUBLE
  - name: tip_amount
    type: DOUBLE
  - name: tolls_amount
    type: DOUBLE
  - name: improvement_surcharge
    type: DOUBLE
  - name: total_amount
    type: DOUBLE
  - name: congestion_surcharge
    type: DOUBLE
  - name: airport_fee
    type: DOUBLE
  - name: source_taxi_type
    type: VARCHAR
  - name: lpep_pickup_datetime
    type: TIMESTAMP
  - name: lpep_dropoff_datetime
    type: TIMESTAMP
  - name: trip_type
    type: DOUBLE

@bruin"""

import os
import json
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd
import requests
from io import BytesIO


def _month_range(start_date: date, end_date: date):
  cur = date(start_date.year, start_date.month, 1)
  last = date(end_date.year, end_date.month, 1)
  while cur <= last:
    yield cur.year, cur.month
    cur = (cur + relativedelta(months=1))


def _read_parquet_url(url: str) -> pd.DataFrame:
  # Try direct read; fall back to streaming bytes if necessary
  try:
    return pd.read_parquet(url, engine="pyarrow")
  except Exception:
    resp = requests.get(url, stream=True, timeout=30)
    resp.raise_for_status()
    return pd.read_parquet(BytesIO(resp.content), engine="pyarrow")


def materialize():
  """
  Ingest NYC taxi monthly parquet files for the given date window and taxi types.

  Expects the following environment variables provided by Bruin runtime:
  - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
  - BRUIN_VARS: JSON string with optional key `taxi_types` (list of strings)

  Behavior:
  - Builds monthly file URLs for each taxi type and date in window
  - Attempts to read each parquet into a DataFrame and concatenates all reads
  - Adds `extracted_at` column (UTC timestamp)
  - Returns the concatenated DataFrame for Bruin Python materialization
  """
  start_str = os.getenv("BRUIN_START_DATE")
  end_str = os.getenv("BRUIN_END_DATE")
  vars_json = os.getenv("BRUIN_VARS", "{}")

  if not start_str or not end_str:
    raise RuntimeError("BRUIN_START_DATE and BRUIN_END_DATE must be set")

  start = datetime.fromisoformat(start_str).date()
  end = datetime.fromisoformat(end_str).date()

  try:
    pipeline_vars = json.loads(vars_json)
  except Exception:
    pipeline_vars = {}

  taxi_types = pipeline_vars.get("taxi_types", ["yellow"])

  frames = []
  for taxi in taxi_types:
    for y, m in _month_range(start, end):
      url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{y}-{m:02d}.parquet"
      try:
        df = _read_parquet_url(url)
      except Exception:
        # Skip missing or unreadable months (keep ingestion robust)
        continue

      # attach metadata for lineage
      df["extracted_at"] = pd.Timestamp.utcnow()
      df["source_taxi_type"] = taxi
      frames.append(df)

  if not frames:
    # return empty DataFrame with minimal schema
    return pd.DataFrame(columns=["pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance", "payment_type", "extracted_at", "source_taxi_type"])

  out = pd.concat(frames, ignore_index=True)
  return out
