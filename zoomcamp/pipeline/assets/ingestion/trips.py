"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: trip_id
    type: string
    primary_key: true
    description: Unique identifier for each trip
  - name: vendor_id
    type: int
    description: Identifier for the vendor (payment provider)
  - name: tpep_pickup_datetime
    type: timestamp
    description: Trip pickup date and time
  - name: tpep_dropoff_datetime
    type: timestamp
    description: Trip dropoff date and time
  - name: passenger_count
    type: int
    description: Number of passengers in the trip
  - name: trip_distance
    type: double
    description: Distance traveled in miles
  - name: pickup_location_id
    type: int
    description: TLC location ID for pickup
  - name: dropoff_location_id
    type: int
    description: TLC location ID for dropoff
  - name: rate_code_id
    type: int
    description: Rate code applied to the trip
  - name: payment_type
    type: int
    description: Payment method code
  - name: fare_amount
    type: double
    description: Base fare amount
  - name: extra
    type: double
    description: Extra charges (rush hour, tolls, etc.)
  - name: mta_tax
    type: double
    description: MTA tax amount
  - name: tip_amount
    type: double
    description: Tip amount (if credit card payment)
  - name: tolls_amount
    type: double
    description: Tolls paid in the trip
  - name: total_amount
    type: double
    description: Total amount charged for the trip
  - name: extracted_at
    type: timestamp
    description: Timestamp when the data was extracted

@bruin"""
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime, timedelta
import os
import json
import logging

logger = logging.getLogger(__name__)


def materialize():
    """
    Fetch NYC Taxi trip data from the TLC public API for the given date range and taxi types.
    
    Uses Bruin runtime context:
    - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD) for the incremental window
    - BRUIN_VARS (JSON) containing `taxi_types` configuration
    
    Returns a DataFrame with trip records, adding an `extracted_at` timestamp column
    for lineage and debugging.
    """
    
    # Read environment variables set by Bruin runtime
    start_date_str = os.getenv("BRUIN_START_DATE")
    end_date_str = os.getenv("BRUIN_END_DATE")
    bruin_vars_str = os.getenv("BRUIN_VARS", "{}")
    
    if not start_date_str or not end_date_str:
        raise ValueError(
            "BRUIN_START_DATE and BRUIN_END_DATE environment variables are required."
        )
    
    # Parse dates
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    # Parse pipeline variables (taxi_types configuration)
    try:
        bruin_vars = json.loads(bruin_vars_str)
        taxi_types = bruin_vars.get("taxi_types", ["yellow", "green"])
    except json.JSONDecodeError:
        logger.warning("Failed to parse BRUIN_VARS, using default taxi_types")
        taxi_types = ["yellow", "green"]
    
    logger.info(
        f"Fetching trip data from {start_date_str} to {end_date_str} "
        f"for taxi types: {taxi_types}"
    )
    
    # Base URL for NYC TLC data
    base_url = "https://d37ci6vzqshvs5.cloudfront.net/trip-data"
    
    # Generate list of months to fetch
    current_date = start_date
    months_to_fetch = []
    
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        months_to_fetch.append((year, month))
        # Move to next month
        if month == 12:
            current_date = current_date.replace(year=year + 1, month=1)
        else:
            current_date = current_date.replace(month=month + 1)
    
    # Fetch data for each taxi type and month
    dataframes = []
    extracted_at = datetime.utcnow()
    
    for taxi_type in taxi_types:
        for year, month in months_to_fetch:
            # Construct the parquet file URL
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            url = f"{base_url}/{filename}"
            
            try:
                logger.info(f"Fetching {filename}...")
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                # Read parquet data from the response
                df = pd.read_parquet(BytesIO(response.content))
                
                # Normalize column names to lowercase (NYC TLC uses mixed case)
                df.columns = df.columns.str.lower()
                
                # Add extraction timestamp
                df["extracted_at"] = extracted_at
                
                dataframes.append(df)
                logger.info(f"Successfully fetched {len(df)} records from {filename}")
                
            except requests.HTTPError as e:
                if response.status_code == 404:
                    logger.warning(f"File not found: {filename} (skipping)")
                else:
                    logger.error(f"HTTP error fetching {filename}: {e}")
            except Exception as e:
                logger.error(f"Error fetching {filename}: {e}")
    
    if not dataframes:
        logger.warning("No data was fetched. Returning empty DataFrame.")
        return pd.DataFrame()
    
    # Concatenate all dataframes
    final_df = pd.concat(dataframes, ignore_index=True)
    
    logger.info(f"Total records fetched: {len(final_df)}")
    
    # Ensure all expected columns are present (NaN for missing ones)
    expected_columns = [
        "trip_id", "vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "pickup_location_id", "dropoff_location_id",
        "rate_code_id", "payment_type", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "total_amount", "extracted_at"
    ]
    
    for col in expected_columns:
        if col not in final_df.columns:
            final_df[col] = None
    
    # Select and reorder columns
    final_df = final_df[expected_columns]
    
    return final_df



