"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default
depends:
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
  - name: pickup_location_id
    type: integer
    description: "TLC taxi zone for pickup"
  - name: dropoff_location_id
    type: integer
    description: "TLC taxi zone for dropoff"
  - name: fare_amount
    type: float
    description: "Base fare in USD"
  - name: taxi_type
    type: string
    description: "yellow or green"
  - name: payment_type
    type: integer
    description: "Payment type code (joins to payment_lookup)"
@bruin"""

import os
import json
from datetime import datetime
from io import BytesIO

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
MAX_MONTHS_PER_RUN = 12  # Avoid OOM; for larger backfills run multiple times with different date ranges


def _parse_date(s: str) -> datetime:
    """Parse YYYY-MM-DD to datetime."""
    return datetime.strptime(s.strip()[:10], "%Y-%m-%d")


def _month_range(start_date: datetime, end_date: datetime):
    """Yield (year, month) tuples from start_date through end_date."""
    current = datetime(start_date.year, start_date.month, 1)
    end = datetime(end_date.year, end_date.month, 1)
    while current <= end:
        yield current.year, current.month
        current += relativedelta(months=1)


def _normalize_trips(df: pd.DataFrame, taxi_type: str) -> pd.DataFrame:
    """Map TLC column names to a common schema and add taxi_type."""
    pickup_col = "tpep_pickup_datetime" if "tpep_pickup_datetime" in df.columns else "lpep_pickup_datetime"
    dropoff_col = "tpep_dropoff_datetime" if "tpep_dropoff_datetime" in df.columns else "lpep_dropoff_datetime"
    rename = {
        pickup_col: "pickup_datetime",
        dropoff_col: "dropoff_datetime",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
    }
    out = df.rename(columns=rename)
    out["taxi_type"] = taxi_type
    required = ["pickup_datetime", "dropoff_datetime", "pickup_location_id", "dropoff_location_id", "fare_amount", "taxi_type", "payment_type"]
    return out[[c for c in required if c in out.columns]]


def materialize():
    start_date = _parse_date(os.environ["BRUIN_START_DATE"])
    end_date = _parse_date(os.environ["BRUIN_END_DATE"])
    vars_str = os.environ.get("BRUIN_VARS", "{}")
    taxi_types = json.loads(vars_str).get("taxi_types", ["yellow"])

    num_months = sum(1 for _ in _month_range(start_date, end_date))
    if num_months > MAX_MONTHS_PER_RUN:
        raise ValueError(
            f"Date range is {num_months} months (max {MAX_MONTHS_PER_RUN} per run to avoid OOM). "
            f"Use --start-date and --end-date to limit the range, e.g. "
            f"--start-date 2022-01-01 --end-date 2022-03-01"
        )

    frames = []
    for taxi_type in taxi_types:
        for year, month in _month_range(start_date, end_date):
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            url = f"{BASE_URL}{filename}"
            try:
                resp = requests.get(url, timeout=60)
                resp.raise_for_status()
                df = pd.read_parquet(BytesIO(resp.content))
                if df.empty:
                    continue
                frames.append(_normalize_trips(df, taxi_type))
            except Exception as e:
                print(f"Skipping {url}: {e}")
                continue

    if not frames:
        return pd.DataFrame(
            columns=["pickup_datetime", "dropoff_datetime", "pickup_location_id", "dropoff_location_id", "fare_amount", "taxi_type", "payment_type"]
        )
    return pd.concat(frames, ignore_index=True)