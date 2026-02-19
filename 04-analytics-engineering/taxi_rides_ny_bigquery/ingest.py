"""
Data Ingestion Script for NYC Taxi Data → BigQuery

This script downloads NYC taxi trip data (yellow and green taxis) from GitHub releases,
converts CSV.gz files to Parquet using pandas/pyarrow, uploads Parquet to GCS,
validates column and type compatibility with the target BigQuery schema,
and loads the data into BigQuery.

No DuckDB is used; everything is BigQuery- and GCS-based.
"""

import os
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden

# Base URL for NYC TLC taxi data from GitHub releases
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

# All data paths are relative to the script directory so they work regardless of cwd
SCRIPT_DIR = Path(__file__).resolve().parent

# GCP config (override via env or set here)
BUCKET_NAME = os.environ.get("GCS_BUCKET", "taxi-zoomcamp-cool-agility-485919-g8")
GCP_PROJECT = os.environ.get("GCP_PROJECT", "cool-agility-485919-g8")
BQ_DATASET = os.environ.get("BQ_DATASET", "zoomcamp")
# Path to service account JSON; if unset, use ADC (e.g. gcloud auth application-default login)
CREDENTIALS_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "keys/gcs2_bucket.json")

CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB for GCS uploads

# Expected raw columns for dbt (sources.yml: yellow_tripdata -> yellow_tripdata_non_partitioned)
# Must match what stg_yellow_tripdata.sql and staging expect (lowercase names).
REQUIRED_YELLOW_COLUMNS = [
    "vendorid", "ratecodeid", "pulocationid", "dolocationid",
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "store_and_fwd_flag",
    "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "payment_type",
]

# BigQuery schema for yellow_tripdata raw table (matches dbt source and staging casts).
BQ_YELLOW_SCHEMA = [
    bigquery.SchemaField("vendorid", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("ratecodeid", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("pulocationid", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("dolocationid", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("tpep_pickup_datetime", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("tpep_dropoff_datetime", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("store_and_fwd_flag", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("passenger_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("trip_distance", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("fare_amount", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("extra", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("mta_tax", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("tip_amount", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("tolls_amount", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("improvement_surcharge", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_amount", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("payment_type", "INTEGER", mode="NULLABLE"),
]

# Green taxi: same structure as yellow but lpep_pickup_datetime / lpep_dropoff_datetime (plus optional trip_type).
REQUIRED_GREEN_COLUMNS = [
    "vendorid", "ratecodeid", "pulocationid", "dolocationid",
    "lpep_pickup_datetime", "lpep_dropoff_datetime", "store_and_fwd_flag",
    "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "payment_type",
]

BQ_GREEN_SCHEMA = [
    bigquery.SchemaField("vendorid", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("ratecodeid", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("pulocationid", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("dolocationid", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("lpep_pickup_datetime", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("lpep_dropoff_datetime", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("store_and_fwd_flag", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("passenger_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("trip_distance", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("fare_amount", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("extra", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("mta_tax", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("tip_amount", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("tolls_amount", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("improvement_surcharge", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_amount", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("payment_type", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("trip_type", "INTEGER", mode="NULLABLE"),  # 1=street-hail, 2=dispatch (green only)
]


def _get_gcs_client():
    if os.path.exists(CREDENTIALS_FILE):
        return storage.Client.from_service_account_json(CREDENTIALS_FILE)
    return storage.Client(project=GCP_PROJECT)


def _get_bq_client():
    if os.path.exists(CREDENTIALS_FILE):
        return bigquery.Client.from_service_account_json(CREDENTIALS_FILE)
    return bigquery.Client(project=GCP_PROJECT)


# Columns that must be written as INT64 in Parquet for BigQuery schema compatibility.
# Pandas often infers these as float when CSV has nulls or mixed types.
_YELLOW_INT_COLUMNS = [
    "vendorid", "ratecodeid", "pulocationid", "dolocationid",
    "passenger_count", "payment_type",
]
_YELLOW_FLOAT_COLUMNS = [
    "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount",
]
_YELLOW_DATETIME_COLUMNS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

_GREEN_INT_COLUMNS = [
    "vendorid", "ratecodeid", "pulocationid", "dolocationid",
    "passenger_count", "payment_type", "trip_type",
]
_GREEN_FLOAT_COLUMNS = [
    "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount",
]
_GREEN_DATETIME_COLUMNS = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]


def _cast_yellow_df_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """Casts yellow taxi columns to types that write as INT64/FLOAT64/TIMESTAMP in Parquet."""
    df = df.copy()
    for col in _YELLOW_INT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in _YELLOW_FLOAT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    # BigQuery expects TIMESTAMP in microseconds; pandas/pyarrow default is nanoseconds (TIMESTAMP_NANOS) which BQ rejects.
    for col in _YELLOW_DATETIME_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").astype("datetime64[us]")
    if "store_and_fwd_flag" in df.columns:
        df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype("string")
    return df


def _cast_green_df_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """Casts green taxi columns to types that write as INT64/FLOAT64/TIMESTAMP in Parquet."""
    df = df.copy()
    for col in _GREEN_INT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in _GREEN_FLOAT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    for col in _GREEN_DATETIME_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").astype("datetime64[us]")
    if "store_and_fwd_flag" in df.columns:
        df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype("string")
    return df


def download_and_convert_to_parquet(taxi_type: str) -> None:
    """
    Downloads CSV.gz files for a given taxi type and converts them to Parquet using pandas.

    - Creates data/{taxi_type}/
    - For years 2019–2020, months 1–12: downloads CSV.gz if Parquet does not exist.
    - Converts CSV.gz to Parquet (no DuckDB); normalizes column names to lowercase for BigQuery/dbt.
    - Casts yellow/green columns to INT64/FLOAT64/TIMESTAMP so BigQuery load matches schema.
    - Removes CSV.gz after conversion.
    - If green load fails with BYTE_ARRAY/timestamp mismatch, remove data/green and re-run.
    """
    data_dir = SCRIPT_DIR / "data" / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    for year in [2019, 2020]:
        for month in range(1, 13):
            parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = data_dir / parquet_filename

            if parquet_filepath.exists():
                print(f"Skipping {parquet_filename} (already exists)")
                continue

            csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = data_dir / csv_gz_filename

            url = f"{BASE_URL}/{taxi_type}/{csv_gz_filename}"
            print(f"Downloading {csv_gz_filename}...")
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(csv_gz_filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"Converting {csv_gz_filename} to Parquet...")
            df = pd.read_csv(csv_gz_filepath, compression="gzip", low_memory=False)
            # Normalize column names to lowercase for BigQuery/dbt
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
            if taxi_type == "yellow":
                df = _cast_yellow_df_for_parquet(df)
            elif taxi_type == "green":
                df = _cast_green_df_for_parquet(df)
            df.to_parquet(parquet_filepath, index=False)
            csv_gz_filepath.unlink()
            print(f"Completed {parquet_filename}")


def update_gitignore() -> None:
    """Ensures data/ is in .gitignore."""
    gitignore_path = SCRIPT_DIR / ".gitignore"
    content = gitignore_path.read_text() if gitignore_path.exists() else ""
    if "data/" not in content:
        with open(gitignore_path, "a") as f:
            f.write("\n# Data directory\ndata/\n" if content else "# Data directory\ndata/\n")


def create_bucket_if_needed(client: storage.Client, bucket_name: str) -> storage.Bucket:
    """Creates the GCS bucket if it does not exist; exits if name is taken by another project."""
    try:
        bucket = client.get_bucket(bucket_name)
        project_buckets = [b.id for b in client.list_buckets()]
        if bucket_name in project_buckets:
            print(f"Bucket '{bucket_name}' exists and belongs to your project.")
        else:
            print(f"Bucket '{bucket_name}' exists but does not belong to your project.")
            sys.exit(1)
        return bucket
    except NotFound:
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
        return bucket
    except Forbidden:
        print(f"Bucket '{bucket_name}' exists but is not accessible (name taken).")
        sys.exit(1)


def upload_to_gcs(
    client: storage.Client,
    bucket: storage.Bucket,
    file_path: str,
    gcs_prefix: str = "",
    max_retries: int = 3,
) -> None:
    """Uploads a single file to GCS with retries and verification."""
    path = Path(file_path)
    blob_name = f"{gcs_prefix}{path.name}".lstrip("/") if gcs_prefix else path.name
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {path.name} to gs://{bucket.name}/{blob_name} (attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            if blob.exists(client):
                print(f"Uploaded and verified: gs://{bucket.name}/{blob_name}")
                return
            print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Upload failed: {e}")
        time.sleep(5)
    print(f"Giving up on {file_path} after {max_retries} attempts.")
    sys.exit(1)


def validate_parquet_columns(taxi_type: str, required_columns: list[str]) -> bool:
    """
    Checks that at least one Parquet file under data/{taxi_type}/ has all required columns.
    Uses script-directory paths. Tries multiple files if one is corrupted or not Parquet.
    Returns True if validation passes.
    """
    data_dir = SCRIPT_DIR / "data" / taxi_type
    parquet_files = sorted(data_dir.glob("*.parquet"))
    if not parquet_files:
        print(f"No Parquet files found in {data_dir}. Run download/convert first.")
        return False

    for sample in parquet_files:
        path_str = str(sample.resolve())
        try:
            df = pd.read_parquet(path_str, engine="pyarrow")
        except Exception as e:
            print(f"Warning: Could not read {sample.name} ({e}), trying next file.")
            continue
        missing = [c for c in required_columns if c not in df.columns]
        if missing:
            print(f"Validation failed for {sample.name}: Parquet columns do not match BigQuery expectation.")
            print(f"  Missing: {missing}")
            print(f"  Parquet columns: {list(df.columns)}")
            continue
        print(f"Column validation passed for {taxi_type} (checked {sample.name}).")
        return True

    print("Validation failed: no Parquet file had all required columns. Delete data/ and re-run to re-download and convert with pandas.")
    return False


def load_parquet_from_gcs_to_bigquery(
    bq_client: bigquery.Client,
    bucket_name: str,
    gcs_prefix: str,
    project: str,
    dataset: str,
    table_id: str,
    schema: list,
    write_disposition: str = "WRITE_TRUNCATE",
) -> None:
    """
    Loads Parquet files from GCS into a BigQuery table with the given schema.
    gcs_prefix is e.g. 'yellow/' for gs://bucket/yellow/*.parquet.
    """
    uri = f"gs://{bucket_name}/{gcs_prefix}*.parquet"
    table_ref = f"{project}.{dataset}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=schema,
        write_disposition=write_disposition,
    )

    load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
    print(f"BigQuery load job started: {load_job.job_id}")
    load_job.result()
    print(f"Loaded into {table_ref}.")


if __name__ == "__main__":
    update_gitignore()

    # 1) Download and convert to Parquet (no DuckDB)
    for taxi_type in ["yellow", "green"]:
        download_and_convert_to_parquet(taxi_type)

    # 2) GCS: create bucket and upload Parquet files
    gcs_client = _get_gcs_client()
    bucket = create_bucket_if_needed(gcs_client, BUCKET_NAME)

    file_paths = []
    for taxi_type in ["yellow", "green"]:
        data_dir = SCRIPT_DIR / "data" / taxi_type
        if data_dir.exists():
            file_paths.extend([(taxi_type, str(p.resolve())) for p in data_dir.glob("*.parquet")])

    def _upload_one(item):
        taxi_type, file_path = item
        upload_to_gcs(gcs_client, bucket, file_path, gcs_prefix=f"{taxi_type}/")

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(_upload_one, file_paths))

    # 3) Validate columns before creating/overwriting BigQuery tables
    if not validate_parquet_columns("yellow", REQUIRED_YELLOW_COLUMNS):
        sys.exit(1)
    if not validate_parquet_columns("green", REQUIRED_GREEN_COLUMNS):
        sys.exit(1)

    # 4) Load yellow and green data into BigQuery (table names match sources.yml identifiers)
    bq_client = _get_bq_client()
    dataset_ref = f"{GCP_PROJECT}.{BQ_DATASET}"
    try:
        bq_client.get_dataset(dataset_ref)
    except NotFound:
        bq_client.create_dataset(bigquery.Dataset(dataset_ref))

    load_parquet_from_gcs_to_bigquery(
        bq_client,
        BUCKET_NAME,
        "yellow/",
        GCP_PROJECT,
        BQ_DATASET,
        "yellow_tripdata_non_partitioned",
        BQ_YELLOW_SCHEMA,
    )

    load_parquet_from_gcs_to_bigquery(
        bq_client,
        BUCKET_NAME,
        "green/",
        GCP_PROJECT,
        BQ_DATASET,
        "green_tripdata_non_partitioned",
        BQ_GREEN_SCHEMA,
    )

    print("Ingestion complete: data downloaded, converted, uploaded to GCS, and loaded into BigQuery (yellow + green).")