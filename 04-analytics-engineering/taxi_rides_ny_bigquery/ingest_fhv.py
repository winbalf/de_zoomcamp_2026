"""
Data Ingestion Script for NYC FHV (For-Hire Vehicle) Trip Data → BigQuery

This script downloads NYC FHV trip data from GitHub releases,
converts CSV.gz files to Parquet using pandas/pyarrow, uploads Parquet to GCS,
validates column and type compatibility with the target BigQuery schema,
and loads the data into BigQuery for use by stg_fhv_tripdata.sql.

Uses the same GCP/BigQuery setup as ingest.py (yellow/green). Target table: fhv_tripdata.
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

# Base URL for NYC TLC FHV data from GitHub releases
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

# All data paths are relative to the script directory
SCRIPT_DIR = Path(__file__).resolve().parent

# GCP config (override via env or set here; should match ingest.py)
BUCKET_NAME = os.environ.get("GCS_BUCKET", "taxi-zoomcamp-cool-agility-485919-g8")
GCP_PROJECT = os.environ.get("GCP_PROJECT", "cool-agility-485919-g8")
BQ_DATASET = os.environ.get("BQ_DATASET", "zoomcamp")
CREDENTIALS_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "keys/gcs2_bucket.json")

CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB for GCS uploads

# FHV release tag and table name (sources.yml: fhv_tripdata -> identifier fhv_tripdata)
FHV_RELEASE_TAG = "fhv"
BQ_FHV_TABLE = "fhv_tripdata"

# Expected raw columns for stg_fhv_tripdata (lowercase names after normalization)
REQUIRED_FHV_COLUMNS = [
    "dispatching_base_num",
    "pickup_datetime",
    "dropoff_datetime",
    "pulocationid",
    "dolocationid",
]

# BigQuery schema for fhv_tripdata (matches dbt source and stg_fhv_tripdata casts)
BQ_FHV_SCHEMA = [
    bigquery.SchemaField("dispatching_base_num", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pickup_datetime", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("dropoff_datetime", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("pulocationid", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("dolocationid", "INTEGER", mode="NULLABLE"),
]

# Optional column in some FHV files (shared ride flag); include if present
_FHV_INT_COLUMNS = ["pulocationid", "dolocationid"]
_FHV_DATETIME_COLUMNS = ["pickup_datetime", "dropoff_datetime"]

# Normalize possible CSV header variants to our expected names
_FHV_COLUMN_ALIASES = {
    "drop_off_datetime": "dropoff_datetime",
    "pickup_date": "pickup_datetime",
    "dropoff_date": "dropoff_datetime",
}


def _get_gcs_client():
    if os.path.exists(CREDENTIALS_FILE):
        return storage.Client.from_service_account_json(CREDENTIALS_FILE)
    return storage.Client(project=GCP_PROJECT)


def _get_bq_client():
    if os.path.exists(CREDENTIALS_FILE):
        return bigquery.Client.from_service_account_json(CREDENTIALS_FILE)
    return bigquery.Client(project=GCP_PROJECT)


def _cast_fhv_df_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """Casts FHV columns to types that write as INT64/TIMESTAMP/STRING in Parquet."""
    df = df.copy()
    for col in _FHV_INT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in _FHV_DATETIME_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").astype("datetime64[us]")
    if "dispatching_base_num" in df.columns:
        df["dispatching_base_num"] = df["dispatching_base_num"].astype("string")
    return df


def download_and_convert_to_parquet() -> None:
    """
    Downloads FHV CSV.gz files and converts them to Parquet.
    Uses 2019 data by default (per homework); optional 2020/2021 via FHV_YEARS env.
    """
    data_dir = SCRIPT_DIR / "data" / FHV_RELEASE_TAG
    data_dir.mkdir(exist_ok=True, parents=True)

    years_str = os.environ.get("FHV_YEARS", "2019")
    years = [int(y.strip()) for y in years_str.split(",")]

    for year in years:
        for month in range(1, 13):
            parquet_filename = f"fhv_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = data_dir / parquet_filename

            if parquet_filepath.exists():
                print(f"Skipping {parquet_filename} (already exists)")
                continue

            csv_gz_filename = f"fhv_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = data_dir / csv_gz_filename

            url = f"{BASE_URL}/{FHV_RELEASE_TAG}/{csv_gz_filename}"
            print(f"Downloading {csv_gz_filename}...")
            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()
            except Exception as e:
                print(f"Skip {csv_gz_filename}: {e}")
                continue

            with open(csv_gz_filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"Converting {csv_gz_filename} to Parquet...")
            df = pd.read_csv(csv_gz_filepath, compression="gzip", low_memory=False)
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
            for old_name, new_name in _FHV_COLUMN_ALIASES.items():
                if old_name in df.columns and new_name not in df.columns:
                    df[new_name] = df[old_name]
            df = _cast_fhv_df_for_parquet(df)
            missing = [c for c in REQUIRED_FHV_COLUMNS if c not in df.columns]
            if missing:
                print(f"Skip {parquet_filename}: CSV missing columns {missing}. Columns: {list(df.columns)}")
                csv_gz_filepath.unlink()
                continue
            df = df[REQUIRED_FHV_COLUMNS]
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


def validate_parquet_columns(required_columns: list[str]) -> bool:
    """Checks that at least one Parquet file under data/fhv/ has all required columns."""
    data_dir = SCRIPT_DIR / "data" / FHV_RELEASE_TAG
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
            print(f"Validation failed for {sample.name}: missing columns {missing}")
            continue
        print(f"Column validation passed for FHV (checked {sample.name}).")
        return True

    print("Validation failed: no Parquet file had all required columns.")
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
    """Loads Parquet files from GCS into a BigQuery table with the given schema."""
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

    # 1) Download and convert FHV to Parquet
    download_and_convert_to_parquet()

    # 2) GCS: create bucket and upload Parquet files
    gcs_client = _get_gcs_client()
    bucket = create_bucket_if_needed(gcs_client, BUCKET_NAME)

    data_dir = SCRIPT_DIR / "data" / FHV_RELEASE_TAG
    file_paths = [(str(p.resolve()),) for p in data_dir.glob("*.parquet")] if data_dir.exists() else []

    def _upload_one(item):
        (file_path,) = item
        upload_to_gcs(gcs_client, bucket, file_path, gcs_prefix=f"{FHV_RELEASE_TAG}/")

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(_upload_one, file_paths))

    if not file_paths:
        print("No Parquet files to upload. Exiting.")
        sys.exit(1)

    # 3) Validate columns before BigQuery load
    if not validate_parquet_columns(REQUIRED_FHV_COLUMNS):
        sys.exit(1)

    # 4) Load FHV data into BigQuery (table name matches sources.yml identifier)
    bq_client = _get_bq_client()
    dataset_ref = f"{GCP_PROJECT}.{BQ_DATASET}"
    try:
        bq_client.get_dataset(dataset_ref)
    except NotFound:
        bq_client.create_dataset(bigquery.Dataset(dataset_ref))

    load_parquet_from_gcs_to_bigquery(
        bq_client,
        BUCKET_NAME,
        f"{FHV_RELEASE_TAG}/",
        GCP_PROJECT,
        BQ_DATASET,
        BQ_FHV_TABLE,
        BQ_FHV_SCHEMA,
    )

    print("FHV ingestion complete: data downloaded, converted, uploaded to GCS, and loaded into BigQuery (fhv_tripdata).")
