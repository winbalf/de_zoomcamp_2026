import os
import sys
import urllib.request
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time


# Change this to your bucket name (must be globally unique; e.g. include project ID)
BUCKET_NAME = "taxi-zoomcamp-cool-agility-485919-g8"

# If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "keys/gcs2_bucket.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
# If commented initialize client with the following
# client = storage.Client(project='zoomcamp-mod3-datawarehouse')


# BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-"BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
MONTHS = [f"{i:02d}" for i in range(1, 7)]
DOWNLOAD_DIR = "./data/yellow"

CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def download_and_convert_files(taxi_type):
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    for year in [2019, 2020]:
        for month in range(1, 13):
            parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = data_dir / parquet_filename

            if parquet_filepath.exists():
                print(f"Skipping {parquet_filename} (already exists)")
                continue

            # Download CSV.gz file
            csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = data_dir / csv_gz_filename

            response = requests.get(f"{BASE_URL}/{taxi_type}/{csv_gz_filename}", stream=True)
            response.raise_for_status()

            with open(csv_gz_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"Converting {csv_gz_filename} to Parquet...")
            con = duckdb.connect()
            con.execute(f"""
                COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath}'))
                TO '{parquet_filepath}' (FORMAT PARQUET)
            """)
            con.close()

            # Remove the CSV.gz file to save space
            csv_gz_filepath.unlink()
            print(f"Completed {parquet_filename}")

def update_gitignore():
    gitignore_path = Path(".gitignore")

    # Read existing content or start with empty string
    content = gitignore_path.read_text() if gitignore_path.exists() else ""

    # Add data/ if not already present
    if 'data/' not in content:
        with open(gitignore_path, 'a') as f:
            f.write('\n# Data directory\ndata/\n' if content else '# Data directory\ndata/\n')


def create_bucket(bucket_name):
    try:
        # Get bucket details
        bucket = client.get_bucket(bucket_name)

        # Check if the bucket belongs to the current project
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            print(
                f"Bucket '{bucket_name}' exists and belongs to your project. Proceeding..."
            )
        else:
            print(
                f"A bucket with the name '{bucket_name}' already exists, but it does not belong to your project."
            )
            sys.exit(1)

    except NotFound:
        # If the bucket doesn't exist, create it
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        # If the request is forbidden, it means the bucket exists but you don't have access to see details
        print(
            f"A bucket with the name '{bucket_name}' exists, but it is not accessible. Bucket name is taken. Please try a different bucket name."
        )
        sys.exit(1)


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    create_bucket(BUCKET_NAME)

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")

def update_gitignore():
    gitignore_path = Path(".gitignore")

    # Read existing content or start with empty string
    content = gitignore_path.read_text() if gitignore_path.exists() else ""

    # Add data/ if not already present
    if 'data/' not in content:
        with open(gitignore_path, 'a') as f:
            f.write('\n# Data directory\ndata/\n' if content else '# Data directory\ndata/\n')

if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    # update_gitignore()

    for taxi_type in ["yellow", "green"]:
        download_and_convert_files(taxi_type)

    # Collect all parquet files under data/yellow and data/green for upload
    file_paths = []
    for taxi_type in ["yellow", "green"]:
        data_dir = Path("data") / taxi_type
        if data_dir.exists():
            file_paths.extend(str(p) for p in data_dir.glob("*.parquet"))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

    print("All files processed and verified.")