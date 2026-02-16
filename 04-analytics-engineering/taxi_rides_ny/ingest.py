"""
Data Ingestion Script for NYC Taxi Data

This script downloads NYC taxi trip data (yellow and green taxis) from GitHub releases,
converts the CSV.gz files to Parquet format for efficient storage and querying,
and loads the data into a DuckDB database for analytics.

The script processes data for years 2019 and 2020, with 12 months of data per year.
"""

import duckdb
import requests
from pathlib import Path

# Base URL for downloading NYC TLC taxi data from GitHub releases
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"


def download_and_convert_files(taxi_type):
    """
    Downloads CSV.gz files for a given taxi type and converts them to Parquet format.
    
    This function:
    1. Creates a data directory structure (data/{taxi_type}/)
    2. Iterates through years 2019-2020 and months 1-12
    3. Downloads CSV.gz files if the Parquet version doesn't already exist
    4. Converts CSV.gz to Parquet using DuckDB for better performance
    5. Deletes the CSV.gz file after conversion to save disk space
    
    Args:
        taxi_type (str): Type of taxi data to download (e.g., "yellow", "green")
    """
    # Create directory structure: data/{taxi_type}/
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    # Process data for years 2019 and 2020
    for year in [2019, 2020]:
        # Process all 12 months for each year
        for month in range(1, 13):
            # Construct expected Parquet filename (e.g., yellow_tripdata_2019-01.parquet)
            parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = data_dir / parquet_filename

            # Skip download if Parquet file already exists (resume capability)
            if parquet_filepath.exists():
                print(f"Skipping {parquet_filename} (already exists)")
                continue

            # Download CSV.gz file from GitHub releases
            csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = data_dir / csv_gz_filename

            # Stream download to handle large files efficiently
            response = requests.get(f"{BASE_URL}/{taxi_type}/{csv_gz_filename}", stream=True)
            response.raise_for_status()  # Raise an exception for bad status codes

            # Write downloaded file in chunks to manage memory usage
            with open(csv_gz_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Convert CSV.gz to Parquet format using DuckDB
            # Parquet is more efficient for analytics: columnar storage, compression, type preservation
            print(f"Converting {csv_gz_filename} to Parquet...")
            con = duckdb.connect()
            con.execute(f"""
                COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath}'))
                TO '{parquet_filepath}' (FORMAT PARQUET)
            """)
            con.close()

            # Remove the CSV.gz file after conversion to save disk space
            csv_gz_filepath.unlink()
            print(f"Completed {parquet_filename}")


def update_gitignore():
    """
    Updates the .gitignore file to exclude the data directory from version control.
    
    This prevents large data files from being committed to the repository.
    The function checks if 'data/' is already in .gitignore before adding it.
    """
    gitignore_path = Path(".gitignore")

    # Read existing .gitignore content, or start with empty string if file doesn't exist
    content = gitignore_path.read_text() if gitignore_path.exists() else ""

    # Add data/ directory to .gitignore if it's not already present
    if 'data/' not in content:
        with open(gitignore_path, 'a') as f:
            # Add newline before entry if file already has content, otherwise just add the entry
            f.write('\n# Data directory\ndata/\n' if content else '# Data directory\ndata/\n')


if __name__ == "__main__":
    """
    Main execution block that orchestrates the data ingestion process:
    1. Updates .gitignore to exclude data directory
    2. Downloads and converts data for yellow and green taxis
    3. Creates DuckDB database and schema
    4. Loads all Parquet files into DuckDB tables for querying
    """
    # Ensure data directory is excluded from version control
    update_gitignore()

    # Download and convert data for both yellow and green taxi types
    for taxi_type in ["yellow", "green"]:
        download_and_convert_files(taxi_type)

    # Connect to DuckDB database (creates file if it doesn't exist)
    con = duckdb.connect("taxi_rides_ny.duckdb")
    
    # Create a 'prod' schema to organize production tables
    con.execute("CREATE SCHEMA IF NOT EXISTS prod")

    # Load all Parquet files for each taxi type into DuckDB tables
    # union_by_name=true allows reading multiple Parquet files with potentially different schemas
    for taxi_type in ["yellow", "green"]:
        con.execute(f"""
            CREATE OR REPLACE TABLE prod.{taxi_type}_tripdata AS
            SELECT * FROM read_parquet('data/{taxi_type}/*.parquet', union_by_name=true)
        """)

    # Close the database connection
    con.close()