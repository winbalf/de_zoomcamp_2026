#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import os


def ingest_data(
        file_path: str,
        engine,
        target_table: str,
        chunksize: int = 100000,
) -> pd.DataFrame:
    """
    Ingest parquet or CSV data into PostgreSQL database.
    
    Args:
        file_path: Path to the parquet or CSV file
        engine: SQLAlchemy engine
        target_table: Target table name
        chunksize: Number of rows per chunk (for large files)
    """
    # Detect file type
    file_ext = os.path.splitext(file_path)[1].lower()
    
    if file_ext == '.parquet':
        # Read parquet file
        df = pd.read_parquet(file_path, engine='pyarrow')
    elif file_ext == '.csv':
        # Read CSV file
        df = pd.read_csv(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_ext}. Supported formats: .parquet, .csv")
    
    # Get total number of rows
    total_rows = len(df)
    print(f"Total rows to ingest: {total_rows}")
    
    # For small files, ingest directly without chunking
    if total_rows <= chunksize:
        df.to_sql(
            name=target_table,
            con=engine,
            if_exists="replace",
            index=False
        )
        print(f"Table {target_table} created and populated with {total_rows} rows")
        print(f'Done ingesting {total_rows} rows to {target_table}')
        return
    
    # For large files, process in chunks
    # Create table schema from first chunk
    first_chunk = df.head(0)
    first_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="replace"
    )
    
    print(f"Table {target_table} created")
    
    # Process data in chunks
    num_chunks = (total_rows // chunksize) + (1 if total_rows % chunksize else 0)
    
    for i in tqdm(range(num_chunks), desc="Ingesting chunks"):
        start_idx = i * chunksize
        end_idx = min((i + 1) * chunksize, total_rows)
        df_chunk = df.iloc[start_idx:end_idx]
        
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False
        )
        print(f"Inserted chunk {i+1}/{num_chunks}: {len(df_chunk)} rows")
    
    print(f'Done ingesting {total_rows} rows to {target_table}')

@click.command()
@click.option('--pg-user', default='postgres', help='PostgreSQL username')
@click.option('--pg-pass', default='postgres', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--file-path', required=True, help='Path to the parquet or CSV file')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')
@click.option('--target-table', default='green_taxi_data', help='Target table name')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, file_path, chunksize, target_table):
    """Ingest NY Taxi data (parquet or CSV) into PostgreSQL."""
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    ingest_data(
        file_path=file_path,
        engine=engine,
        target_table=target_table,
        chunksize=chunksize
    )

if __name__ == '__main__':
    main()