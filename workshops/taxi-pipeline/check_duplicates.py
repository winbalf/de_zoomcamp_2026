"""Check for duplicate trip records in the pipeline's DuckDB after a run."""
import os

import duckdb

import dlt

pipeline = dlt.pipeline(pipeline_name="taxi_pipeline", destination="duckdb")

# Prefer DB in script dir (same place pipeline creates it when run from project); then cwd; then pipelines_dir.
script_dir = os.path.dirname(os.path.abspath(__file__))
duck_path = None
for candidate in [
    os.path.join(script_dir, "taxi_pipeline.duckdb"),
    os.path.join(os.getcwd(), "taxi_pipeline.duckdb"),
]:
    if os.path.isfile(candidate):
        duck_path = candidate
        break
if not duck_path:
    for root, _dirs, files in os.walk(pipeline.pipelines_dir):
        for f in files:
            if f.endswith(".duckdb"):
                duck_path = os.path.join(root, f)
                break
        if duck_path:
            break

if not duck_path:
    print("No DuckDB file found. Run the pipeline from this project directory first:")
    print(f"  cd {script_dir}")
    print("  uv run python taxi_pipeline.py")
    exit(1)

conn = duckdb.connect(duck_path, read_only=True)
# List schemas/tables in case table is under a dataset (match case-insensitively)
tables = conn.execute(
    """SELECT table_schema, table_name
       FROM information_schema.tables
       WHERE LOWER(table_name) = 'trips'
       AND table_schema NOT IN ('information_schema', 'pg_catalog')"""
).fetchall()
if not tables:
    # Show what tables exist to help debug (e.g. empty load or different naming)
    all_tables = conn.execute(
        """SELECT table_schema, table_name
           FROM information_schema.tables
           WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
           ORDER BY table_schema, table_name"""
    ).fetchall()
    conn.close()
    if all_tables:
        print("No 'trips' table found. Tables in DuckDB:")
        for sch, tbl in all_tables:
            print(f"  - {sch or '(default)'}.{tbl}")
    else:
        print("No 'trips' table found. DuckDB has no user tables yet.")
    print(f"Database: {duck_path}")
    print("Run the pipeline from this project directory first: uv run python taxi_pipeline.py")
    exit(1)
conn.close()

schema, table = tables[0]
qualified = f'"{schema}"."{table}"' if schema else "trips"

conn = duckdb.connect(duck_path, read_only=True)
r = conn.execute(
    f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT trip_pickup_date_time || '|' || COALESCE(CAST(start_lat AS VARCHAR), '') || '|'
            || COALESCE(CAST(start_lon AS VARCHAR), '') || '|' || trip_dropoff_date_time || '|'
            || COALESCE(CAST(fare_amt AS VARCHAR), '')) AS distinct_rows
    FROM {qualified}
    """
).fetchone()
conn.close()

total, distinct = r[0], r[1]
dupes = total - distinct
print(f"Total rows: {total:,} | Distinct (by trip key): {distinct:,} | Duplicates: {dupes:,}")
if dupes > 0:
    print(
        "-> API is likely re-sending the same records (e.g. offset wraps). "
        "Use MAXIMUM_OFFSET in taxi_pipeline.py to cap the run."
    )
else:
    print("-> No duplicates detected in loaded data.")
