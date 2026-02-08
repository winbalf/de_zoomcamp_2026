# Data Warehouse - Yellow Taxi Data

## Files

- `load_yellow_taxi_data.py` - Downloads and uploads yellow taxi data to GCS
- `gcs.json` - GCP service account credentials

## Setup

### Install dependencies:
   ```bash
   pip install google-cloud-storage
   ```

### Ensure `gcs.json` credentials file is in the same directory

### Load Data to GCS

```bash
python3 load_yellow_taxi_data.py
```

This will:
- Download yellow taxi parquet files for months 01-06 of 2024
- Upload them to the GCS bucket


## Project Details

- **Project ID**: `cool-agility-485919-g8`
- **GCS Bucket**: `kestra-zoomcamp-edwin-demo-2219`
- **Dataset**: `zoomcamp`
- **Table**: `external_yellow_tripdata`

-----------------------------------------------------------------------------------

### Question 1 - Counting records

#### 1. What is count of records for the 2024 Yellow Taxi Data?

```sql
SELECT COUNT(*)
FROM `cool-agility-485919-g8.zoomcamp.yellow_tripdata_non_partitioned`;
```

**Ans:** 20,332,093


### Question 2 - Data read estimation

#### 2. Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

```sql

SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocationids
FROM `cool-agility-485919-g8.zoomcamp.external_yellow_tripdata`;

SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocationids
FROM `cool-agility-485919-g8.zoomcamp.yellow_tripdata_non_partitioned`; 
```

**Ans:** 0 MB for the External Table and 155.12 MB for the Materialized Table

### Question 3 - Understanding columnar storage

#### 3. Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

```sql
-- Query 1: Retrieve only PULocationID
SELECT PULocationID
FROM `cool-agility-485919-g8.zoomcamp.yellow_tripdata_non_partitioned`;

-- Query 2: Retrieve both PULocationID and DOLocationID
SELECT PULocationID, DOLocationID
FROM `cool-agility-485919-g8.zoomcamp.yellow_tripdata_non_partitioned`;
```

**Ans:** BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed. 

### Question 4 - Counting zero fare trips

#### 4. How many records have a fare_amount of 0?

```sql
SELECT COUNT(*) AS records_with_zero_fare
FROM `cool-agility-485919-g8.zoomcamp.yellow_tripdata_non_partitioned`
WHERE fare_amount = 0;
```

**Ans:** 8,333

### Question 5 - Partitioning and clustering

#### 5. What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

```sql
CREATE OR REPLACE TABLE `cool-agility-485919-g8.zoomcamp.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `cool-agility-485919-g8.zoomcamp.yellow_tripdata_non_partitioned`;
```

**Ans:** Partition by tpep_dropoff_datetime and Cluster on VendorID 

### Question 6 - Partition benefits

#### 6. Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive). Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? 

```sql
-- Query 1: Using the non-partitioned materialized table
SELECT DISTINCT VendorID
FROM `cool-agility-485919-g8.zoomcamp.yellow_tripdata_non_partitioned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Query 2: Using the partitioned and clustered table
SELECT DISTINCT VendorID
FROM `cool-agility-485919-g8.zoomcamp.yellow_tripdata_partitioned_clustered`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```

**Expected Results:**
- **Non-partitioned table** (`yellow_tripdata_non_partitioned`): Will process a larger amount of data because BigQuery needs to scan the entire table (or a significant portion) to filter rows by the date range, even though only a subset of dates are needed.

- **Partitioned table** (`yellow_tripdata_partitioned_clustered`): Will process significantly fewer bytes because:
  1. **Partitioning** by `DATE(tpep_dropoff_datetime)` allows BigQuery to skip partitions outside the date range (2024-03-01 to 2024-03-15), only scanning the relevant partitions (March 1-15).
  2. **Clustering** by `VendorID` helps organize data within partitions, making the DISTINCT operation more efficient.

The partitioned table should show a substantial reduction in bytes processed compared to the non-partitioned table, demonstrating the performance benefits of partitioning when filtering by the partition column.

**Ans:** 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

### Question 7 - External table storage

#### 7. Where is the data stored in the External Table you created?

**Ans:** **GCP Bucket**

The data in the External Table (`external_yellow_tripdata`) is stored in **Google Cloud Storage (GCS)**. 

Unlike materialized tables (like `yellow_tripdata_non_partitioned`), external tables do not store data in BigQuery's native storage. Instead, the data remains in the original location (GCS in this case), and BigQuery reads directly from GCS when queries are executed. The external table definition in BigQuery only contains metadata that points to the GCS location where the actual parquet files are stored.

### Question 8 - Clustering best practices

#### 8. It is best practice in Big Query to always cluster your data:

**Ans:** **False**

Clustering is not always best practice in BigQuery. It should be used strategically based on your specific use case:

**When clustering is beneficial:**
- Large tables (typically > 1GB) where clustering can significantly reduce bytes scanned
- Frequent filtering or aggregation by the clustering columns
- Query patterns that align with the clustering columns
- When combined with partitioning for optimal performance

**When clustering may not be necessary:**
- Small tables where the overhead may outweigh benefits
- Tables with diverse query patterns that don't align with specific columns
- Tables that are already well-optimized with partitioning alone
- Cost-sensitive scenarios where the maintenance overhead isn't justified

Clustering is a powerful optimization tool, but it should be applied thoughtfully based on your query patterns, table size, and performance requirements rather than as a blanket best practice.

### Question 9 - Understanding table scans

#### 9. Write a **SELECT COUNT(*)** query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

```sql
SELECT COUNT(*)
FROM `cool-agility-485919-g8.zoomcamp.yellow_tripdata_non_partitioned`;
```

**Ans:** The query will estimate reading **0 B** (or a very small amount, typically less than 1 MB). 

**Why:** BigQuery maintains row count metadata for materialized tables. When you run `COUNT(*)` without a WHERE clause, BigQuery can use this metadata to return the exact row count without scanning the actual data columns. Since BigQuery is a columnar database, it doesn't need to read all columns to count rows - it can use the table's metadata statistics. This is one of the key optimizations that makes COUNT(*) queries very efficient on materialized tables in BigQuery. 


