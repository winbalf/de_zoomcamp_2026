# Streaming homework — runbook & answers

This folder has scripts and commands for **Data Engineering Zoomcamp** Module 7 (Kafka / Redpanda, producer & consumer, PyFlink → PostgreSQL) and the **homework quiz answers** for questions 1–6.

---

## Table of contents

| Section | What you’ll find |
|--------|-------------------|
| [Setup & data](#1-setup--data) | Working directory, download parquet, create topic |
| [Producer & consumer](#2-producer--consumer) | Send trips to Kafka; count trips by distance |
| [PostgreSQL & PyFlink](#3-postgresql--pyflink) | Tables, start stack, submit Flink jobs, notes |
| [Troubleshooting](#36-troubleshooting) | Flink `NaN` parse errors, TaskManager, topic reset |
| [Homework answers (Q1–Q6)](#4-homework-questions--verification-sql--answers) | Verification commands, SQL, quiz answers |

---

## 1. Setup & data

### Working directory

```bash
cd 07-streaming/homework
mkdir -p data
```

### Download the Green Taxi parquet (October 2025)

```bash
curl -o data/green_tripdata_2025-10.parquet \
  "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
```

### Create the Kafka topic

```bash
docker exec -it homework-redpanda-1 rpk topic create green-trips
```

**Optional — reset the topic** (same commands as before, kept for convenience):

```bash
docker exec homework-redpanda-1 rpk topic delete green-trips
docker exec -it homework-redpanda-1 rpk topic create green-trips
```

<!--
docker exec homework-redpanda-1 rpk topic delete green-trips
docker exec -it homework-redpanda-1 rpk topic create green-trips 
-->

---

## 2. Producer & consumer

### Producer — send all rows to `green-trips`

```bash
uv run python src/producers/producer_green_trips.py \
  --parquet-path data/green_tripdata_2025-10.parquet \
  --bootstrap-server localhost:9092 \
  --topic green-trips
```

### Consumer — count trips with `trip_distance` above a threshold

```bash
uv run python src/consumers/consumer_green_trips.py \
  --bootstrap-server localhost:9092 \
  --topic green-trips \
  --threshold-km 5.0
```

---

## 3. PostgreSQL & PyFlink

### 3.1 Create PostgreSQL tables (run once before Flink)

Start Postgres, then apply the DDL (use `-T` when piping/heredoc stdin — avoids *"the input device is not a TTY"*).

```bash
docker compose up postgres -d
docker compose exec -T postgres psql -U postgres -d postgres <<'SQL'
CREATE TABLE processed_events (
    PULocationID INTEGER,
    DOLocationID INTEGER,
    trip_distance DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    pickup_datetime TIMESTAMP
);

CREATE TABLE processed_events_aggregated (
    window_start TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (window_start, PULocationID)
);

-- Question 4: 5-minute tumbling windows, trips per pickup location
CREATE TABLE q4_tumbling_5min_pickup (
    window_start TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);

-- Question 5: session windows (5-minute gap) per PULocationID
CREATE TABLE q5_session_pickup (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (window_start, window_end, PULocationID)
);

-- Question 6: 1-hour tumbling windows, total tips per hour
CREATE TABLE q6_hourly_tips (
    window_start TIMESTAMP,
    total_tip_amount DOUBLE PRECISION,
    PRIMARY KEY (window_start)
);
SQL
```

### 3.2 Start Flink (JobManager + TaskManager)

```bash
docker compose up jobmanager taskmanager -d
```

### 3.3 Submit Flink jobs

**Pass-through** (writes into `processed_events`):

```bash
docker exec -it homework-jobmanager-1 flink run -py /opt/src/job/pass_through_job.py
```

**Aggregation** (writes into `processed_events_aggregated`):

```bash
docker exec -it homework-jobmanager-1 flink run -py /opt/src/job/aggregation_job.py
```

**Question 4** — 5 min tumbling, count per `PULocationID` → table `q4_tumbling_5min_pickup`:

```bash
docker exec -it homework-jobmanager-1 flink run -py /opt/src/job/q4_tumbling_5min_pickup.py
```

**Question 5** — session window, 5 min gap on `PULocationID` → table `q5_session_pickup`:

```bash
docker exec -it homework-jobmanager-1 flink run -py /opt/src/job/q5_session_5min_pickup.py
```

**Question 6** — 1 h tumbling, sum `tip_amount` → table `q6_hourly_tips`:

```bash
docker exec -it homework-jobmanager-1 flink run -py /opt/src/job/q6_tumbling_1h_tips.py
```

### 3.4 After the jobs run

- Let streaming jobs run until checkpoints flush (**~1–2 minutes**), then query PostgreSQL, for example:

  ```bash
  docker compose exec postgres psql -U postgres -d postgres -c "SELECT * FROM processed_events LIMIT 10;"
  ```

- If you re-send Kafka data multiple times, **delete & recreate** the topic to avoid duplicates:

  ```bash
  docker exec -it homework-redpanda-1 rpk topic delete green-trips
  docker exec -it homework-redpanda-1 rpk topic create green-trips
  ```

### 3.5 Quick copy-paste — Flink Q4–Q6 only

Same three `flink run` lines as in §3.3, kept here for fast reruns:

```bash
docker exec -it homework-jobmanager-1 flink run -py /opt/src/job/q4_tumbling_5min_pickup.py
docker exec -it homework-jobmanager-1 flink run -py /opt/src/job/q5_session_5min_pickup.py
docker exec -it homework-jobmanager-1 flink run -py /opt/src/job/q6_tumbling_1h_tips.py
```

---

## 4. Homework questions — verification SQL & answers

Official wording from the course homework; commands below use **this repo’s** container names (`homework-redpanda-1`, `homework-jobmanager-1`) and paths (`src/job/` → `/opt/src/job/` in Flink).

---

### Question 1. Redpanda version

**Question**

Run `rpk version` inside the Redpanda container:

```bash
docker exec -it homework-redpanda-1 rpk version
```

What version of Redpanda are you running?

**Answer**

**`v25.3.9`**

**Explanation**

Matches the image in `docker-compose.yaml` (`redpandadata/redpanda:v25.3.9`). `rpk version` prints `rpk version: v25.3.9`.

---

### Question 2. Sending data to Redpanda

**Question**

Create a topic called `green-trips`:

```bash
docker exec -it homework-redpanda-1 rpk topic create green-trips
```

Now write a producer to send the green taxi data to this topic.

Read the parquet file and keep only these columns:

- `lpep_pickup_datetime`
- `lpep_dropoff_datetime`
- `PULocationID`
- `DOLocationID`
- `passenger_count`
- `trip_distance`
- `tip_amount`
- `total_amount`

Convert each row to a dictionary and send it to the `green-trips` topic. You’ll need to handle the datetime columns — convert them to strings before serializing to JSON.

Measure the time it takes to send the entire dataset and flush:

```python
from time import time

t0 = time()

# send all rows ...

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
```

How long did it take to send the data?

**Options**

- 10 seconds
- 60 seconds
- 120 seconds
- 300 seconds

**Answer (this repo)**

**`10 seconds`** (quiz option). The homework producer prints `took … seconds` after `producer.flush()`.

**Explanation**

A typical local Docker run finishes in a few seconds for ~49k rows (for example **~2–3 s**); that falls in the **10 seconds** bucket, not 60 / 120 / 300. If your run prints a different time, pick the bucket that contains it.

---

### Question 3. Consumer — trip distance

**Question**

Write a Kafka consumer that reads all messages from the `green-trips` topic (set `auto_offset_reset='earliest'`).

Count how many trips have a `trip_distance` greater than 5.0 kilometers.

How many trips have `trip_distance` > 5?

**Options**

- 6506
- 7506
- 8506
- 9506

**Verification (this repo)**

```bash
uv run python src/consumers/consumer_green_trips.py \
  --bootstrap-server localhost:9092 \
  --topic green-trips \
  --threshold-km 5.0
```

**Answer**

**`8506`**

**Explanation**

Same count as `(trip_distance > 5.0)` on **`green_tripdata_2025-10.parquet`**.

---

### Part 2: PyFlink (Questions 4–6)

**Context (from homework)**

For the PyFlink questions, you’ll adapt the workshop code to work with the green taxi data. The key differences from the workshop:

- Topic name: `green-trips` (instead of `rides`)
- Datetime columns use `lpep_` prefix (instead of `tpep_`)
- You’ll need to handle timestamps as strings (not epoch milliseconds)

You can convert string timestamps to Flink timestamps in your source DDL:

```sql
lpep_pickup_datetime VARCHAR,
event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
```

Before running the Flink jobs, create the necessary PostgreSQL tables for your results.

**Important notes for the Flink jobs**

- Place your job files in **`src/job/`** in this repo — the directory is mounted into the Flink containers at **`/opt/src/job/`**
- Submit jobs with:  
  `docker exec -it homework-jobmanager-1 flink run -py /opt/src/job/your_job.py`
- The `green-trips` topic has **1 partition**, so set **parallelism to 1** in your Flink jobs (`env.set_parallelism(1)`). With higher parallelism, idle consumer subtasks prevent the watermark from advancing.
- Flink streaming jobs run continuously. Let the job run for a minute or two until results appear in PostgreSQL, then query the results. You can cancel the job from the Flink UI at http://localhost:8081
- If you sent data to the topic multiple times, delete and recreate the topic to avoid duplicates:  
  `docker exec -it homework-redpanda-1 rpk topic delete green-trips`

Results below are **precomputed** on **`green_tripdata_2025-10.parquet`**. Run the matching Flink job, then query Postgres to confirm.

---

### Question 4. Tumbling window — pickup location

**Question**

Create a Flink job that reads from `green-trips` and uses a **5-minute tumbling window** to count trips per `PULocationID`.

Write the results to a PostgreSQL table with columns: `window_start`, `PULocationID`, `num_trips`.

After the job processes all data, query the results:

```sql
SELECT PULocationID, num_trips
FROM <your_table>
ORDER BY num_trips DESC
LIMIT 3;
```

Which `PULocationID` had the most trips in a single 5-minute window?

**Options**

- 42
- 74
- 75
- 166

**Verification (this repo — table `q4_tumbling_5min_pickup`)**

```sql
SELECT PULocationID, num_trips
FROM q4_tumbling_5min_pickup
ORDER BY num_trips DESC
LIMIT 3;
```

**Answer**

**`74`** — **`PULocationID = 74`** had the most trips in a single 5-minute window.

---

### Question 5. Session window — longest streak

**Question**

Create another Flink job that uses a **session window** with a **5-minute gap** on `PULocationID`, using `lpep_pickup_datetime` as the event time with a **5-second watermark** tolerance.

A session window groups events that arrive within 5 minutes of each other. When there’s a gap of more than 5 minutes, the window closes.

Write the results to a PostgreSQL table and find the `PULocationID` with the longest session (most trips in a single session).

How many trips were in the longest session?

**Options**

- 12
- 31
- 51
- 81

**Verification (this repo — table `q5_session_pickup`)**

```sql
SELECT PULocationID, num_trips
FROM q5_session_pickup
ORDER BY num_trips DESC
LIMIT 5;
```

**Answer**

**`81`** (multiple choice).

**Explanation**

On this dataset, the largest session has **`COUNT(*) = 82`** trips (Flink counts whole rows). The quiz option **`81`** matches the number of **distinct** `lpep_pickup_datetime` values in that same session (two trips share the same timestamp). Use **`81`** for the multiple-choice if required; otherwise trust **`MAX(num_trips)`** from Postgres after the job finishes.

---

### Question 6. Tumbling window — largest tip

**Question**

Create a Flink job that uses a **1-hour tumbling window** to compute the total `tip_amount` per hour (across all locations).

Which hour had the highest total tip amount?

**Options**

- 2025-10-01 18:00:00
- 2025-10-16 18:00:00
- 2025-10-22 08:00:00
- 2025-10-30 16:00:00

**Verification (this repo — table `q6_hourly_tips`)**

```sql
SELECT window_start, total_tip_amount
FROM q6_hourly_tips
ORDER BY total_tip_amount DESC
LIMIT 5;
```

**Answer**

**`2025-10-16 18:00:00`**

**Explanation**

The hour with the highest total **`tip_amount`** is **`2025-10-16 18:00:00`**, aligned with event-time hours from `lpep_pickup_datetime`.

