# New dbt Project for BigQuery

This guide explains how to add a **new** dbt project that connects to BigQuery while keeping your existing DuckDB project (`taxi_rides_ny`) unchanged.

---

## 1. Create a new dbt project

From the `04-analytics-engineering` directory:

```bash
cd 04-analytics-engineering
dbt init my_bigquery_project
cd my_bigquery_project
```

Or copy your existing `taxi_rides_ny` folder and rename it (e.g. `taxi_rides_ny_bq`) if you want to reuse models and adapt them for BigQuery.

---

## 2. Install the BigQuery adapter

Your current project uses **dbt-duckdb**. BigQuery needs **dbt-bigquery**. You can:

- **Option A – Same environment:** Install both adapters (fine for learning):
  ```bash
  pip install dbt-bigquery
  ```
- **Option B – Separate environment (recommended for clarity):** Use a dedicated virtualenv for the BigQuery project:
  ```bash
  python -m venv .venv_bq
  source .venv_bq/bin/activate   # Linux/macOS
  pip install dbt-core dbt-bigquery
  ```

---

## 3. Configure BigQuery in `profiles.yml`

dbt looks for `~/.dbt/profiles.yml` (or `DBT_PROFILES_DIR`). Add a **new profile** for your BigQuery project (keep your existing `taxi_rides_ny` profile for DuckDB).

### Local development (OAuth via gcloud)

1. Install [Google Cloud SDK](https://cloud.google.com/sdk/docs/install).
2. Log in:
   ```bash
   gcloud auth application-default login
   ```
3. In `~/.dbt/profiles.yml` add (or use `taxi_rides_ny_bigquery/profiles.example.yml` as a template):

```yaml
# Existing DuckDB profile stays as-is (taxi_rides_ny)
# Add a new profile for BigQuery with dev + prod datasets:

taxi_rides_ny_bigquery:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: YOUR_GCP_PROJECT_ID
      dataset: dbt_dev
      threads: 4
    prod:
      type: bigquery
      method: oauth
      project: YOUR_GCP_PROJECT_ID
      dataset: dbt_prod
      threads: 4
```

- **dev** – development dataset (default when you run `dbt run`).
- **prod** – production dataset; run with `dbt run --target prod` or `dbt build --target prod`.

Create both datasets in BigQuery (`dbt_dev` and `dbt_prod`) in your GCP project.

### Service account (CI/CD or production)

```yaml
taxi_rides_ny_bigquery:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: YOUR_GCP_PROJECT_ID
      dataset: dbt_dev
      threads: 4
      keyfile: /path/to/your-service-account-key.json
    prod:
      type: bigquery
      method: service-account
      project: YOUR_GCP_PROJECT_ID
      dataset: dbt_prod
      threads: 4
      keyfile: /path/to/your-service-account-key.json
```

---

## 4. Point the new project to the BigQuery profile

In the **new** project’s `dbt_project.yml`, set the profile name to match the one in `profiles.yml`:

```yaml
name: 'my_bigquery_project'
profile: 'my_bigquery_project'
# ... rest of config
```

---

## 5. GCP prerequisites

- **GCP project** with BigQuery enabled.
- **BigQuery dataset** (e.g. `dbt_dev`) in that project.
- **Permissions** for the account (OAuth or service account):  
  BigQuery User, BigQuery Data Editor.

---

## 6. Verify the connection

From the new project directory:

```bash
dbt debug
```

If the output shows a successful BigQuery connection, run:

```bash
dbt run
dbt test
```

To run against the **production** dataset:

```bash
dbt run --target prod
dbt test --target prod
```

---

## 7. SQL differences (DuckDB vs BigQuery)

If you copy models from `taxi_rides_ny` (DuckDB) to the BigQuery project, watch for:

- **Types:** BigQuery uses `INT64`, `FLOAT64`, `STRING`, `TIMESTAMP`, etc. dbt’s `cast()` and macros usually map these.
- **Functions:** Some DuckDB functions differ (e.g. string/date). Prefer dbt’s cross-database macros or `dbt_utils` when possible.
- **Incrementals:** BigQuery uses `merge` and supports `partition_by` and `cluster_by` in model config.

Example BigQuery-specific config in a model:

```sql
{{
  config(
    materialized='incremental',
    partition_by={'field': 'pickup_datetime', 'data_type': 'timestamp'},
    cluster_by=['pickup_location_id']
  )
}}
```

---

## Troubleshooting

### "Dataset &lt;project&gt;:&lt;dataset&gt; was not found in location US"

dbt runs BigQuery jobs in a **location**. The job location must match where your dataset lives. If the dataset exists but the error still appears, it’s usually a **location mismatch** (e.g. dataset in `us-central1`, job defaulting to multi-region `US`).

**Fix 1 – Set `location` in your profile (recommended when the dataset already exists):**

In `~/.dbt/profiles.yml`, set `location` to the dataset’s **Data location** (e.g. from BigQuery Console → dataset → Details). For a dataset in `us-central1`:

```yaml
taxi_rides_ny_bigquery:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: cool-agility-485919-g8
      dataset: zoomcamp
      location: us-central1   # must match the dataset’s Data location
      threads: 4
```

**Fix 2 – Create the dataset if it doesn’t exist:**

If the target dataset (e.g. `zoomcamp` or `dbt_dev`) doesn’t exist yet, create it in the same location you use in the profile:

```bash
# Example: dataset in us-central1 (match location in profile)
bq --project_id=cool-agility-485919-g8 mk --location=us-central1 zoomcamp
```

---

## Summary

| Step | Action |
|------|--------|
| 1 | Create new project: `dbt init my_bigquery_project` (or copy and rename `taxi_rides_ny`) |
| 2 | Install adapter: `pip install dbt-bigquery` (or in a dedicated venv) |
| 3 | Add BigQuery profile to `~/.dbt/profiles.yml` (oauth or service-account) |
| 4 | Set `profile: 'my_bigquery_project'` in the new project’s `dbt_project.yml` |
| 5 | Create GCP project + BigQuery dataset and set permissions |
| 6 | Run `dbt debug` then `dbt run` / `dbt test` |

Your existing DuckDB project continues to use its own profile and adapter; the new project uses BigQuery only.
