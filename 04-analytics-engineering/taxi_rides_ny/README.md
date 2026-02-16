Welcome to your new dbt project!

### Using the starter project

Run dbt from this directory so the profile path `taxi_rides_ny.duckdb` points to the right file and the source database name `taxi_rides_ny` resolves correctly:

```bash
cd 04-analytics-engineering/taxi_rides_ny   # if you're in the repo root
dbt run
dbt test
```

Alternatively, set an absolute path so it works from any directory:

```bash
export DBT_TAXI_RIDES_DUCKDB_PATH="/absolute/path/to/taxi_rides_ny/taxi_rides_ny.duckdb"
```

Try running:
- `dbt run`
- `dbt test`


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices




## Commands

### Inspecting raw data (DuckDB CLI)

Open the DuckDB database and run ad-hoc queries to inspect source tables:

```bash
duckdb taxi_rides_ny.duckdb
```

Inside the DuckDB shell, check row counts for the raw trip data:

```sql
SELECT count(*) FROM prod.green_tripdata;
SELECT count(*) FROM prod.yellow_tripdata;
```

---

### Project setup & configuration

| Command | Description |
|--------|-------------|
| `dbt init` | Creates a new dbt project (run from the parent directory). Use only when starting a project from scratch. |
| `dbt debug` | Verifies your profile and database connection. Run this first if `dbt run` or `dbt test` fail. |
| `dbt deps` | Installs packages listed in `packages.yml` (e.g. dbt_utils). Run after cloning or when adding new dependencies. |
| `dbt clean` | Removes the `target/` and `dbt_packages/` directories. Use when you want a clean compile/run or after changing packages. |

---

### Loading reference data

| Command | Description |
|--------|-------------|
| `dbt seed` | Loads CSV files from `seeds/` (e.g. `payment_type_lookup.csv`, `taxi_zone_lookup.csv`) into the database. Run after adding or updating seed files. |
| `dbt snapshot` | Executes snapshot models to track slowly changing dimensions (SCD Type 2). Use when you have snapshot models defined in `snapshots/`. |

---

### Building & running models

| Command | Description |
|--------|-------------|
| `dbt compile` | Compiles SQL from your models and macros without running them. Useful to check for syntax or reference errors. |
| `dbt run` | Builds all models in dependency order. Creates/updates tables and views in the target database. |
| `dbt run --select <model>` | Builds only the specified model and its upstream dependencies. Examples: |
| `dbt run --select stg_green_tripdata` | Builds the green taxi staging model only. |
| `dbt run --select stg_yellow_tripdata` | Builds the yellow taxi staging model only. |
| `dbt run --select int_trips_unioned` | Builds the unioned trips intermediate model (and its staging dependencies). |
| `dbt run --select monthly_revenue_per_locations` | Builds the monthly revenue mart (and all upstream models). |
| `dbt run --select intermediate` | Builds all models in the `intermediate` folder (by folder name). |
| `dbt run --select +int_trips_unioned` | Builds `int_trips_unioned` and **all** of its upstream dependencies (staging, etc.). |
| `dbt run --select int_trips_unioned+` | Builds `int_trips_unioned` and **all** downstream models that depend on it. |
| `dbt run --full-refresh` | Rebuilds all models from scratch (drops and recreates tables). Use when schema or logic changed significantly. |
| `dbt run --full-refresh --select <model>` | Full refresh only for the selected model (e.g. `stg_yellow_tripdata`, `int_trips_unioned`, `monthly_revenue_per_locations`). |
| `dbt run --fail-fast` | Stops on the first model that fails instead of continuing with the rest. Helps debug errors quickly. |
| `dbt run --select state:modified --state ./target` | Runs only models that changed compared to a previous run (uses `./target` as the prior state). Useful for incremental CI/CD. |
| `dbt build` | Runs models and then runs tests on their results (run + test in one step). Good for a full validation pass. |

---

### Inspecting model results

| Command | Description |
|--------|-------------|
| `dbt show --select <model> --limit 20` | Compiles the model, runs it in the warehouse, and prints sample rows (e.g. 20). No table is created. Use to sanity-check output. |
| `dbt show --select int_trips_unioned --limit 20` | Preview the unioned trips model output. |
| `dbt show --select monthly_revenue_per_locations --limit 20` | Preview the monthly revenue mart. |
| `dbt show --select stg_green_tripdata --limit 20` | Preview green taxi staging. |
| `dbt show --select stg_yellow_tripdata --limit 20` | Preview yellow taxi staging. |

---

### Testing

| Command | Description |
|--------|-------------|
| `dbt test` | Runs all tests (singular + generic) defined in the project. Use after `dbt run` to validate data quality. |
| `dbt test --select <model>` | Runs only tests that reference the specified model(s). |
| `dbt test --select stg_green_tripdata` | Tests for the green staging model. |
| `dbt test --select stg_yellow_tripdata` | Tests for the yellow staging model. |
| `dbt test --select int_trips_unioned` | Tests for the unioned trips model. |
| `dbt test --select monthly_revenue_per_locations` | Tests for the monthly revenue mart. |
| `dbt test -t prod` | Runs tests using the `prod` target from your profile (e.g. production database). |
| `dbt retry` | Re-runs only the tests that failed in the last `dbt test` run. |

---

### Documentation

| Command | Description |
|--------|-------------|
| `dbt docs generate` | Generates a static docs site from your project (models, columns, lineage, etc.) and writes it to `target/`. |
| `dbt docs serve` | Serves the generated docs locally (usually http://localhost:8080). Run after `dbt docs generate` to browse. |
| `dbt source freshness` | Checks how old your source data is (based on `freshness` in `sources.yml`). Use to monitor pipeline timeliness. |

---

### CLI reference

| Command | Description |
|--------|-------------|
| `dbt --help` or `dbt -h` | Prints available commands and global options. |
| `dbt --version` | Shows the installed dbt version. |
| `dbt --profile` | Shows the profile that would be used (from `~/.dbt/profiles.yml` or `DBT_PROFILES_DIR`). |
| `dbt --profile=taxi_rides_ny` | Explicitly use the `taxi_rides_ny` profile. |
| `dbt --profile=taxi_rides_ny --target=dev` | Use the `taxi_rides_ny` profile with the `dev` target (e.g. dev database/schema). |