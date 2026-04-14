# Quack mode example

Quack mode (`--quack`) lets you run most of your DAG locally on [DuckLake](https://ducklake.select/), only hitting the native database (e.g. BigQuery) for scripts that have external dependencies. SQL is automatically transpiled from the native dialect to DuckDB via [SQLGlot](https://sqlglot.com/).

This example uses publicly available NYC Citi Bike data from BigQuery. Here is the scripts file structure:

```
scripts
├── core
│   ├── stations.sql          → BigQuery (external dependency)
│   └── trips.sql             → BigQuery (external dependency)
└── analytics
    ├── station_metrics.sql   → DuckLake (depends on core)
    ├── trip_patterns.sql     → DuckLake (depends on core)
    └── peak_hours_by_station.sql → DuckLake (depends on analytics only)
```

## How it works

lea automatically classifies each script:

- **Native** (BigQuery): scripts that read from external sources not defined in the DAG, plus all their upstream ancestors.
- **Duck** (DuckLake): everything else. These scripts are transpiled to DuckDB dialect and run locally. They read native-DB results via the DuckDB BigQuery extension.

In this example, `core.trips` and `core.stations` read from `bigquery-public-data`, so they run on BigQuery. The three analytics scripts only depend on DAG tables, so they run locally on DuckLake.

## Setup

Create an `.env` file:

```sh
echo "
LEA_WAREHOUSE=bigquery
LEA_BQ_LOCATION=US
LEA_BQ_PROJECT_ID=<your-gcp-project>
LEA_BQ_COMPUTE_PROJECT_ID=<your-gcp-project>
LEA_BQ_DATASET_NAME=citibike

LEA_QUACK_DUCKLAKE_CATALOG_DATABASE=quack.ducklake
LEA_QUACK_DUCKLAKE_DATA_PATH=./quack_data/
" > .env
```

Make sure you're authenticated with Google Cloud:

```sh
gcloud auth application-default login
```

## Running

Run the full DAG in quack mode:

```sh
lea run --quack
```

You'll see that core scripts run on BigQuery (marked `(native)`), while analytics scripts run locally on DuckLake (marked `(ducklake)`).

## Iterating on analytics

Once the core tables are populated in BigQuery, you can iterate on the analytics layer without touching BigQuery at all:

```sh
lea run --quack --select analytics/
```

This runs only the three analytics scripts on DuckLake, reading from the already-materialized core tables via the BigQuery extension.

Even faster — `peak_hours_by_station` only depends on other analytics views, so it doesn't need to read from BigQuery at all:

```sh
lea run --quack --select analytics.peak_hours_by_station
```

This finishes in under a second.

## DuckLake storage

By default, DuckLake stores data locally in `./quack_data/` as Parquet files. You can also use cloud storage by setting `LEA_QUACK_DUCKLAKE_SECRET` to the body of a DuckDB [`CREATE SECRET`](https://duckdb.org/docs/current/configuration/secrets_manager) statement:

```sh
LEA_QUACK_DUCKLAKE_CATALOG_DATABASE=quack.ducklake
LEA_QUACK_DUCKLAKE_DATA_PATH=s3://my-bucket/quack-data/
LEA_QUACK_DUCKLAKE_SECRET="TYPE s3, KEY_ID 'key_id', SECRET 'secret_key', ENDPOINT 'storage.googleapis.com'"
```
