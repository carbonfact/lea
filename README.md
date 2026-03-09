<h1>lea</h1>

<img src="https://github.com/carbonfact/lea/assets/8095957/df2bcf1e-fcc9-4111-9897-ec29427aeeaa" width="33%" align="right" />

<p>
<!-- CI -->
<a href="https://github.com/carbonfact/lea/actions/workflows/ci.yml">
    <img src="https://github.com/carbonfact/lea/actions/workflows/ci.yml/badge.svg" alt="CI">
</a>

<!-- PyPI -->
<a href="https://pypi.org/project/lea-cli">
    <img src="https://img.shields.io/pypi/v/lea-cli.svg?label=release&color=blue" alt="pypi">
</a>

<!-- License -->
<a href="https://opensource.org/license/apache-2-0/">
    <img src="https://img.shields.io/github/license/carbonfact/lea" alt="license">
</a>
</p>

lea is a simple SQL runner. You write SQL scripts, organize them in folders, and lea takes care of the rest: it figures out the dependency order, runs them concurrently, and materializes the results as tables in your data warehouse.

Think of it as a minimalist alternative to [dbt](https://www.getdbt.com/) or [SQLMesh](https://sqlmesh.com/). We use it every day at [Carbonfact](https://www.carbonfact.com/) to manage our BigQuery data warehouse.

## TLDR

You organize SQL scripts in a `scripts/` directory:

```
scripts/
    staging/
        customers.sql
        orders.sql
    core/
        revenue.sql
```

Each script is a `SELECT` statement:

```sql
-- scripts/core/revenue.sql
SELECT
    customers.name,
    SUM(orders.amount) AS total
FROM staging.customers
JOIN staging.orders ON orders.customer_id = customers.id
GROUP BY 1
```

Then you run them:

```sh
lea run
```

lea parses the SQL, sees that `core.revenue` depends on `staging.customers` and `staging.orders`, and runs them in the right order. Each script becomes a table in your warehouse.

## Table of contents

- [Installation](#installation)
- [Configuration](#configuration)
  - [BigQuery](#bigquery)
  - [DuckDB](#duckdb)
  - [MotherDuck](#motherduck)
  - [DuckLake](#ducklake)
- [Usage](#usage)
  - [Selecting scripts](#selecting-scripts)
  - [Development vs. production](#development-vs-production)
  - [Jinja templating](#jinja-templating)
  - [Testing](#testing)
  - [Write-Audit-Publish (WAP)](#write-audit-publish-wap)
  - [Skipping unmodified scripts](#skipping-unmodified-scripts)
  - [Quack mode](#quack-mode)
- [Warehouse specific features](#warehouse-specific-features)
  - [BigQuery](#bigquery-1)
- [Examples](#examples)
- [Contributing](#contributing)
- [License](#license)

## Installation

```sh
pip install lea-cli
```

This installs the `lea` command. It also makes the `lea` Python library available.

## Configuration

lea is configured via environment variables. They can be provided in an `.env` file, or directly in the shell. Each command also has an `--env` flag to provide a path to an `.env` file.

### BigQuery

```sh
# Required
LEA_WAREHOUSE=bigquery
# Required
LEA_BQ_LOCATION=EU
# Required
LEA_BQ_DATASET_NAME=kaya
# Required, the project where the dataset is located
LEA_BQ_PROJECT_ID=carbonfact-dwh
# Optional, allows using a different project for compute
LEA_BQ_COMPUTE_PROJECT_ID=carbonfact-dwh-compute
# Not necessary if you're logged in with the gcloud CLI
LEA_BQ_SERVICE_ACCOUNT=<JSON dump of the service account file>  # not a path
# Defaults to https://www.googleapis.com/auth/bigquery
LEA_BQ_SCOPES=https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/drive
# LOGICAL or PHYSICAL, defaults to PHYSICAL
LEA_BQ_STORAGE_BILLING_MODEL=PHYSICAL
```

### DuckDB

```sh
# Required
LEA_WAREHOUSE=duckdb
# Required
LEA_DUCKDB_PATH=duckdb.db
# Optional
LEA_DUCKDB_EXTENSIONS=parquet,httpfs
```

### MotherDuck

```sh
# Required
LEA_WAREHOUSE=motherduck
# Required
MOTHERDUCK_TOKEN=<get this from https://app.motherduck.com/settings/tokens>
# Required
LEA_MOTHERDUCK_DATABASE=bike_sharing
# Optional
LEA_DUCKDB_EXTENSIONS=parquet,httpfs
```

### DuckLake

```sh
# Required
LEA_WAREHOUSE=ducklake
# Required
LEA_DUCKLAKE_DATA_PATH=gcs://bike-sharing-analytics
# Required
LEA_DUCKLAKE_CATALOG_DATABASE=metadata.ducklake
# Optional
LEA_DUCKLAKE_S3_ENDPOINT=storage.googleapis.com
# Optional
LEA_DUCKDB_EXTENSIONS=parquet,httpfs
```

DuckLake needs a database to [manage metadata](https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database), which is what `LEA_DUCKLAKE_CATALOG_DATABASE` is for.

## Usage

### Selecting scripts

By default, `lea run` runs all scripts. You can select specific scripts:

```sh
lea run --select core.revenue
lea run --select core.revenue --select core.users
```

You can select all scripts in a schema:

```sh
lea run --select core/
```

Graph operators let you include dependencies or dependents:

```sh
lea run --select core.revenue+   # revenue and everything that depends on it
lea run --select +core.revenue   # revenue and everything it depends on
lea run --select +core.revenue+  # both directions
```

You can also select scripts that have been modified in the current Git branch:

```sh
lea run --select git
lea run --select git+  # modified scripts and their dependents
```

This is handy in continuous integration.

You can exclude scripts with `--unselect`:

```sh
lea run --unselect tests/
```

### Development vs. production

By default, lea creates a development environment isolated from production by appending `_<user>` to dataset names. Use `--production` to target the production environment:

```sh
lea run --production
```

The `<user>` is determined from the login name. It can be overridden with the `LEA_USERNAME` environment variable.

### Jinja templating

SQL queries can be templated with [Jinja](https://jinja.palletsprojects.com/en/3.1.x/). Use a `.sql.jinja` extension.

You have access to `env` (i.e. `os.environ`) and a `load_yaml` function:

```jinja
{% set taxonomy = load_yaml('core/taxonomies/product.yaml') %}

SELECT
  {% for dim in taxonomy.dimensions %}
  MAX(IF(key = '{{ dim.key }}', value, NULL)) AS {{ dim.column }},
  {% endfor %}
  account_slug
FROM core.raw_attributes
```

### Testing

There is no separate `lea test` command. Tests run alongside regular scripts during `lea run`. The run stops whenever a test fails.

There are two types of tests:

- **Singular tests** are queries stored in a `tests/` directory. They fail if they return any rows.
- **Assertion tests** are comment annotations in the queries themselves:

```sql
SELECT
    -- #UNIQUE
    -- #NO_NULLS
    user_id,
    -- #NO_NULLS
    address,
    -- #UNIQUE_BY(address)
    full_name,
    -- #SET{'A', 'B', 'AB', 'O'}
    blood_type
FROM core.users
```

Available tags: `#NO_NULLS`, `#UNIQUE`, `#UNIQUE_BY(<by>)`, `#SET{<elements>}`.

When you run a script, all applicable tests are run as well.

### Write-Audit-Publish (WAP)

lea follows the [WAP](https://lakefs.io/blog/data-engineering-patterns-write-audit-publish/) pattern by default. When you execute `lea run`, scripts are first materialized into temporary `___audit` tables. These are promoted to replace the real tables only once everything has succeeded without errors.

This prevents partial updates. If script `bar` depends on `foo` and `bar` fails, `foo` is not modified either.

### Skipping unmodified scripts

lea doesn't re-run scripts when the audit table already exists and the script hasn't been modified since. This avoids unnecessary work during development. You can disable this with:

```sh
lea run --restart
```

### Quack mode

Quack mode runs your scripts locally with [DuckDB](https://duckdb.org/) instead of your cloud warehouse. This makes local iteration much faster and doesn't incur any cloud costs.

```sh
lea run --select core.users --quack
```

lea automatically pulls the necessary upstream tables from your warehouse into a [DuckLake](https://ducklake.select/) instance, and only pulls what's missing. SQL is transpiled to DuckDB automatically.

You'll need to configure a DuckLake instance for storage, in addition to your regular warehouse configuration. If your dependencies are small, a local path works fine:

```sh
LEA_QUACK_DUCKLAKE_CATALOG_DATABASE=quack.ducklake
LEA_QUACK_DUCKLAKE_DATA_PATH=/path/to/quack/data
```

For larger dependencies, it's recommended to use an S3-compatible target instead. DuckLake supports this natively, which means the data lives in the cloud rather than on your machine:

```sh
LEA_QUACK_DUCKLAKE_CATALOG_DATABASE=quack.ducklake
LEA_QUACK_DUCKLAKE_DATA_PATH=s3://my-bucket/quack/data
LEA_QUACK_DUCKLAKE_S3_ENDPOINT=storage.googleapis.com
```

## Warehouse specific features

### BigQuery

#### Default clustering

lea can automatically cluster tables that contain given fields:

```sh
LEA_BQ_DEFAULT_CLUSTERING_FIELDS=account_slug
LEA_BQ_DEFAULT_CLUSTERING_FIELDS=account_slug,brand_slug
```

For each table, lea uses whichever configured clustering fields are present and ignores the rest.

#### Table specific clustering

You can also define clustering fields for a specific table:

```sql
SELECT
  account_slug,
  -- #CLUSTERING_FIELD
  object_kind,
  value
FROM my_table
```

Table-specific clustering fields are added *in addition to* the default ones.

#### Script-specific compute projects

You can route specific scripts to different compute projects:

```sh
LEA_BQ_SCRIPT_SPECIFIC_COMPUTE_PROJECT_IDS={"dataset.schema__table": "reservation-project-id"}
```

Scripts not listed use the default `LEA_BQ_COMPUTE_PROJECT_ID`.

#### Big Blue Pick API

[Big Blue](https://biq.blue/) provides a [Pick API](https://biq.blue/blog/compute/how-to-implement-bigquery-autoscaling-reservation-in-10-minutes) that suggests whether to run a query on-demand or on a reservation. lea supports this out of the box:

```sh
LEA_BQ_BIG_BLUE_PICK_API_KEY=<get from https://your-company.biq.blue/settings.html>
LEA_BQ_BIG_BLUE_PICK_API_URL=https://pick.biq.blue
LEA_BQ_BIG_BLUE_PICK_API_ON_DEMAND_PROJECT_ID=on-demand-compute-project-id
LEA_BQ_BIG_BLUE_PICK_API_REVERVATION_PROJECT_ID=reservation-compute-project-id
```

## Examples

- [Jaffle shop](examples/jaffle_shop/)
- [Incremental](examples/incremental)
- [School](examples/school/)
- [Compare development to production](examples/diff/)
- [Using MotherDuck](examples/motherduck/)

## Contributing

Feel free to reach out to [max@carbonfact.com](mailto:max@carbonfact.com) if you want to know more and/or contribute.

We have suggested [some issues](https://github.com/carbonfact/lea/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc+label%3A%22good+first+issue%22) as good places to get started.

## License

lea is free and open-source software licensed under the Apache License, Version 2.0.
