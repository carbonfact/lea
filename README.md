<h1>lea</h1>

<img src="https://github.com/carbonfact/lea/assets/8095957/df2bcf1e-fcc9-4111-9897-ec29427aeeaa" width="33%" align="right" />

<p>
<!-- Tests -->
<a href="https://github.com/carbonfact/lea/actions/workflows/unit-tests.yml">
    <img src="https://github.com/carbonfact/lea/actions/workflows/unit-tests.yml/badge.svg" alt="tests">
</a>

<!-- Code quality -->
<a href="https://github.com/carbonfact/lea/actions/workflows/code-quality.yml">
    <img src="https://github.com/carbonfact/lea/actions/workflows/code-quality.yml/badge.svg" alt="code_quality">
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

lea is a minimalist alternative to SQL orchestrators like [dbt](https://www.getdbt.com/) and [SQLMesh](https://sqlmesh.com/).

lea aims to be simple and provides sane defaults. We happily use it every day at [Carbonfact](https://www.carbonfact.com/) to manage our BigQuery data warehouse. We will actively maintain it and add features, while welcoming contributions.

- [Examples](#examples)
- [Installation](#installation)
- [Configuration](#configuration)
  - [DuckDB](#duckdb)
  - [BigQuery](#bigquery)
- [Usage](#usage)
  - [`lea run`](#lea-run)
  - [File structure](#file-structure)
    - [Jinja templating](#jinja-templating)
  - [Development vs. production](#development-vs-production)
  - [Selecting scripts](#selecting-scripts)
  - [Write-Audit-Publish (WAP)](#write-audit-publish-wap)
  - [Testing while running](#testing-while-running)
  - [Skipping unmodified scripts during development](#skipping-unmodified-scripts-during-development)
- [Contributing](#contributing)
- [License](#license)

## Examples

- [Jaffle shop 🥪](examples/jaffle_shop/)
- [Incremental 🕐](examples/incremental)
- [School 🏫](examples/school/)
- [Compare development to production 👯‍♀️](examples/diff/)
- [Using MotherDuck 🦆](examples/motherduck/)

## Installation

Use one of the following commands, depending on which warehouse you wish to use:

```sh
pip install lea-cli
```

This installs the `lea` command. It also makes the `lea` Python library available.

## Configuration

lea is configured via environment variables.

### DuckDB

```sh
LEA_WAREHOUSE=duckdb
LEA_DUCKDB_PATH=duckdb.db
```

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
LEA_BQ_SERVICE_ACCOUNT=<JSON dump of the service account file>  # not a path ⚠️
# Defaults to https://www.googleapis.com/auth/bigquery
LEA_BQ_SCOPES=https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/drive
# LOGICAL or PHYSICAL, defaults to PHYSICAL
LEA_BQ_STORAGE_BILLING_MODEL=PHYSICAL
```

## Usage

These parameters can be provided in an `.env` file, or directly in the shell. Each command also has an `--env` flag to provide a path to an `.env` file.

### `lea run`

This is the main command. It runs SQL queries stored in the `scripts` directory:

```sh
lea run
```

You can indicate the directory where the scripts are stored:

```sh
lea run --scripts /path/to/scripts
```

The scripts are run concurrently. They are organized in a DAG, which is traversed in a topological order. The DAG's structure is determined [automatically](https://maxhalford.github.io/blog/dbt-ref-rant/) by analyzing the dependency between queries.

### File structure

Each query is expected to be placed under a schema, represented by a directory. Schemas can have sub-schemas. Here's an example:

```
scripts/
    schema_1/
        table_1.sql
        table_2.sql
    schema_2/
        table_3.sql
        table_4.sql
        sub_schema_2_1/
            table_5.sql
            table_6.sql
```

Each script is materialized into a table. The table is named according to the script's name, following the warehouse convention.

#### Jinja templating

SQL queries can be templated with [Jinja](https://jinja.palletsprojects.com/en/3.1.x/). A `.sql.jinja` extension is necessary for lea to recognise them.

You have access to an `env` variable within the template context, which is simply an access point to `os.environ`.

### Development vs. production

By default, lea creates an isolation layer with production. The way this is done depends on your warehouse:

- BigQuery : by appending a `_<user>` suffix to schema names
- DuckDB : by adding a suffix `_<user>` to database file.

In other words, a development environment is used by default. Use the `--production` flag when executing `lea run` to disable this behaviour, and instead target the product environment.

```sh
lea run --production
```

The `<user>` is determined automatically from the [login name](https://docs.python.org/3/library/getpass.html#getpass.getuser). It can be overriden by setting the `LEA_USERNAME` environment variable.

### Selecting scripts

A single script can be run:

```sh
lea run --select core.users
```

Several scripts can be run:

```sh
lea run --select core.users --select core.orders
```

Similar to dbt, lea also supports graph operators:

```sh
lea run --select core.users+   # users and everything that depends on it
lea run --select +core.users   # users and everything it depends on
lea run --select +core.users+  # users and all its dependencies
```

You can select all scripts in a schema:

```sh
lea run --select core/
```

This also work with sub-schemas:

```sh
lea run --select analytics.finance/
```

There are thus 8 possible operators:

```
schema.table    (table by itself)
schema.table+   (table with its descendants)
+schema.table   (table with its ancestors)
+schema.table+  (table with its ancestors and descendants)
schema/         (all tables in schema)
schema/+        (all tables in schema with their descendants)
+schema/        (all tables in schema with their ancestors)
+schema/+       (all tables in schema with their ancestors and descendants)
```

Combinations are possible:

```sh
lea run --select core.users+ --select +core.orders
```

There's an Easter egg that allows choosing scripts that have been committed or modified in the current Git branch:

```sh
lea run --select git
lea run --select git+  # includes all descendants
```

This becomes very handy when using lea in continuous integration.

### Write-Audit-Publish (WAP)

[WAP](https://lakefs.io/blog/data-engineering-patterns-write-audit-publish/) is a data engineering pattern that ensures data consistency and reliability. It's the data engineering equivalent of [blue-green deployment](https://en.wikipedia.org/wiki/Blue%E2%80%93green_deployment) in the software engineering world.

lea follows the WAP pattern by default. When you execute `lea run`, it actually creates temporary tables that have an `___audit` suffix. The latter tables are promoted to replace the existing tables, once they have all been materialized without errors.

This is a good default behavior. Let's say you refresh table `foo`. Then you refresh table `bar` that depends on `foo`. If the refresh of `bar` fails, you're left with a corrupt state. This is what the WAP pattern solves. In WAP mode, when you run `foo`'s script, it creates a `foo___audit` table. If `bar`'s script fails, then the run stops and `foo` is not modified.

### Testing while running

There is no `lea test` command. Tests are run together with the regular script when `lea run` is executed. The run stops whenever a test fails.

There are two types of tests:

- Singular tests — these are queries which return failing rows. They are stored in a `tests` directory.
- Assertion tests — these are comment annotations in the queries themselves:
  - `#NO_NULLS` — checks that all values in a column are not null.
  - `#UNIQUE` — checks that a column's values are unique.
  - `#UNIQUE_BY(<by>)` — checks that a column's values are unique within a group.
  - `#SET{<elements>}` — checks that a column's values are in a set of values.

Here's an example of a query annotated with assertion tests:

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

You can run a single test via the `--select` flag:

```sh
lea run --select tests.check_n_users
```

Or even run all the tests, as so:

```sh
lea run --select tests/
```

You may decide to run all scripts without executing tests, which is obviously not advisable:

```sh
lea run --unselect tests/
```

### Skipping unmodified scripts during development

When you call `lea run`, it generates audit tables, which are then promoted to replace the original tables. This is done to ensure that the data is consistent and reliable. lea doesn't run scripts when the audit table already exists, and when the script hasn't modified since the last time the audit table was created. This is to avoid unnecessary re-runs of scripts that haven't changed.

For instance:

1. You execute `lea run` to sync all tables from sources, no errors, all tables are materialized.
2. You modify a script named `core/expenses.sql` depending on `staging/customers.sql` and `staging/orders.sql`
3. You execute `lea run core.expenses+` to run again all impacted tables
4. `core__expenses___audit` is materialized in your data warehouse but the `-- #NO_NULLS` assertion test on a column fails
5. After reviewing data in `core__expenses___audit`, you edit and fix `core/expenses.sql` to filter out results where NULLs are appearing
6. You execute `lea run`
7. The `staging/customers.sql` and `staging/orders.sql` scripts are skipped because they were modified before `staging__customers` and `staging__orders` was last materialized
8. The `core/expenses.sql` script is run because it was modified after `core__expenses` was last materialized
9. All audit tables are wipped out from database as the whole DAG has run successfully ! 🎉

You can disable this behavior altogether:

```sh
lea run --restart
```

## Contributing

Feel free to reach out to [max@carbonfact.com](mailto:max@carbonfact.com) if you want to know more and/or contribute 😊

We have suggested [some issues](https://github.com/carbonfact/lea/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc+label%3A%22good+first+issue%22) as good places to get started.

## License

lea is free and open-source software licensed under the Apache License, Version 2.0.
