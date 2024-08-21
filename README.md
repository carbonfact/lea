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

lea is a minimalist alternative to tools like [dbt](https://www.getdbt.com/), [SQLMesh](https://sqlmesh.com/), and [Google's Dataform](https://cloud.google.com/dataform).

lea aims to be simple and opinionated, and yet offers the possibility to be extended. We happily use it every day at [Carbonfact](https://www.carbonfact.com/) to manage our data warehouse. We will actively maintain it and add features, while welcoming contributions.

Right now lea is compatible with BigQuery (used at Carbonfact) and DuckDB (quack quack).

- [Examples](#examples)
- [Teaser](#teaser)
- [Installation](#installation)
- [Usage](#usage)
  - [Configuration](#configuration)
  - [`lea run`](#lea-run)
    - [File structure](#file-structure)
    - [Development vs. production](#development-vs-production)
    - [Select which views to run](#select-which-views-to-run)
    - [Write-Audit-Publish (WAP)](#write-audit-publish-wap)
    - [Workflow tips](#workflow-tips)
  - [`lea test`](#lea-test)
  - [`lea docs`](#lea-docs)
  - [`lea diff`](#lea-diff)
  - [`lea teardown`](#lea-teardown)
  - [Jinja templating](#jinja-templating)
  - [Python scripts](#python-scripts)
  - [Dependency freezing](#dependency-freezing)
- [Contributing](#contributing)
- [License](#license)

## Examples

- [Jaffle shop 🥪](examples/jaffle_shop/)
- [Compare development to production 👯‍♀️](examples/diff/)
- [Using MotherDuck 🦆](examples/motherduck/)

## Teaser

<p align="center">
  <img width="85%" src="https://github.com/carbonfact/lea/assets/8095957/77e3fdb8-2ea6-4771-b32a-8eea8aa0a7aa" />
</p>

## Installation

Use one of the following commands, depending on which warehouse you wish to use:

```sh
pip install lea-cli[duckdb]
pip install lea-cli[bigquery]
```

This installs the `lea` command. It also makes the `lea` Python library available.

## Usage

### Configuration

lea is configured by setting environment variables. The following variables are available:

```sh
# General configuration
LEA_USERNAME=max

# DuckDB 🦆
LEA_WAREHOUSE=duckdb
LEA_DUCKDB_PATH=duckdb.db

# BigQuery 🦏
LEA_WAREHOUSE=bigquery
LEA_BQ_LOCATION=EU
LEA_BQ_PROJECT_ID=carbonfact-dwh
LEA_BQ_DATASET_NAME=kaya
LEA_BQ_SERVICE_ACCOUNT=<JSON dump of the service account file>  # not a path ⚠️
LEA_BQ_SCOPES=https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/drive
```

These parameters can be provided in an `.env` file, or directly in the shell. Each command also has an `--env` flag to provide a path to an `.env` file.

The `prepare` command has to be run once to create whatever needs creating. For instance, when using BigQuery, a dataset has to be created:

```sh
lea prepare
```

### `lea run`

This is the main command. It runs the queries in the `views` directory.

```sh
lea run
```

The queries are run concurrently. They are organized in a DAG, which is traversed in a topological order. The DAG's structure is determined [automatically](https://maxhalford.github.io/blog/dbt-ref-rant/) by analyzing the dependency between queries.

#### File structure

Each query is expected to be placed under a schema, represented by a directory. Schemas can have sub-schemas. For instance, the following file structure is valid:

```
views/
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

Each view will be named according to its location, following the warehouse convention:

| Warehouse | Dataset   | Username | Schema   | Table   | Name                                         |
| --------- | --------- | -------- | -------- | ------- | -------------------------------------------- |
| DuckDB    | `dataset` | `user`   | `schema` | `table` | `schema.table` (stored in `dataset_user.db`) |
| BigQuery  | `dataset` | `user`   | `schema` | `table` | `dataset_user.schema__table`                 |

The convention in lea to reference a table in a sub-schema is to use a double underscore `__`:

| Warehouse | Dataset   | Username | Schema   | Sub-schema | Table   | Name                                              |
| --------- | --------- | -------- | -------- | ---------- | ------- | ------------------------------------------------- |
| DuckDB    | `dataset` | `user`   | `schema` | `sub`      | `table` | `schema.sub__table` (stored in `dataset_user.db`) |
| BigQuery  | `dataset` | `user`   | `schema` | `sub`      | `table` | `dataset_user.schema__sub__table`                 |

Schemas are expected to be placed under a `views` directory. This can be changed by providing an argument to the `run` command:

```sh
lea run /path/to/views
```

This argument also applies to other commands in lea.

#### Development vs. production

By default, lea appends a `_<user>` suffix to schema names. This way you can have a development schema and a production schema. Use the `--production` flag to disable this behavior.

```sh
lea run --production
```

The `<user>` is determined automatically from the [login name](https://docs.python.org/3/library/getpass.html#getpass.getuser). It can be overriden by setting the `LEA_USERNAME` environment variable.

#### Select which views to run

A single view can be run:

```sh
lea run --select core.users
```

Several views can be run:

```sh
lea run --select core.users --select core.orders
```

Similar to dbt, lea also supports graph operators:

```sh
lea run --select core.users+   # users and everything that depends on it
lea run --select +core.users   # users and everything it depends on
lea run --select +core.users+  # users and all its dependencies
```

You can select all views in a schema:

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

There's an Easter egg that allows selecting views that have been commited or modified in the current Git branch:

```sh
lea run --select git
lea run --select git+  # includes all descendants
```

This becomes very handy when using lea in continuous integration. See [dependency freezing](#dependency-freezing) for more information.

#### Write-Audit-Publish (WAP)

[WAP](https://lakefs.io/blog/data-engineering-patterns-write-audit-publish/) is a data engineering pattern that ensures data consistency and reliability. It's the data engineering equivalent of [blue-green deployment](https://en.wikipedia.org/wiki/Blue%E2%80%93green_deployment) in the software engineering world.

By default, when you run a refresh, the tables gets progressively overwritten. This isn't necessarily a good idea. Let's say you refresh table A. Then you refresh table B that depends on A. If the refresh of B fails, you're left with a corrupted state. This is what the WAP pattern is trying to solve.

With lea, the WAP patterns works by creating temporary tables in the same schema as the original tables. The temporary tables are then populated with the new data. Once the temporary tables are ready, the original tables are swapped with the temporary tables. If the refresh fails, the switch doesn't happen, and the original tables are untouched.

```sh
lea run --wap
```

#### Workflow tips

The `lea run` command creates a `.cache.pkl` file during the run. This file is a checkpoint containing the state of the DAG. It is used to determine which queries to run next time. That is, if some queries have failed, only those queries and their descendants will be run again next time. The `.cache.pkl` is deleted once all queries have succeeded.

This checkpointing logic can be disabled with the `--fresh` flag.

```sh
lea run --fresh
```

The `--fail-fast` flag can be used to immediately stop if a query fails:

```sh
lea run --fail-fast
```

For debugging purposes, it is possible to print out a query and copy it to the clipboard:

```sh
lea run --select core.users --print | pbcopy
```

### `lea test`

```sh
lea test
```

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

As with the `run` command, there is a `--production` flag to disable the `<user>` suffix, and therefore target production data.

You can select a subset views, which will thus run the tests that depend on them:

```sh
lea test --select-views core.users
```

### `lea docs`

It is possible to generate documentation for the queries. This is done by inspecting the schema of the generated views and extracting the comments in the queries.

```sh
lea docs
    --output-dir docs  # where to put the generated files
```

This will also create a Mermaid diagram in the `docs` directory. This diagram is a visualization of the DAG. See [here](https://github.com/carbonfact/kaya/tree/main/lea/examples/jaffle_shop/docs) for an example.

### `lea diff`

```sh
lea diff
```

This prints out a summary of the difference between development and production. Here is an example output:

```diff
  core__users
+ 42 rows
+ age
+ email

- core__coupons
- 129 rows
- coupon_id
- amount
- user_id
- has_aggregation_key

  core__orders
- discount
+ supplier

  core__sales
+ 100 rows
```

This is handy in pull requests. For instance, at Carbonfact, we have a dataset for each pull request. We compare it to the production dataset and post the diff as a comment in the pull request. The diff is updated every time the pull request is updated. Check out [this example](examples/diff) for more information.

### `lea teardown`

```sh
lea teardown
```

This deletes the schema created by `lea prepare`. This is handy during continuous integration. For example, you might create a temporary schema in a branch. You would typically want to delete it after testing is finished and/or when the branch is merged.

### Jinja templating

SQL queries can be templated with [Jinja](https://jinja.palletsprojects.com/en/3.1.x/). A `.sql.jinja` extension is necessary for lea to recognise them.

You have access to an `env` variable within the template context, which is simply an access point to `os.environ`.

### Python scripts

You can write views with Python scripts. The only requirement is that the script contains a dataframe with a pandas DataFrame with the same name as the script. For instance, `users.py` should contain a `users` variable.

```python
import pandas as pd

users = pd.DataFrame(
    [
        {"id": 1, "name": "Max"},
        {"id": 2, "name": "Angie"},
    ]
)
```

### Dependency freezing

The `lea run` command can be used to only refresh a subset of views. Let's say we have this DAG:

```
fee -> fi -> fo -> fum
```

Assuming `LEA_USERNAME=max`, running `lea run --select fo+` will

1. Execute `fo` and materialize it to `fo_max`.
2. Execute `fum` and materialize it to `fum_max`.

This only works if `fee_max` and `fi_max` already exist. This might be the case if you've run a full refresh before. But if you're running a first refresh, then `fee_max` and `fi_max` won't exist! This is where the `freeze-unselected` flag comes into play:

```sh
lea run --select fo+ --freeze-unselected
```

This means the main `fee` and `fi` tables will be used instead of `fee_max` and `fi_max`.

Dependency freezing is particularly useful when using lea in a CI/CD context. You can run the following command in a pull request:

```sh
lea run --select git+ --freeze-unselected
```

This will only run the modified views and their descendants. The dependencies of these modified will be taken from production. This is akin to dbt't [defer](https://docs.getdbt.com/reference/node-selection/defer) command, but without the need for managing artifacts.

The added benefit is that you are guaranteed to do a comparison with the same base tables when running [`lea diff`](#lea-diff). Check out [this](https://maxhalford.github.io/blog/efficient-data-transformation/) article to learn more.

## Contributing

Feel free to reach out to [max@carbonfact.com](mailto:max@carbonfact.com) if you want to know more and/or contribute 😊

We have suggested [some issues](https://github.com/carbonfact/lea/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc+label%3A%22good+first+issue%22) as good places to get started.

## License

lea is free and open-source software licensed under the Apache License, Version 2.0.
