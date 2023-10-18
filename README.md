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

- [Example](#example)
- [Teaser](#teaser)
- [Installation](#installation)
- [Usage](#usage)
  - [Configuration](#configuration)
  - [`lea run`](#lea-run)
    - [File structure](#file-structure)
    - [Development vs. production](#development-vs-production)
    - [Select which views to run](#select-which-views-to-run)
    - [Workflow tips](#workflow-tips)
  - [`lea test`](#lea-test)
  - [`lea docs`](#lea-docs)
  - [`lea diff`](#lea-diff)
  - [`lea teardown`](#lea-teardown)
  - [Jinja templating](#jinja-templating)
  - [Python scripts](#python-scripts)
  - [Import lea as a Python library](#import-lea-as-a-python-library)
- [Roadmap](#roadmap)
- [License](#license)

## Example

- [Jaffle shop 🥪](examples/jaffle_shop/)

## Teaser

![render1697638950297](https://github.com/carbonfact/lea/assets/8095957/77e3fdb8-2ea6-4771-b32a-8eea8aa0a7aa)

## Installation

```sh
pip install lea-cli
```

This installs the `lea` command. It also makes the `lea` Python library available.

## Usage

### Configuration

lea is configured by setting environment variables. The following variables are available:

```sh
# General configuration
LEA_USERNAME=max
LEA_WAREHOUSE=bigquery

# DuckDB 🦆
LEA_DUCKDB_PATH=duckdb.db

# BigQuery 🦏
LEA_BQ_LOCATION=EU
LEA_BQ_PROJECT_ID=carbonfact-dwh
LEA_BQ_DATASET_NAME=kaya
LEA_BQ_SERVICE_ACCOUNT=<a JSON dump of the service account file>
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

Each view will be named according to its location, following the warehouse convention. For instance, `schema_1/table_1.sql` will be named `dataset.schema_1__table_1` in BigQuery and `schema_1.table_1` in DuckDB.

The schemas are expected to be placed under a `views` directory. This can be changed by providing an argument to the `run` command:

```sh
lea run /path/to/views
```

This also applies to the other commands.

#### Development vs. production

By default, lea appends a `_<user>` suffix to schema names. This way you can have a development schema and a production schema. Use the `--production` flag to disable this behavior.

```sh
lea run --production
```

The `<user>` is determined automatically from the [login name](https://docs.python.org/3/library/getpass.html#getpass.getuser). It can be overriden by setting the `LEA_USERNAME` environment variable.

#### Select which views to run

A single view can be run:

```sh
lea run --only core.users
```

Several views can be run:

```sh
lea run --only core.users --only core.orders
```

Similar to dbt, lea also supports graph operators:

```sh
lea run --only core.users+   # users and everything that depends on it
lea run --only +core.users   # users and everything it depends on
lea run --only +core.users+  # users and all its dependencies
```

You can select all views in a schema:

```sh
lea run --only core/
```

This also work with sub-schemas:

```sh
lea run --only analytics.finance/
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
lea run --only core.users+ --only +core.orders
```

#### Workflow tips

The `lea run` command creates a `.cache.pkl` file during the run. This file is a checkpoint containing the state of the DAG. It is used to determine which queries to run next time. That is, if some queries have failed, only those queries and their descendants will be run again next time. The `.cache.pkl` is deleted once all queries have succeeded.

This checkpointing logic can be disabled with the `--fresh` flag.

```sh
lea run --fresh
```

The `--raise-exceptions` flag can be used to immediately stop if a query fails:

```sh
lea run --raise-exceptions
```

For debugging purposes, it is possible to print out a query and copy it to the clipboard:

```sh
lea run --print --only core.users | pbcopy
```

### `lea test`

```sh
lea test views
```

There are two types of tests:

- Singular tests -- these are queries which return failing rows. They are stored in a `tests` directory.
- Assertion tests -- these are comment annotations in the queries themselves:
  - `@UNIQUE` -- checks that a column's values are unique.

As with the `run` command, there is a `--production` flag to disable the `<user>` suffix and thus test production data.

### `lea docs`

It is possible to generate documentation for the queries. This is done by inspecting the schema of the generated views and extracting the comments in the queries.

```sh
lea docs views
    --output-dir docs  # where to put the generated files
```

This will also create a Mermaid diagram in the `docs` directory. This diagram is a visualization of the DAG. See [here](https://github.com/carbonfact/kaya/tree/main/lea/examples/jaffle_shop/docs) for an example.

### `lea diff`

```sh
lea diff origin destination
```

This prints out a summary of the difference between two schemas in terms of structure. This is handy in pull requests. For instance, at Carbonfact we often compare our development schemas with our production schema:

```sh
lea diff kaya_max kaya
```

Here is an example:

```diff
  core__users
+ age
+ email

- core__coupons
- coupon_id
- amount
- user_id
- has_aggregation_key

  core__orders
- discount
+ supplier
```

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

### Import lea as a Python library

lea is meant to be used as a CLI. But you can import it as a Python library too. For instance, we do this at Carbonfact to craft custom commands.

**Parsing a directory of queries**

```py
>>> import lea

>>> views = lea.views.load_views('examples/jaffle_shop/views', sqlglot_dialect='duckdb')
>>> views = [v for v in views if v.schema != 'tests']
>>> for view in sorted(views, key=str):
...     print(view)
...     print(sorted(view.dependencies))
analytics.finance.kpis
[('core', 'orders')]
analytics.kpis
[('core', 'customers'), ('core', 'orders')]
core.customers
[('staging', 'customers'), ('staging', 'orders'), ('staging', 'payments')]
core.orders
[('staging', 'orders'), ('staging', 'payments')]
staging.customers
[]
staging.orders
[]
staging.payments
[]

```

**Organizing queries into a DAG**

```py
>>> import lea

>>> views = lea.views.load_views('examples/jaffle_shop/views', sqlglot_dialect='duckdb')
>>> views = [v for v in views if v.schema != 'tests']
>>> dag = lea.views.DAGOfViews(views)
>>> while dag.is_active():
...     for node in sorted(dag.get_ready()):
...         print(dag[node])
...         dag.done(node)
staging.customers
staging.orders
staging.payments
core.customers
core.orders
analytics.finance.kpis
analytics.kpis

```

## Roadmap

- Incremental queries
- Exporting
- Historization
- Linting
- Data lineage
- Extending the CLI
- Only refresh what changed, based on git
- Splitting a query into CTEs
- Metric layer
- Shell auto-completion
- Hot-swapping after success

Some of these features already exist at Carbonfact. We just don't feel they're polished enough for public consumption. Feel free to reach out if you want to know more and/or contribute 😊

## License

lea is free and open-source software licensed under the Apache License, Version 2.0.
