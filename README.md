<h1>lea</h1>

lea is a minimalist alternative to tools like [dbt](https://www.getdbt.com/), [SQLMesh](https://sqlmesh.com/), and [Dataform](https://cloud.google.com/dataform).

lea is intended to be simple, opinionated, while offering the ability to be extended. We use it every day at Carbonfact to manage our data warehouse.

- [Example](#example)
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
  - [Import `lea` as a library](#import-lea-as-a-library)
- [Roadmap](#roadmap)

## Example

See [examples](examples).

## Usage

### Configuration

lea is configured by setting environment variables. The following variables are available:

```sh
# General configuration
LEA_SCHEMA=kaya
LEA_USERNAME=max
LEA_WAREHOUSE=bigquery

# DuckDB
LEA_DUCKDB_PATH=duckdb.db

# BigQuery
LEA_BQ_LOCATION=EU
LEA_BQ_PROJECT_ID=carbonfact-dwh
LEA_BQ_SERVICE_ACCOUNT=<a JSON dump of the service account file>
```

The `prepare` command has to be run once to create whatever needs creating. For instance, when working with BigQuery, a dataset has to be created:

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

```
views/
    schema_1/
        table_1.sql
        table_2.sql
    schema_2/
        table_3.sql
        table_4.sql
```

#### Development vs. production

By default, lea appends a `_<user>` suffix to schema names. This way you can have a development schema and a production schema. To disable this behavior, use the `--production` flag.

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

You may of course do combinations:

```sh
lea run --only core.users+ --only +core.orders
```

#### Workflow tips

The `lea run` command creates a `.cache.pkl` file is created during the run. This file is a checkpoint containing the state of the DAG. It is used to determine which queries to run next time. That is, if some queries have failed, only those queries and their descendants will be run again next time. The `.cache.pkl` is deleted once all queries have succeeded.

This checkpointing logic can be disabled with the `--fresh` flag.

```sh
lea run --fresh
```

The `--raise-exceptions` flag can be used to immediately stop if a query fails:

```sh
lea --raise-exceptions
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
- Annotation tests -- these are comment annotations in the queries themselves:
  - `@UNIQUE` -- checks that a column's values are unique.

As with the `run` command, there is a `--production` flag to disable the `<user>` suffix.

### `lea docs`

It is possible to generate documentation for the queries. This is done by inspecting the schema of the generated views and extracting the comments in the queries.

```sh
lea docs views
    --output-dir docs  # where to put the generated files
```

This will also create a Mermaid diagram in the `docs` directory. This diagram is a visualization of the DAG.

### `lea diff`

```sh
lea diff origin destination
```

This prints out a summary of the difference between two schemas in terms of structure. This is handy in pull requests.

### `lea teardown`

```sh
lea teardown
```

This deletes the schema created by `lea prepare`. This is handy during continuous integration.

### Jinja templating

SQL queries can be templated with Jinja. A `.sql.jinja` extension for lea to recognise them.

You have access to an `env` variable, which is simply an access point to `os.environ`.

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

### Import `lea` as a library

lea is meant to be used as a CLI. But you can use it as a library too.

**Parsing a directory of queries**

```py
from lea import views

views = views.load_views('views', sqlglot_dialect='bigquery')
for view in views:
    print(view)
    print(view.dependencies)
```

**Organizing queries into a DAG**

```py
from lea import views

views = views.load_views('views', sqlglot_dialect='bigquery')
dag = views.DAGOfViews(views)
for schema, table in dag.get_ready():
    print(schema, table)
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

Some of these features already exist at Carbonfact. We just don't feel they're polished enough for public consumption just yet. Feel free to reach out if you want to know more and/or contribute 😊
