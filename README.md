<h1>lea</h1>

lea is an opinionated alternative to tools like [dbt](https://www.getdbt.com/), [SQLMesh](https://sqlmesh.com/), and [Dataform](https://cloud.google.com/dataform).

lea is designed to be simple and easy to extend. We use it every day at Carbonfact to manage our data warehouse.

- [Example](#example)
- [Usage](#usage)
  - [Configuration](#configuration)
  - [`lea run`](#lea-run)
  - [`lea test`](#lea-test)
  - [`lea docs`](#lea-docs)
  - [`lea diff`](#lea-diff)
  - [`lea teardown`](#lea-teardown)
  - [Development schema](#development-schema)
  - [Jinja templating](#jinja-templating)
  - [Python scripts](#python-scripts)
  - [Using `lea` as a library](#using-lea-as-a-library)
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
LEA_BQ_PROJECT_ID=carbonfact-gsheet
LEA_BQ_SERVICE_ACCOUNT=<a JSON dump of the service account file>
```

The `prepare` command has to be run once to create whatever needs creating. For instance, when working with BigQuery, a dataset has to be created:

```sh
lea prepare
```

### `lea run`

```sh
lea run ./views
    --production        # don't append a suffix to the schema name
    --fresh             # don't use the cache
    --dry               # don't actually run anything
    --only core.users   # only refresh this view
    --threads 4         # run 4 queries concurrently
    --show 20           # number of views showed during refresh
    --raise-exceptions  # raise exceptions instead of skipping
```

The queries in the `views` directory are run concurrently. They are organized in a DAG, and the DAG is traversed in a topological order. The DAG's structure is determined automatically by looking at the `FROM` clauses of the queries.

A `.cache.pkl` file is created during the run. This file is a checkpoint containing the state of the DAG. It is used to determine which queries to run next time. That is, if some queries have failed, only those queries and their descendants will be run again next time. This can be disabled with the `--fresh` flag.

### `lea test`

```sh
lea test ./views
    --threads 4         # run 4 queries concurrently
    --raise-exceptions  # raise exceptions instead of skipping
```

There are two types of tests:

- Singular tests -- these are queries which return failing rows. They are stored in a `tests` directory.
- Annotation tests -- these are comment annotations in the queries themselves:
  - `@UNIQUE` -- checks that a column's values are unique.

### `lea docs`

It is possible to generate documentation for the queries. This is done by inspecting the schema of the generated views and extracting the comments in the queries.

```sh
lea docs ./views
    --output-dir ./docs  # where to put the generated files
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

### Development schema

lea appends a `_<user>` suffix to schema names by default. This way you can have a development schema and a production schema. To

The `<user>` is determined automatically from the [login name](https://docs.python.org/3/library/getpass.html#getpass.getuser). It can be overriden by setting the `LEA_USERNAME` environment variable.

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

### Using `lea` as a library

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
- Archiving
- Historization
- Linting
- Data lineage
- Extending the CLI
- Only refresh what changed, based on git
- Splitting a query into CTEs
- Metric layer
- Shell auto-completion

Note: some of these features already exist at Carbonfact. We just don't feel they're polished enough for public consumption just yet.
