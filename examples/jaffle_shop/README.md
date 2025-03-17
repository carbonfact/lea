# Jaffle shop example

This example is taken from the [`jaffle_shop` example](https://github.com/dbt-labs/jaffle_shop/) from dbt. Here is the scripts file structure:

```
scripts
├── analytics
│   ├── finance
│   │   └── kpis.sql
│   └── kpis.sql
├── core
│   ├── customers.sql
│   └── orders.sql.jinja
├── staging
│   ├── customers.sql
│   ├── orders.sql
│   └── payments.sql
└── tests
    └── orders_are_dated.sql
```

The first thing to do is create an `.env` file, as so:

```sh
echo "
LEA_USERNAME=max
LEA_WAREHOUSE=duckdb
LEA_DUCKDB_PATH=jaffle_shop.db
" > .env
```

This example uses DuckDB as the data warehouse. With lea, the convention when using DuckDB is to use a separate `.db` file per environment. For instance, in production, the file would be called `jaffle_shop.db`. In development, the file would be called `jaffle_shop_max.db`. The `max` suffix is the username from the `.env` file.

You can run the scripts:

```sh
lea run
```

lea will create audit tables, run tests against audit tables and if successfull.

There are a couple of cool things:

1. The staging schema is populated using SQL scripts and native DuckDB parsing of CSV files.
2. The `core.orders` table is created using a Jinja SQL script. lea will automatically run the script through Jinja, and then execute the resulting SQL.
3. Skip feature (TBC)

Now, if you were running in production and not in development mode, you would add a `--production` flag to each command:

```sh
lea run --production
```
