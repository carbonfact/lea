# Jaffle shop example

This example is taken from the [`jaffle_shop` example](https://github.com/dbt-labs/jaffle_shop/) from dbt. Here is the views file structure:

```
views
├── core
│   ├── customers.sql
│   └── orders.sql.jinja
├── staging
│   ├── customers.py
│   ├── orders.py
│   └── payments.py
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

The following command creates the `jaffle_shop_max.db` file and the necessary schemas:

```sh
lea prepare
```

```
Created schema analytics
Created schema core
Created schema staging
```

Now you can run the views:

```sh
lea run
```

There are a couple of cool things:

1. The staging schema is populated using Python scripts. Each one outputs a pandas DataFrame, which lea automcatically writes to DuckDB.
2. The `core.orders` table is created using a Jinja SQL script. lea will automatically run the script through Jinja, and then execute the resulting SQL.

You can then run some tests:

```sh
lea test
```

You can also generate documentation:

```sh
lea docs
```

This generates a [`docs` folder](docs) with the following structure:

```
docs
├── README.md
├── core
│   └── README.md
└── staging
    └── README.md
```

Now, if you were running in production and not in development mode, you would add a `--production` flag to each command:

```sh
lea prepare --production
lea run --production
lea test --production
lea docs --production
```
