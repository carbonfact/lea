# Using MotherDuck

lea works with DuckDB, and thus can be used with [MotherDuck](https://motherduck.com/) too.

Here is an example `.env` file:

```sh
echo "
LEA_USERNAME=max
LEA_WAREHOUSE=duckdb
LEA_DUCKDB_PATH=md:jaffle_shop
MOTHERDUCK_TOKEN=<provided by MotherDuck>
" > .env
```

The token can be obtained by logging into MotherDuck from the terminal, as documented [here](https://motherduck.com/docs/getting-started/connect-query-from-python/installation-authentication#authenticating-to-motherduck).

Then, you can run the usual commands. For the sake of example, let's re-use the jaffle shop views:

```sh
lea prepare ../jaffle_shop/views
```

```
Created schema analytics
Created schema staging
Created schema core
```

```sh
lea run ../jaffle_shop/views
```

You should see the views in your MotherDuck UI:
