# MotherDuck

This example runs the [jaffle shop](../jaffle_shop/) pipeline on [MotherDuck](https://motherduck.com/). Local CSV files are read via hybrid execution and materialized as tables in MotherDuck.

Create a token at [app.motherduck.com/settings/tokens](https://app.motherduck.com/settings/tokens).

```sh
echo "
LEA_USERNAME=max
LEA_WAREHOUSE=motherduck
LEA_MOTHERDUCK_DATABASE=jaffle_shop
MOTHERDUCK_TOKEN=<your token>
" > .env
```

```sh
ln -s ../jaffle_shop/jaffle_shop jaffle_shop
```

```sh
lea run --scripts ../jaffle_shop/scripts
```
