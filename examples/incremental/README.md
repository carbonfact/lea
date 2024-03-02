```sh
echo "
LEA_USERNAME=max
LEA_WAREHOUSE=duckdb
LEA_DUCKDB_PATH=incremental.db
" > .env
```

```sh
lea prepare
```

```sh
lea run
```
