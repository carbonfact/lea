# R2 bucket + DuckLake

```sh
echo "
LEA_USERNAME=max
LEA_WAREHOUSE=ducklake
LEA_DUCKLAKE_CATALOG_DATABASE=metadata.ducklake
LEA_DUCKLAKE_DATA_PATH=./data
" > .env
```

```sh
ln -s ../jaffle_shop/jaffle_shop jaffle_shop
```

```sh
lea run --scripts ../jaffle_shop/scripts
```
