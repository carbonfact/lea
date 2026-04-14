# R2 bucket + DuckLake

This example runs the [jaffle shop](../jaffle_shop/) pipeline with [DuckLake](https://ducklake.select/) as the warehouse, storing Parquet files in a [Cloudflare R2](https://developers.cloudflare.com/r2/) bucket. The DuckLake metadata catalog is kept locally in a `.ducklake` file, while the actual data lives in R2 — giving you a serverless data lake with S3-compatible storage.

Create a bucket in the Cloudflare dashboard, then generate an R2 API token by following [these instructions](https://developers.cloudflare.com/r2/api/s3/tokens/). You'll need the Access Key ID, Secret Access Key, and your Account ID.

```sh
echo "
LEA_USERNAME=max
LEA_WAREHOUSE=ducklake
LEA_DUCKLAKE_CATALOG_DATABASE=metadata.ducklake
LEA_DUCKLAKE_DATA_PATH=r2://your-bucket-name
LEA_DUCKLAKE_SECRET=TYPE r2, KEY_ID 'your_access_key_id', SECRET 'your_secret_access_key', ACCOUNT_ID 'your_account_id'
" > .env
```

The `LEA_DUCKLAKE_SECRET` value is the body of a DuckDB [`CREATE SECRET`](https://duckdb.org/docs/current/configuration/secrets_manager) statement. This same pattern works for S3, GCS, Azure, etc.

Symlink the jaffle shop scripts:

```sh
ln -s ../jaffle_shop/jaffle_shop jaffle_shop
```

And then run the scripts:

```sh
lea run --scripts ../jaffle_shop/scripts
```

This will create the `staging`, `core`, and `analytics` tables as Parquet files in your R2 bucket, with the metadata tracked locally in `metadata.ducklake`.
