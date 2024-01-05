# Compare development to production

The first thing to do is create an `.env` file, as so:

```sh
echo "
LEA_USERNAME=max
LEA_WAREHOUSE=duckdb
LEA_DUCKDB_PATH=jaffle_shop.db
" > .env
```

This example is about comparing data in development to what's in production. For the purpose of this example, there's a `views/prod` directory and a `views/dev` directory.

Let's start by running the views in production. First, the schemas needs to be created:

```sh
lea prepare views/prod --production
```

```
Created schema staging
Created schema core
```

The views can now be run in production:

```sh
lea run views/prod --production
```

Now let's say we're working in development. We would start by creating the schemas:

```sh
lea prepare views/dev
```

```
Created schema staging
Created schema core
Created schema analytics
```

We do some changes by editing the `views/dev` directory. Then we can run the views in development:

```sh
lea run views/dev
```

Now we can compare the data in development to the data in production:

```sh
lea diff
```

```diff
+ analytics.kpis
+ 1 rows
+ metric
+ value

- core.customers
- 100 rows
- customer_id
- customer_lifetime_value
- first_name
- first_order
- last_name
- most_recent_order
- number_of_orders

  core.orders
- 29 rows
```

The diff shows several things:

- The `customers` view got dropped.
- The `orders` didn't get dropped, but it lost some rows. This is because we added a `WHERE` to the underlying SQL.
- The `kpis` view got added, and it contains a single row.

The nice thing is that `lea diff` prints out a neat summary. This output can be highlighted on GitHub, which what we've done above, by using a `diff` code block.

In a pull request, an automated message can be posted with the diff. Here is an example of a GitHub action that does this:

````yaml
name: Branch tests

on:
  pull_request:
    branches:
      - "*"

jobs:
  run:
    runs-on: ubuntu-latest
    env:
      LEA_WAREHOUSE: bigquery
      LEA_BQ_SERVICE_ACCOUNT: ${{ secrets.LEA_BQ_SERVICE_ACCOUNT }}
      LEA_BQ_LOCATION: EU
      LEA_BQ_PROJECT_ID: carbonlytics
      LEA_SCHEMA: kaya
    steps:
      - uses: actions/checkout@v4
      - uses: ./.github/actions/install-env

      - name: Check code quality
        run: poetry run pre-commit run --all-files

      - name: Set environment variables
        run: |
          export PR_NUMBER=$(cut -d'/' -f3 <<< "$GITHUB_REF")
          export LEA_USERNAME="pr$PR_NUMBER"
          echo "LEA_USERNAME=$LEA_USERNAME" >> $GITHUB_ENV

      - name: Create BigQuery dataset for this pull request
        run: poetry run lea prepare

      - name: Refresh views
        run: poetry run lea run --raise-exceptions

      - name: Calculate diff
        run: |
          export DIFF=$(poetry run lea diff kaya_$LEA_USERNAME kaya)
          DIFF=$(echo "$DIFF" | sed '1d')
          EOF=$(dd if=/dev/urandom bs=15 count=1 status=none | base64)
          echo "DIFF<<$EOF" >> "$GITHUB_ENV"
          echo "$DIFF" >> "$GITHUB_ENV"
          echo "$EOF" >> "$GITHUB_ENV"

      - name: Comment PR with execution number
        uses: thollander/actions-comment-pull-request@v2
        with:
          message: |
            ```diff
            ${{ env.DIFF }}
            ```
          comment_tag: execution

      - name: Run tests
        run: poetry run lea test --raise-exceptions
````
