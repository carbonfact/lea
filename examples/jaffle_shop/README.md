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
3. Skip feature can help fasten development cycle during WAP pattern. If a table is not passing through audit, all materialized tables won't be run again if the associated SQL script has'nt changed.
   If the script has changed, the audit table will be generated again, and all it's related childs in the DAG.

Let's take the example given in [README.md](README.md).

- Tables are materialized since you ran earlier `lea run`

## Write

- Add a new script `core/expenses.sql`

```sh
echo '''
with customer_orders as (

    select
        customer_id,

        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(order_id) as number_of_orders
    from staging.orders

    group by customer_id

),

customer_payments as (

    select
        orders.customer_id,
        sum(payments.amount) as total_amount

    from staging.payments as payments

    left join staging.orders as orders
        on payments.order_id = orders.order_id

    group by orders.customer_id

),

expenses as (
    select
    -- #UNIQUE
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order,
        customer_orders.most_recent_order,
        customer_orders.number_of_orders,
        -- #NO_NULLS
        customer_payments.total_amount as customer_lifetime_value
    from staging.customers as customers --comment here
    left join customer_orders  --comment here
      on customers.customer_id = customer_orders.customer_id  --comment here
    -- FROM customer_orders  --uncomment here
    -- left join staging.customers as customers  --uncomment here
    --     on  customer_orders.customer_id = customers.customer_id  --uncomment here
    left join customer_payments
        on customers.customer_id = customer_payments.customer_id
)

select * from expenses
''' > scripts/core/expenses.sql
```

## Audit

- Run the scripts `lea run` : `lea_duckdb_max.tests.core__expenses__customer_lifetime_value___no_nulls___audit` is failing ❌
- Uncomment and comment lines to reverse the JOIN orders, and exclude customers absent from orders tables.

```sh
sed -i '' '/--comment here/s/^/--/' scripts/core/expenses.sql
sed -i '' '/--uncomment here/s/-- //' scripts/core/expenses.sql
```

- Run again scripts, you should see that all stagings audit tables are not executed again.
- `core.expenses` is executed as lea detected modification on the script
- All tests are now passing 🎉
- Audit tables are wipped out from development warehouse.

## Publish

- As all tests passed, tables are materialized in the development warehouse.
- If you want now to run it against production and not development warehouse, you would add a `--production` flag to each command:

```sh
lea run --production
```
