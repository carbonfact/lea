# staging

## Table of contents

- [staging.customers](#staging.customers)
- [staging.orders](#staging.orders)
- [staging.payments](#staging.payments)

## Views

### staging.customers

Docstring for the customers view.

```sql
SELECT *
FROM staging.customers
```

| Column      | Type      | Description   | Unique   |
|:------------|:----------|:--------------|:---------|
| customer_id | `BIGINT`  |               |          |
| first_name  | `VARCHAR` |               |          |
| last_name   | `VARCHAR` |               |          |

### staging.orders

Docstring for the orders view.

```sql
SELECT *
FROM staging.orders
```

| Column      | Type      | Description   | Unique   |
|:------------|:----------|:--------------|:---------|
| customer_id | `BIGINT`  |               |          |
| order_date  | `VARCHAR` |               |          |
| order_id    | `BIGINT`  |               |          |
| status      | `VARCHAR` |               |          |

### staging.payments

```sql
SELECT *
FROM staging.payments
```

| Column         | Type      | Description   | Unique   |
|:---------------|:----------|:--------------|:---------|
| amount         | `DOUBLE`  |               |          |
| order_id       | `BIGINT`  |               |          |
| payment_id     | `BIGINT`  |               |          |
| payment_method | `VARCHAR` |               |          |

