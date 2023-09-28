# staging

## Table of contents

- [customers](#customers)
- [orders](#orders)
- [payments](#payments)

## Views

### customers

Docstring for the customers view.

```sql
SELECT *
FROM jaffle_shop_max.staging__customers
```

| Column      | Type      | Description   | Unique   |
|:------------|:----------|:--------------|:---------|
| customer_id | `BIGINT`  |               |          |
| first_name  | `VARCHAR` |               |          |
| last_name   | `VARCHAR` |               |          |

### orders

Docstring for the orders view.

```sql
SELECT *
FROM jaffle_shop_max.staging__orders
```

| Column      | Type      | Description   | Unique   |
|:------------|:----------|:--------------|:---------|
| customer_id | `BIGINT`  |               |          |
| order_date  | `VARCHAR` |               |          |
| order_id    | `BIGINT`  |               |          |
| status      | `VARCHAR` |               |          |

### payments

```sql
SELECT *
FROM jaffle_shop_max.staging__payments
```

| Column         | Type      | Description   | Unique   |
|:---------------|:----------|:--------------|:---------|
| amount         | `DOUBLE`  |               |          |
| order_id       | `BIGINT`  |               |          |
| payment_id     | `BIGINT`  |               |          |
| payment_method | `VARCHAR` |               |          |

