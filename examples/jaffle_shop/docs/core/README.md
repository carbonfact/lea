# core

## Table of contents

- [customers](#customers)
- [orders](#orders)

## Views

### customers

```sql
SELECT *
FROM jaffle_shop_max.core__customers
```

| Column                  | Type      | Description   | Unique   |
|:------------------------|:----------|:--------------|:---------|
| customer_id             | `BIGINT`  |               | ✅       |
| customer_lifetime_value | `DOUBLE`  |               |          |
| first_name              | `VARCHAR` |               |          |
| first_order             | `VARCHAR` |               |          |
| last_name               | `VARCHAR` |               |          |
| most_recent_order       | `VARCHAR` |               |          |
| number_of_orders        | `BIGINT`  |               |          |

### orders

```sql
SELECT *
FROM jaffle_shop_max.core__orders
```

| Column               | Type      | Description   | Unique   |
|:---------------------|:----------|:--------------|:---------|
| amount               | `DOUBLE`  |               |          |
| bank_transfer_amount | `DOUBLE`  |               |          |
| coupon_amount        | `DOUBLE`  |               |          |
| credit_card_amount   | `DOUBLE`  |               |          |
| customer_id          | `BIGINT`  |               |          |
| gift_card_amount     | `DOUBLE`  |               |          |
| order_date           | `VARCHAR` |               |          |
| order_id             | `BIGINT`  |               |          |
| status               | `VARCHAR` |               |          |

