# core

## Table of contents

- [core.customers](#corecustomers)
- [core.orders](#coreorders)

## Views

### core.customers

```sql
SELECT *
FROM core.customers
```

| Column                  | Description   | Unique   |
|:------------------------|:--------------|:---------|
| customer_id             |               | ✅       |
| first_name              |               |          |
| last_name               |               |          |
| first_order             |               |          |
| most_recent_order       |               |          |
| number_of_orders        |               |          |
| customer_lifetime_value |               |          |

### core.orders

```sql
SELECT *
FROM core.orders
```

| Column               | Description   | Unique   |
|:---------------------|:--------------|:---------|
| order_id             |               |          |
| customer_id          |               |          |
| order_date           |               |          |
| status               |               |          |
| credit_card_amount   |               |          |
| coupon_amount        |               |          |
| bank_transfer_amount |               |          |
| gift_card_amount     |               |          |
| amount               |               |          |

