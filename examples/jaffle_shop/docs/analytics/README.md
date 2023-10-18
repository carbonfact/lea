# analytics

## Table of contents

- [analytics.finance.kpis](#analytics.finance.kpis)
- [analytics.kpis](#analytics.kpis)

## Views

### analytics.finance.kpis

```sql
SELECT *
FROM analytics_max.finance__kpis
```

| Column              | Type     | Description   | Unique   |
|:--------------------|:---------|:--------------|:---------|
| average_order_value | `DOUBLE` |               |          |
| total_order_value   | `DOUBLE` |               |          |

### analytics.kpis

```sql
SELECT *
FROM analytics_max.kpis
```

| Column   | Type      | Description   | Unique   |
|:---------|:----------|:--------------|:---------|
| metric   | `VARCHAR` |               |          |
| value    | `BIGINT`  |               |          |

