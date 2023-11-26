# analytics

## Table of contents

- [analytics.finance.kpis](#analyticsfinancekpis)
- [analytics.kpis](#analyticskpis)

## Views

### analytics.finance.kpis

```sql
SELECT *
FROM analytics.finance__kpis
```

| Column              | Type     | Description   | Unique   |
|:--------------------|:---------|:--------------|:---------|
| average_order_value | `DOUBLE` |               |          |
| total_order_value   | `DOUBLE` |               |          |

### analytics.kpis

```sql
SELECT *
FROM analytics.kpis
```

| Column   | Type      | Description   | Unique   |
|:---------|:----------|:--------------|:---------|
| metric   | `VARCHAR` |               |          |
| value    | `BIGINT`  |               |          |

