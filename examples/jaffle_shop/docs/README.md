# Views

## Schemas

- [`analytics`](./analytics)
- [`core`](./core)
- [`staging`](./staging)

## Schema flowchart

```mermaid
%%{init: {"flowchart": {"defaultRenderer": "elk"}} }%%
flowchart TB
    analytics(analytics)
    staging(staging)
    core(core)
    staging --> core
    core --> analytics
```

## Flowchart

```mermaid
%%{init: {"flowchart": {"defaultRenderer": "elk"}} }%%
flowchart TB
    subgraph analytics
    analytics.kpis(kpis)
    end

    subgraph core
    core.customers(customers)
    core.orders(orders)
    end

    subgraph staging
    staging.customers(customers)
    staging.orders(orders)
    staging.payments(payments)
    end

    staging.customers --> core.customers
    staging.orders --> core.customers
    staging.payments --> core.customers
    staging.orders --> core.orders
    staging.payments --> core.orders
    core.customers --> analytics.kpis
    core.orders --> analytics.kpis
```

