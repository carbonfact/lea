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
    core(core)
    staging(staging)
    core --> analytics
    staging --> core
```

## Flowchart

```mermaid
%%{init: {"flowchart": {"defaultRenderer": "elk"}} }%%
flowchart TB

    subgraph analytics

    subgraph finance
        analytics.finance.kpis(kpis)
    end

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

    core.orders --> analytics.finance.kpis
    core.customers --> analytics.kpis
    core.orders --> analytics.kpis
    staging.customers --> core.customers
    staging.orders --> core.customers
    staging.payments --> core.customers
    staging.orders --> core.orders
    staging.payments --> core.orders
```

