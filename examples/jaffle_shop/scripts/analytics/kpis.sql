SELECT
    'n_customers' AS metric,
    COUNT(*) AS value
FROM
    core.customers

UNION ALL

SELECT
    'n_orders' AS metric,
    COUNT(*) AS value
FROM
    core.orders
