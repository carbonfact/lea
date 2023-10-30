SELECT
    'n_orders' AS metric,
    COUNT(*) AS value
FROM
    core.orders
