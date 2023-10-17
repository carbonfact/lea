SELECT
    'n_customers' AS metric,
    COUNT(*) AS value
FROM
    jaffle_shop.core__customers

UNION ALL

SELECT
    'n_orders' AS metric,
    COUNT(*) AS value
FROM
    jaffle_shop.core__orders
