SELECT
    SUM(amount) AS total_order_value,
    AVG(amount) AS average_order_value
FROM core.orders
