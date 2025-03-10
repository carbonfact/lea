WITH raw_orders AS (
    SELECT
        id,
        user_id,
        order_date,
        status
    FROM 'jaffle_shop/seeds/raw_orders.csv'
)

SELECT
    id AS order_id,
    user_id AS customer_id,
    order_date,
    status
FROM raw_orders;
