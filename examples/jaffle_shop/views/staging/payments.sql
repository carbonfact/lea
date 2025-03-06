WITH raw_payments AS (SELECT * FROM 'jaffle_shop/seeds/raw_payments.csv')

SELECT
    id AS payments_id,
    order_id,
    payment_method,
    amount / 100 AS amount
FROM raw_payments;
