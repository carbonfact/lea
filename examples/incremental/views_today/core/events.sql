SELECT
    DATE '2023-01-01' + INTERVAL (i) DAY AS created_at,
    i + 1 AS day_of_year
FROM GENERATE_SERIES(1, 3) AS t(i)
