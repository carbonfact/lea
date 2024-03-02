SELECT
    -- @INCREMENTAL
    DATE '2023-01-01' + INTERVAL (i) DAY AS date,
    i AS day_of_year
FROM GENERATE_SERIES(1, 5) AS t(i)
