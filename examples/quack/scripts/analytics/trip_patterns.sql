-- Aggregates trip patterns by hour, day of week, and user type.
-- In quack mode, this is transpiled and runs on DuckLake.
SELECT
    EXTRACT(HOUR FROM started_at) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM started_at) AS day_of_week,
    user_type,
    COUNT(*) AS n_trips,
    AVG(trip_duration_seconds) AS avg_duration_seconds,
    AVG(trip_duration_minutes) AS avg_duration_minutes
FROM citibike.core__trips
GROUP BY
    EXTRACT(HOUR FROM started_at),
    EXTRACT(DAYOFWEEK FROM started_at),
    user_type
