-- Finds the busiest hour for each station by combining station metrics and trip patterns.
-- Only depends on analytics views, so in quack mode this is instant if analytics is populated.
-- In quack mode, this is transpiled and runs on DuckLake.
SELECT
    sm.station_id,
    sm.station_name,
    sm.n_total_trips,
    tp.hour_of_day AS peak_hour,
    tp.n_trips AS peak_hour_trips,
    tp.avg_duration_minutes AS peak_hour_avg_duration_minutes
FROM citibike.analytics__station_metrics AS sm
CROSS JOIN (
    SELECT
        hour_of_day,
        SUM(n_trips) AS n_trips,
        AVG(avg_duration_minutes) AS avg_duration_minutes
    FROM citibike.analytics__trip_patterns
    GROUP BY hour_of_day
    ORDER BY n_trips DESC
    LIMIT 1
) AS tp
WHERE sm.n_total_trips > 0
ORDER BY sm.n_total_trips DESC
