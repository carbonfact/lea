-- Computes per-station departure and arrival metrics from trip data.
-- In quack mode, this is transpiled and runs on DuckLake.
WITH departures AS (
    SELECT
        start_station_id AS station_id,
        start_station_name AS station_name,
        COUNT(*) AS n_departures,
        AVG(trip_duration_seconds) AS avg_departure_duration_seconds
    FROM citibike.core__trips
    GROUP BY start_station_id, start_station_name
),

arrivals AS (
    SELECT
        end_station_id AS station_id,
        COUNT(*) AS n_arrivals
    FROM citibike.core__trips
    GROUP BY end_station_id
)

SELECT
    d.station_id,
    d.station_name,
    d.n_departures,
    COALESCE(a.n_arrivals, 0) AS n_arrivals,
    d.n_departures + COALESCE(a.n_arrivals, 0) AS n_total_trips,
    d.avg_departure_duration_seconds
FROM departures AS d
LEFT JOIN arrivals AS a ON d.station_id = a.station_id
