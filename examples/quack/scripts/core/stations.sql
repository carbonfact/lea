-- Reads from BigQuery public data (external source, not part of the DAG).
-- In quack mode, this runs on BigQuery.
SELECT
    CAST(station_id AS STRING) AS station_id,
    name AS station_name,
    short_name,
    latitude,
    longitude,
    capacity,
    num_bikes_available,
    num_docks_available,
    is_installed,
    is_renting,
    is_returning
FROM `bigquery-public-data.new_york_citibike.citibike_stations`
