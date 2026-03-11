-- Reads from BigQuery public data (external source, not part of the DAG).
-- In quack mode, this runs on BigQuery.
SELECT
    CAST(bikeid AS STRING) AS bike_id,
    usertype AS user_type,
    tripduration AS trip_duration_seconds,
    ROUND(tripduration / 60.0, 1) AS trip_duration_minutes,
    starttime AS started_at,
    stoptime AS ended_at,
    CAST(start_station_id AS STRING) AS start_station_id,
    start_station_name,
    start_station_latitude,
    start_station_longitude,
    CAST(end_station_id AS STRING) AS end_station_id,
    end_station_name,
    end_station_latitude,
    end_station_longitude,
    birth_year,
    gender
FROM `bigquery-public-data.new_york_citibike.citibike_trips`
WHERE starttime >= '2018-01-01'
  AND starttime < '2018-02-01'
