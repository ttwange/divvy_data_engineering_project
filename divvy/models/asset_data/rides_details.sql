-- models/bike_share/rides_with_details.sql
{{config(materialized='table',schema='staging')}}

WITH joined_data AS (
    SELECT
        r.ride_id,
        r.rideable_type,
        r.started_at,
        r.ended_at,
        r.start_station_name,
        r.start_station_id,
        s1.station_name AS start_station_name,
        r.end_station_name,
        r.end_station_id,
        s2.station_name AS end_station_name,
        r.start_lat,
        r.start_lng,
        r.end_lat,
        r.end_lng,
        m.membership_type AS member_casual
    FROM
        rides r
    LEFT JOIN
        stations s1 ON r.start_station_id = s1.station_id
    LEFT JOIN
        stations s2 ON r.end_station_id = s2.station_id
    LEFT JOIN
        memberships m ON r.member_casual = m.membership_id
)

SELECT * FROM joined_data
