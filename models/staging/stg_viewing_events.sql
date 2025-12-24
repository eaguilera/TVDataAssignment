{{ config(materialized='view') }}

/*
 *  Loading of the Viewing Events
 *  Developed by: 
 *               Eng. Emerick Aguilera Gonzalez
 */
WITH source AS (

    SELECT
        event_id,
        user_id,
        content_id,
        tenant,
        event_type,
        start_time,
        end_time,
        device_type
    FROM {{ source('raw', 'viewing_events') }}

),

typed AS (

    SELECT
        event_id,
        user_id,
        content_id,
        tenant,
        event_type,
        device_type,

        -- Raw strings
        start_time,
        end_time,

        -- Parsed timestamps
        start_time::timestamp AT TIME ZONE 'Europe/Vilnius'
            AS start_time_ts,

        CASE
            WHEN end_time IS NOT NULL AND trim(end_time) <> ''
            THEN end_time::timestamp AT TIME ZONE 'Europe/Vilnius'
            ELSE NULL
        END AS end_time_ts

    FROM source
)

SELECT *
FROM typed
