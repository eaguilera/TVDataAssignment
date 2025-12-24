{{ config(materialized='view') }}

/*
 *  Loading of the Broadcast Schedule
 *  Developed by: 
 *               Eng. Emerick Aguilera Gonzalez
 */
WITH source AS (

    SELECT
        schedule_id,
        tenant,
        channel_id,
        program_id,
        broadcast_start,
        broadcast_end
    FROM {{ source('raw', 'broadcast_schedule') }}

),

typed AS (

    SELECT
        schedule_id,
        tenant,
        channel_id,
        program_id,

        broadcast_start::timestamp AT TIME ZONE 'Europe/Vilnius'
            AS broadcast_start_ts,

        broadcast_end::timestamp AT TIME ZONE 'Europe/Vilnius'
            AS broadcast_end_ts

    FROM source
)

SELECT *
FROM typed
