{{ config(materialized='view') }}

/*
 *  int_valudated_events
 *  Developed by: 
 *               Eng. Emerick Aguilera Gonzalez
 */
SELECT
    *,
    ARRAY_REMOVE(ARRAY[
        CASE WHEN end_time_ts IS NULL THEN 'MISSING_END_TIME' END,
        CASE WHEN end_time_ts < start_time_ts THEN 'NEGATIVE_DURATION' END,
        CASE WHEN tenant NOT IN ('swe','fin','nor') THEN 'INVALID_TENANT' END
    ], NULL) AS validation_reasons
FROM {{ ref('stg_viewing_events') }}
