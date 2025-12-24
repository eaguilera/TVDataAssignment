{{ config(materialized='view') }}

/*
 *  int_schedule_events.sql
 *  Developed by: 
 *               Eng. Emerick Aguilera Gonzalez
 */
SELECT DISTINCT
    v.event_id,
    TRUE AS has_schedule_match
FROM {{ ref('stg_viewing_events') }} v
JOIN {{ ref('stg_broadcast_schedule') }} s
  ON v.tenant = s.tenant
 AND v.content_id = s.program_id
 AND v.start_time_ts >= s.broadcast_start_ts
 AND v.start_time_ts < s.broadcast_end_ts
WHERE v.event_type = 'linear'
