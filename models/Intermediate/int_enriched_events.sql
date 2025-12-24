{{ config(materialized='view') }}

/*
 *  int_enriched_events.sql
 *  Developed by: 
 *               Eng. Emerick Aguilera Gonzalez
 */
SELECT
    v.*,
    COALESCE(m.has_schedule_match, FALSE) AS has_schedule_match,

    p.title,
    p.genre,
    p.program_duration_minutes,

    ROUND(
        EXTRACT(EPOCH FROM (v.end_time_ts - v.start_time_ts)) / 60,
        2
    ) AS viewing_duration_minutes,

    ROUND(
        LEAST(
            100.0,
            (
              EXTRACT(EPOCH FROM (v.end_time_ts - v.start_time_ts)) / 60
              / p.program_duration_minutes
            ) * 100
        ),
        2
    ) AS completion_rate

FROM {{ ref('int_validated_events') }} v

LEFT JOIN {{ ref('int_schedule_matches') }} m
  ON v.event_id = m.event_id

LEFT JOIN {{ ref('stg_program_metadata') }} p
  ON v.content_id = p.program_id
 AND v.start_time_ts >= p.valid_from_ts
 AND v.start_time_ts < p.valid_to_ts
