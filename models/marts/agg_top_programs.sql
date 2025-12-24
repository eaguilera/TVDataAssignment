{{ config(materialized='table') }}

/*
 * agg_top_programs.sql
 *  Top 3 programs in the viewing events table
 *  Developed by Eng. Emerick Aguilera Gonzalez
 */
SELECT
    program_id,
    ROUND(SUM(viewing_duration_minutes), 2) AS total_viewing_minutes
FROM {{ ref('fct_enriched_events') }}
GROUP BY program_id
ORDER BY total_viewing_minutes DESC
LIMIT 3
