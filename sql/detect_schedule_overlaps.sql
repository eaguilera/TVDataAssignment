/* ============================================================
   Task 2: Detect Overlapping Broadcast Schedules 
   Developed by: Emerick Aguilera Gonzalez
   ============================================================ */

WITH base AS (
    SELECT
        tenant,
        channel_id,
        schedule_id,
        CAST(broadcast_start AS TIMESTAMP) AS broadcast_start_ts,
        CAST(broadcast_end   AS TIMESTAMP) AS broadcast_end_ts
    FROM broadcast_schedule
),

pairwise_overlaps AS (
    SELECT
        a.tenant,
        a.channel_id,

        a.schedule_id AS schedule_id_1,
        b.schedule_id AS schedule_id_2,

        GREATEST(a.broadcast_start_ts, b.broadcast_start_ts) AS overlap_start,
        LEAST(a.broadcast_end_ts, b.broadcast_end_ts)       AS overlap_end

    FROM base a
    JOIN base b
        ON  a.tenant       = b.tenant
        AND a.channel_id   = b.channel_id
        AND a.schedule_id < b.schedule_id        -- prevents self-join + duplicates

        /* Core interval overlap condition */
        AND a.broadcast_start_ts < b.broadcast_end_ts
        AND b.broadcast_start_ts < a.broadcast_end_ts
),

final AS (
    SELECT
        tenant,
        channel_id,
        schedule_id_1,
        schedule_id_2,
        overlap_start,
        overlap_end,

        /* Overlap duration in minutes */
        ROUND(
            EXTRACT(EPOCH FROM (overlap_end - overlap_start)) / 60.0,
            2
        ) AS overlap_minutes
		---------------------------------------------------------------------
		-- Spark SQL 
		-- (unix_timestamp(overlap_end) - unix_timestamp(overlap_start)) / 60 AS overlap_minutes
		---------------------------------------------------------------------
		-- BigQuery
		-- TIMESTAMP_DIFF(overlap_end, overlap_start, MINUTE) AS overlap_minutes
		---------------------------------------------------------------------
		-- SnowFlake
		-- DATEDIFF(minute, overlap_start, overlap_end) AS overlap_minutes
		---------------------------------------------------------------------

    FROM pairwise_overlaps
)

SELECT
    tenant,
    channel_id,
    schedule_id_1,
    schedule_id_2,
    overlap_start,
    overlap_end,
    overlap_minutes,

    /* Severe overlap flag */
    CASE
        WHEN overlap_minutes > 10 THEN TRUE
        ELSE FALSE
    END AS is_severe_overlap

FROM final
ORDER BY tenant, channel_id, overlap_minutes DESC;
