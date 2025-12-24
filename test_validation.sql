-- ============================================================================
-- test_validation.sql  —  SQL test suite for TV Viewing Assignment
-- Created by Emerick Aguilera Gonzalez
-- ============================================================================

SET client_min_messages = WARNING;

-------------------------------------------------------------------------------
-- TEST 1 — MISSING_END_TIME rule
-------------------------------------------------------------------------------
WITH bad AS (
    SELECT *
    FROM valid_events
    WHERE validation_reason LIKE '%MISSING_END_TIME%'
      AND end_time_ts IS NOT NULL
)
SELECT 'TEST_1_FAIL' AS test_name, * FROM bad;

-------------------------------------------------------------------------------
-- TEST 2 — NEGATIVE_DURATION rule
-------------------------------------------------------------------------------
WITH bad AS (
    SELECT *
    FROM valid_events
    WHERE validation_reason LIKE '%NEGATIVE_DURATION%'
      AND end_time_ts >= start_time_ts
)
SELECT 'TEST_2_FAIL' AS test_name, * FROM bad;

-------------------------------------------------------------------------------
-- TEST 3 — INVALID_TENANT rule
-------------------------------------------------------------------------------
WITH bad AS (
    SELECT *
    FROM valid_events
    WHERE validation_reason LIKE '%INVALID_TENANT%'
      AND tenant IN ('swe','fin','nor')
)
SELECT 'TEST_3_FAIL' AS test_name, * FROM bad;

-------------------------------------------------------------------------------
-- TEST 4 — NO_SCHEDULE_MATCH rule
-- If rule fires, has_schedule_match must be FALSE
-------------------------------------------------------------------------------
WITH bad AS (
    SELECT *
    FROM valid_events v
    JOIN enriched_events e USING (event_id)
    WHERE v.validation_reason LIKE '%NO_SCHEDULE_MATCH%'
      AND e.schedule_matched = TRUE
)
SELECT 'TEST_4_FAIL' AS test_name, * FROM bad;

-------------------------------------------------------------------------------
-- TEST 5 — SCD2 join logic must select only programs where:
-- valid_from_ts <= event_start < valid_to_ts
-------------------------------------------------------------------------------
WITH bad AS (
    SELECT *
    FROM enriched_events
    WHERE program_id IS NOT NULL
      AND NOT (start_time_ts >= valid_from_ts
               AND start_time_ts < valid_to_ts)
)
SELECT 'TEST_5_FAIL' AS test_name, * FROM bad;

-------------------------------------------------------------------------------
-- TEST 6 — viewing_duration_minutes correctness
-------------------------------------------------------------------------------
WITH recomputed AS (
    SELECT
        e.*,
        ROUND(EXTRACT(EPOCH FROM (end_time_ts - start_time_ts)) / 60.0, 2)
            AS expected_duration
    FROM enriched_events e
),
bad AS (
    SELECT *
    FROM recomputed
    WHERE viewing_duration_minutes IS NOT NULL
      AND viewing_duration_minutes <> expected_duration
)
SELECT 'TEST_6_FAIL' AS test_name, * FROM bad;

-------------------------------------------------------------------------------
-- TEST 7 — completion_rate correctness
-------------------------------------------------------------------------------
WITH recomputed AS (
    SELECT
        e.*,
        ROUND(LEAST(100.0,
               (viewing_duration_minutes / program_duration_minutes) * 100.0
        ), 2) AS expected_rate
    FROM enriched_events e
),
bad AS (
    SELECT *
    FROM recomputed
    WHERE completion_rate IS NOT NULL
      AND completion_rate <> expected_rate
)
SELECT 'TEST_7_FAIL' AS test_name, * FROM bad;

-------------------------------------------------------------------------------
-- END OF TEST SUITE
-------------------------------------------------------------------------------
