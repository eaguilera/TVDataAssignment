-- ============================================================================
--  schema.sql  —  PostgreSQL schema for TV Viewing Assignment
-- Created by Emerick Aguilera Gonzalez
-- ============================================================================

SET client_min_messages = WARNING;

-- Drop tables if they already exist (CI-friendly cleanup)
DROP TABLE IF EXISTS viewing_events CASCADE;
DROP TABLE IF EXISTS broadcast_schedule CASCADE;
DROP TABLE IF EXISTS program_metadata CASCADE;
DROP TABLE IF EXISTS valid_events CASCADE;
DROP TABLE IF EXISTS enriched_events CASCADE;

-- ============================================================================
-- 1) Raw input tables (CSV imports)
-- ============================================================================

CREATE TABLE viewing_events (
    event_id        TEXT PRIMARY KEY,
    user_id         TEXT,
    content_id      TEXT,
    tenant          TEXT,
    event_type      TEXT,
    start_time      TEXT,           -- raw CSV format
    end_time        TEXT,
    device_type     TEXT
);

CREATE TABLE broadcast_schedule (
    schedule_id     TEXT PRIMARY KEY,
    tenant          TEXT,
    channel_id      TEXT,
    program_id      TEXT,
    broadcast_start TEXT,
    broadcast_end   TEXT
);

CREATE TABLE program_metadata (
    program_id                  TEXT,
    title                       TEXT,
    genre                       TEXT,
    program_duration_minutes    NUMERIC,
    valid_from                  TEXT,
    valid_to                    TEXT
);

-- ============================================================================
-- 2) Normalized & validated tables (intermediate transformations)
-- ============================================================================

CREATE TABLE valid_events (
    event_id        TEXT PRIMARY KEY,
    user_id         TEXT,
    content_id      TEXT,
    tenant          TEXT,
    event_type      TEXT,
    device_type     TEXT,
    start_time_ts   TIMESTAMP,
    end_time_ts     TIMESTAMP,
    is_valid        BOOLEAN,
    validation_reason TEXT
);

-- ============================================================================
-- 3) Enriched SCD2 join output
-- ============================================================================

CREATE TABLE enriched_events (
    event_id                    TEXT PRIMARY KEY,
    user_id                     TEXT,
    content_id                  TEXT,
    tenant                      TEXT,
    device_type                 TEXT,
    event_type                  TEXT,
    start_time_ts               TIMESTAMP,
    end_time_ts                 TIMESTAMP,
    schedule_matched            BOOLEAN,
    program_id                  TEXT,
    title                       TEXT,
    genre                       TEXT,
    program_duration_minutes    NUMERIC,
    valid_from_ts               TIMESTAMP,
    valid_to_ts                 TIMESTAMP,
    viewing_duration_minutes    NUMERIC,
    completion_rate             NUMERIC
);
