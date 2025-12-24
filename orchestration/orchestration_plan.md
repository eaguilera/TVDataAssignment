# TV Data Assignment -- Orchestration Plan (Task 4)
Author:

    Eng. Emerick Aguilera Gonzalez

## 1. Overview

This document describes an Airflow-style orchestration plan for the TV
viewing analytics pipeline. The pipeline coordinates Spark processing,
SQL analytics, dbt transformations, and data quality gating.

The pipeline is designed to run **daily** and supports TV platforms
across Sweden, Finland, and Norway. 

------------------------------------------------------------------------

## 2. High-Level DAG Flow

``` text
tv_data_assignment_dag
┌───────────────────────────────────────────────────────────┐
│ extract_and_validate_events (Spark)                       │
│  - validation (Task 1A)                                   │
│  - enrichment (Task 1B)                                   │
│  - summary JSON (Task 1C)                                 │
└─────────────┬─────────────────────────────────────────────┘
              │
              ▼
┌───────────────────────────────────────────────────────────┐
│ detect_schedule_overlaps (SQL)                            │
│  - broadcast overlaps (Task 2)                            │
└─────────────┬─────────────────────────────────────────────┘
              │
              ▼
┌───────────────────────────────────────────────────────────┐
│ dbt_build_dim_program_metadata (dbt)                      │
│  - SCD2 program dimension (Task 3)                        │
└─────────────┬─────────────────────────────────────────────┘
              │
              ▼
┌───────────────────────────────────────────────────────────┐
│ data_quality_gate                                         │
│  - invalid ratio threshold                                │
│  - severe overlap threshold                               │
│  - metadata completeness                                  │
└─────────────┬─────────────────────────────────────────────┘
      ┌───────┴─────────┐
      ▼                 ▼
┌────────────────┐  ┌────────────────────────────┐
│ pipeline_done  │  │ alert_failure (Slack/Email)│
└────────────────┘  └────────────────────────────┘
```

------------------------------------------------------------------------

## 3. Task Definitions

### 3.1 extract_and_validate_events (Spark)

**Inputs:** - viewing_events.csv - broadcast_schedule.csv -
program_metadata.csv

**Processing:** - Validation rules: - Missing end time - Negative
duration - Invalid tenant - Orphan linear events - SCD2 enrichment with
program metadata - Derived metrics: - viewing_duration_minutes -
completion_rate - Generate validation_summary.json

**Outputs:** - valid_events.parquet - invalid_events.parquet -
enriched_events.parquet - validation_summary.json

**Failure Policy:** - Any input read failure → task fails immediately -
Any Spark failure → triggers alert

------------------------------------------------------------------------

### 3.2 detect_schedule_overlaps (SQL)

**Inputs:** - broadcast_schedule table

**Processing:** - Self-join by tenant and channel - Interval overlap
detection - Calculate overlap duration - Flag severe overlaps (\>10
minutes)

**Outputs:** - schedule_overlaps table or view

**Failure Policy:** - SQL execution failure → pipeline halted

------------------------------------------------------------------------

### 3.3 dbt_build_dim_program_metadata (dbt)

**Inputs:** - Daily snapshot of program metadata

**Processing:** - Build stg_program_metadata - Build
dim_program_metadata with SCD2 logic

**Outputs:** - Historical program dimension table

**Failure Policy:** - dbt failure → pipeline halted and alerted

------------------------------------------------------------------------

### 3.4 data_quality_gate

**Inputs:** - validation_summary.json - schedule_overlaps table -
dim_program_metadata table

**Checks:** 1. Invalid event ratio ≤ 5% 2. Severe overlaps below
threshold 3. Metadata completeness ≥ 99%

**Behavior:** - Pass → pipeline completes successfully - Fail → triggers
alert_failure

------------------------------------------------------------------------

### 3.5 alert_failure

**Purpose:** - Notifies operators via Slack or email

**Trigger Rule:** - Executes when any upstream task fails

------------------------------------------------------------------------

## 4. Scheduling & SLA

-   **Schedule:** Daily at 03:00
-   **Estimated runtimes:**
    -   Spark processing: 30--45 min
    -   SQL overlaps: \< 10 min
    -   dbt build: 10--20 min
    -   DQ gate: \< 5 min

------------------------------------------------------------------------

## 5. Failure Handling Strategy

-   Fail-fast on data corruption
-   Automatic task retries (2 attempts)
-   Centralized alerting on failure
-   Manual re-run supported for failed partitions

------------------------------------------------------------------------

## 6. Observability

-   validation_summary.json used as daily quality report
-   schedule_overlaps used for conflict monitoring
-   Can be integrated with:
    -   Grafana
    -   Power BI
    -   Great Expectations

------------------------------------------------------------------------

## 7. Configuration

-   Airflow Variables for:
    -   Spark master
    -   Database connection
    -   dbt target
-   All paths configurable via environment variables

------------------------------------------------------------------------

✅ End of orchestration plan.
