![CI](https://github.com/eaguilera/Assignment_Telia/actions/workflows/ci.yml/badge.svg)
![Dashboard](https://img.shields.io/badge/Dashboard-Live-brightgreen)
# Assignment_Telia
# TV Data Engineering Assignment

Author:

    Eng. Emerick Aguilera Gonzalez

This repository contains the solution for the **TV Viewing Analytics
Pipeline** technical assignment.\
The pipeline simulates a real-world streaming/linear TV data platform
covering:

-   **Viewing event validation & enrichment (Spark)**
-   **Broadcast schedule overlap detection (SQL)**
-   **Slowly Changing Dimension Type 2 design with dbt**
-   **Production-style orchestration design (Airflow-style)**

The solution is designed with a **production mindset**, focusing on: -
Data quality - Historical correctness (SCD2) - Clear orchestration -
Robust error handling

------------------------------------------------------------------------

## Task Description Status

| Task | Description | Status |
|------|-------------|--------|
| 1A | Spark ingestion & validation | ✅ |
| 1B | Spark enrichment with SCD2 metadata | ✅ |
| 1C | Validation summary JSON | ✅ |
| 2  | SQL broadcast schedule overlap detection | ✅ |
| 3  | dbt SCD2 dimension design (pseudocode) | ✅ |
| 4  | Orchestration design (Airflow-style DAG) | ✅ |


------------------------------------------------------------------------

## Repository Structure

    Assignment_TV_Viewing/
    ├── README.md
    ├── APPROACH.md
    ├── requirements.txt
	├── dashboard/
	│   ├── data_quality_dashboard.html
	│   ├── generate_data_quality_dashboard.py
	│   ├── README.md
	│   └── screenshot.png
    ├── data/
    │   ├── viewing_events.csv
    │   ├── broadcast_schedule.csv
    │   └── program_metadata.csv
    ├── models/
    │   └── marts/
    │       └── dim_program_metadata.sql
    │   └── staging/
    │       └── stg_program_metadata.sql
    ├── scripts/
    │   └── ingest_and_validate.py
    ├── sql/
    │   ├── detect_schedule_overlaps.sql
    │   ├── Get_Current_Metadata.sql
    │   └── Get_Specific_Metadata.sql
    ├── orchestration/
    │   ├── orchestration_plan.md
    │   └── tv_data_assignment_dag.py
    ├── output/
    │   ├── valid_events.parquet/
    │   ├── invalid_events.parquet/
    │   ├── enriched_events.parquet/
    │   ├── enriched_events_csv/ 
    │   └── validation_summary.json/
    └── tests/
        └── test_validation_rules.py

------------------------------------------------------------------------

## Tech Stack

-   **Apache Spark (PySpark)** --- validation & enrichment
-   **SQL (ANSI-style)** --- schedule overlap detection
-   **dbt (pseudocode)** --- SCD2 program dimension modeling
-   **Airflow-style DAG** --- orchestration design
-   **PyTest + Local Spark** --- unit testing

------------------------------------------------------------------------

## How to Run

### 1. Install dependencies

``` bash
pip install -r requirements.txt
```

Example `requirements.txt`:

``` txt
# Core
pyspark==3.5.1
pandas==2.2.2
numpy==1.26.4

# Dashboard
plotly==5.22.0

# Testing
pytest==8.2.0

# Utilities
python-dateutil==2.9.0.post0
```

------------------------------------------------------------------------

### 2. Run Task 1 (Validation + Enrichment + Summary)

``` bash
spark-submit scripts/validate_and_enrich.py   --data-dir data   --output-dir output   --write-human-readable
```

This generates:

-   `output/valid_events.parquet`
-   `output/invalid_events.parquet`
-   `output/enriched_events.parquet`
-   `output/validation_summary.json`
-   Optional CSV copies for inspection

------------------------------------------------------------------------

### 3. Run Task 2 (Schedule Overlap SQL)

Run in your SQL engine of choice:

``` sql
sql/detect_schedule_overlaps.sql
```

------------------------------------------------------------------------

### 4. Run Unit Tests

``` bash
pytest tests/
```

------------------------------------------------------------------------

## Task Details

### Task 1 --- Spark Validation & Enrichment

**Validation Rules Implemented:** - `MISSING_END_TIME` -
`NEGATIVE_DURATION` - `INVALID_TENANT` - `NO_SCHEDULE_MATCH` (linear
only)

**Enrichment:** - Point-in-time SCD2 join to `program_metadata` -
Added: - `program_title` - `genre` - `program_duration_minutes` -
Derived metrics: - `viewing_duration_minutes` - `completion_rate`
(capped at 100%, rounded to 2 decimals)

**Summary Output (`validation_summary.json`):** - Total events -
Valid/invalid counts - Top 3 programs by viewing duration

------------------------------------------------------------------------

### Task 2 --- Schedule Overlap Detection

-   Self-join on `tenant` and `channel_id`

-   Overlap logic:

        a.start < b.end AND b.start < a.end

-   Calculates:

    -   Overlap window
    -   Overlap duration in minutes
    -   Severe overlap flag (\> 10 minutes)

------------------------------------------------------------------------

### Task 3 --- dbt SCD2 Design

-   Staging model:
    -   `stg_program_metadata`
-   Dimension model:
    -   `dim_program_metadata`
-   Supports:
    -   `valid_from`, `valid_to`
    -   `is_current`
    -   Surrogate key generation

------------------------------------------------------------------------

### Task 4 --- Orchestration Design

Airflow-style DAG coordinating:

1.  Spark validation + enrichment
2.  SQL overlap detection
3.  dbt SCD2 dimension build
4.  Data quality gate
5.  Failure alerting (Slack/Email)

------------------------------------------------------------------------

## Data Quality & Observability

-   `validation_summary.json` provides:
    -   High-level quality metrics
    -   Top-viewed programs

------------------------------------------------------------------------

## Reviewer Access

Repository access has been granted to:

**GitHub ID:** `PandeeshwaranPothirajan`


