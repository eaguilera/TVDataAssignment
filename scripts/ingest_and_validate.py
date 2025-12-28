#!/usr/bin/env python3

#
# Program designed and executed by 
# Eng. Emerick Aguilera Gonzalez
# 2025-12
#

from __future__ import annotations 

import argparse
from typing import List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


# -------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------

VALID_TENANTS: List[str] = ["swe", "fin", "nor"]


# -------------------------------------------------------------------
# Custom Error
# -------------------------------------------------------------------

class PipelineError(RuntimeError):
    """Raised when the ingestion/validation/enrichment pipeline fails."""


# -------------------------------------------------------------------
# Argument Parsing
# -------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TV Data Assignment - Task 1A + 1B + 1C")
    parser.add_argument("--data-dir", default="data", help="Input CSV directory")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    parser.add_argument(
        "--write-human-readable",
        action="store_true",
        help="Write additional CSV outputs for inspection",
    )
    return parser.parse_args()


# -------------------------------------------------------------------
# Data Loading
# -------------------------------------------------------------------

def load_data(spark: SparkSession, data_dir: str) -> Tuple[DataFrame, DataFrame, DataFrame]:

    try:
        viewing_df = (
            spark.read.option("header", True)
            .csv(f"{data_dir}/viewing_events.csv")
            .withColumn("start_time_ts", F.to_timestamp("start_time"))
            .withColumn("end_time_ts", F.to_timestamp("end_time"))
        )

        schedule_df = (
            spark.read.option("header", True)
            .csv(f"{data_dir}/broadcast_schedule.csv")
            .withColumn("broadcast_start_ts", F.to_timestamp("broadcast_start"))
            .withColumn("broadcast_end_ts", F.to_timestamp("broadcast_end"))
        )

        program_df = (
            spark.read.option("header", True)
            .csv(f"{data_dir}/program_metadata.csv")
            .withColumn("valid_from_ts", F.to_timestamp("valid_from"))
            .withColumn("valid_to_ts", F.to_timestamp("valid_to"))
            # HARD SAFETY FILTER — remove impossible Windows timestamps
            .filter(
                F.col("valid_from_ts").between("1900-01-01", "2100-01-01")
                & F.col("valid_to_ts").between("1900-01-01", "2100-01-01")
            )
            .withColumn(
                "program_duration_minutes",
                F.col("duration_minutes").cast("int")
            )
        )

        if viewing_df.rdd.isEmpty():
            raise PipelineError("viewing_events.csv is empty")
        if schedule_df.rdd.isEmpty():
            raise PipelineError("broadcast_schedule.csv is empty")
        if program_df.rdd.isEmpty():
            raise PipelineError("program_metadata.csv is empty")

        return viewing_df, schedule_df, program_df

    except Exception as exc:
        raise PipelineError(f"Failed loading datasets: {exc}") from exc


# -------------------------------------------------------------------
# Schedule Matching (Task 1A)
# -------------------------------------------------------------------

def find_schedule_matches(viewing_df: DataFrame, schedule_df: DataFrame) -> DataFrame:

    # Execute the WHERE clause of the query 

    linear_events = viewing_df.filter(F.col("event_type") == "linear")

    join_cond = (
        (linear_events["tenant"] == schedule_df["tenant"])
        & (linear_events["content_id"] == schedule_df["program_id"])
        & (linear_events["start_time_ts"] >= schedule_df["broadcast_start_ts"])
        & (linear_events["start_time_ts"] <= schedule_df["broadcast_end_ts"])
    )

    # Add schedule_small to accoubt for skewed data
    schedule_small = F.broadcast(schedule_df)
    matched_events = (
        # linear_events.join(schedule_df, join_cond, "inner")
        linear_events.join(schedule_small, join_cond, "inner")
        .select("event_id")
        .dropDuplicates()
        .withColumn("has_schedule_match", F.lit(True))
    )

    return (
        viewing_df.join(matched_events, on="event_id", how="left")
        .withColumn(
            "has_schedule_match",
            F.coalesce(F.col("has_schedule_match"), F.lit(False))
        )
    )


# -------------------------------------------------------------------
# Validation Rules (Task 1A)
# -------------------------------------------------------------------

def apply_validation_rules(df: DataFrame) -> DataFrame:

    validation_reasons = F.expr("""
      filter(
        array(
          CASE WHEN end_time IS NULL OR trim(end_time) = '' THEN 'MISSING_END_TIME' ELSE NULL END,
          CASE WHEN end_time < start_time THEN 'NEGATIVE_DURATION' ELSE NULL END,
          CASE WHEN tenant NOT IN ('swe','fin','nor') THEN 'INVALID_TENANT' ELSE NULL END,
          CASE
            WHEN event_type = 'linear' AND has_schedule_match = false
            THEN 'NO_SCHEDULE_MATCH'
            ELSE NULL
          END
        ),
        x -> x IS NOT NULL
      )
    """)

    df = (
        df.withColumn("validation_reasons", validation_reasons)
          .withColumn(
              "validation_reason",
              F.when(
                  F.size(F.col("validation_reasons")) > 0,
                  F.concat_ws(",", F.col("validation_reasons"))
              ).otherwise(None)
          )
          .withColumn(
              "is_valid",
              F.size(F.col("validation_reasons")) == 0
          )
    )
    
    return df

# -------------------------------------------------------------------
# SCD2 Enrichment + Metrics (Task 1B)
# -------------------------------------------------------------------

def enrich_with_program_metadata(
    valid_events: DataFrame,
    program_df: DataFrame,
) -> DataFrame:

    scd_join_cond = (
        (valid_events["content_id"] == program_df["program_id"])
        & (valid_events["start_time_ts"] >= program_df["valid_from_ts"])
        & (valid_events["start_time_ts"] <= program_df["valid_to_ts"])
    )

    # Account for skewed data
    program_small = F.broadcast(program_df)
    # enriched = valid_events.join(program_df, scd_join_cond, "left")
    enriched = valid_events.join(program_small, scd_join_cond, "left")

    enriched = enriched.withColumn(
        "viewing_duration_minutes",
        F.round(
            (F.unix_timestamp("end_time_ts") - F.unix_timestamp("start_time_ts")) / 60,
            2,
        ),
    )

    enriched = enriched.withColumn(
        "completion_rate",
        F.round(
            F.least(
                F.lit(100.0),
                (F.col("viewing_duration_minutes") / F.col("program_duration_minutes")) * 100,
            ),
            2,
        ),
    )

    return enriched


# -------------------------------------------------------------------
# Output Writing
# -------------------------------------------------------------------

def write_parquet_and_optional_csv(
    df: DataFrame,
    parquet_path: str,
    csv_path: str,
    write_human_readable: bool,
) -> None:
    try:
        df.write.mode("overwrite").parquet(parquet_path)

        if write_human_readable:
            df.write.mode("overwrite").option("header", True).csv(csv_path)

    except Exception as exc:
        raise PipelineError(f"Failed writing dataset: {exc}") from exc


# -------------------------------------------------------------------
# Summary Report (Task 1C)
# -------------------------------------------------------------------

def generate_validation_summary(
    validated_df: DataFrame,
    enriched_df: DataFrame,
    output_dir: str,
) -> None:
    """
    Creates validation_summary.json with:
    - total_events
    - valid_events
    - invalid_events
    - top_3_programs_by_viewing_duration
    """
    try:
        total_events = validated_df.count()
        valid_events = validated_df.filter(F.col("is_valid") == True).count()
        invalid_events = validated_df.filter(F.col("is_valid") == False).count()

        # Top 3 programs by total viewing duration
        top_programs_df = (
            enriched_df
            .groupBy("program_id")
            .agg(
                F.round(F.sum("viewing_duration_minutes"), 2)
                .alias("total_viewing_minutes")
            )
            .orderBy(F.col("total_viewing_minutes").desc())
            .limit(3)
        )

        top_programs = [
            {
                "program_id": row["program_id"],
                "total_viewing_minutes": float(row["total_viewing_minutes"]),
            }
            for row in top_programs_df.collect()
        ]

        summary_df = enriched_df.sparkSession.createDataFrame(
            [
                {
                    "total_events_processed": total_events,
                    "valid_events": valid_events,
                    "invalid_events": invalid_events,
                    "top_3_programs_by_viewing_duration": top_programs,
                }
            ]
        )

        summary_df.write.mode("overwrite").json(
            f"{output_dir}/validation_summary.json"
        )

    except Exception as exc:
        raise PipelineError(f"Failed generating validation summary: {exc}") from exc


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main() -> None:
    try:
        args = parse_args()

        spark = (
            SparkSession.builder
            .appName("tv-data-assignment-task-1a-1b-1c")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.shuffle.partitions", "200")  # tune as needed
            .getOrCreate()
        )

        viewing_df, schedule_df, program_df = load_data(spark, args.data_dir)

        with_matches = find_schedule_matches(viewing_df, schedule_df)
        validated = apply_validation_rules(with_matches)

        valid_events = validated.filter(F.col("is_valid") == True)
        invalid_events = validated.filter(F.col("is_valid") == False)

        enriched = enrich_with_program_metadata(valid_events, program_df)

        write_parquet_and_optional_csv(
            enriched,
            parquet_path=f"{args.output_dir}/enriched_events.parquet",
            csv_path=f"{args.output_dir}/enriched_events_csv",
            write_human_readable=args.write_human_readable,
        )
		
		# Write VALID events
        write_parquet_and_optional_csv(
            valid_events,
            parquet_path=f"{args.output_dir}/valid_events.parquet",
            csv_path=f"{args.output_dir}/valid_events_csv",
            write_human_readable=args.write_human_readable,
        )
        
        # Write INVALID events
        write_parquet_and_optional_csv(
            invalid_events,
            parquet_path=f"{args.output_dir}/invalid_events.parquet",
            csv_path=f"{args.output_dir}/invalid_events_csv",
            write_human_readable=args.write_human_readable,
        )
		
        # Task 1C Summary Report
        generate_validation_summary(
            validated_df=validated,
            enriched_df=enriched,
            output_dir=args.output_dir,
        )		

        spark.stop()
        print("Task 1A + 1B + 1C successfully completed.")

    except PipelineError as pe:
        print(f"PipelineError: {pe}")
        raise

    except Exception as exc:
        print(f"Unexpected failure: {exc}")
        raise


if __name__ == "__main__":
    main()
