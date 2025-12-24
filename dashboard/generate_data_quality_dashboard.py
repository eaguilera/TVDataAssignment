# dashboard/generate_data_quality_dashboard.py

import argparse
import glob
import json
import os
from typing import Dict, Any, List

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def load_validation_summary(summary_dir):

    pattern = os.path.join(summary_dir, "part-*.json")
    files = glob.glob(pattern)

    if not files:
        raise FileNotFoundError(f"No JSON files found in {summary_dir}")

    records = []

    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            raw = f.read().strip()

            if not raw:
                print(f"Empty file skipped: {file}")
                continue

            # Case 1: JSON array
            if raw.startswith("["):
                try:
                    records.extend(json.loads(raw))
                except Exception as e:
                    raise ValueError(f"Invalid JSON array in file: {file}\n{e}")

            # Case 2: Single JSON object
            elif raw.startswith("{"):
                try:
                    records.append(json.loads(raw))
                except Exception as e:
                    raise ValueError(f"Invalid JSON object in file: {file}\n{e}")

            # Case 3: JSON Lines (one object per line)
            else:
                print(f"Attempting JSONL fallback for: {file}")
                for line in raw.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(json.loads(line))
                    except Exception:
                        print(f"Skipped invalid line in {file}: {line}")

    if not records:
        raise ValueError("No valid JSON records found in any part file.")

    # Merge all dictionaries into a single summary
    final_summary = {}
    for r in records:
        if isinstance(r, dict):
            final_summary.update(r)

    return final_summary




def build_dashboard(
    summary: Dict[str, Any],
    enriched_parquet_path: str,
    output_html: str,
) -> None:
    total_events = int(summary.get("total_events_processed", 0))
    valid_events = int(summary.get("valid_events", 0))
    invalid_events = int(summary.get("invalid_events", 0))
    valid_ratio = (valid_events / total_events * 100.0) if total_events > 0 else 0.0

    top_programs = summary.get("top_3_programs_by_viewing_duration", [])
    # Make a DataFrame for nicer handling
    top_prog_df = pd.DataFrame(top_programs)
    if not top_prog_df.empty:
        # Normalize column names
        if "total_viewing_minutes" in top_prog_df.columns:
            top_prog_df["total_viewing_minutes"] = (
                top_prog_df["total_viewing_minutes"].astype(float)
            )
        else:
            # Fallback: if key was different
            first_numeric = next(
                (c for c in top_prog_df.columns if "minute" in c), None
            )
            if first_numeric:
                top_prog_df["total_viewing_minutes"] = (
                    top_prog_df[first_numeric].astype(float)
                )

        if "program_id" not in top_prog_df.columns:
            top_prog_df["program_id"] = top_prog_df.index.astype(str)

        # Handle NULL program_ids (Spark → None)
        top_prog_df["program_id"] = top_prog_df["program_id"].fillna("UNKNOWN_PROGRAM")

    # Try to read enriched parquet for additional charts (optional)
    enriched_df = None
    if os.path.exists(enriched_parquet_path):
        try:
            enriched_df = pd.read_parquet(enriched_parquet_path)
        except Exception as e:  # pragma: no cover - just defensive
            print(f"Warning: could not read enriched parquet: {e}")

    fig = make_subplots(
        rows=2,
        cols=2,
        specs=[
            [{"type": "indicator"}, {"type": "indicator"}],
            [{"type": "bar"}, {"type": "bar"}],
        ],
        subplot_titles=(
            "Valid vs Invalid Events",
            "Valid Share (%)",
            "Top Programs by Viewing Duration",
            "Events by Tenant (valid only)",
        ),
    )

    # KPI 1 – counts
    fig.add_trace(
        go.Indicator(
            mode="number+delta",
            value=valid_events,
            delta={"reference": invalid_events, "relative": False},
            title={"text": "Valid events (delta vs invalid)"},
        ),
        row=1,
        col=1,
    )

    # KPI 2 – % valid
    fig.add_trace(
        go.Indicator(
            mode="gauge+number",
            value=valid_ratio,
            gauge={"axis": {"range": [0, 100]}},
            title={"text": "Valid events %"},
        ),
        row=1,
        col=2,
    )

    # Chart 3 – top programs
    if top_prog_df is not None and not top_prog_df.empty:
        fig.add_trace(
            go.Bar(
                x=top_prog_df["program_id"],
                y=top_prog_df["total_viewing_minutes"],
                text=top_prog_df["total_viewing_minutes"],
                textposition="auto",
            ),
            row=2,
            col=1,
        )

    # Chart 4 – valid events by tenant from enriched parquet (if available)
    if enriched_df is not None and "tenant" in enriched_df.columns:
        tenant_counts = (
            enriched_df.groupby("tenant")["tenant"].count().sort_values(ascending=False)
        )
        fig.add_trace(
            go.Bar(
                x=tenant_counts.index.astype(str),
                y=tenant_counts.values,
                text=tenant_counts.values,
                textposition="auto",
            ),
            row=2,
            col=2,
        )

    fig.update_layout(
        title_text="TV Data Assignment – Data Quality Dashboard",
        height=800,
        showlegend=False,
    )

    fig.write_html(output_html, include_plotlyjs="cdn")
    print(f"Dashboard written to {output_html}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Data Quality Dashboard from Spark outputs."
    )
    parser.add_argument(
        "--summary-dir",
        default="output/validation_summary.json",
        help="Directory containing Spark JSON summary (part-*.json).",
    )
    parser.add_argument(
        "--parquet-dir",
        default="output/enriched_events.parquet",
        help="Directory with enriched_events parquet dataset.",
    )
    parser.add_argument(
        "--output-html",
        default="output/data_quality_dashboard.html",
        help="Path for the generated HTML dashboard.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    summary_dict = load_validation_summary(args.summary_dir)
    # Parquet directory → treat as dataset, let pandas figure it out
    enriched_path = args.parquet_dir
    build_dashboard(summary_dict, enriched_path, args.output_html)
