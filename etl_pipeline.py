import pandas as pd
import json
import uuid
import logging
from pathlib import Path
from io import StringIO
from datetime import datetime
import argparse

# -------------------------------
# Configure logging for visibility
# -------------------------------
logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.INFO
)

# ----------------------------------------
# Load user profile data from CSV file(s)
# Handles UTF-8 BOM and strips extra quotes
# ----------------------------------------
def load_user_profiles(csv_paths: list[str]) -> pd.DataFrame:
    logging.info(f"Loading user profiles from {len(csv_paths)} file(s)...")
    dfs = []
    for csv_path in csv_paths:
        logging.info(f"Reading profile file: {csv_path}")
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            lines = [line.strip().strip('"') for line in f]
        fixed_csv = "\n".join(lines)
        df = pd.read_csv(StringIO(fixed_csv), parse_dates=['registration_date'])
        dfs.append(df)
    combined = pd.concat(dfs, ignore_index=True)
    logging.info(f"Loaded {len(combined)} user profile records.")
    return combined

# ----------------------------------------
# Load user event data from JSON file(s)
# Parses timestamps and extracts event_date
# ----------------------------------------
def load_user_events(json_paths: list[str]) -> pd.DataFrame:
    logging.info(f"Loading user events from {len(json_paths)} file(s)...")
    dfs = []
    for path in json_paths:
        logging.info(f"Reading event file: {path}")
        df = pd.read_json(path)
        dfs.append(df)
    df_all = pd.concat(dfs, ignore_index=True)
    df_all['timestamp'] = pd.to_datetime(df_all['timestamp'])
    df_all['event_date'] = df_all['timestamp'].dt.date
    logging.info(f"Loaded {len(df_all)} event records.")
    return df_all

# --------------------------------------------------
# Extract specified fields from nested 'details' JSON
# Also adds a raw JSON string version as 'details_raw'
# --------------------------------------------------
def extract_details_fields(df: pd.DataFrame, fields: list[str] = None) -> pd.DataFrame:
    logging.info("Extracting detail fields from 'details' column...")
    df['details_raw'] = df['details'].apply(json.dumps)

    default_fields = ['page_url', 'button_id', 'item_id']
    all_fields = sorted(set(default_fields + (fields if fields else [])))

    for field in all_fields:
        df[field] = df['details'].apply(lambda x: x.get(field) if isinstance(x, dict) else None)

    logging.info(f"Extracted fields from 'details': {all_fields}")
    return df

# -------------------------------------
# Join user profiles and event records
# Left join ensures all events are kept
# -------------------------------------
def join_data(profiles_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Joining events with user profiles...")
    df = events_df.merge(profiles_df, on='user_id', how='left')
    logging.info("Join complete.")
    return df

# ----------------------------------------
# Add a unique UUID-based event ID column
# ----------------------------------------
def generate_event_id(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Generating unique event IDs...")
    df['event_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    return df

# -----------------------------------------------------
# Filter and reorder final output columns for delivery
# Includes optional fields extracted from `details`
# -----------------------------------------------------
def select_final_columns(df: pd.DataFrame, extra_fields: list[str] = None) -> pd.DataFrame:
    logging.info("Selecting final output columns...")
    base_columns = [
        'event_id', 'user_id', 'name', 'location', 'registration_date',
        'event_type', 'timestamp', 'event_date', 'details_raw'
    ]
    default_fields = ['page_url', 'button_id', 'item_id']
    all_fields = sorted(set(default_fields + (extra_fields if extra_fields else [])))
    final_columns = base_columns + all_fields
    available_columns = [col for col in final_columns if col in df.columns]
    return df[available_columns]

# ------------------------------------------------------
# Write DataFrame to Parquet, partitioned by event_date
# ------------------------------------------------------
def write_to_parquet(df: pd.DataFrame, output_path: str, partition_by_date: bool = True):
    logging.info(f"Writing output to Parquet format in directory: {output_path}")
    path = Path(output_path)
    path.mkdir(parents=True, exist_ok=True)
    if partition_by_date:
        df.to_parquet(path, partition_cols=['event_date'], index=False)
    else:
        df.to_parquet(path / 'output.parquet', index=False)
    logging.info("Parquet file(s) written successfully.")

# ---------------------------
# Master ETL pipeline runner
# ---------------------------
def run_etl(profiles_paths: list[str], events_paths: list[str], output_dir: str, extract_fields: list[str] = None):
    logging.info("Starting ETL pipeline...")
    profiles = load_user_profiles(profiles_paths)
    events = load_user_events(events_paths)
    events = extract_details_fields(events, fields=extract_fields)
    full_data = join_data(profiles, events)
    full_data = generate_event_id(full_data)
    final_df = select_final_columns(full_data, extra_fields=extract_fields)
    write_to_parquet(final_df, output_dir)
    logging.info("ETL pipeline completed successfully.")

# ----------------------------
# CLI interface for execution
# ----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL pipeline for user profiles and events.")
    parser.add_argument('--profiles', nargs='+', required=True, help='List of CSV files containing user profiles')
    parser.add_argument('--events', nargs='+', required=True, help='List of JSON files containing user events')
    parser.add_argument('--output', required=True, help='Directory to store output Parquet files')
    parser.add_argument('--extract_fields', nargs='*', default=[], help='Additional fields to extract from event details')

    args = parser.parse_args()

    run_etl(
        profiles_paths=args.profiles,
        events_paths=args.events,
        output_dir=args.output,
        extract_fields=args.extract_fields
    )
