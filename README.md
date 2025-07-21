# OrchidETLPipeline

## Objective:

Design a robust and extensible data ingestion and transformation pipeline (ETL/ELT) that prepares user interaction data for analytical use.

---

## Data Flow Overview

### Ingests:

* **`user_profiles.csv`**: Static user data.
* **`user_events_YYYYMMDD.json`**: Daily log files with user activity (JSON or JSON Lines).

### Transforms:

* Joins user data and event logs on `user_id`
* Extracts key and optional fields from nested `details`
* Converts `timestamp` to ISO format and extracts `event_date`
* Assigns a UUID-based `event_id` to each row
* Preserves raw JSON from `details` as `details_raw`

### Outputs:

* Parquet files partitioned by `event_date`
* Final schema includes:

  ```
  event_id, user_id, name, location, registration_date,
  event_type, timestamp, event_date, details_raw,
  page_url, button_id, item_id, [any additional user-specified fields]
  ```

---

## Features

* CLI support for dynamic input/output specification
* Merges multiple profile and event files
* Dynamic detail-field extraction via `--extract_fields`
* Built-in logging for progress tracking

---

## CLI Usage

```bash
python etl_pipeline.py \
  --profiles data/profiles1.csv data/profiles2.csv \
  --events data/logs1.json data/logs2.json \
  --output output_dir/ \
  --extract_fields referrer price quantity
```

### Arguments:

* `--profiles`: One or more CSV files containing user profiles
* `--events`: One or more JSON files with user activity logs
* `--output`: Directory to store partitioned Parquet output
* `--extract_fields`: (Optional) Additional keys to extract from `details`

---

## Output Example

Sample output row in the resulting Parquet file:

| event\_id | user\_id | event\_type | timestamp            | event\_date | page\_url        | button\_id       | item\_id  | referrer |
| --------- | -------- | ----------- | -------------------- | ----------- | ---------------- | ---------------- | --------- | -------- |
| uuid...   | 2        | purchase    | 2023-10-26T10:20:00Z | 2023-10-26  | /product/item123 | buy\_now\_button | item\_abc | homepage |

---

## Requirements

* Python 3.8+
* pandas
* duckdb
* pyarrow or fastparquet

Install dependencies:

```bash
pip install pandas duckdb pyarrow
```

---

## Project Files

* `etl_pipeline.py`: Main ETL pipeline with CLI and logging
* `query_output.py`: Utility for querying output Parquet files with DuckDB
* `README.md`: Documentation for using the pipeline

---
## Querying the Output

Once the ETL pipeline has written partitioned Parquet files, you can query them directly using the `query_output.py` utility powered by [DuckDB](https://duckdb.org/).

### Example

```bash
python query_output.py \
  --query "SELECT * FROM read_parquet('{parquet_path}') WHERE event_date = '2023-10-26' LIMIT 10" \
  --parquet_dir output/ \
  --output query_results.csv
