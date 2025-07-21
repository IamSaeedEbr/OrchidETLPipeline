import duckdb
import pandas as pd
from pathlib import Path
import argparse


def query_output_parquet(query: str, parquet_path: str) -> pd.DataFrame:
    """
    Execute a SQL query using DuckDB on a Parquet file or directory.

    Args:
        query (str): SQL query string. Should reference the Parquet path as '{parquet_path}'.
        parquet_path (str): Path to the Parquet file or folder.

    Returns:
        pd.DataFrame: Query result as a DataFrame.
    """
    resolved_path = Path(parquet_path).resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Provided path does not exist: {resolved_path}")

    formatted_query = query.replace("{parquet_path}", str(resolved_path / "**/*.parquet"))

    con = duckdb.connect()
    try:
        result_df = con.execute(formatted_query).df()
    except Exception as e:
        raise RuntimeError(f"Query failed: {e}")
    finally:
        con.close()
    return result_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run SQL queries on Parquet output using DuckDB.")
    parser.add_argument(
        "--query",
        required=True,
        help="Path to a .sql file or a raw SQL query string with '{parquet_path}' placeholder."
    )
    parser.add_argument(
        "--parquet_dir",
        required=True,
        help="Path to the directory containing Parquet files."
    )
    parser.add_argument(
        "--output",
        help="Optional path to save results as CSV."
    )

    args = parser.parse_args()

    # Read query from file if applicable
    query_arg = args.query
    if Path(query_arg).is_file():
        with open(query_arg, "r", encoding="utf-8") as f:
            query_text = f.read()
    else:
        query_text = query_arg

    # Execute query
    df = query_output_parquet(query_text, args.parquet_dir)
    print(df)

    # Optionally export
    if args.output:
        df.to_csv(args.output, index=False)
        print(f"\nQuery result written to: {args.output}")
