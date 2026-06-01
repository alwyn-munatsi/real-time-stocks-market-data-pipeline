"""
Snowflake S3 Loader

Called by the Airflow DAG every hour. Executes a COPY INTO command that reads
all NEW Parquet files from the S3 stage and inserts them into RAW.STOCK_PRICES.

Snowflake tracks which files have already been loaded in an internal metadata
table, so running COPY INTO multiple times is safe — it never double-loads.
"""

import os

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA_RAW"),
    )


def copy_s3_to_raw() -> int:
    """
    Run COPY INTO on the S3 stage and return the number of rows loaded.

    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE tells Snowflake to automatically
    map Parquet column names to table column names — no manual column mapping
    needed as long as the names match (which they do, since the consumer writes
    the same field names the table was created with).

    ON_ERROR = CONTINUE means a single bad file doesn't abort the entire load;
    the bad file is skipped and reported in the load history.

    PURGE = FALSE keeps the Parquet files in S3 after loading. This is
    intentional — S3 is your permanent archive and source of truth.
    """
    conn   = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        COPY INTO STOCK_PRICES
        FROM @s3_stage
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR  = CONTINUE
        PURGE     = FALSE
    """)

    # COPY INTO returns one row per file loaded.
    # Columns: file, status, rows_parsed, rows_loaded, ...
    # We sum column index 3 (rows_loaded) across all rows.
    rows_loaded = sum(row[3] for row in cursor.fetchall() if row[3])

    cursor.close()
    conn.close()
    return rows_loaded


if __name__ == "__main__":
    n = copy_s3_to_raw()
    print(f"Loaded {n} rows into STOCK_DB.RAW.STOCK_PRICES")
