"""
Snowflake One-Time Setup

Run this script once before starting the pipeline. It creates:
  1. The STOCK_DB database
  2. Three schemas: RAW, CLEANED, BUSINESS_READY
  3. The RAW.STOCK_PRICES table that receives data from S3
  4. An external stage pointing at your S3 bucket so Snowflake knows
     where to find files when you run COPY INTO

Usage:
    python snowflake/setup.py
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
    )


def run_setup() -> None:
    s3_bucket  = os.getenv("S3_BUCKET_NAME")
    aws_key    = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

    conn   = get_connection()
    cursor = conn.cursor()

    steps = [
        # ── Database & schemas ────────────────────────────────────────────────
        ("Create database",
         "CREATE DATABASE IF NOT EXISTS STOCK_DB"),

        ("Create RAW schema",
         "CREATE SCHEMA IF NOT EXISTS STOCK_DB.RAW"),

        ("Create CLEANED schema",
         "CREATE SCHEMA IF NOT EXISTS STOCK_DB.CLEANED"),

        ("Create BUSINESS_READY schema",
         "CREATE SCHEMA IF NOT EXISTS STOCK_DB.BUSINESS_READY"),

        # ── Raw table ─────────────────────────────────────────────────────────
        # Column names match exactly what the Kafka consumer writes to Parquet.
        # _loaded_at is a metadata column added automatically on insert —
        # useful for auditing and incremental load logic in dbt.
        ("Create RAW.STOCK_PRICES table",
         """
         CREATE TABLE IF NOT EXISTS STOCK_DB.RAW.STOCK_PRICES (
             ticker          VARCHAR(10),
             timestamp       TIMESTAMP_NTZ,
             price           FLOAT,
             open            FLOAT,
             high            FLOAT,
             low             FLOAT,
             volume          BIGINT,
             previous_close  FLOAT,
             market_cap      BIGINT,
             exchange        VARCHAR(20),
             _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
         )
         """),

        # ── External stage ────────────────────────────────────────────────────
        # A Snowflake STAGE is a named pointer to an external storage location.
        # Once created, you can COPY INTO a table FROM @s3_stage instead of
        # writing out the full S3 URL every time.
        ("Create S3 external stage",
         f"""
         CREATE STAGE IF NOT EXISTS STOCK_DB.RAW.s3_stage
           URL = 's3://{s3_bucket}/stock-prices/'
           CREDENTIALS = (
             AWS_KEY_ID     = '{aws_key}'
             AWS_SECRET_KEY = '{aws_secret}'
           )
           FILE_FORMAT = (TYPE = PARQUET)
         """),
    ]

    for description, sql in steps:
        print(f"  {description}...", end=" ", flush=True)
        cursor.execute(sql.strip())
        print("done")

    cursor.close()
    conn.close()
    print("\nSnowflake setup complete. Your pipeline is ready to receive data.")


if __name__ == "__main__":
    print("Setting up Snowflake resources...\n")
    run_setup()
