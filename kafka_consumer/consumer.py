"""
Kafka Consumer — Kafka → Amazon S3

Runs as an always-on Docker service. Reads messages from the Kafka topic
stock-prices and buffers them by ticker symbol. When a buffer reaches
BATCH_SIZE records it serialises the batch as a Parquet file and uploads it
to S3 under a Hive-style partition path so Snowflake can later query or load
specific date/hour slices efficiently.

S3 key format:
  stock-prices/ticker=AAPL/year=2026/month=05/day=31/hour=14/AAPL_2026-05-31_14.parquet
"""

import io
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC_STOCK_PRICES", "stock-prices")
S3_BUCKET               = os.getenv("S3_BUCKET_NAME", "alwyn-stock-pipeline-raw")
AWS_REGION              = os.getenv("AWS_REGION", "us-east-1")
BATCH_SIZE              = int(os.getenv("CONSUMER_BATCH_SIZE", "50"))

# Global buffer: { "AAPL": [record, record, ...], "MSFT": [...], ... }
buffer: dict[str, list[dict]] = {}
s3_client = None


def build_consumer() -> KafkaConsumer:
    """Connect to Kafka with retry logic."""
    for attempt in range(1, 11):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                # earliest: on first start, read from the very beginning of the topic
                # so no messages are missed even if the consumer starts late
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="stock-pipeline-consumer-group",
                # If no new messages arrive for 5 s, the for-loop yields so we
                # can flush any partial batches that haven't hit BATCH_SIZE yet
                consumer_timeout_ms=5000,
            )
            print(f"Connected to Kafka | topic: {KAFKA_TOPIC} | batch size: {BATCH_SIZE}")
            return consumer
        except NoBrokersAvailable:
            wait = attempt * 5
            print(f"Kafka not ready (attempt {attempt}/10). Retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError("Could not connect to Kafka after 10 attempts.")


def build_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def flush_to_s3(ticker: str, records: list[dict]) -> None:
    """
    Convert a list of record dicts to a Parquet file and upload to S3.

    Parquet is chosen over CSV because:
    - It's columnar: Snowflake only reads the columns it needs (faster, cheaper)
    - It's compressed by default (~5x smaller than equivalent CSV)
    - It preserves data types (no guessing whether a column is int or float)
    """
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    hour_str = now.strftime("%H")

    df = pd.DataFrame(records)

    # Write the DataFrame to an in-memory Parquet buffer (no temp files needed)
    table  = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    # Hive partition format: Snowflake and Athena can query specific partitions
    # without scanning the entire dataset
    s3_key = (
        f"stock-prices/"
        f"ticker={ticker}/"
        f"year={now.year}/"
        f"month={now.strftime('%m')}/"
        f"day={now.strftime('%d')}/"
        f"hour={hour_str}/"
        f"{ticker}_{date_str}_{hour_str}_{now.strftime('%M%S')}.parquet"
    )

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    print(f"  Flushed {len(records):3d} records → s3://{S3_BUCKET}/{s3_key}")


def flush_all_buffers() -> None:
    """Flush every ticker's remaining records — called on shutdown."""
    for ticker, records in buffer.items():
        if records:
            flush_to_s3(ticker, records)
            buffer[ticker] = []


def handle_shutdown(signum, frame) -> None:
    print("\nShutdown signal received. Flushing remaining buffers...")
    flush_all_buffers()
    sys.exit(0)


def main() -> None:
    global s3_client
    s3_client = build_s3_client()

    # Gracefully flush buffers when the container is stopped (SIGTERM from Docker)
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT,  handle_shutdown)

    consumer = build_consumer()
    print("Listening for messages...\n")

    while True:
        try:
            for message in consumer:
                record = message.value
                ticker = record.get("ticker", "UNKNOWN")

                if ticker not in buffer:
                    buffer[ticker] = []
                buffer[ticker].append(record)

                # Flush this ticker's buffer when it reaches BATCH_SIZE
                if len(buffer[ticker]) >= BATCH_SIZE:
                    flush_to_s3(ticker, buffer[ticker])
                    buffer[ticker] = []

        except StopIteration:
            # consumer_timeout_ms elapsed with no new messages — flush partial batches
            # so data doesn't sit in memory indefinitely during quiet periods
            for ticker, records in buffer.items():
                if records:
                    print(f"  Timeout flush: {ticker} ({len(records)} records)")
                    flush_to_s3(ticker, records)
                    buffer[ticker] = []

        except Exception as exc:
            print(f"Consumer error: {exc}. Restarting in 10s...")
            time.sleep(10)


if __name__ == "__main__":
    main()
