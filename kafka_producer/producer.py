"""
Kafka Producer — Yahoo Finance → Kafka

Runs as an always-on Docker service. Every POLL_INTERVAL_SECONDS seconds it
fetches the latest quote for each ticker in STOCK_TICKERS and publishes one
JSON message per ticker to the Kafka topic defined by KAFKA_TOPIC_STOCK_PRICES.
"""

import json
import os
import time
from datetime import datetime, timezone

import yfinance as yf
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC_STOCK_PRICES", "stock-prices")
STOCK_TICKERS           = os.getenv("STOCK_TICKERS", "AAPL,MSFT,GOOGL,TSLA,NVDA,AMZN,META,JPM").split(",")
POLL_INTERVAL_SECONDS   = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))


def build_producer() -> KafkaProducer:
    """Connect to Kafka with retry logic — Kafka may still be starting up."""
    for attempt in range(1, 11):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                # Serialise the Python dict → JSON bytes before sending
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # Serialise the ticker string → bytes so Kafka can use it as a key
                key_serializer=lambda k: k.encode("utf-8"),
                # acks='all' means the broker confirms the message was written
                # to all replicas before acknowledging — prevents data loss
                acks="all",
                retries=3,
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            wait = attempt * 5
            print(f"Kafka not ready (attempt {attempt}/10). Retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError("Could not connect to Kafka after 10 attempts.")


def fetch_quote(ticker_symbol: str) -> dict | None:
    """
    Fetch the latest quote for a single ticker using yfinance.

    yfinance's fast_info property makes a lightweight HTTP call to Yahoo Finance
    and returns key fields without downloading the full historical dataset.
    Returns None if the market is closed or data is unavailable.
    """
    try:
        info = yf.Ticker(ticker_symbol).fast_info

        price = info.last_price
        if price is None or price <= 0:
            return None

        return {
            "ticker":         ticker_symbol,
            "timestamp":      datetime.now(timezone.utc).isoformat(),
            "price":          round(float(price), 4),
            "open":           round(float(info.open), 4)           if info.open           else None,
            "high":           round(float(info.day_high), 4)       if info.day_high       else None,
            "low":            round(float(info.day_low), 4)        if info.day_low        else None,
            "volume":         int(info.last_volume)                if info.last_volume    else None,
            "previous_close": round(float(info.previous_close), 4) if info.previous_close else None,
            "market_cap":     int(info.market_cap)                 if info.market_cap     else None,
            "exchange":       info.exchange,
        }
    except Exception as exc:
        print(f"  [{ticker_symbol}] fetch error: {exc}")
        return None


def main() -> None:
    print(f"Starting producer | tickers: {STOCK_TICKERS} | interval: {POLL_INTERVAL_SECONDS}s")
    producer = build_producer()

    while True:
        poll_start = time.time()
        published = 0

        for ticker in STOCK_TICKERS:
            quote = fetch_quote(ticker)
            if quote:
                # key=ticker lets Kafka route all messages for the same ticker
                # to the same partition, preserving ordering per ticker
                producer.send(topic=KAFKA_TOPIC, key=ticker, value=quote)
                print(f"  Published {ticker} @ ${quote['price']}  [{quote['timestamp']}]")
                published += 1
            else:
                print(f"  Skipped  {ticker} — no data (market may be closed)")

        # flush() blocks until all buffered messages have been sent and acknowledged
        producer.flush()
        print(f"Poll complete: {published}/{len(STOCK_TICKERS)} published. "
              f"Next poll in {POLL_INTERVAL_SECONDS}s.\n")

        # Sleep for the remainder of the interval (accounts for time spent fetching)
        elapsed = time.time() - poll_start
        sleep_for = max(0, POLL_INTERVAL_SECONDS - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
