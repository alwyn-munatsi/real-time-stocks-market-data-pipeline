# Real-Time Stock Market Data Pipeline: Kafka → S3 → Snowflake → dbt → Power BI

[![Streaming](https://img.shields.io/badge/Streaming-Apache%20Kafka-231F20?logo=apachekafka)](https://kafka.apache.org/)
[![Data Lake](https://img.shields.io/badge/Data%20Lake-Amazon%20S3%20(Parquet)-569A31?logo=amazons3)](https://aws.amazon.com/s3/)
[![Warehouse](https://img.shields.io/badge/Warehouse-Snowflake-29B5E8?logo=snowflake)](https://www.snowflake.com/)
[![Transform](https://img.shields.io/badge/Transform-dbt-FF694B?logo=dbt)](https://www.getdbt.com/)
[![Orchestration](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE?logo=apacheairflow)](https://airflow.apache.org/)
[![Containers](https://img.shields.io/badge/Runtime-Docker%20Compose-2496ED?logo=docker)](https://docs.docker.com/compose/)

An end-to-end, containerised streaming data engineering pipeline that captures live stock quotes from **Yahoo Finance**, streams them through **Apache Kafka**, lands them in an **Amazon S3** data lake as Hive-partitioned **Parquet**, loads them into **Snowflake** on an hourly **Apache Airflow** schedule, and transforms them with **dbt** through a layered warehouse (**RAW → CLEANED → BUSINESS_READY**) into a daily performance fact table with moving averages and % change, ready for a **Power BI** dashboard.

---

## 1. System Architecture & End-to-End Data Flow

![Architecture Diagram](docs/Architecture%20Diagram.png)

### End-to-End Execution Flow
$$\text{Yahoo Finance} \xrightarrow{\text{Producer (60s)}} \text{Kafka topic} \xrightarrow{\text{Consumer}} \text{S3 (Parquet)} \xrightarrow{\text{Airflow COPY INTO}} \text{RAW} \xrightarrow{\text{dbt}} \text{CLEANED} \xrightarrow{\text{dbt}} \text{BUSINESS\_READY} \xrightarrow{} \text{Power BI}$$

1. **Kafka Producer (`kafka_producer/producer.py`):** An always-on container that polls `yfinance` `fast_info` every `POLL_INTERVAL_SECONDS` (default 60) for each ticker and publishes one JSON message per ticker, keyed by ticker symbol.
2. **Kafka (`stock-prices` topic):** Confluent Kafka 7.5.3 with Zookeeper buffers the stream and decouples ingestion from storage.
3. **Kafka Consumer (`kafka_consumer/consumer.py`):** Buffers messages per ticker and flushes each buffer to S3 as a Snappy-compressed Parquet file when it reaches `CONSUMER_BATCH_SIZE` (default 50) or when the topic goes quiet.
4. **S3 Data Lake:** Files land under Hive-style partitions: `stock-prices/ticker=AAPL/year=2026/month=05/day=31/hour=14/AAPL_2026-05-31_14_<MMSS>.parquet`.
5. **Airflow (`stock_market_pipeline` DAG):** Runs hourly: `COPY INTO` from the S3 stage → `dbt run` → `dbt test`.
6. **Snowflake RAW:** `STOCK_DB.RAW.STOCK_PRICES` receives every tick with a `_loaded_at` audit column.
7. **dbt CLEANED:** `stg_stock_prices` (typed, filtered ticks) and `int_daily_ohlcv` (one OHLCV row per ticker per day).
8. **dbt BUSINESS_READY:** `fct_stock_performance` adds 7- and 30-day moving averages, daily % change and intraday range.
9. **Power BI:** Connects to `BUSINESS_READY` for trend and performance reporting.

---

## 2. Infrastructure & Service Directory

All services except Snowflake and S3 run locally from `docker-compose.yml`:

| Service | Image / Platform | Purpose / Function in Solution | Configuration Details |
| :--- | :--- | :--- | :--- |
| **`zookeeper`** | `confluentinc/cp-zookeeper:7.5.3` | Kafka cluster coordination | Local single node |
| **`kafka`** | `confluentinc/cp-kafka:7.5.3` | Message broker for the `stock-prices` topic | Reachable in-network at `kafka:9092` |
| **`kafka-producer`** | `python:3.11-slim` | Yahoo Finance → Kafka | `acks="all"`, 3 retries, connection retry with backoff |
| **`kafka-consumer`** | `python:3.11-slim` | Kafka → S3 Parquet | Consumer group `stock-pipeline-consumer-group`, `auto_offset_reset=earliest` |
| **`postgres`** | `postgres:15` | Airflow metadata database | Named volume `postgres-db-volume` |
| **`airflow-webserver` / `airflow-scheduler`** | `apache/airflow:2.8.1-python3.11` | Hourly orchestration | UI on `localhost:8080` |
| **Amazon S3** | AWS | Raw Parquet data lake | Bucket from `S3_BUCKET_NAME` |
| **Snowflake** | `STOCK_DB` | Warehouse with `RAW`, `CLEANED`, `BUSINESS_READY` schemas | External stage `RAW.s3_stage` (Parquet) |

---

## 3. Engineering Decisions

| Concern | Simpler Alternative | This Implementation | Technical Justification |
| :--- | :--- | :--- | :--- |
| **Ingestion** | Script writes straight to the warehouse | **Kafka between producer and storage** | Decouples API polling from storage; the consumer can fall behind or restart without losing quotes. |
| **Delivery guarantees** | Fire-and-forget | **`acks="all"` + retries; per-ticker message keys** | The broker confirms writes to all replicas; keys keep each ticker's ticks in order within a partition. |
| **Lake file format** | CSV | **Snappy Parquet, Hive-partitioned** | Columnar, typed and compressed; partitions let engines prune by ticker/date/hour. |
| **Small-file control** | One file per message | **Per-ticker buffers flushed at 50 records or on timeout** | Fewer, larger files; a SIGTERM handler flushes partial buffers on shutdown so nothing is lost. |
| **Warehouse loading** | Insert row by row | **`COPY INTO` from an external stage with `MATCH_BY_COLUMN_NAME`** | Snowflake's load history skips already-loaded files, so hourly reruns never double-load. |
| **Transformations** | Ad-hoc SQL | **dbt staging → intermediate → marts with schema tests** | Versioned, testable SQL; `dbt test` runs right after every build in the DAG. |
| **Daily OHLCV** | Self-joins / subqueries | **Snowflake `QUALIFY ROW_NUMBER()`** | Cleanest way to pick the first and last tick of each day. |

---

## 4. Phase 1: Streaming Ingestion (Yahoo Finance → Kafka)

Tracked tickers (configurable via `STOCK_TICKERS`): `AAPL` `MSFT` `GOOGL` `TSLA` `NVDA` `AMZN` `META` `JPM`

Each message looks like this:

```python
{
    "ticker":         ticker_symbol,
    "timestamp":      datetime.now(timezone.utc).isoformat(),
    "price":          round(float(price), 4),
    "open":           round(float(info.open), 4),
    "high":           round(float(info.day_high), 4),
    "low":            round(float(info.day_low), 4),
    "volume":         int(info.last_volume),
    "previous_close": round(float(info.previous_close), 4),
    "market_cap":     int(info.market_cap),
    "exchange":       info.exchange,
}
```

Quotes with a missing or non-positive price (market closed, fetch error) are skipped rather than published.

---

## 5. Phase 2: Landing in the S3 Data Lake (Kafka → Parquet)

```python
table  = pa.Table.from_pandas(df, preserve_index=False)
buffer = io.BytesIO()
pq.write_table(table, buffer, compression="snappy")

s3_key = (
    f"stock-prices/ticker={ticker}/year={now.year}/month={now.strftime('%m')}/"
    f"day={now.strftime('%d')}/hour={hour_str}/"
    f"{ticker}_{date_str}_{hour_str}_{now.strftime('%M%S')}.parquet"
)
s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
```

Parquet is written in memory, so no temp files are needed inside the container.

---

## 6. Phase 3: Snowflake Setup & Hourly Load

### One-time setup (`snowflake/setup.py`)
Creates `STOCK_DB`, the three schemas, the raw table and the S3 stage:

```sql
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
);

CREATE STAGE IF NOT EXISTS STOCK_DB.RAW.s3_stage
  URL = 's3://<bucket>/stock-prices/'
  CREDENTIALS = (AWS_KEY_ID = '...' AWS_SECRET_KEY = '...')
  FILE_FORMAT = (TYPE = PARQUET);
```

### Hourly load (`snowflake/load.py`)
```sql
COPY INTO STOCK_PRICES
FROM @s3_stage
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR  = CONTINUE
PURGE     = FALSE;
```

---

## 7. Phase 4: dbt Transformations (CLEANED → BUSINESS_READY)

| Schema | Model | Materialization | Description |
| :--- | :--- | :--- | :--- |
| `CLEANED` | `stg_stock_prices` | view | Renamed, typed ticks; drops rows with null ticker or null/zero price |
| `CLEANED` | `int_daily_ohlcv` | view | One Open/High/Low/Close/Volume row per ticker per day, plus tick count |
| `BUSINESS_READY` | `fct_stock_performance` | table | MA-7, MA-30, daily % change, intraday range ($ and %) |

### Daily OHLCV with `QUALIFY`
```sql
daily_open AS (
    SELECT ticker, DATE(price_timestamp) AS trade_date, open_price AS daily_open
    FROM base
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ticker, DATE(price_timestamp)
        ORDER BY price_timestamp ASC
    ) = 1
)
```

### Moving averages and % change
```sql
ROUND(AVG(daily_close) OVER (
    PARTITION BY ticker ORDER BY trade_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
), 4) AS moving_avg_7d,

ROUND(AVG(daily_close) OVER (
    PARTITION BY ticker ORDER BY trade_date
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
), 4) AS moving_avg_30d,

ROUND(((daily_close - prev_close) / NULLIF(prev_close, 0)) * 100, 4) AS daily_pct_change
```

### Data quality tests
`not_null` on keys, dates, closes, volumes and moving averages, plus `accepted_values` on `ticker` in the staging and mart models.

---

## 8. Phase 5: Orchestration with Airflow

The `stock_market_pipeline` DAG runs `@hourly` with `catchup=False` and 3 retries:

```text
load_s3_to_snowflake_raw  >>  dbt_run_transformations  >>  dbt_test_data_quality
   (PythonOperator)              (BashOperator)               (BashOperator)
```

---

## 9. Repository Structure

```text
real-time-stocks-market-data-pipeline/
├── README.md
├── docs/
│   └── Architecture Diagram.png
├── docker-compose.yml                  <-- Kafka, Zookeeper, producer, consumer, Airflow, Postgres
├── .env.example                        <-- All required environment variables
├── requirements.txt
├── requirements-airflow.txt
├── kafka_producer/
│   └── producer.py                     <-- Yahoo Finance → Kafka
├── kafka_consumer/
│   └── consumer.py                     <-- Kafka → S3 Parquet
├── snowflake/
│   ├── setup.py                        <-- One-time: DB, schemas, table, S3 stage
│   └── load.py                         <-- Hourly: COPY INTO RAW
├── dbt_stock/
│   ├── dbt_project.yml
│   ├── macros/generate_schema_name.sql <-- Writes models to CLEANED / BUSINESS_READY
│   └── models/
│       ├── staging/                    <-- stg_stock_prices + sources
│       ├── intermediate/               <-- int_daily_ohlcv
│       └── marts/                      <-- fct_stock_performance
└── dags/
    └── stock_pipeline_dag.py           <-- Airflow DAG
```

---

## 10. Complete Step-by-Step Reproduction Guide

### Prerequisites
Docker Desktop, Python 3.11+, a Snowflake account and an AWS account with an S3 bucket.

### Step 1: Clone and Configure
```bash
git clone https://github.com/alwyn-munatsi/real-time-stocks-market-data-pipeline.git
cd real-time-stocks-market-data-pipeline
cp .env.example .env
```
Fill in `.env`:
```text
AWS_ACCESS_KEY_ID=            SNOWFLAKE_ACCOUNT=
AWS_SECRET_ACCESS_KEY=        SNOWFLAKE_USER=
AWS_REGION=                   SNOWFLAKE_PASSWORD=
S3_BUCKET_NAME=               SNOWFLAKE_ROLE=
                              SNOWFLAKE_WAREHOUSE=
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC_STOCK_PRICES=stock-prices
STOCK_TICKERS=AAPL,MSFT,GOOGL,TSLA,NVDA,AMZN,META,JPM
```

### Step 2: Set Up Snowflake (one-time)
```bash
pip install snowflake-connector-python python-dotenv
python snowflake/setup.py
```

### Step 3: Start All Services
```bash
docker-compose up -d
```
Check the producer and consumer logs to confirm quotes are flowing and Parquet files are landing in S3.

### Step 4: Enable the Airflow DAG
Open `http://localhost:8080` (default `admin` / `admin`) and switch on `stock_market_pipeline`.

### Step 5: (Optional) Run dbt Manually
```bash
cd dbt_stock
pip install "dbt-snowflake>=1.8.0,<1.9.0" "dbt-core>=1.8.0,<1.9.0" "dbt-adapters>=1.0.0,<2.0.0"
dbt run --profiles-dir . --target prod
dbt test --profiles-dir . --target prod
```

### Step 6: Connect Power BI
Use the Snowflake connector, point it at `STOCK_DB.BUSINESS_READY.FCT_STOCK_PERFORMANCE`, and build visuals on close price vs MA-7/MA-30, daily % change and volume by ticker.
