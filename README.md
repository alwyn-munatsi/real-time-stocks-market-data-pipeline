# Real-Time Stock Market Data Pipeline

An end-to-end data engineering pipeline that streams live stock market prices, stores them in a data lake, transforms them through a multi-layer data warehouse, and serves business-ready analytics to a Power BI dashboard — all containerised with Docker.

![Architecture](docs/Architecture%20Diagram.png)

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Source | Yahoo Finance (`yfinance`) |
| Streaming | Apache Kafka + Zookeeper |
| Data Lake | Amazon S3 (Parquet) |
| Data Warehouse | Snowflake |
| Transformations | dbt (staging → intermediate → marts) |
| Orchestration | Apache Airflow |
| Containerisation | Docker + Docker Compose |
| Visualisation | Power BI |

---

## Architecture

```
Yahoo Finance API
      │  (every 60s)
      ▼
Kafka Producer ──► Kafka Topic (stock-prices)
                         │
                         ▼
                  Kafka Consumer ──► S3 Bucket (Parquet files)
                                           │
                                           ▼ (Airflow — hourly)
                                    Snowflake RAW
                                           │
                                           ▼ (dbt)
                                    Snowflake CLEANED
                                           │
                                           ▼ (dbt)
                                   BUSINESS_READY ──► Power BI
```

---

## Tracked Tickers

`AAPL` `MSFT` `GOOGL` `TSLA` `NVDA` `AMZN` `META` `JPM`

---

## Project Structure

```
stock-pipeline/
├── kafka_producer/         # Polls Yahoo Finance, publishes to Kafka
│   └── producer.py
├── kafka_consumer/         # Reads Kafka, writes Parquet to S3
│   └── consumer.py
├── snowflake/
│   ├── setup.py            # One-time: creates DB, schemas, table, S3 stage
│   └── load.py             # Hourly: COPY INTO from S3 → Snowflake RAW
├── dbt_stock/
│   ├── models/
│   │   ├── staging/        # stg_stock_prices (clean + type raw data)
│   │   ├── intermediate/   # int_daily_ohlcv (collapse ticks → daily OHLCV)
│   │   └── marts/          # fct_stock_performance (MA-7, MA-30, % change)
│   └── macros/
├── dags/
│   └── stock_pipeline_dag.py   # Airflow DAG: load S3 → dbt run → dbt test
├── docker-compose.yml      # All services: Kafka, Airflow, Postgres
├── requirements.txt
└── requirements-airflow.txt
```

---

## dbt Data Layers

| Schema | Model | Description |
|---|---|---|
| `CLEANED` | `stg_stock_prices` | Typed and cleaned raw ticks |
| `CLEANED` | `int_daily_ohlcv` | One OHLCV row per ticker per day |
| `BUSINESS_READY` | `fct_stock_performance` | Daily metrics with MA-7, MA-30, daily % change |

---

## Getting Started

### Prerequisites
- Docker Desktop
- Python 3.11+
- Snowflake account
- AWS account with an S3 bucket

### 1. Clone the repo

```bash
git clone https://github.com/alwyn-munatsi/real-time-stocks-market-data-pipeline.git
cd real-time-stocks-market-data-pipeline
```

### 2. Configure environment variables

Copy the example and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env` with your Snowflake account, AWS keys, and Kafka settings.

### 3. Set up Snowflake (one-time)

```bash
pip install snowflake-connector-python python-dotenv
python snowflake/setup.py
```

### 4. Start all services

```bash
docker-compose up -d
```

### 5. Enable the Airflow DAG

Open `http://localhost:8080` (admin / admin) and enable `stock_market_pipeline`.

### 6. Run dbt manually (optional)

```bash
cd dbt_stock
pip install "dbt-snowflake>=1.8.0,<1.9.0" "dbt-core>=1.8.0,<1.9.0" "dbt-adapters>=1.0.0,<2.0.0"
dbt run --profiles-dir . --target prod
dbt test --profiles-dir . --target prod
```

---

## Airflow Pipeline

The DAG `stock_market_pipeline` runs **hourly** and executes three tasks in sequence:

1. `load_s3_to_snowflake_raw` — COPY INTO new Parquet files from S3
2. `dbt_run_transformations` — run all dbt models
3. `dbt_test_data_quality` — validate output with dbt schema tests

---

## Environment Variables

Create a `.env` file with the following (see `.env.example`):

```
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=
S3_BUCKET_NAME=

SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_ROLE=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA_RAW=RAW
SNOWFLAKE_SCHEMA_CLEANED=CLEANED
SNOWFLAKE_SCHEMA_BUSINESS=BUSINESS_READY

KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC_STOCK_PRICES=stock-prices
STOCK_TICKERS=AAPL,MSFT,GOOGL,TSLA,NVDA,AMZN,META,JPM

AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
```
