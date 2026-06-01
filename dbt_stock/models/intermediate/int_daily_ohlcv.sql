-- Intermediate model: int_daily_ohlcv
--
-- Purpose: collapse intraday price ticks into one canonical OHLCV row
-- per ticker per trading day.
--
-- OHLCV stands for:
--   Open  — the price at the FIRST tick of that trading day
--   High  — the HIGHEST price seen across all ticks that day
--   Low   — the LOWEST price seen across all ticks that day
--   Close — the price at the LAST tick of that trading day
--   Volume — SUM of all volume values recorded that day
--
-- QUALIFY is a Snowflake-specific clause that filters rows AFTER window
-- functions have been evaluated — it's the cleanest way to pick the first
-- or last row within a partition without a subquery.

{{ config(materialized='view') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_stock_prices') }}
),

-- First tick of each (ticker, day) pair → gives us the Open price
daily_open AS (
    SELECT
        ticker,
        DATE(price_timestamp) AS trade_date,
        open_price            AS daily_open
    FROM base
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ticker, DATE(price_timestamp)
        ORDER BY price_timestamp ASC
    ) = 1
),

-- Last tick of each (ticker, day) pair → gives us the Close price
daily_close AS (
    SELECT
        ticker,
        DATE(price_timestamp) AS trade_date,
        close_price           AS daily_close
    FROM base
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ticker, DATE(price_timestamp)
        ORDER BY price_timestamp DESC
    ) = 1
),

-- Aggregate the rest: High, Low, Volume, and tick count
daily_agg AS (
    SELECT
        ticker,
        DATE(price_timestamp) AS trade_date,
        MAX(high_price)       AS daily_high,
        MIN(low_price)        AS daily_low,
        SUM(volume)           AS daily_volume,
        COUNT(*)              AS ticks_in_day
    FROM base
    GROUP BY ticker, DATE(price_timestamp)
)

SELECT
    agg.ticker,
    agg.trade_date,
    o.daily_open,
    agg.daily_high,
    agg.daily_low,
    c.daily_close,
    agg.daily_volume,
    agg.ticks_in_day
FROM daily_agg   agg
JOIN daily_open  o ON agg.ticker = o.ticker AND agg.trade_date = o.trade_date
JOIN daily_close c ON agg.ticker = c.ticker AND agg.trade_date = c.trade_date
