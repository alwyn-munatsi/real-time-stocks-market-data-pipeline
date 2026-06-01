-- Mart model: fct_stock_performance
--
-- Purpose: the final, business-ready fact table that Power BI reads from.
-- Adds moving averages, daily % change, and intraday range metrics on top
-- of the daily OHLCV data.
--
-- Moving averages explain:
--   MA-7  = average closing price over the last 7 trading days
--   MA-30 = average closing price over the last 30 trading days
-- These are used by analysts to smooth out noise and spot trends:
--   - When close_price > MA-30 the stock is in an upward trend
--   - When MA-7 crosses above MA-30 it's a "golden cross" — a bullish signal
--
-- ROWS BETWEEN N PRECEDING AND CURRENT ROW is a sliding window frame:
-- for each row, Snowflake looks back N rows within the same ticker partition
-- and computes the average over that window.

{{ config(materialized='table') }}

WITH daily AS (
    SELECT * FROM {{ ref('int_daily_ohlcv') }}
),

enriched AS (
    SELECT
        ticker,
        trade_date,
        daily_open,
        daily_high,
        daily_low,
        daily_close,
        daily_volume,
        ticks_in_day,

        -- 7-day simple moving average (requires at least 1 row, partial windows are fine)
        ROUND(AVG(daily_close) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 4) AS moving_avg_7d,

        -- 30-day simple moving average
        ROUND(AVG(daily_close) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 4) AS moving_avg_30d,

        -- Previous trading day's close — used to compute daily_pct_change below
        LAG(daily_close) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
        ) AS prev_close,

        -- Dollar range of the trading day (high - low)
        ROUND(daily_high - daily_low, 4) AS intraday_range,

        -- Range as a percentage of the open price
        ROUND(
            ((daily_high - daily_low) / NULLIF(daily_open, 0)) * 100,
            4
        ) AS intraday_range_pct
    FROM daily
)

SELECT
    ticker,
    trade_date,
    daily_open,
    daily_high,
    daily_low,
    daily_close,
    daily_volume,
    ticks_in_day,
    moving_avg_7d,
    moving_avg_30d,
    -- NULLIF(prev_close, 0) prevents division-by-zero on the very first row
    ROUND(
        ((daily_close - prev_close) / NULLIF(prev_close, 0)) * 100,
        4
    ) AS daily_pct_change,
    intraday_range,
    intraday_range_pct,
    CURRENT_TIMESTAMP() AS _dbt_updated_at
FROM enriched
