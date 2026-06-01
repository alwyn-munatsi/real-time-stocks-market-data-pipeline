-- Staging model: stg_stock_prices
--
-- Purpose: clean and rename the raw source columns into a consistent,
-- well-typed format that every downstream model can rely on.
--
-- Rules applied here:
--   1. Rename columns to snake_case descriptive names
--   2. Cast timestamp to TIMESTAMP_NTZ (timezone-free UTC)
--   3. Filter out rows where price is NULL or zero — these indicate
--      a failed Yahoo Finance fetch and should not flow downstream
--   4. No aggregation, no joins — staging is strictly 1-to-1 with source rows

{{ config(materialized='view') }}

SELECT
    ticker,
    CAST(timestamp AS TIMESTAMP_NTZ)       AS price_timestamp,
    price                                  AS close_price,
    open                                   AS open_price,
    high                                   AS high_price,
    low                                    AS low_price,
    volume,
    previous_close,
    market_cap,
    exchange,
    _loaded_at
FROM {{ source('raw', 'STOCK_PRICES') }}
WHERE
    ticker     IS NOT NULL
    AND price  IS NOT NULL
    AND price  > 0
