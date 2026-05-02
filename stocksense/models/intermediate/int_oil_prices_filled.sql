-- Note: 2013-01-01 has no oil price in the source data (stg_oil).
-- Forward-fill cannot fill this row since it is the first date in the series.
-- Decision: leave NULL rather than backfill. Downstream consumers must handle
-- NULL oil_price_usd appropriately. See README for full rationale.

WITH date_spine AS (
    SELECT 
        UNNEST(generate_series(
            (SELECT MIN(sale_date) FROM {{ ref('stg_train') }}), 
            (SELECT MAX(sale_date) FROM {{ ref('stg_train') }}), 
            INTERVAL 1 DAY
        )) AS price_date
),

joined_oil AS (
    -- 2. LEFT JOIN the staging oil data onto the date spine
    SELECT 
        ds.price_date,
        so.oil_price_usd AS raw_price
    FROM date_spine ds
    LEFT JOIN {{ ref('stg_oil') }} so 
        ON ds.price_date = so.date
),

oil_forward_filled AS (
    -- 3. Forward-fill the NULLs using the last known non-null price
    SELECT 
        price_date,
        raw_price,
        LAST_VALUE(raw_price IGNORE NULLS) OVER (
            ORDER BY price_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS forward_filled_price
    FROM joined_oil
)

SELECT
    price_date,
    forward_filled_price AS oil_price_usd
FROM oil_forward_filled