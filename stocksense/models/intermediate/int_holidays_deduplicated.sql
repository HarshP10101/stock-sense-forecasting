-- One holiday row is kept per holiday_date for downstream date-level joins.
-- Precedence rule: National holidays override Regional holidays, and Regional
-- holidays override Local holidays. When multiple rows exist at the same scope,
-- non-transferred holidays are preferred over transferred holidays. If multiple
-- rows are still tied, holiday_name is used as a deterministic tie-breaker.
-- This keeps dim_date at one row per date while preserving the most broadly
-- applicable holiday context.

WITH ranked_holidays AS (
    SELECT
        holiday_date,
        holiday_type,
        region_scope,
        region_name,
        holiday_name,
        is_transferred,

        ROW_NUMBER() OVER (
            PARTITION BY holiday_date
            ORDER BY
                CASE region_scope
                    WHEN 'National' THEN 1
                    WHEN 'Regional' THEN 2
                    WHEN 'Local' THEN 3
                    ELSE 99
                END,
                is_transferred,
                holiday_name
        ) AS row_num

    FROM {{ ref('stg_holidays_events') }}
)

SELECT
    holiday_date,
    holiday_type,
    region_scope,
    region_name,
    holiday_name,
    is_transferred
FROM ranked_holidays
WHERE row_num = 1