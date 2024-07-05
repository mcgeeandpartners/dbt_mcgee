{{config(
    materialized="view"
)}}

WITH ColumnMapping AS (
    SELECT
        *,
        CASE 
            WHEN EXTRACT(MONTH FROM CURRENT_DATE) = 7 THEN JUN_24 + MAY_24 + APR_24 + MAR_24 + FEB_24 + JAN_24 + DEC_23 + NOV_23 + OCT_23 + SEP_23 + AUG_23 + JUL_23
        END AS rolling_sum_last_12_months
    FROM {{source('xero_sp', 'salary_forecast_sheet_1')}}
)
SELECT 
    *,
    estimate_cac_ * rolling_sum_last_12_months AS total_marketing_expense
FROM ColumnMapping
