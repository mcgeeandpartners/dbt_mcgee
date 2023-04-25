{{ config(alias="dash_kpi_all_by_reporting_period") }}

SELECT * FROM {{ ref('dash_consolidated_by_reporting_period_ad') }}
UNION
SELECT * FROM {{ ref('dash_consolidated_by_reporting_period_alice_ames') }}