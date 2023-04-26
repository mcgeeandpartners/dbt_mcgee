{{ config(alias="dash_kpi_all_by_reporting_period") }}

SELECT 'American Duchess' as sub_company, * FROM {{ ref('dash_consolidated_by_reporting_period_ad') }}
UNION
SELECT 'Alice and Ames' as sub_company, * FROM {{ ref('dash_consolidated_by_reporting_period_alice_ames') }}