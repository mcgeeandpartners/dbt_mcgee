{% set dict_data={'impress': ['Client', 'Directors','IAG436'],'pearshop': ['Client', 'Directors','IAG436']} %}

select *,'ACTUAL' as DATA_TYPE from (
{{ xero_financials_common(dict_data) }}
)

union all

select 
NULL as JOURNAL_LINE_ID,
ACCOUNT_ID,
ACCOUNT_CODE,
ACCOUNT_TYPE,
ACCOUNT_NAME,
net_amount as net_amount_original,  
CASE 
    WHEN account_type in ('REVENUE','OTHERINCOME') THEN net_amount * -1  
    ELSE net_amount * 1
END AS net_amount,
NULL as JOURNAL_ID,
TRANSACTION_DATE as JOURNAL_DATE,
ACCOUNT_CATEGORY,
ACCOUNT_REPORT,
ACCOUNT_CLASS,
WORKING_CAPITAL,
ENTITY,
NULL as DIRECTORS,
NULL as CLIENT,
NULL as IAG436,
FISCAL_YEAR,
FISCAL_QUARTER,
TO_CHAR(YEARMONTH::TIMESTAMP, 'YYYY-MM')  as YEARMONTH,
DATA_TYPE
 from impress_database.impress_share_point.FORECAST_SNOWFLAKE_COMBINED_FORECAST