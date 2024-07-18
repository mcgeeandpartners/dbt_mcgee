{{ config(materialized="view") }}

with
    columnmapping as (
        select
            *,
            case
                when extract(month from current_date) = 7
                then
                    jun_24
                    + may_24
                    + apr_24
                    + mar_24
                    + feb_24
                    + jan_24
                    + dec_23
                    + nov_23
                    + oct_23
                    + sep_23
                    + aug_23
                    + jul_23
            end as rolling_sum_last_12_months
        from {{ source("xero_sp", "salary_forecast_salary_forecast") }}
    )
select *, estimate_cac_ * rolling_sum_last_12_months as total_marketing_expense
from columnmapping
