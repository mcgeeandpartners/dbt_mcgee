with
    base as (
        select
            department,
            employee_name,
            employment_type,
            region,
            annual_cost,
            payroll_region,
            role,
            estimate_cac_,
            account_id,
            case
                when substr(month_year, 1, 3) = 'JAN'
                then '01'
                when substr(month_year, 1, 3) = 'FEB'
                then '02'
                when substr(month_year, 1, 3) = 'MAR'
                then '03'
                when substr(month_year, 1, 3) = 'APR'
                then '04'
                when substr(month_year, 1, 3) = 'MAY'
                then '05'
                when substr(month_year, 1, 3) = 'JUN'
                then '06'
                when substr(month_year, 1, 3) = 'JUL'
                then '07'
                when substr(month_year, 1, 3) = 'AUG'
                then '08'
                when substr(month_year, 1, 3) = 'SEP'
                then '09'
                when substr(month_year, 1, 3) = 'OCT'
                then '10'
                when substr(month_year, 1, 3) = 'NOV'
                then '11'
                when substr(month_year, 1, 3) = 'DEC'
                then '12'
            end as month,
            '20' || substr(month_year, 5, 2) as year,
            to_date(
                concat(
                    '20',
                    substr(month_year, 5, 2),
                    '-',
                    case
                        when substr(month_year, 1, 3) = 'JAN'
                        then '01'
                        when substr(month_year, 1, 3) = 'FEB'
                        then '02'
                        when substr(month_year, 1, 3) = 'MAR'
                        then '03'
                        when substr(month_year, 1, 3) = 'APR'
                        then '04'
                        when substr(month_year, 1, 3) = 'MAY'
                        then '05'
                        when substr(month_year, 1, 3) = 'JUN'
                        then '06'
                        when substr(month_year, 1, 3) = 'JUL'
                        then '07'
                        when substr(month_year, 1, 3) = 'AUG'
                        then '08'
                        when substr(month_year, 1, 3) = 'SEP'
                        then '09'
                        when substr(month_year, 1, 3) = 'OCT'
                        then '10'
                        when substr(month_year, 1, 3) = 'NOV'
                        then '11'
                        when substr(month_year, 1, 3) = 'DEC'
                        then '12'
                    end,
                    '-01'
                ),
                'YYYY-MM-DD'
            ) as month_year_date,
            amount
        from
            {{ source("google_sheets", "salary_forecast") }} unpivot (
                amount for month_year in (
                    jan_24,
                    feb_24,
                    mar_24,
                    apr_24,
                    may_24,
                    jun_24,
                    jul_24,
                    aug_24,
                    sep_24,
                    oct_24,
                    nov_24,
                    dec_24,
                    jan_25,
                    feb_25,
                    mar_25,
                    apr_25,
                    may_25,
                    jun_25,
                    jul_25,
                    jul_23,
                    aug_23,
                    sep_23,
                    oct_23,
                    nov_23,
                    dec_23
                )
            ) as unpvt
    )
select base.*, dd.fiscal_year, dd.fiscal_quarter, 'FORECAST' as data_type
from base
left join {{ ref("dim_date") }} dd on base.month_year_date::date = dd.date
