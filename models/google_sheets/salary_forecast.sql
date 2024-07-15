select
    department,
    employee_name,
    employment_type,
    region,
    annual_cost,
    payroll_region,
    role,
    estimate_cac_,
    case
        when substr(month_year, 1, 3) = 'JAN'
        then 'JANUARY'
        when substr(month_year, 1, 3) = 'FEB'
        then 'FEBRUARY'
        when substr(month_year, 1, 3) = 'MAR'
        then 'MARCH'
        when substr(month_year, 1, 3) = 'APR'
        then 'APRIL'
        when substr(month_year, 1, 3) = 'MAY'
        then 'MAY'
        when substr(month_year, 1, 3) = 'JUN'
        then 'JUNE'
        when substr(month_year, 1, 3) = 'JUL'
        then 'JULY'
        when substr(month_year, 1, 3) = 'AUG'
        then 'AUGUST'
        when substr(month_year, 1, 3) = 'SEP'
        then 'SEPTEMBER'
        when substr(month_year, 1, 3) = 'OCT'
        then 'OCTOBER'
        when substr(month_year, 1, 3) = 'NOV'
        then 'NOVEMBER'
        when substr(month_year, 1, 3) = 'DEC'
        then 'DECEMBER'
    end as month,
    '20' || substr(month_year, 5, 2) as year,
    amount
from
    {{ source("google_sheets", "salary_forecast_sheet_1") }} unpivot (
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
            jul_23,
            aug_23,
            sep_23,
            oct_23,
            nov_23,
            dec_23
        )
    ) as unpvt
