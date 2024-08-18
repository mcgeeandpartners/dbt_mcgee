with polymer as (
    select
    jl.journal_line_id, 
    jl.account_id, 
    jl.account_code, 
    jl.account_type, 
    jl.account_name, 
    CASE 
    WHEN ACCOUNT_REPORT = 'PROFITLOSS' THEN jl.net_amount * -1  
    ELSE jl.net_amount * 1
    END AS net_amount,
    jl.net_amount  as net_amount_original, 
    j.journal_id, 
    j.journal_date, 
    psp.account_category, 
    psp.account_report, 
    psp.account_class,
    'POLYMER' as entity

from {{ source("polymer", "journal") }} j
left join {{ source("polymer", "journal_line") }} jl on j.journal_id = jl.journal_id
left join
    {{ source("polymer", "journal_line_has_tracking_category") }} jlt
    on jl.journal_line_id = jlt.journal_line_id
    and jl.journal_id = jlt.journal_id
left join
    {{ source("polymer", "tracking_category_has_option") }} tcho
    on jlt.tracking_category_id = tcho.tracking_category_id
    and jlt.tracking_category_option_id = tcho.tracking_option_id
left join
    {{ source("polymer", "tracking_category_option") }} tco
    on tcho.tracking_option_id = tco.tracking_option_id
left join
    {{ source("polymer", "tracking_category") }} tc
    on tc.tracking_category_id = jlt.tracking_category_id

left join
    {{ source("polymer_sp", "coa_metadata_account") }} psp
    on lower(psp.account_id) = lower(jl.account_id)
)
select pl.*, dd.fiscal_year, dd.fiscal_quarter,dd.yearmonth from polymer pl

left join {{ ref("dim_date") }} dd on pl.journal_date::date = dd.date