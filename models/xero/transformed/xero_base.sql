select jl.journal_line_id,
    jl.account_id,
    jl.account_code,
    jl.account_type,
    jl.account_name,
    jl.description,
    jl.tax_type,
    jl.tax_name,
    jl.tax_amount,
    jl.gross_amount*-1 as gross_amount,
    jl.net_amount*-1 as net_amount,
    j.journal_id,
    j.created_date_utc,
    j.journal_date,
    j.journal_number,
    j.reference,
    j.source_id,
    j.source_type,
    TRIM(SPLIT_PART(jl.account_name, '-', 1)) AS product,
    TRIM(SPLIT_PART(jl.account_name, '-', 2)) AS revenue_type,
    jlt.tracking_category_id,
    jlt.option,
    tcho.tracking_option_id,
    tco.name as customer_name,
    tc.name as tracking_category_name,
    cms.country,
    cms.region
from {{source('xero', 'journal')}} j 
left join {{source('xero', 'journal_line')}} jl
on j.journal_id = jl.journal_id
left join {{source('xero', 'journal_line_has_tracking_category')}} jlt 
on jl.journal_line_id = jlt.journal_line_id 
and jl.journal_id = jlt.journal_id
left join {{source('xero', 'tracking_category_has_option')}} tcho
on jlt.tracking_category_id = tcho.tracking_category_id
and jlt.tracking_category_option_id = tcho.tracking_option_id
left join {{source('xero', 'tracking_category_option')}} tco
on tcho.tracking_option_id = tco.tracking_option_id
left join {{source('xero', 'tracking_category')}} tc 
on tc.tracking_category_id = jlt.tracking_category_id
left join {{source('xero_sp', 'customer_metadata_sheet_1')}} cms 
on lower(cms.CUSTOMER_XERO_) = lower(tco.name)