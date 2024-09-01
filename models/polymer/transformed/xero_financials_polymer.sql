 with polymer_clients as (
    select
    jl.journal_line_id, 
    jl.account_id, 
    jl.account_code, 
    jl.account_type, 
    jl.account_name, 
    jl.net_amount  as net_amount_original,  
    CASE 
    WHEN jl.account_type in ('REVENUE','OTHERINCOME')THEN jl.net_amount * -1  
    ELSE jl.net_amount * 1
    END AS net_amount,
    j.journal_id, 
    j.journal_date, 
    psp.account_category, 
    psp.account_report, 
    psp.account_class,
    psp.working_apital_,
    'POLYMER' as entity,
 --   jlt.tracking_category_id,
 --   jlt.option,
 --   tcho.tracking_option_id,
 --   tco.name as customer_name,
 --   tc.name as tracking_category_name,
   tco.name as Client

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
where tc.name='Clients' or tc.name is NULL 
)
,
  polymer_division as (
    select
    jl.journal_line_id, 
    jl.account_id, 
    jl.account_code, 
    jl.account_type, 
    jl.account_name, 
    jl.net_amount  as net_amount_original,  
    CASE 
    WHEN jl.account_type in ('REVENUE','OTHERINCOME')THEN jl.net_amount * -1  
    ELSE jl.net_amount * 1
    END AS net_amount, 
    j.journal_id, 
    j.journal_date, 
    psp.account_category, 
    psp.account_report, 
    psp.account_class,
    psp.working_apital_,
    'POLYMER' as entity,
   -- jlt.tracking_category_id,
   -- jlt.option,
   -- tcho.tracking_option_id,
   -- tco.name as customer_name,
   -- tc.name as tracking_category_name,
     tco.name as Division

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
    on (psp.account_id) = (jl.account_id)
where tc.name='Division'
),
polymer as (
    select 
        COALESCE(pd.journal_line_id, pc.journal_line_id) AS journal_line_id,
    COALESCE(pd.account_id, pc.account_id) AS account_id,
    COALESCE(pd.account_code, pc.account_code) AS account_code,
    COALESCE(pd.account_type, pc.account_type) AS account_type,
    COALESCE(pd.account_name, pc.account_name) AS account_name,
    COALESCE(pd.net_amount_original,pc.net_amount_original) as net_amount_original,
    COALESCE(pd.net_amount, pc.net_amount) AS net_amount,
    COALESCE(pd.journal_id, pc.journal_id) AS journal_id,
    COALESCE(pd.journal_date, pc.journal_date) AS journal_date,
    COALESCE(pd.account_category, pc.account_category) AS account_category,
    COALESCE(pd.account_report, pc.account_report) AS account_report,
    COALESCE(pd.account_class, pc.account_class) AS account_class,
    COALESCE(pd.working_apital_, pc.working_apital_) AS working_apital,
    'POLYMER' AS entity,

   -- COALESCE(pd.tracking_category_id, pc.tracking_category_id) AS tracking_category_id,
   -- COALESCE(pd.option, pc.option) AS option,
   -- COALESCE(pd.tracking_option_id, pc.tracking_option_id) AS tracking_option_id,
   -- COALESCE(pd.customer_name, pc.customer_name) AS customer_name,
   -- COALESCE(pd.tracking_category_name, pc.tracking_category_name) AS tracking_category_name,
    pd.Division,
	pc.Client
     from polymer_clients pc
    full outer join polymer_division  pd on pc.journal_line_id=pd.journal_line_id 
),
 reactivate_clients as (
    select
    jl.journal_line_id, 
    jl.account_id, 
    jl.account_code, 
    jl.account_type, 
    jl.account_name,
    jl.net_amount  as net_amount_original,  
    CASE 
    WHEN jl.account_type in ('REVENUE','OTHERINCOME')THEN jl.net_amount * -1  
    ELSE jl.net_amount * 1
    END AS net_amount,
    j.journal_id, 
    j.journal_date, 
    psp.account_category, 
    psp.account_report, 
    psp.account_class,
    psp.working_apital_ as  working_apital,
    'REACTIVATE' as entity,
    -- jlt.tracking_category_id,
    -- jlt.option,
    -- tcho.tracking_option_id,
    -- tco.name as customer_name,
    -- tc.name as tracking_category_name,
     NULL as Division,
     tco.name as Clients 

from {{ source("reactivate", "journal") }} j
left join {{ source("reactivate", "journal_line") }} jl on j.journal_id = jl.journal_id
left join
    {{ source("reactivate", "journal_line_has_tracking_category") }} jlt
    on jl.journal_line_id = jlt.journal_line_id
    and jl.journal_id = jlt.journal_id
left join
    {{ source("reactivate", "tracking_category_has_option") }} tcho
    on jlt.tracking_category_id = tcho.tracking_category_id
    and jlt.tracking_category_option_id = tcho.tracking_option_id
left join
    {{ source("reactivate", "tracking_category_option") }} tco
    on tcho.tracking_option_id = tco.tracking_option_id
left join
    {{ source("reactivate", "tracking_category") }} tc
    on tc.tracking_category_id = jlt.tracking_category_id

left join
    {{ source("polymer_sp", "coa_metadata_account") }} psp
    on lower(psp.account_id) = lower(jl.account_id)
where tc.name='Clients' or tc.name is null
)
select pl.*, dd.fiscal_year, dd.fiscal_quarter,dd.yearmonth from polymer pl

left join {{ ref("dim_date") }} dd on pl.journal_date::date = dd.date

union all

select rc.*, dd.fiscal_year, dd.fiscal_quarter,dd.yearmonth from reactivate_clients rc

left join {{ ref("dim_date") }} dd on rc.journal_date::date = dd.date

