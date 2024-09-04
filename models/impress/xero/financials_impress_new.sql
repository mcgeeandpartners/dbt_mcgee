 with impress_clients as (
    select
    jl.journal_line_id, 
    jl.account_id, 
    jl.account_code, 
    jl.account_type, 
    jl.account_name, 
    jl.net_amount  as net_amount_original,  
    CASE 
    WHEN jl.account_type in ('REVENUE','DIRECTCOSTS')THEN jl.net_amount * -1  
    ELSE jl.net_amount * 1
    END AS net_amount,
    j.journal_id, 
    j.journal_date, 
    psp.account_category, 
    psp.account_report, 
    --psp.account_class,
    a.class as account_class,
    psp.working_apital_,
    'IMPRESS' as entity,
 --   jlt.tracking_category_id,
 --   jlt.option,
 --   tcho.tracking_option_id,
 --   tco.name as customer_name,
 --   tc.name as tracking_category_name,
   tco.name as Client

from {{ source("impress", "journal") }} j
left join {{ source("impress", "journal_line") }} jl on j.journal_id = jl.journal_id
left join
    {{ source("impress", "journal_line_has_tracking_category") }} jlt
    on jl.journal_line_id = jlt.journal_line_id
    and jl.journal_id = jlt.journal_id
left join
    {{ source("impress", "tracking_category_has_option") }} tcho
    on jlt.tracking_category_id = tcho.tracking_category_id
    and jlt.tracking_category_option_id = tcho.tracking_option_id
left join
    {{ source("impress", "tracking_category_option") }} tco
    on tcho.tracking_option_id = tco.tracking_option_id
left join
    {{ source("impress", "tracking_category") }} tc
    on tc.tracking_category_id = jlt.tracking_category_id
left join
    {{ source("impress", "account") }} a
    on jl.account_id = a.id
left join
    {{ source("impress_sp", "coa_metadata") }} psp
    on lower(psp.account_id) = lower(jl.account_id)
where tc.name='Client' or tc.name is NULL 
)
,
  impress_division as (
    select
    jl.journal_line_id, 
    jl.account_id, 
    jl.account_code, 
    jl.account_type, 
    jl.account_name, 
    jl.net_amount  as net_amount_original,  
    CASE 
    WHEN jl.account_type in ('REVENUE','DIRECTCOSTS')THEN jl.net_amount * -1  
    ELSE jl.net_amount * 1
    END AS net_amount, 
    j.journal_id, 
    j.journal_date, 
    psp.account_category, 
    psp.account_report, 
    --psp.account_class,
    a.class as account_class,
    psp.working_apital_,
    'IMPRESS' as entity,
   -- jlt.tracking_category_id,
   -- jlt.option,
   -- tcho.tracking_option_id,
   -- tco.name as customer_name,
   -- tc.name as tracking_category_name,
     tco.name as Directors

from {{ source("impress", "journal") }} j
left join {{ source("impress", "journal_line") }} jl on j.journal_id = jl.journal_id
left join
    {{ source("impress", "journal_line_has_tracking_category") }} jlt
    on jl.journal_line_id = jlt.journal_line_id
    and jl.journal_id = jlt.journal_id
left join
    {{ source("impress", "tracking_category_has_option") }} tcho
    on jlt.tracking_category_id = tcho.tracking_category_id
    and jlt.tracking_category_option_id = tcho.tracking_option_id
left join
    {{ source("impress", "tracking_category_option") }} tco
    on tcho.tracking_option_id = tco.tracking_option_id
left join
    {{ source("impress", "tracking_category") }} tc
    on tc.tracking_category_id = jlt.tracking_category_id
left join
    {{ source("impress", "account") }} a
    on jl.account_id = a.id
left join
    {{ source("impress_sp", "coa_metadata") }} psp
    on lower(psp.account_id) = lower(jl.account_id)
where tc.name='Directors'
),
impress as (
    select 
        COALESCE(pd.journal_line_id, pc.journal_line_id) AS journal_line_id,
    COALESCE(pd.account_id, pc.account_id) AS account_id,
    COALESCE(pd.account_code, pc.account_code) AS account_code,
    COALESCE(pd.account_type, pc.account_type) AS account_type,
    COALESCE(pd.account_name, pc.account_name) AS account_name,
    COALESCE(pd.net_amount_original,pc.net_amount_original) as net_amount_original,
    COALESCE(pd.net_amount, pc.net_amount) AS net_amount,
    COALESCE(pd.journal_id, pc.journal_id) AS journal_id,
    COALESCE(pd.journal_date, pc.journal_date) AS transaction_date,
    COALESCE(pd.account_category, pc.account_category) AS account_category,
    COALESCE(pd.account_report, pc.account_report) AS account_report,
    COALESCE(pd.account_class, pc.account_class) AS account_class,
    COALESCE(pd.working_apital_, pc.working_apital_) AS working_apital,
    'IMPRESS' AS entity,

   -- COALESCE(pd.tracking_category_id, pc.tracking_category_id) AS tracking_category_id,
   -- COALESCE(pd.option, pc.option) AS option,
   -- COALESCE(pd.tracking_option_id, pc.tracking_option_id) AS tracking_option_id,
   -- COALESCE(pd.customer_name, pc.customer_name) AS customer_name,
   -- COALESCE(pd.tracking_category_name, pc.tracking_category_name) AS tracking_category_name,
    pd.Directors,
	pc.Client
     from impress_clients pc
    full outer join impress_division  pd on pc.journal_line_id=pd.journal_line_id 
),
 pearshop_clients as (
    select
    jl.journal_line_id, 
    jl.account_id, 
    jl.account_code, 
    jl.account_type, 
    jl.account_name,
    jl.net_amount  as net_amount_original,  
    CASE 
    WHEN jl.account_type in ('REVENUE','DIRECTCOSTS')THEN jl.net_amount * -1  
    ELSE jl.net_amount * 1
    END AS net_amount,
    j.journal_id, 
    j.journal_date, 
    psp.account_category, 
    psp.account_report, 
    --psp.account_class,
    a.class as account_class,
    psp.working_apital_,
    'PEARSHOP' as entity,
    -- jlt.tracking_category_id,
    -- jlt.option,
    -- tcho.tracking_option_id,
    -- tco.name as customer_name,
    -- tc.name as tracking_category_name,
    -- NULL as Director,
     tco.name as Client

from {{ source("pearshop", "journal") }} j
left join {{ source("pearshop", "journal_line") }} jl on j.journal_id = jl.journal_id
left join
    {{ source("pearshop", "journal_line_has_tracking_category") }} jlt
    on jl.journal_line_id = jlt.journal_line_id
    and jl.journal_id = jlt.journal_id
left join
    {{ source("pearshop", "tracking_category_has_option") }} tcho
    on jlt.tracking_category_id = tcho.tracking_category_id
    and jlt.tracking_category_option_id = tcho.tracking_option_id
left join
    {{ source("pearshop", "tracking_category_option") }} tco
    on tcho.tracking_option_id = tco.tracking_option_id
left join
    {{ source("pearshop", "tracking_category") }} tc
    on tc.tracking_category_id = jlt.tracking_category_id
left join
    {{ source("pearshop", "account") }} a
    on jl.account_id = a.id
left join
    {{ source("impress_sp", "coa_metadata") }} psp
    on lower(psp.account_id) = lower(jl.account_id)
where tc.name='Client' or tc.name is null
),
pearshop_division as (
    select
    jl.journal_line_id, 
    jl.account_id, 
    jl.account_code, 
    jl.account_type, 
    jl.account_name, 
    jl.net_amount  as net_amount_original,  
    CASE 
    WHEN jl.account_type in ('REVENUE','DIRECTCOSTS')THEN jl.net_amount * -1  
    ELSE jl.net_amount * 1
    END AS net_amount, 
    j.journal_id, 
    j.journal_date, 
    psp.account_category, 
    psp.account_report, 
    -- psp.account_class,
    a.class as account_class,
    psp.working_apital_,
    'IMPRESS' as entity,
   -- jlt.tracking_category_id,
   -- jlt.option,
   -- tcho.tracking_option_id,
   -- tco.name as customer_name,
   -- tc.name as tracking_category_name,
     tco.name as Directors

from {{ source("pearshop", "journal") }} j
left join {{ source("pearshop", "journal_line") }} jl on j.journal_id = jl.journal_id
left join
    {{ source("pearshop", "journal_line_has_tracking_category") }} jlt
    on jl.journal_line_id = jlt.journal_line_id
    and jl.journal_id = jlt.journal_id
left join
    {{ source("pearshop", "tracking_category_has_option") }} tcho
    on jlt.tracking_category_id = tcho.tracking_category_id
    and jlt.tracking_category_option_id = tcho.tracking_option_id
left join
    {{ source("pearshop", "tracking_category_option") }} tco
    on tcho.tracking_option_id = tco.tracking_option_id
left join
    {{ source("pearshop", "tracking_category") }} tc
    on tc.tracking_category_id = jlt.tracking_category_id
left join
    {{ source("pearshop", "account") }} a
    on jl.account_id = a.id
left join
    {{ source("impress_sp", "coa_metadata") }} psp
    on lower(psp.account_id) = lower(jl.account_id)
where tc.name='Directors'
),
pearshop as (
    select 
        COALESCE(rd.journal_line_id, rc.journal_line_id) AS journal_line_id,
    COALESCE(rd.account_id, rc.account_id) AS account_id,
    COALESCE(rd.account_code, rc.account_code) AS account_code,
    COALESCE(rd.account_type, rc.account_type) AS account_type,
    COALESCE(rd.account_name, rc.account_name) AS account_name,
    COALESCE(rd.net_amount_original,rc.net_amount_original) as net_amount_original,
    COALESCE(rd.net_amount, rc.net_amount) AS net_amount,
    COALESCE(rd.journal_id, rc.journal_id) AS journal_id,
    COALESCE(rd.journal_date, rc.journal_date) AS transaction_date,
    COALESCE(rd.account_category, rc.account_category) AS account_category,
    COALESCE(rd.account_report, rc.account_report) AS account_report,
    COALESCE(rd.account_class, rc.account_class) AS account_class,
    COALESCE(rd.working_apital_, rc.working_apital_) AS working_apital,
    'PEARSHOP' AS entity,

   -- COALESCE(pd.tracking_category_id, pc.tracking_category_id) AS tracking_category_id,
   -- COALESCE(pd.option, pc.option) AS option,
   -- COALESCE(pd.tracking_option_id, pc.tracking_option_id) AS tracking_option_id,
   -- COALESCE(pd.customer_name, pc.customer_name) AS customer_name,
   -- COALESCE(pd.tracking_category_name, pc.tracking_category_name) AS tracking_category_name,
    rd.Directors,
	rc.Client
     from pearshop_clients rc
    full outer join pearshop_division  rd on rc.journal_line_id=rd.journal_line_id 
)
select 
pl.*, 'ACTUAL' AS DATA_TYPE, dd.fiscal_year, dd.fiscal_quarter,dd.yearmonth from impress pl

left join {{ ref("dim_date") }} dd on pl.journal_date::date = dd.date

union all

select rl.*, 'ACTUAL' AS DATA_TYPE, dd.fiscal_year, dd.fiscal_quarter,dd.yearmonth from pearshop rl

left join {{ ref("dim_date") }} dd on rl.journal_date::date = dd.date

