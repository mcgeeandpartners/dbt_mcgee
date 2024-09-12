


select 
JOURNAL_LINE_ID,
 ACCOUNT_ID,
ACCOUNT_CODE,
ACCOUNT_TYPE,
PRODUCT,
REVENUE_TYPE,
ACCOUNT_NAME,
NET_AMOUNT_ORIGINAL, 
NET_AMOUNT,
JOURNAL_ID,
JOURNAL_DATE, 
ACCOUNT_CATEGORY,
ACCOUNT_REPORT,
ACCOUNT_CLASS, 
ENTITY, 
CLIENT, 
PAYROLL_REGION,
FISCAL_YEAR, 
FISCAL_QUARTER,
YEARMONTH,
sah.PAYROLL_TAX_REGION,
sah.GLOBAL_REGION,
sah.DEPARTMENT
 from (



with  

xero_combined as
   ( select
        jl.journal_line_id, 
        jl.account_id, 
        psp.account_code, --jl
        psp.account_type, --jl
        case when   psp.account_type='REVENUE'
        then split( psp.account_name,'-')[0]::string
        else null end as PRODUCT,
        case when   psp.account_type='REVENUE'
        then split( psp.account_name,'-')[1]::string
        else null end as REVENUE_TYPE,
        psp.account_name, --jl
        jl.net_amount as net_amount_original,  
        CASE 
            WHEN jl.account_type in ('REVENUE','OTHERINCOME') THEN jl.net_amount * -1  
            ELSE jl.net_amount * 1
        END AS net_amount,
        j.journal_id, 
        j.journal_date, 
        psp.account_category, 
        psp.account_report, 
        a.class as account_class,
      --  psp.working_capital as working_capital,
        'swoop' as entity,
        CASE 
            WHEN tc.name = 'Client' THEN tco.name 
            ELSE NULL 
        END AS Client
         , 
        CASE 
            WHEN tc.name = 'Payroll Regions' THEN tco.name 
            ELSE NULL 
        END AS Payroll_Region

        
    from swoop_database.swoop_xero.journal j
    left join swoop_database.swoop_xero.journal_line jl on j.journal_id = jl.journal_id
    left join swoop_database.swoop_xero.journal_line_has_tracking_category jlt
        on jl.journal_line_id = jlt.journal_line_id
        and jl.journal_id = jlt.journal_id
    left join swoop_database.swoop_xero.account a on jl.account_id=a.account_id

    left join swoop_database.swoop_xero.tracking_category_has_option tcho
        on jlt.tracking_category_id = tcho.tracking_category_id
        and jlt.tracking_category_option_id = tcho.tracking_option_id
    left join swoop_database.swoop_xero.tracking_category_option tco
        on tcho.tracking_option_id = tco.tracking_option_id
    left join swoop_database.swoop_xero.tracking_category tc
        on tc.tracking_category_id = jlt.tracking_category_id
     and tc.STATUS='ACTIVE'
    left join swoop_database.swoop_sharepoint.COA_METADATA_COA_METADATA psp
 
   
        on lower(psp.account_id) = lower(jl.account_id)
    where tc.name IN (
           
        'Client'
         
           , 
        
        
        'Payroll Regions'
         
          
        
        
        )
    or tc.name is NULL 
)






select xero_combined.*, dd.fiscal_year, dd.fiscal_quarter,dd.yearmonth from xero_combined

left join SWOOP_QUERIES.transformed.dim_date dd on xero_combined.journal_date::date = dd.date




) as xf left join
 SWOOP_DATABASE.swoop_sharepoint.swoop_account_category_hardcoded as sah 
on sah.NAME=xf.Payroll_Region
--where  Payroll_Region is not null