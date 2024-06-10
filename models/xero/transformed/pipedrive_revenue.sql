select dm.deal_id, 
    dm.month, 
    dm.mrr as net_amount, 
    case 
        when dm.pipeline_name = 'Pipeline' then 'Net New Revenue'
        when dm.pipeline_name = 'Renewal-Pipeline' then 'Recurring Revenue'
    end as revenue_type, 
    dm.stage_name, 
    dm.organization_name as customer_name, 
    dm.status,
    cms.region,
    cms.country
from {{ref('dim_deals_mrr')}} dm
left join {{source('xero_sp', 'customer_metadata_sheet_1')}} cms 
on lower(cms.CUSTOMER_XERO_) = lower(dm.organization_name)
where deal_id in (
    select distinct deal_id from {{ref('dim_deals')}}
    where expected_invoice_date > (select max(date)
from {{source('xero', 'invoice')}}
where type = 'ACCREC'
)
and status in ('won', 'open')
and stage_name in (
    'Negotiating',
    'Deal Won - Invoiced',
    'Cruise',
    'Signed',
    'Negotiation',
    'Onboarding Session'
)
)