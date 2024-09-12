
--select tem.* from (
select
    dm.deal_id,
    dm.month,
    dm.mrr as net_amount,
    case
        when dm.pipeline_name = 'Pipeline'
        then 'Net New Revenue'
        when dm.pipeline_name = 'Renewal-Pipeline'
        then 'Recurring Revenue'
    end as revenue_type,
    dm.stage_name,
    dm.organization_name as customer_name,
    dm.status,
    dd.region,
    dd.country,
    cms.CUSTOMER_REPORT_NAME
from {{ ref("dim_deals_mrr") }} dm
left join
    {{ source("xero_sp", "customer_metadata_sheet_1") }} cms
    on lower(cms.CUSTOMER_PIPEDRIVE_) = lower(dm.organization_name)
left join 
 ( select id,organization_name,region,country from {{ ref("dim_deals") }} ) dd
   on lower(dd.id) = lower(dm.deal_id)

where
    deal_id in (
        select distinct deal_id
        from {{ ref("dim_deals") }}
        where
            expected_invoice_date > (
                select max(date)
                from {{ source("xero", "invoice") }}
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
--) as tem 
-- left join SWOOP_DATABASE.SWOOP_SHAREPOINT.CUSTOMER_METADATA_SHEET_1 as cms1
-- on tem.customer_name=cms1.CUSTOMER_PIPEDRIVE_
