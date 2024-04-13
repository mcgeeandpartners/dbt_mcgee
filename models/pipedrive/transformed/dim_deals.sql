select
    dh.*,
    dh.id as deal_id,
    p.name as pipeline_name,
    s.name as stage_name,
    per.name as person_name,
    o.name as organization_name,
    case when dh.currency != 'AUD' then value / er.rate else value end as value_aud
from {{ ref("stg_deal_history") }} dh
left join {{ ref("stg_pipeline") }} p on dh.pipeline_id = p.id
left join {{ ref("stg_stage") }} s on dh.stage_id = s.id
left join {{ ref("stg_person") }} per on dh.person_id = per.id
left join {{ ref("stg_organization") }} o on dh.org_id = o.id
join transformed_wh.transformed.exchange_rates er on er.currency = dh.currency
