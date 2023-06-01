select
      order_id
    , max(case when ot.key = 'utm_campaign' then ot.value end) as utm_campaign
    , max(case when ot.key = 'utm_medium' then ot.value end) as utm_medium
    , max(case when ot.key = 'utm_source' then ot.value end) as utm_source
from {{source('shopify_little_poppy_co', 'order_url_tag')}} as ot
group by 1