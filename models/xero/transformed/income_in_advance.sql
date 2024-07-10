select customer_name, sum(net_amount) as total
from {{ ref("xero_base") }}
where account_id = '8704505b-38c0-4781-a74d-134be48c78ef'
group by 1
