with base as (
    select 
        (order_gross_revenue_total - order_discount - order_refund) as order_net_revenue_total_prev,
        order_net_revenue_total
    from {{ ref('fact_order_line_item_alice_ames') }}
)

select round(order_net_revenue_total_prev - order_net_revenue_total) as diff
from base 
where diff != 0