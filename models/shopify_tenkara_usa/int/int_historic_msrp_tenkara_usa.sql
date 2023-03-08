{# 
    This is replicated from Alice Ames data model where the founders would run a sale, they will update the product prices instead of using the product discount code - which will make the order appear as if it never received a discount even though the product was inherently discounted.
    For data prior to the product_variant snapshot table, Caroline has given a logic to get historic MSRP. In simple terms, its the max trailing price of a product. A decline in the product price would mean that its price was updated by founders.
    We re replicating this logic for tenkara here. But this might not be needed. 
 #}
{{
  config(
    materialized = 'table'
    )
}}

with cte_max_daily_price as (

    select 
          ol.product_title
        , o.order_placed_at_utc::date as order_date
        , max(ol.order_line_item_price) as max_daily_price
    from {{ ref('stg_order_line_item_tenkara_usa') }} as ol 
    inner join {{ ref('stg_order_tenkara_usa') }} as o 
        on ol.order_id = o.order_id
    where order_line_item_vendor != 'route' --not business revenue
    group by 1,2

)

, historic_msrp as (
    select 
          product_title
        , order_date
        , max(max_daily_price) over 
        (partition by cmdp.product_title order by cmdp.order_date rows between unbounded preceding and current row) as msrp
    from cte_max_daily_price as cmdp
)

select * 
from historic_msrp