{{ config(alias="rpt_product") }}

with base_max_revenue as (
    select
          oli.product_key
        , dates.date as order_date
        , sum(oli.order_line_item_gross_revenue) as daily_gross_revenue
        , sum(oli.order_line_item_gross_revenue - oli.order_line_item_total_discount) as daily_net_revenue
    from {{ ref('fact_order_line_item_lpc') }} as oli
    left join {{ ref('dim_date_lpc') }} as dates
        on oli.date_key = dates.date_key
    group by 1, 2
),

max_revenue_product as (
    select 
          product_key
        , round(max(daily_gross_revenue),2) as max_daily_gross_revenue_lifetime
        , round(max(daily_net_revenue),2) as max_daily_net_revenue_lifetime
    from base_max_revenue
    group by 1  
),

orders_history_liftime as (
    select
          oli.product_key
        , rp.max_daily_gross_revenue_lifetime 
        , rp.max_daily_net_revenue_lifetime
        , min(dates.date) as first_order_date
        , max(dates.date) as last_order_date
        , count(distinct dates.date) as days_on_sale_lifetime
        , count(distinct oli.order_id) as unique_orders_lifetime
        , sum(case when oli.is_new_customer_order = true then 1 else 0 end) as new_customer_orders_lifetime
        , round(avg(oli.order_line_item_msrp),2) as avg_msrp_lifetime
        , sum(oli.order_line_item_units) as units_sold_lifetime
        , round(sum(oli.order_line_item_gross_revenue),2) as gross_revenue_lifetime
        , round(avg(oli.order_line_item_total_discount/nullif(oli.order_line_item_gross_revenue,0)),2) as discount_avg_lifetime
        , round(sum(oli.order_line_item_gross_revenue - oli.order_line_item_total_discount),2) as net_revenue_lifetime

    from {{ ref('fact_order_line_item_lpc') }} as oli
    left join {{ ref('dim_date_lpc') }} as dates
        on oli.date_key = dates.date_key
    left join max_revenue_product as rp
        on oli.product_key = rp.product_key
    group by 1, 2, 3
)

select
    p.product_key
  , p.product_variant_id
  , p.product_id
  , p.product_title
  , p.product_variant_title
  , p.product_sku
  , p.product_barcode
  , p.product_price as product_current_price
  , p.compare_at_price as product_current_compare_at_price
  /*Eventually need to fill these in with SKU mapping work*/
  , p.product_category
  , p.product_type
  , p.product_sub_type
  , p.product_made_in
  , p.product_manufacturer 

  , p.inventory_item_id as product_inventory_id
  , p.inventory_cost as product_current_cogs
  , p.inventory_available_quantity as product_current_available_inventory
  , ol.max_daily_gross_revenue_lifetime 
  , ol.max_daily_net_revenue_lifetime
  , ol.first_order_date
  , ol.last_order_date
  , ol.days_on_sale_lifetime
  , ol.unique_orders_lifetime
  , ol.new_customer_orders_lifetime
  , ol.avg_msrp_lifetime
  , ol.units_sold_lifetime
  , ol.gross_revenue_lifetime
  , ol.discount_avg_lifetime
  , ol.net_revenue_lifetime

from {{ ref('dim_product_lpc') }} as p
left join orders_history_liftime as ol
  on p.product_key = ol.product_key