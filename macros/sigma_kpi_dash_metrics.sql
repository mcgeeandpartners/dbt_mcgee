
{% macro insert_kpi_metrics() -%}

  /*Customer metrics*/
  , count(distinct o.customer_id) as unique_customers
  , count(distinct case when o.is_new_customer_order then o.customer_id end) as unique_new_customers
  , count(distinct case when not o.is_new_customer_order then o.customer_id end) as unique_recur_customers
  , median(o.customer_months_since_acq) as median_age_customer_months_since_acq
  , avg(o.customer_months_since_acq) as avg_age_customer_months_since_acq
  /*Orders metrics*/
  , count(o.order_id) as orders
  , sum(case when o.is_new_customer_order then 1 end) as orders_new_customers
  , sum(case when not o.is_new_customer_order then 1 end) as orders_recur_customers
  , sum(o.units_product) as units_product
  , sum(o.units_route) as units_route
  /*Financial metrics*/
  , sum(o.gross_revenue_product) as gross_revenue_product
  , sum(o.gross_revenue_product - o.discount_total) as net_revenue_product
  , sum(case when o.is_new_customer_order then o.gross_revenue_product end) as gross_revenue_product_new_customers
  , sum(case when not o.is_new_customer_order then o.gross_revenue_product end) as gross_revenue_product_recur_customers 
  , sum(case when o.is_new_customer_order then o.gross_revenue_product - o.discount_total end) as net_revenue_product_new_customers
  , sum(case when not o.is_new_customer_order then o.gross_revenue_product - o.discount_total end) as net_revenue_product_recur_customers
  , sum(o.gross_revenue_route) as gross_revenue_route
  , sum(o.gross_revenue_shipping) as gross_revenue_shipping
  , sum(o.gross_revenue_tax) as gross_revenue_tax
  , sum(o.gross_revenue_total) as gross_revenue_total
  , avg(o.gross_revenue_total) as AOV_gross_total
  , sum(case when o.is_new_customer_order then o.gross_revenue_total end) as gross_revenue_total_new_customers
  , sum(case when not o.is_new_customer_order then o.gross_revenue_total end) as gross_revenue_total_recur_customers
  , avg(case when o.is_new_customer_order then o.gross_revenue_total end) as AOV_gross_total_new_customers
  , avg(case when not o.is_new_customer_order then o.gross_revenue_total end) as AOV_gross_total_recur_customers  
  , sum(o.discount_total) as discounts_total
  , sum(o.discount_total)/sum(o.gross_revenue_total) as discounts_percent
  , sum(case when o.is_new_customer_order then o.discount_total end) as discounts_total_new_customers
  , sum(case when o.is_new_customer_order then o.discount_total end)
        /sum(case when o.is_new_customer_order then o.gross_revenue_total end) as discounts_percent_new_customers
  , sum(case when not o.is_new_customer_order then o.discount_total end) as discounts_total_recur_customers
  , sum(case when not o.is_new_customer_order then o.discount_total end)
        /sum(case when not o.is_new_customer_order then o.gross_revenue_total end) as discounts_percent_recur_customers
  , sum(o.refunds_total) as refunds_total
  , sum(o.net_revenue_total) as net_revenue_total
  , sum(case when o.is_new_customer_order then o.net_revenue_total end) as net_revenue_total_new_customers
  , sum(case when not o.is_new_customer_order then o.net_revenue_total end) as net_revenue_total_recur_customers 
  , avg(o.net_revenue_total) as AOV_net_total
  , avg(case when o.is_new_customer_order then o.net_revenue_total end) as AOV_net_total_new_customers
  , avg(case when not o.is_new_customer_order then o.net_revenue_total end) as AOV_net_total_recur_customers

{%- endmacro %}

{% macro insert_kpi_comparison_metrics() -%}

    , ty.unique_customers
    , ly.unique_customers as ly_unique_customers
    , (ty.unique_customers/nullif(ly.unique_customers,0))-1 as yoy_unique_customers
    , ty.unique_new_customers
    , ly.unique_new_customers as ly_unique_new_customers
    , (ty.unique_new_customers/nullif(ly.unique_new_customers,0))-1 as yoy_unique_new_customers
    , ty.unique_recur_customers
    , ly.unique_recur_customers as ly_unique_recur_customers
    , (ty.unique_recur_customers/nullif(ly.unique_recur_customers,0))-1 as yoy_unique_recur_customers    
    , ty.median_age_customer_months_since_acq
    , ly.median_age_customer_months_since_acq as ly_median_age_customer_months_since_acq
    , (ty.median_age_customer_months_since_acq/nullif(ly.median_age_customer_months_since_acq,0))-1 as yoy_median_age_customer_months_since_acq
    , ty.avg_age_customer_months_since_acq
    , ly.avg_age_customer_months_since_acq as ly_avg_age_customer_months_since_acq
    , (ty.avg_age_customer_months_since_acq/nullif(ly.avg_age_customer_months_since_acq,0))-1 as yoy_avg_age_customer_months_since_acq
    /*orders metrics*/
    , ty.orders
    , ly.orders as ly_orders
    , (ty.orders/nullif(ly.orders,0))-1 as yoy_orders
    , ty.orders_new_customers
    , ly.orders_new_customers as ly_orders_new_customers
    , (ty.orders_new_customers/nullif(ly.orders_new_customers,0))-1 as yoy_orders_new_customers
    , ty.orders_recur_customers
    , ly.orders_recur_customers as ly_orders_recur_customers
    , (ty.orders_recur_customers/nullif(ly.orders_recur_customers,0))-1 as yoy_orders_recur_customers
    , ty.units_product
    , ly.units_product as ly_units_product
    , (ty.units_product/nullif(ly.units_product,0))-1 as yoy_units_product
    , ty.units_route
    , ly.units_route as ly_units_route
    , (ty.units_route/nullif(ly.units_route,0))-1 as yoy_units_route
    /*financial metrics*/
    , ty.gross_revenue_product
    , ly.gross_revenue_product as ly_gross_revenue_product
    , (ty.gross_revenue_product/nullif(ly.gross_revenue_product,0))-1 as yoy_gross_revenue_product
    , ty.net_revenue_product
    , ly.net_revenue_product as ly_net_revenue_product
    , (ty.net_revenue_product/nullif(ly.net_revenue_product,0))-1 as yoy_net_revenue_product 
    , ty.gross_revenue_product_new_customers
    , ly.gross_revenue_product_new_customers as ly_gross_revenue_product_new_customers
    , (ty.gross_revenue_product_new_customers/nullif(ly.gross_revenue_product_new_customers,0))-1 as yoy_gross_revenue_product_new_customers
    , ty.gross_revenue_product_recur_customers
    , ly.gross_revenue_product_recur_customers as ly_gross_revenue_product_recur_customers
    , (ty.gross_revenue_product_recur_customers/nullif(ly.gross_revenue_product_recur_customers,0))-1 as yoy_gross_revenue_product_recur_customers
    , ty.net_revenue_product_new_customers
    , ly.net_revenue_product_new_customers as ly_net_revenue_product_new_customers
    , (ty.net_revenue_product_new_customers/nullif(ly.net_revenue_product_new_customers,0))-1 as yoy_net_revenue_product_new_customers
   , ty.net_revenue_product_recur_customers
    , ly.net_revenue_product_recur_customers as ly_net_revenue_product_recur_customers
    , (ty.net_revenue_product_recur_customers/nullif(ly.net_revenue_product_recur_customers,0))-1 as yoy_net_revenue_product_recur_customers
    , ty.gross_revenue_route
    , ly.gross_revenue_route as ly_gross_revenue_route
    , (ty.gross_revenue_route/nullif(ly.gross_revenue_route,0))-1 as yoy_gross_revenue_route
    , ty.gross_revenue_shipping
    , ly.gross_revenue_shipping as ly_gross_revenue_shipping
    , (ty.gross_revenue_shipping/nullif(ly.gross_revenue_shipping,0))-1 as yoy_gross_revenue_shipping
    , ty.gross_revenue_tax
    , ly.gross_revenue_tax as ly_gross_revenue_tax
    , (ty.gross_revenue_tax/nullif(ly.gross_revenue_tax,0))-1 as yoy_gross_revenue_tax
    , ty.gross_revenue_total
    , ly.gross_revenue_total as ly_gross_revenue_total
    , (ty.gross_revenue_total/nullif(ly.gross_revenue_total,0))-1 as yoy_gross_revenue_total
    , ty.aov_gross_total
    , ly.aov_gross_total as ly_aov_gross_total
    , (ty.aov_gross_total/nullif(ly.aov_gross_total,0))-1 as yoy_aov_gross_total
    , ty.gross_revenue_total_new_customers
    , ly.gross_revenue_total_new_customers as ly_gross_revenue_total_new_customers
    , (ty.gross_revenue_total_new_customers/nullif(ly.gross_revenue_total_new_customers,0))-1 as yoy_gross_revenue_total_new_customers
    , ty.gross_revenue_total_recur_customers
    , ly.gross_revenue_total_recur_customers as ly_gross_revenue_total_recur_customers
    , (ty.gross_revenue_total_recur_customers/nullif(ly.gross_revenue_total_recur_customers,0))-1 as yoy_gross_revenue_total_recur_customers
    , ty.aov_gross_total_new_customers
    , ly.aov_gross_total_new_customers as ly_aov_gross_total_new_customers
    , (ty.aov_gross_total_new_customers/nullif(ly.aov_gross_total_new_customers,0))-1 as yoy_aov_gross_total_new_customers
    , ty.aov_gross_total_recur_customers
    , ly.aov_gross_total_recur_customers as ly_aov_gross_total_recur_customers
    , (ty.aov_gross_total_recur_customers/nullif(ly.aov_gross_total_recur_customers,0))-1 as yoy_aov_gross_total_recur_customers
    , ty.discounts_total
    , ly.discounts_total as ly_discounts_total
    , (ty.discounts_total/nullif(ly.discounts_total,0))-1 as yoy_discounts_total
    , ty.discounts_percent
    , ly.discounts_percent as ly_discounts_percent
    , (ty.discounts_percent/nullif(ly.discounts_percent,0))-1 as yoy_discounts_percent
    , ty.discounts_total_new_customers
    , ly.discounts_total_new_customers as ly_discounts_total_new_customers
    , (ty.discounts_total_new_customers/nullif(ly.discounts_total_new_customers,0))-1 as yoy_discounts_total_new_customers
    , ty.discounts_percent_new_customers
    , ly.discounts_percent_new_customers as ly_discounts_percent_new_customers
    , (ty.discounts_percent_new_customers/nullif(ly.discounts_percent_new_customers,0))-1 as yoy_discounts_percent_new_customers
    , ty.discounts_total_recur_customers
    , ly.discounts_total_recur_customers as ly_discounts_total_recur_customers
    , (ty.discounts_total_recur_customers/nullif(ly.discounts_total_recur_customers,0))-1 as yoy_discounts_total_recur_customers
    , ty.discounts_percent_recur_customers
    , ly.discounts_percent_recur_customers as ly_discounts_percent_recur_customers
    , (ty.discounts_percent_recur_customers/nullif(ly.discounts_percent_recur_customers,0))-1 as yoy_discounts_percent_recur_customers
    , ty.refunds_total
    , ly.refunds_total as ly_refunds_total
    , (ty.refunds_total/nullif(ly.refunds_total,0))-1 as yoy_refunds_total
    , ty.net_revenue_total
    , ly.net_revenue_total as ly_net_revenue_total
    , (ty.net_revenue_total/nullif(ly.net_revenue_total,0))-1 as yoy_net_revenue_total
    , ty.net_revenue_total_new_customers
    , ly.net_revenue_total_new_customers as ly_net_revenue_total_new_customers
    , (ty.net_revenue_total_new_customers/nullif(ly.net_revenue_total_new_customers,0))-1 as yoy_net_revenue_total_new_customers
    , ty.net_revenue_total_recur_customers
    , ly.net_revenue_total_recur_customers as ly_net_revenue_total_recur_customers
    , (ty.net_revenue_total_recur_customers/nullif(ly.net_revenue_total_recur_customers,0))-1 as yoy_net_revenue_total_recur_customers
    , ty.aov_net_total
    , ly.aov_net_total as ly_aov_net_total
    , (ty.aov_net_total/nullif(ly.aov_net_total,0))-1 as yoy_aov_net_total
    , ty.aov_net_total_new_customers
    , ly.aov_net_total_new_customers as ly_aov_net_total_new_customers
    , (ty.aov_net_total_new_customers/nullif(ly.aov_net_total_new_customers,0))-1 as yoy_aov_net_total_new_customers
    , ty.aov_net_total_recur_customers
    , ly.aov_net_total_recur_customers as ly_aov_net_total_recur_customers
    , (ty.aov_net_total_recur_customers/nullif(ly.aov_net_total_recur_customers,0))-1 as yoy_aov_net_total_recur_customers


{%- endmacro %}

{% macro insert_marketing_kpi_metrics() -%}

  /*Marketing metrics*/
  , sum(m.impressions_fb) as impressions_fb
  , sum(m.clicks_fb) as clicks_fb
  , sum(m.clicks_fb) / sum(nullif(m.impressions_fb,0)) as ctr_fb
  , sum(m.spend_fb) as spend_fb
  , sum(m.reach_fb) as reach_fb
  , sum(m.impressions_goog) as impressions_goog
  , sum(m.clicks_goog) as clicks_goog
  , sum(m.clicks_goog) / sum(nullif(m.impressions_goog,0)) as ctr_goog
  , sum(m.spend_goog) as spend_goog
  , sum(m.reach_goog) as reach_goog
  , sum(ifnull(m.impressions_fb,0) + ifnull(m.impressions_goog,0)) as impressions_total
  , sum(ifnull(m.clicks_fb,0) + ifnull(m.clicks_goog,0)) as clicks_total
  , sum(ifnull(m.spend_fb,0) + ifnull(m.spend_goog,0)) as spend_total
  , sum(ifnull(m.reach_fb,0) + ifnull(m.reach_goog,0)) as reach_total

{%- endmacro %}

{% macro insert_marketing_kpi_comparison_metrics() -%}


    , ty.impressions_fb
    , ly.impressions_fb as ly_impressions_fb
    , (ty.impressions_fb/nullif(ly.impressions_fb,0))-1 as yoy_impressions_fb
    , ty.clicks_fb
    , ly.clicks_fb as ly_clicks_fb
    , (ty.clicks_fb/nullif(ly.clicks_fb,0))-1 as yoy_clicks_fb
    , ty.ctr_fb
    , ly.ctr_fb as ly_ctr_fb
    , (ty.ctr_fb/nullif(ly.ctr_fb,0))-1 as yoy_ctr_fb
    , ty.spend_fb
    , ly.spend_fb as ly_spend_fb
    , (ty.spend_fb/nullif(ly.spend_fb,0))-1 as yoy_spend_fb
    , ty.reach_fb
    , ly.reach_fb as ly_reach_fb
    , (ty.reach_fb/nullif(ly.reach_fb,0))-1 as yoy_reach_fb
    , ty.impressions_goog
    , ly.impressions_goog as ly_impressions_goog
    , (ty.impressions_goog/nullif(ly.impressions_goog,0))-1 as yoy_impressions_goog
    , ty.clicks_goog
    , ly.clicks_goog as ly_clicks_goog
    , (ty.clicks_goog/nullif(ly.clicks_goog,0))-1 as yoy_clicks_goog
    , ty.ctr_goog
    , ly.ctr_goog as ly_ctr_goog
    , (ty.ctr_goog/nullif(ly.ctr_goog,0))-1 as yoy_ctr_goog
    , ty.spend_goog
    , ly.spend_goog as ly_spend_goog
    , (ty.spend_goog/nullif(ly.spend_goog,0))-1 as yoy_spend_goog
    , ty.reach_goog
    , ly.reach_goog as ly_reach_goog
    , (ty.reach_goog/nullif(ly.reach_goog,0))-1 as yoy_reach_goog
    , ty.impressions_total
    , ly.impressions_total as ly_impressions_total
    , (ty.impressions_total/nullif(ly.impressions_total,0))-1 as yoy_impressions_total
    , ty.clicks_total
    , ly.clicks_total as ly_clicks_total
    , (ty.clicks_total/nullif(ly.clicks_total,0))-1 as yoy_clicks_total
    , ty.spend_total
    , ly.spend_total as ly_spend_total
    , (ty.spend_total/nullif(ly.spend_total,0))-1 as yoy_spend_total
    , ty.reach_total
    , ly.reach_total as ly_reach_total
    , (ty.reach_total/nullif(ly.reach_total,0))-1 as yoy_reach_total

{%- endmacro %}

{% macro insert_order_line_agg_metrics() -%}

  /*Customer metrics*/
  , count(distinct o.customer_id) as unique_customers
  , count(distinct case when o.is_new_customer_order then o.customer_id end) as unique_new_customers
  , count(distinct case when not o.is_new_customer_order then o.customer_id end) as unique_recur_customers
  , median(o.customer_months_since_acq) as median_age_customer_months_since_acq
  , avg(o.customer_months_since_acq) as avg_age_customer_months_since_acq
  /*Order and order line item metrics*/
  , count(o.order_id) as orders
  , sum(case when o.is_new_customer_order then 1 end) as orders_new_customers
  , sum(case when not o.is_new_customer_order then 1 end) as orders_recur_customers
  , sum(o.units) as units_sold
  /*Financial metrics*/
  , max(o.MSRP) as product_msrp
  , sum(o.gross_revenue) as gross_revenue_product
  , sum(case when o.is_new_customer_order then o.gross_revenue end) as gross_revenue_product_new_customers
  , sum(case when not o.is_new_customer_order then o.gross_revenue end) as gross_revenue_product_recur_customers 
  , sum(o.gross_revenue - o.discount_total) as net_revenue_product
  , sum(case when o.is_new_customer_order then o.gross_revenue - o.discount_total end) as net_revenue_product_new_customers
  , sum(case when not o.is_new_customer_order then o.gross_revenue - o.discount_total end) as net_revenue_product_recur_customers
  , sum(o.discount_total) as discounts_total
  , sum(case when o.is_new_customer_order then o.discount_total end) as discounts_total_new_customers
  , sum(case when not o.is_new_customer_order then o.discount_total end) as discounts_total_recur_customers

{%- endmacro %}

{% macro insert_order_line_agg_comparison_metrics() -%}

    , ty.unique_customers
    , ly.unique_customers as ly_unique_customers
    , (ty.unique_customers/nullif(ly.unique_customers,0))-1 as yoy_unique_customers
    , ty.unique_new_customers
    , ly.unique_new_customers as ly_unique_new_customers
    , (ty.unique_new_customers/nullif(ly.unique_new_customers,0))-1 as yoy_unique_new_customers
    , ty.unique_recur_customers
    , ly.unique_recur_customers as ly_unique_recur_customers
    , (ty.unique_recur_customers/nullif(ly.unique_recur_customers,0))-1 as yoy_unique_recur_customers    
    , ty.median_age_customer_months_since_acq
    , ly.median_age_customer_months_since_acq as ly_median_age_customer_months_since_acq
    , (ty.median_age_customer_months_since_acq/nullif(ly.median_age_customer_months_since_acq,0))-1 as yoy_median_age_customer_months_since_acq
    , ty.avg_age_customer_months_since_acq
    , ly.avg_age_customer_months_since_acq as ly_avg_age_customer_months_since_acq
    , (ty.avg_age_customer_months_since_acq/nullif(ly.avg_age_customer_months_since_acq,0))-1 as yoy_avg_age_customer_months_since_acq
    /*orders metrics*/
    , ty.orders
    , ly.orders as ly_orders
    , (ty.orders/nullif(ly.orders,0))-1 as yoy_orders
    , ty.orders_new_customers
    , ly.orders_new_customers as ly_orders_new_customers
    , (ty.orders_new_customers/nullif(ly.orders_new_customers,0))-1 as yoy_orders_new_customers
    , ty.orders_recur_customers
    , ly.orders_recur_customers as ly_orders_recur_customers
    , (ty.orders_recur_customers/nullif(ly.orders_recur_customers,0))-1 as yoy_orders_recur_customers
    , ty.units_sold
    , ly.units_sold as ly_units_sold
    , (ty.units_sold/nullif(ly.units_sold,0))-1 as yoy_units_sold
    /*financial metrics*/
    , ty.gross_revenue_product
    , ly.gross_revenue_product as ly_gross_revenue_product
    , (ty.gross_revenue_product/nullif(ly.gross_revenue_product,0))-1 as yoy_gross_revenue_product
    , ty.gross_revenue_product_new_customers
    , ly.gross_revenue_product_new_customers as ly_gross_revenue_product_new_customers
    , (ty.gross_revenue_product_new_customers/nullif(ly.gross_revenue_product_new_customers,0))-1 as yoy_gross_revenue_product_new_customers
    , ty.gross_revenue_product_recur_customers
    , ly.gross_revenue_product_recur_customers as ly_gross_revenue_product_recur_customers
    , (ty.gross_revenue_product_recur_customers/nullif(ly.gross_revenue_product_recur_customers,0))-1 as yoy_gross_revenue_product_recur_customers
    , ty.net_revenue_product
    , ly.net_revenue_product as ly_net_revenue_product
    , (ty.net_revenue_product/nullif(ly.net_revenue_product,0))-1 as yoy_net_revenue_product 
    , ty.net_revenue_product_new_customers
    , ly.net_revenue_product_new_customers as ly_net_revenue_product_new_customers
    , (ty.net_revenue_product_new_customers/nullif(ly.net_revenue_product_new_customers,0))-1 as yoy_net_revenue_product_new_customers
    , ty.net_revenue_product_recur_customers
    , ly.net_revenue_product_recur_customers as ly_net_revenue_product_recur_customers
    , (ty.net_revenue_product_recur_customers/nullif(ly.net_revenue_product_recur_customers,0))-1 as yoy_net_revenue_product_recur_customers
    , ty.discounts_total
    , ly.discounts_total as ly_discounts_total
    , (ty.discounts_total/nullif(ly.discounts_total,0))-1 as yoy_discounts_total
    , ty.discounts_total_new_customers
    , ly.discounts_total_new_customers as ly_discounts_total_new_customers
    , (ty.discounts_total_new_customers/nullif(ly.discounts_total_new_customers,0))-1 as yoy_discounts_total_new_customers
    , ty.discounts_total_recur_customers
    , ly.discounts_total_recur_customers as ly_discounts_total_recur_customers
    , (ty.discounts_total_recur_customers/nullif(ly.discounts_total_recur_customers,0))-1 as yoy_discounts_total_recur_customers

{%- endmacro %}

/*NEW*/
{% macro insert_kpi_product_metrics() -%}

  /*Customer metrics*/
  , count(distinct o.customer_id) as unique_customers
  , count(distinct case when o.is_new_customer_order then o.customer_id end) as unique_new_customers
  , count(distinct case when not o.is_new_customer_order then o.customer_id end) as unique_recur_customers
  , median(o.customer_months_since_acq) as median_age_customer_months_since_acq
  , avg(o.customer_months_since_acq) as avg_age_customer_months_since_acq
  /*Orders metrics*/
  , count(o.order_id) as orders
  , sum(case when o.is_new_customer_order then 1 end) as orders_new_customers
  , sum(case when not o.is_new_customer_order then 1 end) as orders_recur_customers
  , sum(o.units) as units
  /*Financial metrics*/
  , sum(o.gross_revenue) as gross_revenue_product
  , sum(o.gross_revenue - o.discount_total) as net_revenue_product
  , sum(case when o.is_new_customer_order then o.gross_revenue end) as gross_revenue_product_new_customers
  , sum(case when not o.is_new_customer_order then o.gross_revenue end) as gross_revenue_product_recur_customers 
  , sum(case when o.is_new_customer_order then o.gross_revenue - o.discount_total end) as net_revenue_product_new_customers
  , sum(case when not o.is_new_customer_order then o.gross_revenue - o.discount_total end) as net_revenue_product_recur_customers
  , sum(o.discount_total) as discounts_total
  , sum(o.discount_total)/nullif(sum(o.msrp),0) as discounts_percent
  , sum(case when o.is_new_customer_order then o.discount_total end) as discounts_total_new_customers
  , sum(case when o.is_new_customer_order then o.discount_total end)
        /nullif(sum(case when o.is_new_customer_order then o.msrp end),0) as discounts_percent_new_customers
  , sum(case when not o.is_new_customer_order then o.discount_total end) as discounts_total_recur_customers
  , sum(case when not o.is_new_customer_order then o.discount_total end)
        /nullif(sum(case when not o.is_new_customer_order then o.msrp end),0) as discounts_percent_recur_customers

{%- endmacro %}

{% macro insert_kpi_product_comparison_metrics() -%}

    , ty.unique_customers
    , ly.unique_customers as ly_unique_customers
    , (ty.unique_customers/nullif(ly.unique_customers,0))-1 as yoy_unique_customers
    , ty.unique_new_customers
    , ly.unique_new_customers as ly_unique_new_customers
    , (ty.unique_new_customers/nullif(ly.unique_new_customers,0))-1 as yoy_unique_new_customers
    , ty.unique_recur_customers
    , ly.unique_recur_customers as ly_unique_recur_customers
    , (ty.unique_recur_customers/nullif(ly.unique_recur_customers,0))-1 as yoy_unique_recur_customers    
    , ty.median_age_customer_months_since_acq
    , ly.median_age_customer_months_since_acq as ly_median_age_customer_months_since_acq
    , (ty.median_age_customer_months_since_acq/nullif(ly.median_age_customer_months_since_acq,0))-1 as yoy_median_age_customer_months_since_acq
    , ty.avg_age_customer_months_since_acq
    , ly.avg_age_customer_months_since_acq as ly_avg_age_customer_months_since_acq
    , (ty.avg_age_customer_months_since_acq/nullif(ly.avg_age_customer_months_since_acq,0))-1 as yoy_avg_age_customer_months_since_acq
    /*orders metrics*/
    , ty.orders
    , ly.orders as ly_orders
    , (ty.orders/nullif(ly.orders,0))-1 as yoy_orders
    , ty.orders_new_customers
    , ly.orders_new_customers as ly_orders_new_customers
    , (ty.orders_new_customers/nullif(ly.orders_new_customers,0))-1 as yoy_orders_new_customers
    , ty.orders_recur_customers
    , ly.orders_recur_customers as ly_orders_recur_customers
    , (ty.orders_recur_customers/nullif(ly.orders_recur_customers,0))-1 as yoy_orders_recur_customers
    , ty.units
    , ly.units as ly_units
    , (ty.units/nullif(ly.units,0))-1 as yoy_units
    /*financial metrics*/
    , ty.gross_revenue_product
    , ly.gross_revenue_product as ly_gross_revenue_product
    , (ty.gross_revenue_product/nullif(ly.gross_revenue_product,0))-1 as yoy_gross_revenue_product
    , ty.net_revenue_product
    , ly.net_revenue_product as ly_net_revenue_product
    , (ty.net_revenue_product/nullif(ly.net_revenue_product,0))-1 as yoy_net_revenue_product 
    , ty.gross_revenue_product_new_customers
    , ly.gross_revenue_product_new_customers as ly_gross_revenue_product_new_customers
    , (ty.gross_revenue_product_new_customers/nullif(ly.gross_revenue_product_new_customers,0))-1 as yoy_gross_revenue_product_new_customers
    , ty.gross_revenue_product_recur_customers
    , ly.gross_revenue_product_recur_customers as ly_gross_revenue_product_recur_customers
    , (ty.gross_revenue_product_recur_customers/nullif(ly.gross_revenue_product_recur_customers,0))-1 as yoy_gross_revenue_product_recur_customers
    , ty.net_revenue_product_new_customers
    , ly.net_revenue_product_new_customers as ly_net_revenue_product_new_customers
    , (ty.net_revenue_product_new_customers/nullif(ly.net_revenue_product_new_customers,0))-1 as yoy_net_revenue_product_new_customers
   , ty.net_revenue_product_recur_customers
    , ly.net_revenue_product_recur_customers as ly_net_revenue_product_recur_customers
    , (ty.net_revenue_product_recur_customers/nullif(ly.net_revenue_product_recur_customers,0))-1 as yoy_net_revenue_product_recur_customers
    , ty.discounts_total
    , ly.discounts_total as ly_discounts_total
    , (ty.discounts_total/nullif(ly.discounts_total,0))-1 as yoy_discounts_total
    , ty.discounts_percent
    , ly.discounts_percent as ly_discounts_percent
    , (ty.discounts_percent/nullif(ly.discounts_percent,0))-1 as yoy_discounts_percent
    , ty.discounts_total_new_customers
    , ly.discounts_total_new_customers as ly_discounts_total_new_customers
    , (ty.discounts_total_new_customers/nullif(ly.discounts_total_new_customers,0))-1 as yoy_discounts_total_new_customers
    , ty.discounts_percent_new_customers
    , ly.discounts_percent_new_customers as ly_discounts_percent_new_customers
    , (ty.discounts_percent_new_customers/nullif(ly.discounts_percent_new_customers,0))-1 as yoy_discounts_percent_new_customers
    , ty.discounts_total_recur_customers
    , ly.discounts_total_recur_customers as ly_discounts_total_recur_customers
    , (ty.discounts_total_recur_customers/nullif(ly.discounts_total_recur_customers,0))-1 as yoy_discounts_total_recur_customers
    , ty.discounts_percent_recur_customers
    , ly.discounts_percent_recur_customers as ly_discounts_percent_recur_customers
    , (ty.discounts_percent_recur_customers/nullif(ly.discounts_percent_recur_customers,0))-1 as yoy_discounts_percent_recur_customers


{%- endmacro %}