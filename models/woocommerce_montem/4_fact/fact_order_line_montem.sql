{{ config(alias="fact_order_line_item") }}

with
    new_order_line_item as (
        select
            oli.*,
            case
                when contains(oli.name, 'Route Shipping Protection')
                then 'Route'
                else 'Montem'
            end as order_line_item_vendor,
            oli.subtotal / oli.quantity as order_line_item_msrp,
            oli.quantity as order_line_item_units,
            oli.subtotal as order_line_item_gross_revenue,
            oli.subtotal - oli.total as order_line_item_total_discount,
            oli.sku as order_line_item_sku,
            null as product_barcode,
            null as order_line_item_is_gift_card,
            null as order_line_item_is_taxable
        from {{ ref("dim_order_line_item_montem") }} oli
    ),
    new_oli_agg as (
        select
            oli.order_id,
            oli.id as order_line_item_id,
            sum(
                case
                    when oli.order_line_item_vendor = 'Montem'
                    then oli.order_line_item_units
                    else 0
                end
            ) as order_units_product,
            sum(
                case
                    when oli.order_line_item_vendor = 'Route'
                    then oli.order_line_item_units
                    else 0
                end
            ) as order_units_route,
            sum(
                case
                    when oli.order_line_item_vendor = 'Montem'
                    then oli.order_line_item_gross_revenue
                    else 0
                end
            ) as order_gross_revenue_product,
            sum(
                case
                    when oli.order_line_item_vendor = 'Route'
                    then oli.order_line_item_gross_revenue
                    else 0
                end
            ) as order_gross_revenue_route
        from new_order_line_item oli
        group by 1, 2
    ),
    refunds_agg as (
        select order_refund.order_id, sum(refund.amount) as refunded_amount
        from {{ref('dim_order_refund_montem')}} order_refund
        left join
            {{ref('dim_refund_montem')}} refund
            on order_refund.refund_id = refund.id
        group by 1
    )
select
    o.id as order_id,
    c.customer_key,
    d.date_key,
    null as product_key,
    null as order_cancel_reason, 
    null as order_financial_status, 
    null as order_fulfillment_status, 
    oli_agg.order_line_item_id,
    o.currency as order_currency,
    o.prices_include_tax as order_has_tax_included,
    null as is_new_customer_order, 
    null as landing_site_base_url,
    null as referring_site,
    oli_agg.order_units_product,
    oli_agg.order_units_route,
    oli_agg.order_gross_revenue_product,
    oli_agg.order_gross_revenue_route,
    o.total_tax as order_gross_revenue_tax,
    o.shipping_total as order_gross_revenue_shipping,
    oli_agg.order_gross_revenue_product
    + oli_agg.order_gross_revenue_route
    + o.total_tax
    + o.shipping_total as order_gross_revenue_total,
    o.discount_total as order_discount,
    refunds_agg.refunded_amount as order_refund,
    o.total - refunds_agg.refunded_amount as order_net_revenue_total,
    NULL AS ORDER_LINE_ITEM_UNITS,
    NULL AS ORDER_LINE_ITEM_IS_GIFT_CARD,
    NULL AS ORDER_LINE_ITEM_MSRP,
    NULL AS PRODUCT_BARCODE,
    NULL AS ORDER_LINE_ITEM_SKU,
    NULL AS ORDER_LINE_ITEM_TOTAL_DISCOUNT,
    NULL as ORDER_LINE_ITEM_IS_TAXABLE,
    NULL AS ORDER_LINE_ITEM_GROSS_REVENUE,
    NULL as ORDER_LINE_ITEM_PRICE_MONTEM,
    NULL as order_line_item_vendor
from {{ref('stg_order_montem')}} o
left join {{ref('dim_customer_montem')}} as c 
    on o.customer_id = c.customer_id
left join new_oli_agg oli_agg on o.id = oli_agg.order_id
left join refunds_agg on o.id = refunds_agg.order_id
left join {{ref('dim_date_montem')}}  as d 
    on d."DATE" = o.DATE_CREATED_GMT::date
