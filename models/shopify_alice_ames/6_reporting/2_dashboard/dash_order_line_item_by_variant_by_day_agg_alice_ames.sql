{{ config(alias="dash_order_line_item_by_variant_by_day_agg") }}

with orders_by_day as (
    select
        order_date
        , o.day_num_in_week
        , o.day_name
        , o.day_abbrev
        , o.day_num_in_month
        , o.us_holiday_ind
        , o.product_variant_id
        , o.product_id
        , o.product_title
        , o.product_sku
        , o.product_barcode
        , o.product_category
        , o.product_type
        , o.product_sub_type
        , o.product_campaign
        , o.product_campaign_year
        , o.product_is_print
        , o.product_print_solid_name
        , o.product_base_fabric_color
        , o.product_made_in
        , o.product_manufacturer
        , o.vendor
        , o.is_gift_card

        {{ insert_order_line_agg_metrics() }}

    from {{ ref('base_rpt_order_line_item_alice_ames') }} as o
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
)

select 
    ty.order_date
    , ly.order_date as ly_order_date
    , ty.day_num_in_week
    , ty.day_name
    , ty.day_abbrev
    , ty.day_num_in_month
    , ty.us_holiday_ind
    , ty.product_variant_id
    , ty.product_id
    , ty.product_title
    , ty.product_sku
    , ty.product_barcode
    , ty.product_category
    , ty.product_type
    , ty.product_sub_type
    , ty.product_campaign
    , ty.product_campaign_year
    , ty.product_is_print
    , ty.product_print_solid_name
    , ty.product_base_fabric_color
    , ty.product_made_in
    , ty.product_manufacturer
    , ty.vendor
    , ty.is_gift_card

    {{ insert_order_line_agg_comparison_metrics() }}

from orders_by_day ty
left join orders_by_day ly
    on ty.order_date = dateadd('year', 1, ly.order_date)
    and ty.product_variant_id = ly.product_variant_id