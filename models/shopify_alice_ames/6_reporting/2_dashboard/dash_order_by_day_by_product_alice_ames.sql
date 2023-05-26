{{ config(alias="dash_order_by_day_by_product_agg") }}

with orders_by_day as (
    select
        o.order_date
        , p.product_key
        , p.product_title
        , p.product_variant_title
        , p.product_sku
        , p.product_barcode
        , p.product_category
        , p.product_type
        , p.product_sub_type
        , p.product_campaign
        , p.campaign_year as product_campaign_year
        , p.is_print as product_is_print
        , p.print_solid_name as product_print_solid_name
        , p.base_fabric_color as product_base_fabric_color

        {{ insert_kpi_product_metrics() }}

    from {{ ref('base_rpt_order_line_item_alice_ames') }} as o
    left join {{ ref('dim_product_alice_ames') }} as p
        ON o.product_key = p.product_key
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)

select 
    ty.order_date
    , ly.order_date as ly_order_date
    , NVL(ty.product_title, ly.product_title) as product_title
    , NVL(ty.product_variant_title, ly.product_variant_title) as product_variant_title
    , NVL(ty.product_sku, ly.product_sku) as product_sku
    , NVL(ty.product_barcode, ly.product_barcode) as product_barcode
    , NVL(ty.product_category, ly.product_category) as product_category
    , NVL(ty.product_type, ly.product_type) as product_type
    , NVL(ty.product_sub_type, ly.product_sub_type) as product_sub_type
    , NVL(ty.product_campaign, ly.product_campaign) as product_campaign
    , NVL(ty.product_campaign_year, ly.product_campaign_year) as product_campaign_year
    , NVL(ty.product_is_print, ly.product_is_print) as product_is_print
    , NVL(ty.product_print_solid_name, ly.product_print_solid_name) as product_print_solid_name
    , NVL(ty.product_base_fabric_color, ly.product_base_fabric_color) as product_base_fabric_color

    {{ insert_kpi_product_comparison_metrics() }}

from orders_by_day ty
full outer join orders_by_day ly
/*using full outer join to capture all products from LY even if they dont exist this year*/
    on ty.order_date = dateadd('year', 1, ly.order_date)
    and ty.product_key = ly.product_key
where ty.order_date is not null