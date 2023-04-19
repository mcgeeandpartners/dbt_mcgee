{{ config(alias="dash_order_by_day_agg") }}

with orders_by_day as (
    select
        o.order_date

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order_alice_ames') }} as o
    group by 1
)

select 
    ty.order_date
    , ly.order_date as ly_order_date

    {{ insert_kpi_comparison_metrics() }}

from orders_by_day ty
left join orders_by_day ly
    on ty.order_date = dateadd('year', 1, ly.order_date)