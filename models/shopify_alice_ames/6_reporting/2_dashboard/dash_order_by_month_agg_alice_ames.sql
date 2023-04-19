{{ config(alias="dash_order_by_month_agg") }}

with orders_by as (
    select
        date_trunc('month', o.order_date) as order_by

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order_alice_ames') }} as o
    group by 1
)

select 
    ty.order_by as order_month
    , ly.order_by as ly_order_month

    {{ insert_kpi_comparison_metrics() }}

from orders_by ty
left join orders_by ly
    on ty.order_by = dateadd('month', 12, ly.order_by)