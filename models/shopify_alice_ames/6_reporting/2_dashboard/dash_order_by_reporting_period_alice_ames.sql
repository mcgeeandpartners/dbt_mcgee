{{ config(alias="dash_order_by_reporting_period") }}

--== Last 30 Days

with orders_l30d_ty as (
    select
          'L30D' as reporting_period
        , 'Aggregate' as reporting_window
        , null as reporting_date

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order') }} as o
    where datediff('day', o.order_date, current_date() ) <= 30
        and o.order_date != current_date()
    group by 1,2
)

, orders_l30d_ly as (
    select
          'L30D' as reporting_period
        , 'Aggregate' as reporting_window        
        , null as reporting_date

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order') }} as o
    where datediff('day', o.order_date, dateadd('day', -365, current_date()) ) <= 30
        and o.order_date < dateadd ('day', -365, current_date())
    group by 1,2
)

, l30d as (
    select 
          ty.reporting_period
        , ty.reporting_window
        , ty.reporting_date

        {{ insert_kpi_comparison_metrics() }}
        
    from orders_l30d_ty as ty
    left join orders_l30d_ly as ly
        on ty.reporting_period = ly.reporting_period
)

--== Last 30 Days Daily

, orders_l30d_daily_ty AS (
    select
          'L30D' as reporting_period
        , 'Daily' as reporting_window
        , o.order_date as reporting_date

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order') }} as o
  where datediff('day', o.order_date, current_date()) <= 30
    and o.order_date <> current_date()
  group by 1,2,3
)

, orders_l30d_daily_ly AS (
    select
          'L30D' as reporting_period
        , 'Daily' as reporting_window
        , o.order_date as reporting_date

    {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order') }} as o
    where datediff('day', o.order_date, dateadd('day',-365,current_date())) <= 30
        and o.order_date < dateadd('day',-365,current_date())
    group by 1,2,3
)

, l30d_daily AS (
    select
          ty.reporting_period
        , ty.reporting_window
        , ty.reporting_date

        {{ insert_kpi_comparison_metrics() }}

    from orders_l30d_daily_ty as ty
    left join orders_l30d_daily_ly as ly
        on ty.reporting_date = dateadd('year',1, ly.reporting_date)
)

--== Year To Date

, orders_ytd_ty as (
    select
          'YTD' as reporting_period
        , 'Aggregate' as reporting_window
        , null as reporting_date

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order') }} as o
    where o.order_date >= date_trunc('year',current_date()) 
        and o.order_date < current_date()
    group by 1,2,3
)

, orders_ytd_ly as (
    select
          'YTD' as reporting_period
        , 'Aggregate' as reporting_window
        , null as reporting_date

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order') }} as o
    where o.order_date >= dateadd('year',-1,date_trunc('year',current_date())) 
        and o.order_date < dateadd('year',-1,current_date())
    group by 1,2,3
)

, ytd as (
    select
          ty.reporting_period
        , ty.reporting_window
        , ty.reporting_date

        {{ insert_kpi_comparison_metrics() }}

    from orders_ytd_ty as ty
    left join orders_ytd_ly as ly
        on ty.reporting_period = ly.reporting_period
)

--== Year To Date Monthly

, orders_ytd_monthly_ty as (
    select
          'YTD' as reporting_period
        , 'Monthly' as reporting_window
        , date_trunc('month',o.order_date) as reporting_date

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order') }} as o
    where o.order_date >= date_trunc('year',current_date()) 
        and o.order_date < current_date()
    group by 1,2,3
)

, orders_ytd_monthly_ly as (
    select
          'YTD' as reporting_period
        , 'Monthly' as reporting_window
        , date_trunc('month',o.order_date) as reporting_date

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order') }} as o
    where o.order_date >= dateadd('year',-1,date_trunc('year',current_date())) 
        and o.order_date < dateadd('year',-1,current_date())
    group by 1,2,3
)

, ytd_monthly as (
    select
          ty.reporting_period
        , ty.reporting_window
        , ty.reporting_date

        {{ insert_kpi_comparison_metrics() }}

    from orders_ytd_monthly_ty as ty
    left join orders_ytd_monthly_ly as ly
        on ty.reporting_date = dateadd('year',1, ly.reporting_date)
)

--== Last 12 months

, orders_l12m_ty as (
    select
          'LTM' as reporting_period
        , 'Aggregate' as reporting_window
        , null as reporting_date

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order') }} as o
    where o.order_date >= dateadd('day',-365,current_date()) 
        and o.order_date < current_date() 
    group by 1,2,3
)

, orders_l12m_ly as (
    select
          'LTM' as reporting_period
        , 'Aggregate' as reporting_window
        , null as reporting_date

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order') }} as o
    where o.order_date >= dateadd('day',-365*2,current_date()) 
        and o.order_date < dateadd('day',-365,current_date())
    group by 1,2,3
)

, l12m as (
    select
          ty.reporting_period
        , ty.reporting_window
        , ty.reporting_date

        {{ insert_kpi_comparison_metrics() }}

    from orders_l12m_ty as ty
    left join orders_l12m_ly as ly
        on ty.reporting_period = ly.reporting_period
)

--== Last 12 months Monthly

, orders_l12m_monthly_ty as (
    select
          'LTM' as reporting_period
        , 'Monthly' as reporting_window
        , date_trunc('month',o.order_date) as reporting_date

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order') }} as o
    where o.order_date >= dateadd('day',-365,current_date()) 
        and o.order_date < current_date() 
    group by 1,2,3
)

, orders_l12m_monthly_ly as (
    select
          'LTM' as reporting_period
        , 'Monthly' as reporting_window
        , date_trunc('month',o.order_date) as reporting_date

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order') }} as o
    where o.order_date >= dateadd('day',-365*2,current_date()) 
        and o.order_date < dateadd('day',-365,current_date())
    group by 1,2,3
)

, l12m_monthly as (
    select
          ty.reporting_period
        , ty.reporting_window
        , ty.reporting_date

        {{ insert_kpi_comparison_metrics() }}

    from orders_l12m_monthly_ty as ty
    left join orders_l12m_monthly_ly as ly
        on ty.reporting_date = dateadd('year',1, ly.reporting_date)
)

--== Month To Date

, orders_mtd_ty as (
    select
          'MTD' as reporting_period
        , 'Aggregate' as reporting_window
        , null as reporting_date

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order') }} as o
    where o.order_date >= date_trunc('month',current_date()) 
        and o.order_date < current_date()
    group by 1,2,3
)

, orders_mtd_ly as (
    select
          'MTD' as reporting_period
        , 'Aggregate' as reporting_window
        , null as reporting_date

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order') }} as o
    where o.order_date >= dateadd('year',-1,date_trunc('month', current_date())) 
        and o.order_date < dateadd('year',-1,current_date())
    group by 1,2,3
)

, mtd as (
    select
          ty.reporting_period
        , ty.reporting_window
        , ty.reporting_date

        {{ insert_kpi_comparison_metrics() }}

    from orders_mtd_ty as ty
    left join orders_mtd_ly as ly
        on ty.reporting_period = ly.reporting_period
)

--== Month To Date Daily

, orders_mtd_daily_ty as (
    select
          'MTD' as reporting_period
        , 'Daily' as reporting_window
        , o.order_date as reporting_date

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order') }} as o
    where o.order_date >= date_trunc('month',current_date()) 
        and o.order_date < current_date()
    group by 1,2,3
)

, orders_mtd_daily_ly as (
    select
          'MTD' as reporting_period
        , 'Daily' as reporting_window
        , o.order_date as reporting_date

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order') }} as o
    where o.order_date >= dateadd('year',-1,date_trunc('month', current_date())) 
        and o.order_date < dateadd('year',-1,current_date())
    group by 1,2,3
)

, mtd_daily as (
    select
          ty.reporting_period
        , ty.reporting_window
        , ty.reporting_date

        {{ insert_kpi_comparison_metrics() }}

    from orders_mtd_daily_ty as ty
    left join orders_mtd_daily_ly as ly
        on ty.reporting_date = dateadd('year',1, ly.reporting_date)
)


select * from l30d 
union all
select * from l30d_daily
union all
select * from ytd
union all 
select * from ytd_monthly
union all 
select * from l12m
union all 
select * from l12m_monthly
union all
select * from mtd
union all 
select * from mtd_daily