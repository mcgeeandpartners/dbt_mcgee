select
    customer_name,
    region,
    country,
    sum(  /* change every month*/
        case when fiscal_year = 'FY2024' and month(month) = 6 then net_amount else 0 end
    ) as fy_2024_revenue,
    sum(  /* change every month*/
        case when fiscal_year = 'FY2023' and month(month) = 6 then net_amount else 0 end
    ) as fy_2023_revenue,
    case
        when fy_2023_revenue <= 0 then 0 when fy_2023_revenue > 0 then fy_2024_revenue
    end as modified_fy_2024_revenue,
    coalesce(modified_fy_2024_revenue, 0) - coalesce(fy_2023_revenue, 0) as variance,
    case
        when fy_2023_revenue <= 0
        then null
        when fy_2023_revenue > 0
        then round(fy_2024_revenue / fy_2023_revenue, 2)
    end as nrr
from {{ ref("combined_revenue") }}
group by 1, 2, 3
