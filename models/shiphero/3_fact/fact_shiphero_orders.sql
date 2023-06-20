{{ config(alias="fact_orders") }}

select o.id as order_id,
    r.return_key,
    w.warehouse_key,
    d.date_key,
    o.subtotal
from {{ref('stg_shiphero_orders')}} o
left join {{ref('dim_shiphero_returns')}} r 
on o.id = r.order_id
left join {{ref('dim_shiphero_warehouses')}} w 
on o.account_id = w.account_id
left join {{ref('dim_shiphero_date')}} d 
on o.order_date::date = d."DATE"