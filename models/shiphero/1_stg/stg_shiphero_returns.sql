{{ config(alias="stg_returns") }}

with base as (
    select
        total_items_expected,
        reason,
        total_items_restocked,
        address,
        total_items_received,
        cost_to_customer,
        label_type,
        line_items,
        shipping_carrier,
        account_id,
        shipping_method,
        legacy_id,
        id,
        order_id,
        status,
        label_cost,
        _fivetran_synced,
        row_number() over (
            partition by id order by _fivetran_synced desc
        ) as ranking
    from {{ source("shiphero", "returns") }}
    )
select *
from base
where ranking = 1
