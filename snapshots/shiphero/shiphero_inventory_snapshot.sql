{% snapshot shiphero_inventory_snapshot %}

{{
    config(
      target_database='aestuary_dw',
      target_schema='shiphero',
      unique_key='id',
      strategy='check',
      check_cols=['on_hand']
    )
}}

select * from {{ref('dim_shiphero_warehouse_products')}}

{% endsnapshot %}