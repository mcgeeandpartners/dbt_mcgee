{{ config(materialized="view") }}

{{ config(alias="dim_date") }} {{ generate_date_dim() }}
