with source as (
    select 
        _row::varchar as row_sequence,
        end_date_if_applicable_::date as end_date_if_applicable,
        _discount as discount,
        lower(discount_type) as discount_type,
        lower(event_name) as event_name,
        lower(event_type) as event_type,
        lower(code) as code,
        launch_date::date as launch_date,
        lower(notes) as notes,
        _fivetran_synced
    from {{ source('aestuary_gheets', 'marketing_calendar_alice_ames') }}
)

select * 
from source