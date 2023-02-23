with source as (
    select * 
    from {{ source('seeds', 'marketing_calendar_alice_ames') }}
)

select * 
from source