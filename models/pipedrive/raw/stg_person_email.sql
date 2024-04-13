select person_id, index, label, primary, email, _fivetran_synced
from {{ source("pipedrive", "person_email") }}
