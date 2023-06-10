select *
from {{ source('recharge_lpc', 'plan') }}