select
    store_code,
    store_name,
    locality,
    zone,
    format,
    pos_system,
    cast(open_date as date) as open_date,
    cast(sqft as integer)   as sqft,
    is_active = 'Y'         as is_active
from {{ source('master', 'stores') }}
