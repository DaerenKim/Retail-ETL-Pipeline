select
    product_code,
    product_name,
    category,
    brand,
    pack_size,
    cast(mrp as decimal(12, 2)) as mrp,
    cast(gst_rate as integer)   as gst_rate
from {{ source('master', 'products') }}
