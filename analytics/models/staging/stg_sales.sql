-- One row per valid sales line (returns included as negative lines).
select
    store_code,
    cast(business_date as date)                    as business_date,
    bill_no,
    bill_datetime,
    product_code,
    quantity,
    unit_price,
    discount,
    net_amount,
    cast(quantity * unit_price as decimal(14, 2))  as gross_amount,
    payment_mode,
    is_return,
    source_file
from {{ source('silver', 'sales') }}
