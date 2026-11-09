-- Selling above the maximum retail price (MRP, in USD) is not allowed; the ETL must never load such a line.
select s.*
from {{ ref('stg_sales') }} s
join {{ ref('stg_products') }} p using (product_code)
where s.unit_price > p.mrp * 1.01
