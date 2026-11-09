-- Grain: one row per business date per product category.
select
    s.business_date,
    p.category,
    count(*)            as lines,
    sum(s.quantity)     as net_units,
    sum(s.net_amount)   as net_amount,
    sum(s.discount)     as discount_amount
from {{ ref('stg_sales') }} s
join {{ ref('stg_products') }} p using (product_code)
group by s.business_date, p.category
