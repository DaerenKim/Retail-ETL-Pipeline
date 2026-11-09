-- Product ranking for the period.
select
    p.product_code,
    p.product_name,
    p.category,
    p.brand,
    sum(s.quantity)                                         as net_units,
    round(sum(s.net_amount), 2)                             as net_sales,
    count(distinct s.store_code)                            as stores_selling,
    rank() over (order by sum(s.net_amount) desc)           as sales_rank,
    rank() over (partition by p.category order by sum(s.net_amount) desc) as rank_in_category
from {{ ref('stg_sales') }} s
join {{ ref('stg_products') }} p using (product_code)
group by p.product_code, p.product_name, p.category, p.brand
