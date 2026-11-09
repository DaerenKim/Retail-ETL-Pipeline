-- Grain: one row per store per business date.
with lines as (select * from {{ ref('stg_sales') }})
select
    store_code || '|' || cast(business_date as varchar)                  as store_day_key,
    store_code,
    business_date,
    dayname(business_date)                                               as day_name,
    isodow(business_date) in (6, 7)                                      as is_weekend,
    count(distinct bill_no) filter (where not is_return)                 as sales_bills,
    count(distinct bill_no) filter (where is_return)                     as return_bills,
    count(*)                                                             as lines,
    sum(quantity)                                                        as net_units,
    sum(gross_amount)                                                    as gross_amount,
    sum(discount)                                                        as discount_amount,
    sum(net_amount)                                                      as net_amount,
    coalesce(sum(net_amount) filter (where not is_return), 0)            as sales_amount,
    coalesce(sum(net_amount) filter (where is_return), 0)                as returns_amount,
    round(coalesce(sum(net_amount) filter (where not is_return), 0)
          / nullif(count(distinct bill_no) filter (where not is_return), 0), 2) as avg_basket_value
from lines
group by store_code, business_date
