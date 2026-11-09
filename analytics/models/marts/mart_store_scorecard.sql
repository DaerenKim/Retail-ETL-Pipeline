-- One row per store: headline KPIs for the period and how the store compares with its format peers.
with daily as (select * from {{ ref('fct_daily_store_sales') }}),
per_store as (
    select
        store_code,
        count(*)                                                    as trading_days,
        sum(net_amount)                                             as net_sales,
        avg(net_amount)                                             as avg_daily_net,
        sum(sales_bills)                                            as sales_bills,
        sum(sales_amount) / nullif(sum(sales_bills), 0)             as avg_basket_value,
        -100 * sum(returns_amount) / nullif(sum(sales_amount), 0)   as return_rate_pct,
        100 * sum(discount_amount) / nullif(sum(gross_amount), 0)   as discount_rate_pct,
        avg(net_amount) filter (where is_weekend)
            / nullif(avg(net_amount) filter (where not is_weekend), 0) - 1 as weekend_uplift
    from daily
    group by store_code
)
select
    st.store_code,
    st.store_name,
    st.zone,
    st.format,
    st.pos_system,
    st.sqft,
    p.trading_days,
    round(p.net_sales, 2)                                           as net_sales,
    round(p.avg_daily_net, 2)                                       as avg_daily_net,
    p.sales_bills,
    round(p.avg_basket_value, 2)                                    as avg_basket_value,
    round(p.return_rate_pct, 2)                                     as return_rate_pct,
    round(p.discount_rate_pct, 2)                                   as discount_rate_pct,
    round(100 * p.weekend_uplift, 1)                                as weekend_uplift_pct,
    round(p.net_sales / nullif(st.sqft, 0), 2)                      as net_sales_per_sqft,
    rank() over (partition by st.format order by p.net_sales desc)  as rank_in_format,
    round(100 * (p.avg_daily_net / avg(p.avg_daily_net) over (partition by st.format) - 1), 1)
                                                                    as vs_format_avg_pct
from {{ ref('stg_stores') }} st
join per_store p using (store_code)
