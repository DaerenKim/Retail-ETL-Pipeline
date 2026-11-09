-- The daily fact must account for every line: totals at both grains must agree.
with a as (select sum(net_amount) as v, count(*) as n from {{ ref('stg_sales') }}),
     b as (select sum(net_amount) as v, sum(lines) as n from {{ ref('fct_daily_store_sales') }})
select * from a, b where a.n <> b.n or abs(a.v - b.v) > 0.01
