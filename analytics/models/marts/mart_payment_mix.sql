-- Payment mode share by store format (sales bills only).
with bills as (
    select s.store_code, st.format, s.bill_no, any_value(s.payment_mode) as payment_mode, sum(s.net_amount) as bill_net
    from {{ ref('stg_sales') }} s
    join {{ ref('stg_stores') }} st using (store_code)
    where not s.is_return
    group by s.store_code, st.format, s.bill_no
)
select
    format,
    payment_mode,
    count(*)                                                                as bills,
    round(sum(bill_net), 2)                                                 as net_amount,
    round(100.0 * count(*) / sum(count(*)) over (partition by format), 2)  as share_of_bills_pct,
    round(100.0 * sum(bill_net) / sum(sum(bill_net)) over (partition by format), 2) as share_of_value_pct
from bills
group by format, payment_mode
