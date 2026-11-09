-- Every loaded line must satisfy net = qty * price - discount (within $0.05). Returns rows that fail.
select * from {{ ref('stg_sales') }}
where abs(quantity * unit_price - discount - net_amount) > 0.05
