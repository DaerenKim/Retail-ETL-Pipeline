-- Return lines carry negative quantity and non-positive amount; sales lines the opposite.
select * from {{ ref('stg_sales') }}
where (is_return and (quantity >= 0 or net_amount > 0))
   or (not is_return and (quantity <= 0 or net_amount < 0))
