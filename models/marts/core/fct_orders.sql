
with payments as (
    select * from {{ ref('stg_payments') }}
),
fact_orders as (
    select 
        order_id,
        Sum(case when status = 'success' then amount end) as amount
    from payments
    group by order_id
),
orders as (
    select * from {{ ref('stg_orders') }}
),
final as (
    select 
        fact_orders.order_id,
        fact_orders.amount,
        orders.customer_id,
        orders.order_date
    from fact_orders 
    left join orders using (order_id)
)
    select * from final  

