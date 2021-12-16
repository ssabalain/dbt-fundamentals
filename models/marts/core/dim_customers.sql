WITH customers AS (
    SELECT * FROM {{ref('stg_customers')}}
),
orders AS (
    SELECT * FROM {{ref('stg_orders')}}   
),
ft_orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
),

total_amount_by_order AS (
    SELECT 
        --order_id,
        customer_id,
        SUM(amount) AS total_amount
    FROM
        ft_orders
        JOIN orders using (order_id)
    WHERE
        order_status = 'success'
    GROUP by
        --order_id,
        customer_id
),
customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),



final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        total_amount_by_order.total_amount

    from customers
    left join customer_orders using (customer_id)
    left join total_amount_by_order using(c.customer_id)

)

select * from final