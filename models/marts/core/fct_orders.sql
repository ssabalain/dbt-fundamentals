WITH orders AS(
    SELECT 
        p.order_id,
        o.customer_id,
        p.order_status,
        p.amount
    FROM
        {{ref('stg_payment')}} AS p
        JOIN {{ref('stg_orders')}} AS O ON P.order_id = O.order_id

)

SELECT * FROM orders