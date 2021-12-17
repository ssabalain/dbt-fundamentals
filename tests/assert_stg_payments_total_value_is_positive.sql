with payments as (
    SELECT * FROM {{ ref('stg_payments') }}
)

SELECT
    order_id,
    SUM(amount) as total_amount
FROM
    payments
GROUP BY
    order_id
HAVING 
    total_amount < 0