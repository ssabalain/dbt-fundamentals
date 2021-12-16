WITH payment AS(
    SELECT 
        id AS payment_id,
        orderid AS order_id,
        paymentmethod AS payment_method,
        status AS order_status,
        amount/100 as amount,
        created AS created_at
    FROM 
        raw.stripe.payment
)
SELECT * FROM payment
