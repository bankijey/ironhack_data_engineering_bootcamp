SELECT
    product,
    COUNT(order_id) AS total_orders,
    SUM(amount) as product_revenue
FROM {{ ref('sales_data_ext') }}
GROUP BY product
ORDER BY product_revenue DESC