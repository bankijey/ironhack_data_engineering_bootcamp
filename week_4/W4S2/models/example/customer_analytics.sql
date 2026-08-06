SELECT
    customer,
    SUM(amount) AS total_sales,
    COUNT(order_id) AS total_orders,
    AVG(amount) AS mean_sales
    
FROM {{ ref('sales_data_ext') }}
-- FROM {{ ref('sales_data') }}
GROUP BY customer
ORDER BY total_sales DESC