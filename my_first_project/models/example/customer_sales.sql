SELECT
    customer,
    SUM(amount) AS total_sales
FROM {{ ref('sales_data') }}
GROUP BY customer
