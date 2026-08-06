SELECT
    customer,
    total_sales,
    case
        when total_sales >= 300000 then 'high'
        when total_sales >= 150000 then 'medium'
        else 'low'
    end as customer_segment
FROM {{ ref('customer_analytics') }}
ORDER by total_sales DESC