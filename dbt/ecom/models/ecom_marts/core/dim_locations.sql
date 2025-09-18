{{ config(
    materialized='view',
    tags=['marts', 'dimensions']
) }}

SELECT
    l.location_id,
    city,
    state,
    country,
    COUNT(DISTINCT c.customer_id) as total_customers,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.total_amount) as total_revenue
FROM {{ source('ecom_intermediate', 'locations') }} l
LEFT JOIN {{ source('ecom_intermediate', 'customers_enriched') }} c on c.location_id = l.location_id
LEFT JOIN {{ source('ecom_intermediate', 'orders') }} o on o.customer_id = c.customer_id
GROUP BY 1, 2, 3, 4