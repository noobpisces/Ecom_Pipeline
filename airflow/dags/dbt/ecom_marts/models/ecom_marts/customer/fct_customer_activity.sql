{{ config(
    materialized='incremental',
    tags=['marts', 'customers']
) }}

SELECT
    c.customer_id AS customer_id,
    c.email       AS email,
    toStartOfMonth(i.event_date) AS activity_month,

    COUNTIf(i.event_type = 'view') AS total_views,
    COUNT(DISTINCT if(i.event_type = 'view', i.product_id, NULL)) AS unique_products_viewed,

    COUNTIf(i.event_type = 'cart_add') AS cart_adds,
    COUNT(DISTINCT if(i.event_type = 'cart_add', i.product_id, NULL)) AS unique_products_added,

    COUNTIf(i.event_type = 'purchase') AS purchases,
    COUNT(DISTINCT if(i.event_type = 'purchase', i.product_id, NULL)) AS unique_products_purchased,

    COUNT(DISTINCT i.session_id) AS total_sessions,
    COUNT(DISTINCT i.device_type) AS devices_used,

    now() AS updated_at
FROM {{ source('ecom_intermediate', 'customers_enriched') }} c
LEFT JOIN {{ source('ecom_intermediate', 'customer_interactions') }} i 
    ON c.customer_id = i.customer_id
WHERE i.event_date IS NOT NULL
GROUP BY 
    c.customer_id, 
    email, 
    toStartOfMonth(i.event_date)
