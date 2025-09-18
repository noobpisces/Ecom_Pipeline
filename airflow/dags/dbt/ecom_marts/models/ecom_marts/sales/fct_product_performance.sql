{{ config(
    materialized='table',
    tags=['marts', 'sales']
) }}

SELECT
    p.product_id,
    dateTrunc('month', o.order_date) as month,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(oi.quantity) as units_sold,
    SUM(oi.total_price) as revenue,
    AVG(r.review_score) as avg_rating,
    COUNT(DISTINCT r.review_score) as review_count
FROM {{ source('ecom_intermediate', 'products_enriched') }} p
LEFT JOIN {{ source('ecom_intermediate', 'order_items') }} oi 
    ON p.product_id = oi.product_id
LEFT JOIN {{ source('ecom_intermediate', 'orders') }} o 
    ON oi.order_id = o.order_id
LEFT JOIN {{ source('ecom_intermediate', 'reviews_enriched') }} r 
    ON p.product_id = r.product_id
GROUP BY 1,2
