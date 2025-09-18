{{ config(
    materialized='table',
    tags=['marts', 'sales']
) }}

SELECT
    p.product_id,
    p.category_id,
    date_trunc('month', o.order_date) as sales_month,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(oi.quantity) as units_sold,
    SUM(oi.total_price) as revenue,
    AVG(oi.unit_price) as avg_selling_price
FROM {{ source('ecom_intermediate', 'products_enriched') }} p
JOIN {{ source('ecom_intermediate', 'order_items') }} oi p.product_id = oi.product_id
JOIN {{ source('ecom_intermediate', 'orders') }} o o.order_id = oi.order_id
GROUP BY 1, 2, 3