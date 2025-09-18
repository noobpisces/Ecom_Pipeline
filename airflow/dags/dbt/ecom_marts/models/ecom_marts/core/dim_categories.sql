{{ config(
    materialized='view',
    tags=['marts', 'dimensions']
) }}

SELECT
    c.category_id,
    c.category_name,
    c.subcategory_count,
    c.product_count,
    SUM(oi.total_price) as category_revenue
FROM {{ source('ecom_intermediate', 'categories_enriched') }} c
LEFT JOIN {{ source('ecom_intermediate', 'subcategories_enriched') }} s on s.category_id = c.category_id
LEFT JOIN {{ source('ecom_intermediate', 'products_enriched') }} p on p.category_id = c.category_id
LEFT JOIN {{ source('ecom_intermediate', 'order_items') }} oi on oi.product_id = p.product_id
GROUP BY 1, 2,3,4
