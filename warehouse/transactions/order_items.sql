{{ 
   config(
       materialized='table',
       tags=['transaction']
   )
}}

WITH
order_items_base AS (
    SELECT *
    FROM {{ source('ecom_staging', 'stg_order_items') }}
    WHERE product_id IS NOT NULL
),
reviews_deduped AS (
    SELECT 
        order_id,
        product_id,
        argMax(review_score, loaded_at) AS review_score
    FROM {{ source('ecom_staging', 'stg_reviews') }}
    GROUP BY order_id, product_id
)

SELECT DISTINCT
    oi.order_item_id,
    oi.order_id, 
    oi.product_id,
    o.customer_id,
    oi.quantity,
    oi.unit_price,
    oi.total_price,
    p.category_id,
    p.subcategory_id,
    p.brand_id,
    r.review_score,
    oi.created_at
FROM order_items_base AS oi
INNER JOIN {{ ref('products_enriched') }} AS p 
    ON oi.product_id = p.product_id   -- ✅ lọc sản phẩm hợp lệ ở đây
LEFT JOIN {{ source('ecom_staging', 'stg_orders') }} AS o 
    USING (order_id)
LEFT JOIN reviews_deduped AS r 
    USING (order_id, product_id)
