{{
   config(
       materialized='table',
       tags=['transaction']
   )
}}

WITH order_items_base AS (
   SELECT * 
   FROM {{ source('ecom_staging', 'stg_order_items') }}
   WHERE product_id IS NOT NULL
),

valid_products AS (
   SELECT DISTINCT product_id 
   FROM {{ ref('products_enriched') }}
),

validated_items AS (
   SELECT DISTINCT oi.*
   FROM order_items_base oi
   INNER JOIN valid_products vp
       ON oi.product_id = vp.product_id
),

reviews_deduped AS (
    SELECT DISTINCT 
        order_id,
        product_id,
        anyLast(review_score) AS review_score   -- ClickHouse equivalent of FIRST_VALUE by order
    FROM {{ source('ecom_staging', 'stg_reviews') }}
    GROUP BY order_id, product_id
)

SELECT DISTINCT
  oi.order_item_id,
  oi.order_id order_id, 
  oi.product_id product_id,
  o.customer_id,
  oi.quantity quantity,
  oi.unit_price,
  oi.total_price total_price,
  p.category_id,
  p.subcategory_id,
  p.brand_id,
  r.review_score,
  oi.created_at
FROM validated_items oi
LEFT JOIN {{ source('ecom_staging', 'stg_orders') }} o 
  ON oi.order_id = o.order_id
LEFT JOIN {{ ref('products_enriched') }} p 
  ON oi.product_id = p.product_id
LEFT JOIN reviews_deduped r 
  ON oi.order_id = r.order_id 
 AND oi.product_id = r.product_id
