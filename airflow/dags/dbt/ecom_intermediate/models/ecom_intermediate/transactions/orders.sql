SELECT
   o.order_id order_id,
   o.customer_id customer_id,
   o.order_date order_date,
   os.status_id, 
   pm.payment_method_id,
   sa.address_id AS shipping_address_id,
   ba.address_id AS billing_address_id,
   o.total_amount total_amount,
   o.shipping_cost,
   COUNT(DISTINCT oi.product_id) AS unique_products,
   SUM(oi.quantity) AS total_items,
   countIf(r.review_score IS NOT NULL) > 0 AS has_review,  -- ClickHouse equivalent of BOOL_OR
   AVG(r.review_score) AS avg_review_score,
   o.created_at AS created_at
FROM {{ source('ecom_staging', 'stg_orders') }} o
LEFT JOIN {{ ref('order_statuses') }} os 
   ON o.status = os.status_name
LEFT JOIN {{ ref('payment_methods') }} pm 
   ON o.payment_method = pm.method_name  
LEFT JOIN {{ ref('addresses') }} sa 
   ON o.shipping_address = sa.street_address
LEFT JOIN {{ ref('addresses') }} ba 
   ON o.billing_address = ba.street_address
LEFT JOIN {{ source('ecom_staging', 'stg_order_items') }} oi 
   ON o.order_id = oi.order_id
LEFT JOIN {{ source('ecom_staging', 'stg_reviews') }} r 
   ON o.order_id = r.order_id
GROUP BY 
   o.order_id,
   o.customer_id,
   o.order_date,
   os.status_id, 
   pm.payment_method_id,
   sa.address_id,
   ba.address_id,
   o.total_amount,
   o.shipping_cost,
   o.created_at
   
