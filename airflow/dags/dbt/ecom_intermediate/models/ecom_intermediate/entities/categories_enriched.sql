SELECT
    c.category_id category_id,
    c.category_name category_name,
    COUNT(DISTINCT s.subcategory_id) AS subcategory_count,
    COUNT(DISTINCT p.product_id) AS product_count,
    c.created_at
FROM {{ source('ecom_staging', 'stg_categories') }} c
LEFT JOIN {{ source('ecom_staging', 'stg_subcategories') }} s 
    ON c.category_id = s.category_id
LEFT JOIN {{ source('ecom_staging', 'stg_products') }} p 
    ON c.category_id = p.category_id
GROUP BY 
    c.category_id, 
    c.category_name, 
    c.created_at
