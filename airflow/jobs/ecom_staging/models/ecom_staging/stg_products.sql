WITH source AS (
   SELECT * FROM {{ source('ecom_raw', 'PRODUCTS') }}
),
casted AS (
   SELECT
       CAST(PRODUCT_ID AS VARCHAR) as product_id,
       CAST(CATEGORY_ID AS VARCHAR) as category_id,
       CAST(SUBCATEGORY_ID AS VARCHAR) as subcategory_id,
       CAST(PRODUCT_NAME AS VARCHAR) as product_name,
       CAST(DESCRIPTION AS TEXT) as description,
       CAST(BASE_PRICE AS DECIMAL(12,2)) as base_price,
       CAST(SALE_PRICE AS DECIMAL(12,2)) as sale_price,
       CAST(STOCK_QUANTITY AS INTEGER) as stock_quantity,
       CAST(WEIGHT_KG AS DECIMAL(8,2)) as weight_kg,
       CAST(IS_ACTIVE AS BOOLEAN) as is_active,
       CAST(CREATED_AT AS TIMESTAMP) as created_at,
       CAST(BRAND AS VARCHAR) as brand,
       CAST(SKU AS VARCHAR) as sku,
       CAST(RATING AS DECIMAL(3,1)) as rating,
       CAST(REVIEW_COUNT AS INTEGER) as review_count,
       CAST(DATA_SOURCE AS VARCHAR) as data_source,
       CAST(BATCH_ID AS VARCHAR) as batch_id,
       CAST(LOADED_AT AS TIMESTAMP) as loaded_at
   FROM source
)
SELECT * FROM casted