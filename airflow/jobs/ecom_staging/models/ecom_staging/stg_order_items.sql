WITH source AS (
   SELECT * FROM {{ source('ecom_raw', 'ORDER_ITEMS') }}
),
casted AS (
   SELECT
       CAST(ORDER_ITEM_ID AS VARCHAR) as order_item_id,
       CAST(ORDER_ID AS VARCHAR) as order_id,
       CAST(PRODUCT_ID AS VARCHAR) as product_id,
       CAST(QUANTITY AS INTEGER) as quantity,
       CAST(UNIT_PRICE AS DECIMAL(12,2)) as unit_price,
       CAST(TOTAL_PRICE AS DECIMAL(12,2)) as total_price,
       CAST(CREATED_AT AS TIMESTAMP) as created_at,
       CAST(DATA_SOURCE AS VARCHAR) as data_source,
       CAST(BATCH_ID AS VARCHAR) as batch_id,
       CAST(LOADED_AT AS TIMESTAMP) as loaded_at
   FROM source
)
SELECT * FROM casted