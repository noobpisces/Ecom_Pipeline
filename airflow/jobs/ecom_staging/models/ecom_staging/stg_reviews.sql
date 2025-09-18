WITH source AS (
    SELECT * FROM {{ source('ecom_raw', 'REVIEWS') }}
),
casted AS (
    SELECT
        CAST(REVIEW_ID AS VARCHAR) as review_id,
        CAST(PRODUCT_ID AS VARCHAR) as product_id,
        CAST(ORDER_ID AS VARCHAR) as order_id,
        CAST(CUSTOMER_ID AS VARCHAR) as customer_id,
        CAST(REVIEW_SCORE AS INTEGER) as review_score,
        CAST(REVIEW_TEXT AS TEXT) as review_text,
        CAST(DATA_SOURCE AS VARCHAR) as data_source,
        CAST(BATCH_ID AS VARCHAR) as batch_id,
        CAST(LOADED_AT AS TIMESTAMP) as loaded_at
    FROM source
)
SELECT * FROM casted