WITH source AS (
    SELECT * FROM {{ source('ecom_raw', 'CATEGORIES') }}
),
casted AS (
    SELECT
        CAST(CATEGORY_ID AS VARCHAR) as category_id,
        CAST(CATEGORY_NAME AS VARCHAR) as category_name,
        CAST(CREATED_AT AS TIMESTAMP) as created_at,
        CAST(DATA_SOURCE AS VARCHAR) as data_source,
        CAST(BATCH_ID AS VARCHAR) as batch_id,
        CAST(LOADED_AT AS TIMESTAMP) AS loaded_at
    FROM source
)
SELECT * FROM casted
