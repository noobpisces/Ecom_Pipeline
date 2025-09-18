WITH source AS (
   SELECT * FROM {{ source('ecom_raw', 'INTERACTIONS') }}
),
casted AS (
   SELECT
       CAST(EVENT_ID AS VARCHAR) as event_id,
       CAST(CUSTOMER_ID AS VARCHAR) as customer_id,
       CAST(PRODUCT_ID AS VARCHAR) as product_id,
       CAST(EVENT_TYPE AS VARCHAR) as event_type,
       CAST(EVENT_DATE AS TIMESTAMP) as event_date,
       CAST(DEVICE_TYPE AS VARCHAR) as device_type,
       CAST(SESSION_ID AS VARCHAR) as session_id,
       CAST(CREATED_AT AS TIMESTAMP) as created_at,
       CAST(DATA_SOURCE AS VARCHAR) as data_source,
       CAST(BATCH_ID AS VARCHAR) as batch_id,
       CAST(LOADED_AT AS TIMESTAMP) as loaded_at
   FROM source
)
SELECT * FROM casted