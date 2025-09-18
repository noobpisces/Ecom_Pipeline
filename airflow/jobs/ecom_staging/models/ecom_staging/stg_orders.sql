WITH source AS (
   SELECT * FROM {{ source('ecom_raw', 'ORDERS') }}
),
casted AS (
   SELECT
       CAST(ORDER_ID AS VARCHAR) as order_id,
       CAST(CUSTOMER_ID AS VARCHAR) as customer_id,
       CAST(ORDER_DATE AS TIMESTAMP) as order_date,
       CAST(STATUS AS VARCHAR) as status,
       CAST(TOTAL_AMOUNT AS DECIMAL(12,2)) as total_amount,
       CAST(SHIPPING_COST AS DECIMAL(12,2)) as shipping_cost,
       CAST(PAYMENT_METHOD AS VARCHAR) as payment_method,
       CAST(SHIPPING_ADDRESS AS VARCHAR) as shipping_address,
       CAST(BILLING_ADDRESS AS VARCHAR) as billing_address,
       CAST(CREATED_AT AS TIMESTAMP) as created_at,
       CAST(UPDATED_AT AS TIMESTAMP) as updated_at,
       CAST(DATA_SOURCE AS VARCHAR) as data_source,
       CAST(BATCH_ID AS VARCHAR) as batch_id,
       CAST(LOADED_AT AS TIMESTAMP) as loaded_at
   FROM source
)
SELECT * FROM casted