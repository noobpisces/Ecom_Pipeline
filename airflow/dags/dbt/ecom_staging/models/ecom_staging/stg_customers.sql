WITH source AS (
    SELECT * FROM {{ source('ecom_raw', 'CUSTOMERS') }}
),
casted AS (
    SELECT
        CAST(CUSTOMER_ID AS VARCHAR) as customer_id,
        CAST(EMAIL AS VARCHAR) as email,
        CAST(FIRST_NAME AS VARCHAR) as first_name,
        CAST(LAST_NAME AS VARCHAR) as last_name,
        CAST(AGE AS INTEGER) as age,
        CAST(GENDER AS VARCHAR) as gender,
        CAST(ANNUAL_INCOME AS DECIMAL(12,2)) as annual_income,
        CAST(MARITAL_STATUS AS VARCHAR) as marital_status,
        CAST(EDUCATION AS VARCHAR) as education,
        CAST(LOCATION_TYPE AS VARCHAR) as location_type,
        CAST(CITY AS VARCHAR) as city,
        CAST(STATE AS VARCHAR) as state,
        CAST(COUNTRY AS VARCHAR) as country,
        CAST(SIGNUP_DATE AS TIMESTAMP) as signup_date,
        parseDateTime64BestEffortOrNull(NULLIF(NULLIF(LAST_LOGIN, ''), 'null')) as last_login,
        CAST(PREFERRED_CHANNEL AS VARCHAR) as preferred_channel,
        CAST(IS_ACTIVE AS BOOLEAN) as is_active,
        CAST(DATA_SOURCE AS VARCHAR) as data_source,
        CAST(BATCH_ID AS VARCHAR) as batch_id,
        CAST(LOADED_AT AS TIMESTAMP) as loaded_at
    FROM source
)
SELECT * FROM casted