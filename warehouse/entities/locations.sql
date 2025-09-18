WITH shipping_addresses AS (
    SELECT DISTINCT
        TRIM(arrayElement(splitByChar(',', shipping_address), length(splitByChar(',', shipping_address)) - 2)) AS city,
        TRIM(arrayElement(splitByChar(',', shipping_address), length(splitByChar(',', shipping_address)) - 1)) AS state,
        TRIM(arrayElement(splitByChar(',', shipping_address), length(splitByChar(',', shipping_address)))) AS country
    FROM {{ source('ecom_staging', 'stg_orders') }}
    WHERE shipping_address IS NOT NULL
),

billing_addresses AS (
    SELECT DISTINCT
        TRIM(arrayElement(splitByChar(',', billing_address), length(splitByChar(',', billing_address)) - 2)) AS city,
        TRIM(arrayElement(splitByChar(',', billing_address), length(splitByChar(',', billing_address)) - 1)) AS state,
        TRIM(arrayElement(splitByChar(',', billing_address), length(splitByChar(',', billing_address)))) AS country
    FROM {{ source('ecom_staging', 'stg_orders') }}
    WHERE billing_address IS NOT NULL
),

customer_addresses AS (
    SELECT DISTINCT
        TRIM(city) as city,
        TRIM(state) as state,
        TRIM(country) as country
    FROM {{ source('ecom_staging', 'stg_customers') }}
    WHERE city IS NOT NULL 
    AND state IS NOT NULL 
    AND country IS NOT NULL
),

all_locations AS (
    SELECT * FROM shipping_addresses
    UNION DISTINCT
    SELECT * FROM billing_addresses
    UNION DISTINCT
    SELECT * FROM customer_addresses
),

cleaned_locations AS (
    SELECT DISTINCT
        city,
        state,
        country
    FROM all_locations
    WHERE city != ''
    AND state != ''
    AND country != ''
    AND city IS NOT NULL
    AND state IS NOT NULL
    AND country IS NOT NULL
)

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['city', 'state', 'country']) }} AS location_id,
    city,
    state,
    country,
    now() AS created_at
FROM cleaned_locations
