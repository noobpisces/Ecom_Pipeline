WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(total_amount) AS total_spent,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date
    FROM {{ source('ecom_staging', 'stg_orders') }}
    GROUP BY 1
),

validated_customers AS (
    SELECT *,
        TRIM(city)   AS cleaned_city,
        TRIM(state)  AS cleaned_state,
        TRIM(country) AS cleaned_country
    FROM {{ source('ecom_staging', 'stg_customers') }}
    WHERE city IS NOT NULL
      AND state IS NOT NULL
      AND country IS NOT NULL
      AND TRIM(city)   != ''
      AND TRIM(state)  != ''
      AND TRIM(country)!= ''
)

SELECT
    c.customer_id,
    c.email,
    c.first_name,
    c.last_name,
    c.age,
    c.gender,
    c.annual_income,
    e.education_id,
    m.marital_status_id,
    COALESCE(
        l.location_id, 
        {{ dbt_utils.generate_surrogate_key(['cleaned_city','cleaned_state','cleaned_country']) }}
    ) AS location_id,

    -- CHỈNH 3 CỘT THỜI GIAN
    parseDateTime64BestEffortOrNull(c.signup_date, 3) AS signup_date,
    parseDateTime64BestEffortOrNull(c.last_login, 3)  AS last_login,
    parseDateTime64BestEffortOrNull(co.first_order_date, 3) AS first_order_date,
    parseDateTime64BestEffortOrNull(co.last_order_date, 3)  AS last_order_date,

    c.loaded_at AS created_at
FROM validated_customers c
LEFT JOIN {{ ref('education_types') }} e 
    ON c.education = e.education_type
LEFT JOIN {{ ref('marital_statuses') }} m 
    ON c.marital_status = m.status_type
LEFT JOIN {{ ref('locations') }} l 
    ON c.cleaned_city   = l.city
   AND c.cleaned_state  = l.state
   AND c.cleaned_country= l.country
LEFT JOIN customer_orders co 
    USING (customer_id);
