{{ config(
    materialized='view',
    tags=['marts', 'dimensions']
) }}

SELECT DISTINCT
    date_day,
    toYear(date_day) AS year,
    toMonth(date_day) AS month,
    toDayOfWeek(date_day) AS day_of_week,
    toStartOfMonth(date_day) AS first_day_of_month,
    addMonths(toStartOfMonth(date_day), 1) - 1 AS last_day_of_month
FROM (
    SELECT DISTINCT order_date AS date_day
    FROM {{ source('ecom_intermediate', 'orders') }}
    
    UNION all

    SELECT DISTINCT event_date
    FROM {{ source('ecom_intermediate', 'customer_interactions') }}
)
