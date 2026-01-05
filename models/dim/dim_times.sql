{{ config(
    engine='MergeTree()',
    materialized='table',
    order_by='time_key'
) }}

WITH time_numbers AS (
    SELECT
        number AS total_seconds
    FROM system.numbers
    LIMIT 86400
),
time_calculation AS (
    SELECT
        addSeconds(toDateTime('2000-01-01 00:00:00'), total_seconds) AS t
    FROM time_numbers
)
SELECT
    total_seconds AS time_key,
    toHour(t) AS hour,
    toMinute(t) AS minute,
    toSecond(t) AS second,
    CASE 
        WHEN hour >= 5 AND hour < 12 THEN 'Morning'
        WHEN hour >= 12 AND hour < 17 THEN 'Afternoon'
        WHEN hour >= 17 AND hour < 21 THEN 'Evening'
        ELSE 'Night'
    END AS period_of_day
FROM (
    SELECT 
        total_seconds,
        addSeconds(toDateTime('2000-01-01 00:00:00'), total_seconds) AS t
    FROM time_numbers
)
ORDER BY time_key