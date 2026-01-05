{{ config(engine='MergeTree()', materialized='table') }}

WITH date_span AS (
    SELECT 
        toDate('2000-01-01') AS start_date,
        toDate('2050-12-31') AS end_date
),
numbers AS (
    SELECT
        number AS n
    FROM system.numbers
    WHERE number <= dateDiff('day', (SELECT start_date FROM date_span), (SELECT end_date FROM date_span))
)
SELECT
    date AS date_key,
    toDayOfMonth(date) AS day,
    toISOWeek(date) AS week,
    toMonth(date) AS month,
    toYear(date) AS year
FROM (
    SELECT
        addDays((SELECT start_date FROM date_span), n) AS date
    FROM numbers
) AS src
ORDER BY date
