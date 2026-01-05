{{ config(order_by='("month")', engine='MergeTree()', materialized='table') }}

{% set months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'] %}

WITH src AS (
    {%- for i in range(0, 12) -%}
    
    SELECT
        {{i + 1}} AS "month",
        '{{months[i]}}' AS "month.name",
        CASE 
            WHEN {{i + 1}} IN (1, 2, 3) THEN 1
            WHEN {{i + 1}} IN (4, 5, 6) THEN 2
            WHEN {{i + 1}} IN (7, 8, 9) THEN 3
            ELSE 4 
        END AS "quarter.num",
        CASE 
            WHEN {{i + 1}} <= 6 THEN 1 
            ELSE 2 
        END AS "halfyear.num"
    
    {% if not loop.last %}
        UNION ALL
    {% endif %}
    
    {% endfor -%}
)
SELECT
    *
FROM 
    src