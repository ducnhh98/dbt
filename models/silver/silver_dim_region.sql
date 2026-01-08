{{ config(
    materialized='table',
    engine='MergeTree()',
) }}

WITH source AS (
    SELECT
        *
    FROM 
        {{ ref('bronze_Region') }} 
)

SELECT 
    *
FROM source
