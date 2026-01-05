{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(Company, RegionCode)'
) }}

{% set source_relation = ref("raw_RegionSvc") %}

WITH source_data AS (
    SELECT 
        Company,
        RegionCode,
        Description
    FROM 
        {{ source_relation }}
)

SELECT *
FROM source_data