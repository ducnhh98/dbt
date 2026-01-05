{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(Company, TerritoryID)'
) }}

{% set source_relation = ref("raw_SalesTerSvc") %}

WITH source_data AS (
    SELECT 
        Company,
        TerritoryID,
        TerritoryDesc,
        RegionCode
    FROM 
        {{ source_relation }}
)

SELECT *
FROM source_data