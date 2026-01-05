{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(Company, CustID)'
) }}

{% set source_relation = ref("raw_CustomerSvc") %}

WITH source_data AS (
    SELECT 
        Company,
        CustNum,
        CustID,
        Name,
        CustomerType,
        GroupCode,
        TerritoryID
    FROM 
        {{ source_relation }}
)

SELECT *
FROM source_data