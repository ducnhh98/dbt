{{ config(
    engine='ReplacingMergeTree(from_raw)',
    materialized='incremental',
    unique_key='(Company, TerritoryID)',
    order_by='(Company, TerritoryID)',
    on_schema_change='append_new_columns'
) }}

{% set source_relation = ref("raw_SalesTerSvc") %}
{% set time_update = "toTimeZone(now(), 'Asia/Ho_Chi_Minh')" %}

WITH source_data AS (
    SELECT 
        toString(Company) AS Company,
        toString(TerritoryID) AS TerritoryID,
        toString(TerritoryDesc) AS TerritoryDesc,
        toString(RegionCode) AS RegionCode,
        from_epicor,
        {{time_update}} AS from_raw
    FROM 
        {{ source_relation }}
    {% if is_incremental() %}
    
    WHERE from_epicor >= (SELECT max(from_epicor) - INTERVAL 1 HOUR FROM {{ this }})
    
    {% endif %}
)

SELECT *
FROM source_data