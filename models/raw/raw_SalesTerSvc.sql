{{ config(
    engine='ReplacingMergeTree(from_epicor)', 
    materialized='incremental',
    unique_key='SysRowID',
    order_by='SysRowID',
    on_schema_change='append_new_columns' 
) }}

{% set time_update = "toTimeZone(now(), 'Asia/Ho_Chi_Minh')" %}

WITH source AS (
    SELECT
        (data.Company) AS Company,
        (data.TerritoryID) AS TerritoryID,
        (data.TerritoryDesc) AS TerritoryDesc,
        (data.RegionCode) AS RegionCode,
        (data.RegionDescription) AS RegionDescription,
        (data.PrimeSalesRepCode) AS PrimeSalesRepCode,
        (data.PrimeBillingTypeCD) AS PrimeBillingTypeCD,
        (data.DefTaskSetID) AS DefTaskSetID,
        (data.DefTaskTaskSetDescription) AS DefTaskTaskSetDescription,
        (data.DefTaskWorkflowType) AS DefTaskWorkflowType,
        (data.Comment) AS Comment,
        (data.Inactive) AS Inactive,
        (data.ConsToPrim) AS ConsToPrim,
        (data.GlobalSalesTer) AS GlobalSalesTer,
        (data.GlobalLock) AS GlobalLock,
        (data.SysRevID) AS SysRevID,
        toUUID(data.SysRowID) AS SysRowID,
        {{time_update}} AS from_epicor

    FROM url(
            'https://portal.3ssoft.com.vn/srv17kineticedu/api/v2/odata/EPIC06/Erp.BO.SalesTerSvc/List',
            'JSON',
            'value Array(JSON)',
            headers(
                'Authorization' = 'Basic bWFuYWdlcjptYW5hZ2Vy',
                'X-API-Key' = 'mDjcqzkHOz37K3wB5vJGPjCQ3MymDrhj5gjbncaecXgVW'
            )
        )
    ARRAY JOIN value AS data
)
SELECT *
FROM source