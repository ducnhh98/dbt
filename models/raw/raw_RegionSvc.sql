{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(Company, RegionCode)'
) }}

{% set time_update = "toTimeZone(now(), 'Asia/Ho_Chi_Minh')" %}

WITH source AS (
    SELECT
        toString(data.Company) AS Company,
        toString(data.RegionCode) AS RegionCode,
        toString(data.Description) AS Description,

        toUUID(data.SysRowID) AS SysRowID,
        toString(data.RowMod) AS RowMod,

        {{time_update}} AS from_epicor
    FROM url(
            'https://portal.3ssoft.com.vn/srv17kineticedu/api/v2/odata/EPIC06/Erp.BO.RegionSvc/List',
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