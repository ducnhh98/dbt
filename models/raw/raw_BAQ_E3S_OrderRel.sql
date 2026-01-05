{{ config(
    materialized='incremental',
    engine='MergeTree()'
) }}

{% set time_update = "toTimeZone(now(), 'Asia/Ho_Chi_Minh')" %}

WITH source AS (
    SELECT
        (data.OrderRel_Company) AS OrderRel_Company,
        (data.OrderRel_OrderNum) AS OrderRel_OrderNum,
        (data.OrderRel_OrderLine) AS OrderRel_OrderLine,
        (data.OrderRel_OrderRelNum) AS OrderRel_OrderRelNum,

        (data.OrderRel_PartNum) AS OrderRel_PartNum,
        (data.OrderRel_RevisionNum) AS OrderRel_RevisionNum,
        (data.OrderRel_SellingReqQty) AS OrderRel_SellingReqQty,
        (data.OrderRel_SellingStockShippedQty) AS OrderRel_SellingStockShippedQty,
        (data.OrderRel_SellingJobShippedQty) AS OrderRel_SellingJobShippedQty,

        (data.OrderRel_NeedByDate) AS OrderRel_NeedByDate,
        (data.OrderRel_ReqDate) AS OrderRel_ReqDate,
    
        (data.OrderRel_ShipToNum) AS OrderRel_ShipToNum,
        (data.OrderRel_ShipViaCode) AS OrderRel_ShipViaCode,

        (data.OrderRel_FirmRelease) AS OrderRel_FirmRelease,
        (data.OrderRel_OpenRelease) AS OrderRel_OpenRelease,
        (data.OrderRel_VoidRelease) AS OrderRel_VoidRelease,
        (data.OrderRel_Make) AS OrderRel_Make, 
     
        (data.RowIdent) AS RowIdent,

        toDate(data.OrderRel_ChangeDate) AS OrderRel_ChangeDate,
        toUInt32(data.OrderRel_ChangeTime) AS OrderRel_ChangeTime,

        {{time_update}} AS from_epicor
    FROM url(
            'https://portal.3ssoft.com.vn/SRV17KineticEdu/api/v2/odata/EPIC06/BaqSvc/E3S_OrderRel/Data',
            'JSON',
            'value Array(JSON)',
            headers(
                'Authorization' = 'Basic bWFuYWdlcjptYW5hZ2Vy',
                'X-API-Key' = 'mDjcqzkHOz37K3wB5vJGPjCQ3MymDrhj5gjbncaecXgVW'
            )
        )
    ARRAY JOIN value AS data
    {% if is_incremental() %}
WHERE
    (
        OrderRel_ChangeDate >
        (SELECT max(OrderRel_ChangeDate) FROM {{ this }})
    )
    OR
    (
        OrderRel_ChangeDate =
        (SELECT max(OrderRel_ChangeDate) FROM {{ this }})
        AND
        OrderRel_ChangeTime >
        (
            SELECT max(OrderRel_ChangeTime)
            FROM {{ this }}
            WHERE OrderRel_ChangeDate =
                  (SELECT max(OrderRel_ChangeDate) FROM {{ this }})
        )
    )
{% endif %}
)
SELECT *
FROM source