{{ config(
    materialized='incremental',
    engine='MergeTree()'
) }}

{% set time_update = "toTimeZone(now(), 'Asia/Ho_Chi_Minh')" %}

WITH source AS (
    SELECT
        (data.OrderDtl_Company) AS OrderDtl_Company,
        (data.OrderDtl_OrderNum) AS OrderDtl_OrderNum,
        (data.OrderDtl_OrderLine) AS OrderDtl_OrderLine,

        (data.OrderDtl_DocUnitPrice) AS OrderDtl_DocUnitPrice,
        (data.OrderDtl_SalesUM) AS OrderDtl_SalesUM,
        (data.OrderDtl_PricePerCode) AS OrderDtl_PricePerCode,
        (data.OrderDtl_DiscountPercent) AS OrderDtl_DiscountPercent,

        (data.OrderDtl_PartNum) AS OrderDtl_PartNum,
        (data.OrderDtl_XPartNum) AS OrderDtl_XPartNum, 
        (data.OrderDtl_ProdCode) AS OrderDtl_ProdCode,
        (data.OrderDtl_TaxCatID) AS OrderDtl_TaxCatID,
        (data.OrderDtl_PriceListCode) AS OrderDtl_PriceListCode,

        (data.OrderDtl_VoidLine) AS OrderDtl_VoidLine, 
        (data.OrderDtl_OpenLine) AS OrderDtl_OpenLine,

        toDate(data.OrderDtl_ChangeDate) AS OrderDtl_ChangeDate,
        toUInt32(data.OrderDtl_ChangeTime) AS OrderDtl_ChangeTime,

        (data.RowIdent) AS RowIdent,

        {{time_update}} AS from_epicor
    FROM url(
            'https://portal.3ssoft.com.vn/SRV17KineticEdu/api/v2/odata/EPIC06/BaqSvc/E3S_OrderDtl/Data',
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
        OrderDtl_ChangeDate >
        (SELECT max(OrderDtl_ChangeDate) FROM {{ this }})
    )
    OR
    (
        OrderDtl_ChangeDate =
        (SELECT max(OrderDtl_ChangeDate) FROM {{ this }})
        AND
        OrderDtl_ChangeTime >
        (
            SELECT max(OrderDtl_ChangeTime)
            FROM {{ this }}
            WHERE OrderDtl_ChangeDate =
                  (SELECT max(OrderDtl_ChangeDate) FROM {{ this }})
        )
    )
{% endif %}
)
SELECT *
FROM source