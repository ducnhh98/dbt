{{ config(
    materialized="incremental",
    engine='MergeTree()'
) }}

{% set time_update = "toTimeZone(now(), 'Asia/Ho_Chi_Minh')" %}

WITH source AS (
    SELECT
        (data.InvcDtl_Company) AS InvcDtl_Company,
        (data.InvcDtl_InvoiceNum) AS InvcDtl_InvoiceNum,
        (data.InvcDtl_InvoiceLine) AS InvcDtl_InvoiceLine,
        (data.InvcDtl_OrderNum) AS InvcDtl_OrderNum,
        (data.InvcDtl_OrderLine) AS InvcDtl_OrderLine,
        (data.InvcDtl_OrderRelNum) AS InvcDtl_OrderRelNum,
        (data.InvcDtl_LineDesc) AS InvcDtl_LineDesc,

        (data.InvcDtl_PartNum) AS InvcDtl_PartNum,
        (data.InvcDtl_OurShipQty) AS InvcDtl_OurShipQty,
        (data.InvcDtl_SalesUM) AS InvcDtl_SalesUM,
        
        (data.InvcDtl_DocUnitPrice) AS InvcDtl_DocUnitPrice,
        (data.InvcDtl_DocExtPrice) AS InvcDtl_DocExtPrice,
        (data.InvcDtl_DocDiscount) AS InvcDtl_DocDiscount,
        
        (data.InvcDtl_ProdCode) AS InvcDtl_ProdCode,
        (data.InvcDtl_SalesCatID) AS InvcDtl_SalesCatID,

        (data.InvcDtl_MtlUnitCost) AS InvcDtl_MtlUnitCost,
        (data.InvcDtl_LbrUnitCost) AS InvcDtl_LbrUnitCost,
        (data.InvcDtl_BurUnitCost) AS InvcDtl_BurUnitCost,
        (data.InvcDtl_SubUnitCost) AS InvcDtl_SubUnitCost,
        (data.InvcDtl_MtlBurUnitCost) AS InvcDtl_MtlBurUnitCost,

        (data.InvcDtl_CustNum) AS InvcDtl_CustNum,
        
        toDate(data.InvcDtl_ChangeDate) AS InvcDtl_ChangeDate,
        toUInt32(data.InvcDtl_ChangeTime) AS InvcDtl_ChangeTime,

        (data.RowIdent) AS RowIdent,

        {{time_update}} AS from_epicor
    FROM url(
            'https://portal.3ssoft.com.vn/SRV17KineticEdu/api/v2/odata/EPIC06/BaqSvc/E3S_InvcDtl/Data',
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
        InvcDtl_ChangeDate >
        (SELECT max(InvcDtl_ChangeDate) FROM {{ this }})
    )
    OR
    (
        InvcDtl_ChangeDate =
        (SELECT max(InvcDtl_ChangeDate) FROM {{ this }})
        AND
        InvcDtl_ChangeTime >
        (
            SELECT max(InvcDtl_ChangeTime)
            FROM {{ this }}
            WHERE InvcDtl_ChangeDate =
                  (SELECT max(InvcDtl_ChangeDate) FROM {{ this }})
        )
    )
{% endif %}
)
SELECT *
FROM source