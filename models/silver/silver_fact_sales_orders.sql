{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(Company, OrderNum, OrderLine, OrderRelNum)'
) }}

WITH source AS (
    SELECT
        a.OrderHed_Company AS Company,
        c.OrderRel_OrderNum AS OrderNum,
        b.OrderDtl_OrderLine AS OrderLine,
        c.OrderRel_OrderRelNum AS OrderRelNum,
        
        a.OrderHed_OrderDate AS OrderDate,
        a.OrderHed_PONum AS PONum,
        a.OrderHed_CurrencyCode AS CurrencyCode,
        a.OrderHed_ExchangeRate AS ExchangeRate,
        a.OrderHed_TermsCode AS TermsCode,
        a.OrderHed_FOB AS FOB,
        a.OrderHed_EntryPerson AS EntryPerson,
        a.OrderHed_OpenOrder AS OpenOrder,
        a.OrderHed_VoidOrder AS VoidOrder,
        a.OrderHed_CustNum AS CustNum,

        b.OrderDtl_DocUnitPrice AS DocUnitPrice,
        b.OrderDtl_SalesUM AS SalesUM,
        b.OrderDtl_OpenLine AS OpenLine,
        b.OrderDtl_VoidLine AS VoidLine,
        b.OrderDtl_DiscountPercent AS DiscountPercent,
        b.OrderDtl_PricePerCode AS PricePerCode,

        c.OrderRel_PartNum AS PartNum,
        c.OrderRel_SellingReqQty AS SellingReqQty,
        c.OrderRel_NeedByDate AS NeedByDate,
        c.OrderRel_ReqDate AS ReqDate,
        c.OrderRel_ShipToNum AS ShipToNum,
        c.OrderRel_SellingStockShippedQty AS SellingStockShippedQty,
        c.OrderRel_SellingJobShippedQty AS SellingJobShippedQty,
        c.OrderRel_ShipViaCode AS ShipViaCode,
        c.OrderRel_FirmRelease AS FirmRelease,
        c.OrderRel_OpenRelease AS OpenRelease,
        c.OrderRel_VoidRelease AS VoidRelease,
        c.OrderRel_Make AS Make

    FROM {{ ref('bronze_OrderHead') }} a
    INNER JOIN {{ ref('bronze_OrderDtl') }} b 
        ON a.OrderHed_Company = b.OrderDtl_Company AND a.OrderHed_OrderNum = b.OrderDtl_OrderNum
    INNER JOIN {{ ref('bronze_OrderRel') }} c 
        ON c.OrderRel_Company = b.OrderDtl_Company AND c.OrderRel_OrderNum = b.OrderDtl_OrderNum
),

calculated_base AS (
    SELECT 
        *,
        (DocUnitPrice * SellingReqQty) AS LineAmount,
        (SellingReqQty - SellingJobShippedQty - SellingStockShippedQty) AS OpenQty
    FROM 
        source
),

final_calculation AS (
    SELECT
        *,
        (CASE
            WHEN OpenQty > 0 THEN 
                (
                    OpenQty / (
                        CASE 
                            WHEN PricePerCode = 'M' THEN 1000
                            WHEN PricePerCode = 'C' THEN 100
                            ELSE 1
                        END
                    )
                ) * DocUnitPrice * (1 - (DiscountPercent / 100))
            ELSE 0
        END) AS OpenValue
    FROM
        calculated_base
)

SELECT 
    *
FROM 
    final_calculation
