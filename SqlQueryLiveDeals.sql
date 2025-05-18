WITH datamart_aligned AS (
    SELECT
        uniqueId,
        versiontimestamp,
        lakestoragetimestamp,
        product.maturitydate,
        product.effectiveMaturityDate,
        negotiatedcanceledindicator,
        destroyedindicator
    FROM datamart_table
),
trade_otc_aligned AS (
    SELECT
        uniqueId,
        versiontimestamp,
        lakestoragetimestamp,
        product.maturitydate,
        product.effectiveMaturityDate,
        negotiatedcanceledindicator,
        destroyedindicator
    FROM trade_otc_table
),
combined_data AS (
    SELECT * FROM datamart_aligned
    UNION ALL
    SELECT * FROM trade_otc_aligned
),
ranked_trades AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY uniqueId
               ORDER BY versiontimestamp DESC, lakestoragetimestamp DESC
           ) AS rnk
    FROM combined_data
),
latest_trades AS (
    SELECT * FROM ranked_trades WHERE rnk = 1
),
live_deals AS (
    SELECT *,
        CASE
            WHEN product.maturitydate IS NOT NULL AND 
                 DATEDIFF(product.maturitydate, DATE '1970-01-01') >= DATEDIFF(DATE '{{runDate}}', DATE '1970-01-01')
            THEN true ELSE false
        END AS liveaspermaturitydate,

        CASE
            WHEN product.effectiveMaturityDate IS NOT NULL AND 
                 DATEDIFF(product.effectiveMaturityDate, DATE '1970-01-01') >= DATEDIFF(DATE '{{runDate}}', DATE '1970-01-01')
            THEN true ELSE false
        END AS liveaspereffectivematuritydate
    FROM latest_trades
    WHERE (
        (product.maturitydate IS NOT NULL AND DATEDIFF(product.maturitydate, DATE '1970-01-01') >= DATEDIFF(DATE '{{runDate}}', DATE '1970-01-01'))
        OR
        (product.effectiveMaturityDate IS NOT NULL AND DATEDIFF(product.effectiveMaturityDate, DATE '1970-01-01') >= DATEDIFF(DATE '{{runDate}}', DATE '1970-01-01'))
    )
    AND COALESCE(negotiatedcanceledindicator, false) = false
    AND COALESCE(destroyedindicator, false) = false
)

SELECT * FROM live_deals;
