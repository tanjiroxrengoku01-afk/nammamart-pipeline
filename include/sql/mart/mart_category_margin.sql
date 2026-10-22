-- Business question 2: revenue and gross margin by product category (DELIVERED only).
-- margin = revenue - quantity x cost_price; margin_pct only uses orders whose cost_price is known.

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    category,
    COUNT(*)                                     AS delivered_orders,
    SUM(quantity)                                AS units,
    ROUND(SUM(net_revenue), 2)                   AS revenue,
    ROUND(SUM(gross_margin), 2)                  AS gross_margin,
    ROUND(100 * SUM(gross_margin)
          / NULLIF(SUM(CASE WHEN gross_margin IS NOT NULL THEN net_revenue END), 0), 2) AS margin_pct
FROM mart_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY category
ORDER BY revenue DESC;
