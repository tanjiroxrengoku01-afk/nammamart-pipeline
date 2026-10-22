-- MART fact: one row per clean order (dq_status = 'PASS'), enriched with the current dimension rows.
-- The ELT twin of etl_fact_orders, and the base of every other mart.
--   net_revenue   = order_amount for DELIVERED orders, else 0 (only DELIVERED counts as revenue)
--   gross_margin  = net_revenue - quantity x cost_price (DELIVERED only; NULL when cost_price is unusable)
--   within_15_min = 1/0 for DELIVERED orders, NULL otherwise

CREATE OR REPLACE TABLE mart_fact_orders AS
SELECT
    o.order_id, o.order_ts, o.order_date, o.customer_id, o.store_id, o.product_id,
    o.quantity, o.unit_price, o.discount_pct, o.order_amount, o.payment_mode, o.order_status,
    o.channel, o.delivery_minutes,
    CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END                AS net_revenue,
    CASE WHEN o.order_status = 'DELIVERED'
         THEN ROUND(o.order_amount - o.quantity * p.cost_price, 2) ELSE 0 END             AS gross_margin,
    CASE WHEN o.order_status = 'DELIVERED' THEN CAST(o.delivery_minutes <= 15 AS INTEGER) END AS within_15_min,
    s.zone, s.store_name, p.category, c.loyalty_tier,
    o._source_file, o._source_row, o._run_id, o._merged_at
FROM stg_orders o
LEFT JOIN stg_stores    s ON s.store_id    = o.store_id    AND s.is_current
LEFT JOIN stg_products  p ON p.product_id  = o.product_id  AND p.is_current
LEFT JOIN stg_customers c ON c.customer_id = o.customer_id AND c.is_current
WHERE o.dq_status = 'PASS';
