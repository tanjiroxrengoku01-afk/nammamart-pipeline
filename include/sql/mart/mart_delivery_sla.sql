-- Business question 3: average delivery time per store and % of DELIVERED orders within 15 minutes.
-- slower_than_35_min counts deliveries outside the client's normal 8-35 minute range (still < 120).

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    zone, store_id, store_name,
    COUNT(*)                                                 AS delivered_orders,
    ROUND(AVG(delivery_minutes), 1)                          AS avg_delivery_minutes,
    ROUND(median(delivery_minutes), 1)                       AS median_delivery_minutes,
    SUM(within_15_min)                                       AS delivered_within_15_min,
    ROUND(100 * AVG(within_15_min), 1)                       AS pct_within_15_min,
    COUNT(*) FILTER (WHERE delivery_minutes > 35)            AS slower_than_35_min
FROM mart_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY zone, store_id, store_name
ORDER BY pct_within_15_min DESC, store_id;
