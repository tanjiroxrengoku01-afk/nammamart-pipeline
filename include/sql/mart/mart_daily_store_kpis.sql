-- Business question 1: daily net revenue by zone and by store (DELIVERED only, after discount),
-- plus the day's delivery KPIs and a store-capacity check (orders vs capacity_orders_per_day).

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    f.order_date, f.zone, f.store_id, f.store_name,
    COUNT(*)                                                         AS total_orders,
    COUNT(*) FILTER (WHERE f.order_status = 'DELIVERED')             AS delivered_orders,
    COUNT(*) FILTER (WHERE f.order_status = 'CANCELLED')             AS cancelled_orders,
    COUNT(*) FILTER (WHERE f.order_status = 'RETURNED')              AS returned_orders,
    ROUND(SUM(f.net_revenue), 2)                                     AS net_revenue,
    ROUND(AVG(f.delivery_minutes) FILTER (WHERE f.order_status = 'DELIVERED'), 1) AS avg_delivery_minutes,
    ROUND(100 * AVG(f.within_15_min), 1)                             AS pct_within_15_min,
    s.capacity_orders_per_day,
    COALESCE(COUNT(*) > NULLIF(s.capacity_orders_per_day, 0), false) AS over_capacity
FROM mart_fact_orders f
LEFT JOIN stg_stores s ON s.store_id = f.store_id AND s.is_current
GROUP BY f.order_date, f.zone, f.store_id, f.store_name, s.capacity_orders_per_day
ORDER BY f.order_date, f.zone, f.store_id;
