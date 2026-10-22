-- Q3: average delivery time per store and % of delivered orders within 15 minutes
SELECT store_id, store_name, zone, delivered_orders, avg_delivery_minutes, pct_within_15_min
FROM mart_delivery_sla
ORDER BY store_id
