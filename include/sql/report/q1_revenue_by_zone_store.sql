-- Q1: net revenue by zone and store (DELIVERED, after discount). Add order_date to the GROUP BY for daily.
SELECT zone, store_id, store_name, SUM(delivered_orders) AS delivered_orders, ROUND(SUM(net_revenue), 2) AS net_revenue
FROM mart_daily_store_kpis
GROUP BY zone, store_id, store_name
ORDER BY zone, net_revenue DESC
