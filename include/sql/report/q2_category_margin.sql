-- Q2: revenue and gross margin by product category
SELECT category, delivered_orders, revenue, gross_margin, margin_pct
FROM mart_category_margin
ORDER BY revenue DESC
