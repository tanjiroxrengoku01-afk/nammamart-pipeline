-- Business question 5: customers ranked by spend (DELIVERED net revenue) with loyalty tier.
-- Only masked contact details; no full name, raw email or raw phone. Top 10: WHERE spend_rank <= 10.

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    RANK() OVER (ORDER BY SUM(f.net_revenue) DESC)   AS spend_rank,
    f.customer_id,
    c.loyalty_tier,
    c.email_masked,
    c.phone_masked,
    c.area,
    COUNT(*)                                         AS delivered_orders,
    ROUND(SUM(f.net_revenue), 2)                     AS total_spend
FROM mart_fact_orders f
LEFT JOIN stg_customers c ON c.customer_id = f.customer_id AND c.is_current
WHERE f.order_status = 'DELIVERED'
GROUP BY f.customer_id, c.loyalty_tier, c.email_masked, c.phone_masked, c.area
ORDER BY spend_rank, f.customer_id;
