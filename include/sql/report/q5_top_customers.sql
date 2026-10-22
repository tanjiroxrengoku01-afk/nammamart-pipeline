-- Q5: top 10 customers by spend, with loyalty tier (masked contact details only)
SELECT spend_rank, customer_id, loyalty_tier, email_masked, phone_masked, delivered_orders, total_spend
FROM mart_top_customers
WHERE spend_rank <= 10
ORDER BY spend_rank, customer_id
