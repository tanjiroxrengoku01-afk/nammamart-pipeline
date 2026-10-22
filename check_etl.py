import duckdb

con = duckdb.connect("include/warehouse_slick.duckdb", read_only=True)

queries = {
    "fact rows": "SELECT COUNT(*) FROM etl_fact_orders",
    "delivered orders": "SELECT COUNT(*) FROM etl_fact_orders WHERE order_status = 'DELIVERED'",
    "net revenue": "SELECT ROUND(SUM(net_revenue), 2) FROM etl_fact_orders",
    "gross margin": "SELECT ROUND(SUM(gross_margin), 2) FROM etl_fact_orders",
    "reconciliation rows": "SELECT COUNT(*) FROM etl_payment_reconciliation",
    "unreconciled amount": "SELECT ROUND(SUM(unreconciled_amount), 2) FROM etl_payment_reconciliation",
    "customers": "SELECT COUNT(*) FROM etl_dim_customer",
    "products": "SELECT COUNT(*) FROM etl_dim_product",
    "stores": "SELECT COUNT(*) FROM etl_dim_store",
    "duplicate order ids": "SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM etl_fact_orders",
}
for name, sql in queries.items():
    print(f"{name}: {con.execute(sql).fetchone()[0]}")

print()
print(con.execute("SELECT * FROM etl_payment_reconciliation LIMIT 3").df().to_string(index=False))
print()
print(con.execute("SELECT customer_id, email_masked, phone_masked FROM etl_dim_customer LIMIT 3").df().to_string(index=False))

print()
print(con.execute("""
SELECT order_status, COUNT(*) AS orders FROM etl_fact_orders
WHERE order_id IN (
    SELECT order_id FROM read_csv('include/data/nammamart/orders.csv', all_varchar=true)
    INTERSECT
    SELECT order_id FROM read_csv('include/data/nammamart/orders_2026-10-01_increment.csv', all_varchar=true))
GROUP BY 1""").df())