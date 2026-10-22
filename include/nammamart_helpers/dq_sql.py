"""Part D - NammaMart data-quality rules, SQL side (used by the ELT DAG, DuckDB dialect).

Each rule name from quality_rules.RULES has one SQL predicate here that is TRUE for a failing row.
The check name, dimension and severity come from quality_rules, so both pipelines always report the
same checks. Predicates run against the RAW tables (raw_orders, raw_payments, ...), whose columns are
all text. Blanks may be NULL in DuckDB, so every text test goes through T(col) = COALESCE(TRIM(col), '').

  insert_dq_results_sql(table, run_id, dag_id, run_ts)  -> INSERT that logs one dq_results row per check
  failure_reason_sql(table, run_ts)                      -> expression listing the Critical rules a row breaks
  critical_predicate_sql(table, run_ts)                  -> TRUE when a row breaks any Critical rule
"""
from __future__ import annotations

from .quality_rules import CRITICAL, DQ_RESULTS_DDL, rules_for

TS = "'%Y-%m-%d %H:%M:%S'"
TOL = "0.011"


def T(col):
    return f"COALESCE(TRIM({col}), '')"


def N(col):
    return f"TRY_CAST({col} AS DOUBLE)"


MODE_CLEAN = (
    f"CASE LOWER({T('payment_mode')}) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' "
    "WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery' "
    "WHEN 'wallet' THEN 'Wallet' END"
)
EMAIL = r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$"


def _first_dup(table, key):  # every repeat after the first row (rowid = load order)
    return f"rowid NOT IN (SELECT MIN(rowid) FROM raw_{table} GROUP BY {key})"


def _not_in_master(col, table, key):
    return f"{T(col)} NOT IN (SELECT TRIM({key}) FROM raw_{table} WHERE {key} IS NOT NULL)"


# :run_ts is replaced by TIMESTAMP '<value>' when the SQL is built
SQL: dict[str, str] = {
    # ---- orders
    "orders_customer_id_not_null": f"{T('customer_id')} = ''",
    "orders_delivered_has_delivery_minutes": f"UPPER({T('order_status')}) = 'DELIVERED' AND {T('delivery_minutes')} = ''",
    "orders_order_id_unique": _first_dup("orders", "order_id"),
    "orders_order_status_valid": f"UPPER({T('order_status')}) NOT IN ('DELIVERED','CANCELLED','RETURNED')",
    "orders_order_status_casing": (f"UPPER({T('order_status')}) IN ('DELIVERED','CANCELLED','RETURNED') "
                                   f"AND COALESCE(order_status, '') <> UPPER({T('order_status')})"),
    "orders_payment_mode_standardised": "COALESCE(payment_mode, '') NOT IN ('UPI','Card','Cash on Delivery','Wallet')",
    "orders_payment_mode_valid_after_cleaning": f"({MODE_CLEAN}) IS NULL",
    "orders_order_ts_parses": f"TRY_STRPTIME({T('order_ts')}, {TS}) IS NULL",
    "orders_discount_pct_0_100": f"{N('discount_pct')} < 0 OR {N('discount_pct')} > 100",
    "orders_quantity_positive": f"{N('quantity')} <= 0",
    "orders_order_amount_matches_formula": (
        f"NOT COALESCE(ABS(ROUND({N('quantity')} * {N('unit_price')} * (1 - {N('discount_pct')} / 100.0), 2) "
        f"- {N('order_amount')}) <= {TOL}, false)"),
    "orders_delivery_minutes_below_120": f"{N('delivery_minutes')} >= 120",
    "orders_customer_id_exists": f"{T('customer_id')} <> '' AND {_not_in_master('customer_id', 'customers', 'customer_id')}",
    "orders_product_id_exists": _not_in_master("product_id", "products", "product_id"),
    "orders_store_id_exists": _not_in_master("store_id", "stores", "store_id"),
    "orders_order_ts_not_future": f"TRY_STRPTIME({T('order_ts')}, {TS}) > :run_ts",
    # table-scope rule: one boolean for the whole table
    "orders_latest_order_fresh": (
        f"COALESCE((SELECT :run_ts - MAX(ts) FROM (SELECT TRY_STRPTIME({T('order_ts')}, {TS}) AS ts FROM raw_orders) "
        "WHERE ts <= :run_ts) > INTERVAL 1 DAY, true)"),
    # ---- payments
    "payments_payment_id_unique": _first_dup("payments", "payment_id"),
    "payments_gateway_ref_present_for_non_cod": f"COALESCE({MODE_CLEAN}, '?') <> 'Cash on Delivery' AND {T('gateway_ref')} = ''",
    "payments_payment_status_valid": f"UPPER({T('payment_status')}) NOT IN ('SUCCESS','FAILED','REFUNDED')",
    "payments_payment_mode_standardised": "COALESCE(payment_mode, '') NOT IN ('UPI','Card','Cash on Delivery','Wallet')",
    "payments_payment_mode_valid_after_cleaning": f"({MODE_CLEAN}) IS NULL",
    "payments_amount_paid_positive": f"{N('amount_paid')} <= 0",
    "payments_order_id_exists": _not_in_master("order_id", "orders", "order_id"),
    "payments_success_amount_matches_order": (
        f"UPPER({T('payment_status')}) = 'SUCCESS' AND ABS({N('amount_paid')} - TRY_CAST((SELECT arg_min(o.order_amount, o.rowid) "
        f"FROM raw_orders o WHERE TRIM(o.order_id) = {T('raw_payments.order_id')}) AS DOUBLE)) > {TOL}"),
    "payments_payment_ts_not_future": f"TRY_STRPTIME({T('payment_ts')}, {TS}) > :run_ts",
    # ---- customers
    "customers_customer_id_unique": _first_dup("customers", "customer_id"),
    "customers_city_not_null": f"{T('city')} = ''",
    "customers_email_valid": f"NOT REGEXP_MATCHES({T('email')}, '{EMAIL}')",
    "customers_phone_10_digits": f"NOT REGEXP_MATCHES({T('phone')}, '^[0-9]{{10}}$')",
    "customers_pincode_6_digits": f"NOT REGEXP_MATCHES({T('pincode')}, '^[0-9]{{6}}$')",
    "customers_loyalty_tier_casing": "COALESCE(loyalty_tier, '') NOT IN ('Bronze','Silver','Gold','Platinum')",
    "customers_signup_date_not_future": f"TRY_STRPTIME({T('signup_date')}, '%Y-%m-%d') > :run_ts",
    # ---- products
    "products_product_id_unique": _first_dup("products", "product_id"),
    "products_category_not_null": f"{T('category')} = ''",
    "products_mrp_positive": f"{N('mrp')} <= 0",
    "products_cost_price_le_mrp": f"{N('cost_price')} > {N('mrp')}",
    # ---- stores
    "stores_store_id_unique": _first_dup("stores", "store_id"),
    "stores_zone_not_null": f"{T('zone')} = ''",
    "stores_capacity_positive": f"{N('capacity_orders_per_day')} <= 0",
}

_missing = {r.name for r in rules_for("orders") + rules_for("payments") + rules_for("customers")
            + rules_for("products") + rules_for("stores")} ^ set(SQL)
assert not _missing, f"Python rules and SQL predicates are out of sync: {sorted(_missing)}"


def _sql(name, run_ts):
    return SQL[name].replace(":run_ts", f"TIMESTAMP '{run_ts}'")


def insert_dq_results_sql(table, run_id, dag_id, run_ts):
    """One INSERT ... SELECT that logs every check for `table`. Run create_dq_results_sql() first.
    Counts use plain scalar sub-selects (WHERE <predicate>) so sub-queries inside predicates stay valid."""
    selects = []
    for r in rules_for(table):
        pred = _sql(r.name, run_ts)
        if r.scope == "table":
            counts = f"SELECT 1 AS rows_checked, CASE WHEN {pred} THEN 1 ELSE 0 END AS rows_failed"
        else:
            counts = (f"SELECT (SELECT COUNT(*) FROM raw_{table}) AS rows_checked, "
                      f"(SELECT COUNT(*) FROM raw_{table} WHERE {pred}) AS rows_failed")
        bad = "'FAIL'" if r.severity == CRITICAL else "'WARN'"
        selects.append(
            f"SELECT '{run_id}' AS run_id, '{dag_id}' AS dag_id, '{r.name}' AS check_name, "
            f"'{r.dimension}' AS dimension, '{r.severity}' AS severity, rows_checked, rows_failed, "
            f"CASE WHEN rows_failed = 0 THEN 'PASS' ELSE {bad} END AS status, now() AS checked_at "
            f"FROM ({counts})")
    body = "\nUNION ALL\n".join(selects)
    return f"INSERT INTO dq_results\n{body};"


def delete_previous_sql(table, run_id, dag_id):
    """Run before the INSERT so a re-run replaces its rows instead of duplicating them (idempotent)."""
    names = ", ".join(f"'{r.name}'" for r in rules_for(table))
    return f"DELETE FROM dq_results WHERE run_id = '{run_id}' AND dag_id = '{dag_id}' AND check_name IN ({names});"


def create_dq_results_sql():
    return DQ_RESULTS_DDL


def failure_reason_sql(table, run_ts):
    """SQL expression: '; '-joined names of the Critical rules the row breaks (NULL-safe, '' when none).
    Paste into a staging model as `... AS failure_reason`."""
    parts = [f"CASE WHEN {_sql(r.name, run_ts)} THEN '{r.name}' END"
             for r in rules_for(table) if r.severity == CRITICAL and r.scope == "row"]
    return "concat_ws('; ', " + ", ".join(parts) + ")"


def critical_predicate_sql(table, run_ts):
    parts = [f"COALESCE({_sql(r.name, run_ts)}, false)" for r in rules_for(table)
             if r.severity == CRITICAL and r.scope == "row"]
    return " OR ".join(parts)
