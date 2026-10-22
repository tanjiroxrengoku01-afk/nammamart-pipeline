"""Part D - NammaMart data-quality rules, Python side (used by the ETL DAG).

Every rule is defined ONCE here. dq_sql.py holds the matching SQL predicate for each
rule name (used by the ELT DAG), so both pipelines apply identical checks.

A rule's function returns True for each row that FAILS the check.
  Critical -> row is quarantined (removed from `passed`, added to `failed` with a reason)
  Warning  -> row still loads; the failure is only counted in dq_results

All rules run on the RAW text columns (read with dtype=str, keep_default_na=False), so no
rule depends on the order of cleaning steps. `run_ts` is the "as of" time for the
timeliness rules - see the note in the README/summary about choosing it.
"""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable

import pandas as pd

CRITICAL, WARNING = "Critical", "Warning"
COMPLETENESS, UNIQUENESS, VALIDITY = "Completeness", "Uniqueness", "Validity"
ACCURACY, CONSISTENCY, TIMELINESS = "Accuracy", "Consistency", "Timeliness"

TS_FMT = "%Y-%m-%d %H:%M:%S"
DATE_FMT = "%Y-%m-%d"
ORDER_STATUSES = ["DELIVERED", "CANCELLED", "RETURNED"]
PAYMENT_STATUSES = ["SUCCESS", "FAILED", "REFUNDED"]
PAYMENT_MODES = ["UPI", "Card", "Cash on Delivery", "Wallet"]
PAYMENT_MODE_MAP = {  # keys are lower-cased + trimmed raw values; 'Cashh' is deliberately NOT mapped
    "upi": "UPI", "card": "Card", "cash on delivery": "Cash on Delivery",
    "cod": "Cash on Delivery", "wallet": "Wallet",
}
LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$"
PHONE_RE = r"^[0-9]{10}$"
PINCODE_RE = r"^[0-9]{6}$"
AMOUNT_TOLERANCE = 0.011  # one paisa plus float slop


@dataclass(frozen=True)
class Rule:
    name: str
    table: str
    dimension: str
    severity: str
    description: str
    fn: Callable  # fn(df, ctx) -> bool Series (scope "row") or bool (scope "table")
    scope: str = "row"


# ---------------------------------------------------------------- helpers
def _t(df, col):
    """Trimmed text; NaN/None become ''."""
    return df[col].fillna("").astype(str).str.strip()


def _num(df, col):
    return pd.to_numeric(df[col], errors="coerce")


def _ts(df, col, fmt):
    return pd.to_datetime(df[col].fillna("").astype(str), format=fmt, errors="coerce")


def _mode_clean(df):
    """Standardised payment mode; NaN when the raw value cannot be mapped (e.g. 'Cashh')."""
    return _t(df, "payment_mode").str.lower().map(PAYMENT_MODE_MAP)


def _keep_first_dup(df, col):
    """True for every repeat of a key after its first occurrence (survivorship = first row)."""
    return df[col].duplicated(keep="first")


def _regex_fail(df, col, pattern):
    return ~_t(df, col).str.match(pattern)


def _orders_stale(df, ctx):
    ts = _ts(df, "order_ts", TS_FMT)
    ts = ts[ts <= ctx["run_ts"]]
    return bool(ts.empty or (ctx["run_ts"] - ts.max()) > pd.Timedelta(days=1))


def _payment_amount_mismatch(df, ctx):
    first_amount = ctx["orders"].drop_duplicates("order_id").set_index("order_id")["order_amount"]
    due = pd.to_numeric(_t(df, "order_id").map(first_amount), errors="coerce")
    return (_t(df, "payment_status").str.upper() == "SUCCESS") & ((_num(df, "amount_paid") - due).abs() > AMOUNT_TOLERANCE)


def _order_amount_mismatch(df, ctx):
    calc = (_num(df, "quantity") * _num(df, "unit_price") * (1 - _num(df, "discount_pct") / 100)).round(2)
    return ~((calc - _num(df, "order_amount")).abs() <= AMOUNT_TOLERANCE)


# ---------------------------------------------------------------- rules
R = Rule
RULES: list[Rule] = [
    # ---- orders
    R("orders_customer_id_not_null", "orders", COMPLETENESS, CRITICAL, "customer_id present",
      lambda d, c: _t(d, "customer_id") == ""),
    R("orders_delivered_has_delivery_minutes", "orders", COMPLETENESS, CRITICAL, "DELIVERED orders have delivery_minutes",
      lambda d, c: (_t(d, "order_status").str.upper() == "DELIVERED") & (_t(d, "delivery_minutes") == "")),
    R("orders_order_id_unique", "orders", UNIQUENESS, CRITICAL, "one row per order_id (first kept)",
      lambda d, c: _keep_first_dup(d, "order_id")),
    R("orders_order_status_valid", "orders", VALIDITY, CRITICAL, "status in DELIVERED/CANCELLED/RETURNED (ignoring case)",
      lambda d, c: ~_t(d, "order_status").str.upper().isin(ORDER_STATUSES)),
    R("orders_order_status_casing", "orders", VALIDITY, WARNING, "status is already upper-case and trimmed",
      lambda d, c: _t(d, "order_status").str.upper().isin(ORDER_STATUSES)
      & (d["order_status"].fillna("") != _t(d, "order_status").str.upper())),
    R("orders_payment_mode_standardised", "orders", VALIDITY, WARNING, "payment_mode already in canonical form",
      lambda d, c: ~d["payment_mode"].fillna("").isin(PAYMENT_MODES)),
    R("orders_payment_mode_valid_after_cleaning", "orders", VALIDITY, CRITICAL, "payment_mode maps to an allowed value",
      lambda d, c: _mode_clean(d).isna()),
    R("orders_order_ts_parses", "orders", VALIDITY, CRITICAL, "order_ts is YYYY-MM-DD HH:MM:SS",
      lambda d, c: _ts(d, "order_ts", TS_FMT).isna()),
    R("orders_discount_pct_0_100", "orders", VALIDITY, CRITICAL, "0 <= discount_pct <= 100",
      lambda d, c: (_num(d, "discount_pct") < 0) | (_num(d, "discount_pct") > 100)),
    R("orders_quantity_positive", "orders", ACCURACY, CRITICAL, "quantity > 0",
      lambda d, c: _num(d, "quantity") <= 0),
    R("orders_order_amount_matches_formula", "orders", ACCURACY, CRITICAL, "order_amount = qty x price x (1 - discount/100)",
      _order_amount_mismatch),
    R("orders_delivery_minutes_below_120", "orders", ACCURACY, CRITICAL, "delivery_minutes < 120",
      lambda d, c: _num(d, "delivery_minutes") >= 120),
    R("orders_customer_id_exists", "orders", CONSISTENCY, CRITICAL, "customer_id exists in customers (blank handled above)",
      lambda d, c: (_t(d, "customer_id") != "") & ~_t(d, "customer_id").isin(_t(c["customers"], "customer_id"))),
    R("orders_product_id_exists", "orders", CONSISTENCY, CRITICAL, "product_id exists in products",
      lambda d, c: ~_t(d, "product_id").isin(_t(c["products"], "product_id"))),
    R("orders_store_id_exists", "orders", CONSISTENCY, CRITICAL, "store_id exists in stores",
      lambda d, c: ~_t(d, "store_id").isin(_t(c["stores"], "store_id"))),
    R("orders_order_ts_not_future", "orders", TIMELINESS, CRITICAL, "order_ts <= run_ts",
      lambda d, c: _ts(d, "order_ts", TS_FMT) > c["run_ts"]),
    R("orders_latest_order_fresh", "orders", TIMELINESS, WARNING, "latest valid order is at most 1 day older than run_ts",
      _orders_stale, scope="table"),
    # ---- payments
    R("payments_payment_id_unique", "payments", UNIQUENESS, CRITICAL, "one row per payment_id (first kept)",
      lambda d, c: _keep_first_dup(d, "payment_id")),
    R("payments_gateway_ref_present_for_non_cod", "payments", COMPLETENESS, WARNING, "non-COD payments have a gateway_ref",
      lambda d, c: (_mode_clean(d).fillna("?") != "Cash on Delivery") & (_t(d, "gateway_ref") == "")),
    R("payments_payment_status_valid", "payments", VALIDITY, CRITICAL, "status in SUCCESS/FAILED/REFUNDED",
      lambda d, c: ~_t(d, "payment_status").str.upper().isin(PAYMENT_STATUSES)),
    R("payments_payment_mode_standardised", "payments", VALIDITY, WARNING, "payment_mode already in canonical form",
      lambda d, c: ~d["payment_mode"].fillna("").isin(PAYMENT_MODES)),
    R("payments_payment_mode_valid_after_cleaning", "payments", VALIDITY, CRITICAL, "payment_mode maps to an allowed value",
      lambda d, c: _mode_clean(d).isna()),
    R("payments_amount_paid_positive", "payments", ACCURACY, CRITICAL, "amount_paid > 0",
      lambda d, c: _num(d, "amount_paid") <= 0),
    R("payments_order_id_exists", "payments", CONSISTENCY, CRITICAL, "payment belongs to an existing order",
      lambda d, c: ~_t(d, "order_id").isin(_t(c["orders"], "order_id"))),
    R("payments_success_amount_matches_order", "payments", CONSISTENCY, WARNING,
      "SUCCESS amount_paid = order_amount (mismatches flow to the reconciliation table)", _payment_amount_mismatch),
    R("payments_payment_ts_not_future", "payments", TIMELINESS, CRITICAL, "payment_ts <= run_ts",
      lambda d, c: _ts(d, "payment_ts", TS_FMT) > c["run_ts"]),
    # ---- customers
    R("customers_customer_id_unique", "customers", UNIQUENESS, CRITICAL, "one row per customer_id (first kept)",
      lambda d, c: _keep_first_dup(d, "customer_id")),
    R("customers_city_not_null", "customers", COMPLETENESS, WARNING, "city present",
      lambda d, c: _t(d, "city") == ""),
    R("customers_email_valid", "customers", VALIDITY, WARNING, "email format",
      lambda d, c: _regex_fail(d, "email", EMAIL_RE)),
    R("customers_phone_10_digits", "customers", VALIDITY, WARNING, "phone is 10 digits",
      lambda d, c: _regex_fail(d, "phone", PHONE_RE)),
    R("customers_pincode_6_digits", "customers", VALIDITY, WARNING, "pincode is 6 digits",
      lambda d, c: _regex_fail(d, "pincode", PINCODE_RE)),
    R("customers_loyalty_tier_casing", "customers", CONSISTENCY, WARNING, "loyalty_tier uses canonical casing",
      lambda d, c: ~d["loyalty_tier"].fillna("").isin(LOYALTY_TIERS)),
    R("customers_signup_date_not_future", "customers", TIMELINESS, CRITICAL, "signup_date <= run_ts",
      lambda d, c: _ts(d, "signup_date", DATE_FMT) > c["run_ts"]),
    # ---- products
    R("products_product_id_unique", "products", UNIQUENESS, CRITICAL, "one row per product_id (first kept)",
      lambda d, c: _keep_first_dup(d, "product_id")),
    R("products_category_not_null", "products", COMPLETENESS, CRITICAL, "category present",
      lambda d, c: _t(d, "category") == ""),
    R("products_mrp_positive", "products", ACCURACY, CRITICAL, "mrp > 0",
      lambda d, c: _num(d, "mrp") <= 0),
    R("products_cost_price_le_mrp", "products", ACCURACY, CRITICAL, "cost_price <= mrp",
      lambda d, c: _num(d, "cost_price") > _num(d, "mrp")),
    # ---- stores
    R("stores_store_id_unique", "stores", UNIQUENESS, CRITICAL, "one row per store_id (first kept)",
      lambda d, c: _keep_first_dup(d, "store_id")),
    R("stores_zone_not_null", "stores", COMPLETENESS, CRITICAL, "zone present",
      lambda d, c: _t(d, "zone") == ""),
    R("stores_capacity_positive", "stores", ACCURACY, WARNING, "capacity_orders_per_day > 0",
      lambda d, c: _num(d, "capacity_orders_per_day") <= 0),
]


def rules_for(table: str) -> list[Rule]:
    return [r for r in RULES if r.table == table]


# ---------------------------------------------------------------- engine
RESULT_COLUMNS = ["run_id", "dag_id", "check_name", "dimension", "severity",
                  "rows_checked", "rows_failed", "status", "checked_at"]


def build_context(raw: dict, run_ts) -> dict:
    """raw = {'orders': df, 'payments': df, 'customers': df, 'products': df, 'stores': df} (raw text).
    Cross-table rules look up the RAW master tables, so run them before any cleaning."""
    return {**raw, "run_ts": pd.Timestamp(run_ts)}


def run_rules(table, df, ctx, run_id, dag_id, checked_at=None):
    """Apply every rule for `table`. Returns (passed_df, failed_df, results_df).

    failed_df = rows with at least one Critical failure, plus a `failure_reason` column listing the
    rule names ('; '-separated). passed_df keeps everything else, including Warning failures.
    results_df has one row per rule and matches the dq_results table."""
    df = df.reset_index(drop=True)
    checked_at = pd.Timestamp(checked_at) if checked_at is not None else pd.Timestamp.now()
    reasons, results = defaultdict(list), []
    for rule in rules_for(table):
        out = rule.fn(df, ctx)
        if rule.scope == "table":
            checked, failed = 1, int(bool(out))
        else:
            mask = pd.Series(out, index=df.index).fillna(False).astype(bool)
            checked, failed = len(df), int(mask.sum())
            if rule.severity == CRITICAL:
                for i in df.index[mask]:
                    reasons[i].append(rule.name)
        status = "PASS" if failed == 0 else ("FAIL" if rule.severity == CRITICAL else "WARN")
        results.append([run_id, dag_id, rule.name, rule.dimension, rule.severity, checked, failed, status, checked_at])
    failed_df = df.loc[sorted(reasons)].copy()
    failed_df["failure_reason"] = ["; ".join(reasons[i]) for i in failed_df.index]
    passed_df = df.drop(index=list(reasons)).reset_index(drop=True)
    return passed_df, failed_df.reset_index(drop=True), pd.DataFrame(results, columns=RESULT_COLUMNS)


# ---------------------------------------------------------------- output helpers
DQ_RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR,
    rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP)
"""


def write_dq_results(con, results_df):
    """Idempotent: re-running the same run_id/dag_id/check replaces its row instead of duplicating it."""
    con.execute(DQ_RESULTS_DDL)
    con.register("_dq_incoming", results_df)
    con.execute("""DELETE FROM dq_results WHERE EXISTS (
        SELECT 1 FROM _dq_incoming i WHERE i.run_id = dq_results.run_id
          AND i.dag_id = dq_results.dag_id AND i.check_name = dq_results.check_name)""")
    cols = ", ".join(RESULT_COLUMNS)
    con.execute(f"INSERT INTO dq_results ({cols}) SELECT {cols} FROM _dq_incoming")
    con.unregister("_dq_incoming")


def write_quarantine(failed_df, table, run_id, learner_name, base_dir="/usr/local/airflow/include/quarantine"):
    """Write quarantined rows to <base_dir>/<learner>/<table>_failed_<run_id>.csv (None if nothing failed).
    The run_id is sanitised because Airflow run_ids contain ':' which breaks file names on Windows mounts."""
    if failed_df.empty:
        return None
    import os
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", str(run_id))
    folder = os.path.join(base_dir, learner_name)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{table}_failed_{safe}.csv")
    failed_df.to_csv(path, index=False)
    return path
