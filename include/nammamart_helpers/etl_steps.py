"""Part A - NammaMart ETL building blocks (pandas side + DuckDB load helpers).

The ETL DAG (dags/nammamart_etl_<name>.py) is a thin wrapper around these functions, so each step
can be tested outside Airflow. Order of work:  read_source -> validate_all -> transform_all ->
load_* -> reconcile -> REPORT_SQL.
"""
from __future__ import annotations

import os
import re

import numpy as np
import pandas as pd

from .quality_rules import (PAYMENT_MODE_MAP, RESULT_COLUMNS, build_context,
                            run_rules, write_dq_results)

TABLES = ["orders", "payments", "customers", "products", "stores"]
KEYS = {"orders": "order_id", "payments": "payment_id", "customers": "customer_id",
        "products": "product_id", "stores": "store_id"}
EXPECTED_COLUMNS = {
    "orders": "order_id,order_ts,customer_id,store_id,product_id,quantity,unit_price,discount_pct,order_amount,payment_mode,order_status,channel,delivery_minutes",
    "payments": "payment_id,order_id,payment_ts,payment_mode,amount_paid,payment_status,gateway_ref",
    "customers": "customer_id,full_name,email,phone,city,area,pincode,signup_date,loyalty_tier,is_active",
    "products": "product_id,product_name,category,sub_category,brand,mrp,cost_price,unit,is_perishable,launch_date",
    "stores": "store_id,store_name,zone,area,pincode,open_date,manager_name,capacity_orders_per_day",
}
EXPECTED_COLUMNS = {k: v.split(",") for k, v in EXPECTED_COLUMNS.items()}
TOL = 0.011


# ================================================================ extract
def read_source(path, table):
    """Read a CSV as text (nothing is converted) and fail LOUDLY if columns are missing."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]
    missing = [c for c in EXPECTED_COLUMNS[table] if c not in df.columns]
    if missing:
        raise ValueError(f"{os.path.basename(path)}: missing expected columns {missing}; found {list(df.columns)}")
    df["_source_file"] = os.path.basename(path)
    df["_source_row"] = range(2, len(df) + 2)  # 1-based file line (line 1 is the header)
    return df


def apply_increment(orders, increment):
    """Rows in the increment replace older rows with the same order_id (late status changes);
    the remaining rows are appended. Duplicates INSIDE one file are kept so validation can catch them."""
    keep = orders[~orders["order_id"].str.strip().isin(increment["order_id"].str.strip())]
    return pd.concat([keep, increment], ignore_index=True)


# ================================================================ validate
def mask_email(s):
    def f(v):
        v = str(v).strip()
        if not v:
            return ""
        if "@" not in v:
            return "***"
        local, _, domain = v.partition("@")
        return local[:2] + "***@" + domain
    return s.fillna("").map(f)


def mask_phone(s):
    def f(v):
        d = re.sub(r"\D", "", str(v))
        if not d:
            return ""
        return "*" * len(d) if len(d) <= 4 else d[:2] + "*" * (len(d) - 4) + d[-2:]
    return s.fillna("").map(f)


# A dimension row that fails a Critical rule is quarantined. If a clean order still points at it, the row is
# loaded in REPAIRED form (the failed attribute blanked/'Unknown', dim_status='REPAIRED') so the order keeps its
# revenue and the star schema stays complete. Orders are never dropped because of a dimension defect.
REPAIR = {
    "stores_zone_not_null": {"zone": "Unknown"},
    "products_category_not_null": {"category": "Unknown"},
    "products_mrp_positive": {"mrp": pd.NA},
    "products_cost_price_le_mrp": {"cost_price": pd.NA},
    "customers_signup_date_not_future": {"signup_date": pd.NaT},
}
DIM_SPECS = [  # (name, source table, orders column, key column)
    ("customer", "customers", "customer_id", "customer_id"),
    ("product", "products", "product_id", "product_id"),
    ("store", "stores", "store_id", "store_id"),
]


def keys_to_repair(orders_passed, dim_passed, col, key):
    return sorted(set(orders_passed[col].str.strip()) - set(dim_passed[key].str.strip()))


def repair_check_results(passed, run_id, dag_id, checked_at):
    """Warning checks: how many clean orders point at a dimension row that had to be repaired."""
    rows = []
    for name, table, col, key in DIM_SPECS:
        bad = set(keys_to_repair(passed["orders"], passed[table], col, key))
        n = int(passed["orders"][col].str.strip().isin(bad).sum())
        rows.append([run_id, dag_id, f"orders_{name}_dimension_repaired", "Consistency", "Warning",
                     len(passed["orders"]), n, "PASS" if n == 0 else "WARN", checked_at])
    return pd.DataFrame(rows, columns=RESULT_COLUMNS)


def validate_all(raw, as_of, run_id, dag_id):
    """raw = {table: raw text df}. Returns (passed, failed, results_df, rejected_df)."""
    checked_at = pd.Timestamp.now()
    ctx = build_context(raw, as_of)
    passed, failed, results = {}, {}, []
    for t in ["stores", "products", "customers", "orders", "payments"]:
        p, f, r = run_rules(t, raw[t], ctx, run_id, dag_id, checked_at)
        passed[t], failed[t] = p, f
        results.append(r)
    results.append(repair_check_results(passed, run_id, dag_id, checked_at))
    rej = []
    for t, f in failed.items():
        if f.empty:
            continue
        r = pd.DataFrame({"run_id": run_id, "dag_id": dag_id, "source_file": f["_source_file"].values,
                          "table_name": t, "row_key": f[KEYS[t]].values,
                          "source_row": pd.to_numeric(f["_source_row"]).astype("Int64").values,
                          "failure_reason": f["failure_reason"].str.split("; ").values}).explode("failure_reason")
        rej.append(r)
    rejected = pd.concat(rej, ignore_index=True) if rej else pd.DataFrame(columns=REJECTED_COLUMNS[:-1])
    rejected["rejected_at"] = checked_at
    return passed, failed, pd.concat(results, ignore_index=True), rejected[REJECTED_COLUMNS]


REJECTED_COLUMNS = ["run_id", "dag_id", "source_file", "table_name", "row_key", "source_row", "failure_reason", "rejected_at"]


def quarantine_copy(table, failed_df):
    """Quarantine files must not leak personal data: mask email/phone before writing."""
    f = failed_df.copy()
    if table == "customers" and not f.empty:
        f["email"], f["phone"] = mask_email(f["email"]), mask_phone(f["phone"])
    return f


# ================================================================ transform
def _strip_all(df):
    return df.apply(lambda s: s.fillna("").astype(str).str.strip())


def _mode(s):
    return s.str.lower().map(PAYMENT_MODE_MAP)


def _f(s):
    return pd.to_numeric(s, errors="coerce").astype("Float64")


def _date(s):
    return pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")


def standardise_orders(df):
    d = _strip_all(df.drop(columns=["_source_file", "_source_row"], errors="ignore"))
    d["order_status"] = d["order_status"].str.upper()
    d["payment_mode"] = _mode(d["payment_mode"])
    d["channel"] = d["channel"].str.title()
    d["order_ts"] = pd.to_datetime(d["order_ts"], format="%Y-%m-%d %H:%M:%S")
    d["order_date"] = d["order_ts"].dt.date
    d["quantity"] = pd.to_numeric(d["quantity"]).astype("Int64")
    for c in ["unit_price", "discount_pct", "order_amount", "delivery_minutes"]:
        d[c] = _f(d[c])
    return d.drop_duplicates("order_id", keep="first").reset_index(drop=True)


def standardise_customers(df):
    d = _strip_all(df.drop(columns=["_source_file", "_source_row"], errors="ignore"))
    d["loyalty_tier"] = d["loyalty_tier"].str.title()
    d["email_masked"], d["phone_masked"] = mask_email(d["email"]), mask_phone(d["phone"])
    d["signup_date"] = _date(d["signup_date"])
    return (d.drop(columns=["email", "phone"]).drop_duplicates("customer_id", keep="first").reset_index(drop=True))


def standardise_products(df):
    d = _strip_all(df.drop(columns=["_source_file", "_source_row"], errors="ignore"))
    d["mrp"], d["cost_price"] = _f(d["mrp"]), _f(d["cost_price"])
    d["is_perishable"] = d["is_perishable"].str.lower() == "true"
    d["launch_date"] = _date(d["launch_date"])
    return d.drop_duplicates("product_id", keep="first").reset_index(drop=True)


def standardise_stores(df):
    d = _strip_all(df.drop(columns=["_source_file", "_source_row"], errors="ignore"))
    d["capacity_orders_per_day"] = pd.to_numeric(d["capacity_orders_per_day"]).astype("Int64")
    d["open_date"] = _date(d["open_date"])
    return d.drop_duplicates("store_id", keep="first").reset_index(drop=True)


def standardise_payments(df):
    d = _strip_all(df.drop(columns=["_source_file", "_source_row"], errors="ignore"))
    d["payment_status"] = d["payment_status"].str.upper()
    d["payment_mode"] = _mode(d["payment_mode"])
    d["amount_paid"] = pd.to_numeric(d["amount_paid"], errors="coerce")
    return d.drop_duplicates("payment_id", keep="first").reset_index(drop=True)


def _repaired_dim(std_fn, passed_dim, failed_dim, key, keys):
    """Standardised dimension rows for `keys` that only exist in quarantine, with the failed attribute blanked."""
    f = failed_dim[failed_dim[key].str.strip().isin(keys)].drop_duplicates(key, keep="first").reset_index(drop=True)
    if f.empty:
        return std_fn(passed_dim.iloc[0:0]).assign(dim_status="REPAIRED")
    reasons = f["failure_reason"].str.split("; ").tolist()
    d = std_fn(f.drop(columns=["failure_reason"]))
    for i, rs in enumerate(reasons):
        for r in rs:
            for col, val in REPAIR.get(r, {}).items():
                d.at[i, col] = val
    d["dim_status"] = "REPAIRED"
    return d


def transform_all(passed, failed, run_id, loaded_at):
    """passed/failed = validated raw frames. Returns the star schema (3 dimensions + order fact)."""
    std = {"customers": standardise_customers, "products": standardise_products, "stores": standardise_stores}
    dims = {}
    for name, table, col, key in DIM_SPECS:
        ok = std[table](passed[table]).assign(dim_status="OK")
        fix = _repaired_dim(std[table], passed[table], failed[table], key, keys_to_repair(passed["orders"], passed[table], col, key))
        dims[name] = pd.concat([ok, fix], ignore_index=True) if len(fix) else ok
    o = standardise_orders(passed["orders"])
    f = o.merge(dims["product"][["product_id", "cost_price"]], on="product_id", how="left", validate="m:1")
    delivered = f["order_status"] == "DELIVERED"
    f["net_revenue"] = f["order_amount"].where(delivered, 0.0).astype("Float64")  # only DELIVERED counts as revenue
    f["gross_margin"] = (f["net_revenue"] - f["quantity"] * f["cost_price"]).where(delivered, 0.0).astype("Float64").round(2)
    f["within_15_min"] = (f["delivery_minutes"] <= 15).astype("Int64").where(delivered)
    f["loaded_run_id"], f["loaded_at"] = run_id, loaded_at
    fact = f[["order_id", "order_ts", "order_date", "customer_id", "store_id", "product_id", "quantity", "unit_price",
              "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes",
              "net_revenue", "gross_margin", "within_15_min", "loaded_run_id", "loaded_at"]]
    return {"fact": fact, "dim_customer": dims["customer"], "dim_product": dims["product"], "dim_store": dims["store"]}


# ================================================================ reconcile
def reconcile(fact, payments, run_id, reconciled_at):
    """One row per mismatch between the CLEAN orders in this batch and ALL payments in payments.csv.
    Expected per order status: DELIVERED -> one SUCCESS payment = order_amount; RETURNED -> a REFUNDED payment
    = order_amount; CANCELLED -> no SUCCESS payment. Payments for orders not in the clean set are listed too."""
    p = payments
    o = fact[["order_id", "order_status", "order_amount"]].drop_duplicates("order_id").copy()
    o["order_amount"] = o["order_amount"].astype(float)
    for tag, status in [("s", "SUCCESS"), ("f", "FAILED"), ("r", "REFUNDED")]:
        g = p[p["payment_status"] == status].groupby("order_id").agg(
            **{f"{tag}_n": ("payment_id", "count"), f"{tag}_amt": ("amount_paid", "sum"), f"{tag}_pid": ("payment_id", "first")})
        o = o.merge(g, left_on="order_id", right_index=True, how="left")
    for c in ["s_n", "f_n", "r_n", "s_amt", "r_amt"]:
        o[c] = o[c].fillna(0)
    amt, D, R, C = o["order_amount"], o["order_status"] == "DELIVERED", o["order_status"] == "RETURNED", o["order_status"] == "CANCELLED"
    parts = []

    def add(mask, reason, diff, paid, pid, detail):
        if mask.any():
            x = o[mask]
            parts.append(pd.DataFrame({
                "order_id": x["order_id"], "payment_id": pid[mask].replace("", np.nan) if hasattr(pid, "replace") else pid,
                "order_status": x["order_status"], "order_amount": x["order_amount"],
                "amount_paid": paid[mask], "difference": diff[mask], "reason": reason[mask] if hasattr(reason, "__getitem__") and not isinstance(reason, str) else reason,
                "detail": detail}))

    nopay = D & (o["s_n"] == 0)
    add(nopay, pd.Series(np.select([o["f_n"] > 0, o["r_n"] > 0], ["PAYMENT_FAILED", "DELIVERED_BUT_REFUNDED"], "NO_PAYMENT_RECORD"), index=o.index),
        -amt, pd.Series(np.nan, index=o.index), o["f_pid"].fillna(o["r_pid"]).fillna(""), "delivered order has no SUCCESS payment")
    add(D & (o["s_n"] > 1), "DUPLICATE_SUCCESS_PAYMENT", o["s_amt"] - amt, o["s_amt"], o["s_pid"].fillna(""), "more than one SUCCESS payment")
    add(D & (o["s_n"] == 1) & ((o["s_amt"] - amt).abs() > TOL), "AMOUNT_MISMATCH", o["s_amt"] - amt, o["s_amt"], o["s_pid"].fillna(""), "SUCCESS amount differs from order_amount")
    add(R & (o["r_n"] == 0), pd.Series(np.where(o["s_n"] > 0, "RETURNED_NOT_REFUNDED", "RETURNED_NO_PAYMENT_RECORD"), index=o.index),
        o["s_amt"], o["s_amt"].where(o["s_n"] > 0), o["s_pid"].fillna(""), "returned order has no REFUNDED payment")
    add(R & (o["r_n"] >= 1) & ((o["r_amt"] - amt).abs() > TOL), "REFUND_AMOUNT_MISMATCH", o["r_amt"] - amt, o["r_amt"], o["r_pid"].fillna(""), "REFUNDED amount differs from order_amount")
    add(C & (o["s_n"] > 0), "PAYMENT_FOR_CANCELLED_ORDER", o["s_amt"], o["s_amt"], o["s_pid"].fillna(""), "SUCCESS payment on a cancelled order")
    orphan = p[~p["order_id"].isin(fact["order_id"])]
    if len(orphan):
        parts.append(pd.DataFrame({
            "order_id": orphan["order_id"].values, "payment_id": orphan["payment_id"].values, "order_status": None,
            "order_amount": np.nan, "amount_paid": orphan["amount_paid"].values,
            "difference": np.where(orphan["payment_status"] == "SUCCESS", orphan["amount_paid"], 0.0),
            "reason": "ORDER_NOT_IN_CLEAN_ORDERS", "detail": "order unknown, or its order row was quarantined"}))
    cols = ["run_id", "order_id", "payment_id", "order_status", "order_amount", "amount_paid", "difference",
            "unreconciled_amount", "reason", "detail", "reconciled_at"]
    if not parts:
        return pd.DataFrame(columns=cols)
    r = pd.concat(parts, ignore_index=True)
    r["difference"] = r["difference"].astype(float).round(2)
    r["unreconciled_amount"] = r["difference"].abs()
    r["run_id"], r["reconciled_at"] = run_id, reconciled_at
    return r[cols]


# ================================================================ warehouse (DuckDB)
DDL = {
    "etl_rejected_rows": """CREATE TABLE IF NOT EXISTS etl_rejected_rows (run_id VARCHAR, dag_id VARCHAR, source_file VARCHAR,
        table_name VARCHAR, row_key VARCHAR, source_row BIGINT, failure_reason VARCHAR, rejected_at TIMESTAMP)""",
    "etl_payment_reconciliation": """CREATE TABLE IF NOT EXISTS etl_payment_reconciliation (run_id VARCHAR, order_id VARCHAR,
        payment_id VARCHAR, order_status VARCHAR, order_amount DOUBLE, amount_paid DOUBLE, difference DOUBLE,
        unreconciled_amount DOUBLE, reason VARCHAR, detail VARCHAR, reconciled_at TIMESTAMP)""",
}


def _table_exists(con, table):
    return con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table]).fetchall()[0][0] > 0


def upsert(con, table, df, key, also_delete=()):
    """Idempotent load: delete the keys being loaded (plus `also_delete`), then insert. Returns (inserted, updated).
    INSERT ... BY NAME fails loudly if a column is missing or unexpected, so schema drift cannot load silently."""
    con.register("_stage", df)
    try:
        if not _table_exists(con, table):
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM _stage")
            return len(df), 0
        updated = con.execute(f"SELECT COUNT(*) FROM {table} WHERE {key} IN (SELECT {key} FROM _stage)").fetchall()[0][0]
        if also_delete:
            con.register("_gone", pd.DataFrame({key: list(also_delete)}))
            con.execute(f"DELETE FROM {table} WHERE {key} IN (SELECT {key} FROM _gone)")
            con.unregister("_gone")
        con.execute(f"DELETE FROM {table} WHERE {key} IN (SELECT {key} FROM _stage)")
        con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM _stage")
        return len(df) - updated, updated
    finally:
        con.unregister("_stage")


def load_star(con, frames, quarantined_order_ids, run_id):
    """Dims first, then the fact, in ONE transaction. Quarantined order ids that are not also loaded are removed
    from the fact so a row that turns bad never stays in reporting. Guards fail the run if the star is broken."""
    out = {}
    con.execute("BEGIN")
    try:
        for name, key in [("dim_customer", "customer_id"), ("dim_product", "product_id"), ("dim_store", "store_id")]:
            out[name] = upsert(con, f"etl_{name}", frames[name], key)
        loaded = set(frames["fact"]["order_id"])
        gone = [i for i in set(quarantined_order_ids) if i not in loaded]
        out["fact_orders"] = upsert(con, "etl_fact_orders", frames["fact"], "order_id", also_delete=gone)
        dup = con.execute("SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM etl_fact_orders").fetchall()[0][0]
        orphan = con.execute("""SELECT COUNT(*) FROM etl_fact_orders f
            LEFT JOIN etl_dim_customer c USING (customer_id) LEFT JOIN etl_dim_product p USING (product_id)
            LEFT JOIN etl_dim_store s USING (store_id)
            WHERE c.customer_id IS NULL OR p.product_id IS NULL OR s.store_id IS NULL""").fetchall()[0][0]
        if dup or orphan:
            raise RuntimeError(f"load guard failed: {dup} duplicate order_ids, {orphan} facts without a dimension row")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    out["fact_total"] = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchall()[0][0]
    return out


def write_rejected(con, rejected_df, run_id, dag_id):
    con.execute(DDL["etl_rejected_rows"])
    con.execute("DELETE FROM etl_rejected_rows WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
    if len(rejected_df):
        con.register("_rej", rejected_df)
        con.execute("INSERT INTO etl_rejected_rows BY NAME SELECT * FROM _rej")
        con.unregister("_rej")


def write_reconciliation(con, rec_df, batch_order_ids):
    """Latest state per order: replace the rows of every order/payment order_id seen in this batch."""
    con.execute(DDL["etl_payment_reconciliation"])
    ids = pd.DataFrame({"order_id": sorted(set(batch_order_ids) | set(rec_df["order_id"]))})
    con.register("_ids", ids)
    con.execute("DELETE FROM etl_payment_reconciliation WHERE order_id IN (SELECT order_id FROM _ids)")
    con.unregister("_ids")
    if len(rec_df):
        con.register("_rec", rec_df)
        con.execute("INSERT INTO etl_payment_reconciliation BY NAME SELECT * FROM _rec")
        con.unregister("_rec")

REPORT_SQL = {
    "etl_daily_zone_revenue": """
        CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
        SELECT f.order_date, s.zone, s.store_id, s.store_name,
               COUNT(*) AS delivered_orders, ROUND(SUM(f.net_revenue), 2) AS net_revenue
        FROM etl_fact_orders f JOIN etl_dim_store s ON f.store_id = s.store_id
        WHERE f.order_status = 'DELIVERED'
        GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3""",
    "etl_category_margin": """
        CREATE OR REPLACE TABLE etl_category_margin AS
        SELECT p.category, COUNT(*) AS delivered_orders, SUM(f.quantity) AS units,
               ROUND(SUM(f.net_revenue), 2) AS revenue, ROUND(SUM(f.gross_margin), 2) AS gross_margin,
               ROUND(100 * SUM(f.gross_margin) / NULLIF(SUM(CASE WHEN f.gross_margin IS NOT NULL THEN f.net_revenue END), 0), 2) AS margin_pct
        FROM etl_fact_orders f JOIN etl_dim_product p ON f.product_id = p.product_id
        WHERE f.order_status = 'DELIVERED'
        GROUP BY 1 ORDER BY revenue DESC""",
}