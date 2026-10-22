"""NammaMart ETL pipeline: Extract -> Validate -> Transform -> Load -> Reconcile -> Report.

Bad rows are stopped BEFORE the warehouse: validated in Python (include/nammamart_helpers/quality_rules.py),
quarantined to include/quarantine/<name>/<table>_failed_<run_id>.csv, and only clean, standardised, masked data
is loaded into DuckDB as a star schema (etl_fact_orders + etl_dim_customer / etl_dim_product / etl_dim_store).

Trigger with config to run the experiments, e.g. {"increment_file": "orders_2026-10-01_increment.csv"}.
Params: data_dir, increment_file ('' = none), as_of ('' = now, else 'YYYY-MM-DD HH:MM:SS' used by the timeliness rules).
Intermediate files go to the container's temp folder (never into the repo) because they hold raw customer data.
"""
from __future__ import annotations

import os
import re
import tempfile
from datetime import timedelta

import pandas as pd
import pendulum
from airflow.decorators import dag, task
try:  # Airflow 3
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:  # Airflow 2
    from airflow.exceptions import AirflowFailException

try:  # Airflow 3
    from airflow.sdk import get_current_context
except ImportError:  # Airflow 2
    from airflow.operators.python import get_current_context

from include.nammamart_helpers import etl_steps as es
from include.nammamart_helpers import observability as obs
from include.nammamart_helpers.quality_rules import write_dq_results, write_quarantine

LEARNER = "slick"  # <- change to your own name; it names the DAG, warehouse file and quarantine folder
DAG_ID = f"nammamart_etl_{LEARNER}"
PROJECT = "/usr/local/airflow"
WAREHOUSE = f"{PROJECT}/include/warehouse_{LEARNER}.duckdb"
QUARANTINE_DIR = f"{PROJECT}/include/quarantine"
PARAMS = {
    "data_dir": f"{PROJECT}/include/data/nammamart",
    "increment_file": "",
    "as_of": "2026-10-01 23:59:59",
}
DEFAULT_ARGS = {  # Part C
    "owner": LEARNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}


def _run():
    ctx = get_current_context()
    return ctx["run_id"], ctx["params"]


def _work_dir(run_id):
    path = os.path.join(tempfile.gettempdir(), f"nammamart_{LEARNER}", re.sub(r"[^A-Za-z0-9_.-]", "_", run_id))
    os.makedirs(path, exist_ok=True)
    return path


def _as_of(params):
    return (params.get("as_of") or "").strip() or pendulum.now("Asia/Kolkata").format("YYYY-MM-DD HH:mm:ss")


def _connect():
    return obs.connect_warehouse(WAREHOUSE)  # waits if the ELT DAG holds the DuckDB lock


def _make_extract(table):
    @task(task_id=f"extract_{table}")
    def extract():
        run_id, params = _run()
        df = es.read_source(os.path.join(params["data_dir"], f"{table}.csv"), table)
        rows_read = len(df)
        if table == "orders" and (params.get("increment_file") or "").strip():
            inc = es.read_source(os.path.join(params["data_dir"], params["increment_file"].strip()), "orders")
            rows_read += len(inc)
            df = es.apply_increment(df, inc)
            print(f"increment applied: {len(inc)} rows from {params['increment_file']}")
        path = os.path.join(_work_dir(run_id), f"raw_{table}.pkl")
        df.to_pickle(path)
        print(f"extract_{table}: {rows_read} rows read, {len(df)} rows staged")
        return {"table": table, "path": path, "rows_read": rows_read, "rows": len(df)}
    return extract()


@task
def validate(extracts):
    run_id, params = _run()
    work = _work_dir(run_id)
    raw = {e["table"]: pd.read_pickle(e["path"]) for e in extracts}
    passed, failed, results, rejected = es.validate_all(raw, _as_of(params), run_id, DAG_ID)
    summary, files = {}, {}
    for t in es.TABLES:
        files[t] = write_quarantine(es.quarantine_copy(t, failed[t]), t, run_id, LEARNER, base_dir=QUARANTINE_DIR)
        summary[t] = {"rows_in": len(raw[t]), "passed": len(passed[t]), "quarantined": len(failed[t])}
        print(f"validate {t}: in={len(raw[t])} passed={len(passed[t])} quarantined={len(failed[t])} file={files[t]}")
    con = _connect()
    try:
        write_dq_results(con, results)
        es.write_rejected(con, rejected, run_id, DAG_ID)
        obs.report_volume(con, run_id, DAG_ID, len(raw["orders"]))
    finally:
        con.close()
    print(results[results["rows_failed"] > 0][["check_name", "severity", "rows_failed", "status"]].to_string(index=False))
    pd.to_pickle({"passed": passed, "failed": failed}, os.path.join(work, "validated.pkl"))
    return {"validated_path": os.path.join(work, "validated.pkl"), "raw_payments_path": raw_path(extracts, "payments"),
            "summary": summary, "quarantine_files": files}


def raw_path(extracts, table):
    return next(e["path"] for e in extracts if e["table"] == table)


@task
def transform(v):
    run_id, _ = _run()
    v_ = pd.read_pickle(v["validated_path"])
    frames = es.transform_all(v_["passed"], v_["failed"], run_id, pd.Timestamp.now())
    path = os.path.join(_work_dir(run_id), "star.pkl")
    pd.to_pickle(frames, path)
    for k, d in frames.items():
        print(f"transform {k}: {len(d)} rows")
    repaired = {k: int((d["dim_status"] == "REPAIRED").sum()) for k, d in frames.items() if "dim_status" in d}
    print("repaired dimension members:", repaired)
    return {"star_path": path, "fact_rows": len(frames["fact"])}


@task
def load(t, v):
    run_id, _ = _run()
    frames = pd.read_pickle(t["star_path"])
    failed = pd.read_pickle(v["validated_path"])["failed"]
    quarantined_ids = failed["orders"]["order_id"].str.strip().tolist()
    con = _connect()
    try:
        out = es.load_star(con, frames, quarantined_ids, run_id)
    finally:
        con.close()
    for k, val in out.items():
        print(f"load {k}: {val}  (inserted, updated)" if isinstance(val, (list, tuple)) else f"load {k}: {val}")
    return {k: (list(val) if isinstance(val, (list, tuple)) else val) for k, val in out.items()}


@task
def reconcile_payments(t, v, loaded):  # `loaded` only makes this run after the load
    run_id, _ = _run()
    fact = pd.read_pickle(t["star_path"])["fact"]
    pay = es.standardise_payments(pd.read_pickle(v["raw_payments_path"]))
    rec = es.reconcile(fact, pay, run_id, pd.Timestamp.now())
    con = _connect()
    try:
        es.write_reconciliation(con, rec, set(fact["order_id"]) | set(pay["order_id"]))
    finally:
        con.close()
    by_reason = rec.groupby("reason").agg(rows=("order_id", "count"), unreconciled=("unreconciled_amount", "sum")).round(2)
    print(by_reason.to_string())
    total = round(float(rec["unreconciled_amount"].sum()), 2)
    print(f"TOTAL unreconciled amount: {total}")
    return {"mismatch_rows": len(rec), "unreconciled_amount": total}


@task
def report(rec):
    run_id, _ = _run()
    con = _connect()
    try:
        for name, sql in es.REPORT_SQL.items():
            con.execute(sql)
        print("== Net revenue by zone (DELIVERED, after discount) ==")
        print(con.execute("SELECT zone, SUM(delivered_orders) AS orders, ROUND(SUM(net_revenue),2) AS net_revenue "
                          "FROM etl_daily_zone_revenue GROUP BY 1 ORDER BY 3 DESC").df().to_string(index=False))
        print("== Last 5 days by zone ==")
        print(con.execute("SELECT order_date, zone, ROUND(SUM(net_revenue),2) AS net_revenue FROM etl_daily_zone_revenue "
                          "WHERE order_date >= (SELECT MAX(order_date) - INTERVAL 4 DAY FROM etl_daily_zone_revenue) "
                          "GROUP BY 1, 2 ORDER BY 1, 2").df().to_string(index=False))
        print("== Category revenue and gross margin ==")
        print(con.execute("SELECT * FROM etl_category_margin").df().to_string(index=False))
        print("== Rows rejected this run, by file and reason ==")
        print(con.execute("SELECT source_file, failure_reason, COUNT(*) AS rows FROM etl_rejected_rows WHERE run_id = ? "
                          "GROUP BY 1, 2 ORDER BY 1, 3 DESC", [run_id]).df().to_string(index=False))
    finally:
        con.close()
    return {"reports": list(es.REPORT_SQL)}


@task(trigger_rule="all_done", retries=0)
def audit_run():
    """Runs even when something upstream failed: one row per run in pipeline_runs, then fail the run if needed."""
    ctx = get_current_context()
    ti, dag_run = ctx["ti"], ctx["dag_run"]
    states = {k: v for k, v in obs.task_states(ti, DAG_ID, dag_run.run_id).items() if k != ti.task_id}
    failed = sorted(k for k, v in states.items() if str(v) in obs.FAILED_STATES)
    extracts = [ti.xcom_pull(task_ids=f"extract_{t}") for t in es.TABLES]
    validated = ti.xcom_pull(task_ids="validate") or {}
    loaded = ti.xcom_pull(task_ids="load") or {}
    reported = ti.xcom_pull(task_ids="report")
    status = "FAILED" if failed or not reported else "SUCCESS"
    row = {"run_id": dag_run.run_id, "dag_id": DAG_ID, "start_time": dag_run.start_date,
           "end_time": pendulum.now("UTC"),
           "rows_extracted": sum(e["rows_read"] for e in extracts if e) if any(extracts) else None,
           "rows_loaded": sum(loaded["fact_orders"]) if "fact_orders" in loaded else None,
           "rows_quarantined": sum(v["quarantined"] for v in validated["summary"].values()) if validated else None,
           "status": status, "failed_tasks": ", ".join(failed) or None}
    con = _connect()
    try:
        obs.write_pipeline_run(con, row)
    finally:
        con.close()
    print(f"pipeline_runs: {row}")
    if status == "FAILED":
        raise AirflowFailException(f"run failed; failed tasks: {failed or 'unknown'}")


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",  # 9:00 AM IST, Monday-Saturday
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    params=PARAMS,
    max_active_runs=1,
    tags=["nammamart", "etl"],
    doc_md=__doc__,
)
def nammamart_etl():
    extracts = [_make_extract(t) for t in es.TABLES]  # five parallel tasks
    v = validate(extracts)
    t = transform(v)
    loaded = load(t, v)
    rec = reconcile_payments(t, v, loaded)
    report(rec) >> audit_run()


nammamart_etl()
