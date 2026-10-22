"""NammaMart ELT pipeline: Extract -> Load raw -> Transform in the warehouse with SQL (staging -> marts).

Every source row is loaded into raw_<table> exactly as received (all text + _loaded_at, _source_file,
_source_row, _run_id). Nothing is cleaned before the warehouse: all transformations are the .sql files in
include/sql/ (raw/, staging/, mart/, report/), run by the tasks below. The same 40 data-quality rules as the
ETL DAG are applied in SQL (include/nammamart_helpers/dq_sql.py) and logged to dq_results on every run.

Trigger with config to run the experiments, e.g. {"increment_file": "orders_2026-10-01_increment.csv"}:
the increment rows are appended to raw_orders and MERGEd into stg_orders on order_id.
Params: data_dir, increment_file ('' = none), as_of ('' = now, else 'YYYY-MM-DD HH:MM:SS' used by the timeliness rules).
"""
from __future__ import annotations

import os
from datetime import timedelta

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

from include.nammamart_helpers import dq_sql
from include.nammamart_helpers import observability as obs
from include.nammamart_helpers.etl_steps import EXPECTED_COLUMNS, TABLES
from include.nammamart_helpers.sql_runner import run_sql

LEARNER = "slick"  # <- same name as the ETL DAG: both pipelines share one warehouse file
DAG_ID = f"nammamart_elt_{LEARNER}"
PROJECT = "/usr/local/airflow"
WAREHOUSE = f"{PROJECT}/include/warehouse_{LEARNER}.duckdb"
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
STAGING_MODELS = ["stg_customers", "stg_products", "stg_stores", "stg_payments", "stg_orders_checked"]
MARTS = ["mart_fact_orders", "mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
         "mart_payment_reconciliation", "mart_top_customers"]
REPORTS = ["q1_revenue_by_zone_store", "q2_category_margin", "q3_delivery_sla",
           "q4_payment_mode_and_unreconciled", "q5_top_customers", "q6_rejected_rows"]


def _run():
    ctx = get_current_context()
    return ctx["run_id"], ctx["params"]


def _as_of(params):
    return (params.get("as_of") or "").strip() or pendulum.now("Asia/Kolkata").format("YYYY-MM-DD HH:mm:ss")


def _ts(value):
    return f"TIMESTAMP '{value}'"


def _now():
    return _ts(pendulum.now("UTC").format("YYYY-MM-DD HH:mm:ss"))


def _increment(params):
    return (params.get("increment_file") or "").strip()


def _in_transaction(con, fn):
    con.execute("BEGIN")
    try:
        out = fn()
        con.execute("COMMIT")
        return out
    except Exception:
        con.execute("ROLLBACK")
        raise


def _make_extract(table):
    @task(task_id=f"extract_{table}")
    def extract():
        """Check the file exists and has the expected columns (fail loudly), and count its rows. No cleaning."""
        import duckdb
        _, params = _run()
        names = [f"{table}.csv"] + ([_increment(params)] if table == "orders" and _increment(params) else [])
        files = []
        con = duckdb.connect()  # in-memory: only reads the file, writes nothing
        try:
            for name in names:
                path = os.path.join(params["data_dir"], name)
                if not os.path.exists(path):
                    raise FileNotFoundError(f"source file missing: {path}")
                src = f"read_csv('{path}', all_varchar = true, header = true)"
                columns = [r[0].strip() for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()]
                missing = [c for c in EXPECTED_COLUMNS[table] if c not in columns]
                if missing:
                    raise ValueError(f"{name}: missing expected columns {missing}; found {columns}")
                rows = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
                print(f"extract_{table}: {name} has {rows} rows, columns OK")
                files.append({"file": name, "path": path, "rows": rows})
        finally:
            con.close()
        return {"table": table, "files": files, "rows": sum(f["rows"] for f in files)}
    return extract()


@task
def load_raw(extracts):
    """RAW layer: append every row of every file, as text, in one transaction."""
    run_id, _ = _run()
    con = obs.connect_warehouse(WAREHOUSE)
    loaded_at = _now()

    def work():
        for e in extracts:
            for f in e["files"]:
                run_sql(con, "raw/load_raw.sql", path=f["path"], raw_table=f"raw_{e['table']}", file=f["file"],
                        run_id=run_id, loaded_at=loaded_at)
        return {t: con.execute(f"SELECT COUNT(*), COUNT(*) FILTER (WHERE _run_id = ?) FROM raw_{t}", [run_id]).fetchone()
                for t in TABLES}
    try:
        counts = _in_transaction(con, work)
    finally:
        con.close()
    for t, (total, this_run) in counts.items():
        print(f"raw_{t}: +{this_run} rows this run, {total} rows in total (append-only history)")
    return {"rows_extracted": sum(e["rows"] for e in extracts), "raw_rows_this_run": sum(c[1] for c in counts.values())}


@task
def build_staging(raw):
    """STAGING layer: type, standardise, mask and flag every row with dq_status / failure_reason; log dq_results."""
    run_id, params = _run()
    run_ts, inc = _as_of(params), _increment(params)
    con = obs.connect_warehouse(WAREHOUSE)

    def work():
        run_sql(con, "staging/00_macros.sql")
        run_sql(con, "staging/01_work_tables.sql", run_id=run_id, increment_file=inc)
        for model in STAGING_MODELS:
            table = model.removeprefix("stg_").removesuffix("_checked")
            run_sql(con, f"staging/{model}.sql", run_ts=_ts(run_ts), loaded_at=_now(),
                    failure_reason=dq_sql.failure_reason_sql(table, run_ts, src="wrk_"),
                    warnings=dq_sql.warning_reason_sql(table, run_ts, src="wrk_"))
        con.execute(dq_sql.create_dq_results_sql())
        for t in TABLES:
            con.execute(dq_sql.delete_previous_sql(t, run_id, DAG_ID))
            con.execute(dq_sql.insert_dq_results_sql(t, run_id, DAG_ID, run_ts, src="wrk_"))
        run_sql(con, "staging/dq_repair_checks.sql", run_id=run_id, dag_id=DAG_ID)
        run_sql(con, "staging/stg_rejected_rows.sql", run_id=run_id, dag_id=DAG_ID, loaded_at=_now())
        obs.report_volume(con, run_id, DAG_ID, con.execute("SELECT COUNT(*) FROM wrk_orders").fetchone()[0])
        summary = con.execute("""
            SELECT 'orders' AS t, COUNT(*), COUNT(*) FILTER (WHERE dq_status = 'FAIL') FROM wrk_orders_checked
            UNION ALL SELECT 'payments', COUNT(*), COUNT(*) FILTER (WHERE dq_status = 'FAIL') FROM stg_payments
            UNION ALL SELECT 'customers', COUNT(*), COUNT(*) FILTER (WHERE dq_status = 'FAIL') FROM stg_customers
            UNION ALL SELECT 'products', COUNT(*), COUNT(*) FILTER (WHERE dq_status = 'FAIL') FROM stg_products
            UNION ALL SELECT 'stores', COUNT(*), COUNT(*) FILTER (WHERE dq_status = 'FAIL') FROM stg_stores""").fetchall()
        failed_checks = con.execute("""SELECT check_name, severity, rows_failed, status FROM dq_results
            WHERE run_id = ? AND dag_id = ? AND rows_failed > 0 ORDER BY severity, check_name""",
                                    [run_id, DAG_ID]).df()
        return summary, failed_checks
    try:
        summary, failed_checks = _in_transaction(con, work)
    finally:
        con.close()
    for t, rows, failed in summary:
        print(f"staging {t}: in={rows} passed={rows - failed} flagged FAIL={failed}")
    print(failed_checks.to_string(index=False))
    return {"rows_quarantined": sum(f for _, _, f in summary),
            "orders_passed": next(r - f for t, r, f in summary if t == "orders")}


@task
def merge_stg_orders(staged):
    """MERGE the validated orders into stg_orders on order_id: daily file first, then the increment file."""
    run_id, params = _run()
    sources = ["orders.csv"] + ([_increment(params)] if _increment(params) else [])
    con = obs.connect_warehouse(WAREHOUSE)
    try:
        stats = _in_transaction(con, lambda: [run_sql(con, "staging/merge_stg_orders.sql", source_file=s) for s in sources])
    finally:
        con.close()
    out = []
    for s in stats:
        r = s.iloc[0].to_dict()
        print(f"MERGE {r['source_file']}: {r['source_rows']} rows -> {r['new_rows']} inserted, {r['matched_rows']} updated "
              f"({r['status_changed']} changed status, {r['dq_status_changed']} changed dq_status); "
              f"stg_orders now {r['stg_orders_total']} rows, {r['duplicate_order_ids']} duplicate order_ids")
        if r["duplicate_order_ids"]:
            raise RuntimeError("stg_orders has duplicate order_ids after MERGE")
        out.append({k: (int(v) if k != "source_file" else v) for k, v in r.items()})
    return out


@task
def build_marts(merged):
    """MART layer: business-ready tables, all CREATE OR REPLACE from staging (idempotent)."""
    run_id, _ = _run()
    con = obs.connect_warehouse(WAREHOUSE)

    def work():
        counts = {}
        for m in MARTS:
            run_sql(con, f"mart/{m}.sql", run_id=run_id, loaded_at=_now())
            counts[m] = con.execute(f"SELECT COUNT(*) FROM {m}").fetchone()[0]
        return counts
    try:
        counts = _in_transaction(con, work)
    finally:
        con.close()
    for m, n in counts.items():
        print(f"{m}: {n} rows")
    return counts


@task
def report(marts):
    """Print the answers to the six business questions (include/sql/report/*.sql)."""
    run_id, _ = _run()
    con = obs.connect_warehouse(WAREHOUSE)
    try:
        for q in REPORTS:
            print(f"== {q} ==")
            print(run_sql(con, f"report/{q}.sql", run_id=run_id, dag_id=DAG_ID).to_string(index=False))
        totals = con.execute("""SELECT COUNT(*), ROUND(SUM(net_revenue), 2) FROM mart_fact_orders""").fetchone()
        print(f"mart_fact_orders: {totals[0]} clean orders, net revenue {totals[1]}")
    finally:
        con.close()
    return {"fact_rows": totals[0], "net_revenue": totals[1]}


@task(trigger_rule="all_done", retries=0)
def audit_run():
    """Runs even when something upstream failed: one row per run in pipeline_runs, then fail the run if needed."""
    ctx = get_current_context()
    ti, dag_run = ctx["ti"], ctx["dag_run"]
    states = {k: v for k, v in obs.task_states(ti, DAG_ID, dag_run.run_id).items() if k != ti.task_id}
    failed = sorted(k for k, v in states.items() if str(v) in obs.FAILED_STATES)
    raw = ti.xcom_pull(task_ids="load_raw") or {}
    staged = ti.xcom_pull(task_ids="build_staging") or {}
    reported = ti.xcom_pull(task_ids="report")
    status = "FAILED" if failed or not reported else "SUCCESS"
    row = {"run_id": dag_run.run_id, "dag_id": DAG_ID, "start_time": dag_run.start_date,
           "end_time": pendulum.now("UTC"), "rows_extracted": raw.get("rows_extracted"),
           "rows_loaded": staged.get("orders_passed"), "rows_quarantined": staged.get("rows_quarantined"),
           "status": status, "failed_tasks": ", ".join(failed) or None}
    con = obs.connect_warehouse(WAREHOUSE)
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
    tags=["nammamart", "elt"],
    doc_md=__doc__,
)
def nammamart_elt():
    extracts = [_make_extract(t) for t in TABLES]  # five parallel tasks
    raw = load_raw(extracts)
    staged = build_staging(raw)
    merged = merge_stg_orders(staged)
    marts = build_marts(merged)
    report(marts) >> audit_run()


nammamart_elt()
