"""Proves the ETL (Python) and ELT (SQL) rules give identical failure counts on your data.
Run from the project root:   python tests/dq_parity_check.py
Needs: pip install duckdb pandas  (or run it inside the Astro container)."""
import sys
sys.path.insert(0, ".")
import duckdb
import pandas as pd
from include.nammamart_helpers import dq_sql
from include.nammamart_helpers.quality_rules import RULES, build_context, run_rules

DATA = "include/data/nammamart"
RUN_TS = "2026-10-01 23:59:59"
TABLES = ["orders", "payments", "customers", "products", "stores"]

raw = {t: pd.read_csv(f"{DATA}/{t}.csv", dtype=str, keep_default_na=False) for t in TABLES}
ctx = build_context(raw, RUN_TS)

con = duckdb.connect(":memory:")
for t in TABLES:  # raw layer: every column as text, no cleaning
    con.execute(f"CREATE TABLE raw_{t} AS SELECT * FROM read_csv('{DATA}/{t}.csv', all_varchar=true)")
con.execute(dq_sql.create_dq_results_sql())

py = pd.concat([run_rules(t, raw[t], ctx, "parity", "etl", RUN_TS)[2] for t in TABLES])
for t in TABLES:
    con.execute(dq_sql.insert_dq_results_sql(t, "parity", "elt", RUN_TS))
sq = con.execute("SELECT check_name, rows_checked, rows_failed FROM dq_results").df()

m = py.merge(sq, on="check_name", suffixes=("_python", "_sql"))
m["match"] = (m.rows_failed_python == m.rows_failed_sql) & (m.rows_checked_python == m.rows_checked_sql)
print(m[["check_name", "rows_failed_python", "rows_failed_sql", "match"]].to_string(index=False))
assert len(m) == len(RULES), "some checks are missing on one side"
bad = m[~m.match]
if bad.empty:
    print(f"\nALL {len(m)} CHECKS MATCH between Python and SQL")
else:
    print(f"\n{len(bad)} MISMATCH(ES) - send me this output"); sys.exit(1)
