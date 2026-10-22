"""Part F - builds the input folders for observability experiments 4-9 (the originals are never edited).
Run from the project root:   python tests/make_experiment_data.py
Each folder in include/data/experiments/ is a full copy of the five source files with ONE change; trigger a DAG with
{"data_dir": "/usr/local/airflow/include/data/experiments/<folder>"} to run the experiment against it."""
import os
import shutil

import pandas as pd

SRC = "include/data/nammamart"
OUT = "include/data/experiments"
FILES = ["orders.csv", "payments.csv", "customers.csv", "products.csv", "stores.csv"]


def folder(name, orders=None, skip=()):
    path = os.path.join(OUT, name)
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path)
    for f in FILES:
        if f in skip:
            continue
        if f == "orders.csv" and orders is not None:
            orders.to_csv(os.path.join(path, f), index=False)
        else:
            shutil.copy(os.path.join(SRC, f), path)
    print(f"{path}: {', '.join(f for f in FILES if f not in skip)}")


orders = pd.read_csv(os.path.join(SRC, "orders.csv"), dtype=str, keep_default_na=False)


def amount(qty, price, disc):
    return f"{round(float(qty) * float(price) * (1 - float(disc) / 100), 2):.2f}"


# 4. missing source file
folder("exp04_missing_products", skip=("products.csv",))

# 5. five new bad rows, each breaking exactly one rule (order_amount kept consistent with the formula)
good = orders[(orders["order_status"] == "DELIVERED") & (orders["order_id"] == orders["order_id"].str.strip())].iloc[0]
bad = []
for i, change in enumerate([
    {"customer_id": ""},                                   # null customer
    {"quantity": "-2"},                                    # negative quantity
    {"store_id": "S99"},                                   # unknown store
    {"order_status": "SHIPPED"},                           # status outside the allowed list
    {"order_ts": "2027-01-15 10:30:00"},                   # timestamp in 2027
], start=1):
    r = good.copy()
    r.update(change)
    r["order_id"] = f"ORD99000{i}"
    r["order_amount"] = amount(r["quantity"], r["unit_price"], r["discount_pct"])
    bad.append(r)
folder("exp05_bad_rows", pd.concat([orders, pd.DataFrame(bad)], ignore_index=True))

# 6. schema change: order_amount renamed to total
folder("exp06_schema_change", orders.rename(columns={"order_amount": "total"}))

# 7. volume anomaly: only 50 orders
folder("exp07_volume_50_orders", orders.head(50))

# 8. distribution drift: unit_price x 100 for store S01 (order_amount left as exported)
drift = orders.copy()
s01 = drift["store_id"].str.strip() == "S01"
drift.loc[s01, "unit_price"] = (pd.to_numeric(drift.loc[s01, "unit_price"], errors="coerce") * 100).round(2).astype(str)
folder("exp08_drift_s01_price_x100", drift)

# 9. freshness: every order_ts moved 30 days back, so the latest order is ~30 days old at run time
stale = orders.copy()
ts = pd.to_datetime(stale["order_ts"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
stale.loc[ts.notna(), "order_ts"] = (ts[ts.notna()] - pd.Timedelta(days=30)).dt.strftime("%Y-%m-%d %H:%M:%S")
folder("exp09_stale_orders_30_days", stale)
