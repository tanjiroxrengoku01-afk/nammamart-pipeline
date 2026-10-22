"""Part 0 - NammaMart data profiling + row-level Data Issues Log.
Usage: python profile_nammamart.py [data_dir] [out_csv]
Reads all six CSVs as text (nothing is cleaned or edited) and writes one row per issue."""
import sys, re
import pandas as pd

D = sys.argv[1] if len(sys.argv) > 1 else "include/data/nammamart"
OUT = sys.argv[2] if len(sys.argv) > 2 else "data_issues_log.csv"
TODAY = pd.Timestamp("2026-10-01 23:59:59")  # business "as of" date of the data (increment file is the 1 Oct export)
rd = lambda f: pd.read_csv(f"{D}/{f}.csv", dtype=str, keep_default_na=False)
o, p, c, pr, s, inc = (rd(x) for x in ["orders", "payments", "customers", "products", "stores", "orders_2026-10-01_increment"])
num = lambda x: pd.to_numeric(x, errors="coerce")
issues = []

def add(file, keys, col, problem, dim, rule, sev):
    for k in keys:
        issues.append(dict(file=file, row_key=k, column=col, problem=problem,
                           quality_dimension=dim, proposed_rule=rule, severity=sev))

# ---------------- orders ----------------
F = "orders.csv"
od = o.drop_duplicates()
add(F, o[o.duplicated(keep="first")].order_id, "order_id", "Exact duplicate row", "Uniqueness", "Keep first row per order_id; quarantine extras", "Critical")
add(F, o[o.customer_id == ""].order_id, "customer_id", "customer_id is blank", "Completeness", "customer_id NOT NULL", "Critical")
ts = pd.to_datetime(o.order_ts, errors="coerce", format="%Y-%m-%d %H:%M:%S")
add(F, o[ts.isna()].order_id, "order_ts", "Unparseable/blank timestamp (N/A, 2026-13-05, 31/09/2026)", "Validity", "order_ts parses as YYYY-MM-DD HH:MM:SS", "Critical")
add(F, o[ts > TODAY].order_id, "order_ts", "Future-dated order_ts (Oct/Nov 2026, Jan 2027)", "Timeliness", "order_ts <= run time", "Critical")
st = o.order_status
add(F, o[~st.isin(["DELIVERED", "CANCELLED", "RETURNED"]) & st.str.strip().str.upper().isin(["DELIVERED", "CANCELLED", "RETURNED"])].order_id, "order_status", "Wrong casing ('delivered')", "Validity", "Standardise to upper case before check", "Warning")
add(F, o[~st.str.strip().str.upper().isin(["DELIVERED", "CANCELLED", "RETURNED"])].order_id, "order_status", "Invalid status ('Pending??')", "Validity", "order_status in {DELIVERED,CANCELLED,RETURNED}", "Critical")
pm = ["UPI", "Card", "Cash on Delivery", "Wallet"]
add(F, o[~o.payment_mode.isin(pm)].order_id, "payment_mode", "Non-standard payment_mode (upi, 'UPI ', CARD, cod, Cashh)", "Validity", "trim + map to allowed list; unmappable ('Cashh') -> quarantine or map by rule agreed with client", "Warning")
q, u, dp, oa, dm = num(o.quantity), num(o.unit_price), num(o.discount_pct), num(o.order_amount), num(o.delivery_minutes)
add(F, o[q <= 0].order_id, "quantity", "quantity <= 0 (0, -1..-3)", "Accuracy", "quantity > 0", "Critical")
add(F, o[(dp < 0) | (dp > 100)].order_id, "discount_pct", "discount_pct outside 0-100 (120/150/250)", "Validity", "0 <= discount_pct <= 100", "Critical")
add(F, o[((q * u * (1 - dp / 100)).round(2) - oa).abs() > 0.011].order_id, "order_amount", "order_amount != qty x price x (1-disc)", "Accuracy", "recompute and compare, tolerance 0.01", "Critical")
isdel = st.str.strip().str.upper() == "DELIVERED"
add(F, o[isdel & (o.delivery_minutes == "")].order_id, "delivery_minutes", "DELIVERED order with no delivery_minutes", "Completeness", "DELIVERED => delivery_minutes NOT NULL", "Critical")
add(F, o[dm >= 120].order_id, "delivery_minutes", "Impossible delivery time (480/900/1440 min)", "Accuracy", "delivery_minutes < 120 (warn if >35)", "Critical")
add(F, o[(dm > 35) & (dm < 120)].order_id, "delivery_minutes", "Outside normal 8-35 min", "Accuracy", "warn if >35", "Warning")
add(F, o[~o.store_id.isin(s.store_id)].order_id, "store_id", "store_id not in stores (S17,S99,S00)", "Consistency", "FK store_id exists in stores", "Critical")
add(F, o[~o.product_id.isin(pr.product_id)].order_id, "product_id", "product_id not in products (P9xxx)", "Consistency", "FK product_id exists in products", "Critical")
add(F, o[(o.customer_id != "") & ~o.customer_id.isin(c.customer_id)].order_id, "customer_id", "customer_id not in customers", "Consistency", "FK customer_id exists", "Critical")

# ---------------- payments ----------------
F = "payments.csv"
add(F, p[p.duplicated(keep="first")].payment_id, "payment_id", "Exact duplicate payment row", "Uniqueness", "payment_id unique", "Critical")
add(F, p[~p.order_id.isin(o.order_id)].payment_id, "order_id", "Orphan payment: order_id not in orders (ORD9xxxxx)", "Consistency", "payment.order_id exists in orders", "Critical")
pa = num(p.amount_paid)
add(F, p[pa <= 0].payment_id, "amount_paid", "amount_paid <= 0 (6 negative, 4 zero)", "Accuracy", "amount_paid > 0", "Critical")
add(F, p[~p.payment_mode.isin(pm)].payment_id, "payment_mode", "Non-standard payment_mode", "Validity", "trim + map to allowed list", "Warning")
add(F, p[p.payment_ts.pipe(pd.to_datetime, errors="coerce") > TODAY].payment_id, "payment_ts", "Future-dated payment_ts", "Timeliness", "payment_ts <= run time", "Critical")
m = p.drop_duplicates().merge(od.drop_duplicates("order_id")[["order_id", "order_amount"]], on="order_id")
sm = m[(m.payment_status == "SUCCESS") & ((num(m.amount_paid) - num(m.order_amount)).abs() > 0.011)]
add(F, sm.payment_id, "amount_paid", "SUCCESS payment != order_amount (short-paid by 10-100 etc.)", "Consistency", "SUCCESS amount_paid = order_amount -> reconciliation table", "Critical")

# ---------------- customers ----------------
F = "customers.csv"
add(F, c[c.customer_id.duplicated(keep="first")].customer_id, "customer_id", "Duplicate customer_id with conflicting phone/area", "Uniqueness", "one row per customer_id (pick latest/most complete; log conflict)", "Critical")
add(F, c[~c.email.str.match(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$", case=False)].customer_id, "email", "Invalid email (N/A, missing @, no TLD)", "Validity", "email regex", "Warning")
add(F, c[~c.phone.str.fullmatch(r"\d{10}")].customer_id, "phone", "Phone not 10 digits", "Validity", "phone = 10 digits", "Warning")
add(F, c[~c.pincode.str.fullmatch(r"\d{6}")].customer_id, "pincode", "Pincode '5600XX' not 6 digits", "Validity", "pincode = 6 digits", "Warning")
add(F, c[c.city == ""].customer_id, "city", "city blank", "Completeness", "city NOT NULL (warn)", "Warning")
add(F, c[~c.loyalty_tier.isin(["Bronze", "Silver", "Gold", "Platinum"])].customer_id, "loyalty_tier", "Inconsistent casing (BRONZE, bronze, SILVER)", "Consistency", "INITCAP loyalty_tier", "Warning")
add(F, c[pd.to_datetime(c.signup_date, errors="coerce") > TODAY].customer_id, "signup_date", "Future signup_date", "Timeliness", "signup_date <= run date", "Critical")

# ---------------- products ----------------
F = "products.csv"
add(F, pr[pr.product_id.duplicated(keep="last")].product_id, "product_id", "Duplicate product_id: '(old)' row with older MRP", "Uniqueness", "one row per product_id; drop '(old)' version", "Critical")
add(F, pr[pr.category == ""].product_id, "category", "category blank", "Completeness", "category NOT NULL", "Critical")
add(F, pr[num(pr.mrp) <= 0].product_id, "mrp", "Negative mrp", "Accuracy", "mrp > 0", "Critical")
add(F, pr[num(pr.cost_price) > num(pr.mrp)].product_id, "cost_price", "cost_price > mrp (selling below cost)", "Accuracy", "cost_price <= mrp", "Critical")

# ---------------- stores ----------------
F = "stores.csv"
add(F, s[s.zone == ""].store_id, "zone", "zone blank (S07 BTM Layout; South by geography - confirm)", "Completeness", "zone NOT NULL", "Critical")
add(F, s[num(s.capacity_orders_per_day) <= 0].store_id, "capacity_orders_per_day", "capacity = 0 (S12)", "Accuracy", "capacity > 0", "Warning")

# ---------------- increment ----------------
F = "orders_2026-10-01_increment.csv"
add(F, inc[inc.order_id.isin(o.order_id)].order_id, "order_status", "Correction of existing order (DELIVERED -> RETURNED): upsert, don't append", "Consistency", "MERGE on order_id", "Warning")
latest = pd.to_datetime(o.order_ts, errors="coerce")[lambda s_: s_ <= TODAY].max()
add("orders.csv", ["(file)"], "order_ts", f"Latest valid order is {latest:%Y-%m-%d}; stale vs run date", "Timeliness", "latest order_ts within 1 day of run (warn)", "Warning")

log = pd.DataFrame(issues)
log.to_csv(OUT, index=False)
print(log.groupby(["file", "quality_dimension"]).size().unstack(fill_value=0))
print(f"\nTotal issue rows: {len(log)} -> {OUT}")
