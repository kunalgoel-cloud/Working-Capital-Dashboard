import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
from datetime import datetime

st.set_page_config(page_title="Working Capital Warehouse", layout="wide")

def get_conn():
    return st.connection("postgresql", type="sql")

conn = get_conn()

# ─────────────────────────────────────────────
# 1. DATABASE INITIALIZATION
# ─────────────────────────────────────────────
def init_db():
    with conn.session as s:
        s.execute(text("CREATE TABLE IF NOT EXISTS item_mappings (zoho_name TEXT PRIMARY KEY, inventory_title TEXT);"))
        s.execute(text("""CREATE TABLE IF NOT EXISTS customer_history (
            customer_id TEXT, customer_name TEXT, closing_balance FLOAT,
            invoiced_amount FLOAT, amount_received FLOAT,
            snapshot_date DATE, PRIMARY KEY (customer_id, snapshot_date));"""))
        s.execute(text("""CREATE TABLE IF NOT EXISTS inventory_history (
            sku_title TEXT, qty FLOAT, value FLOAT, snapshot_date DATE,
            PRIMARY KEY (sku_title, snapshot_date));"""))
        s.execute(text("""CREATE TABLE IF NOT EXISTS sales_history (
            item_name TEXT, quantity_sold FLOAT, amount FLOAT, snapshot_date DATE,
            PRIMARY KEY (item_name, snapshot_date));"""))
        s.execute(text("""CREATE TABLE IF NOT EXISTS bills_history (
            bill_id TEXT, vendor_name TEXT, bcy_balance FLOAT, bcy_total FLOAT,
            bill_date DATE, snapshot_date DATE,
            PRIMARY KEY (bill_id, snapshot_date));"""))
        s.commit()

init_db()

# ─────────────────────────────────────────────
# 2. INPUT VALIDATION
# ─────────────────────────────────────────────
REQUIRED_COLS = {
    "ledger":    ["customer_id", "customer_name", "closing_balance", "invoiced_amount", "amount_received"],
    "inventory": ["title", "Qty", "Value"],
    "sales":     ["item_name", "quantity_sold", "amount"],
    "bills":     ["bill_number", "vendor_name", "bcy_balance", "bcy_total", "date"],
}

def validate_csv(df, required_cols, label):
    missing = set(required_cols) - set(df.columns)
    if missing:
        st.error(f"❌ {label}: Missing columns: {missing}")
        return False
    if len(df) == 0:
        st.error(f"❌ {label}: File is empty.")
        return False
    return True

# ─────────────────────────────────────────────
# 3. SAFE DIVISION
# ─────────────────────────────────────────────
def safe_div(num, den, fallback=0.0):
    return (num / den) if (den is not None and den != 0) else fallback

# ─────────────────────────────────────────────
# 4. SYNC ENGINE
#    KEY FIX: inventory is pre-aggregated by title before archiving
#    so that ON CONFLICT doesn't overwrite partial batches with single rows
# ─────────────────────────────────────────────
def sync_to_db(df, mode):
    today = datetime.now().date()
    table_map = {
        "ledger":    "customer_history",
        "inventory": "inventory_history",
        "sales":     "sales_history",
        "bills":     "bills_history",
    }
    with conn.session as s:
        count = s.execute(
            text(f"SELECT COUNT(*) FROM {table_map[mode]} WHERE snapshot_date = :d"), {"d": today}
        ).scalar()
        if count > 0:
            st.warning(f"⚠️ {mode.capitalize()} snapshot for {today} already exists ({count} rows). Re-archiving will overwrite.")

    # ── PRE-AGGREGATE inventory so each title appears once ──
    if mode == "inventory":
        df = df.groupby("title", as_index=False).agg({"Qty": "sum", "Value": "sum"})

    prog = st.progress(0)
    stat = st.empty()
    total_rows = len(df)

    with conn.session as s:
        for i, (_, row) in enumerate(df.iterrows()):
            if mode == "ledger":
                q = text("""INSERT INTO customer_history
                            (customer_id, customer_name, closing_balance, invoiced_amount, amount_received, snapshot_date)
                            VALUES (:id, :n, :bal, :inv, :recv, :d)
                            ON CONFLICT (customer_id, snapshot_date) DO UPDATE
                            SET closing_balance=EXCLUDED.closing_balance,
                                invoiced_amount=EXCLUDED.invoiced_amount,
                                amount_received=EXCLUDED.amount_received""")
                p = {"id": str(row["customer_id"]), "n": row["customer_name"],
                     "bal": row["closing_balance"], "inv": row["invoiced_amount"],
                     "recv": row["amount_received"], "d": today}
            elif mode == "inventory":
                q = text("""INSERT INTO inventory_history (sku_title, qty, value, snapshot_date)
                            VALUES (:t, :q, :v, :d)
                            ON CONFLICT (sku_title, snapshot_date) DO UPDATE
                            SET value=EXCLUDED.value, qty=EXCLUDED.qty""")
                p = {"t": row["title"], "q": row["Qty"], "v": row["Value"], "d": today}
            elif mode == "sales":
                q = text("""INSERT INTO sales_history (item_name, quantity_sold, amount, snapshot_date)
                            VALUES (:n, :q, :amt, :d)
                            ON CONFLICT (item_name, snapshot_date) DO UPDATE
                            SET quantity_sold=EXCLUDED.quantity_sold, amount=EXCLUDED.amount""")
                p = {"n": row["item_name"], "q": row["quantity_sold"],
                     "amt": row["amount"], "d": today}
            elif mode == "bills":
                q = text("""INSERT INTO bills_history
                            (bill_id, vendor_name, bcy_balance, bcy_total, bill_date, snapshot_date)
                            VALUES (:id, :v, :bal, :tot, :bd, :d)
                            ON CONFLICT (bill_id, snapshot_date) DO UPDATE
                            SET bcy_balance=EXCLUDED.bcy_balance, bcy_total=EXCLUDED.bcy_total""")
                p = {"id": str(row["bill_number"]), "v": row["vendor_name"],
                     "bal": row["bcy_balance"], "tot": row["bcy_total"],
                     "bd": pd.to_datetime(row["date"]).date(), "d": today}
            s.execute(q, p)
            if i % 100 == 0:
                prog.progress(int((i + 1) / total_rows * 100))
        s.commit()
    prog.progress(100)
    stat.success(f"✅ {mode.capitalize()} archived for {today} ({total_rows} rows).")

# ─────────────────────────────────────────────
# 5. SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.header("🎯 Targets")
    t_dso = st.number_input("Target DSO (days)", value=120)
    t_dio = st.number_input("Target DIO (days)", value=45)
    t_dpo = st.number_input("Target DPO (days)", value=90)
    st.divider()

    f_sum = st.file_uploader("1. Customer Summary", type="csv")
    df_s = None
    if f_sum:
        df_s = pd.read_csv(f_sum)
        if not validate_csv(df_s, REQUIRED_COLS["ledger"], "Customer Summary"):
            df_s = None
        elif st.button("Archive Ledger"):
            sync_to_db(df_s, "ledger")

    f_wh = st.file_uploader("2. Warehouse Export", type="csv")
    df_wh = None
    if f_wh:
        df_wh = pd.read_csv(f_wh)
        df_wh = df_wh.drop(columns=[c for c in df_wh.columns if "Unnamed" in str(c)], errors="ignore")
        if not validate_csv(df_wh, REQUIRED_COLS["inventory"], "Warehouse Export"):
            df_wh = None
        elif st.button("Archive Inventory"):
            sync_to_db(df_wh, "inventory")

    f_sales = st.file_uploader("3. Sales Items", type="csv")
    df_sl = None
    if f_sales:
        df_sl = pd.read_csv(f_sales)
        if not validate_csv(df_sl, REQUIRED_COLS["sales"], "Sales Items"):
            df_sl = None
        elif st.button("Archive Sales"):
            sync_to_db(df_sl, "sales")

    f_bill = st.file_uploader("4. Bill Details", type="csv")
    df_b = None
    if f_bill:
        df_b = pd.read_csv(f_bill)
        df_b["date"] = pd.to_datetime(df_b["date"], errors="coerce")
        if not validate_csv(df_b, REQUIRED_COLS["bills"], "Bill Details"):
            df_b = None

    st.divider()
    date_range = st.date_input(
        "Analysis Period",
        [pd.to_datetime("2025-04-01"), pd.to_datetime("2026-03-31")]
    )

# ─────────────────────────────────────────────
# 6. HISTORIC SNAPSHOT SELECTOR
#    Lets user load a past snapshot from DB instead of uploading fresh files
# ─────────────────────────────────────────────
st.sidebar.divider()
st.sidebar.subheader("🗄️ Load Historic Snapshot")

# Fetch available snapshot dates across all tables
try:
    snap_inv = conn.query("SELECT DISTINCT snapshot_date FROM inventory_history ORDER BY snapshot_date DESC", ttl=0)
    snap_cust = conn.query("SELECT DISTINCT snapshot_date FROM customer_history ORDER BY snapshot_date DESC", ttl=0)
    snap_sales = conn.query("SELECT DISTINCT snapshot_date FROM sales_history ORDER BY snapshot_date DESC", ttl=0)
    snap_bills = conn.query("SELECT DISTINCT snapshot_date FROM bills_history ORDER BY snapshot_date DESC", ttl=0)

    all_dates = sorted(
        set(snap_inv["snapshot_date"].astype(str).tolist()) &
        set(snap_cust["snapshot_date"].astype(str).tolist()) &
        set(snap_sales["snapshot_date"].astype(str).tolist()) &
        set(snap_bills["snapshot_date"].astype(str).tolist()),
        reverse=True
    )
    partial_dates = sorted(
        set(snap_inv["snapshot_date"].astype(str).tolist()) |
        set(snap_cust["snapshot_date"].astype(str).tolist()) |
        set(snap_sales["snapshot_date"].astype(str).tolist()) |
        set(snap_bills["snapshot_date"].astype(str).tolist()),
        reverse=True
    )
except Exception:
    all_dates, partial_dates = [], []

use_historic = False
hist_date = None

if all_dates:
    hist_date_str = st.sidebar.selectbox(
        "📅 Full snapshot dates (all 4 tables):",
        ["— use uploaded files —"] + all_dates
    )
    if hist_date_str != "— use uploaded files —":
        use_historic = True
        hist_date = hist_date_str
        st.sidebar.success(f"Loaded snapshot: {hist_date}")
elif partial_dates:
    st.sidebar.info(f"Partial snapshots available: {', '.join(partial_dates)}\n(Not all 4 tables have data for the same date yet.)")
else:
    st.sidebar.info("No archived snapshots yet. Upload & Archive files to build history.")

# ─────────────────────────────────────────────
# 7. LOAD DATA — from DB snapshot OR uploaded files
# ─────────────────────────────────────────────
if use_historic:
    df_s  = conn.query(f"SELECT * FROM customer_history  WHERE snapshot_date = '{hist_date}'", ttl=0)
    df_wh = conn.query(f"SELECT sku_title AS title, qty AS \"Qty\", value AS \"Value\" FROM inventory_history WHERE snapshot_date = '{hist_date}'", ttl=0)
    df_sl = conn.query(f"SELECT * FROM sales_history     WHERE snapshot_date = '{hist_date}'", ttl=0)
    df_b  = conn.query(f"SELECT bill_id, vendor_name, bcy_balance, bcy_total, bill_date AS date, snapshot_date FROM bills_history WHERE snapshot_date = '{hist_date}'", ttl=0)
    df_b["date"] = pd.to_datetime(df_b["date"], errors="coerce")
    st.info(f"📦 Showing archived snapshot from **{hist_date}**. Upload new files to see today's data.")

data_ready = all([df_s is not None and len(df_s) > 0,
                  df_wh is not None and len(df_wh) > 0,
                  df_sl is not None and len(df_sl) > 0,
                  df_b  is not None and len(df_b) > 0])

# ─────────────────────────────────────────────
# 8. MAIN TABS
# ─────────────────────────────────────────────
t1, t2, t3, t4, t5 = st.tabs(["📊 Dashboard", "📈 Trends", "⏳ Ageing", "🗄️ DB Inspector", "🔧 Mappings"])

if data_ready:
    df_map = conn.query("SELECT * FROM item_mappings", ttl=0)

    # ── Days in analysis period ──
    start_date = pd.Timestamp(date_range[0])
    end_date   = pd.Timestamp(date_range[1])
    days = max((end_date - start_date).days, 1)

    # ── Filter bills by date range (only file with dates) ──
    df_b["date"] = pd.to_datetime(df_b["date"], errors="coerce")
    df_b_filtered = df_b[(df_b["date"] >= start_date) & (df_b["date"] <= end_date)]
    if df_b_filtered.empty:
        st.warning("⚠️ No bills in selected date range — using all bills.")
        df_b_filtered = df_b

    # ── Warehouse: aggregate by title (critical: raw file has multiple rows per title) ──
    wh_sum = df_wh.groupby("title", as_index=False).agg({"Qty": "sum", "Value": "sum"})
    wh_sum["unit_cost"] = wh_sum.apply(lambda r: safe_div(r["Value"], r["Qty"]), axis=1)

    # ────────────────────────────────────────────────────────────────────────
    # DSO FORMULA
    #   Standard: DSO = (Accounts Receivable / Credit Sales) × Days
    #   We use: closing_balance = AR outstanding; invoiced_amount = credit sales proxy
    #   IMPROVEMENT: exclude customers with invoiced_amount <= 0 (B2C/advance cases)
    #   where invoiced_amount=0 inflates the AR numerator without a denominator match
    # ────────────────────────────────────────────────────────────────────────
    df_s_b2b = df_s[(df_s["invoiced_amount"] > 0) & (df_s["closing_balance"] > 0)].copy()
    total_ar        = df_s_b2b["closing_balance"].sum()
    total_invoiced  = df_s_b2b["invoiced_amount"].sum()
    avg_dso = safe_div(total_ar, total_invoiced) * days

    # ────────────────────────────────────────────────────────────────────────
    # DPO FORMULA
    #   Standard: DPO = (Accounts Payable / COGS or Purchases) × Days
    #   bcy_balance = outstanding payable; bcy_total = total purchase bill value
    # ────────────────────────────────────────────────────────────────────────
    total_ap        = df_b_filtered["bcy_balance"].sum()
    total_purchases = df_b_filtered["bcy_total"].sum()
    avg_dpo = safe_div(total_ap, total_purchases) * days

    # ────────────────────────────────────────────────────────────────────────
    # DIO FORMULA
    #   Standard: DIO = (Average Inventory / COGS) × Days
    #   We use: current inventory value / estimated COGS from mapped sales
    #   COGS = Σ (quantity_sold × unit_cost) for mapped items
    #   Numerator uses ALL inventory; denominator uses MAPPED items only
    #   → Show unmapped warning so user can fix mappings
    # ────────────────────────────────────────────────────────────────────────
    sales_mapped = pd.merge(df_sl, df_map, left_on="item_name", right_on="zoho_name", how="left")
    unmapped     = sales_mapped[sales_mapped["inventory_title"].isna()]
    mapped       = sales_mapped[sales_mapped["inventory_title"].notna()]

    dio_data = pd.merge(
        mapped,
        wh_sum[["title", "unit_cost"]],
        left_on="inventory_title", right_on="title",
        how="left"
    )
    total_cogs     = (dio_data["quantity_sold"] * dio_data["unit_cost"].fillna(0)).sum()
    total_inv_val  = wh_sum["Value"].sum()
    avg_dio = safe_div(total_inv_val, total_cogs) * days

    # ── Cash Conversion Cycle ──
    ccc = avg_dso + avg_dio - avg_dpo

    # ── CEI: Collection Efficiency Index ──
    # Standard: CEI = (Received / Invoiced) × 100, capped at 100%
    # Only meaningful for customers with invoiced_amount > 0
    df_s["CEI"] = df_s.apply(
        lambda r: min(safe_div(r["amount_received"], r["invoiced_amount"]) * 100, 100.0)
        if r["invoiced_amount"] > 0 else 0.0,
        axis=1
    )

    # ─────────────────────────────────────────
    # TAB 1: DASHBOARD
    # ─────────────────────────────────────────
    with t1:
        # Unmapped warning at top if relevant
        if len(unmapped) > 0:
            unmapped_val = unmapped["amount"].sum() if "amount" in unmapped.columns else 0
            st.warning(
                f"⚠️ {len(unmapped)} sale line(s) worth ₹{unmapped_val:,.0f} are not mapped to inventory. "
                "DIO is understated. Fix in 🔧 Mappings tab."
            )

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("DSO",        f"{avg_dso:.1f}d",  f"{avg_dso - t_dso:+.1f}d",  "inverse")
        m2.metric("DIO",        f"{avg_dio:.1f}d",  f"{avg_dio - t_dio:+.1f}d",  "inverse")
        m3.metric("DPO",        f"{avg_dpo:.1f}d",  f"{avg_dpo - t_dpo:+.1f}d")
        m4.metric("Cash Cycle", f"{ccc:.1f}d")

        # Metric explanation expander
        with st.expander("ℹ️ How metrics are calculated"):
            st.markdown(f"""
| Metric | Formula | Values used |
|--------|---------|-------------|
| **DSO** | (AR / Invoiced Sales) × {days}d | AR = ₹{total_ar:,.0f} · Sales = ₹{total_invoiced:,.0f} · B2B customers only (invoiced > 0) |
| **DIO** | (Inventory Value / COGS) × {days}d | Inv = ₹{total_inv_val:,.0f} · COGS = ₹{total_cogs:,.0f} (mapped SKUs) |
| **DPO** | (AP Outstanding / Total Purchases) × {days}d | AP = ₹{total_ap:,.0f} · Purchases = ₹{total_purchases:,.0f} (date-filtered) |
| **CCC** | DSO + DIO − DPO | {avg_dso:.1f} + {avg_dio:.1f} − {avg_dpo:.1f} |
            """)

        st.divider()
        st.subheader("🔍 Product Deep Dive")
        product_list = sorted(wh_sum["title"].unique().tolist())
        sel_p = st.selectbox("Search SKU:", ["All Products"] + product_list)

        if sel_p != "All Products":
            p_w     = wh_sum[wh_sum["title"] == sel_p].iloc[0]
            p_s_qty = dio_data[dio_data["inventory_title"] == sel_p]["quantity_sold"].sum()
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Current Value", f"₹{p_w['Value']:,.0f}")
            c2.metric("Stock Qty",     f"{p_w['Qty']:,.0f}")
            c3.metric("Units Sold",    f"{p_s_qty:,.0f}")
            if p_s_qty == 0:
                c4.metric("SKU DIO", "N/A")
                st.warning("⚠️ No mapped sales for this SKU — go to Mappings tab.")
            else:
                p_cogs = p_s_qty * p_w["unit_cost"]
                p_dio  = safe_div(p_w["Value"], p_cogs) * days
                c4.metric("SKU DIO", f"{p_dio:.1f}d")

        st.divider()
        top_10 = df_s.sort_values("closing_balance", ascending=False).head(10).copy()
        fig_cei = px.bar(
            top_10, x="customer_name", y="closing_balance",
            text=top_10["CEI"].apply(lambda x: f"{x:.1f}%"),
            color="CEI", color_continuous_scale="RdYlGn",
            title="Top 10 AR Debtors with Collection Efficiency"
        )
        fig_cei.update_layout(xaxis_tickangle=-30)
        st.plotly_chart(fig_cei, use_container_width=True)

    # ─────────────────────────────────────────
    # TAB 2: TREND ANALYSIS
    # ─────────────────────────────────────────
    with t2:
        st.header("📈 Historical Trends")
        st.caption("Charts populate as you archive daily snapshots. Inventory value now reflects aggregated totals.")

        try:
            debt_trend = conn.query(
                "SELECT snapshot_date, SUM(closing_balance) as total_ar, SUM(invoiced_amount) as total_invoiced "
                "FROM customer_history GROUP BY snapshot_date ORDER BY snapshot_date", ttl=0
            )
        except Exception:
            debt_trend = pd.DataFrame()

        try:
            inv_trend = conn.query(
                "SELECT snapshot_date, SUM(value) as total_inventory "
                "FROM inventory_history GROUP BY snapshot_date ORDER BY snapshot_date", ttl=0
            )
        except Exception:
            inv_trend = pd.DataFrame()

        try:
            bills_trend = conn.query(
                "SELECT snapshot_date, SUM(bcy_balance) as total_ap, SUM(bcy_total) as total_purchases "
                "FROM bills_history GROUP BY snapshot_date ORDER BY snapshot_date", ttl=0
            )
        except Exception:
            bills_trend = pd.DataFrame()

        c1, c2 = st.columns(2)
        with c1:
            if not debt_trend.empty:
                st.plotly_chart(
                    px.line(debt_trend, x="snapshot_date", y="total_ar",
                            title="Total AR (Receivables) Trend", markers=True),
                    use_container_width=True
                )
            else:
                st.info("No receivables history yet.")

        with c2:
            if not inv_trend.empty:
                st.plotly_chart(
                    px.line(inv_trend, x="snapshot_date", y="total_inventory",
                            title="Total Inventory Value Trend", markers=True),
                    use_container_width=True
                )
            else:
                st.info("No inventory history yet.")

        if not bills_trend.empty:
            st.plotly_chart(
                px.line(bills_trend, x="snapshot_date", y=["total_ap", "total_purchases"],
                        title="Payables: Outstanding vs Total Purchases", markers=True),
                use_container_width=True
            )

    # ─────────────────────────────────────────
    # TAB 3: AGEING
    # ─────────────────────────────────────────
    with t3:
        st.subheader("📦 Inventory Risk Ageing (by DIO)")

        item_sales_vol = dio_data.groupby("inventory_title")["quantity_sold"].sum().reset_index()
        item_stats     = pd.merge(wh_sum, item_sales_vol, left_on="title", right_on="inventory_title", how="left")
        item_stats["quantity_sold"] = item_stats["quantity_sold"].fillna(0)
        item_stats["Item_DIO"] = item_stats.apply(
            lambda r: safe_div(r["Value"], r["quantity_sold"] * r["unit_cost"]) * days
            if r["quantity_sold"] > 0 else float("inf"),
            axis=1
        )
        item_stats["Item_DIO_display"] = item_stats["Item_DIO"].apply(
            lambda x: f"{x:.0f}d" if x != float("inf") else "Unmapped / No Sales"
        )

        def get_bucket(d):
            if d == float("inf"): return "⚫ Unmapped"
            if d <= 30:  return "🟢 Fast (≤30d)"
            if d <= 90:  return "🟡 Healthy (31–90d)"
            return "🔴 High Risk (>90d)"

        item_stats["Bucket"] = item_stats["Item_DIO"].apply(get_bucket)

        col1, col2 = st.columns([1, 2])
        with col1:
            st.plotly_chart(
                px.pie(item_stats, values="Value", names="Bucket", hole=0.4,
                       title="Inventory Risk by Value"),
                use_container_width=True
            )
        with col2:
            st.dataframe(
                item_stats[["title", "Value", "Qty", "Item_DIO_display", "Bucket"]]
                .sort_values("Value", ascending=False)
                .rename(columns={"Item_DIO_display": "DIO"}),
                use_container_width=True
            )

        st.divider()
        st.subheader("🧾 Receivables Ageing (by Collection Efficiency)")
        ar_buckets = df_s[df_s["closing_balance"] > 0].copy()
        ar_buckets["Risk"] = ar_buckets["CEI"].apply(
            lambda c: "🟢 Well Collected (≥90%)" if c >= 90
                 else "🟡 At Risk (50–89%)"      if c >= 50
                 else "🔴 Overdue (<50%)"
        )
        c1, c2 = st.columns([1, 2])
        with c1:
            st.plotly_chart(
                px.pie(ar_buckets, values="closing_balance", names="Risk", hole=0.4,
                       title="AR Risk by Outstanding Value"),
                use_container_width=True
            )
        with c2:
            st.dataframe(
                ar_buckets[["customer_name", "invoiced_amount", "amount_received", "closing_balance", "CEI", "Risk"]]
                .sort_values("closing_balance", ascending=False),
                use_container_width=True
            )

    # ─────────────────────────────────────────
    # TAB 4: DB INSPECTOR  ← NEW
    # ─────────────────────────────────────────
    with t4:
        st.header("🗄️ Database Inspector")
        st.caption("Check what's stored in PostgreSQL across all archive tables.")

        tables = {
            "customer_history":  "customer_history",
            "inventory_history": "inventory_history",
            "sales_history":     "sales_history",
            "bills_history":     "bills_history",
            "item_mappings":     "item_mappings",
        }

        for table_label, table_name in tables.items():
            with st.expander(f"📋 {table_label}", expanded=(table_label == "inventory_history")):
                try:
                    # Snapshot date coverage
                    if table_name != "item_mappings":
                        snap_df = conn.query(
                            f"SELECT snapshot_date, COUNT(*) as rows, "
                            f"{'SUM(value)' if table_name == 'inventory_history' else 'SUM(closing_balance)' if table_name == 'customer_history' else 'COUNT(*)'} as metric "
                            f"FROM {table_name} GROUP BY snapshot_date ORDER BY snapshot_date DESC",
                            ttl=0
                        )
                        if snap_df.empty:
                            st.info("No data archived yet.")
                        else:
                            col_label = "Total Value" if table_name == "inventory_history" else \
                                        "Total AR"    if table_name == "customer_history" else "Row Count"
                            snap_df.columns = ["Snapshot Date", "Rows", col_label]
                            st.dataframe(snap_df, use_container_width=True)

                            # Preview latest snapshot
                            latest = snap_df["Snapshot Date"].iloc[0]
                            prev = conn.query(f"SELECT * FROM {table_name} WHERE snapshot_date = '{latest}' LIMIT 20", ttl=0)
                            st.caption(f"Preview of latest snapshot ({latest}):")
                            st.dataframe(prev, use_container_width=True)
                    else:
                        mapping_df = conn.query("SELECT * FROM item_mappings", ttl=0)
                        st.write(f"{len(mapping_df)} mapping(s) stored.")
                        st.dataframe(mapping_df, use_container_width=True)
                except Exception as e:
                    st.error(f"Query failed: {e}")

        # Quick SQL runner
        st.divider()
        st.subheader("🔍 Custom SQL Query")
        sql_input = st.text_area("Run a SELECT query:", value="SELECT snapshot_date, SUM(value) as total_value FROM inventory_history GROUP BY snapshot_date ORDER BY snapshot_date DESC;", height=80)
        if st.button("Run Query"):
            try:
                result = conn.query(sql_input, ttl=0)
                st.dataframe(result, use_container_width=True)
                st.caption(f"{len(result)} row(s) returned.")
            except Exception as e:
                st.error(f"Error: {e}")

    # ─────────────────────────────────────────
    # TAB 5: MAPPINGS
    # ─────────────────────────────────────────
    with t5:
        st.subheader("🔧 Item Mappings (Zoho Sales → Warehouse SKU)")
        with st.form("map_form"):
            z = st.selectbox("Zoho Item Name", sorted(df_sl["item_name"].unique()))
            w = st.selectbox("Warehouse SKU",  sorted(wh_sum["title"].unique()))
            if st.form_submit_button("Save Mapping"):
                with conn.session as s:
                    s.execute(
                        text("INSERT INTO item_mappings (zoho_name, inventory_title) VALUES (:z, :w) "
                             "ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"),
                        {"z": z, "w": w}
                    )
                    s.commit()
                st.rerun()

        st.dataframe(df_map, use_container_width=True)

        if len(unmapped) > 0:
            st.subheader("🔴 Unmapped Sale Items (need mapping to be included in DIO)")
            st.dataframe(
                unmapped[["item_name", "quantity_sold", "amount"]]
                .drop_duplicates("item_name")
                .sort_values("amount", ascending=False),
                use_container_width=True
            )

else:
    st.info("Upload all four files in the sidebar **or** select a historic snapshot to view the dashboard.")
    st.markdown("""
**Getting started:**
1. Upload your 4 CSV files in the sidebar
2. Click each **Archive** button to save today's snapshot to PostgreSQL
3. The **DB Inspector** tab (available after upload) lets you verify what's stored
4. Use **Load Historic Snapshot** in the sidebar to compare past dates
    """)
