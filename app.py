import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
from datetime import datetime

st.set_page_config(page_title="Working Capital Warehouse", layout="wide")

def get_conn():
    return st.connection("postgresql", type="sql")

conn = get_conn()

# --- 1. DATABASE INITIALIZATION ---
def init_db():
    with conn.session as s:
        s.execute(text("CREATE TABLE IF NOT EXISTS item_mappings (zoho_name TEXT PRIMARY KEY, inventory_title TEXT);"))
        s.execute(text("""CREATE TABLE IF NOT EXISTS customer_history (
            customer_id TEXT, customer_name TEXT, closing_balance FLOAT, snapshot_date DATE,
            PRIMARY KEY (customer_id, snapshot_date));"""))
        s.execute(text("""CREATE TABLE IF NOT EXISTS inventory_history (
            sku_title TEXT, qty FLOAT, value FLOAT, snapshot_date DATE,
            PRIMARY KEY (sku_title, snapshot_date));"""))
        s.execute(text("""CREATE TABLE IF NOT EXISTS sales_history (
            item_name TEXT, quantity_sold FLOAT, snapshot_date DATE,
            PRIMARY KEY (item_name, snapshot_date));"""))
        s.execute(text("""CREATE TABLE IF NOT EXISTS bills_history (
            bill_id TEXT, vendor_name TEXT, bcy_balance FLOAT, snapshot_date DATE,
            PRIMARY KEY (bill_id, snapshot_date));"""))
        s.commit()

init_db()

# --- 2. INPUT VALIDATION ---
def validate_csv(df, required_cols, label):
    """Returns True if all required columns are present and file is non-empty."""
    missing = set(required_cols) - set(df.columns)
    if missing:
        st.error(f"❌ {label}: Missing columns: {missing}")
        return False
    if len(df) == 0:
        st.error(f"❌ {label}: File is empty.")
        return False
    return True

REQUIRED_COLS = {
    "ledger":    ["customer_id", "customer_name", "closing_balance", "invoiced_amount", "amount_received"],
    "inventory": ["title", "Qty", "Value"],
    "sales":     ["item_name", "quantity_sold", "amount"],
    "bills":     ["bill_number", "vendor_name", "bcy_balance", "bcy_total", "date"],
}

# --- 3. UNIVERSAL SYNC ENGINE (bulk insert, duplicate guard) ---
def sync_to_db(df, mode):
    today = datetime.now().date()

    # Guard: warn if today's snapshot already exists
    table_map = {"ledger": "customer_history", "inventory": "inventory_history",
                 "sales": "sales_history", "bills": "bills_history"}
    with conn.session as s:
        count = s.execute(
            text(f"SELECT COUNT(*) FROM {table_map[mode]} WHERE snapshot_date = :d"), {"d": today}
        ).scalar()
        if count > 0:
            st.warning(f"⚠️ {mode.capitalize()} snapshot for {today} already exists ({count} rows). Re-archiving will overwrite.")

    prog = st.progress(0)
    stat = st.empty()
    total_rows = len(df)

    with conn.session as s:
        for i, row in df.iterrows():
            if mode == "ledger":
                q = text("""INSERT INTO customer_history (customer_id, customer_name, closing_balance, snapshot_date)
                            VALUES (:id, :n, :bal, :d)
                            ON CONFLICT (customer_id, snapshot_date) DO UPDATE SET closing_balance=EXCLUDED.closing_balance""")
                p = {"id": str(row["customer_id"]), "n": row["customer_name"], "bal": row["closing_balance"], "d": today}
            elif mode == "inventory":
                q = text("""INSERT INTO inventory_history (sku_title, qty, value, snapshot_date)
                            VALUES (:t, :q, :v, :d)
                            ON CONFLICT (sku_title, snapshot_date) DO UPDATE SET value=EXCLUDED.value, qty=EXCLUDED.qty""")
                p = {"t": row["title"], "q": row["Qty"], "v": row["Value"], "d": today}
            elif mode == "sales":
                q = text("""INSERT INTO sales_history (item_name, quantity_sold, snapshot_date)
                            VALUES (:n, :q, :d)
                            ON CONFLICT (item_name, snapshot_date) DO UPDATE SET quantity_sold=EXCLUDED.quantity_sold""")
                p = {"n": row["item_name"], "q": row["quantity_sold"], "d": today}
            elif mode == "bills":
                q = text("""INSERT INTO bills_history (bill_id, vendor_name, bcy_balance, snapshot_date)
                            VALUES (:id, :v, :bal, :d)
                            ON CONFLICT (bill_id, snapshot_date) DO UPDATE SET bcy_balance=EXCLUDED.bcy_balance""")
                p = {"id": str(row["bill_number"]), "v": row["vendor_name"], "bal": row["bcy_balance"], "d": today}
            s.execute(q, p)
            if i % 100 == 0:
                prog.progress(int((i + 1) / total_rows * 100))
        s.commit()
    prog.progress(100)
    stat.success(f"✅ {mode.capitalize()} archived for {today}.")

# --- 4. SAFE DIVISION HELPER ---
def safe_div(numerator, denominator, fallback=0.0):
    return (numerator / denominator) if denominator and denominator > 0 else fallback

# --- 5. SIDEBAR ---
with st.sidebar:
    st.header("🎯 Targets")
    t_dso = st.number_input("Target DSO", value=120)
    t_dio = st.number_input("Target DIO", value=45)
    t_dpo = st.number_input("Target DPO", value=90)
    st.divider()

    # 1. Customer Summary
    f_sum = st.file_uploader("1. Customer Summary", type="csv")
    df_s = None
    if f_sum:
        df_s = pd.read_csv(f_sum)
        if not validate_csv(df_s, REQUIRED_COLS["ledger"], "Customer Summary"):
            df_s = None
        elif st.button("Archive Ledger"):
            sync_to_db(df_s, "ledger")

    # 2. Warehouse Export — FIX #1: strip unnamed index column
    f_wh = st.file_uploader("2. Warehouse Export", type="csv")
    df_wh = None
    if f_wh:
        df_wh = pd.read_csv(f_wh)
        df_wh = df_wh.drop(columns=[c for c in df_wh.columns if "Unnamed" in str(c)], errors="ignore")
        if not validate_csv(df_wh, REQUIRED_COLS["inventory"], "Warehouse Export"):
            df_wh = None
        elif st.button("Archive Inventory"):
            sync_to_db(df_wh, "inventory")

    # 3. Sales Items
    f_sales = st.file_uploader("3. Sales Items", type="csv")
    df_sl = None
    if f_sales:
        df_sl = pd.read_csv(f_sales)
        if not validate_csv(df_sl, REQUIRED_COLS["sales"], "Sales Items"):
            df_sl = None
        elif st.button("Archive Sales"):
            sync_to_db(df_sl, "sales")

    # 4. Bill Details
    f_bill = st.file_uploader("4. Bill Details", type="csv")
    df_b = None
    if f_bill:
        df_b = pd.read_csv(f_bill)
        df_b["date"] = pd.to_datetime(df_b["date"], errors="coerce")
        if not validate_csv(df_b, REQUIRED_COLS["bills"], "Bill Details"):
            df_b = None

    date_range = st.date_input(
        "Analysis Period",
        [pd.to_datetime("2025-04-01"), pd.to_datetime("2026-03-31")]
    )

# --- 6. MAIN TABS ---
t1, t2, t3, t4 = st.tabs(["📊 Dashboard", "📈 Trend Analysis", "⏳ Ageing", "🔧 Mappings"])

if all([df_s is not None, df_wh is not None, df_sl is not None, df_b is not None]):
    df_map = conn.query("SELECT * FROM item_mappings", ttl=0)

    # Calculate days from selected period (FIX #3 — was ignored before)
    start_date = pd.Timestamp(date_range[0])
    end_date   = pd.Timestamp(date_range[1])
    days = (end_date - start_date).days
    if days <= 0:
        st.warning("⚠️ Invalid date range — defaulting to 365 days.")
        days = 365

    # FIX #3: Apply date range filter to Bills (only file with a date column)
    df_b_filtered = df_b[(df_b["date"] >= start_date) & (df_b["date"] <= end_date)]
    if df_b_filtered.empty:
        st.warning("⚠️ No bills fall within the selected date range. Using all bill data.")
        df_b_filtered = df_b

    # Note about missing date columns in other files
    st.info(
        "ℹ️ Customer Summary and Sales files don't contain date columns, so they reflect "
        "the full export snapshot. Bills are filtered by the selected analysis period."
    )

    # FIX #2: Proper division — no +1 hacks
    total_invoiced   = df_s["invoiced_amount"].sum()
    total_receivable = df_s["closing_balance"].sum()
    avg_dso = safe_div(total_receivable, total_invoiced) * days

    total_bcy_amount  = df_b_filtered["bcy_total"].sum()
    total_payables    = df_b_filtered["bcy_balance"].sum()
    avg_dpo = safe_div(total_payables, total_bcy_amount) * days

    # Warehouse summary
    wh_sum = df_wh.groupby("title").agg({"Qty": "sum", "Value": "sum"}).reset_index()
    wh_sum["unit_cost"] = wh_sum.apply(
        lambda r: safe_div(r["Value"], r["Qty"]), axis=1
    )

    # FIX #4: LEFT join so unmapped items are not silently dropped
    sales_mapped = pd.merge(df_sl, df_map, left_on="item_name", right_on="zoho_name", how="left")
    unmapped = sales_mapped[sales_mapped["inventory_title"].isna()]
    if len(unmapped) > 0:
        unmapped_val = unmapped["amount"].sum() if "amount" in unmapped.columns else 0
        st.warning(
            f"⚠️ {len(unmapped)} sale line(s) (₹{unmapped_val:,.0f}) are not mapped to inventory. "
            "DIO will exclude these items. Go to the 🔧 Mappings tab to fix this."
        )

    # Only use mapped rows for DIO
    dio_data = pd.merge(
        sales_mapped[sales_mapped["inventory_title"].notna()],
        wh_sum[["title", "unit_cost"]],
        left_on="inventory_title", right_on="title",
        how="left"
    )
    total_cogs = (dio_data["quantity_sold"] * dio_data["unit_cost"].fillna(0)).sum()
    avg_dio = safe_div(wh_sum["Value"].sum(), total_cogs) * days

    ccc = avg_dso + avg_dio - avg_dpo

    # ── TAB 1: DASHBOARD ──────────────────────────────────────────────────────
    with t1:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("DSO",        f"{avg_dso:.1f}d", f"{avg_dso - t_dso:+.1f}", "inverse")
        m2.metric("DIO",        f"{avg_dio:.1f}d", f"{avg_dio - t_dio:+.1f}", "inverse")
        m3.metric("DPO",        f"{avg_dpo:.1f}d", f"{avg_dpo - t_dpo:+.1f}")
        m4.metric("Cash Cycle", f"{ccc:.1f}d")
        st.divider()

        st.subheader("🔍 Product Deep Dive")
        product_list = sorted(wh_sum["title"].unique().tolist())
        sel_p = st.selectbox("Search SKU:", ["All Products"] + product_list)

        if sel_p != "All Products":
            p_w = wh_sum[wh_sum["title"] == sel_p].iloc[0]
            p_s_qty = dio_data[dio_data["inventory_title"] == sel_p]["quantity_sold"].sum()

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Current Value", f"₹{p_w['Value']:,.0f}")
            c2.metric("Stock Qty",     f"{p_w['Qty']:,}")
            c3.metric("Units Sold",    f"{p_s_qty:,}")

            # FIX #9: Proper handling when no sales data exists for a SKU
            if p_s_qty == 0:
                st.warning("⚠️ No mapped sales data for this SKU — cannot calculate SKU DIO. Check Mappings tab.")
                c4.metric("SKU DIO", "N/A")
            else:
                p_cogs = p_s_qty * p_w["unit_cost"]
                p_dio  = safe_div(p_w["Value"], p_cogs) * days
                c4.metric("SKU DIO", f"{p_dio:.1f}d")

        st.divider()

        # FIX #8: CEI capped at 100%, no +1 hack
        df_s["CEI"] = df_s.apply(
            lambda r: min((safe_div(r["amount_received"], r["invoiced_amount"]) * 100), 100),
            axis=1
        )
        top_10 = df_s.sort_values("closing_balance", ascending=False).head(10).copy()
        fig_cei = px.bar(
            top_10, x="customer_name", y="closing_balance",
            text=top_10["CEI"].apply(lambda x: f"{x:.1f}%"),
            color="CEI", color_continuous_scale="RdYlGn",
            title="Top 10 AR Debtors"
        )
        st.plotly_chart(fig_cei, use_container_width=True)

    # ── TAB 2: TREND ANALYSIS ─────────────────────────────────────────────────
    with t2:
        st.header("📈 Historical Trends")
        col_hist1, col_hist2 = st.columns(2)
        debt_trend = conn.query(
            "SELECT snapshot_date, SUM(closing_balance) as total_debt FROM customer_history "
            "GROUP BY snapshot_date ORDER BY snapshot_date", ttl=3600  # FIX #13: cache 1 hr
        )
        inv_trend = conn.query(
            "SELECT snapshot_date, SUM(value) as total_inventory FROM inventory_history "
            "GROUP BY snapshot_date ORDER BY snapshot_date", ttl=3600
        )
        with col_hist1:
            if not debt_trend.empty:
                st.plotly_chart(
                    px.line(debt_trend, x="snapshot_date", y="total_debt",
                            title="Total Receivables Trend", markers=True),
                    use_container_width=True
                )
            else:
                st.info("No receivables history yet. Archive the Ledger to start tracking.")
        with col_hist2:
            if not inv_trend.empty:
                st.plotly_chart(
                    px.line(inv_trend, x="snapshot_date", y="total_inventory",
                            title="Total Inventory Value Trend", markers=True),
                    use_container_width=True
                )
            else:
                st.info("No inventory history yet. Archive the Inventory to start tracking.")
        st.info("💡 Trend charts update every time you click 'Archive' in the sidebar.")

    # ── TAB 3: AGEING ─────────────────────────────────────────────────────────
    with t3:
        st.subheader("📦 Inventory Risk Ageing")
        item_sales_vol = dio_data.groupby("inventory_title")["quantity_sold"].sum().reset_index()
        item_stats = pd.merge(wh_sum, item_sales_vol, left_on="title", right_on="inventory_title", how="left")
        item_stats["Item_DIO"] = item_stats.apply(
            lambda r: safe_div(r["Value"], (r.get("quantity_sold", 0) or 0) * r["unit_cost"]) * days, axis=1
        )

        def get_bucket(d):
            return "Fast (≤30d)" if d <= 30 else "Healthy (31–90d)" if d <= 90 else "High Risk (>90d)"

        item_stats["Bucket"] = item_stats["Item_DIO"].apply(get_bucket)
        st.plotly_chart(
            px.pie(item_stats, values="Value", names="Bucket", hole=0.4,
                   title="Inventory Risk Distribution by Value"),
            use_container_width=True
        )
        st.dataframe(
            item_stats[["title", "Value", "Qty", "Item_DIO", "Bucket"]].sort_values("Value", ascending=False),
            use_container_width=True
        )

        # Receivables Ageing (new)
        st.divider()
        st.subheader("🧾 Receivables Ageing")
        if "closing_balance" in df_s.columns:
            ar_buckets = df_s[df_s["closing_balance"] > 0].copy()
            # Without per-invoice dates, bucket by CEI as proxy for overdue risk
            ar_buckets["Risk"] = ar_buckets["CEI"].apply(
                lambda c: "Collected (≥90%)" if c >= 90 else "At Risk (50–90%)" if c >= 50 else "Overdue (<50%)"
            )
            st.plotly_chart(
                px.pie(ar_buckets, values="closing_balance", names="Risk", hole=0.4,
                       title="Receivables Risk by Collection Efficiency"),
                use_container_width=True
            )
            st.dataframe(
                ar_buckets[["customer_name", "invoiced_amount", "amount_received", "closing_balance", "CEI", "Risk"]]
                .sort_values("closing_balance", ascending=False),
                use_container_width=True
            )

    # ── TAB 4: MAPPINGS ───────────────────────────────────────────────────────
    with t4:
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

        # Show unmapped items for easy actioning
        if len(unmapped) > 0:
            st.subheader("🔴 Unmapped Sale Items")
            st.dataframe(unmapped[["item_name", "quantity_sold", "amount"]].drop_duplicates("item_name"), use_container_width=True)

else:
    st.info("Upload all four files in the sidebar to see the Dashboard. Trends (Tab 2) will populate as you archive daily snapshots.")
