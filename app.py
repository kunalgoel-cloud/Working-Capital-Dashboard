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

# --- 2. UNIVERSAL SYNC ENGINE ---
def sync_to_db(df, mode):
    today = datetime.now().date()
    total_rows = len(df)
    prog = st.progress(0)
    stat = st.empty()
    with conn.session as s:
        for i, row in df.iterrows():
            if mode == "ledger":
                q = text("INSERT INTO customer_history (customer_id, customer_name, closing_balance, snapshot_date) VALUES (:id, :n, :bal, :d) ON CONFLICT (customer_id, snapshot_date) DO UPDATE SET closing_balance=EXCLUDED.closing_balance")
                p = {"id": str(row['customer_id']), "n": row['customer_name'], "bal": row['closing_balance'], "d": today}
            elif mode == "inventory":
                q = text("INSERT INTO inventory_history (sku_title, qty, value, snapshot_date) VALUES (:t, :q, :v, :d) ON CONFLICT (sku_title, snapshot_date) DO UPDATE SET value=EXCLUDED.value, qty=EXCLUDED.qty")
                p = {"t": row['title'], "q": row['Qty'], "v": row['Value'], "d": today}
            elif mode == "sales":
                q = text("INSERT INTO sales_history (item_name, quantity_sold, snapshot_date) VALUES (:n, :q, :d) ON CONFLICT (item_name, snapshot_date) DO UPDATE SET quantity_sold=EXCLUDED.quantity_sold")
                p = {"n": row['item_name'], "q": row['quantity_sold'], "d": today}
            elif mode == "bills":
                q = text("INSERT INTO bills_history (bill_id, vendor_name, bcy_balance, snapshot_date) VALUES (:id, :v, :bal, :d) ON CONFLICT (bill_id, snapshot_date) DO UPDATE SET bcy_balance=EXCLUDED.bcy_balance")
                p = {"id": str(row['bill_number']), "v": row['vendor_name'], "bal": row['bcy_balance'], "d": today}
            s.execute(q, p)
            if i % 100 == 0: prog.progress(int((i+1)/total_rows*100))
        s.commit()
    stat.success(f"✅ {mode.capitalize()} archived for {today}.")

# --- 3. SIDEBAR ---
with st.sidebar:
    st.header("🎯 Targets")
    t_dso = st.number_input("Target DSO", value=120)
    t_dio = st.number_input("Target DIO", value=45)
    t_dpo = st.number_input("Target DPO", value=90)
    st.divider()
    f_sum = st.file_uploader("1. Customer Summary", type="csv")
    df_s = pd.read_csv(f_sum) if f_sum else None
    if df_s is not None and st.button("Archive Ledger"): sync_to_db(df_s, "ledger")
    f_wh = st.file_uploader("2. Warehouse Export", type="csv")
    df_wh = pd.read_csv(f_wh) if f_wh else None
    if df_wh is not None and st.button("Archive Inventory"): sync_to_db(df_wh, "inventory")
    f_sales = st.file_uploader("3. Sales Items", type="csv")
    df_sl = pd.read_csv(f_sales) if f_sales else None
    if df_sl is not None and st.button("Archive Sales"): sync_to_db(df_sl, "sales")
    f_bill = st.file_uploader("4. Bill Details", type="csv")
    df_b = pd.read_csv(f_bill) if f_bill else None
    if df_b is not None and st.button("Archive Bills"): sync_to_db(df_b, "bills")
    date_range = st.date_input("Analysis Period", [pd.to_datetime("2025-04-01"), pd.to_datetime("2026-03-31")])

# --- 4. MAIN TABS ---
t1, t2, t3, t4 = st.tabs(["📊 Dashboard", "📈 Trend Analysis", "⏳ Ageing", "🔧 Mappings"])

if all([df_s is not None, df_wh is not None, df_sl is not None, df_b is not None]):
    df_map = conn.query("SELECT * FROM item_mappings", ttl=0)
    days = (date_range[1] - date_range[0]).days or 365
    
    # Calculation Logic
    avg_dso = (df_s['closing_balance'].sum() / (df_s['invoiced_amount'].sum() + 1)) * days
    avg_dpo = (df_b['bcy_balance'].sum() / (df_b['bcy_total'].sum() + 1)) * days
    
    # THE 11 LAKH FIX: Calculate Total Value from RAW Warehouse File
    total_warehouse_value = df_wh['Value'].sum()
    
    wh_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
    wh_sum['unit_cost'] = wh_sum['Value'] / wh_sum['Qty'].replace(0, 1)
    
    sales_mapped = pd.merge(df_sl, df_map, left_on='item_name', right_on='zoho_name', how='inner')
    dio_data = pd.merge(sales_mapped, wh_sum[['title', 'unit_cost']], left_on='inventory_title', right_on='title', how='left')
    total_cogs = (dio_data['quantity_sold'] * dio_data['unit_cost'].fillna(0)).sum()
    
    # DIO now based on Total Warehouse Value (11L)
    avg_dio = (total_warehouse_value / (total_cogs + 1)) * days
    ccc = avg_dso + avg_dio - avg_dpo

    with t1:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("DSO", f"{avg_dso:.1f}d")
        m2.metric("DIO (Total Stock)", f"{avg_dio:.1f}d", help=f"Based on full value: ₹{total_warehouse_value:,.0f}")
        m3.metric("DPO", f"{avg_dpo:.1f}d")
        m4.metric("Cash Cycle", f"{ccc:.1f}d")
        
        st.divider()
        st.subheader("🔍 Product Deep Dive")
        # Restored Filter
        product_list = sorted(wh_sum['title'].unique().tolist())
        sel_p = st.selectbox("Search SKU:", ["All Products"] + product_list)
        if sel_p != "All Products":
            p_w = wh_sum[wh_sum['title'] == sel_p].iloc[0]
            p_s_qty = dio_data[dio_data['inventory_title'] == sel_p]['quantity_sold'].sum()
            p_dio = (p_w['Value'] / ((p_s_qty * p_w['unit_cost']) + 1)) * days
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Value", f"₹{p_w['Value']:,.0f}")
            c2.metric("Qty", f"{p_w['Qty']:,}")
            c3.metric("Sold", f"{p_s_qty:,}")
            c4.metric("SKU DIO", f"{p_dio:.1f}d")

    with t2:
        st.header("📈 Historical Trends")
        debt_t = conn.query("SELECT snapshot_date, SUM(closing_balance) as val FROM customer_history GROUP BY snapshot_date ORDER BY snapshot_date", ttl=0)
        inv_t = conn.query("SELECT snapshot_date, SUM(value) as val FROM inventory_history GROUP BY snapshot_date ORDER BY snapshot_date", ttl=0)
        c_h1, c_h2 = st.columns(2)
        if not debt_t.empty: c_h1.plotly_chart(px.line(debt_t, x='snapshot_date', y='val', title="AR Trend"), use_container_width=True)
        if not inv_t.empty: c_h2.plotly_chart(px.line(inv_t, x='snapshot_date', y='val', title="Inventory Trend"), use_container_width=True)

    with t3:
        st.header("⏳ Inventory Health")
        # Mark Discontinued in health tab
        item_sales_vol = dio_data.groupby('inventory_title')['quantity_sold'].sum().reset_index()
        item_stats = pd.merge(wh_sum, item_sales_vol, left_on='title', right_on='inventory_title', how='left')
        item_stats['Item_DIO'] = (item_stats['Value'] / ((item_stats['quantity_sold'].fillna(0) * item_stats['unit_cost']) + 1)) * days
        
        # Merge with mappings to find "Discontinued"
        item_stats = pd.merge(item_stats, df_map, left_on='title', right_on='inventory_title', how='left')
        
        def get_bucket(row):
            if row['zoho_name'] == "DISCONTINUED/OBSOLETE": return "Discontinued"
            d = row['Item_DIO']
            return "Fast" if d <= 30 else "Healthy" if d <= 90 else "High Risk"
            
        item_stats['Bucket'] = item_stats.apply(get_bucket, axis=1)
        st.plotly_chart(px.pie(item_stats, values='Value', names='Bucket', hole=0.4, 
                               color_discrete_map={"Discontinued":"#808080", "High Risk":"#EF553B", "Healthy":"#636EFA", "Fast":"#00CC96"}), use_container_width=True)
        st.dataframe(item_stats[['title', 'Value', 'Item_DIO', 'Bucket']].sort_values('Value', ascending=False))

    with t4:
        st.header("🔧 Mappings")
        with st.form("map_form"):
            z = st.selectbox("Zoho Item Name", sorted(df_sl['item_name'].unique()) + ["DISCONTINUED/OBSOLETE"])
            w = st.selectbox("Warehouse SKU", sorted(wh_sum['title'].unique()))
            if st.form_submit_button("Save Mapping"):
                with conn.session as s:
                    s.execute(text("INSERT INTO item_mappings (zoho_name, inventory_title) VALUES (:z, :w) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), {"z":z, "w":w})
                    s.commit()
                st.rerun()
        st.dataframe(df_map)
else:
    st.info("Upload files to begin.")
