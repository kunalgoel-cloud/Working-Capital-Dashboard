import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
from datetime import datetime
from difflib import get_close_matches

st.set_page_config(page_title="Working Capital Dash", layout="wide")

def get_conn():
    return st.connection("postgresql", type="sql")

conn = get_conn()

# --- 1. DATABASE SETUP ---
def init_db():
    with conn.session as s:
        s.execute(text("CREATE TABLE IF NOT EXISTS item_mappings (zoho_name TEXT PRIMARY KEY, inventory_title TEXT);"))
        s.execute(text("CREATE TABLE IF NOT EXISTS inventory_history (id SERIAL PRIMARY KEY, sku_title TEXT, qty FLOAT, value FLOAT, snapshot_date DATE, UNIQUE(sku_title, snapshot_date));"))
        s.execute(text("CREATE TABLE IF NOT EXISTS invoices (invoice_id TEXT PRIMARY KEY, bcy_balance FLOAT, bcy_total FLOAT, last_updated TIMESTAMP);"))
        s.commit()

init_db()

# --- 2. SIDEBAR & DATA INGESTION ---
with st.sidebar:
    st.header("🎯 Business Targets")
    t_dso = st.number_input("Target DSO", value=120)
    t_dio = st.number_input("Target DIO", value=45)
    t_dpo = st.number_input("Target DPO", value=90)
    
    st.divider()
    st.header("📂 Data Ingestion")
    f_sum = st.file_uploader("1. Customer Balance Summary (DSO Truth)", type="csv")
    f_wh = st.file_uploader("2. Warehouse Export (DIO Truth)", type="csv")
    f_sales = st.file_uploader("3. Sales by Item (COGS Context)", type="csv")
    f_bill = st.file_uploader("4. Bill Details (DPO Truth)", type="csv")
    f_inv = st.file_uploader("5. Invoice Details (Optional)", type="csv")
    date_range = st.date_input("Period", [pd.to_datetime("2025-04-01"), pd.to_datetime("2026-03-31")])

# --- 3. PROCESSING ENGINE ---
if all([f_sum, f_wh, f_sales, f_bill]):
    # Load files
    df_sum, df_wh, df_sales, df_bill = pd.read_csv(f_sum), pd.read_csv(f_wh), pd.read_csv(f_sales), pd.read_csv(f_bill)
    df_map = conn.query("SELECT * FROM item_mappings", ttl=0)
    days = (date_range[1] - date_range[0]).days or 365

    # DSO (Ledger Based)
    avg_dso = (df_sum['closing_balance'].sum() / (df_sum['invoiced_amount'].sum() + 1)) * days
    # DPO (Bill Based)
    avg_dpo = (df_bill['bcy_balance'].sum() / (df_bill['bcy_total'].sum() + 1)) * days
    
    # DIO (Mapped COGS Based)
    wh_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
    wh_sum['unit_cost'] = wh_sum['Value'] / wh_sum['Qty'].replace(0, 1)
    sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
    dio_data = pd.merge(sales_mapped, wh_sum[['title', 'unit_cost']], left_on='inventory_title', right_on='title', how='left')
    total_cogs = (dio_data['quantity_sold'] * dio_data['unit_cost'].fillna(0)).sum()
    avg_dio = (wh_sum['Value'].sum() / (total_cogs + 1)) * days
    
    ccc = avg_dso + avg_dio - avg_dpo
    coverage = (len(df_map[df_map['zoho_name'].isin(df_sales['item_name'])]) / df_sales['item_name'].nunique()) * 100

    # --- TABS ---
    t1, t2, t3 = st.tabs(["📊 Dashboard", "⏳ Ageing", "🔧 Mappings"])

    with t1:
        # Header Metrics
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("DSO (Receivables)", f"{avg_dso:.1f}d", f"{avg_dso-t_dso:+.1f}", "inverse")
        m2.metric("DIO (Inventory)", f"{avg_dio:.1f}d", f"{avg_dio-t_dio:+.1f}", "inverse")
        m3.metric("DPO (Payables)", f"{avg_dpo:.1f}d", f"{avg_dpo-t_dpo:+.1f}")
        m4.metric("Cash Cycle (CCC)", f"{ccc:.1f}d")
        st.caption(f"Mapping Coverage: {coverage:.1f}%")

        # Product Deep Dive (Restored Grid)
        st.divider()
        st.subheader("🔍 Product Deep Dive")
        sel_p = st.selectbox("Select SKU:", ["All"] + sorted(wh_sum['title'].tolist()))
        if sel_p != "All":
            p_w = wh_sum[wh_sum['title'] == sel_p].iloc[0]
            p_s = dio_data[dio_data['inventory_title'] == sel_p]['quantity_sold'].sum()
            p_d = (p_w['Value'] / ((p_s * p_w['unit_cost']) + 1)) * days
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Stock Value", f"₹{p_w['Value']:,.0f}")
            c2.metric("Qty on Hand", f"{p_w['Qty']:,}")
            c3.metric("Units Sold", f"{p_s:,}")
            c4.metric("SKU DIO", f"{p_d:.1f}d")

        # CEI Chart
        st.divider()
        df_sum['CEI'] = (df_sum['amount_received'] / (df_sum['invoiced_amount'] + 1)) * 100
        fig_cei = px.bar(df_sum.sort_values('closing_balance', ascending=False).head(10), 
                         x='customer_name', y='closing_balance', text=df_sum['CEI'].apply(lambda x: f"{x:.1f}%"),
                         color='CEI', color_continuous_scale='RdYlGn', title="Top 10 AR Balances & Collection Efficiency")
        st.plotly_chart(fig_cei, use_container_width=True)

    with t2:
        st.header("⏳ Inventory Ageing")
        item_sales_vol = dio_data.groupby('inventory_title')['quantity_sold'].sum().reset_index()
        item_stats = pd.merge(wh_sum, item_sales_vol, left_on='title', right_on='inventory_title', how='left')
        item_stats['Item_DIO'] = (item_stats['Value'] / ((item_stats['quantity_sold'].fillna(0) * item_stats['unit_cost']) + 1)) * days
        def get_b(d): return "Fast" if d <= 30 else "Healthy" if d <= 90 else "High Risk"
        item_stats['Bucket'] = item_stats['Item_DIO'].apply(get_b)
        st.plotly_chart(px.pie(item_stats, values='Value', names='Bucket', hole=0.4), use_container_width=True)
        st.dataframe(item_stats[['title', 'Value', 'Item_DIO', 'Bucket']].sort_values('Value', ascending=False))

    with t3:
        st.header("🔧 Mappings")
        with st.form("map"):
            z_i = st.selectbox("Zoho Item", sorted(df_sales['item_name'].unique()))
            w_i = st.selectbox("Warehouse SKU", sorted(wh_sum['title'].unique()))
            if st.form_submit_button("Save"):
                with conn.session as s:
                    s.execute(text("INSERT INTO item_mappings (zoho_name, inventory_title) VALUES (:z, :i) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), {"z": z_i, "i": w_i})
                    s.commit()
                st.rerun()
        st.dataframe(df_map)
else:
    st.info("Upload the 4 core files in the sidebar to begin.")
