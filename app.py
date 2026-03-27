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

# --- DATABASE SETUP ---
def init_db():
    with conn.session as s:
        s.execute(text("CREATE TABLE IF NOT EXISTS item_mappings (zoho_name TEXT PRIMARY KEY, inventory_title TEXT);"))
        s.commit()

init_db()

# --- SIDEBAR ---
with st.sidebar:
    st.header("🎯 Business Targets")
    target_dso = st.number_input("Target DSO (Days)", value=120)
    target_dio = st.number_input("Target DIO (Days)", value=45)
    
    st.divider()
    st.header("📂 Data Ingestion")
    f_inv = st.file_uploader("Upload Invoices", type="csv")
    f_bill = st.file_uploader("Upload Bills", type="csv")
    f_sales = st.file_uploader("Upload Sales Items", type="csv")
    f_wh = st.file_uploader("Upload Warehouse Export", type="csv")
    date_range = st.date_input("Analysis Period", [pd.to_datetime("2025-04-01"), pd.to_datetime("2026-03-31")])

# --- MAIN TABS ---
tab_dash, tab_ageing, tab_map = st.tabs(["📊 Performance Dashboard", "⏳ Inventory Ageing", "🔧 Mappings"])

if all([f_inv, f_bill, f_sales, f_wh]):
    # 1. LOAD DATA
    df_inv, df_bill = pd.read_csv(f_inv), pd.read_csv(f_bill)
    df_sales, df_wh = pd.read_csv(f_sales), pd.read_csv(f_wh)
    df_map = conn.query("SELECT * FROM item_mappings", ttl=0)
    days_in_period = (date_range[1] - date_range[0]).days or 365

    # 2. DATA COVERAGE & CORE METRICS
    total_sales_items = df_sales['item_name'].nunique()
    mapped_items_count = len(df_map[df_map['zoho_name'].isin(df_sales['item_name'])])
    coverage_pct = (mapped_items_count / total_sales_items) * 100 if total_sales_items > 0 else 0

    avg_dso = (df_inv['bcy_balance'].sum() / (df_inv['bcy_total'].sum() + 1)) * days_in_period
    avg_dpo = (df_bill['bcy_balance'].sum() / (df_bill['bcy_total'].sum() + 1)) * days_in_period

    wh_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
    wh_sum['unit_cost'] = wh_sum['Value'] / wh_sum['Qty'].replace(0, 1)
    
    sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
    dio_data = pd.merge(sales_mapped, wh_sum[['title', 'unit_cost']], left_on='inventory_title', right_on='title', how='left')
    
    total_cogs = (dio_data['quantity_sold'] * dio_data['unit_cost'].fillna(0)).sum()
    avg_dio = (wh_sum['Value'].sum() / (total_cogs + 1)) * days_in_period
    ccc = avg_dso + avg_dio - avg_dpo

    # --- TAB 1: DASHBOARD ---
    with tab_dash:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Current DSO", f"{avg_dso:.1f}d", f"{avg_dso - target_dso:+.1f}", delta_color="inverse")
        m2.metric("Current DIO", f"{avg_dio:.1f}d", f"{avg_dio - target_dio:+.1f}", delta_color="inverse")
        m3.metric("Data Coverage", f"{coverage_pct:.1f}%")
        m4.metric("Cash Cycle (CCC)", f"{ccc:.1f}d")

        # Dynamic Product Filter (Maintained)
        st.divider()
        st.subheader("🔍 Product Deep Dive")
        selected_product = st.selectbox("Select SKU to inspect:", ["All Products"] + sorted(wh_sum['title'].tolist()))
        if selected_product != "All Products":
            p_wh = wh_sum[wh_sum['title'] == selected_product].iloc[0]
            p_sales = dio_data[dio_data['inventory_title'] == selected_product]
            p_qty = p_sales['quantity_sold'].sum()
            st.info(f"**{selected_product}**: Stock: ₹{p_wh['Value']:,.0f} | Qty: {p_wh['Qty']} | Sold: {p_qty}")

        # CEI BY CUSTOMER BAR CHART (NEW)
        st.divider()
        st.subheader("💳 Collection Efficiency & AR by Customer")
        
        # Calculate CEI: (Total Billed - Current Balance) / Total Billed
        cust_stats = df_inv.groupby('customer_name').agg({
            'bcy_total': 'sum',
            'bcy_balance': 'sum'
        }).reset_index()
        
        cust_stats['collected'] = cust_stats['bcy_total'] - cust_stats['bcy_balance']
        cust_stats['CEI (%)'] = (cust_stats['collected'] / (cust_stats['bcy_total'] + 1)) * 100
        cust_stats = cust_stats.sort_values('bcy_balance', ascending=False).head(10)

        fig_cei = px.bar(
            cust_stats, 
            x='customer_name', 
            y='bcy_balance',
            text=cust_stats['CEI (%)'].apply(lambda x: f"CEI: {x:.1f}%"),
            title="Top 10 AR Balances with Collection Efficiency Index",
            labels={'bcy_balance': 'Outstanding Balance (BCY)', 'customer_name': 'Customer'},
            color='CEI (%)',
            color_continuous_scale='RdYlGn' # Red = Low Collection, Green = High Collection
        )
        st.plotly_chart(fig_cei, use_container_width=True)

    # --- TAB 2: INVENTORY AGEING (Maintained) ---
    with tab_ageing:
        st.header("⏳ Inventory Ageing")
        item_sales_vol = dio_data.groupby('inventory_title')['quantity_sold'].sum().reset_index()
        item_stats = pd.merge(wh_sum, item_sales_vol, left_on='title', right_on='inventory_title', how='left')
        item_stats['Item_DIO'] = (item_stats['Value'] / ((item_stats['quantity_sold'].fillna(0) * item_stats['unit_cost']) + 1)) * days_in_period
        
        def get_bucket(d):
            if d <= 30: return "0-30 Days (Fast)"
            if d <= 90: return "31-90 Days (Healthy)"
            return "90+ Days (High Risk)"
        
        item_stats['Bucket'] = item_stats['Item_DIO'].apply(get_bucket)
        st.plotly_chart(px.pie(item_stats, values='Value', names='Bucket', hole=0.4), use_container_width=True)
        st.dataframe(item_stats[['title', 'Value', 'Item_DIO', 'Bucket']].sort_values('Value', ascending=False), use_container_width=True)

    # --- TAB 3: MAPPINGS (Maintained) ---
    with tab_map:
        st.header("🔧 Mapping Editor")
        with st.form("manual_map"):
            c1, c2 = st.columns(2)
            z_name = c1.selectbox("Zoho Item", sorted(df_sales['item_name'].unique()))
            w_name = c2.selectbox("Warehouse SKU", sorted(wh_sum['title'].unique()))
            if st.form_submit_button("Save Mapping"):
                with conn.session as s:
                    s.execute(text("INSERT INTO item_mappings (zoho_name, inventory_title) VALUES (:z, :i) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), {"z": z_name, "i": w_name})
                    s.commit()
                st.rerun()
        st.subheader("Current Database")
        st.dataframe(df_map, use_container_width=True)

else:
    st.info("Upload your 4 CSV files to activate all dashboard features.")
