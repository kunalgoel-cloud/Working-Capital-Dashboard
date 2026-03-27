import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
from datetime import datetime

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
    st.header("🎯 Business Objectives")
    target_dso = st.number_input("Target DSO (Days)", value=120)
    target_dio = st.number_input("Target DIO (Days)", value=45)
    target_dpo = st.number_input("Target DPO (Days)", value=90)
    
    st.divider()
    st.header("📂 Data Ingestion")
    f_inv = st.file_uploader("Upload Invoices", type="csv")
    f_bill = st.file_uploader("Upload Bills", type="csv")
    f_sales = st.file_uploader("Upload Sales Items", type="csv")
    f_wh = st.file_uploader("Upload Warehouse Export", type="csv")
    date_range = st.date_input("Analysis Period", [pd.to_datetime("2025-04-01"), pd.to_datetime("2026-03-31")])

# --- MAIN TABS ---
tab_dash, tab_ageing, tab_map = st.tabs(["📊 Performance Dashboard", "⏳ Inventory Ageing", "🔧 Manage Mappings"])

if all([f_inv, f_bill, f_sales, f_wh]):
    # 1. LOAD DATA
    df_inv, df_bill = pd.read_csv(f_inv), pd.read_csv(f_bill)
    df_sales, df_wh = pd.read_csv(f_sales), pd.read_csv(f_wh)
    df_map = conn.query("SELECT * FROM item_mappings", ttl=0)

    # 2. CORE CALCULATIONS
    days_in_period = (date_range[1] - date_range[0]).days or 365
    
    # DSO & DPO
    avg_dso = (df_inv['bcy_balance'].sum() / (df_inv['bcy_total'].sum() + 1)) * days_in_period
    avg_dpo = (df_bill['bcy_balance'].sum() / (df_bill['bcy_total'].sum() + 1)) * days_in_period

    # DIO & Mapping Logic
    wh_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
    wh_sum['unit_cost'] = wh_sum['Value'] / wh_sum['Qty'].replace(0, 1)
    
    sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
    dio_data = pd.merge(sales_mapped, wh_sum[['title', 'unit_cost']], left_on='inventory_title', right_on='title', how='left')
    
    # Safety: If no items are mapped yet, show 0 instead of millions
    total_cogs = (dio_data['quantity_sold'] * dio_data['unit_cost'].fillna(0)).sum()
    avg_dio = (wh_sum['Value'].sum() / (total_cogs + 1)) * days_in_period if total_cogs > 0 else 0
    ccc = avg_dso + avg_dio - avg_dpo

    # --- TAB 1: DASHBOARD ---
    with tab_dash:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("DSO (Receivables)", f"{avg_dso:.1f}", f"{avg_dso - target_dso:+.1f} vs Goal", delta_color="inverse")
        m2.metric("DIO (Inventory)", f"{avg_dio:.1f}", f"{avg_dio - target_dio:+.1f} vs Goal", delta_color="inverse")
        m3.metric("DPO (Payables)", f"{avg_dpo:.1f}", f"{avg_dpo - target_dpo:+.1f} vs Goal")
        m4.metric("Cash Cycle (CCC)", f"{ccc:.1f}")

        st.divider()
        
        # Actionable Insights Row
        st.subheader("💡 Strategic Insights")
        c1, c2 = st.columns(2)
        with c1:
            st.error(f"**Collections Priority:** You are {avg_dso - target_dso:.1f} days over target. Focus on **RK WORLDINFOCOM** (₹{df_inv.groupby('customer_name')['bcy_balance'].sum().max():,.0f}) first.")
        with c2:
            st.warning(f"**Inventory Priority:** Your current inventory value is ₹{wh_sum['Value'].sum():,.0f}. Aiming for {target_dio} days DIO would free up significant capital.")

        st.subheader("Top 10 AR Balances")
        cust_bal = df_inv.groupby('customer_name')['bcy_balance'].sum().reset_index().sort_values('bcy_balance', ascending=False).head(10)
        st.plotly_chart(px.bar(cust_bal, x='customer_name', y='bcy_balance', color_discrete_sequence=['#1f77b4']), use_container_width=True)

    # --- TAB 2: INVENTORY AGEING ---
    with tab_ageing:
        st.header("⏳ Inventory Turnover & Ageing")
        
        # Calculate Ageing Buckets based on Item DIO
        item_stats = dio_data.groupby('inventory_title').agg({'quantity_sold':'sum', 'unit_cost':'mean'}).reset_index()
        item_stats = pd.merge(wh_sum, item_stats, left_on='title', right_on='inventory_title', how='left')
        item_stats['Item_DIO'] = (item_stats['Value'] / ((item_stats['quantity_sold'] * item_stats['unit_cost']) + 1)) * days_in_period
        
        def get_bucket(d):
            if d <= 30: return "0-30 Days (Fast)"
            if d <= 60: return "31-60 Days (Healthy)"
            if d <= 90: return "61-90 Days (Slow)"
            return "90+ Days (High Risk)"
        
        item_stats['Ageing Bucket'] = item_stats['Item_DIO'].apply(get_bucket)
        
        fig_age = px.pie(item_stats, values='Value', names='Ageing Bucket', color_discrete_map={
            "0-30 Days (Fast)": "#2ecc71", "31-60 Days (Healthy)": "#3498db", 
            "61-90 Days (Slow)": "#f1c40f", "90+ Days (High Risk)": "#e74c3c"
        })
        st.plotly_chart(fig_age, use_container_width=True)
        st.dataframe(item_stats[['title', 'Qty', 'Value', 'Item_DIO', 'Ageing Bucket']].sort_values('Value', ascending=False), use_container_width=True)

    # --- TAB 3: MAPPINGS (RESTORED EDITOR) ---
    with tab_map:
        st.header("🔧 Manage Zoho to Warehouse Mappings")
        
        unmapped = [n for n in df_sales['item_name'].unique() if n not in df_map['zoho_name'].tolist()]
        if unmapped:
            st.warning(f"You have {len(unmapped)} unmapped items. Map them below to fix the DIO calculation.")
            with st.expander("Map New Items", expanded=True):
                item_to_map = unmapped[0]
                col_a, col_b = st.columns([3, 1])
                choice = col_a.selectbox(f"Map '{item_to_map}' to:", sorted(list(wh_sum['title'].unique()) + ["DISCONTINUED"]))
                if col_b.button("Save Mapping"):
                    with conn.session as s:
                        s.execute(text("INSERT INTO item_mappings VALUES (:z, :i) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), {"z":item_to_map, "i":choice})
                        s.commit()
                    st.rerun()
        
        st.subheader("Current Mapping Table")
        st.dataframe(df_map, use_container_width=True)

else:
    st.info("👋 Welcome! Please upload your 4 CSV files in the sidebar to populate the dashboard.")
