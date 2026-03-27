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

# --- SIDEBAR: CLEAN & FOCUSED ---
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
tab_dash, tab_ageing, tab_map = st.tabs(["📊 Dashboard", "⏳ Inventory Ageing", "🔧 Manage Mappings"])

if all([f_inv, f_bill, f_sales, f_wh]):
    # 1. LOAD DATA
    df_inv = pd.read_csv(f_inv)
    df_bill = pd.read_csv(f_bill)
    df_sales = pd.read_csv(f_sales)
    df_wh = pd.read_csv(f_wh)
    df_map = conn.query("SELECT * FROM item_mappings", ttl=0)

    # Convert dates
    df_inv['date'] = pd.to_datetime(df_inv['date'])
    days_in_period = (date_range[1] - date_range[0]).days or 365

    # 2. CALCULATIONS
    # Receivables (DSO)
    total_sales = df_inv['bcy_total'].sum()
    current_ar = df_inv['bcy_balance'].sum()
    avg_dso = (current_ar / (total_sales + 1)) * days_in_period

    # Payables (DPO)
    total_purchases = df_bill['bcy_total'].sum()
    current_ap = df_bill['bcy_balance'].sum()
    avg_dpo = (current_ap / (total_purchases + 1)) * days_in_period

    # Inventory (DIO) - FIXED NameError Logic
    wh_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
    # Calculate average unit cost across warehouse to avoid mapping gaps
    wh_sum['unit_cost'] = wh_sum['Value'] / wh_sum['Qty'].replace(0, 1)
    
    sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
    # Use actual unit costs for COGS
    dio_data = pd.merge(sales_mapped, wh_sum[['title', 'unit_cost']], left_on='inventory_title', right_on='title', how='left')
    total_cogs = (dio_data['quantity_sold'] * dio_data['unit_cost'].fillna(0)).sum()
    
    total_inv_val = wh_sum['Value'].sum()
    avg_dio = (total_inv_val / (total_cogs + 1)) * days_in_period
    
    ccc = avg_dso + avg_dio - avg_dpo

    # --- TAB 1: DASHBOARD (The "Old View" style) ---
    with tab_dash:
        st.header("Working Capital Summary")
        
        # Key Metrics Row
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Rec. Days (DSO)", f"{avg_dso:.1f}", f"{avg_dso - target_dso:+.1f} vs Goal", delta_color="inverse")
        m2.metric("Inv. Days (DIO)", f"{avg_dio:.1f}", f"{avg_dio - target_dio:+.1f} vs Goal", delta_color="inverse")
        m3.metric("Pay. Days (DPO)", f"{avg_dpo:.1f}", f"{avg_dpo - target_dpo:+.1f} vs Goal")
        m4.metric("Cash Cycle (CCC)", f"{ccc:.1f}", help="Days until inventory turns back into cash")

        st.divider()
        
        col_left, col_right = st.columns([2, 1])
        with col_left:
            st.subheader("Top 10 AR Balances")
            cust_bal = df_inv.groupby('customer_name')['bcy_balance'].sum().reset_index()
            top_10 = cust_bal.sort_values('bcy_balance', ascending=False).head(10)
            fig = px.bar(top_10, x='customer_name', y='bcy_balance', color='bcy_balance', color_continuous_scale='Blues')
            st.plotly_chart(fig, use_container_width=True)
            
        with col_right:
            st.subheader("Cash Opportunity")
            daily_sales = total_sales / days_in_period
            unlock = max(0, current_ar - (daily_sales * target_dso))
            st.info(f"Targeting {target_dso} days DSO would unlock **₹{unlock:,.2f}** in stagnant cash.")

    # --- TAB 2: INVENTORY AGEING ---
    with tab_ageing:
        st.header("Inventory Ageing Breakdown")
        ageing_df = wh_sum[['title', 'Qty', 'Value']].sort_values('Value', ascending=False)
        st.dataframe(ageing_df, use_container_width=True)

    # --- TAB 3: MAPPINGS ---
    with tab_map:
        st.header("Manage Zoho to Warehouse Mappings")
        st.dataframe(df_map, use_container_width=True)

else:
    st.info("👋 Welcome! Please upload your 4 CSV files in the sidebar to populate the dashboard.")
