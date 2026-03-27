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
    st.header("🎯 Business Objectives")
    target_dso = st.number_input("Target DSO (Days)", value=120)
    target_dio = st.number_input("Target DIO (Days)", value=45)
    
    st.divider()
    st.header("🔮 What-If Analysis")
    what_if_dso = st.slider("Simulated DSO Target", min_value=30, max_value=150, value=int(target_dso))

    st.header("📂 Data Upload")
    f_inv = st.file_uploader("Invoices", type="csv")
    f_bill = st.file_uploader("Bills", type="csv")
    f_sales = st.file_uploader("Sales Items", type="csv")
    f_wh = st.file_uploader("Warehouse Export", type="csv")
    date_range = st.date_input("Analysis Period", [pd.to_datetime("2025-04-01"), pd.to_datetime("2026-03-31")])

# --- TABS ---
tab_dash, tab_insights, tab_ageing, tab_map = st.tabs(["📊 Dashboard", "💡 Trends & Actions", "⏳ Inventory Ageing", "🔧 Mappings"])

if all([f_inv, f_bill, f_sales, f_wh]):
    # 1. Load Data
    df_inv = pd.read_csv(f_inv)
    df_bill = pd.read_csv(f_bill)
    df_sales = pd.read_csv(f_sales)
    df_wh = pd.read_csv(f_wh)
    df_map = conn.query("SELECT * FROM item_mappings", ttl=0)

    # Date Prep
    df_inv['date'] = pd.to_datetime(df_inv['date'])
    days_in_period = (date_range[1] - date_range[0]).days or 365

    # 2. Performance Metrics
    total_sales = df_inv['bcy_total'].sum()
    current_ar = df_inv['bcy_balance'].sum()
    avg_dso = (current_ar / (total_sales + 1)) * days_in_period
    
    # 3. Inventory Logic (Fixed wh_sum reference)
    wh_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
    wh_sum['unit_cost'] = wh_sum['Value'] / wh_sum['Qty'].replace(0, 1)
    
    # Merge Sales + Mapping to get COGS
    sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
    dio_data = pd.merge(sales_mapped, wh_sum, left_on='inventory_title', right_on='title', how='left')
    dio_data['unit_cost'] = dio_data['unit_cost'].fillna(wh_sum['unit_cost'].mean() or 0)
    
    total_cogs = (dio_data['quantity_sold'] * dio_data['unit_cost']).sum()
    avg_dio = (wh_sum['Value'].sum() / (total_cogs + 1)) * days_in_period

    # --- TAB 1: DASHBOARD ---
    with tab_dash:
        daily_sales = total_sales / days_in_period
        cash_unlock = max(0, current_ar - (daily_sales * what_if_dso))
        
        st.info(f"💰 **Cash Unlock Potential:** Achieving a {what_if_dso}d DSO would free up **₹{cash_unlock:,.2f}**")
        
        m1, m2, m3 = st.columns(3)
        m1.metric("Current DSO", f"{avg_dso:.1f} Days", f"{avg_dso - target_dso:+.1f} vs Goal", delta_color="inverse")
        m2.metric("Current DIO", f"{avg_dio:.1f} Days", f"{avg_dio - target_dio:+.1f} vs Goal", delta_color="inverse")
        m3.metric("Total AR Balance", f"₹{current_ar:,.0f}")

        st.subheader("📋 Top Customer Balances (Sortable)")
        cust_table = df_inv.groupby('customer_name').agg({'bcy_balance':'sum', 'bcy_total':'sum'}).reset_index()
        st.dataframe(cust_table.sort_values('bcy_balance', ascending=False), use_container_width=True)

    # --- TAB 3: INVENTORY AGEING ---
    with tab_ageing:
        st.header("📦 Inventory Ageing Analysis")
        
        # Calculate individual SKU velocity
        item_ageing = dio_data.groupby('inventory_title').agg({'quantity_sold':'sum', 'Value':'sum', 'unit_cost':'mean'}).reset_index()
        item_ageing['Item_DIO'] = (item_ageing['Value'] / ((item_ageing['quantity_sold'] * item_ageing['unit_cost']) + 1)) * days_in_period
        
        def bucket_age(d):
            if d <= 30: return "0-30 Days (Fast)"
            if d <= 60: return "31-60 Days (Healthy)"
            if d <= 90: return "61-90 Days (Slow)"
            return "90+ Days (High Risk)"
            
        item_ageing['Ageing Bucket'] = item_ageing['Item_DIO'].apply(bucket_age)
        
        # Charts
        age_summary = item_ageing.groupby('Ageing Bucket')['Value'].sum().reset_index()
        fig_age = px.pie(age_summary, values='Value', names='Ageing Bucket', hole=0.4,
                         color_discrete_map={
                             "0-30 Days (Fast)": "#2ecc71",
                             "31-60 Days (Healthy)": "#3498db",
                             "61-90 Days (Slow)": "#f1c40f",
                             "90+ Days (High Risk)": "#e74c3c"
                         })
        
        c1, c2 = st.columns([1, 2])
        c1.plotly_chart(fig_age, use_container_width=True)
        with c2:
            st.subheader("Inventory Distribution")
            st.dataframe(age_summary.sort_values('Value', ascending=False), use_container_width=True)

        st.divider()
        st.subheader("🔍 At-Risk Stock Detail (Sorted by Value)")
        st.dataframe(item_ageing[item_ageing['Ageing Bucket'] == "90+ Days (High Risk)"].sort_values('Value', ascending=False), use_container_width=True)

    # --- TAB 4: MAPPINGS ---
    with tab_map:
        st.header("Manage Mappings")
        st.dataframe(df_map, use_container_width=True)
else:
    st.info("Please upload all 4 CSV files to generate the dashboard and ageing report.")
