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
    
    st.divider()
    st.header("💰 Cash Forecast Inputs")
    current_cash = st.number_input("Current Bank Balance", value=500000)
    monthly_fixed_costs = st.number_input("Monthly Fixed Costs", value=150000)
    
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
tabs = st.tabs(["📊 Dashboard", "⏳ Inventory Ageing", "📉 Cash Runway", "✉️ Debtor Alerts", "🔧 Mappings"])
tab_dash, tab_ageing, tab_runway, tab_alerts, tab_map = tabs

if all([f_inv, f_bill, f_sales, f_wh]):
    # 1. Load Data
    df_inv, df_bill = pd.read_csv(f_inv), pd.read_csv(f_bill)
    df_sales, df_wh = pd.read_csv(f_sales), pd.read_csv(f_wh)
    df_map = conn.query("SELECT * FROM item_mappings", ttl=0)

    # Date Prep
    df_inv['date'] = pd.to_datetime(df_inv['date'])
    days_in_period = (date_range[1] - date_range[0]).days or 365

    # 2. Performance Metrics
    total_sales = df_inv['bcy_total'].sum()
    current_ar = df_inv['bcy_balance'].sum()
    avg_dso = (current_ar / (total_sales + 1)) * days_in_period
    
    # 3. Inventory Logic (FIXED: Consistently using wh_sum)
    wh_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
    wh_sum['unit_cost'] = wh_sum['Value'] / wh_sum['Qty'].replace(0, 1)
    
    sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
    dio_data = pd.merge(sales_mapped, wh_sum, left_on='inventory_title', right_on='title', how='left')
    dio_data['unit_cost'] = dio_data['unit_cost'].fillna(wh_sum['unit_cost'].mean() or 0)
    
    total_cogs = (dio_data['quantity_sold'] * dio_data['unit_cost']).sum()
    avg_dio = (wh_sum['Value'].sum() / (total_cogs + 1)) * days_in_period

    # --- TAB 1: DASHBOARD ---
    with tab_dash:
        daily_sales = total_sales / days_in_period
        cash_unlock = max(0, current_ar - (daily_sales * what_if_dso))
        st.info(f"💰 **Opportunity:** Hitting your {what_if_dso}d target would unlock **₹{cash_unlock:,.2f}**")
        
        m1, m2, m3 = st.columns(3)
        m1.metric("Current DSO", f"{avg_dso:.1f}d", f"{avg_dso - target_dso:+.1f}", delta_color="inverse")
        m2.metric("Current DIO", f"{avg_dio:.1f}d", f"{avg_dio - target_dio:+.1f}", delta_color="inverse")
        m3.metric("Total AR", f"₹{current_ar:,.0f}")

        st.subheader("Top Debtor Impacts")
        cust_table = df_inv.groupby('customer_name').agg({'bcy_balance':'sum'}).reset_index()
        st.dataframe(cust_table.sort_values('bcy_balance', ascending=False), use_container_width=True)

    # --- TAB 3: CASH RUNWAY ---
    with tab_runway:
        st.header("📉 Cash Runway Analysis")
        avg_monthly_coll = (total_sales - current_ar) / (days_in_period / 30)
        avg_monthly_spend = (df_bill['bcy_total'].sum() / (days_in_period / 30)) + monthly_fixed_costs
        net_burn = avg_monthly_spend - avg_monthly_coll
        runway = current_cash / net_burn if net_burn > 0 else 12
        
        st.metric("Monthly Net Burn", f"₹{net_burn:,.2f}", delta="Negative Burn" if net_burn < 0 else "Burning Cash", delta_color="inverse")
        
        forecast = [{"Month": i, "Cash": max(0, current_cash - (net_burn * i))} for i in range(7)]
        st.plotly_chart(px.area(pd.DataFrame(forecast), x='Month', y='Cash', title="6-Month Projection"))

    # --- TAB 4: DEBTOR ALERTS ---
    with tab_alerts:
        st.header("✉️ Automated Collection Drafts")
        top_debtor = cust_table.sort_values('bcy_balance', ascending=False).iloc[0]
        st.subheader(f"Draft for {top_debtor['customer_name']}")
        email_body = f"""
        Subject: Outstanding Balance Notice - ₹{top_debtor['bcy_balance']:,.2f}
        
        Dear Finance Team,
        
        Our records show an outstanding balance of ₹{top_debtor['bcy_balance']:,.2f}. 
        As we are streamlining our working capital for the new quarter, 
        we would appreciate a status update on this payment.
        """
        st.text_area("Copy and send:", email_body, height=200)

    # --- TAB 2: INVENTORY AGEING ---
    with tab_ageing:
        st.header("⏳ Inventory Ageing")
        item_ageing = dio_data.groupby('inventory_title').agg({'quantity_sold':'sum', 'Value':'sum', 'unit_cost':'mean'}).reset_index()
        item_ageing['Item_DIO'] = (item_ageing['Value'] / ((item_ageing['quantity_sold'] * item_ageing['unit_cost']) + 1)) * days_in_period
        st.dataframe(item_ageing.sort_values('Value', ascending=False), use_container_width=True)

else:
    st.info("Upload all 4 CSV files to view the fixed dashboard and runway forecast.")
