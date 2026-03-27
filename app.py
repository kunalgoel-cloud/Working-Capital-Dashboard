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
    st.header("💰 Cash Forecast Inputs")
    current_cash = st.number_input("Current Bank Balance (BCY)", value=500000)
    monthly_fixed_costs = st.number_input("Monthly Fixed Costs (Rent, Salaries, etc.)", value=150000)
    
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
tabs = st.tabs(["📊 Dashboard", "💡 Trends & Actions", "⏳ Inventory Ageing", "📉 Cash Runway", "🔧 Mappings"])
tab_dash, tab_insights, tab_ageing, tab_runway, tab_map = tabs

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
    
    # 3. Inventory & COGS
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
        st.info(f"💰 **Strategic Opportunity:** Achieving your {what_if_dso}d DSO target would unlock **₹{cash_unlock:,.2f}** in cash.")
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("DSO", f"{avg_dso:.1f}d", f"{avg_dso - target_dso:+.1f}", delta_color="inverse")
        m2.metric("DIO", f"{avg_dio:.1f}d", f"{avg_dio - target_dio:+.1f}", delta_color="inverse")
        m3.metric("CCC", f"{avg_dso + avg_dio - 90:.1f}d")
        
        # Simple Runway Calculation for Dashboard
        avg_monthly_collections = (total_sales - current_ar) / (days_in_period / 30)
        avg_monthly_spend = (df_bill['bcy_total'].sum() / (days_in_period / 30)) + monthly_fixed_costs
        net_burn = avg_monthly_spend - avg_monthly_collections
        runway_months = current_cash / net_burn if net_burn > 0 else 12 # Cap at 12 for display
        
        m4.metric("Est. Runway", f"{runway_months:.1f} Mo", delta="Critical" if runway_months < 3 else "Healthy")

    # --- TAB 4: CASH RUNWAY (NEW) ---
    with tab_runway:
        st.header("📉 Cash Runway & Burn Analysis")
        
        c1, c2 = st.columns(2)
        with c1:
            st.write(f"**Average Monthly Spend:** ₹{avg_monthly_spend:,.2f}")
            st.write(f"**Average Monthly Collections:** ₹{avg_monthly_collections:,.2f}")
            st.write(f"**Net Monthly Burn:** ₹{net_burn:,.2f}")
        
        # Forecast Dataframe
        forecast_months = []
        remaining_cash = current_cash
        for i in range(1, 7):
            remaining_cash -= net_burn
            forecast_months.append({"Month": i, "Projected Cash": max(0, remaining_cash)})
        
        df_forecast = pd.DataFrame(forecast_months)
        fig_runway = px.area(df_forecast, x='Month', y='Projected Cash', title="6-Month Cash Projection")
        st.plotly_chart(fig_runway, use_container_width=True)
        
        if net_burn > 0:
            st.warning(f"⚠️ Based on current trends, your business has approximately **{runway_months:.1f} months** of cash remaining.")
        else:
            st.success("✅ Your collections are currently exceeding your spend. Your runway is infinite at this rate!")

    # --- OTHER TABS (Simplified for brevity) ---
    with tab_ageing:
        st.header("⏳ Inventory Ageing")
        st.write("See previous code for full Item DIO bucketing logic.")

    with tab_map:
        st.header("🔧 Mappings")
        st.dataframe(df_map, use_container_width=True)

else:
    st.info("Please upload all 4 CSV files to generate the Runway Forecast.")
