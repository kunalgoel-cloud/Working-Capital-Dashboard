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
    target_dso = st.number_input("Target DSO (Customer Credit)", value=120)
    target_dio = st.number_input("Target DIO (Stock Turn)", value=45)
    target_dpo = st.number_input("Target DPO (Vendor Credit)", value=90)
    
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

    # 2. CORE CALCULATIONS
    days_in_period = (date_range[1] - date_range[0]).days or 365
    
    # Receivables & Payables
    avg_dso = (df_inv['bcy_balance'].sum() / (df_inv['bcy_total'].sum() + 1)) * days_in_period
    avg_dpo = (df_bill['bcy_balance'].sum() / (df_bill['bcy_total'].sum() + 1)) * days_in_period

    # 3. STABILIZED DIO LOGIC
    # Get total value and Qty from warehouse
    wh_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
    wh_sum['unit_cost'] = wh_sum['Value'] / wh_sum['Qty'].replace(0, 1)
    
    # Map sales items to warehouse titles
    sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
    
    # Join unit cost from warehouse sum
    dio_data = pd.merge(sales_mapped, wh_sum[['title', 'unit_cost']], left_on='inventory_title', right_on='title', how='left')
    
    # COGS = Quantity Sold * Unit Cost
    total_cogs = (dio_data['quantity_sold'] * dio_data['unit_cost'].fillna(0)).sum()
    total_inv_val = wh_sum['Value'].sum()
    
    # Final DIO calculation
    avg_dio = (total_inv_val / (total_cogs + 1)) * days_in_period
    ccc = avg_dso + avg_dio - avg_dpo

    # --- TAB 1: DASHBOARD ---
    with tab_dash:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("DSO (Receivables)", f"{avg_dso:.1f}d", f"{avg_dso - target_dso:+.1f}", delta_color="inverse")
        m2.metric("DIO (Inventory)", f"{avg_dio:.1f}d", f"{avg_dio - target_dio:+.1f}", delta_color="inverse")
        m3.metric("DPO (Payables)", f"{avg_dpo:.1f}d", f"{avg_dpo - target_dpo:+.1f}")
        m4.metric("Net Cash Cycle (CCC)", f"{ccc:.1f}d")

        st.divider()
        st.subheader("💡 Strategic Deep Dive")
        c1, c2 = st.columns(2)
        with c1:
            top_debtor = df_inv.groupby('customer_name')['bcy_balance'].sum().idxmax()
            st.error(f"**Collections Priority:** Focus on **{top_debtor}** to reduce DSO.")
        with c2:
            st.warning(f"**Inventory Priority:** Total value **₹{total_inv_val:,.0f}**. COGS tracked: **₹{total_cogs:,.0f}**.")

        cust_bal = df_inv.groupby('customer_name')['bcy_balance'].sum().reset_index().sort_values('bcy_balance', ascending=False).head(10)
        st.plotly_chart(px.bar(cust_bal, x='customer_name', y='bcy_balance', title="Top 10 AR Balances"), use_container_width=True)

    # --- TAB 3: MAPPINGS (RESTORED MANUAL EDITOR) ---
    with tab_map:
        st.header("🔧 Mapping Management")
        
        # Manual Editor Form
        with st.form("manual_map_form"):
            st.subheader("🖊️ Manual Mapping Editor")
            col1, col2 = st.columns(2)
            z_item = col1.selectbox("Zoho Item Name", sorted(df_sales['item_name'].unique()))
            w_sku = col2.selectbox("Warehouse SKU", sorted(wh_sum['title'].unique()))
            if st.form_submit_button("Save Mapping"):
                with conn.session as s:
                    s.execute(text("INSERT INTO item_mappings (zoho_name, inventory_title) VALUES (:z, :i) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), 
                              {"z": z_item, "i": w_sku})
                    s.commit()
                st.rerun()
        
        st.divider()
        st.subheader("Current Mapping Table")
        st.dataframe(df_map, use_container_width=True)

else:
    st.info("👋 Please upload your 4 CSV files to activate the dashboard.")
