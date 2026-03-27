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
    
    # Receivables & Payables
    avg_dso = (df_inv['bcy_balance'].sum() / (df_inv['bcy_total'].sum() + 1)) * days_in_period
    avg_dpo = (df_bill['bcy_balance'].sum() / (df_bill['bcy_total'].sum() + 1)) * days_in_period

    # Inventory Logic
    wh_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
    wh_sum['unit_cost'] = wh_sum['Value'] / wh_sum['Qty'].replace(0, 1)
    
    # Merge mappings
    sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='left')
    dio_data = pd.merge(sales_mapped, wh_sum[['title', 'unit_cost']], left_on='inventory_title', right_on='title', how='left')
    
    dio_data['unit_cost'] = dio_data['unit_cost'].fillna(0)
    total_cogs = (dio_data['quantity_sold'] * dio_data['unit_cost']).sum()
    avg_dio = (wh_sum['Value'].sum() / (total_cogs + 1)) * days_in_period
    ccc = avg_dso + avg_dio - avg_dpo

    # --- TAB 1: DASHBOARD ---
    with tab_dash:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("DSO (Receivables)", f"{avg_dso:.1f}", f"{avg_dso - target_dso:+.1f}", delta_color="inverse")
        m2.metric("DIO (Inventory)", f"{avg_dio:.1f}", f"{avg_dio - target_dio:+.1f}", delta_color="inverse")
        m3.metric("DPO (Payables)", f"{avg_dpo:.1f}", f"{avg_dpo - target_dpo:+.1f}")
        m4.metric("Cash Cycle (CCC)", f"{ccc:.1f}")

        st.divider()
        st.subheader("💡 Strategic Deep Dive")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("### 🔴 Collections Focus")
            top_debtor = df_inv.groupby('customer_name')['bcy_balance'].sum().idxmax()
            top_val = df_inv.groupby('customer_name')['bcy_balance'].sum().max()
            st.write(f"Prioritize **{top_debtor}** (₹{top_val:,.0f} outstanding).")
        with c2:
            st.markdown("### 🟡 Inventory Focus")
            st.write(f"Total Working Capital tied in Stock: **₹{wh_sum['Value'].sum():,.0f}**.")

        cust_bal = df_inv.groupby('customer_name')['bcy_balance'].sum().reset_index().sort_values('bcy_balance', ascending=False).head(10)
        st.plotly_chart(px.bar(cust_bal, x='customer_name', y='bcy_balance', title="Top 10 AR Balances"), use_container_width=True)

    # --- TAB 2: INVENTORY AGEING ---
    with tab_ageing:
        st.header("⏳ Inventory Health & Ageing")
        item_stats = dio_data.groupby('inventory_title').agg({'quantity_sold':'sum', 'unit_cost':'first'}).reset_index()
        item_stats = pd.merge(wh_sum, item_stats, left_on='title', right_on='inventory_title', how='left')
        
        # Ensure unit_cost exists before calculation
        if 'unit_cost_x' in item_stats.columns:
            item_stats['unit_cost'] = item_stats['unit_cost_x']

        item_stats['Item_DIO'] = (item_stats['Value'] / ((item_stats['quantity_sold'].fillna(0) * item_stats['unit_cost'].fillna(0)) + 1)) * days_in_period
        
        def bucket_age(d):
            if d <= 30: return "0-30 Days (Fast)"
            if d <= 60: return "31-60 Days (Healthy)"
            if d <= 90: return "61-90 Days (Slow)"
            return "90+ Days (High Risk)"
        
        item_stats['Ageing Bucket'] = item_stats['Item_DIO'].apply(bucket_age)
        
        fig_age = px.pie(item_stats, values='Value', names='Ageing Bucket', hole=0.4, 
                         color_discrete_map={"0-30 Days (Fast)": "#2ecc71", "31-60 Days (Healthy)": "#3498db", "61-90 Days (Slow)": "#f1c40f", "90+ Days (High Risk)": "#e74c3c"})
        st.plotly_chart(fig_age, use_container_width=True)
        st.dataframe(item_stats[['title', 'Qty', 'Value', 'Item_DIO', 'Ageing Bucket']].sort_values('Value', ascending=False), use_container_width=True)

    # --- TAB 3: MAPPINGS (WITH BULK TOOL) ---
    with tab_map:
        st.header("🔧 Mapping Management")
        
        unmapped = [n for n in df_sales['item_name'].unique() if n not in df_map['zoho_name'].tolist()]
        
        if unmapped:
            st.subheader("⚡ Bulk Suggestion Tool")
            st.info(f"Found {len(unmapped)} unmapped Zoho items. I can suggest matches based on name similarity.")
            
            suggestions = []
            for item in unmapped:
                match = get_close_matches(item, wh_sum['title'].tolist(), n=1, cutoff=0.1)
                suggestions.append({"Zoho Name": item, "Suggested Warehouse SKU": match[0] if match else "No Match Found"})
            
            suggest_df = pd.DataFrame(suggestions)
            st.table(suggest_df)
            
            if st.button("Apply All Suggestions"):
                with conn.session as s:
                    for _, row in suggest_df.iterrows():
                        if row['Suggested Warehouse SKU'] != "No Match Found":
                            s.execute(text("INSERT INTO item_mappings VALUES (:z, :i) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), 
                                      {"z": row['Zoho Name'], "i": row['Suggested Warehouse SKU']})
                    s.commit()
                st.rerun()

            st.divider()
            st.subheader("Manual Mapping")
            item_to_map = st.selectbox("Manually Select Zoho Item:", unmapped)
            warehouse_match = st.selectbox("Link to Warehouse SKU:", sorted(wh_sum['title'].unique()))
            if st.button("Save Manual Mapping"):
                with conn.session as s:
                    s.execute(text("INSERT INTO item_mappings VALUES (:z, :i) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), {"z":item_to_map, "i":warehouse_match})
                    s.commit()
                st.rerun()
        
        st.subheader("Current Mappings")
        st.dataframe(df_map, use_container_width=True)

else:
    st.info("Please upload your data files to get started.")
