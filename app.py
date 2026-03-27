import streamlit as st
import pandas as pd
from sqlalchemy import text
from datetime import datetime
import time
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
    target_dso = st.number_input("Target Receivable Days (DSO)", value=120)
    target_dio = st.number_input("Target Inventory Days (DIO)", value=45)
    target_dpo = st.number_input("Target Payable Days (DPO)", value=90)

    st.header("📂 Data Upload")
    f_inv = st.file_uploader("Invoices", type="csv")
    f_bill = st.file_uploader("Bills", type="csv")
    f_sales = st.file_uploader("Sales Items", type="csv")
    f_wh = st.file_uploader("Warehouse Export", type="csv")
    date_range = st.date_input("Analysis Period", [pd.to_datetime("2025-04-01"), pd.to_datetime("2026-03-31")])

# --- TABS ---
tab_dash, tab_insights, tab_map = st.tabs(["📊 Performance Dashboard", "💡 Deep Dive & Actions", "🔧 Manage Mappings"])

# --- WRAP EVERYTHING IN A CHECK TO PREVENT EMPTYDATAERROR ---
if f_inv and f_bill and f_sales and f_wh:
    # 1. Load Data safely inside the IF block
    df_inv = pd.read_csv(f_inv)
    df_bill = pd.read_csv(f_bill)
    df_sales = pd.read_csv(f_sales)
    df_wh = pd.read_csv(f_wh)
    
    # 2. Fetch Mappings
    df_map = conn.query("SELECT * FROM item_mappings", ttl=0)

    # 3. Sidebar Item Filter (Populated dynamically from the uploaded Sales file)
    with st.sidebar:
        st.divider()
        item_list = ["All Items"] + sorted(df_sales['item_name'].unique().tolist())
        selected_item = st.selectbox("🔍 Filter Metrics by Item", item_list)

    # 4. Global Calculations
    days = (date_range[1] - date_range[0]).days or 365
    
    # DSO Calculation (Total AR / Total Sales)
    total_sales_val = df_inv['bcy_total'].sum()
    avg_dso = (df_inv['bcy_balance'].sum() / (total_sales_val + 1)) * days
    
    # DPO Calculation
    avg_dpo = (df_bill['bcy_balance'].sum() / (df_bill['bcy_total'].sum() + 1)) * days

    # 5. DIO & Item Filtering Logic
    wh_costs = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
    wh_costs['unit_cost'] = wh_costs['Value'] / wh_costs['Qty'].replace(0, 1)
    
    # Merge Sales + Mapping + Costs
    sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
    dio_merge = pd.merge(sales_mapped, wh_costs, left_on='inventory_title', right_on='title', how='left')
    dio_merge['unit_cost'] = dio_merge['unit_cost'].fillna(wh_costs['unit_cost'].mean() or 0)
    
    # Apply filter for DIO calculation
    if selected_item != "All Items":
        plot_df = dio_merge[dio_merge['item_name'] == selected_item]
        current_inv_val = plot_df['Value'].sum()
        item_cogs = (plot_df['quantity_sold'] * plot_df['unit_cost']).sum()
        display_dio = (current_inv_val / (item_cogs + 1)) * days
    else:
        total_cogs = (dio_merge['quantity_sold'] * dio_merge['unit_cost']).sum()
        display_dio = (wh_costs['Value'].sum() / (total_cogs + 1)) * days

    ccc = avg_dso + display_dio - avg_dpo

    # --- TAB 1: DASHBOARD ---
    with tab_dash:
        st.header(f"Performance Snapshot: {selected_item}")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("DSO (Receivables)", f"{avg_dso:.1f}", f"{avg_dso - target_dso:+.1f} vs Goal", delta_color="inverse")
        m2.metric("DIO (Inventory)", f"{display_dio:.1f}", f"{display_dio - target_dio:+.1f} vs Goal", delta_color="inverse")
        m3.metric("DPO (Payables)", f"{avg_dpo:.1f}", f"{avg_dpo - target_dpo:+.1f} vs Goal")
        m4.metric("Cash Cycle (CCC)", f"{ccc:.1f}")

        st.subheader("Top Debtor List")
        cust_table = df_inv.groupby('customer_name').agg({'bcy_balance':'sum', 'bcy_total':'sum'}).reset_index()
        st.dataframe(cust_table.sort_values('bcy_balance', ascending=False), use_container_width=True)

    # --- TAB 2: DEEP DIVE & ACTIONS ---
    with tab_insights:
        st.header("💡 Strategic Insights & Objectives")
        
        # Automated Suggestions
        if avg_dso > target_dso:
            st.error(f"🚩 **High DSO:** You are collecting cash {avg_dso - target_dso:.0f} days slower than your objective. Prioritize collection from your top 3 debtors.")
        
        if display_dio > target_dio:
            st.warning(f"📦 **Inventory Bloat:** Your DIO is {display_dio:.1f} days. Goal is {target_dio}. Consider liquidating slow-moving stock.")

        st.divider()
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Debtor Detail (DSO Impact)")
            st.dataframe(cust_table[cust_table['bcy_balance'] > 0].sort_values('bcy_balance', ascending=False), use_container_width=True)
        with c2:
            st.subheader("Inventory Distribution (DIO Impact)")
            st.dataframe(wh_costs.sort_values('Value', ascending=False), use_container_width=True)

    # --- TAB 3: MAPPINGS ---
    with tab_map:
        st.header("Mapping Management")
        unmapped = [n for n in df_sales['item_name'].unique() if n not in df_map['zoho_name'].tolist()]
        
        if unmapped:
            item = unmapped[0]
            col_a, col_b = st.columns([3, 1])
            choice = col_a.selectbox(f"Map '{item}'", sorted(list(wh_costs['title'].unique()) + ["DISCONTINUED"]), key="map_box")
            if col_b.button("Save & Link"):
                with conn.session as s:
                    s.execute(text("INSERT INTO item_mappings VALUES (:z, :i) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), {"z":item, "i":choice})
                    s.commit()
                st.rerun()
        
        st.subheader("Existing Mappings")
        st.dataframe(df_map, use_container_width=True)

else:
    st.info("👋 Please upload all four CSV files in the sidebar to begin analysis.")
