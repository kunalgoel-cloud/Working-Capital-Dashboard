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

# --- SIDEBAR: OBJECTIVES & FILTERS ---
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

    # --- ITEM LEVEL FILTER ---
    item_filter = "All Items"
    if f_sales:
        df_temp = pd.read_csv(f_sales)
        item_list = ["All Items"] + sorted(df_temp['item_name'].unique().tolist())
        item_filter = st.selectbox("🔍 Filter Metrics by Item", item_list)

# --- TABS ---
tab_dash, tab_insights, tab_map = st.tabs(["📊 Performance Dashboard", "💡 Deep Dive & Actions", "🔧 Manage Mappings"])

if all([f_inv, f_bill, f_sales, f_wh]):
    # Load Data
    df_inv, df_bill = pd.read_csv(f_inv), pd.read_csv(f_bill)
    df_sales, df_wh = pd.read_csv(f_sales), pd.read_csv(f_wh)
    df_map = conn.query("SELECT * FROM item_mappings", ttl=0)

    # Global Calculations (Total Business)
    days = (date_range[1] - date_range[0]).days or 365
    total_sales_val = df_inv['bcy_total'].sum()
    avg_dso = (df_inv['bcy_balance'].sum() / (total_sales_val + 1)) * days
    avg_dpo = (df_bill['bcy_balance'].sum() / (df_bill['bcy_total'].sum() + 1)) * days

    # DIO Calculation Logic
    wh_costs = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
    wh_costs['unit_cost'] = wh_costs['Value'] / wh_costs['Qty'].replace(0, 1)
    
    # Filter sales if item selected
    df_sales_calc = df_sales.copy()
    if item_filter != "All Items":
        df_sales_calc = df_sales[df_sales['item_name'] == item_filter]

    sales_mapped = pd.merge(df_sales_calc, df_map, left_on='item_name', right_on='zoho_name', how='inner')
    dio_merge = pd.merge(sales_mapped, wh_costs, left_on='inventory_title', right_on='title', how='left')
    
    dio_merge['unit_cost'] = dio_merge['unit_cost'].fillna(wh_costs['unit_cost'].mean() or 0)
    total_cogs = (dio_merge['quantity_sold'] * dio_merge['unit_cost']).sum()
    
    # Inventory value logic: If item filtered, only show that item's value
    if item_filter == "All Items":
        current_inv_val = wh_costs['Value'].sum()
    else:
        # Get the mapped title for the filtered item to find its warehouse value
        mapped_title = dio_merge['inventory_title'].iloc[0] if not dio_merge.empty else None
        current_inv_val = wh_costs[wh_costs['title'] == mapped_title]['Value'].sum() if mapped_title else 0

    avg_dio = (current_inv_val / (total_cogs + 1)) * days if total_cogs > 10 else 0
    ccc = avg_dso + avg_dio - avg_dpo

    # --- TAB 1: DASHBOARD ---
    with tab_dash:
        st.header(f"Dashboard: {item_filter}")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("DSO (Receivables)", f"{avg_dso:.1f}", f"{avg_dso - target_dso:+.1f} vs Target", delta_color="inverse")
        m2.metric("DIO (Inventory)", f"{avg_dio:.1f}", f"{avg_dio - target_dio:+.1f} vs Target", delta_color="inverse")
        m3.metric("DPO (Payables)", f"{avg_dpo:.1f}", f"{avg_dpo - target_dpo:+.1f} vs Target")
        m4.metric("Cash Cycle (CCC)", f"{ccc:.1f}")

        st.subheader("Customer Balances (All Items)")
        cust_table = df_inv.groupby('customer_name').agg({'bcy_balance':'sum', 'bcy_total':'sum'}).reset_index()
        st.dataframe(cust_table.sort_values('bcy_balance', ascending=False), use_container_width=True)

    # --- TAB 2: DEEP DIVE & ACTIONS ---
    with tab_insights:
        st.header("💡 Strategic Insights & Actions")
        
        # Recommendation Engine
        with st.expander("🚀 Immediate Action Plan", expanded=True):
            recs = []
            if avg_dso > target_dso:
                top_debtor = cust_table.sort_values('bcy_balance', ascending=False).iloc[0]['customer_name']
                recs.append(f"🔴 **DSO is high ({avg_dso:.1f} days):** Your cash is trapped in unpaid invoices. **Action:** Call **{top_debtor}** and top debtors to collect payments.")
            if avg_dio > target_dio:
                recs.append(f"🟠 **Inventory is heavy ({avg_dio:.1f} days):** You have more than {avg_dio/30:.1f} months of stock. **Action:** Run a promotion for slow-moving SKUs.")
            if avg_dpo < target_dpo:
                recs.append(f"⚠️ **Payables are fast ({avg_dpo:.1f} days):** You are paying vendors before your goal of {target_dpo} days. **Action:** Negotiate longer credit terms to keep cash in the bank.")
            
            if not recs:
                st.success("✅ All metrics are within target! Maintain current operations.")
            else:
                for r in recs:
                    st.markdown(r)

        st.divider()
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("High-Impact Debtors")
            st.caption("Customers with balances > 50,000 BCY")
            st.dataframe(cust_table[cust_table['bcy_balance'] > 50000].sort_values('bcy_balance', ascending=False), use_container_width=True)
        
        with c2:
            st.subheader("Inventory Value Breakdown")
            st.caption("Top 15 SKUs by Warehouse Value")
            st.dataframe(wh_costs.sort_values('Value', ascending=False).head(15), use_container_width=True)

    # --- TAB 3: MANAGE MAPPINGS ---
    with tab_map:
        st.header("Mapping Management")
        # Logic to handle unmapped items
        zoho_names = df_sales['item_name'].unique()
        mapped_names = set(df_map['zoho_name'].tolist())
        unmapped = [n for n in zoho_names if n not in mapped_names]
        
        if unmapped:
            st.warning(f"{len(unmapped)} items need mapping.")
            item_to_map = unmapped[0]
            col_a, col_b = st.columns([3, 1])
            matches = get_close_matches(str(item_to_map), list(wh_costs['title']), n=1, cutoff=0.3)
            default = matches[0] if matches else "DISCONTINUED / OLD SKU"
            
            selection = col_a.selectbox(f"Map '{item_to_map}'", sorted(list(wh_costs['title']) + ["DISCONTINUED / OLD SKU"]), index=0)
            if col_b.button("Save Mapping"):
                with conn.session as s:
                    s.execute(text("INSERT INTO item_mappings VALUES (:z, :i) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), {"z":item_to_map, "i":selection})
                    s.commit()
                st.rerun()
        
        st.subheader("Edit Existing Mappings")
        st.dataframe(df_map, use_container_width=True)

else:
    st.info("Please upload all 4 files (Invoices, Bills, Sales, Warehouse) to see the deep dive.")
