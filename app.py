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
        s.execute(text("CREATE TABLE IF NOT EXISTS invoices (invoice_id BIGINT PRIMARY KEY, customer_name TEXT, bcy_total FLOAT, bcy_balance FLOAT, date DATE);"))
        s.execute(text("CREATE TABLE IF NOT EXISTS bills (bill_id BIGINT PRIMARY KEY, vendor_name TEXT, bcy_total FLOAT, bcy_balance FLOAT, date DATE);"))
        s.execute(text("CREATE TABLE IF NOT EXISTS sales_items (item_id BIGINT PRIMARY KEY, item_name TEXT, quantity_sold FLOAT, amount FLOAT);"))
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

# --- TABS ---
tab_dash, tab_insights, tab_map = st.tabs(["📊 Performance Dashboard", "💡 Deep Dive & Actions", "🔧 Manage Mappings"])

if all([f_inv, f_bill, f_sales, f_wh]):
    # Load Data
    df_inv, df_bill = pd.read_csv(f_inv), pd.read_csv(f_bill)
    df_sales, df_wh = pd.read_csv(f_sales), pd.read_csv(f_wh)
    
    # Standardize Dates
    df_inv['date'] = pd.to_datetime(df_inv['date']).dt.date
    df_bill['date'] = pd.to_datetime(df_bill['date']).dt.date

    # Load Existing Mappings (No Cache)
    df_map = conn.query("SELECT * FROM item_mappings", ttl=0)
    
    # Master SKU List for Dropdowns
    current_wh_titles = list(df_wh['title'].unique())
    master_sku_list = sorted(list(set(current_wh_titles + df_map['inventory_title'].unique().tolist() + ["DISCONTINUED / OLD SKU"])))

    # --- TAB: MANAGE MAPPINGS ---
    with tab_map:
        st.header("Item Mapping Management")
        
        # 1. New Items Logic
        zoho_in_file = df_sales['item_name'].unique()
        mapped_names = set(df_map['zoho_name'].tolist())
        unmapped = [n for n in zoho_in_file if n not in mapped_names]
        
        if unmapped:
            st.subheader(f"⚠️ New Items to Map ({len(unmapped)})")
            item = unmapped[0] 
            c1, c2 = st.columns([3, 1])
            matches = get_close_matches(str(item), current_wh_titles, n=1, cutoff=0.3)
            default_val = matches[0] if matches else "DISCONTINUED / OLD SKU"
            
            choice = c1.selectbox(f"Map '{item}'", master_sku_list, index=master_sku_list.index(default_val), key="new_map")
            if c2.button("Save & Link", use_container_width=True):
                with conn.session as s:
                    s.execute(text("INSERT INTO item_mappings VALUES (:z, :i) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), {"z":item, "i":choice})
                    s.commit()
                st.rerun()
        else:
            st.success("✅ All items in current file are mapped.")

        st.divider()
        
        # 2. Edit Existing Mappings
        st.subheader("📋 Existing Mappings (Search & Edit)")
        search_map = st.text_input("Search mappings...")
        display_edit = df_map
        if search_map:
            display_edit = df_map[df_map['zoho_name'].str.contains(search_map, case=False)]
        
        for idx, row in display_edit.head(10).iterrows():
            c1, c2, c3 = st.columns([3, 3, 1])
            stock_label = "🟢 In Stock" if row['inventory_title'] in current_wh_titles else "🔴 Out of Stock"
            c1.markdown(f"**{row['zoho_name']}**\n\n*{stock_label}*")
            
            curr_idx = master_sku_list.index(row['inventory_title']) if row['inventory_title'] in master_sku_list else 0
            new_choice = c2.selectbox("Update to:", master_sku_list, index=curr_idx, key=f"ed_{row['zoho_name']}")
            if c3.button("Update", key=f"btn_{row['zoho_name']}"):
                with conn.session as s:
                    s.execute(text("UPDATE item_mappings SET inventory_title = :i WHERE zoho_name = :z"), {"i":new_choice, "z":row['zoho_name']})
                    s.commit()
                st.toast("Updated!")
                st.rerun()

    # --- CALCULATIONS (Dashboard & Insights) ---
    days = (date_range[1] - date_range[0]).days or 365
    
    # DSO & DPO
    mask_inv = (df_inv['date'] >= date_range[0]) & (df_inv['date'] <= date_range[1])
    mask_bill = (df_bill['date'] >= date_range[0]) & (df_bill['date'] <= date_range[1])
    
    total_sales = df_inv.loc[mask_inv, 'bcy_total'].sum()
    avg_dso = (df_inv.loc[mask_inv, 'bcy_balance'].sum() / (total_sales + 1)) * days
    
    total_bills = df_bill.loc[mask_bill, 'bcy_total'].sum()
    avg_dpo = (df_bill.loc[mask_bill, 'bcy_balance'].sum() / (total_bills + 1)) * days

    # DIO with Safety Rails
    wh_costs = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
    wh_costs['unit_cost'] = wh_costs['Value'] / wh_costs['Qty'].replace(0, 1)
    
    sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
    dio_merge = pd.merge(sales_mapped, wh_costs, left_on='inventory_title', right_on='title', how='left')
    
    dio_merge['unit_cost'] = dio_merge['unit_cost'].fillna(wh_costs['unit_cost'].mean() or 0)
    total_cogs = (dio_merge['quantity_sold'] * dio_merge['unit_cost']).sum()
    
    avg_dio = (wh_costs['Value'].sum() / (total_cogs + 1)) * days if total_cogs > 100 else 0
    ccc = avg_dso + avg_dio - avg_dpo

    with tab_dash:
        st.header("Business Health Snapshot")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("DSO (Receivables)", f"{avg_dso:.1f}", f"{avg_dso - target_dso:+.1f} vs Target", delta_color="inverse")
        m2.metric("DIO (Inventory)", f"{avg_dio:.1f}", f"{avg_dio - target_dio:+.1f} vs Target", delta_color="inverse")
        m3.metric("DPO (Payables)", f"{avg_dpo:.1f}", f"{avg_dpo - target_dpo:+.1f} vs Target")
        m4.metric("Cash Cycle (CCC)", f"{ccc:.1f}")

        st.subheader("📋 Customer Balances")
        cust_table = df_inv.loc[mask_inv].groupby('customer_name').agg({'bcy_balance':'sum', 'bcy_total':'sum'}).reset_index()
        st.dataframe(cust_table.sort_values('bcy_balance', ascending=False), use_container_width=True)

    with tab_insights:
        st.header("Strategic Insights")
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Top Debtors (DSO Impact)")
            st.dataframe(cust_table[cust_table['bcy_balance'] > 0].sort_values('bcy_balance', ascending=False), use_container_width=True)
        with col2:
            st.subheader("Inventory Distribution")
            st.dataframe(wh_costs[['title', 'Qty', 'Value']].sort_values('Value', ascending=False).head(15), use_container_width=True)
else:
    st.info("Upload files to start.")
