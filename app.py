import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
import time
from difflib import get_close_matches

st.set_page_config(page_title="Working Capital Dashboard", layout="wide")

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

# --- SIDEBAR ---
with st.sidebar:
    st.header("1. Data Upload")
    f_inv = st.file_uploader("Invoices", type="csv")
    f_bill = st.file_uploader("Bills", type="csv")
    f_sales = st.file_uploader("Sales Items", type="csv")
    f_wh = st.file_uploader("Warehouse Export", type="csv")
    date_range = st.date_input("Analysis Period", [pd.to_datetime("2025-04-01"), pd.to_datetime("2026-03-31")])

# --- MAIN INTERFACE ---
tab_dash, tab_map = st.tabs(["📊 Dashboard", "🔧 Manage Mappings"])

if all([f_inv, f_bill, f_sales, f_wh]):
    # Load Data
    df_inv = pd.read_csv(f_inv)
    df_bill = pd.read_csv(f_bill)
    df_sales = pd.read_csv(f_sales)
    df_wh = pd.read_csv(f_wh)
    
    # Standardize types
    df_inv['date'] = pd.to_datetime(df_inv['date']).dt.date
    df_bill['date'] = pd.to_datetime(df_bill['date']).dt.date
    
    # Load existing mappings
    df_map = conn.query("SELECT * FROM item_mappings")

    # Master SKU list for dropdowns
    current_wh_titles = list(df_wh['title'].unique())
    hist_titles = df_map['inventory_title'].unique().tolist()
    master_sku_list = sorted(list(set(current_wh_titles + hist_titles + ["DISCONTINUED / OLD SKU"])))

    with tab_map:
        st.header("Mapping Management")
        
        # Determine items in file that aren't in DB yet
        zoho_in_file = df_sales['item_name'].unique()
        mapped_in_db = df_map['zoho_name'].values
        unmapped = [n for n in zoho_in_file if n not in mapped_in_db]
        
        # --- SECTION: NEW MAPPINGS ---
        if unmapped:
            st.subheader(f"New Unique Items Found ({len(unmapped)})")
            for item in unmapped[:5]:
                c1, c2 = st.columns([3, 1])
                
                # Fuzzy Logic
                matches = get_close_matches(str(item), current_wh_titles, n=1, cutoff=0.3)
                default_val = matches[0] if matches else "DISCONTINUED / OLD SKU"
                
                choice = c1.selectbox(f"Map '{item}'", master_sku_list, 
                                    index=master_sku_list.index(default_val), 
                                    key=f"new_{item}")
                
                if c2.button("Save & Link", key=f"btn_{item}"):
                    with conn.session as s:
                        # FIXED: Use ON CONFLICT to prevent IntegrityError
                        s.execute(text("""
                            INSERT INTO item_mappings (zoho_name, inventory_title) 
                            VALUES (:z, :i) 
                            ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title
                        """), {"z":item, "i":choice})
                        s.commit()
                    st.toast(f"Saved {item}")
                    time.sleep(0.2)
                    st.rerun()
        else:
            st.success("All items in your current file are already mapped.")

        st.divider()
        
        # --- SECTION: EDIT MAPPINGS ---
        st.subheader("Edit/Review Existing Mappings")
        search = st.text_input("Search mappings...")
        
        display_edit = df_map
        if search:
            display_edit = df_map[df_map['zoho_name'].str.contains(search, case=False)]

        for idx, row in display_edit.head(10).iterrows():
            c1, c2, c3 = st.columns([3, 3, 1])
            stock_status = "🟢 In Stock" if row['inventory_title'] in current_wh_titles else "🔴 Out of Stock"
            c1.markdown(f"**{row['zoho_name']}**\n\n*{stock_status}*")
            
            new_choice = c2.selectbox("Update to:", master_sku_list, 
                                    index=master_sku_list.index(row['inventory_title']) if row['inventory_title'] in master_sku_list else 0, 
                                    key=f"ed_{row['zoho_name']}")
            
            if c3.button("Update", key=f"upd_{row['zoho_name']}"):
                with conn.session as s:
                    s.execute(text("UPDATE item_mappings SET inventory_title = :i WHERE zoho_name = :z"), 
                              {"i":new_choice, "z":row['zoho_name']})
                    s.commit()
                st.toast("Updated!")
                st.rerun()

    with tab_dash:
        if len(date_range) == 2:
            start, end = date_range[0], date_range[1]
            days = (end - start).days or 365
            
            # DSO & DPO
            mask_inv = (df_inv['date'] >= start) & (df_inv['date'] <= end)
            mask_bill = (df_bill['date'] >= start) & (df_bill['date'] <= end)
            
            dso = (df_inv.loc[mask_inv, 'bcy_balance'].sum() / (df_inv.loc[mask_inv, 'bcy_total'].sum() + 1)) * days
            dpo = (df_bill.loc[mask_bill, 'bcy_balance'].sum() / (df_bill.loc[mask_bill, 'bcy_total'].sum() + 1)) * days
            
            # DIO (Inventory)
            sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
            inv_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
            inv_sum['unit_cost'] = inv_sum['Value'] / (inv_sum['Qty'] + 0.001)
            
            final_df = pd.merge(sales_mapped, inv_sum, left_on='inventory_title', right_on='title', how='left')
            final_df[['Value', 'unit_cost']] = final_df[['Value', 'unit_cost']].fillna(0)
            
            total_cogs = (final_df['quantity_sold'] * final_df['unit_cost']).sum()
            dio = (inv_sum['Value'].sum() / (total_cogs + 1)) * days

            # Metrics
            st.header(f"Performance Metrics: {start} to {end}")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("DSO (Receivables)", f"{dso:.1f}")
            m2.metric("DIO (Inventory)", f"{dio:.1f}")
            m3.metric("DPO (Payables)", f"{dpo:.1f}")
            m4.metric("Cash Cycle", f"{(dso + dio - dpo):.1f}")
            
            st.plotly_chart(px.bar(df_inv.loc[mask_inv].groupby('customer_name')['bcy_balance'].sum().nlargest(10).reset_index(), x='customer_name', y='bcy_balance'))
else:
    st.info("Please upload your files to view the dashboard.")
