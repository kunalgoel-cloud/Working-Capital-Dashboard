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
    
    # Force date conversion for filters
    df_inv['date'] = pd.to_datetime(df_inv['date']).dt.date
    df_bill['date'] = pd.to_datetime(df_bill['date']).dt.date
    
    # Load mappings from Postgres
    df_map = conn.query("SELECT * FROM item_mappings")

    # Prepare Master List (Warehouse CSV + Anything ever mapped)
    current_wh_titles = list(df_wh['title'].unique())
    historical_mapped_titles = df_map['inventory_title'].unique().tolist()
    master_sku_list = sorted(list(set(current_wh_titles + historical_mapped_titles + ["DISCONTINUED / OLD SKU"])))

    with tab_map:
        st.header("Mapping Management")
        
        # LOGIC: Identify only Zoho items NOT in the database
        zoho_items_in_file = df_sales['item_name'].unique()
        mapped_in_db = df_map['zoho_name'].values
        unmapped = [n for n in zoho_items_in_file if n not in mapped_in_db]
        
        # --- SECTION 1: NEW UNIQUE MAPPINGS ONLY ---
        if unmapped:
            st.subheader(f"New Items to Map ({len(unmapped)})")
            st.caption("Items saved here will move to the 'Edit/Review' list below.")
            
            # Show top 5 to keep UI fast
            for item in unmapped[:5]:
                c1, c2 = st.columns([3, 1])
                
                # Fuzzy Suggestion
                matches = get_close_matches(str(item), current_wh_titles, n=1, cutoff=0.3)
                default_val = matches[0] if matches else "DISCONTINUED / OLD SKU"
                
                choice = c1.selectbox(f"Map '{item}'", master_sku_list, 
                                    index=master_sku_list.index(default_val), 
                                    key=f"new_{item}")
                
                if c2.button("Save & Link", key=f"btn_{item}"):
                    with conn.session as s:
                        s.execute(text("INSERT INTO item_mappings VALUES (:z, :i)"), {"z":item, "i":choice})
                        s.commit()
                    st.toast(f"Linked {item} → {choice}")
                    time.sleep(0.3)
                    st.rerun()
        else:
            st.success("✅ All items in your current Sales file are already mapped.")

        st.divider()
        
        # --- SECTION 2: EDIT / REVIEW LIST ---
        st.subheader("Edit/Review Existing Mappings")
        search = st.text_input("🔍 Search database by Zoho name or Warehouse SKU...")
        
        # Filter the edit list based on search
        if search:
            display_edit = df_map[(df_map['zoho_name'].str.contains(search, case=False)) | 
                                  (df_map['inventory_title'].str.contains(search, case=False))]
        else:
            display_edit = df_map

        if not display_edit.empty:
            for idx, row in display_edit.head(10).iterrows():
                c1, c2, c3 = st.columns([3, 3, 1])
                
                # Show stock status for context
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
                    st.toast("Mapping Updated!")
                    time.sleep(0.2)
                    st.rerun()
        else:
            st.write("No existing mappings found.")

    with tab_dash:
        if len(date_range) == 2:
            start_date, end_date = date_range[0], date_range[1]
            days = (end_date - start_date).days or 365
            
            # Filter DB data by selected date range
            mask_inv = (df_inv['date'] >= start_date) & (df_inv['date'] <= end_date)
            mask_bill = (df_bill['date'] >= start_date) & (df_bill['date'] <= end_date)
            
            curr_inv = df_inv.loc[mask_inv]
            curr_bill = df_bill.loc[mask_bill]

            # --- CALCULATIONS ---
            # DSO & DPO
            avg_dso = (curr_inv['bcy_balance'].sum() / (curr_inv['bcy_total'].sum() + 1)) * days
            avg_dpo = (curr_bill['bcy_balance'].sum() / (curr_bill['bcy_total'].sum() + 1)) * days
            
            # DIO (Handles Out of Stock via Left Join)
            sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
            inv_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
            inv_sum['unit_cost'] = inv_sum['Value'] / (inv_sum['Qty'] + 0.001)
            
            final_df = pd.merge(sales_mapped, inv_sum, left_on='inventory_title', right_on='title', how='left')
            final_df[['Value', 'unit_cost']] = final_df[['Value', 'unit_cost']].fillna(0)
            
            total_cogs = (final_df['quantity_sold'] * final_df['unit_cost']).sum()
            avg_dio = (inv_sum['Value'].sum() / (total_cogs + 1)) * days

            # --- DASHBOARD DISPLAY ---
            st.header(f"Performance: {start_date} to {end_date}")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("DSO (Receivables)", f"{avg_dso:.1f} Days")
            m2.metric("DIO (Inventory)", f"{avg_dio:.1f} Days")
            m3.metric("DPO (Payables)", f"{avg_dpo:.1f} Days")
            m4.metric("Cash Conversion Cycle", f"{(avg_dso + avg_dio - avg_dpo):.1f} Days", 
                      delta=f"{avg_dso + avg_dio - avg_dpo:.1f}", delta_color="inverse")

            st.divider()
            
            col_a, col_b = st.columns(2)
            with col_a:
                st.subheader("Top Customers by Balance")
                st.plotly_chart(px.bar(curr_inv.groupby('customer_name')['bcy_balance'].sum().nlargest(10).reset_index(), 
                                       x='customer_name', y='bcy_balance'), use_container_width=True)
            with col_b:
                st.subheader("Live SKUs Currently Out of Stock")
                oos = final_df[final_df['Qty'].isna() | (final_df['Qty'] == 0)]
                st.dataframe(oos[['item_name', 'quantity_sold', 'amount']], use_container_width=True)

else:
    st.info("Waiting for file uploads to initialize mapping and dashboard.")
