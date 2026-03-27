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
    df_inv = pd.read_csv(f_inv)
    df_bill = pd.read_csv(f_bill)
    df_sales = pd.read_csv(f_sales)
    df_wh = pd.read_csv(f_wh)
    
    # Load mappings from Postgres
    df_map = conn.query("SELECT * FROM item_mappings")

    # --- PREPARE MASTER SKU LIST (Current + Historical) ---
    current_wh_titles = list(df_wh['title'].unique())
    historical_titles = df_map['inventory_title'].unique().tolist()
    master_sku_list = sorted(list(set(current_wh_titles + historical_titles + ["DISCONTINUED / OLD SKU"])))

    with tab_map:
        st.header("Mapping Management")
        unmapped = [n for n in df_sales['item_name'].unique() if n not in df_map['zoho_name'].values]
        
        if unmapped:
            st.subheader(f"New Items to Map ({len(unmapped)})")
            
            for item in unmapped[:10]:
                c1, c2 = st.columns([3, 1])
                
                # Fuzzy Suggestion
                matches = get_close_matches(item, master_sku_list, n=1, cutoff=0.3)
                default_val = matches[0] if matches else "DISCONTINUED / OLD SKU"
                
                choice = c1.selectbox(f"Map '{item}'", master_sku_list, index=master_sku_list.index(default_val), key=f"new_{item}")
                
                if c2.button("Save", key=f"btn_{item}"):
                    with conn.session as s:
                        s.execute(text("INSERT INTO item_mappings VALUES (:z, :i) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), {"z":item, "i":choice})
                        s.commit()
                    st.toast(f"Mapped {item}")
                    time.sleep(0.2)
                    st.rerun()
        
        st.divider()
        st.subheader("Edit Existing Mappings")
        search = st.text_input("Search items...")
        edit_df = df_map[df_map['zoho_name'].str.contains(search, case=False)] if search else df_map
        
        for idx, row in edit_df.head(10).iterrows():
            c1, c2, c3 = st.columns([3, 3, 1])
            c1.text(row['zoho_name'])
            # Status check for out-of-stock items
            status = "✅ In Stock" if row['inventory_title'] in current_wh_titles else "⚠️ Out of Stock"
            c1.caption(status)
            
            new_choice = c2.selectbox("Change to:", master_sku_list, index=master_sku_list.index(row['inventory_title']) if row['inventory_title'] in master_sku_list else 0, key=f"ed_{row['zoho_name']}")
            if c3.button("Update", key=f"upd_{row['zoho_name']}"):
                with conn.session as s:
                    s.execute(text("UPDATE item_mappings SET inventory_title = :i WHERE zoho_name = :z"), {"i":new_choice, "z":row['zoho_name']})
                    s.commit()
                st.rerun()

    with tab_dash:
        # Check if dates are valid
        if len(date_range) == 2:
            start_date, end_date = date_range[0], date_range[1]
            days = (end_date - start_date).days or 365
            
            # --- CALCULATIONS ---
            # 1. DSO (Receivables)
            avg_dso = (df_inv['bcy_balance'].sum() / (df_inv['bcy_total'].sum() + 1)) * days
            
            # 2. DPO (Payables)
            avg_dpo = (df_bill['bcy_balance'].sum() / (df_bill['bcy_total'].sum() + 1)) * days
            
            # 3. DIO (Inventory - Handles Out of Stock)
            # Merge sales with mappings
            sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
            
            # Inventory Summary (Current Stock)
            inv_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
            inv_sum['unit_cost'] = inv_sum['Value'] / (inv_sum['Qty'] + 0.001)
            
            # Merge sales-mappings with inventory (Left join to keep items with 0 stock)
            final_df = pd.merge(sales_mapped, inv_sum, left_on='inventory_title', right_on='title', how='left')
            final_df[['Qty', 'Value', 'unit_cost']] = final_df[['Qty', 'Value', 'unit_cost']].fillna(0)
            
            total_cogs = (final_df['quantity_sold'] * final_df['unit_cost']).sum()
            total_inv_val = inv_sum['Value'].sum()
            avg_dio = (total_inv_val / (total_cogs + 1)) * days

            # --- DISPLAY ---
            st.header("Financial Health Overview")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Rec. Days (DSO)", f"{avg_dso:.1f}")
            m2.metric("Inv. Days (DIO)", f"{avg_dio:.1f}")
            m3.metric("Pay. Days (DPO)", f"{avg_dpo:.1f}")
            m4.metric("Cash Cycle", f"{(avg_dso + avg_dio - avg_dpo):.1f}")
            
            col_left, col_right = st.columns(2)
            with col_left:
                st.subheader("Out of Stock Live Items")
                oos_df = final_df[final_df['Qty'] == 0][['item_name', 'quantity_sold', 'amount']]
                st.dataframe(oos_df, use_container_width=True)
            
            with col_right:
                st.subheader("AR Concentration")
                fig = px.pie(df_inv.groupby('customer_name')['bcy_balance'].sum().nlargest(5).reset_index(), 
                             values='bcy_balance', names='customer_name')
                st.plotly_chart(fig, use_container_width=True)

else:
    st.info("Upload Invoices, Bills, Sales, and Warehouse files to begin.")
