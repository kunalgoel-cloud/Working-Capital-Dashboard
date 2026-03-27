import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
import time

# --- CONFIGURATION ---
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

# --- APP UI ---
st.title("🚀 High-Speed Working Capital Dashboard")

with st.sidebar:
    st.header("1. Data Upload")
    f_inv = st.file_uploader("Invoices", type="csv")
    f_bill = st.file_uploader("Bills", type="csv")
    f_sales = st.file_uploader("Sales Items", type="csv")
    f_wh = st.file_uploader("Warehouse Export", type="csv")
    
    date_range = st.date_input("Analysis Period", [pd.to_datetime("2025-04-01"), pd.to_datetime("2026-03-31")])

# Check if data is loaded in RAM first
if all([f_inv, f_bill, f_sales, f_wh]):
    # FAST LOAD INTO MEMORY
    df_inv = pd.read_csv(f_inv)
    df_bill = pd.read_csv(f_bill)
    df_sales = pd.read_csv(f_sales)
    df_wh = pd.read_csv(f_wh)
    
    # Load existing mappings from DB
    df_map = conn.query("SELECT * FROM item_mappings")

    # --- MAPPING LOGIC (Instant) ---
    st.header("🔗 Step 1: Item Mapping")
    unmapped = [n for n in df_sales['item_name'].unique() if n not in df_map['zoho_name'].values]
    
    if unmapped:
        st.warning(f"{len(unmapped)} items need mapping before metrics can be calculated.")
        with st.expander("Map Items Now"):
            opts = list(df_wh['title'].unique()) + ["DISCONTINUED / OLD SKU"]
            for item in unmapped[:5]: # Show small batches
                c1, c2 = st.columns([3, 1])
                choice = c1.selectbox(f"Map '{item}'", opts, key=item)
                if c2.button("Save", key=f"btn_{item}"):
                    with conn.session as s:
                        s.execute(text("INSERT INTO item_mappings VALUES (:z, :i) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), {"z":item, "i":choice})
                        s.commit()
                    st.rerun()
    
    # --- METRICS LOGIC (Instant) ---
    if not unmapped:
        st.header("📈 Step 2: Instant Metrics")
        
        # Calculate Period Days
        days = (date_range[1] - date_range[0]).days or 365
        
        # Calculations
        dso = (df_inv['bcy_balance'].sum() / (df_inv['bcy_total'].sum() + 1)) * days
        dpo = (df_bill['bcy_balance'].sum() / (df_bill['bcy_total'].sum() + 1)) * days
        
        # DIO Calculation
        sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name')
        inv_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
        inv_sum = pd.concat([inv_sum, pd.DataFrame([{'title':'DISCONTINUED / OLD SKU','Qty':0,'Value':0}])])
        inv_sum['cost'] = inv_sum['Value'] / (inv_sum['Qty'] + 0.001)
        
        dio_merge = pd.merge(sales_mapped, inv_sum, left_on='inventory_title', right_on='title')
        total_cogs = (dio_merge['quantity_sold'] * dio_merge['cost']).sum()
        dio = (inv_sum['Value'].sum() / (total_cogs + 1)) * days

        # DASHBOARD DISPLAY
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Rec. Days (DSO)", f"{dso:.1f}")
        m2.metric("Inv. Days (DIO)", f"{dio:.1f}")
        m3.metric("Pay. Days (DPO)", f"{dpo:.1f}")
        m4.metric("Cash Cycle", f"{(dso + dio - dpo):.1f}")

        # --- DB SYNC SECTION (Manual & Optimized) ---
        st.divider()
        st.subheader("💾 Step 3: Permanent Database Sync")
        st.write("Calculations are complete. Push this data to Neon Postgres for future retrieval?")
        
        if st.button("🚀 Push to Cloud Database"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            start_time = time.time()
            
            # Group datasets for iteration
            datasets = [
                (df_inv[['invoice_id','customer_name','bcy_total','bcy_balance','date']], "invoices", "invoice_id"),
                (df_bill[['bill_id','vendor_name','bcy_total','bcy_balance','date']], "bills", "bill_id"),
                (df_sales[['item_id','item_name','quantity_sold','amount']], "sales_items", "item_id")
            ]
            
            total_steps = len(datasets)
            
            with conn.session as s:
                for i, (df, table, pk) in enumerate(datasets):
                    status_text.text(f"Uploading {table}...")
                    
                    # High-speed batch insertion
                    for index, row in df.iterrows():
                        # We use a simple loop here, but limited to relevant columns to keep it fast
                        params = row.to_dict()
                        cols = ", ".join(params.keys())
                        placeholders = ", ".join([f":{k}" for k in params.keys()])
                        
                        query = f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) ON CONFLICT ({pk}) DO NOTHING"
                        s.execute(text(query), params)
                    
                    # Update progress
                    progress = (i + 1) / total_steps
                    progress_bar.progress(progress)
                    
                    # Estimate remaining time
                    elapsed = time.time() - start_time
                    avg_per_step = elapsed / (i + 1)
                    rem_time = avg_per_step * (total_steps - (i + 1))
                    status_text.text(f"Uploading {table}... Estimated time remaining: {rem_time:.1f} seconds")
                
                s.commit()
            
            st.success(f"Successfully synced to Neon in {time.time() - start_time:.1f} seconds!")

else:
    st.info("Waiting for all 4 files (Invoices, Bills, Sales, Warehouse) to be uploaded...")
