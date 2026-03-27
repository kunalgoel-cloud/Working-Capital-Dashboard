import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
from datetime import datetime

# --- 1. CONFIGURATION ---
st.set_page_config(page_title="Working Capital Dashboard", layout="wide")

def get_conn():
    return st.connection("postgresql", type="sql")

conn = get_conn()

# --- 2. DATABASE INITIALIZATION ---
def init_db():
    with conn.session as s:
        s.execute(text("CREATE TABLE IF NOT EXISTS item_mappings (zoho_name TEXT PRIMARY KEY, inventory_title TEXT);"))
        s.execute(text("CREATE TABLE IF NOT EXISTS invoices (invoice_id BIGINT PRIMARY KEY, customer_name TEXT, bcy_total FLOAT, bcy_balance FLOAT, date DATE, due_date DATE);"))
        s.execute(text("CREATE TABLE IF NOT EXISTS bills (bill_id BIGINT PRIMARY KEY, vendor_name TEXT, bcy_total FLOAT, bcy_balance FLOAT, date DATE, due_date DATE);"))
        s.execute(text("CREATE TABLE IF NOT EXISTS sales_items (item_id BIGINT PRIMARY KEY, item_name TEXT, quantity_sold FLOAT, amount FLOAT, sku TEXT);"))
        s.commit()

init_db()

# --- 3. SIDEBAR & UPLOADS ---
st.sidebar.header("📥 1. Ingest Data")
with st.sidebar.expander("Upload CSVs from Zoho"):
    f_inv = st.file_uploader("Invoices", type="csv")
    f_bill = st.file_uploader("Bills", type="csv")
    f_sales = st.file_uploader("Sales Items", type="csv")
    
    if st.button("Sync to Database"):
        with conn.session as s:
            if f_inv:
                df = pd.read_csv(f_inv)
                for _, r in df.iterrows():
                    s.execute(text("INSERT INTO invoices VALUES (:id, :n, :t, :b, :d, :dd) ON CONFLICT (invoice_id) DO UPDATE SET bcy_balance = EXCLUDED.bcy_balance"),
                              {"id":r['invoice_id'],"n":r['customer_name'],"t":r['bcy_total'],"b":r['bcy_balance'],"d":r['date'],"dd":r['due_date']})
            if f_bill:
                df = pd.read_csv(f_bill)
                for _, r in df.iterrows():
                    s.execute(text("INSERT INTO bills VALUES (:id, :n, :t, :b, :d, :dd) ON CONFLICT (bill_id) DO UPDATE SET bcy_balance = EXCLUDED.bcy_balance"),
                              {"id":r['bill_id'],"n":r['vendor_name'],"t":r['bcy_total'],"b":r['bcy_balance'],"d":r['date'],"dd":r['due_date']})
            if f_sales:
                df = pd.read_csv(f_sales)
                for _, r in df.iterrows():
                    s.execute(text("INSERT INTO sales_items VALUES (:id, :n, :q, :a, :s) ON CONFLICT (item_id) DO NOTHING"),
                              {"id":r['item_id'],"n":r['item_name'],"q":r['quantity_sold'],"a":r['amount'],"s":str(r['sku'])})
            s.commit()
            st.sidebar.success("Sync Complete!")

f_warehouse = st.sidebar.file_uploader("2. Upload Warehouse CSV", type="csv")

st.sidebar.header("📅 3. Filters")
date_range = st.sidebar.date_input("Period", [datetime(2025, 4, 1), datetime(2026, 3, 31)])

# --- 4. DATA CHECK & RETRIEVAL ---
if len(date_range) == 2:
    start, end = date_range[0], date_range[1]
    df_inv = conn.query(f"SELECT * FROM invoices WHERE date BETWEEN '{start}' AND '{end}'")
    df_bill = conn.query(f"SELECT * FROM bills WHERE date BETWEEN '{start}' AND '{end}'")
    df_sales = conn.query("SELECT * FROM sales_items")
    df_map = conn.query("SELECT * FROM item_mappings")

    # Debug Section (Can be removed once working)
    with st.expander("Database Status Check"):
        st.write(f"Invoices in range: {len(df_inv)}")
        st.write(f"Bills in range: {len(df_bill)}")
        st.write(f"Items in DB: {len(df_sales)}")

    if not df_inv.empty and not df_bill.empty and f_warehouse:
        df_wh = pd.read_csv(f_warehouse)
        
        # Mapping Logic
        unmapped = [n for n in df_sales['item_name'].unique() if n not in df_map['zoho_name'].values]
        if unmapped:
            st.warning(f"Map the following {len(unmapped)} items to continue...")
            opts = list(df_wh['title'].unique()) + ["DISCONTINUED / OLD SKU"]
            for item in unmapped[:3]:
                c1, c2 = st.columns([3,1])
                choice = c1.selectbox(f"Map {item}", opts, key=item)
                if c2.button("Save", key=f"btn_{item}"):
                    with conn.session as s:
                        s.execute(text("INSERT INTO item_mappings VALUES (:z, :i)"), {"z":item, "i":choice})
                        s.commit()
                    st.rerun()
        
        # --- 5. FINAL CALCULATIONS ---
        period_days = (end - start).days or 365
        
        # DSO
        avg_dso = (df_inv['bcy_balance'].sum() / (df_inv['bcy_total'].sum() + 1)) * period_days
        # DPO
        avg_dpo = (df_bill['bcy_balance'].sum() / (df_bill['bcy_total'].sum() + 1)) * period_days
        
        # DIO
        sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name')
        inv_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
        inv_sum = pd.concat([inv_sum, pd.DataFrame([{'title':'DISCONTINUED / OLD SKU', 'Qty':0, 'Value':0}])])
        inv_sum['u_cost'] = inv_sum['Value'] / (inv_sum['Qty'] + 0.001)
        
        dio_df = pd.merge(sales_mapped, inv_sum, left_on='inventory_title', right_on='title')
        avg_dio = (inv_sum['Value'].sum() / ((dio_df['quantity_sold'] * dio_df['u_cost']).sum() + 1)) * period_days

        # --- 6. DASHBOARD ---
        st.title("Working Capital Dashboard")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("DSO (Receivables)", f"{avg_dso:.1f}")
        m2.metric("DIO (Inventory)", f"{avg_dio:.1f}")
        m3.metric("DPO (Payables)", f"{avg_dpo:.1f}")
        m4.metric("Cash Cycle", f"{(avg_dso + avg_dio - avg_dpo):.1f}")
        
        st.plotly_chart(px.bar(df_inv.groupby('customer_name')['bcy_balance'].sum().nlargest(10).reset_index(), x='customer_name', y='bcy_balance'))
    else:
        st.info("Still waiting for data. Ensure 'Sync to Database' was clicked and the date range matches your files.")
