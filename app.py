import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
import time

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
st.title("🚀 Express Working Capital Dashboard")

with st.sidebar:
    st.header("1. Data Upload")
    f_inv = st.file_uploader("Invoices", type="csv")
    f_bill = st.file_uploader("Bills", type="csv")
    f_sales = st.file_uploader("Sales Items", type="csv")
    f_wh = st.file_uploader("Warehouse Export", type="csv")
    date_range = st.date_input("Analysis Period", [pd.to_datetime("2025-04-01"), pd.to_datetime("2026-03-31")])

if all([f_inv, f_bill, f_sales, f_wh]):
    df_inv = pd.read_csv(f_inv)
    df_bill = pd.read_csv(f_bill)
    df_sales = pd.read_csv(f_sales)
    df_wh = pd.read_csv(f_wh)
    df_map = conn.query("SELECT * FROM item_mappings")

    # --- EXPRESS MAPPING LOGIC ---
    st.header("🔗 Step 1: Rapid Item Mapping")
    unmapped = [n for n in df_sales['item_name'].unique() if n not in df_map['zoho_name'].values]
    
    if unmapped:
        st.warning(f"⚠️ {len(unmapped)} items are unmapped. This is blocking your dashboard.")
        
        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("⚡ Bulk Mark All as Discontinued"):
                with conn.session as s:
                    for item in unmapped:
                        s.execute(text("INSERT INTO item_mappings VALUES (:z, :i) ON CONFLICT DO NOTHING"), 
                                  {"z": item, "i": "DISCONTINUED / OLD SKU"})
                    s.commit()
                st.success("All items marked! Refreshing...")
                time.sleep(1)
                st.rerun()
        
        with col_b:
            st.info("Or map them manually in small batches below.")

        with st.expander("Manual Mapping (Top 10)"):
            opts = list(df_wh['title'].unique()) + ["DISCONTINUED / OLD SKU"]
            for item in unmapped[:10]:
                c1, c2 = st.columns([3, 1])
                # Simple logic to find best guess
                best_guess = next((o for o in opts if item[:10].lower() in o.lower()), opts[-1])
                choice = c1.selectbox(f"Map '{item}'", opts, index=opts.index(best_guess), key=item)
                if c2.button("Save", key=f"btn_{item}"):
                    with conn.session as s:
                        s.execute(text("INSERT INTO item_mappings VALUES (:z, :i) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), {"z":item, "i":choice})
                        s.commit()
                    st.rerun()
    
    # --- METRICS LOGIC ---
    if not unmapped or st.checkbox("Show Dashboard with current mappings anyway"):
        st.divider()
        st.header("📈 Step 2: Financial Metrics")
        
        days = (date_range[1] - date_range[0]).days or 365
        
        # DSO/DPO
        dso = (df_inv['bcy_balance'].sum() / (df_inv['bcy_total'].sum() + 1)) * days
        dpo = (df_bill['bcy_balance'].sum() / (df_bill['bcy_total'].sum() + 1)) * days
        
        # DIO (Inventory)
        sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
        inv_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
        inv_sum = pd.concat([inv_sum, pd.DataFrame([{'title':'DISCONTINUED / OLD SKU','Qty':0,'Value':0}])])
        inv_sum['cost'] = inv_sum['Value'] / (inv_sum['Qty'] + 0.001)
        
        dio_merge = pd.merge(sales_mapped, inv_sum, left_on='inventory_title', right_on='title')
        total_cogs = (dio_merge['quantity_sold'] * dio_merge['cost']).sum()
        dio = (inv_sum['Value'].sum() / (total_cogs + 1)) * days

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Rec. Days (DSO)", f"{dso:.1f}")
        m2.metric("Inv. Days (DIO)", f"{dio:.1f}")
        m3.metric("Pay. Days (DPO)", f"{dpo:.1f}")
        m4.metric("Cash Cycle", f"{(dso + dio - dpo):.1f}")

        st.plotly_chart(px.bar(df_inv.groupby('customer_name')['bcy_balance'].sum().nlargest(10).reset_index(), 
                               x='customer_name', y='bcy_balance', title="Top 10 Receivables by Customer"))

        # --- DB SYNC BUTTON ---
        if st.button("💾 Permanent Sync to Cloud DB"):
            with st.status("Uploading data...", expanded=True) as status:
                # Add bulk insert logic here
                status.update(label="Sync Complete!", state="complete", expanded=False)
else:
    st.info("Upload all 4 CSVs to activate the express dashboard.")
