import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
from datetime import datetime

# --- 1. CONFIGURATION & DATABASE ---
st.set_page_config(page_title="Working Capital Dashboard", layout="wide")

def get_conn():
    # Setup in Streamlit Secrets under [connections.postgresql]
    return st.connection("postgresql", type="sql")

conn = get_conn()

# Initialize Database Tables
def init_db():
    with conn.session as s:
        s.execute(text("""
            CREATE TABLE IF NOT EXISTS item_mappings (zoho_name TEXT PRIMARY KEY, inventory_title TEXT);
            CREATE TABLE IF NOT EXISTS invoices (invoice_id BIGINT PRIMARY KEY, customer_name TEXT, bcy_total FLOAT, bcy_balance FLOAT, date DATE, due_date DATE);
            CREATE TABLE IF NOT EXISTS bills (bill_id BIGINT PRIMARY KEY, vendor_name TEXT, bcy_total FLOAT, bcy_balance FLOAT, date DATE, due_date DATE);
            CREATE TABLE IF NOT EXISTS sales_items (item_id BIGINT PRIMARY KEY, item_name TEXT, quantity_sold FLOAT, amount FLOAT, sku TEXT);
        """))
        s.commit()

init_db()

# --- 2. DATA INGESTION (UPSERT LOGIC) ---
def upsert_data(df, table_type):
    with conn.session as s:
        if table_type == 'invoices':
            for _, r in df.iterrows():
                s.execute(text("""
                    INSERT INTO invoices (invoice_id, customer_name, bcy_total, bcy_balance, date, due_date)
                    VALUES (:id, :n, :t, :b, :d, :dd)
                    ON CONFLICT (invoice_id) DO UPDATE SET bcy_balance = EXCLUDED.bcy_balance
                """), {"id": r['invoice_id'], "n": r['customer_name'], "t": r['bcy_total'], "b": r['bcy_balance'], "d": r['date'], "dd": r['due_date']})
        
        elif table_type == 'bills':
            for _, r in df.iterrows():
                s.execute(text("""
                    INSERT INTO bills (bill_id, vendor_name, bcy_total, bcy_balance, date, due_date)
                    VALUES (:id, :n, :t, :b, :d, :dd)
                    ON CONFLICT (bill_id) DO UPDATE SET bcy_balance = EXCLUDED.bcy_balance
                """), {"id": r['bill_id'], "n": r['vendor_name'], "t": r['bcy_total'], "b": r['bcy_balance'], "d": r['date'], "dd": r['due_date']})
        
        elif table_type == 'sales_items':
            for _, r in df.iterrows():
                s.execute(text("""
                    INSERT INTO sales_items (item_id, item_name, quantity_sold, amount, sku)
                    VALUES (:id, :n, :q, :a, :s)
                    ON CONFLICT (item_id) DO UPDATE SET quantity_sold = EXCLUDED.quantity_sold, amount = EXCLUDED.amount
                """), {"id": r['item_id'], "n": r['item_name'], "q": r['quantity_sold'], "a": r['amount'], "s": r['sku']})
        s.commit()

# --- 3. SIDEBAR: UPLOADS & FILTERS ---
st.sidebar.header("📥 Data Management")
with st.sidebar.expander("Upload Zoho Files"):
    f_inv = st.file_uploader("Invoice Details", type="csv")
    f_bill = st.file_uploader("Bill Details", type="csv")
    f_sales = st.file_uploader("Sales by Item", type="csv")
    if st.button("Sync Zoho to Database"):
        if f_inv: upsert_data(pd.read_csv(f_inv), 'invoices')
        if f_bill: upsert_data(pd.read_csv(f_bill), 'bills')
        if f_sales: upsert_data(pd.read_csv(f_sales), 'sales_items')
        st.toast("Database Updated!")

f_warehouse = st.sidebar.file_uploader("Warehouse Inventory (Manual)", type="csv")

st.sidebar.header("📅 Filters")
date_range = st.sidebar.date_input("Analysis Period", [datetime(2025, 4, 1), datetime(2026, 3, 31)])

# --- 4. DATA RETRIEVAL & MAPPING ---
# Fetch from DB based on filter
start_date, end_date = date_range[0], date_range[1]
df_invoices = conn.query(f"SELECT * FROM invoices WHERE date BETWEEN '{start_date}' AND '{end_date}'")
df_bills = conn.query(f"SELECT * FROM bills WHERE date BETWEEN '{start_date}' AND '{end_date}'")
df_sales_items = conn.query("SELECT * FROM sales_items")
df_mappings = conn.query("SELECT * FROM item_mappings")

if f_warehouse and not df_sales_items.empty:
    df_warehouse_raw = pd.read_csv(f_warehouse)
    
    # Mapping Interface
    unmapped = [n for n in df_sales_items['item_name'].unique() if n not in df_mappings['zoho_name'].values]
    if unmapped:
        st.warning(f"🔗 {len(unmapped)} items need mapping.")
        with st.expander("Configure Item Mappings"):
            options = list(df_warehouse_raw['title'].unique()) + ["DISCONTINUED / OLD SKU"]
            for item in unmapped[:5]: # Show 5 at a time
                col1, col2 = st.columns([3,1])
                choice = col1.selectbox(f"Map '{item}'", options, key=item)
                if col2.button("Save", key=f"b_{item}"):
                    with conn.session as s:
                        s.execute(text("INSERT INTO item_mappings VALUES (:z, :i)"), {"z":item, "i":choice})
                        s.commit()
                    st.rerun()

    # --- 5. DASHBOARD CALCULATIONS ---
    # Days in period for DSO/DIO/DPO calc
    period_days = (end_date - start_date).days or 365

    # A. Receivable Days (Customer Level)
    dso_df = df_invoices.groupby('customer_name').agg({'bcy_total':'sum', 'bcy_balance':'sum'}).reset_index()
    dso_df['DSO'] = (dso_df['bcy_balance'] / (dso_df['bcy_total'] + 0.1)) * period_days
    avg_dso = (df_invoices['bcy_balance'].sum() / (df_invoices['bcy_total'].sum() + 0.1)) * period_days

    # B. Payable Days (Vendor Level)
    dpo_df = df_bills.groupby('vendor_name').agg({'bcy_total':'sum', 'bcy_balance':'sum'}).reset_index()
    dpo_df['DPO'] = (dpo_df['bcy_balance'] / (dpo_df['bcy_total'] + 0.1)) * period_days
    avg_dpo = (df_bills['bcy_balance'].sum() / (df_bills['bcy_total'].sum() + 0.1)) * period_days

    # C. Inventory Days (Product Level)
    sales_mapped = pd.merge(df_sales_items, df_mappings, left_on='item_name', right_on='zoho_name', how='left')
    inv_sum = df_warehouse_raw.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
    # Add Discontinued Placeholder
    inv_sum = pd.concat([inv_sum, pd.DataFrame([{'title':'DISCONTINUED / OLD SKU', 'Qty':0, 'Value':0}])])
    inv_sum['unit_cost'] = inv_sum['Value'] / (inv_sum['Qty'] + 0.001)
    
    dio_df = pd.merge(sales_mapped, inv_sum, left_on='inventory_title', right_on='title', how='left')
    dio_df['COGS'] = dio_df['quantity_sold'] * dio_df['unit_cost']
    avg_dio = (inv_sum['Value'].sum() / (dio_df['COGS'].sum() + 0.1)) * period_days

    # --- 6. DISPLAY ---
    st.header(f"Working Capital Summary: {start_date} to {end_date}")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Receivable Days (DSO)", f"{avg_dso:.1f}")
    c2.metric("Inventory Days (DIO)", f"{avg_dio:.1f}")
    c3.metric("Payable Days (DPO)", f"{avg_dpo:.1f}")
    c4.metric("Cash Conversion Cycle", f"{(avg_dso + avg_dio - avg_dpo):.1f}", delta_color="inverse")

    tab1, tab2, tab3 = st.tabs(["Receivables", "Inventory Efficiency", "Payables"])
    
    with tab1:
        fig_ar = px.bar(dso_df.sort_values('DSO', ascending=False).head(15), x='customer_name', y='DSO', title="Customer Credit Risk")
        st.plotly_chart(fig_ar, use_container_width=True)
    
    with tab2:
        dio_df['DIO'] = (dio_df['Value'] / (dio_df['COGS'] + 0.1)) * period_days
        st.subheader("Product Level Inventory Days")
        st.dataframe(dio_df[['item_name', 'inventory_title', 'quantity_sold', 'Value', 'DIO']].sort_values('DIO', ascending=False))

    with tab3:
        st.subheader("Vendor Payment Terms (DPO)")
        st.dataframe(dpo_df.sort_values('DPO', ascending=False))

else:
    st.info("Sync your Zoho data in the sidebar and upload your Warehouse Inventory file to see the analysis.")
