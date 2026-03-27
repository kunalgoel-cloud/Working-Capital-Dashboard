import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
from datetime import datetime

st.set_page_config(page_title="Working Capital Warehouse", layout="wide")

def get_conn():
    return st.connection("postgresql", type="sql")

conn = get_conn()

# --- 1. DATABASE INITIALIZATION (ALL TABLES) ---
def init_db():
    with conn.session as s:
        # Mapping Table
        s.execute(text("CREATE TABLE IF NOT EXISTS item_mappings (zoho_name TEXT PRIMARY KEY, inventory_title TEXT);"))
        
        # Ledger Table
        s.execute(text("""
            CREATE TABLE IF NOT EXISTS customer_summary (
                customer_id TEXT PRIMARY KEY, customer_name TEXT, 
                invoiced_amount FLOAT, amount_received FLOAT, 
                closing_balance FLOAT, last_updated TIMESTAMP
            );
        """))
        
        # Bills Table
        s.execute(text("""
            CREATE TABLE IF NOT EXISTS bills (
                bill_id TEXT PRIMARY KEY, vendor_name TEXT, 
                bcy_total FLOAT, bcy_balance FLOAT, last_updated TIMESTAMP
            );
        """))
        
        # Inventory Snapshot Table
        s.execute(text("""
            CREATE TABLE IF NOT EXISTS inventory_history (
                sku_title TEXT, qty FLOAT, value FLOAT, snapshot_date DATE,
                PRIMARY KEY (sku_title, snapshot_date)
            );
        """))
        s.commit()

init_db()

# --- 2. UNIVERSAL SYNC ENGINE ---
def sync_to_db(df, mode):
    total_rows = len(df)
    prog = st.progress(0)
    stat = st.empty()
    
    with conn.session as s:
        for i, row in df.iterrows():
            if mode == "ledger":
                q = text("""INSERT INTO customer_summary (customer_id, customer_name, invoiced_amount, amount_received, closing_balance, last_updated)
                            VALUES (:id, :n, :inv, :rec, :bal, NOW()) ON CONFLICT (customer_id) 
                            DO UPDATE SET invoiced_amount=EXCLUDED.invoiced_amount, amount_received=EXCLUDED.amount_received, closing_balance=EXCLUDED.closing_balance, last_updated=NOW()""")
                p = {"id": str(row['customer_id']), "n": row['customer_name'], "inv": row['invoiced_amount'], "rec": row['amount_received'], "bal": row['closing_balance']}
            
            elif mode == "bills":
                q = text("""INSERT INTO bills (bill_id, vendor_name, bcy_total, bcy_balance, last_updated)
                            VALUES (:id, :v, :t, :b, NOW()) ON CONFLICT (bill_id) 
                            DO UPDATE SET bcy_balance=EXCLUDED.bcy_balance, last_updated=NOW()""")
                p = {"id": str(row['bill_number']), "v": row['vendor_name'], "t": row['bcy_total'], "b": row['bcy_balance']}
            
            s.execute(q, p)
            if i % 50 == 0: prog.progress(int((i+1)/total_rows*100))
        s.commit()
    stat.success(f"✅ {mode.capitalize()} synced to Postgres.")

# --- 3. SIDEBAR & FILE HANDLING (Fixed EmptyDataError) ---
with st.sidebar:
    st.header("🎯 Targets")
    t_dso = st.number_input("Target DSO", value=120)
    t_dio = st.number_input("Target DIO", value=45)
    t_dpo = st.number_input("Target DPO", value=90)
    
    st.divider()
    st.header("📂 Data Sync")
    
    # We read each file ONCE into a variable to avoid "EmptyDataError"
    f_sum = st.file_uploader("1. Customer Summary", type="csv")
    df_s = pd.read_csv(f_sum) if f_sum else None
    if df_s is not None and st.button("Sync Ledger"): sync_to_db(df_s, "ledger")
        
    f_wh = st.file_uploader("2. Warehouse Export", type="csv")
    df_w = pd.read_csv(f_wh) if f_wh else None
    
    f_sales = st.file_uploader("3. Sales Items", type="csv")
    df_sl = pd.read_csv(f_sales) if f_sales else None
    
    f_bill = st.file_uploader("4. Bill Details", type="csv")
    df_b = pd.read_csv(f_bill) if f_bill else None
    if df_b is not None and st.button("Sync Bills"): sync_to_db(df_b, "bills")
    
    date_range = st.date_input("Period", [pd.to_datetime("2025-04-01"), pd.to_datetime("2026-03-31")])

# --- 4. MAIN DASHBOARD ---
if all([df_s is not None, df_w is not None, df_sl is not None, df_b is not None]):
    df_map = conn.query("SELECT * FROM item_mappings", ttl=0)
    days = (date_range[1] - date_range[0]).days or 365

    # 1. DSO
    avg_dso = (df_s['closing_balance'].sum() / (df_s['invoiced_amount'].sum() + 1)) * days
    
    # 2. DPO
    avg_dpo = (df_b['bcy_balance'].sum() / (df_b['bcy_total'].sum() + 1)) * days
    
    # 3. DIO
    wh_sum = df_w.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
    wh_sum['unit_cost'] = wh_sum['Value'] / wh_sum['Qty'].replace(0, 1)
    sales_mapped = pd.merge(df_sl, df_map, left_on='item_name', right_on='zoho_name', how='inner')
    dio_data = pd.merge(sales_mapped, wh_sum[['title', 'unit_cost']], left_on='inventory_title', right_on='title', how='left')
    total_cogs = (dio_data['quantity_sold'] * dio_data['unit_cost'].fillna(0)).sum()
    avg_dio = (wh_sum['Value'].sum() / (total_cogs + 1)) * days
    
    ccc = avg_dso + avg_dio - avg_dpo

    t1, t2, t3 = st.tabs(["📊 Dashboard", "⏳ Ageing", "🔧 Mappings"])

    with t1:
        # Metrics
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("DSO", f"{avg_dso:.1f}d", f"{avg_dso-t_dso:+.1f}", "inverse")
        m2.metric("DIO", f"{avg_dio:.1f}d", f"{avg_dio-t_dio:+.1f}", "inverse")
        m3.metric("DPO", f"{avg_dpo:.1f}d", f"{avg_dpo-t_dpo:+.1f}")
        m4.metric("CCC", f"{ccc:.1f}d")

        # CEI Chart (Fixed ShapeError)
        st.divider()
        df_s['CEI'] = (df_s['amount_received'] / (df_s['invoiced_amount'] + 1)) * 100
        top_10 = df_s.sort_values('closing_balance', ascending=False).head(10).copy()
        fig_cei = px.bar(top_10, x='customer_name', y='closing_balance', 
                         text=top_10['CEI'].apply(lambda x: f"{x:.1f}%"),
                         color='CEI', color_continuous_scale='RdYlGn', range_color=[0, 100],
                         title="Top 10 AR Balances")
        fig_cei.update_traces(textposition='outside')
        st.plotly_chart(fig_cei, use_container_width=True)

    with t2:
        # Ageing (Fixed get_bucket)
        item_sales_vol = dio_data.groupby('inventory_title')['quantity_sold'].sum().reset_index()
        item_stats = pd.merge(wh_sum, item_sales_vol, left_on='title', right_on='inventory_title', how='left')
        item_stats['Item_DIO'] = (item_stats['Value'] / ((item_stats['quantity_sold'].fillna(0) * item_stats['unit_cost']) + 1)) * days
        def get_bucket(d): return "Fast" if d <= 30 else "Healthy" if d <= 90 else "High Risk"
        item_stats['Bucket'] = item_stats['Item_DIO'].apply(get_bucket)
        st.plotly_chart(px.pie(item_stats, values='Value', names='Bucket', hole=0.4), use_container_width=True)
        st.dataframe(item_stats[['title', 'Value', 'Item_DIO', 'Bucket']].sort_values('Value', ascending=False))

    with t3:
        # Mapping Form
        with st.form("map"):
            z = st.selectbox("Zoho Item", sorted(df_sl['item_name'].unique()))
            w = st.selectbox("Warehouse SKU", sorted(wh_sum['title'].unique()))
            if st.form_submit_button("Save Mapping"):
                with conn.session as s:
                    s.execute(text("INSERT INTO item_mappings (zoho_name, inventory_title) VALUES (:z, :w) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), {"z":z, "w":w})
                    s.commit()
                st.rerun()
        st.dataframe(df_map)

else:
    st.info("Upload all 4 files to activate. You can Sync Ledger and Sync Bills individually once uploaded.")
