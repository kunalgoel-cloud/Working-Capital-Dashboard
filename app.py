import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
from datetime import datetime

st.set_page_config(page_title="Working Capital Dashboard Pro", layout="wide")

def get_conn():
    return st.connection("postgresql", type="sql")

conn = get_conn()

# --- 1. DATABASE INITIALIZATION ---
def init_db():
    with conn.session as s:
        s.execute(text("CREATE TABLE IF NOT EXISTS item_mappings (zoho_name TEXT PRIMARY KEY, inventory_title TEXT);"))
        s.execute(text("""
            CREATE TABLE IF NOT EXISTS customer_summary (
                customer_id TEXT PRIMARY KEY,
                customer_name TEXT,
                invoiced_amount FLOAT,
                amount_received FLOAT,
                closing_balance FLOAT,
                last_updated TIMESTAMP
            );
        """))
        s.commit()

init_db()

# --- 2. THE UPSERT ENGINE ---
def sync_ledger_to_db(df):
    total_rows = len(df)
    progress_bar = st.progress(0)
    status_text = st.empty()
    with conn.session as s:
        for index, row in df.iterrows():
            query = text("""
                INSERT INTO customer_summary (customer_id, customer_name, invoiced_amount, amount_received, closing_balance, last_updated)
                VALUES (:cid, :cname, :inv, :rec, :bal, NOW())
                ON CONFLICT (customer_id) DO UPDATE SET 
                    invoiced_amount = EXCLUDED.invoiced_amount,
                    amount_received = EXCLUDED.amount_received,
                    closing_balance = EXCLUDED.closing_balance,
                    last_updated = NOW();
            """)
            s.execute(query, {"cid": str(row['customer_id']), "cname": row['customer_name'], 
                              "inv": row['invoiced_amount'], "rec": row['amount_received'], "bal": row['closing_balance']})
            if index % 50 == 0 or index == total_rows - 1:
                percent = int((index + 1) / total_rows * 100)
                progress_bar.progress(percent)
                status_text.text(f"Syncing: {percent}% Complete")
        s.commit()
    status_text.success(f"✅ Successfully synced {total_rows} records to Database.")

# --- 3. SIDEBAR (DECOUPLED BUTTONS) ---
with st.sidebar:
    st.header("🎯 Business Targets")
    t_dso = st.number_input("Target DSO", value=120)
    t_dio = st.number_input("Target DIO", value=45)
    t_dpo = st.number_input("Target DPO", value=90)
    
    st.divider()
    st.header("📂 Data Ingestion")
    
    f_sum = st.file_uploader("1. Customer Summary (DSO)", type="csv")
    if f_sum and st.button("🔄 Sync Ledger to Postgres"):
        sync_ledger_to_db(pd.read_csv(f_sum))
        
    f_wh = st.file_uploader("2. Warehouse Export (DIO)", type="csv")
    f_sales = st.file_uploader("3. Sales by Item (COGS)", type="csv")
    f_bill = st.file_uploader("4. Bill Details (DPO)", type="csv")
    
    date_range = st.date_input("Period", [pd.to_datetime("2025-04-01"), pd.to_datetime("2026-03-31")])

# --- 4. DASHBOARD LOGIC ---
if all([f_sum, f_wh, f_sales, f_bill]):
    df_sum, df_wh, df_sales, df_bill = pd.read_csv(f_sum), pd.read_csv(f_wh), pd.read_csv(f_sales), pd.read_csv(f_bill)
    df_map = conn.query("SELECT * FROM item_mappings", ttl=0)
    days = (date_range[1] - date_range[0]).days or 365

    # Metrics Calculations
    avg_dso = (df_sum['closing_balance'].sum() / (df_sum['invoiced_amount'].sum() + 1)) * days
    avg_dpo = (df_bill['bcy_balance'].sum() / (df_bill['bcy_total'].sum() + 1)) * days
    
    wh_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
    wh_sum['unit_cost'] = wh_sum['Value'] / wh_sum['Qty'].replace(0, 1)
    sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
    dio_data = pd.merge(sales_mapped, wh_sum[['title', 'unit_cost']], left_on='inventory_title', right_on='title', how='left')
    total_cogs = (dio_data['quantity_sold'] * dio_data['unit_cost'].fillna(0)).sum()
    avg_dio = (wh_sum['Value'].sum() / (total_cogs + 1)) * days
    ccc = avg_dso + avg_dio - avg_dpo

    t1, t2, t3 = st.tabs(["📊 Dashboard", "⏳ Ageing", "🔧 Mappings"])

    with t1:
        # DB Sync Status Header
        last_sync = conn.query("SELECT MAX(last_updated) as ts FROM customer_summary", ttl=0)
        if not last_sync.empty and last_sync['ts'].iloc[0]:
            st.caption(f"DB Last Updated: {last_sync['ts'].iloc[0]}")

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("DSO", f"{avg_dso:.1f}d", f"{avg_dso-t_dso:+.1f}", "inverse")
        m2.metric("DIO", f"{avg_dio:.1f}d", f"{avg_dio-t_dio:+.1f}", "inverse")
        m3.metric("DPO", f"{avg_dpo:.1f}d", f"{avg_dpo-t_dpo:+.1f}")
        m4.metric("CCC", f"{ccc:.1f}d")

        st.divider()
        st.subheader("🔍 Product Deep Dive")
        sel_p = st.selectbox("Select SKU:", ["All"] + sorted(wh_sum['title'].tolist()))
        if sel_p != "All":
            p_w = wh_sum[wh_sum['title'] == sel_p].iloc[0]
            p_s = dio_data[dio_data['inventory_title'] == sel_p]['quantity_sold'].sum()
            p_d = (p_w['Value'] / ((p_s * p_w['unit_cost']) + 1)) * days
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Stock Value", f"₹{p_w['Value']:,.0f}")
            c2.metric("Qty", f"{p_w['Qty']:,}")
            c3.metric("Sold", f"{p_s:,}")
            c4.metric("SKU DIO", f"{p_d:.1f}d")

        st.divider()
        df_sum['CEI'] = (df_sum['amount_received'] / (df_sum['invoiced_amount'] + 1)) * 100
        top_10 = df_sum.sort_values('closing_balance', ascending=False).head(10).copy()
        fig_cei = px.bar(top_10, x='customer_name', y='closing_balance', 
                         text=top_10['CEI'].apply(lambda x: f"{x:.1f}%"),
                         color='CEI', color_continuous_scale='RdYlGn', range_color=[0, 100])
        fig_cei.update_traces(textposition='outside')
        st.plotly_chart(fig_cei, use_container_width=True)

    with t2:
        st.header("⏳ Inventory Ageing")
        item_sales_vol = dio_data.groupby('inventory_title')['quantity_sold'].sum().reset_index()
        item_stats = pd.merge(wh_sum, item_sales_vol, left_on='title', right_on='inventory_title', how='left')
        item_stats['Item_DIO'] = (item_stats['Value'] / ((item_stats['quantity_sold'].fillna(0) * item_stats['unit_cost']) + 1)) * days
        
        def get_bucket(d): return "Fast" if d <= 30 else "Healthy" if d <= 90 else "High Risk"
        item_stats['Bucket'] = item_stats['Item_DIO'].apply(get_bucket)
        
        st.plotly_chart(px.pie(item_stats, values='Value', names='Bucket', hole=0.4), use_container_width=True)
        st.dataframe(item_stats[['title', 'Value', 'Item_DIO', 'Bucket']].sort_values('Value', ascending=False))

    with t3:
        st.header("🔧 Mappings")
        with st.form("map"):
            z = st.selectbox("Zoho", sorted(df_sales['item_name'].unique()))
            w = st.selectbox("Warehouse", sorted(wh_sum['title'].unique()))
            if st.form_submit_button("Save"):
                with conn.session as s:
                    s.execute(text("INSERT INTO item_mappings (zoho_name, inventory_title) VALUES (:z, :w) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), {"z":z, "w":w})
                    s.commit()
                st.rerun()
        st.dataframe(df_map)
else:
    st.info("Upload all 4 files to view the dashboard. You can sync the Ledger Summary to the DB as soon as file #1 is uploaded.")
