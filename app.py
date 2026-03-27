import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
from datetime import datetime
import time

st.set_page_config(page_title="Working Capital Dashboard Pro", layout="wide")

def get_conn():
    return st.connection("postgresql", type="sql")

conn = get_conn()

# --- 1. DATABASE INITIALIZATION ---
def init_db():
    with conn.session as s:
        # Core Mapping Table
        s.execute(text("CREATE TABLE IF NOT EXISTS item_mappings (zoho_name TEXT PRIMARY KEY, inventory_title TEXT);"))
        # Inventory Snapshots
        s.execute(text("""
            CREATE TABLE IF NOT EXISTS inventory_history (
                id SERIAL PRIMARY KEY, 
                sku_title TEXT, 
                qty FLOAT, 
                value FLOAT, 
                snapshot_date DATE, 
                UNIQUE(sku_title, snapshot_date)
            );
        """))
        # Ledger Truth
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

# --- 2. THE UPSERT ENGINE WITH PROGRESS BAR ---
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
            s.execute(query, {
                "cid": str(row['customer_id']),
                "cname": row['customer_name'],
                "inv": row['invoiced_amount'],
                "rec": row['amount_received'],
                "bal": row['closing_balance']
            })
            
            # Update Progress every 50 rows to save resources
            if index % 50 == 0 or index == total_rows - 1:
                percent = int((index + 1) / total_rows * 100)
                progress_bar.progress(percent)
                status_text.text(f"Syncing Ledger: {percent}% ({index+1}/{total_rows})")
        s.commit()
    status_text.success(f"✅ Successfully synced {total_rows} ledger records.")

# --- 3. SIDEBAR & DATA INGESTION ---
with st.sidebar:
    st.header("🎯 Business Targets")
    t_dso = st.number_input("Target DSO", value=120)
    t_dio = st.number_input("Target DIO", value=45)
    t_dpo = st.number_input("Target DPO", value=90)
    
    st.divider()
    st.header("📂 Data Ingestion & Sync")
    f_sum = st.file_uploader("1. Customer Balance Summary (DSO Truth)", type="csv")
    if f_sum and st.button("Sync Ledger to Postgres"):
        sync_ledger_to_db(pd.read_csv(f_sum))
        
    f_wh = st.file_uploader("2. Warehouse Export (DIO Truth)", type="csv")
    f_sales = st.file_uploader("3. Sales by Item (COGS Context)", type="csv")
    f_bill = st.file_uploader("4. Bill Details (DPO Truth)", type="csv")
    
    date_range = st.date_input("Analysis Period", [pd.to_datetime("2025-04-01"), pd.to_datetime("2026-03-31")])

# --- 4. PROCESSING LOGIC ---
if all([f_sum, f_wh, f_sales, f_bill]):
    df_sum, df_wh, df_sales, df_bill = pd.read_csv(f_sum), pd.read_csv(f_wh), pd.read_csv(f_sales), pd.read_csv(f_bill)
    df_map = conn.query("SELECT * FROM item_mappings", ttl=0)
    days = (date_range[1] - date_range[0]).days or 365

    # DSO Calculation (Ledger Truth)
    avg_dso = (df_sum['closing_balance'].sum() / (df_sum['invoiced_amount'].sum() + 1)) * days
    
    # DPO Calculation
    avg_dpo = (df_bill['bcy_balance'].sum() / (df_bill['bcy_total'].sum() + 1)) * days
    
    # DIO Calculation
    wh_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
    wh_sum['unit_cost'] = wh_sum['Value'] / wh_sum['Qty'].replace(0, 1)
    sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
    dio_data = pd.merge(sales_mapped, wh_sum[['title', 'unit_cost']], left_on='inventory_title', right_on='title', how='left')
    total_cogs = (dio_data['quantity_sold'] * dio_data['unit_cost'].fillna(0)).sum()
    avg_dio = (wh_sum['Value'].sum() / (total_cogs + 1)) * days
    
    # CCC
    ccc = avg_dso + avg_dio - avg_dpo
    coverage = (len(df_map[df_map['zoho_name'].isin(df_sales['item_name'])]) / df_sales['item_name'].nunique()) * 100 if df_sales['item_name'].nunique() > 0 else 0

    # --- MAIN TABS ---
    t1, t2, t3 = st.tabs(["📊 Dashboard", "⏳ Ageing", "🔧 Mappings"])

    with t1:
        # Row 1: The 4 Pillars
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("DSO (Receivables)", f"{avg_dso:.1f}d", f"{avg_dso-t_dso:+.1f}", "inverse")
        m2.metric("DIO (Inventory)", f"{avg_dio:.1f}d", f"{avg_dio-t_dio:+.1f}", "inverse")
        m3.metric("DPO (Payables)", f"{avg_dpo:.1f}d", f"{avg_dpo-t_dpo:+.1f}")
        m4.metric("Cash Cycle (CCC)", f"{ccc:.1f}d")
        st.caption(f"Mapping Coverage: {coverage:.1f}%")

        # Row 2: Product Deep Dive
        st.divider()
        st.subheader("🔍 Product Deep Dive")
        sel_p = st.selectbox("Select SKU to inspect:", ["All Products"] + sorted(wh_sum['title'].tolist()))
        if sel_p != "All Products":
            p_w = wh_sum[wh_sum['title'] == sel_p].iloc[0]
            p_s_data = dio_data[dio_data['inventory_title'] == sel_p]
            p_s_qty = p_s_data['quantity_sold'].sum()
            p_d = (p_w['Value'] / ((p_s_qty * p_w['unit_cost']) + 1)) * days
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Stock Value", f"₹{p_w['Value']:,.2f}")
            c2.metric("Qty on Hand", f"{p_w['Qty']:,}")
            c3.metric("Units Sold", f"{p_s_qty:,}")
            c4.metric("Product DIO", f"{p_d:.1f} days")

        # Row 3: CEI Chart
        st.divider()
        st.subheader("💳 Collection Efficiency Index (CEI)")
        df_sum['CEI'] = (df_sum['amount_received'] / (df_sum['invoiced_amount'] + 1)) * 100
        top_10 = df_sum.sort_values('closing_balance', ascending=False).head(10).copy()
        
        fig_cei = px.bar(
            top_10, 
            x='customer_name', 
            y='closing_balance', 
            text=top_10['CEI'].apply(lambda x: f"CEI: {x:.1f}%"),
            color='CEI', 
            color_continuous_scale='RdYlGn', 
            range_color=[0, 100],
            title="Top 10 AR Balances (Ledger Truth)"
        )
        fig_cei.update_traces(textposition='outside')
        st.plotly_chart(fig_cei, use_container_width=True)

    with t2:
        st.header("⏳ Inventory Health")
        item_sales_vol = dio_data.groupby('inventory_title')['quantity_sold'].sum().reset_index()
        item_stats = pd.merge(wh_sum, item_sales_vol, left_on='title', right_on='inventory_title', how='left')
        item_stats['Item_DIO'] = (item_stats['Value'] / ((item_stats['quantity_sold'].fillna(0) * item_stats['unit_cost']) + 1)) * days
        
        # FIXED: Function name consistency (get_bucket)
        def get_bucket(d): 
            return "Fast" if d <= 30 else "Healthy" if d <= 90 else "High Risk"
            
        item_stats['Bucket'] = item_stats['Item_DIO'].apply(get_bucket)
        
        st.plotly_chart(px.pie(item_stats, values='Value', names='Bucket', hole=0.4), use_container_width=True)
        st.dataframe(item_stats[['title', 'Value', 'Item_DIO', 'Bucket']].sort_values('Value', ascending=False))

    with t3:
        st.header("🔧 Mapping Management")
        with st.form("mapping_form"):
            z_item = st.selectbox("Zoho Item Name", sorted(df_sales['item_name'].unique()))
            w_sku = st.selectbox("Warehouse SKU Title", sorted(wh_sum['title'].unique()))
            if st.form_submit_button("Save Mapping"):
                with conn.session as s:
                    s.execute(text("INSERT INTO item_mappings (zoho_name, inventory_title) VALUES (:z, :w) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), {"z": z_item, "w": w_sku})
                    s.commit()
                st.rerun()
        st.subheader("Current Mappings in Database")
        st.dataframe(df_map, use_container_width=True)

else:
    st.info("Upload the 4 required CSV files to activate the dashboard.")
