import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
from datetime import datetime
from difflib import get_close_matches

st.set_page_config(page_title="Working Capital Dash", layout="wide")

def get_conn():
    return st.connection("postgresql", type="sql")

conn = get_conn()

# --- DATABASE SETUP ---
def init_db():
    with conn.session as s:
        s.execute(text("CREATE TABLE IF NOT EXISTS item_mappings (zoho_name TEXT PRIMARY KEY, inventory_title TEXT);"))
        s.commit()

init_db()

# --- SIDEBAR ---
with st.sidebar:
    st.header("🎯 Business Targets")
    target_dso = st.number_input("Target DSO (Customer Credit)", value=120)
    target_dio = st.number_input("Target DIO (Stock Turn)", value=45)
    target_dpo = st.number_input("Target DPO (Vendor Credit)", value=90)
    
    st.divider()
    st.header("📂 Data Ingestion")
    f_inv = st.file_uploader("Upload Invoices", type="csv")
    f_bill = st.file_uploader("Upload Bills", type="csv")
    f_sales = st.file_uploader("Upload Sales Items", type="csv")
    f_wh = st.file_uploader("Upload Warehouse Export", type="csv")
    date_range = st.date_input("Analysis Period", [pd.to_datetime("2025-04-01"), pd.to_datetime("2026-03-31")])

# --- MAIN TABS ---
tab_dash, tab_ageing, tab_map = st.tabs(["📊 Performance Dashboard", "⏳ Inventory Ageing", "🔧 Mappings"])

if all([f_inv, f_bill, f_sales, f_wh]):
    # 1. LOAD DATA
    df_inv, df_bill = pd.read_csv(f_inv), pd.read_csv(f_bill)
    df_sales, df_wh = pd.read_csv(f_sales), pd.read_csv(f_wh)
    df_map = conn.query("SELECT * FROM item_mappings", ttl=0)
    days_in_period = (date_range[1] - date_range[0]).days or 365

    # 2. DATA COVERAGE CALCULATION
    total_sales_items = df_sales['item_name'].nunique()
    mapped_items_count = len(df_map[df_map['zoho_name'].isin(df_sales['item_name'])])
    coverage_pct = (mapped_items_count / total_sales_items) * 100 if total_sales_items > 0 else 0

    # 3. CORE CALCULATIONS
    avg_dso = (df_inv['bcy_balance'].sum() / (df_inv['bcy_total'].sum() + 1)) * days_in_period
    avg_dpo = (df_bill['bcy_balance'].sum() / (df_bill['bcy_total'].sum() + 1)) * days_in_period

    wh_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
    wh_sum['unit_cost'] = wh_sum['Value'] / wh_sum['Qty'].replace(0, 1)
    
    sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
    dio_data = pd.merge(sales_mapped, wh_sum[['title', 'unit_cost']], left_on='inventory_title', right_on='title', how='left')
    
    total_cogs = (dio_data['quantity_sold'] * dio_data['unit_cost'].fillna(0)).sum()
    avg_dio = (wh_sum['Value'].sum() / (total_cogs + 1)) * days_in_period
    ccc = avg_dso + avg_dio - avg_dpo

    # --- TAB 1: DASHBOARD ---
    with tab_dash:
        # Top Row Metrics
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("DSO (Receivables)", f"{avg_dso:.1f}d", f"{avg_dso - target_dso:+.1f}", delta_color="inverse")
        m2.metric("DIO (Inventory)", f"{avg_dio:.1f}d", f"{avg_dio - target_dio:+.1f}", delta_color="inverse")
        m3.metric("DPO (Payables)", f"{avg_dpo:.1f}d", f"{avg_dpo - target_dpo:+.1f}")
        m4.metric("Net Cash Cycle (CCC)", f"{ccc:.1f}d")

        # Data Coverage Gauge
        st.divider()
        c_cov, c_msg = st.columns([1, 3])
        c_cov.metric("Data Mapping Coverage", f"{coverage_pct:.1f}%")
        if coverage_pct < 95:
            c_msg.warning(f"⚠️ **Note:** {100-coverage_pct:.1f}% of sales are unmapped. Your DIO of {avg_dio:.1f}d may be higher than reality. Check the 'Mappings' tab.")
        else:
            c_msg.success("✅ High Data Coverage: Your Inventory metrics are highly reliable.")

        # DYNAMIC PRODUCT FILTER
        st.subheader("🔍 Deep Dive: Product Analysis")
        selected_product = st.selectbox("Select a Warehouse SKU:", ["All Products"] + sorted(wh_sum['title'].tolist()))
        
        if selected_product == "All Products":
            st.info("Showing aggregate data for all mapped products.")
        else:
            # Filter specific product data
            p_wh = wh_sum[wh_sum['title'] == selected_product].iloc[0]
            p_sales = dio_data[dio_data['inventory_title'] == selected_product]
            
            p_qty_sold = p_sales['quantity_sold'].sum()
            p_cogs = (p_qty_sold * p_wh['unit_cost'])
            p_dio = (p_wh['Value'] / (p_cogs + 1)) * days_in_period
            
            p1, p2, p3, p4 = st.columns(4)
            p1.metric("Current Stock Value", f"₹{p_wh['Value']:,.2f}")
            p2.metric("Quantity on Hand", f"{p_wh['Qty']:,}")
            p3.metric("Units Sold (Period)", f"{p_qty_sold:,}")
            p4.metric("Product Specific DIO", f"{p_dio:.1f} days")

        st.divider()
        cust_bal = df_inv.groupby('customer_name')['bcy_balance'].sum().reset_index().sort_values('bcy_balance', ascending=False).head(10)
        st.plotly_chart(px.bar(cust_bal, x='customer_name', y='bcy_balance', title="Top 10 AR Balances"), use_container_width=True)

    # --- TAB 2: INVENTORY AGEING ---
    with tab_ageing:
        st.header("⏳ Inventory Health & Ageing")
        item_sales_vol = dio_data.groupby('inventory_title')['quantity_sold'].sum().reset_index()
        item_stats = pd.merge(wh_sum, item_sales_vol, left_on='title', right_on='inventory_title', how='left')
        item_stats['Item_DIO'] = (item_stats['Value'] / ((item_stats['quantity_sold'].fillna(0) * item_stats['unit_cost']) + 1)) * days_in_period
        
        def get_bucket(d):
            if d <= 30: return "0-30 Days (Fast)"
            if d <= 90: return "31-90 Days (Healthy)"
            return "90+ Days (High Risk)"
        
        item_stats['Ageing Bucket'] = item_stats['Item_DIO'].apply(get_bucket)
        
        st.plotly_chart(px.pie(item_stats, values='Value', names='Ageing Bucket', hole=0.4, 
                         color_discrete_map={"0-30 Days (Fast)": "#2ecc71", "31-60 Days (Healthy)": "#3498db", "90+ Days (High Risk)": "#e74c3c"}), use_container_width=True)
        st.dataframe(item_stats[['title', 'Qty', 'Value', 'Item_DIO', 'Ageing Bucket']].sort_values('Value', ascending=False), use_container_width=True)

    # --- TAB 3: MAPPINGS (FULL EDITOR) ---
    with tab_map:
        st.header("🔧 Mapping Management")
        with st.form("manual_mapping_form"):
            st.subheader("🖊️ Manual Mapping Editor")
            col1, col2 = st.columns(2)
            z_item = col1.selectbox("Zoho Item Name", sorted(df_sales['item_name'].unique()))
            w_sku = col2.selectbox("Warehouse SKU", sorted(wh_sum['title'].unique()))
            if st.form_submit_button("Save Mapping"):
                with conn.session as s:
                    s.execute(text("INSERT INTO item_mappings (zoho_name, inventory_title) VALUES (:z, :i) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), {"z": z_item, "i": w_sku})
                    s.commit()
                st.rerun()

        st.divider()
        unmapped = [n for n in df_sales['item_name'].unique() if n not in df_map['zoho_name'].tolist()]
        if unmapped:
            st.subheader(f"⚡ Bulk Suggestion Tool ({len(unmapped)} items)")
            if st.button("Auto-Match Unmapped Items"):
                with conn.session as s:
                    for item in unmapped:
                        match = get_close_matches(item, wh_sum['title'].tolist(), n=1, cutoff=0.1)
                        if match:
                            s.execute(text("INSERT INTO item_mappings (zoho_name, inventory_title) VALUES (:z, :i) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), {"z": item, "i": match[0]})
                    s.commit()
                st.rerun()

        st.subheader("Current Mapping Table")
        st.dataframe(df_map, use_container_width=True)

else:
    st.info("👋 Please upload your 4 CSV files to activate the dashboard.")
