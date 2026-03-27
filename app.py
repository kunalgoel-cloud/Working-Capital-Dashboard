import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
from datetime import datetime

# --- CONFIGURATION ---
st.set_page_config(page_title="Working Capital Dash", layout="wide")

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

# --- SIDEBAR: DATA UPLOAD & TARGETS ---
with st.sidebar:
    st.header("1. Data Ingestion")
    with st.expander("Upload CSVs"):
        f_inv = st.file_uploader("Invoices", type="csv")
        f_bill = st.file_uploader("Bills", type="csv")
        f_sales = st.file_uploader("Sales Items", type="csv")
        f_wh = st.file_uploader("Warehouse Export", type="csv")
    
    st.header("2. Filters")
    date_range = st.date_input("Analysis Period", [pd.to_datetime("2025-04-01"), pd.to_datetime("2026-03-31")])
    
    # NEW: BUSINESS OBJECTIVE/TARGET SETTING
    st.header("🎯 3. Set Your Objectives")
    st.caption("We use these to measure your actual performance.")
    target_dso = st.number_input("Target Receivable Days (DSO)", value=120)
    target_dio = st.number_input("Target Inventory Days (DIO)", value=45)
    target_dpo = st.number_input("Target Payable Days (DPO)", value=90)


# --- MAIN INTERFACE ---
tab_dash, tab_insights, tab_map = st.tabs(["📊 Dashboard", "💡 Actionable Insights", "🔧 Manage Mappings"])

if all([f_inv, f_bill, f_sales, f_wh]):
    # Load and Clean
    df_inv = pd.read_csv(f_inv)
    df_bill = pd.read_csv(f_bill)
    df_sales = pd.read_csv(f_sales)
    df_wh = pd.read_csv(f_wh)
    
    df_inv['date'] = pd.to_datetime(df_inv['date']).dt.date
    df_bill['date'] = pd.to_datetime(df_bill['date']).dt.date
    
    df_map = conn.query("SELECT * FROM item_mappings")
    current_wh_titles = list(df_wh['title'].unique())

    # --- TAB: MAPPING (RETAINED FROM PREVIOUS STEPS) ---
    with tab_map:
        st.info("You already complete mapping. Go to the Dashboard or Insights.")

    # --- TAB: DASHBOARD (WITH KPI COLORING) ---
    with tab_dash:
        if len(date_range) == 2:
            start, end = date_range[0], date_range[1]
            days = (end - start).days or 365
            
            mask_inv = (df_inv['date'] >= start) & (df_inv['date'] <= end)
            mask_bill = (df_bill['date'] >= start) & (df_bill['date'] <= end)
            
            curr_inv = df_inv.loc[mask_inv]
            curr_bill = df_bill.loc[mask_bill]

            # CALCULATIONS
            # DSO
            avg_dso = (curr_inv['bcy_balance'].sum() / (curr_inv['bcy_total'].sum() + 1)) * days
            dso_status = "red" if avg_dso > target_dso else "green"

            # DPO
            avg_dpo = (curr_bill['bcy_balance'].sum() / (curr_bill['bcy_total'].sum() + 1)) * days
            dpo_status = "green" if avg_dpo >= target_dpo else "red" # High DPO is usually good (more credit)
            
            # DIO (Inventory)
            sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
            inv_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
            inv_sum['unit_cost'] = inv_sum['Value'] / (inv_sum['Qty'] + 0.001)
            
            final_df = pd.merge(sales_mapped, inv_sum, left_on='inventory_title', right_on='title', how='left')
            final_df[['Value', 'unit_cost']] = final_df[['Value', 'unit_cost']].fillna(0)
            
            total_cogs = (final_df['quantity_sold'] * final_df['unit_cost']).sum()
            total_inv_val = inv_sum['Value'].sum()
            avg_dio = (total_inv_val / (total_cogs + 1)) * days
            dio_status = "red" if avg_dio > target_dio else "green"

            ccc = avg_dso + avg_dio - avg_dpo

            # HIGH LEVEL METRICS (KPI Display)
            st.header("Working Capital Summary")
            
            c1, c2, c3, c4 = st.columns(4)
            
            # We use markdown for custom color based on status
            with c1:
                st.markdown(f"**DSO (Receivable Days)**")
                st.markdown(f"<h1 style='color: {dso_status};'>{avg_dso:.1f}</h1>", unsafe_allow_html=True)
                st.caption(f"Goal: {target_dso} | Difference: {avg_dso - target_dso:+.1f}")
                
            with c2:
                st.markdown(f"**DIO (Inventory Days)**")
                st.markdown(f"<h1 style='color: {dio_status};'>{avg_dio:.1f}</h1>", unsafe_allow_html=True)
                st.caption(f"Goal: {target_dio} | Difference: {avg_dio - target_dio:+.1f}")
                
            with c3:
                st.markdown(f"**DPO (Payable Days)**")
                st.markdown(f"<h1 style='color: {dpo_status};'>{avg_dpo:.1f}</h1>", unsafe_allow_html=True)
                st.caption(f"Goal: {target_dpo} | Difference: {avg_dpo - target_dpo:+.1f}")
                
            with c4:
                st.markdown(f"**Cash Cycle (CCC)**")
                # Red is bad for CCC (longer cycle)
                ccc_status = "red" if ccc > (target_dso + target_dio - target_dpo) else "green"
                st.markdown(f"<h1 style='color: {ccc_status};'>{ccc:.1f}</h1>", unsafe_allow_html=True)
                st.caption(f"Goal CCC: {(target_dso + target_dio - target_dpo):.1f}")

            st.divider()
            
            # Bar Chart (Retained)
            st.subheader("Top 10 AR Balances by Customer")
            fig = px.bar(curr_inv.groupby('customer_name')['bcy_balance'].sum().nlargest(10).reset_index(), 
                         x='customer_name', y='bcy_balance', color='bcy_balance', color_continuous_scale='Blues')
            st.plotly_chart(fig, use_container_width=True)

    # --- TAB: ACTIONABLE INSIGHTS (NEW TAB) ---
    with tab_insights:
        st.header("💡 Actionable Insights & Dynamic Recommendations")
        
        c1, c2 = st.columns(2)
        
        # 1. DSO Analysis (Where is the Receivables Problem?)
        with c1:
            st.subheader("⚠️ DSO Breakdown: Customers Exceeding Goal")
            st.caption(f"Calculation DSO = (Customer's BCY Balance / BCY Total Sales) * {days} Days")
            cust_analysis = curr_inv.groupby('customer_name').agg({'bcy_total':'sum', 'bcy_balance':'sum'}).reset_index()
            cust_analysis['Customer DSO'] = (cust_analysis['bcy_balance'] / (cust_analysis['bcy_total'] + 0.1)) * days
            
            # Filter to show only problems
            high_risk = cust_analysis[cust_analysis['Customer DSO'] > target_dso].sort_values('Customer DSO', ascending=False)
            
            if not high_risk.empty:
                st.dataframe(high_risk[['customer_name', 'bcy_total', 'bcy_balance', 'Customer DSO']], use_container_width=True)
                st.success(f"**Insight:** {len(high_risk)} customer(s) are taking longer to pay than your goal of {target_dso} days.")
            else:
                st.success(f"**Insight:** Amazing! No customer exceeds your goal of {target_dso} DSO.")

        # 2. DIO Analysis (Where is the Inventory Problem?)
        with c2:
            st.subheader("🛑 DIO Breakdown: Slow Moving SKUs")
            st.caption(f"Calculation DIO = (SKU's Inventory Value / Period COGS) * {days} Days")
            
            # Use final_df which has mapping and left-joined inventory value
            item_analysis = final_df.copy()
            item_analysis['Period COGS'] = item_analysis['quantity_sold'] * item_analysis['unit_cost']
            item_analysis = item_analysis.groupby('inventory_title').agg({'Value':'sum', 'Period COGS':'sum'}).reset_index()
            
            item_analysis['Item DIO'] = (item_analysis['Value'] / (item_analysis['Period COGS'] + 0.1)) * days
            
            # Filter for items above target DIO or items that have 0 sales in the period but have value
            slow_items = item_analysis[item_analysis['Item DIO'] > target_dio].sort_values('Item DIO', ascending=False)
            
            if not slow_items.empty:
                st.dataframe(slow_items[['inventory_title', 'Value', 'Period COGS', 'Item DIO']], use_container_width=True)
                st.warning(f"**Insight:** {len(slow_items)} item(s) are holding stock for longer than your goal of {target_dio} days.")
            else:
                st.success(f"**Insight:** Excellent inventory flow.")

        st.divider()
        
        # 3. Dynamic Recommendations Section
        st.subheader("📝 Personalized Business Actions")
        recs = []
        
        if avg_dso > target_dso:
            recs.append(f"🔴 DSO is {avg_dso:.1f} vs. target {target_dso}. **Action:** Contact the top {high_risk.head(3)['customer_name'].str.cat(sep=', ')} immediately to collect payments. Re-evaluate their credit limits.")
        else:
            recs.append(f"🟢 DSO is {avg_dso:.1f}, below target. **Action:** Maintain current collection procedures.")
            
        if avg_dio > target_dio:
            recs.append(f"🛑 DIO is {avg_dio:.1f} vs. target {target_dio}. **Action:** The SKUs {slow_items.head(3)['inventory_title'].str.cat(sep=', ')} are slowing you down. Plan a liquidation promotion or reduce future purchase orders for these items.")
        else:
            recs.append(f"🟢 DIO is {avg_dio:.1f}, below target. **Action:** Good job syncing production and sales.")
            
        if avg_dpo < target_dpo:
            recs.append(f"⚠️ DPO is {avg_dpo:.1f} vs. target {target_dpo}. **Action:** You are paying vendors faster than your goal. Negotiate better payment terms (e.g., Net 90 instead of Net 60) to keep cash in the business longer.")
        
        # Output recommendations
        for r in recs:
            st.markdown(r)
else:
    st.info("Awaiting file uploads in the sidebar to populate insights.")
