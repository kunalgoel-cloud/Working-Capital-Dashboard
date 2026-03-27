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

    with tab_map:
        st.header("Mapping Management")
        
        # Section A: New Mappings
        unmapped = [n for n in df_sales['item_name'].unique() if n not in df_map['zoho_name'].values]
        
        if unmapped:
            st.subheader(f"Unmapped Items ({len(unmapped)})")
            if st.button("⚡ Bulk Mark All Unmapped as Discontinued"):
                with conn.session as s:
                    for item in unmapped:
                        s.execute(text("INSERT INTO item_mappings VALUES (:z, :i) ON CONFLICT DO NOTHING"), 
                                  {"z": item, "i": "DISCONTINUED / OLD SKU"})
                    s.commit()
                st.success("Bulk mapping complete!")
                st.rerun()

            opts = list(df_wh['title'].unique()) + ["DISCONTINUED / OLD SKU"]
            for item in unmapped[:5]: # Batch of 5 for speed
                c1, c2 = st.columns([3, 1])
                choice = c1.selectbox(f"Map '{item}'", opts, key=f"new_{item}")
                if c2.button("Save Mapping", key=f"btn_{item}"):
                    with conn.session as s:
                        s.execute(text("INSERT INTO item_mappings VALUES (:z, :i) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title"), {"z":item, "i":choice})
                        s.commit()
                    st.toast(f"Saved: {item}", icon="✅")
                    time.sleep(0.5)
                    st.rerun()
        else:
            st.success("All items are currently mapped!")

        st.divider()
        
        # Section B: Edit Existing Mappings
        st.subheader("Edit Existing Mappings")
        search_term = st.text_input("Search mapped items...")
        
        display_map = df_map.copy()
        if search_term:
            display_map = display_map[display_map['zoho_name'].str.contains(search_term, case=False)]
        
        for idx, row in display_map.iterrows():
            c1, c2, c3 = st.columns([3, 3, 1])
            c1.text(row['zoho_name'])
            new_choice = c2.selectbox("Update to:", opts, index=opts.index(row['inventory_title']) if row['inventory_title'] in opts else 0, key=f"edit_{row['zoho_name']}")
            if c3.button("Update", key=f"upd_{row['zoho_name']}"):
                with conn.session as s:
                    s.execute(text("UPDATE item_mappings SET inventory_title = :i WHERE zoho_name = :z"), {"i":new_choice, "z":row['zoho_name']})
                    s.commit()
                st.toast("Updated successfully!")
                st.rerun()

    with tab_dash:
        if not unmapped or st.checkbox("Show Dashboard with partial data"):
            days = (date_range[1] - date_range[0]).days or 365
            
            # DSO/DPO
            avg_dso = (df_inv['bcy_balance'].sum() / (df_inv['bcy_total'].sum() + 1)) * days
            avg_dpo = (df_bill['bcy_balance'].sum() / (df_bill['bcy_total'].sum() + 1)) * days
            
            # DIO
            sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
            inv_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
            inv_sum = pd.concat([inv_sum, pd.DataFrame([{'title':'DISCONTINUED / OLD SKU','Qty':0,'Value':0}])])
            inv_sum['cost'] = inv_sum['Value'] / (inv_sum['Qty'] + 0.001)
            
            dio_merge = pd.merge(sales_mapped, inv_sum, left_on='inventory_title', right_on='title')
            total_cogs = (dio_merge['quantity_sold'] * dio_merge['cost']).sum()
            avg_dio = (inv_sum['Value'].sum() / (total_cogs + 1)) * days

            st.header("Working Capital Summary")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Receivable Days (DSO)", f"{avg_dso:.1f}")
            m2.metric("Inventory Days (DIO)", f"{avg_dio:.1f}")
            m3.metric("Payable Days (DPO)", f"{avg_dpo:.1f}")
            m4.metric("Cash Cycle", f"{(avg_dso + avg_dio - avg_dpo):.1f}")
            
            st.plotly_chart(px.bar(df_inv.groupby('customer_name')['bcy_balance'].sum().nlargest(10).reset_index(), 
                                   x='customer_name', y='bcy_balance', title="Top 10 AR Balances"))
        else:
            st.info("Go to the 'Manage Mappings' tab to link your items and unlock the dashboard.")
else:
    st.info("Please upload your files in the sidebar to begin.")
