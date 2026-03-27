import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
import time
from difflib import get_close_matches

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
    # Load raw CSV data
    df_inv = pd.read_csv(f_inv)
    df_bill = pd.read_csv(f_bill)
    df_sales = pd.read_csv(f_sales)
    df_wh = pd.read_csv(f_wh)
    
    df_inv['date'] = pd.to_datetime(df_inv['date']).dt.date
    df_bill['date'] = pd.to_datetime(df_bill['date']).dt.date

    # --- CRITICAL: Fetch Mappings with no-cache ---
    # We use a query that bypasses standard caching to ensure updates are visible
    df_map = conn.query("SELECT * FROM item_mappings", ttl=0)

    # Master SKU list (Current + Historical)
    current_wh_titles = list(df_wh['title'].unique())
    master_sku_list = sorted(list(set(current_wh_titles + df_map['inventory_title'].unique().tolist() + ["DISCONTINUED / OLD SKU"])))

    with tab_map:
        st.header("Mapping Management")
        
        # Calculate exactly what is NOT in the database yet
        zoho_in_file = df_sales['item_name'].unique()
        mapped_names = set(df_map['zoho_name'].tolist())
        unmapped = [n for n in zoho_in_file if n not in mapped_names]
        
        # --- TOP SECTION: NEW MAPPINGS ONLY ---
        if unmapped:
            st.subheader(f"⚠️ Items Needing Initial Mapping ({len(unmapped)})")
            st.info("Once saved, these items will move to the 'Existing Mappings' section below.")
            
            # Process one at a time to ensure the 'unmapped' list updates correctly
            item = unmapped[0] 
            c1, c2 = st.columns([3, 1])
            
            matches = get_close_matches(str(item), current_wh_titles, n=1, cutoff=0.3)
            default_val = matches[0] if matches else "DISCONTINUED / OLD SKU"
            
            choice = c1.selectbox(f"Map '{item}'", master_sku_list, 
                                index=master_sku_list.index(default_val), 
                                key=f"new_item_select")
            
            if c2.button("Save & Link", use_container_width=True):
                with conn.session as s:
                    s.execute(text("""
                        INSERT INTO item_mappings (zoho_name, inventory_title) 
                        VALUES (:z, :i) 
                        ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = EXCLUDED.inventory_title
                    """), {"z":item, "i":choice})
                    s.commit()
                st.toast(f"✅ Linked {item}")
                time.sleep(0.5)
                st.rerun() # Forces a refresh to re-calculate the 'unmapped' list
        else:
            st.success("🎉 All items in your current Sales file are mapped and ready!")

        st.divider()
        
        # --- BOTTOM SECTION: EXISTING MAPPINGS (SEARCHABLE) ---
        st.subheader("📋 Existing Mappings (Archive)")
        search = st.text_input("🔍 Search database...")
        
        # Filter logic
        display_edit = df_map.sort_values(by='zoho_name')
        if search:
            display_edit = display_edit[display_edit['zoho_name'].str.contains(search, case=False)]

        if not display_edit.empty:
            for idx, row in display_edit.head(15).iterrows():
                c1, c2, c3 = st.columns([3, 3, 1])
                stock_label = "🟢 In Stock" if row['inventory_title'] in current_wh_titles else "🔴 Out of Stock"
                c1.markdown(f"**{row['zoho_name']}** \n*{stock_label}*")
                
                curr_idx = master_sku_list.index(row['inventory_title']) if row['inventory_title'] in master_sku_list else 0
                new_choice = c2.selectbox("Change mapping:", master_sku_list, index=curr_idx, key=f"edit_{row['zoho_name']}")
                
                if c3.button("Update", key=f"upd_{row['zoho_name']}"):
                    with conn.session as s:
                        s.execute(text("UPDATE item_mappings SET inventory_title = :i WHERE zoho_name = :z"), 
                                  {"i":new_choice, "z":row['zoho_name']})
                        s.commit()
                    st.toast("Mapping Updated!")
                    time.sleep(0.3)
                    st.rerun()
        else:
            st.write("No mappings found.")

    with tab_dash:
        if len(date_range) == 2:
            start, end = date_range[0], date_range[1]
            days = (end - start).days or 365
            
            # Metrics Logic
            mask_inv = (df_inv['date'] >= start) & (df_inv['date'] <= end)
            mask_bill = (df_bill['date'] >= start) & (df_bill['date'] <= end)
            
            dso = (df_inv.loc[mask_inv, 'bcy_balance'].sum() / (df_inv.loc[mask_inv, 'bcy_total'].sum() + 1)) * days
            dpo = (df_bill.loc[mask_bill, 'bcy_balance'].sum() / (df_bill.loc[mask_bill, 'bcy_total'].sum() + 1)) * days
            
            # DIO Logic (handles missing stock via Left Join)
            sales_mapped = pd.merge(df_sales, df_map, left_on='item_name', right_on='zoho_name', how='inner')
            inv_sum = df_wh.groupby('title').agg({'Qty':'sum', 'Value':'sum'}).reset_index()
            inv_sum['unit_cost'] = inv_sum['Value'] / (inv_sum['Qty'] + 0.001)
            
            final_df = pd.merge(sales_mapped, inv_sum, left_on='inventory_title', right_on='title', how='left')
            final_df[['Value', 'unit_cost']] = final_df[['Value', 'unit_cost']].fillna(0)
            
            total_cogs = (final_df['quantity_sold'] * final_df['unit_cost']).sum()
            dio = (inv_sum['Value'].sum() / (total_cogs + 1)) * days

            # Display
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Rec. Days (DSO)", f"{dso:.1f}")
            m2.metric("Inv. Days (DIO)", f"{dio:.1f}")
            m3.metric("Pay. Days (DPO)", f"{dpo:.1f}")
            m4.metric("Cash Conversion Cycle", f"{(dso + dio - dpo):.1f}")
            
            st.plotly_chart(px.bar(df_inv.loc[mask_inv].groupby('customer_name')['bcy_balance'].sum().nlargest(10).reset_index(), 
                                   x='customer_name', y='bcy_balance', title="Top 10 AR Balances"))
else:
    st.info("Please upload your files in the sidebar.")
