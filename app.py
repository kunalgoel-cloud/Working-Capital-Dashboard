import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text

# --- Configuration & DB Connection ---
st.set_page_config(page_title="Working Capital Dashboard", layout="wide")

# Database Connection (Neon Tech Postgres)
# Setup in Streamlit Secrets: [connections.postgresql]
def get_db_connection():
    try:
        # Uses Streamlit's built-in SQL connection
        conn = st.connection("postgresql", type="sql")
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

# --- Helper Functions for Mapping Memory ---
def load_mappings(conn):
    if conn is not None:
        try:
            query = "SELECT zoho_name, inventory_title FROM item_mappings"
            return conn.query(query)
        except:
            # Create table if it doesn't exist
            with conn.session as s:
                s.execute(text("CREATE TABLE IF NOT EXISTS item_mappings (zoho_name TEXT PRIMARY KEY, inventory_title TEXT)"))
                s.commit()
            return pd.DataFrame(columns=["zoho_name", "inventory_title"])
    return pd.DataFrame(columns=["zoho_name", "inventory_title"])

def save_mapping(conn, zoho_name, inv_title):
    with conn.session as s:
        s.execute(
            text("INSERT INTO item_mappings (zoho_name, inventory_title) VALUES (:z, :i) ON CONFLICT (zoho_name) DO UPDATE SET inventory_title = :i"),
            {"z": zoho_name, "i": inv_title}
        )
        s.commit()

# --- App UI ---
st.title("🚀 Working Capital Dashboard")
st.sidebar.header("Upload Data Sources")

# File Uploaders
ar_aging_file = st.sidebar.file_uploader("1. AR Aging Summary", type="csv")
inv_details_file = st.sidebar.file_uploader("2. Invoice Details", type="csv")
sales_item_file = st.sidebar.file_uploader("3. Sales by Item", type="csv")
inventory_file = st.sidebar.file_uploader("4. Manual Inventory Export", type="csv")
bill_details_file = st.sidebar.file_uploader("5. Bill Details", type="csv")

days_in_period = st.sidebar.number_input("Analysis Period (Days)", value=365)

conn = get_db_connection()
mapping_df = load_mappings(conn)

if all([ar_aging_file, inv_details_file, sales_item_file, inventory_file, bill_details_file]):
    # Read Files
    df_aging = pd.read_csv(ar_aging_file)
    df_inv_details = pd.read_csv(inv_details_file)
    df_sales_item = pd.read_csv(sales_item_file)
    df_inventory = pd.read_csv(inventory_file)
    df_bills = pd.read_csv(bill_details_file)

    # --- PART 1: MAPPING LOGIC ---
    st.header("🔗 Item Mapping & Memory")
    
    # Clean column names
    df_sales_item.columns = df_sales_item.columns.str.strip()
    df_inventory.columns = df_inventory.columns.str.strip()
    
    # Identify unique Zoho items
    zoho_items = df_sales_item['item_name'].unique()
    inventory_titles = df_inventory['title'].unique()
    
    unmapped = [item for item in zoho_items if item not in mapping_df['zoho_name'].values]
    
    if unmapped:
        st.warning(f"Found {len(unmapped)} new unmapped items from Zoho.")
        with st.expander("Map New Items"):
            for item in unmapped:
                col1, col2 = st.columns([2,1])
                choice = col1.selectbox(f"Map '{item}' to:", inventory_titles, key=item)
                if col2.button("Save", key=f"btn_{item}"):
                    save_mapping(conn, item, choice)
                    st.rerun()
    else:
        st.success("All items are mapped!")

    # --- PART 2: CALCULATIONS ---
    
    # A. Receivable Days (DSO)
    cust_sales = df_inv_details.groupby('customer_name')['bcy_total'].sum().reset_index()
    ar_data = pd.merge(df_aging, cust_sales, on='customer_name', how='left')
    ar_data['DSO'] = (ar_data['total'] / ar_data['bcy_total']) * days_in_period
    avg_dso = ar_data['DSO'].mean()

    # B. Payable Days (DPO)
    vendor_data = df_bills.groupby('vendor_name').agg({'bcy_total': 'sum', 'bcy_balance': 'sum'}).reset_index()
    vendor_data['DPO'] = (vendor_data['bcy_balance'] / vendor_data['bcy_total']) * days_in_period
    avg_dpo = vendor_data['DPO'].mean()

    # C. Inventory Days (DIO)
    # Apply Mapping
    df_sales_mapped = pd.merge(df_sales_item, mapping_df, left_on='item_name', right_on='zoho_name', how='left')
    # Group inventory by title to get total value/qty
    inv_summary = df_inventory.groupby('title').agg({'Qty': 'sum', 'Value': 'sum'}).reset_index()
    inv_summary['unit_cost'] = inv_summary['Value'] / (inv_summary['Qty'] + 0.001)
    
    dio_data = pd.merge(df_sales_mapped, inv_summary, left_on='inventory_title', right_on='title', how='left')
    dio_data['COGS'] = dio_data['quantity_sold'] * dio_data['unit_cost']
    dio_data['DIO'] = (dio_data['Value'] / (dio_data['COGS'] + 0.1)) * days_in_period
    avg_dio = dio_data['DIO'].mean()

    # --- PART 3: VISUALIZATION ---
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Receivable Days (DSO)", f"{avg_dso:.1f} Days")
    kpi2.metric("Inventory Days (DIO)", f"{avg_dio:.1f} Days")
    kpi3.metric("Payable Days (DPO)", f"{avg_dpo:.1f} Days")
    ccc = avg_dso + avg_dio - avg_dpo
    kpi4.metric("Cash Conversion Cycle", f"{ccc:.1f} Days", delta_color="inverse")

    tab1, tab2, tab3 = st.tabs(["Receivables (Customers)", "Inventory (Products)", "Payables (Vendors)"])

    with tab1:
        st.subheader("Customer DSO Analysis")
        fig_ar = px.bar(ar_data.sort_values('DSO', ascending=False).head(15), 
                        x='customer_name', y='DSO', color='total', title="Top 15 Slowest Paying Customers")
        st.plotly_chart(fig_ar, use_container_width=True)

    with tab2:
        st.subheader("Product Inventory Days")
        fig_inv = px.scatter(dio_data, x='quantity_sold', y='DIO', size='Value', 
                             hover_name='item_name', title="Sales Volume vs Inventory Days")
        st.plotly_chart(fig_inv, use_container_width=True)

    with tab3:
        st.subheader("Vendor DPO Analysis")
        st.dataframe(vendor_data.sort_values('DPO', ascending=False))

else:
    st.info("Waiting for all 5 CSV files to be uploaded to calculate the Working Capital Cycle.")
