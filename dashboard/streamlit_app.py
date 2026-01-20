"""
Streamlit Dashboard for MongoDB Analytics
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
from datetime import datetime, timedelta
import time

# Page config
st.set_page_config(
    page_title="Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# API base URL
API_URL = "http://127.0.0.1:5000/api"

# Styling
st.markdown("""
    <style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .refresh-info {
        background-color: #e8f5e9;
        padding: 10px;
        border-radius: 5px;
        border-left: 4px solid #4caf50;
    }
    </style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)
def fetch_status():
    """Fetch system status."""
    try:
        response = requests.get(f"{API_URL}/status", timeout=5)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Error fetching status: {e}")
    return None

@st.cache_data(ttl=60)
def fetch_kpi():
    """Fetch KPI data."""
    try:
        response = requests.get(f"{API_URL}/kpi", timeout=5)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Error fetching KPI: {e}")
    return None

@st.cache_data(ttl=60)
def fetch_statistics():
    """Fetch statistics."""
    try:
        response = requests.get(f"{API_URL}/statistics", timeout=5)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Error fetching statistics: {e}")
    return None

@st.cache_data(ttl=300)
def fetch_clients(page=0, limit=100):
    """Fetch clients data."""
    try:
        response = requests.get(f"{API_URL}/clients", params={"page": page, "limit": limit}, timeout=5)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Error fetching clients: {e}")
    return None

@st.cache_data(ttl=300)
def fetch_purchases(page=0, limit=100, statut=None):
    """Fetch purchases data."""
    try:
        params = {"page": page, "limit": limit}
        if statut:
            params["statut"] = statut
        response = requests.get(f"{API_URL}/purchases", params=params, timeout=5)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Error fetching purchases: {e}")
    return None

@st.cache_data(ttl=600)
def fetch_sync_logs(days=7):
    """Fetch synchronization logs."""
    try:
        response = requests.get(f"{API_URL}/sync-log", params={"days": days}, timeout=5)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Error fetching sync logs: {e}")
    return None

# Main page
st.title("📊 Analytics Dashboard - MongoDB")
st.markdown("Real-time analytics dashboard powered by MongoDB and Streamlit")

# Sidebar
with st.sidebar:
    st.title("🔧 Settings")
    
    refresh_interval = st.slider("Refresh interval (seconds)", 10, 300, 60)
    
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    st.divider()
    
    st.subheader("API Status")
    api_status = fetch_status()
    if api_status:
        st.success("✓ API Connected")
        st.metric("Total Clients", api_status.get('clients', 0))
        st.metric("Total Purchases", api_status.get('purchases', 0))
        if api_status.get('last_sync'):
            st.caption(f"Last sync: {api_status.get('last_sync')}")
    else:
        st.error("✗ API Not Available")

# KPI Section
st.divider()
st.subheader("📈 Key Performance Indicators")

kpi_data = fetch_kpi()
if kpi_data:
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "Total Clients",
            f"{kpi_data.get('total_clients', 0):,.0f}",
            delta=None,
            delta_color="off"
        )
    
    with col2:
        st.metric(
            "Total Purchases",
            f"{kpi_data.get('total_achats', 0):,.0f}",
            delta=None,
            delta_color="off"
        )
    
    with col3:
        st.metric(
            "Revenue",
            f"${kpi_data.get('ca_total', 0):,.2f}",
            delta=None,
            delta_color="off"
        )
    
    with col4:
        st.metric(
            "Avg Order Value",
            f"${kpi_data.get('panier_moyen', 0):,.2f}",
            delta=None,
            delta_color="off"
        )
    
    with col5:
        st.metric(
            "Cancellation Rate",
            f"{kpi_data.get('taux_annulation', 0):.2f}%",
            delta=None,
            delta_color="off"
        )

# Statistics Section
st.divider()
st.subheader("📊 Statistics")

stats = fetch_statistics()
if stats:
    col1, col2 = st.columns(2)
    
    # By Status
    with col1:
        st.subheader("Purchases by Status")
        status_data = pd.DataFrame(stats.get('by_status', []))
        if not status_data.empty:
            status_data.rename(columns={'_id': 'Status', 'count': 'Count'}, inplace=True)
            fig_status = px.pie(status_data, values='Count', names='Status', 
                               title="Distribution by Status")
            st.plotly_chart(fig_status, use_container_width=True)
    
    # By Category
    with col2:
        st.subheader("Purchases by Category")
        category_data = pd.DataFrame(stats.get('by_category', []))
        if not category_data.empty:
            category_data.rename(columns={'_id': 'Category', 'count': 'Count'}, inplace=True)
            fig_category = px.bar(category_data, x='Category', y='Count',
                                 title="Purchases by Category")
            st.plotly_chart(fig_category, use_container_width=True)

# Clients Section
st.divider()
st.subheader("👥 Clients")

tab1, tab2 = st.tabs(["List", "Analytics"])

with tab1:
    clients_data = fetch_clients(limit=100)
    if clients_data and clients_data.get('data'):
        df_clients = pd.DataFrame(clients_data['data'])
        st.write(f"Showing {len(df_clients)} of {clients_data.get('total', 0)} clients")
        st.dataframe(df_clients, use_container_width=True, height=400)

with tab2:
    stats = fetch_statistics()
    if stats:
        country_data = pd.DataFrame(stats.get('by_country', []))
        if not country_data.empty:
            country_data.rename(columns={'_id': 'Country', 'count': 'Count'}, inplace=True)
            country_data = country_data.sort_values('Count', ascending=False).head(10)
            fig_country = px.bar(country_data, x='Country', y='Count',
                               title="Top 10 Countries by Client Count")
            st.plotly_chart(fig_country, use_container_width=True)

# Purchases Section
st.divider()
st.subheader("🛒 Purchases")

tab1, tab2 = st.tabs(["List", "Filters"])

with tab1:
    purchases_data = fetch_purchases(limit=100)
    if purchases_data and purchases_data.get('data'):
        df_purchases = pd.DataFrame(purchases_data['data'])
        st.write(f"Showing {len(df_purchases)} of {purchases_data.get('total', 0)} purchases")
        st.dataframe(df_purchases, use_container_width=True, height=400)

with tab2:
    col1, col2 = st.columns(2)
    with col1:
        selected_status = st.selectbox("Select Status", ["All", "livré", "en cours", "annulé"])
        status_filter = None if selected_status == "All" else selected_status
    
    if status_filter:
        purchases_data = fetch_purchases(limit=100, statut=status_filter)
        if purchases_data and purchases_data.get('data'):
            df_purchases = pd.DataFrame(purchases_data['data'])
            st.write(f"Found {len(df_purchases)} purchases with status: {status_filter}")
            st.dataframe(df_purchases, use_container_width=True)

# Sync Performance Section
st.divider()
st.subheader("⚡ Synchronization Performance")

sync_logs = fetch_sync_logs(days=7)
if sync_logs and sync_logs.get('data'):
    df_logs = pd.DataFrame(sync_logs['data'])
    
    # Calculate refresh time
    if len(df_logs) >= 2:
        df_logs['timestamp'] = pd.to_datetime(df_logs['timestamp'])
        df_logs = df_logs.sort_values('timestamp', ascending=False)
        
        latest = df_logs.iloc[0]
        previous = df_logs.iloc[1]
        
        refresh_time = (pd.to_datetime(latest['timestamp']) - pd.to_datetime(previous['timestamp'])).total_seconds()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Latest Sync Duration", f"{latest['duration_seconds']} seconds")
        
        with col2:
            st.metric("Refresh Interval", f"{refresh_time:.1f} seconds")
        
        with col3:
            st.metric("Throughput", f"{latest['documents_per_second']:.0f} docs/sec")
        
        with col4:
            st.metric("Last Status", latest['status'])
    
    # Sync logs table
    st.subheader("Sync Logs")
    df_logs['timestamp'] = pd.to_datetime(df_logs['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
    st.dataframe(df_logs[['timestamp', 'collection', 'status', 'row_count', 'duration_seconds']], 
                use_container_width=True, height=300)

# Footer
st.divider()
col1, col2, col3 = st.columns(3)
with col1:
    st.caption("📊 MongoDB Analytics Dashboard")
with col2:
    st.caption(f"🔄 Auto-refresh every {refresh_interval}s")
with col3:
    st.caption(f"⏰ Last updated: {datetime.now().strftime('%H:%M:%S')}")

# Auto-refresh
time.sleep(refresh_interval)
st.rerun()
