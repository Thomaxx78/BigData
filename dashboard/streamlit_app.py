import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
from datetime import datetime
import time

st.set_page_config(
    page_title="Analytics Platform",
    page_icon="",
    layout="wide",
    initial_sidebar_state="collapsed"
)

API_URL = "http://127.0.0.1:5000/api"

COLORS = {
    "khaki": "#8B9A6B",
    "khaki_light": "#A4B382",
    "khaki_dark": "#6B7A4B",
    "white": "#FFFFFF",
    "gray_light": "#F5F5F5",
    "gray": "#E0E0E0",
    "gray_dark": "#9E9E9E",
    "text": "#2D2D2D",
    "text_light": "#6B6B6B",
}

st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    .stApp {{
        background-color: {COLORS["gray_light"]};
        font-family: 'Inter', sans-serif;
    }}

    .main-header {{
        background: linear-gradient(135deg, {COLORS["khaki"]} 0%, {COLORS["khaki_dark"]} 100%);
        padding: 2rem 2.5rem;
        border-radius: 0 0 24px 24px;
        margin: -1rem -1rem 2rem -1rem;
        color: white;
    }}

    .main-header h1 {{
        font-size: 2rem;
        font-weight: 600;
        margin: 0;
        letter-spacing: -0.5px;
    }}

    .main-header p {{
        font-size: 0.95rem;
        opacity: 0.9;
        margin: 0.5rem 0 0 0;
        font-weight: 300;
    }}

    .metric-card {{
        background: {COLORS["white"]};
        border-radius: 16px;
        padding: 1.5rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
        border: 1px solid {COLORS["gray"]};
        transition: all 0.2s ease;
    }}

    .metric-card:hover {{
        box-shadow: 0 4px 16px rgba(0,0,0,0.08);
        transform: translateY(-2px);
    }}

    .metric-label {{
        font-size: 0.8rem;
        color: {COLORS["text_light"]};
        text-transform: uppercase;
        letter-spacing: 0.5px;
        font-weight: 500;
        margin-bottom: 0.5rem;
    }}

    .metric-value {{
        font-size: 1.8rem;
        font-weight: 600;
        color: {COLORS["text"]};
        line-height: 1.2;
    }}

    .metric-value.accent {{
        color: {COLORS["khaki"]};
    }}

    .section-card {{
        background: {COLORS["white"]};
        border-radius: 20px;
        padding: 1.5rem 2rem;
        margin-bottom: 1.5rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
        border: 1px solid {COLORS["gray"]};
    }}

    .section-title {{
        font-size: 1.1rem;
        font-weight: 600;
        color: {COLORS["text"]};
        margin-bottom: 1.5rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }}

    .section-title::before {{
        content: '';
        width: 4px;
        height: 20px;
        background: {COLORS["khaki"]};
        border-radius: 2px;
    }}

    .status-badge {{
        display: inline-flex;
        align-items: center;
        padding: 0.4rem 1rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 500;
    }}

    .status-online {{
        background: rgba(139, 154, 107, 0.15);
        color: {COLORS["khaki_dark"]};
    }}

    .status-offline {{
        background: rgba(220, 53, 69, 0.1);
        color: #dc3545;
    }}

    .data-table {{
        border-radius: 12px;
        overflow: hidden;
    }}

    .stDataFrame {{
        border-radius: 12px;
    }}

    div[data-testid="stMetricValue"] {{
        font-size: 1.6rem;
        font-weight: 600;
        color: {COLORS["text"]};
    }}

    div[data-testid="stMetricLabel"] {{
        font-size: 0.85rem;
        color: {COLORS["text_light"]};
        text-transform: uppercase;
        letter-spacing: 0.3px;
    }}

    .stTabs [data-baseweb="tab-list"] {{
        gap: 8px;
        background: {COLORS["gray_light"]};
        padding: 4px;
        border-radius: 12px;
    }}

    .stTabs [data-baseweb="tab"] {{
        border-radius: 8px;
        padding: 0.5rem 1.5rem;
        font-weight: 500;
    }}

    .stTabs [aria-selected="true"] {{
        background: {COLORS["white"]};
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }}

    .sidebar-card {{
        background: {COLORS["white"]};
        border-radius: 12px;
        padding: 1rem;
        margin-bottom: 1rem;
        border: 1px solid {COLORS["gray"]};
    }}

    footer {{
        text-align: center;
        padding: 2rem;
        color: {COLORS["text_light"]};
        font-size: 0.85rem;
    }}

    .stSelectbox > div > div {{
        border-radius: 10px;
        border-color: {COLORS["gray"]};
    }}

    .stButton > button {{
        background: {COLORS["khaki"]};
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.6rem 1.5rem;
        font-weight: 500;
        transition: all 0.2s ease;
    }}

    .stButton > button:hover {{
        background: {COLORS["khaki_dark"]};
        transform: translateY(-1px);
    }}
</style>
""", unsafe_allow_html=True)

CHART_TEMPLATE = {
    "layout": {
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        "font": {"family": "Inter, sans-serif", "color": COLORS["text"]},
        "margin": {"t": 40, "b": 40, "l": 40, "r": 40},
    }
}

@st.cache_data(ttl=300)
def fetch_status():
    try:
        response = requests.get(f"{API_URL}/status", timeout=5)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

@st.cache_data(ttl=60)
def fetch_kpi():
    try:
        response = requests.get(f"{API_URL}/kpi", timeout=5)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

@st.cache_data(ttl=60)
def fetch_statistics():
    try:
        response = requests.get(f"{API_URL}/statistics", timeout=5)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

@st.cache_data(ttl=300)
def fetch_clients(page=0, limit=100):
    try:
        response = requests.get(f"{API_URL}/clients", params={"page": page, "limit": limit}, timeout=5)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

@st.cache_data(ttl=300)
def fetch_purchases(page=0, limit=100, statut=None):
    try:
        params = {"page": page, "limit": limit}
        if statut:
            params["statut"] = statut
        response = requests.get(f"{API_URL}/purchases", params=params, timeout=5)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

@st.cache_data(ttl=600)
def fetch_sync_logs(days=7):
    try:
        response = requests.get(f"{API_URL}/sync-log", params={"days": days}, timeout=5)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

st.markdown("""
<div class="main-header">
    <h1>Analytics Platform</h1>
    <p>Real-time business intelligence dashboard</p>
</div>
""", unsafe_allow_html=True)

api_status = fetch_status()
col_status, col_refresh = st.columns([3, 1])

with col_status:
    if api_status:
        st.markdown(f'<span class="status-badge status-online">Connected</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="status-badge status-offline">Offline</span>', unsafe_allow_html=True)

with col_refresh:
    if st.button("Refresh"):
        st.cache_data.clear()
        st.rerun()

st.markdown("<br>", unsafe_allow_html=True)

kpi_data = fetch_kpi()
if kpi_data:
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Total Clients</div>
            <div class="metric-value">{kpi_data.get('total_clients', 0):,}</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Total Orders</div>
            <div class="metric-value">{kpi_data.get('total_achats', 0):,}</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Revenue</div>
            <div class="metric-value accent">{kpi_data.get('ca_total', 0):,.0f} EUR</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Avg Order</div>
            <div class="metric-value">{kpi_data.get('panier_moyen', 0):,.0f} EUR</div>
        </div>
        """, unsafe_allow_html=True)

    with col5:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Cancel Rate</div>
            <div class="metric-value">{kpi_data.get('taux_annulation', 0):.1f}%</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

stats = fetch_statistics()
if stats:
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Orders by Status</div>', unsafe_allow_html=True)

        status_data = pd.DataFrame(stats.get('by_status', []))
        if not status_data.empty:
            status_data.rename(columns={'_id': 'Status', 'count': 'Count'}, inplace=True)
            fig = px.pie(
                status_data,
                values='Count',
                names='Status',
                color_discrete_sequence=[COLORS["khaki"], COLORS["gray_dark"], COLORS["khaki_light"]],
                hole=0.5
            )
            fig.update_layout(
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
                margin=dict(t=20, b=60, l=20, r=20),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Revenue by Category</div>', unsafe_allow_html=True)

        category_data = pd.DataFrame(stats.get('by_category', []))
        if not category_data.empty:
            category_data.rename(columns={'_id': 'Category', 'total_amount': 'Revenue', 'count': 'Count'}, inplace=True)
            category_data = category_data.sort_values('Revenue', ascending=True).tail(6)
            fig = px.bar(
                category_data,
                x='Revenue',
                y='Category',
                orientation='h',
                color_discrete_sequence=[COLORS["khaki"]],
            )
            fig.update_layout(
                showlegend=False,
                margin=dict(t=20, b=40, l=20, r=20),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(showgrid=True, gridcolor=COLORS["gray"], title=""),
                yaxis=dict(showgrid=False, title=""),
            )
            fig.update_traces(marker_cornerradius=6)
            st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Clients</div>', unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["Data", "Geography"])

    with tab1:
        clients_data = fetch_clients(limit=50)
        if clients_data and clients_data.get('data'):
            df_clients = pd.DataFrame(clients_data['data'])
            cols_display = ['client_id', 'nom', 'email', 'pays', 'date_inscription']
            cols_available = [c for c in cols_display if c in df_clients.columns]
            st.dataframe(
                df_clients[cols_available],
                use_container_width=True,
                height=350,
                hide_index=True
            )

    with tab2:
        if stats:
            country_data = pd.DataFrame(stats.get('by_country', []))
            if not country_data.empty:
                country_data.rename(columns={'_id': 'Country', 'count': 'Clients'}, inplace=True)
                country_data = country_data.sort_values('Clients', ascending=False).head(8)
                fig = px.bar(
                    country_data,
                    x='Country',
                    y='Clients',
                    color_discrete_sequence=[COLORS["khaki"]],
                )
                fig.update_layout(
                    showlegend=False,
                    margin=dict(t=20, b=40, l=20, r=20),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    xaxis=dict(showgrid=False, title=""),
                    yaxis=dict(showgrid=True, gridcolor=COLORS["gray"], title=""),
                )
                fig.update_traces(marker_cornerradius=6)
                st.plotly_chart(fig, use_container_width=True)

    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Orders</div>', unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["Data", "Filter"])

    with tab1:
        purchases_data = fetch_purchases(limit=50)
        if purchases_data and purchases_data.get('data'):
            df_purchases = pd.DataFrame(purchases_data['data'])
            cols_display = ['achat_id', 'produit', 'montant_total', 'statut', 'date_achat']
            cols_available = [c for c in cols_display if c in df_purchases.columns]
            st.dataframe(
                df_purchases[cols_available],
                use_container_width=True,
                height=350,
                hide_index=True
            )

    with tab2:
        selected_status = st.selectbox("Status", ["All", "livré", "en cours", "annulé"], label_visibility="collapsed")
        status_filter = None if selected_status == "All" else selected_status

        if status_filter:
            filtered_data = fetch_purchases(limit=50, statut=status_filter)
            if filtered_data and filtered_data.get('data'):
                df_filtered = pd.DataFrame(filtered_data['data'])
                st.caption(f"{len(df_filtered)} orders with status: {status_filter}")
                cols_display = ['achat_id', 'produit', 'montant_total', 'date_achat']
                cols_available = [c for c in cols_display if c in df_filtered.columns]
                st.dataframe(df_filtered[cols_available], use_container_width=True, height=300, hide_index=True)

    st.markdown('</div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Sync Performance</div>', unsafe_allow_html=True)

sync_logs = fetch_sync_logs(days=7)
if sync_logs and sync_logs.get('data'):
    df_logs = pd.DataFrame(sync_logs['data'])

    if len(df_logs) >= 2:
        df_logs['timestamp'] = pd.to_datetime(df_logs['timestamp'])
        df_logs = df_logs.sort_values('timestamp', ascending=False)

        latest = df_logs.iloc[0]
        previous = df_logs.iloc[1]
        refresh_time = (pd.to_datetime(latest['timestamp']) - pd.to_datetime(previous['timestamp'])).total_seconds()

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Sync Duration", f"{latest['duration_seconds']:.2f}s")
        with col2:
            st.metric("Refresh Interval", f"{refresh_time:.0f}s")
        with col3:
            st.metric("Throughput", f"{latest['documents_per_second']:.0f} docs/s")
        with col4:
            st.metric("Status", latest['status'].upper())

    st.markdown("<br>", unsafe_allow_html=True)

    df_logs['timestamp'] = pd.to_datetime(df_logs['timestamp']).dt.strftime('%Y-%m-%d %H:%M')
    st.dataframe(
        df_logs[['timestamp', 'collection', 'status', 'row_count', 'duration_seconds']].head(10),
        use_container_width=True,
        hide_index=True
    )

st.markdown('</div>', unsafe_allow_html=True)

st.markdown(f"""
<footer>
    <span style="color: {COLORS['text_light']};">
        Analytics Platform  |  Updated: {datetime.now().strftime('%H:%M:%S')}
    </span>
</footer>
""", unsafe_allow_html=True)
