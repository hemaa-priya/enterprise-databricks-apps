"""
Enterprise Order Analytics Dashboard
Databricks Connect + Streamlit Application

A production-ready analytics dashboard showcasing TPCH sample data
with enterprise-grade features including caching, error handling,
and responsive design.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import sys

# Page configuration - must be first Streamlit command
st.set_page_config(
    page_title="Order Analytics",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'About': "Enterprise Order Analytics Dashboard powered by Databricks Connect"
    }
)

# Custom CSS for enterprise styling
st.markdown("""
<style>
    /* Import distinctive fonts */
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&family=JetBrains+Mono:wght@400;500&display=swap');

    /* Root variables for cohesive theming */
    :root {
        --primary-gradient: linear-gradient(135deg, #FF3621 0%, #FF6B35 50%, #F7931E 100%);
        --secondary-gradient: linear-gradient(135deg, #1B3A4B 0%, #065A82 100%);
        --surface-dark: #0D1117;
        --surface-card: #161B22;
        --surface-elevated: #21262D;
        --text-primary: #F0F6FC;
        --text-secondary: #8B949E;
        --accent-orange: #FF6B35;
        --accent-blue: #58A6FF;
        --accent-green: #3FB950;
        --accent-purple: #A371F7;
    }

    /* Main container styling */
    .main {
        font-family: 'DM Sans', sans-serif;
        background: linear-gradient(180deg, #0D1117 0%, #161B22 100%);
    }

    /* Metric cards with glassmorphism */
    .metric-card {
        background: linear-gradient(145deg, rgba(22, 27, 34, 0.9) 0%, rgba(33, 38, 45, 0.8) 100%);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 107, 53, 0.2);
        border-radius: 16px;
        padding: 24px;
        margin: 8px 0;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        transition: all 0.3s ease;
    }

    .metric-card:hover {
        border-color: rgba(255, 107, 53, 0.5);
        transform: translateY(-2px);
        box-shadow: 0 12px 40px rgba(255, 107, 53, 0.15);
    }

    .metric-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 2.5rem;
        font-weight: 700;
        background: var(--primary-gradient);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin: 0;
        line-height: 1.2;
    }

    .metric-label {
        color: #8B949E;
        font-size: 0.9rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 8px;
    }

    /* Headers styling */
    h1, h2, h3 {
        font-family: 'DM Sans', sans-serif !important;
        color: #F0F6FC !important;
    }

    h1 {
        background: var(--primary-gradient);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        font-weight: 700 !important;
    }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0D1117 0%, #161B22 100%);
        border-right: 1px solid rgba(255, 107, 53, 0.1);
    }

    [data-testid="stSidebar"] .stMarkdown {
        color: #F0F6FC;
    }

    /* DataFrame styling */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: transparent;
    }

    .stTabs [data-baseweb="tab"] {
        background: rgba(22, 27, 34, 0.8);
        border-radius: 8px;
        color: #8B949E;
        border: 1px solid transparent;
        padding: 10px 20px;
    }

    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, rgba(255, 54, 33, 0.2) 0%, rgba(255, 107, 53, 0.2) 100%);
        border: 1px solid rgba(255, 107, 53, 0.5);
        color: #FF6B35;
    }

    /* Status indicator */
    .status-indicator {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 8px 16px;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 500;
    }

    .status-connected {
        background: rgba(63, 185, 80, 0.15);
        color: #3FB950;
        border: 1px solid rgba(63, 185, 80, 0.3);
    }

    .status-error {
        background: rgba(248, 81, 73, 0.15);
        color: #F85149;
        border: 1px solid rgba(248, 81, 73, 0.3);
    }

    /* Divider */
    hr {
        border: none;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(255, 107, 53, 0.3), transparent);
        margin: 2rem 0;
    }

    /* Animation for loading */
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }

    .loading {
        animation: pulse 2s ease-in-out infinite;
    }
</style>
""", unsafe_allow_html=True)


def format_currency(value: float) -> str:
    """Format large numbers as currency with K/M/B suffixes."""
    if value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    elif value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:.2f}"


def format_number(value: float) -> str:
    """Format large numbers with K/M/B suffixes."""
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    elif value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    elif value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:,.0f}"


def ensure_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert object columns to numeric where possible."""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == 'object':
            converted = pd.to_numeric(df[col], errors='coerce')
            if not converted.isna().all():
                df[col] = converted
    return df


def create_metric_card(value: str, label: str) -> str:
    """Create HTML for a styled metric card."""
    return f"""
    <div class="metric-card">
        <p class="metric-value">{value}</p>
        <p class="metric-label">{label}</p>
    </div>
    """


def style_plotly_chart(fig, custom_legend: dict = None) -> None:
    """Apply consistent dark theme styling to a Plotly figure.

    Call this AFTER fig.update_layout() to avoid conflicts.
    """
    # Base styling
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font={'family': 'DM Sans, sans-serif', 'color': '#F0F6FC'},
    )

    # Only style title if one exists
    if fig.layout.title and fig.layout.title.text:
        fig.update_layout(title_font={'size': 18, 'color': '#F0F6FC'})

    # Axis styling (using update_xaxes/update_yaxes avoids conflicts)
    axis_style = {
        'gridcolor': 'rgba(139, 148, 158, 0.1)',
        'zerolinecolor': 'rgba(139, 148, 158, 0.2)',
        'tickfont': {'color': '#8B949E'},
        'title_font': {'color': '#8B949E'}
    }
    fig.update_xaxes(**axis_style)
    fig.update_yaxes(**axis_style)

    # Legend styling
    legend_base = {'bgcolor': 'rgba(0,0,0,0)', 'font': {'color': '#8B949E'}}
    if custom_legend:
        fig.update_layout(legend={**legend_base, **custom_legend})
    else:
        fig.update_layout(legend=legend_base)


# Color palette for charts
CHART_COLORS = ['#FF6B35', '#58A6FF', '#3FB950', '#A371F7', '#F7931E', '#79C0FF', '#7EE787', '#D2A8FF']


def render_overview_tab(data_layer):
    """Render the overview dashboard tab."""
    st.markdown("### Key Performance Indicators")

    with st.spinner("Loading KPIs..."):
        try:
            kpis = data_layer.get_kpi_metrics()

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.markdown(create_metric_card(
                    format_number(kpis['total_orders']),
                    "Total Orders"
                ), unsafe_allow_html=True)

            with col2:
                st.markdown(create_metric_card(
                    format_currency(kpis['total_revenue']),
                    "Total Revenue"
                ), unsafe_allow_html=True)

            with col3:
                st.markdown(create_metric_card(
                    format_number(kpis['total_customers']),
                    "Unique Customers"
                ), unsafe_allow_html=True)

            with col4:
                st.markdown(create_metric_card(
                    format_currency(kpis['avg_order_value']),
                    "Avg Order Value"
                ), unsafe_allow_html=True)

        except Exception as e:
            st.error(f"Failed to load KPIs: {str(e)}")
            return

    st.markdown("---")

    # Revenue Trend Chart
    st.markdown("### Revenue Trend Over Time")

    with st.spinner("Loading trend data..."):
        try:
            orders_summary = data_layer.get_orders_summary()

            fig = go.Figure()

            fig.add_trace(go.Scatter(
                x=orders_summary['order_month'],
                y=orders_summary['total_revenue'],
                mode='lines+markers',
                name='Revenue',
                line=dict(color='#FF6B35', width=3),
                marker=dict(size=8),
                fill='tozeroy',
                fillcolor='rgba(255, 107, 53, 0.1)'
            ))

            fig.update_layout(
                height=400,
                margin=dict(l=20, r=20, t=40, b=20),
                hovermode='x unified',
                xaxis_title="Month",
                yaxis_title="Revenue ($)"
            )
            style_plotly_chart(fig)

            st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"Failed to load trend data: {str(e)}")

    # Two column layout for secondary charts
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Orders by Status")
        try:
            status_data = data_layer.get_orders_by_status()

            fig = px.pie(
                status_data,
                values='order_count',
                names='status',
                color_discrete_sequence=CHART_COLORS,
                hole=0.5
            )

            fig.update_layout(
                height=350,
                margin=dict(l=20, r=20, t=20, b=20),
                showlegend=True
            )
            style_plotly_chart(fig, custom_legend=dict(orientation="h", yanchor="bottom", y=-0.2))

            fig.update_traces(textposition='inside', textinfo='percent+label')

            st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"Failed to load status data: {str(e)}")

    with col2:
        st.markdown("### Orders by Priority")
        try:
            priority_data = data_layer.get_orders_by_priority()

            fig = px.bar(
                priority_data,
                x='priority',
                y='order_count',
                color='priority',
                color_discrete_sequence=CHART_COLORS
            )

            fig.update_layout(
                height=350,
                margin=dict(l=20, r=20, t=20, b=20),
                showlegend=False,
                xaxis_title="Priority Level",
                yaxis_title="Order Count"
            )
            style_plotly_chart(fig)

            st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"Failed to load priority data: {str(e)}")


def render_customers_tab(data_layer):
    """Render the customer analytics tab."""

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Top Customers by Revenue")

        top_n = st.slider("Number of customers to display", 5, 25, 10, key="top_customers")

        try:
            top_customers = data_layer.get_top_customers(limit=top_n)

            fig = px.bar(
                top_customers,
                x='total_spent',
                y='customer_name',
                orientation='h',
                color='market_segment',
                color_discrete_sequence=CHART_COLORS,
                hover_data=['order_count', 'avg_order_value', 'nation']
            )

            fig.update_layout(
                height=500,
                margin=dict(l=20, r=20, t=20, b=20),
                yaxis={'categoryorder': 'total ascending'},
                xaxis_title="Total Revenue ($)",
                yaxis_title=""
            )
            style_plotly_chart(fig)

            st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"Failed to load customer data: {str(e)}")

    with col2:
        st.markdown("### Market Segment Analysis")

        try:
            segment_data = data_layer.get_market_segment_analysis()

            fig = make_subplots(
                rows=1, cols=2,
                specs=[[{"type": "pie"}, {"type": "bar"}]],
                subplot_titles=("Revenue by Segment", "Avg Order Value")
            )

            fig.add_trace(
                go.Pie(
                    labels=segment_data['segment'],
                    values=segment_data['total_revenue'],
                    marker_colors=CHART_COLORS,
                    hole=0.4
                ),
                row=1, col=1
            )

            fig.add_trace(
                go.Bar(
                    x=segment_data['segment'],
                    y=segment_data['avg_order_value'],
                    marker_color=CHART_COLORS[:len(segment_data)]
                ),
                row=1, col=2
            )

            fig.update_layout(
                height=500,
                margin=dict(l=20, r=20, t=40, b=20),
                showlegend=False
            )
            style_plotly_chart(fig)

            st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"Failed to load segment data: {str(e)}")

    # Segment trend over time
    st.markdown("### Revenue Trend by Market Segment")

    try:
        segment_trend = data_layer.get_monthly_trend_by_segment()

        fig = px.area(
            segment_trend,
            x='order_month',
            y='revenue',
            color='segment',
            color_discrete_sequence=CHART_COLORS
        )

        fig.update_layout(
            height=400,
            margin=dict(l=20, r=20, t=20, b=20),
            xaxis_title="Month",
            yaxis_title="Revenue ($)"
        )
        style_plotly_chart(fig, custom_legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Failed to load segment trend: {str(e)}")


def render_geography_tab(data_layer):
    """Render the geographic analytics tab."""

    st.markdown("### Revenue by Region & Nation")

    try:
        geo_data = ensure_numeric_columns(data_layer.get_revenue_by_region())

        # Sunburst chart for hierarchical view
        fig = px.sunburst(
            geo_data,
            path=['region', 'nation'],
            values='total_revenue',
            color='total_revenue',
            color_continuous_scale=['#1B3A4B', '#FF6B35', '#F7931E']
        )

        fig.update_layout(
            height=500,
            margin=dict(l=20, r=20, t=20, b=20)
        )
        style_plotly_chart(fig)

        st.plotly_chart(fig, use_container_width=True)

        # Regional breakdown table
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### Regional Summary")
            regional_summary = geo_data.groupby('region').agg({
                'order_count': 'sum',
                'total_revenue': 'sum'
            }).reset_index()
            regional_summary['avg_order_value'] = regional_summary['total_revenue'] / regional_summary['order_count']
            regional_summary = regional_summary.sort_values('total_revenue', ascending=False)

            st.dataframe(
                regional_summary.style.format({
                    'total_revenue': '${:,.2f}',
                    'order_count': '{:,.0f}',
                    'avg_order_value': '${:,.2f}'
                }),
                use_container_width=True,
                hide_index=True
            )

        with col2:
            st.markdown("#### Top 10 Nations by Revenue")
            top_nations = geo_data.nlargest(10, 'total_revenue')

            fig = px.bar(
                top_nations,
                x='total_revenue',
                y='nation',
                orientation='h',
                color='region',
                color_discrete_sequence=CHART_COLORS
            )

            fig.update_layout(
                height=400,
                margin=dict(l=20, r=20, t=20, b=20),
                yaxis={'categoryorder': 'total ascending'},
                xaxis_title="Revenue ($)",
                yaxis_title=""
            )
            style_plotly_chart(fig)

            st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Failed to load geographic data: {str(e)}")


def render_products_tab(data_layer):
    """Render the products analytics tab."""

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Top Products by Revenue")

        top_n = st.slider("Number of products", 5, 20, 10, key="top_products")

        try:
            products = data_layer.get_top_parts_by_revenue(limit=top_n)

            fig = px.bar(
                products,
                x='revenue',
                y='part_name',
                orientation='h',
                color='brand',
                color_discrete_sequence=CHART_COLORS,
                hover_data=['part_type', 'quantity_sold']
            )

            fig.update_layout(
                height=500,
                margin=dict(l=20, r=20, t=20, b=20),
                yaxis={'categoryorder': 'total ascending'},
                xaxis_title="Revenue ($)",
                yaxis_title=""
            )
            style_plotly_chart(fig)

            st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"Failed to load products data: {str(e)}")

    with col2:
        st.markdown("### Supplier Performance")

        top_suppliers = st.slider("Number of suppliers", 5, 20, 10, key="top_suppliers")

        try:
            suppliers = ensure_numeric_columns(data_layer.get_supplier_performance(limit=top_suppliers))

            fig = px.scatter(
                suppliers,
                x='orders_supplied',
                y='total_supply_value',
                size='avg_line_value',
                color='nation',
                hover_name='supplier_name',
                color_discrete_sequence=CHART_COLORS,
                size_max=60
            )

            fig.update_layout(
                height=500,
                margin=dict(l=20, r=20, t=20, b=20),
                xaxis_title="Orders Supplied",
                yaxis_title="Total Supply Value ($)"
            )
            style_plotly_chart(fig)

            st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"Failed to load supplier data: {str(e)}")

    # Fulfillment metrics
    st.markdown("### Shipping & Fulfillment Metrics")

    try:
        fulfillment = data_layer.get_order_fulfillment_metrics()

        col1, col2 = st.columns(2)

        with col1:
            fig = px.bar(
                fulfillment,
                x='ship_mode',
                y='shipment_count',
                color='ship_mode',
                color_discrete_sequence=CHART_COLORS
            )

            fig.update_layout(
                height=350,
                margin=dict(l=20, r=20, t=40, b=20),
                showlegend=False,
                title="Shipments by Mode",
                xaxis_title="Shipping Mode",
                yaxis_title="Shipment Count"
            )
            style_plotly_chart(fig)

            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # On-time vs late delivery
            fulfillment['on_time_rate'] = fulfillment['on_time_count'] / (fulfillment['on_time_count'] + fulfillment['late_count']) * 100

            fig = go.Figure()

            fig.add_trace(go.Bar(
                name='On Time',
                x=fulfillment['ship_mode'],
                y=fulfillment['on_time_count'],
                marker_color='#3FB950'
            ))

            fig.add_trace(go.Bar(
                name='Late',
                x=fulfillment['ship_mode'],
                y=fulfillment['late_count'],
                marker_color='#F85149'
            ))

            fig.update_layout(
                height=350,
                margin=dict(l=20, r=20, t=40, b=20),
                barmode='stack',
                title="On-Time vs Late Deliveries",
                xaxis_title="Shipping Mode",
                yaxis_title="Shipment Count"
            )
            style_plotly_chart(fig, custom_legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))

            st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Failed to load fulfillment data: {str(e)}")


def render_data_explorer_tab(data_layer):
    """Render the data explorer tab for custom queries."""

    st.markdown("### Custom Query Explorer")

    st.warning("**Note:** Custom queries are not cached and may impact performance. Use responsibly.")

    # Provide sample queries
    sample_queries = {
        "Select a sample query...": "",
        "Top 5 customers with most orders": """
SELECT c.c_name, COUNT(*) as order_count
FROM samples.tpch.orders o
JOIN samples.tpch.customer c ON o.o_custkey = c.c_custkey
GROUP BY c.c_name
ORDER BY order_count DESC
LIMIT 5""",
        "Monthly revenue growth": """
SELECT
    DATE_TRUNC('month', o_orderdate) as month,
    SUM(o_totalprice) as revenue
FROM samples.tpch.orders
GROUP BY DATE_TRUNC('month', o_orderdate)
ORDER BY month""",
        "Part sales by type": """
SELECT
    p.p_type,
    SUM(l.l_extendedprice) as total_sales
FROM samples.tpch.lineitem l
JOIN samples.tpch.part p ON l.l_partkey = p.p_partkey
GROUP BY p.p_type
ORDER BY total_sales DESC
LIMIT 10"""
    }

    selected_sample = st.selectbox("Sample Queries", options=list(sample_queries.keys()))

    default_query = sample_queries.get(selected_sample, "")

    query = st.text_area(
        "Enter your SQL query:",
        value=default_query,
        height=200,
        placeholder="SELECT * FROM samples.tpch.orders LIMIT 10"
    )

    col1, col2 = st.columns([1, 5])

    with col1:
        execute_btn = st.button("▶Execute", type="primary", use_container_width=True)

    if execute_btn and query.strip():
        with st.spinner("Executing query..."):
            try:
                start_time = datetime.now()
                result = data_layer.execute_custom_query(query)
                execution_time = (datetime.now() - start_time).total_seconds()

                st.success(f"Query executed successfully in {execution_time:.2f}s - {len(result)} rows returned")

                st.dataframe(result, use_container_width=True, hide_index=True)

                # Download option
                csv = result.to_csv(index=False)
                st.download_button(
                    label="Download as CSV",
                    data=csv,
                    file_name="query_results.csv",
                    mime="text/csv"
                )

            except Exception as e:
                st.error(f"Query failed: {str(e)}")


def main():
    """Main application entry point."""

    # Header with branding
    st.markdown("""
    <div style="display: flex; align-items: center; gap: 16px; margin-bottom: 1rem;">
        <h1 style="margin: 0;"> Order Analytics</h1>
    </div>
    <p style="color: #8B949E; margin-bottom: 2rem;">
        Enterprise analytics powered by <strong style="color: #FF6B35;">Databricks Connect</strong> • TPCH Sample Dataset
    </p>
    """, unsafe_allow_html=True)

    # Initialize data layer
    try:
        from data_layer import get_data_layer
        data_layer = get_data_layer()

        # Connection status in sidebar
        with st.sidebar:
            st.markdown("## ⚙️ Settings")

            # Health check
            if data_layer.health_check():
                st.markdown("""
                <div class="status-indicator status-connected">
                    <span>●</span> Connected to Databricks
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown("""
                <div class="status-indicator status-error">
                    <span>●</span> Connection Error
                </div>
                """, unsafe_allow_html=True)

            st.markdown("---")

            st.markdown("###  Data Source")
            st.markdown("""
            - **Catalog:** `samples`
            - **Schema:** `tpch`
            - **Tables:** orders, customer, lineitem, part, supplier, nation, region
            """)

            st.markdown("---")

            st.markdown("### About")
            st.markdown("""
            This dashboard demonstrates enterprise-grade
            analytics using **Databricks Connect** with
            **Streamlit**.

            Features:
            - 🔄 Auto-caching (1hr TTL)
            - 📊 Interactive visualizations
            - 🔍 Custom query explorer
            - 📱 Responsive design
            """)

            # Clear cache button
            if st.button("Refresh Cache", use_container_width=True):
                st.cache_data.clear()
                st.rerun()

    except Exception as e:
        st.error(f"""
        ### ⚠️ Connection Error

        Failed to connect to Databricks: {str(e)}

        **For local development, ensure you have:**
        1. Configured `~/.databrickscfg` with your credentials
        2. Or set environment variables:
           - `DATABRICKS_HOST`
           - `DATABRICKS_TOKEN` (or `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET`)
           - `DATABRICKS_CLUSTER_ID`

        **For Databricks Apps deployment:**
        Authentication is handled automatically.
        """)
        return

    # Main content tabs
    tabs = st.tabs([
        "Overview",
        "Customers",
        "Geography",
        "Products & Suppliers",
        "Data Explorer"
    ])

    with tabs[0]:
        render_overview_tab(data_layer)

    with tabs[1]:
        render_customers_tab(data_layer)

    with tabs[2]:
        render_geography_tab(data_layer)

    with tabs[3]:
        render_products_tab(data_layer)

    with tabs[4]:
        render_data_explorer_tab(data_layer)

    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #8B949E; font-size: 0.85rem; padding: 1rem 0;">
        Built by Hema with ❤️ using <strong>Databricks & Streamlit</strong> |
        Data: TPCH Sample Dataset
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
