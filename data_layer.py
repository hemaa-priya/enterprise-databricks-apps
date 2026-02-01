"""
Enterprise Data Access Layer for Databricks
Supports both Serverless SQL Warehouse and Classic Clusters
"""

import os
from typing import Optional
import pandas as pd
import streamlit as st


class DatabricksDataLayer:
    """
    Enterprise-grade data access layer for Databricks.
    Supports:
    - Serverless SQL Warehouse (recommended) - via databricks-sql-connector
    - Classic All-Purpose Cluster - via databricks-connect

    Configuration (Environment Variables):
    - DATABRICKS_HOST: Workspace URL (e.g., https://xxx.cloud.databricks.com)
    - DATABRICKS_TOKEN: Personal access token

    For SQL Warehouse (Recommended):
    - DATABRICKS_WAREHOUSE_ID: SQL Warehouse ID (found in SQL Warehouses page)

    For Classic Cluster:
    - DATABRICKS_CLUSTER_ID: All-Purpose cluster ID
    """

    TPCH_CATALOG = "samples"
    TPCH_SCHEMA = "tpch"

    def __init__(self):
        self._connection = None
        self._spark = None
        self._use_sql_connector = None

    def _detect_connection_type(self) -> bool:
        """
        Detect which connection type to use.
        Returns True for SQL Connector (warehouse), False for Databricks Connect (cluster).
        """
        if self._use_sql_connector is not None:
            return self._use_sql_connector

        # Check for Databricks Apps environment
        if os.getenv("DATABRICKS_RUNTIME_VERSION"):
            self._use_sql_connector = False
            return False

        # Prefer SQL Warehouse if warehouse_id is set
        if os.getenv("DATABRICKS_WAREHOUSE_ID") or os.getenv("DATABRICKS_HTTP_PATH"):
            self._use_sql_connector = True
            return True

        # Fall back to cluster if cluster_id is set
        if os.getenv("DATABRICKS_CLUSTER_ID"):
            self._use_sql_connector = False
            return False

        # Default to SQL connector
        self._use_sql_connector = True
        return True

    def _get_sql_connection(self):
        """Create connection using Databricks SQL Connector (for SQL Warehouse)."""
        from databricks import sql

        host = os.getenv("DATABRICKS_HOST", "").replace("https://", "").replace("http://", "")
        token = os.getenv("DATABRICKS_TOKEN")

        # Support both warehouse_id and http_path
        warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
        http_path = os.getenv("DATABRICKS_HTTP_PATH")

        if not http_path and warehouse_id:
            http_path = f"/sql/1.0/warehouses/{warehouse_id}"

        if not host or not token or not http_path:
            raise ValueError(
                "Missing configuration. Set environment variables:\n"
                "  - DATABRICKS_HOST (e.g., xxx.cloud.databricks.com)\n"
                "  - DATABRICKS_TOKEN (your personal access token)\n"
                "  - DATABRICKS_WAREHOUSE_ID (SQL Warehouse ID)\n"
                "    OR DATABRICKS_HTTP_PATH (e.g., /sql/1.0/warehouses/xxx)"
            )

        return sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token
        )

    def _get_spark_session(self):
        """Create Spark session using Databricks Connect (for Classic Cluster)."""
        from databricks.connect import DatabricksSession

        # For Databricks Apps - uses built-in OAuth
        if os.getenv("DATABRICKS_RUNTIME_VERSION"):
            return DatabricksSession.builder.getOrCreate()

        # For local development with classic cluster
        return DatabricksSession.builder.getOrCreate()

    @property
    def connection(self):
        """Get SQL connection (lazy initialization)."""
        if self._connection is None and self._detect_connection_type():
            self._connection = self._get_sql_connection()
        return self._connection

    @property
    def spark(self):
        """Get Spark session (lazy initialization)."""
        if self._spark is None and not self._detect_connection_type():
            self._spark = self._get_spark_session()
        return self._spark

    def _execute_query(self, query: str) -> pd.DataFrame:
        """Execute query using appropriate connection type."""
        if self._detect_connection_type():
            # Use SQL Connector
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=columns)
                # Convert numeric columns (SQL connector returns objects)
                df = self._convert_numeric_columns(df)
                return df
        else:
            # Use Databricks Connect
            return self.spark.sql(query).toPandas()

    def _convert_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert object columns to numeric where possible."""
        # Create a fresh copy to avoid any backend issues
        df = pd.DataFrame(df.to_dict('list'))

        for col in df.columns:
            if df[col].dtype == 'object':
                # Try to convert to numeric
                try:
                    converted = pd.to_numeric(df[col], errors='coerce')
                    # Only use if conversion was successful (not all NaN)
                    if not converted.isna().all():
                        df[col] = converted
                except Exception:
                    pass
        return df

    def _get_table_path(self, table_name: str) -> str:
        """Get fully qualified table path."""
        return f"{self.TPCH_CATALOG}.{self.TPCH_SCHEMA}.{table_name}"

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_orders_summary(_self) -> pd.DataFrame:
        """Get aggregated orders summary by date. Cached for 1 hour."""
        query = f"""
        SELECT
            DATE_TRUNC('month', o_orderdate) as order_month,
            COUNT(*) as total_orders,
            SUM(o_totalprice) as total_revenue,
            AVG(o_totalprice) as avg_order_value,
            COUNT(DISTINCT o_custkey) as unique_customers
        FROM {_self._get_table_path('orders')}
        GROUP BY DATE_TRUNC('month', o_orderdate)
        ORDER BY order_month
        """
        return _self._execute_query(query)

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_kpi_metrics(_self) -> dict:
        """Get key performance indicators."""
        query = f"""
        SELECT
            COUNT(*) as total_orders,
            SUM(o_totalprice) as total_revenue,
            COUNT(DISTINCT o_custkey) as total_customers,
            AVG(o_totalprice) as avg_order_value
        FROM {_self._get_table_path('orders')}
        """
        result = _self._execute_query(query)
        return result.iloc[0].to_dict()

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_orders_by_status(_self) -> pd.DataFrame:
        """Get order distribution by status."""
        query = f"""
        SELECT
            o_orderstatus as status,
            COUNT(*) as order_count,
            SUM(o_totalprice) as total_value
        FROM {_self._get_table_path('orders')}
        GROUP BY o_orderstatus
        ORDER BY order_count DESC
        """
        return _self._execute_query(query)

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_orders_by_priority(_self) -> pd.DataFrame:
        """Get order distribution by priority."""
        query = f"""
        SELECT
            o_orderpriority as priority,
            COUNT(*) as order_count,
            SUM(o_totalprice) as total_value,
            AVG(o_totalprice) as avg_value
        FROM {_self._get_table_path('orders')}
        GROUP BY o_orderpriority
        ORDER BY order_count DESC
        """
        return _self._execute_query(query)

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_top_customers(_self, limit: int = 10) -> pd.DataFrame:
        """Get top customers by order value."""
        query = f"""
        SELECT
            c.c_name as customer_name,
            c.c_mktsegment as market_segment,
            n.n_name as nation,
            COUNT(o.o_orderkey) as order_count,
            SUM(o.o_totalprice) as total_spent,
            AVG(o.o_totalprice) as avg_order_value
        FROM {_self._get_table_path('orders')} o
        JOIN {_self._get_table_path('customer')} c ON o.o_custkey = c.c_custkey
        JOIN {_self._get_table_path('nation')} n ON c.c_nationkey = n.n_nationkey
        GROUP BY c.c_name, c.c_mktsegment, n.n_name
        ORDER BY total_spent DESC
        LIMIT {limit}
        """
        return _self._execute_query(query)

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_revenue_by_region(_self) -> pd.DataFrame:
        """Get revenue breakdown by region."""
        query = f"""
        SELECT
            r.r_name as region,
            n.n_name as nation,
            COUNT(o.o_orderkey) as order_count,
            SUM(o.o_totalprice) as total_revenue
        FROM {_self._get_table_path('orders')} o
        JOIN {_self._get_table_path('customer')} c ON o.o_custkey = c.c_custkey
        JOIN {_self._get_table_path('nation')} n ON c.c_nationkey = n.n_nationkey
        JOIN {_self._get_table_path('region')} r ON n.n_regionkey = r.r_regionkey
        GROUP BY r.r_name, n.n_name
        ORDER BY total_revenue DESC
        """
        return _self._execute_query(query)

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_market_segment_analysis(_self) -> pd.DataFrame:
        """Get market segment performance analysis."""
        query = f"""
        SELECT
            c.c_mktsegment as segment,
            COUNT(DISTINCT c.c_custkey) as customer_count,
            COUNT(o.o_orderkey) as order_count,
            SUM(o.o_totalprice) as total_revenue,
            AVG(o.o_totalprice) as avg_order_value
        FROM {_self._get_table_path('customer')} c
        LEFT JOIN {_self._get_table_path('orders')} o ON c.c_custkey = o.o_custkey
        GROUP BY c.c_mktsegment
        ORDER BY total_revenue DESC
        """
        return _self._execute_query(query)

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_top_parts_by_revenue(_self, limit: int = 10) -> pd.DataFrame:
        """Get top selling parts by revenue."""
        query = f"""
        SELECT
            p.p_name as part_name,
            p.p_type as part_type,
            p.p_brand as brand,
            SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue,
            SUM(l.l_quantity) as quantity_sold
        FROM {_self._get_table_path('lineitem')} l
        JOIN {_self._get_table_path('part')} p ON l.l_partkey = p.p_partkey
        GROUP BY p.p_name, p.p_type, p.p_brand
        ORDER BY revenue DESC
        LIMIT {limit}
        """
        return _self._execute_query(query)

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_supplier_performance(_self, limit: int = 10) -> pd.DataFrame:
        """Get top suppliers by order volume."""
        query = f"""
        SELECT
            s.s_name as supplier_name,
            n.n_name as nation,
            COUNT(DISTINCT l.l_orderkey) as orders_supplied,
            SUM(l.l_extendedprice) as total_supply_value,
            AVG(l.l_extendedprice) as avg_line_value
        FROM {_self._get_table_path('lineitem')} l
        JOIN {_self._get_table_path('supplier')} s ON l.l_suppkey = s.s_suppkey
        JOIN {_self._get_table_path('nation')} n ON s.s_nationkey = n.n_nationkey
        GROUP BY s.s_name, n.n_name
        ORDER BY total_supply_value DESC
        LIMIT {limit}
        """
        return _self._execute_query(query)

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_monthly_trend_by_segment(_self) -> pd.DataFrame:
        """Get monthly revenue trend by market segment."""
        query = f"""
        SELECT
            DATE_TRUNC('month', o.o_orderdate) as order_month,
            c.c_mktsegment as segment,
            SUM(o.o_totalprice) as revenue
        FROM {_self._get_table_path('orders')} o
        JOIN {_self._get_table_path('customer')} c ON o.o_custkey = c.c_custkey
        GROUP BY DATE_TRUNC('month', o.o_orderdate), c.c_mktsegment
        ORDER BY order_month, segment
        """
        return _self._execute_query(query)

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_order_fulfillment_metrics(_self) -> pd.DataFrame:
        """Get order fulfillment and shipping metrics."""
        query = f"""
        SELECT
            l.l_shipmode as ship_mode,
            COUNT(*) as shipment_count,
            AVG(DATEDIFF(l.l_shipdate, l.l_commitdate)) as avg_days_to_ship,
            SUM(CASE WHEN l.l_shipdate <= l.l_commitdate THEN 1 ELSE 0 END) as on_time_count,
            SUM(CASE WHEN l.l_shipdate > l.l_commitdate THEN 1 ELSE 0 END) as late_count
        FROM {_self._get_table_path('lineitem')} l
        GROUP BY l.l_shipmode
        ORDER BY shipment_count DESC
        """
        return _self._execute_query(query)

    def execute_custom_query(self, query: str) -> pd.DataFrame:
        """Execute a custom SQL query. WARNING: No caching applied."""
        return self._execute_query(query)

    def health_check(self) -> bool:
        """Verify Databricks connection is healthy."""
        try:
            self._execute_query("SELECT 1 as health")
            return True
        except Exception:
            return False

    def get_connection_info(self) -> str:
        """Return current connection type for display."""
        if self._detect_connection_type():
            return "SQL Warehouse (Serverless)"
        return "Classic Cluster (Databricks Connect)"


# Singleton instance for the application
@st.cache_resource
def get_data_layer() -> DatabricksDataLayer:
    """Get singleton instance of data layer."""
    return DatabricksDataLayer()
