# Enterprise Grade Analytics App

Enterprise-grade analytics application built with **Databricks Connect** and **Streamlit**, featuring the TPCH sample dataset.

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

## Features

- **Real-time Analytics**: Connect directly to Databricks for live data analysis
- **Interactive Dashboards**: 5 comprehensive dashboard tabs covering all business dimensions
- **Enterprise Caching**: 1-hour TTL caching for optimal performance
- **Custom Query Explorer**: Execute ad-hoc SQL queries with download capability
- **Modern UI**: Dark theme with glassmorphism effects and responsive design

### Dashboard Tabs

| Tab | Description |
|-----|-------------|
| **Overview** | KPIs, revenue trends, order status distribution |
| **Customers** | Top customers, market segmentation, customer trends |
| **Geography** | Regional revenue breakdown, nation-level analysis |
| **Products** | Top products, supplier performance, shipping metrics |
| **Explorer** | Custom SQL query interface |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Streamlit Frontend (app.py)                  │
│  ┌───────────┬───────────┬───────────┬───────────┬───────────┐  │
│  │  Overview │ Customers │ Geography │ Products  │ Explorer  │  │
│  └───────────┴───────────┴───────────┴───────────┴───────────┘  │
├─────────────────────────────────────────────────────────────────┤
│                  Data Layer (data_layer.py)                     │
│  • Connection Management    • Query Optimization                │
│  • Caching (st.cache_data)  • Error Handling                    │
├─────────────────────────────────────────────────────────────────┤
│                    Databricks Connect                           │
│  • Serverless SQL          • Unity Catalog                      │
│  • Spark DataFrame API     • OAuth Authentication               │
├─────────────────────────────────────────────────────────────────┤
│                   TPCH Sample Dataset                           │
│  samples.tpch.{orders, customer, lineitem, part, supplier, ...} │
└─────────────────────────────────────────────────────────────────┘
```

## Deployment Options

### Option 1: Databricks Apps (Recommended for Production)

Databricks Apps provides managed hosting with automatic OAuth authentication.

#### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Databricks CLI configured (`databricks configure`)
- Access to `samples.tpch` dataset

#### Deployment Steps

```bash
# 1. Navigate to project directory
cd hp-solutions

# 2. Deploy using Databricks CLI
databricks apps deploy order-analytics --source-code-path .

# 3. (Optional) Create the app first if it doesn't exist
databricks apps create order-analytics

# 4. Check deployment status
databricks apps get order-analytics
```

#### Using the Databricks UI

1. Navigate to **Compute** → **Apps** in your Databricks workspace
2. Click **Create App**
3. Upload the project files or connect to your Git repository
4. Set the app name (e.g., `order-analytics`)
5. Click **Deploy**

### Option 2: Local Development

Run locally for development and testing.

#### Prerequisites
- Python 3.10+
- Databricks cluster or SQL warehouse
- Configured authentication

#### Setup

```bash
# 1. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure Databricks authentication (choose one method)

# Method A: Profile-based (recommended)
# Create/edit ~/.databrickscfg:
cat > ~/.databrickscfg << EOF
[DEFAULT]
host = https://your-workspace.cloud.databricks.com
token = dapi_your_token_here
cluster_id = your-cluster-id
EOF

# Method B: Environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi_your_token_here"
export DATABRICKS_CLUSTER_ID="your-cluster-id"

# Method C: OAuth (for service principals)
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"
export DATABRICKS_CLUSTER_ID="your-cluster-id"

# 4. Run the application
streamlit run app.py
```

The app will be available at `http://localhost:8501`

### Option 3: Docker Deployment

For containerized deployments.

```dockerfile
# Dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

```bash
# Build and run
docker build -t order-analytics .
docker run -p 8501:8501 \
  -e DATABRICKS_HOST="https://your-workspace.cloud.databricks.com" \
  -e DATABRICKS_TOKEN="dapi_your_token" \
  -e DATABRICKS_CLUSTER_ID="your-cluster-id" \
  order-analytics
```

## 📁 Project Structure

```
hp-solutions/
├── app.py              # Main Streamlit application
├── data_layer.py       # Data access layer with Databricks Connect
├── app.yaml            # Databricks Apps deployment configuration
├── requirements.txt    # Python dependencies
└── README.md           # This file
```

## Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABRICKS_HOST` | Workspace URL | Yes (local) |
| `DATABRICKS_TOKEN` | Personal access token | Yes (if not using OAuth) |
| `DATABRICKS_CLUSTER_ID` | Compute cluster ID | Yes (local) |
| `DATABRICKS_CLIENT_ID` | Service principal ID | For OAuth |
| `DATABRICKS_CLIENT_SECRET` | Service principal secret | For OAuth |

### Caching Configuration

The data layer uses Streamlit's caching with a 1-hour TTL:

```python
@st.cache_data(ttl=3600, show_spinner=False)
def get_orders_summary(_self) -> pd.DataFrame:
    ...
```

To modify cache duration, update the `ttl` parameter in `data_layer.py`.

## Data Model (TPCH)

The application uses the standard TPCH benchmark dataset:

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `orders` | Customer orders | o_orderkey, o_custkey, o_totalprice, o_orderdate |
| `customer` | Customer information | c_custkey, c_name, c_mktsegment, c_nationkey |
| `lineitem` | Order line items | l_orderkey, l_partkey, l_suppkey, l_quantity |
| `part` | Product catalog | p_partkey, p_name, p_type, p_brand |
| `supplier` | Supplier details | s_suppkey, s_name, s_nationkey |
| `nation` | Nations | n_nationkey, n_name, n_regionkey |
| `region` | Regions | r_regionkey, r_name |

## Security

### Databricks Apps
- Uses built-in OAuth authentication
- Inherits workspace-level permissions
- No credentials stored in code

### Local Development
- Credentials managed via `~/.databrickscfg` or environment variables
- Never commit credentials to version control
- Use service principals for CI/CD

## Testing

```bash
# Run basic health check
python -c "from data_layer import DatabricksDataLayer; d = DatabricksDataLayer(); print('Connected!' if d.health_check() else 'Failed')"

# Run Streamlit in debug mode
streamlit run app.py --logger.level=debug
```

## Troubleshooting

### Connection Issues

**Error**: `Failed to connect to Databricks`
- Verify `DATABRICKS_HOST` is correct (include `https://`)
- Check token validity and permissions
- Ensure cluster is running (for classic compute)

**Error**: `Table not found`
- Verify Unity Catalog access to `samples.tpch`
- Check that the catalog and schema exist

### Performance Issues

**Slow queries**
- Ensure caching is working (check for "Loading..." on first load only)
- Consider using SQL warehouse instead of interactive cluster
- Review query complexity in Data Explorer

### Databricks Apps Deployment Issues

**App not starting**
- Check `app.yaml` syntax
- Verify all files are included in deployment
- Check logs: `databricks apps logs order-analytics`

## License

This project is provided as a reference implementation. Use and modify as needed for your enterprise applications.

---

Built by Hema & Vibe coding using [Databricks Connect](https://docs.databricks.com/en/dev-tools/databricks-connect/index.html) + [Streamlit](https://streamlit.io/)
