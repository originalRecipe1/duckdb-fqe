# DuckDB Federated Query Engine (FQE)

A containerized solution that enables unified SQL queries across multiple heterogeneous databases using DuckDB as a federated query engine with HTTP API access.

## What is this?

This project implements a **Federated Query Engine** that allows you to:
- Query multiple databases (PostgreSQL, MySQL, MariaDB) using a single SQL interface
- Execute cross-database joins and aggregations seamlessly
- Access everything through a web UI or HTTP API
- Run complex analytics across heterogeneous data sources

**Example**: Join customers from PostgreSQL with orders from MySQL in a single query:
```sql
SELECT p.c_name, m.o_orderkey, m.o_totalprice
FROM postgres_db.public.customer p
JOIN mysql_db.db1.orders m ON p.c_custkey = m.o_custkey
LIMIT 10;
```

## Quick Setup

### Prerequisites
- Docker and Docker Compose
- Python 3.8+ (for testing)

### 1. Clone and Start
```bash
git clone <this-repo>
cd duckdb-fqe

# Start the federated query engine + test databases
docker-compose up -d
```

### 2. Access the Web Interface
Open http://localhost:8080 in your browser

### 3. Attach Your Databases
In the web interface, run these commands to connect to databases:
```sql
-- Load extensions
INSTALL postgres; INSTALL mysql;
LOAD postgres; LOAD mysql;

-- Attach your databases
ATTACH 'host=postgres port=5432 dbname=db1 user=postgres password=123456' AS postgres_db (TYPE postgres);
ATTACH 'host=mysql port=3306 database=db1 user=mysql password=123456' AS mysql_db (TYPE mysql);
ATTACH 'host=mariadb port=3306 database=db1 user=mariadb password=123456' AS mariadb_db (TYPE mysql);
```

### 4. Query Across Databases
```sql
-- See all available databases
SHOW DATABASES;

-- Count customers in each database
SELECT 'PostgreSQL' as db, COUNT(*) FROM postgres_db.public.customer
UNION ALL
SELECT 'MySQL' as db, COUNT(*) FROM mysql_db.db1.customer;

-- Cross-database join
SELECT p.c_name, m.c_name
FROM postgres_db.public.customer p
JOIN mysql_db.db1.customer m ON p.c_custkey = m.c_custkey
LIMIT 5;
```

## Testing

Run the comprehensive test suite:
```bash
# Install Python dependencies
pip install -r requirements.txt

# Run tests
python test_connection_updated.py
```

Expected output: **All 5 tests should pass** ✅

## Configuration

### For Your Own Databases

1. **Update connection details** in your queries or create a config file:
```sql
ATTACH 'host=your-postgres-host port=5432 dbname=yourdb user=youruser password=yourpass' AS your_postgres (TYPE postgres);
ATTACH 'host=your-mysql-host port=3306 database=yourdb user=youruser password=yourpass' AS your_mysql (TYPE mysql);
```

2. **Network access**: Ensure DuckDB container can reach your databases
3. **Credentials**: Use environment variables or secrets management for production

### Supported Databases
- ✅ PostgreSQL (any version)
- ✅ MySQL (5.7+, 8.0+)
- ✅ MariaDB (10.0+)
- ✅ Any database with DuckDB extensions

## API Usage

### HTTP API
```bash
# Execute query via HTTP
curl -X POST "http://localhost:8080/?default_format=JSONCompact" \
  -H "Content-Type: text/plain" \
  -d "SELECT COUNT(*) FROM postgres_db.public.customer"
```

### Python Client
```python
from duckdb_client import DuckDBFQEClient

with DuckDBFQEClient() as client:
    # Execute federated query
    result = client.execute_query("""
        SELECT db_name, COUNT(*) as customers
        FROM (
            SELECT 'PostgreSQL' as db_name, c_custkey FROM postgres_db.public.customer
            UNION ALL
            SELECT 'MySQL' as db_name, c_custkey FROM mysql_db.db1.customer
        ) t
        GROUP BY db_name
    """)
    print(result)
```

## Architecture & Stack

### The Stack
```
┌─────────────────────────────────────────┐
│           Web UI / HTTP API             │ ← You interact here
│        (Browser/curl/Python)            │
└─────────────────────────────────────────┘
                    │ HTTP
┌─────────────────────────────────────────┐
│            DuckDB FQE                   │ ← Federated Query Engine
│     (HTTP Server + SQL Engine)          │
│   • Query parsing & optimization        │
│   • Cross-database joins                │
│   • Result aggregation                  │
└─────────────────────────────────────────┘
         │              │              │
    ┌────▼────┐   ┌─────▼─────┐   ┌────▼─────┐
    │postgres │   │   MySQL   │   │ MariaDB  │ ← Your data sources
    │Port:5432│   │ Port:3306 │   │Port:3307 │
    │150k rows│   │ 150k rows │   │150k rows │
    └─────────┘   └───────────┘   └──────────┘
```

### How It Works

1. **Query Submission**: Submit SQL via web UI or HTTP API
2. **Query Planning**: DuckDB analyzes the query and creates an execution plan
3. **Federated Execution**:
   - DuckDB connects to each required database
   - Pushes down predicates and projections when possible
   - Retrieves only necessary data from each source
4. **Data Integration**: DuckDB performs joins, aggregations, and transformations
5. **Result Return**: Unified result set returned as JSON/table format

### Key Components

- **DuckDB Core**: Fast analytical SQL engine with federated capabilities
- **HTTP Server Extension**: Provides REST API access to DuckDB
- **Database Extensions**: PostgreSQL, MySQL connectivity
- **Docker Environment**: Containerized deployment with networking
- **Test Databases**: TPC-H benchmark data for demonstration

### Performance Benefits

- **Query Pushdown**: Filters and projections pushed to source databases
- **Parallel Execution**: Concurrent queries to multiple databases
- **Columnar Processing**: DuckDB's vectorized execution engine
- **Memory Efficiency**: Streaming results, minimal data movement

### Use Cases

- **Data Analytics**: Cross-database business intelligence queries
- **Data Migration**: Query data across old and new systems simultaneously
- **Multi-tenant Systems**: Aggregate data from tenant-specific databases
- **Legacy Integration**: Modern SQL interface for heterogeneous legacy systems
- **Real-time Dashboards**: Live queries across operational databases

## Production Considerations

- **Security**: Use SSL connections, proper authentication, network isolation
- **Performance**: Monitor query execution plans, consider materialized views
- **Scaling**: Deploy multiple DuckDB instances behind a load balancer
- **Monitoring**: Track query performance, database connection health
- **Backup**: Ensure source databases have proper backup strategies

---

**Built with**: DuckDB 1.3.2, Docker, Python | **TPC-H Test Data**: 450,000 total records across 3 databases
