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
FROM postgres.public.customer p
JOIN mysql.db1.orders m ON p.c_custkey = m.o_custkey
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
Open http://localhost:8082 in your browser

### 3. Databases Auto-Attach on Startup
The databases are automatically attached when the container starts:
- `postgres` - PostgreSQL database
- `mysql` - MySQL database
- `mariadb` - MariaDB database

You can verify with:
```sql
SHOW DATABASES;
```

### 4. Query Across Databases
```sql
-- See all available databases
SHOW DATABASES;

-- Count customers in each database
SELECT 'PostgreSQL' as db, COUNT(*) FROM postgres.public.customer
UNION ALL
SELECT 'MySQL' as db, COUNT(*) FROM mysql.db1.customer;

-- Cross-database join
SELECT
    CONCAT(p.c_first_name, ' ', p.c_last_name) as postgres_customer,
    CONCAT(m.c_first_name, ' ', m.c_last_name) as mysql_customer
FROM postgres.public.customer p
JOIN mysql.db1.customer m ON p.c_customer_sk = m.c_customer_sk
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

1. **Update connection details** in `init.sql`:
```sql
-- Edit /app/init.sql in the container or update the local init.sql file
ATTACH 'host=your-postgres-host port=5432 dbname=yourdb user=youruser password=yourpass' AS your_postgres (TYPE postgres);
ATTACH 'host=your-mysql-host port=3306 database=yourdb user=youruser password=yourpass' AS your_mysql (TYPE mysql);
```

2. **Rebuild and restart** the container for changes to take effect:
```bash
docker-compose down -v
docker-compose up -d --build
```

3. **Network access**: Ensure DuckDB container can reach your databases
4. **Credentials**: Use environment variables or secrets management for production

### Supported Databases
- ✅ PostgreSQL (any version)
- ✅ MySQL (5.7+, 8.0+)
- ✅ MariaDB (10.0+)
- ✅ Any database with DuckDB extensions

## API Usage

### HTTP API
```bash
# Execute query via HTTP
curl -X POST "http://localhost:8082/?default_format=JSONCompact" \
  -H "Content-Type: text/plain" \
  -d "SELECT COUNT(*) FROM postgres.public.customer"
```

### Python Client
```python
import requests

def execute_query(query):
    response = requests.post(
        "http://localhost:8082/?default_format=JSONCompact",
        data=query,
        headers={"Content-Type": "text/plain"}
    )
    return response.json()

# Execute federated query
result = execute_query("""
    SELECT db_name, COUNT(*) as customers
    FROM (
        SELECT 'PostgreSQL' as db_name, c_customer_sk FROM postgres.public.customer
        UNION ALL
        SELECT 'MySQL' as db_name, c_customer_sk FROM mysql.db1.customer
    ) t
    GROUP BY db_name
""")
print(result)
```

### JDBC Connection
You can also connect via JDBC using the DuckDB JDBC driver:
```
jdbc:duckdb:http://localhost:8082
```

Example Java code:
```java
import java.sql.*;

Connection conn = DriverManager.getConnection("jdbc:duckdb:http://localhost:8082");
Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM postgres.public.customer");
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
- **Test Databases**: TPC-DS benchmark data for demonstration

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

**Built with**: DuckDB 1.3.2, Docker, Python | **TPC-DS Test Data**: 300,000 total records across 3 databases (100k per database)
