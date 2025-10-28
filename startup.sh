#!/bin/bash

# Start DuckDB with init script in background
duckdb -interactive -init /app/init.sql &
DUCKDB_PID=$!

# Wait for HTTP server to be ready
echo "Waiting for HTTP server to start..."
for i in {1..30}; do
    if wget -q -O- http://localhost:8080/?default_format=JSONCompact --post-data="SELECT 1 as test" --header="Content-Type: text/plain" 2>/dev/null | grep -q "test"; then
        echo "HTTP server is ready!"
        break
    fi
    sleep 1
done

# Attach databases via HTTP
echo "Attaching databases..."
wget -q -O- http://localhost:8080/?default_format=JSONCompact --post-data="LOAD postgres;" --header="Content-Type: text/plain" >/dev/null 2>&1
wget -q -O- http://localhost:8080/?default_format=JSONCompact --post-data="LOAD mysql;" --header="Content-Type: text/plain" >/dev/null 2>&1
wget -q -O- http://localhost:8080/?default_format=JSONCompact --post-data="ATTACH 'host=postgres_ds port=5432 dbname=db1 user=postgres password=123456' AS postgres (TYPE postgres);" --header="Content-Type: text/plain" >/dev/null 2>&1
wget -q -O- http://localhost:8080/?default_format=JSONCompact --post-data="ATTACH 'host=mysql_ds port=3306 database=db1 user=mysql password=123456' AS mysql (TYPE mysql);" --header="Content-Type: text/plain" >/dev/null 2>&1
wget -q -O- http://localhost:8080/?default_format=JSONCompact --post-data="ATTACH 'host=mariadb_ds port=3306 database=db1 user=mariadb password=123456' AS mariadb (TYPE mysql);" --header="Content-Type: text/plain" >/dev/null 2>&1

echo "Databases attached successfully!"

# Keep container running
wait $DUCKDB_PID