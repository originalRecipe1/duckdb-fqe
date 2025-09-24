#!/bin/bash

# Run DuckDB with initialization script and then keep it running
# The DUCKDB_HTTPSERVER_FOREGROUND=1 environment variable should keep the HTTP server alive
duckdb -c ".read /app/init.sql"

# If for some reason the above exits, start DuckDB in interactive mode
echo "Starting DuckDB in interactive mode..."
duckdb