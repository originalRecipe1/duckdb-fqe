# Pin to DuckDB v1.3.2 because the community httpserver extension is not yet
# available for v1.4.0 images.
FROM hfmuehleisen/duckdb:1.3.2

WORKDIR /app

# Copy configuration and initialization files
COPY config.json /app/config.json
COPY init.sql /app/init.sql

# Expose HTTP server port
EXPOSE 8080

# Set environment variable to keep HTTP server in foreground
ENV DUCKDB_HTTPSERVER_FOREGROUND=1

# Run DuckDB with initialization script and interactive mode
CMD ["duckdb", "-interactive", "-init", "/app/init.sql"]
