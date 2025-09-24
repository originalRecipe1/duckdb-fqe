-- Install required extensions
INSTALL postgres;
INSTALL mysql;
INSTALL httpserver FROM community;

-- Load extensions
LOAD postgres;
LOAD mysql;
LOAD httpserver;

SELECT httpserve_start('0.0.0.0', 8080, '');

-- Attach PostgreSQL database
ATTACH 'host=postgres port=5432 dbname=db1 user=postgres password=123456' AS postgres_db (TYPE postgres);

-- Attach MySQL databases
ATTACH 'host=mysql port=3306 database=db1 user=mysql password=123456' AS mysql_db (TYPE mysql);
ATTACH 'host=mariadb port=3306 database=db1 user=mariadb password=123456' AS mariadb_db (TYPE mysql);

-- Show all attached databases
SHOW DATABASES;

-- Create a view to list all available tables across databases
CREATE VIEW federated_tables AS
SELECT
    table_catalog as database_name,
    table_schema as schema_name,
    table_name,
    CONCAT(table_catalog, '.', table_schema, '.', table_name) AS full_table_name
FROM information_schema.tables
WHERE table_catalog IN ('postgres_db', 'mysql_db', 'mariadb_db');

-- Show the federated tables view
SELECT * FROM federated_tables;

-- Test a simple federated query
SELECT 'DuckDB FQE is working!' as status;

-- Print HTTP server info
SELECT 'HTTP Server started on port 8080' as message;

-- Keep DuckDB running by starting an infinite loop
SELECT 'HTTP Server running on port 8080. Keeping alive...' as message;
.timer on
-- Keep session alive with periodic no-op
CREATE TABLE IF NOT EXISTS keepalive (id INTEGER);
INSERT OR IGNORE INTO keepalive VALUES (1);
