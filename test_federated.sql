-- Test federated query functionality
-- Install and load extensions
INSTALL postgres;
INSTALL mysql;
LOAD postgres;
LOAD mysql;

-- Attach databases
ATTACH 'host=postgres port=5432 dbname=db1 user=postgres password=123456' AS postgres_db (TYPE postgres);
ATTACH 'host=mysql port=3306 database=db1 user=mysql password=123456' AS mysql_db (TYPE mysql);
ATTACH 'host=mariadb port=3306 database=db1 user=mariadb password=123456' AS mariadb_db (TYPE mysql);

-- Show databases
SHOW DATABASES;

-- Test basic connectivity
SELECT 'DuckDB FQE is working!' as status;

-- Test cross-database queries
SELECT 'PostgreSQL Customer Count' as source, COUNT(*) as count FROM postgres_db.public.customer;
SELECT 'MySQL Customer Count' as source, COUNT(*) as count FROM mysql_db.db1.customer;
SELECT 'MariaDB Customer Count' as source, COUNT(*) as count FROM mariadb_db.db1.customer;

-- Test federated join
SELECT
    p.c_name as postgres_customer,
    m.c_name as mysql_customer
FROM postgres_db.public.customer p
JOIN mysql_db.db1.customer m ON p.c_custkey = m.c_custkey
LIMIT 5;