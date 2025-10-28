-- Install required extensions
INSTALL postgres;
INSTALL mysql;
INSTALL httpserver FROM community;

-- Load extensions
LOAD postgres;
LOAD mysql;
LOAD httpserver;

-- Attach PostgreSQL database
ATTACH 'host=postgres_ds port=5432 dbname=db1 user=postgres password=123456' AS postgres (TYPE postgres);

-- Attach MySQL databases
ATTACH 'host=mysql_ds port=3306 database=db1 user=mysql password=123456' AS mysql (TYPE mysql);
ATTACH 'host=mariadb_ds port=3306 database=db1 user=mariadb password=123456' AS mariadb (TYPE mysql);

-- Start HTTP server (this needs to be last so attachments are already in place)
SELECT httpserve_start('0.0.0.0', 8080, '');
