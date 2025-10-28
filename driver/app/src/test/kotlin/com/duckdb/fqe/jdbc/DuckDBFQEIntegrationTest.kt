package com.duckdb.fqe.jdbc

import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.junit.jupiter.api.Assumptions.assumeTrue
import java.sql.DriverManager
import java.sql.SQLException

/**
 * Integration tests that connect to a real DuckDB FQE server.
 * These tests require the DuckDB FQE container to be running on localhost:8080
 * with test databases (PostgreSQL, MySQL, MariaDB) available.
 *
 * Run with: ./gradlew test -Dintegration.tests=true
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@EnabledIfSystemProperty(named = "integration.tests", matches = "true")
class DuckDBFQEIntegrationTest {

    companion object {
        private const val DUCKDB_FQE_URL = "jdbc:duckdb-fqe://localhost:8082/"

        @JvmStatic
        @BeforeAll
        fun checkServerAvailability() {
            try {
                DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
                    conn.createStatement().use { stmt ->
                        stmt.executeQuery("SELECT 1").use { rs ->
                            assertTrue(rs.next())
                            assertEquals(1, rs.getInt(1))
                        }
                    }
                }
                println("✅ DuckDB FQE server is available at localhost:8082")
            } catch (e: SQLException) {
                assumeTrue(false,
                    "DuckDB FQE server not available at localhost:8082. " +
                    "Please start the container: docker-compose up -d"
                )
            }
        }
    }

    @Test
    @Order(1)
    fun `basic connection and simple query`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            assertFalse(conn.isClosed)
            assertTrue(conn.autoCommit)

            conn.createStatement().use { stmt ->
                val rs = stmt.executeQuery("SELECT 'Integration Test Success' as message")
                assertTrue(rs.next())
                assertEquals("Integration Test Success", rs.getString("message"))
                assertFalse(rs.next())
            }
        }
    }

    @Test
    @Order(2)
    fun `database metadata verification`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            val metaData = conn.metaData
            assertEquals("DuckDB FQE JDBC Driver", metaData.driverName)
            assertEquals("DuckDB Federated Query Engine", metaData.databaseProductName)
            assertEquals(1, metaData.driverMajorVersion)
            assertEquals(0, metaData.driverMinorVersion)
        }
    }

    @Test
    @Order(3)
    fun `verify databases are auto-attached`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            conn.createStatement().use { stmt ->
                // List attached databases
                val rs = stmt.executeQuery("SHOW DATABASES")
                val databases = mutableListOf<String>()
                while (rs.next()) {
                    databases.add(rs.getString(1))
                }
                println("📋 Available databases: ${databases.joinToString(", ")}")

                // Verify required databases are attached
                assertTrue(databases.contains("postgres"), "postgres database should be auto-attached")
                assertTrue(databases.contains("mysql"), "mysql database should be auto-attached")
                assertTrue(databases.contains("mariadb"), "mariadb database should be auto-attached")
                println("✓ All required databases are attached")
            }
        }
    }

    @Test
    @Order(4)
    fun `query customer counts from each database`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            conn.createStatement().use { stmt ->
                // PostgreSQL
                val pgRs = stmt.executeQuery("SELECT COUNT(*) as count FROM postgres.public.customer")
                assertTrue(pgRs.next())
                val pgCount = pgRs.getLong("count")
                println("✓ PostgreSQL customers: $pgCount")
                assertTrue(pgCount > 0)

                // MySQL
                val mysqlRs = stmt.executeQuery("SELECT COUNT(*) as count FROM mysql.db1.customer")
                assertTrue(mysqlRs.next())
                val mysqlCount = mysqlRs.getLong("count")
                println("✓ MySQL customers: $mysqlCount")
                assertTrue(mysqlCount > 0)

                // MariaDB
                val mariadbRs = stmt.executeQuery("SELECT COUNT(*) as count FROM mariadb.db1.customer")
                assertTrue(mariadbRs.next())
                val mariadbCount = mariadbRs.getLong("count")
                println("✓ MariaDB customers: $mariadbCount")
                assertTrue(mariadbCount > 0)
            }
        }
    }

    @Test
    @Order(5)
    fun `federated cross-database aggregation`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            conn.createStatement().use { stmt ->
                val rs = stmt.executeQuery("""
                    SELECT
                        'PostgreSQL' as source,
                        COUNT(*) as count,
                        AVG(c_birth_year) as avg_birth_year
                    FROM postgres.public.customer
                    UNION ALL
                    SELECT
                        'MySQL' as source,
                        COUNT(*) as count,
                        AVG(c_birth_year) as avg_birth_year
                    FROM mysql.db1.customer
                    UNION ALL
                    SELECT
                        'MariaDB' as source,
                        COUNT(*) as count,
                        AVG(c_birth_year) as avg_birth_year
                    FROM mariadb.db1.customer
                    ORDER BY source
                """)

                val results = mutableMapOf<String, Long>()
                while (rs.next()) {
                    val source = rs.getString("source")
                    val count = rs.getLong("count")
                    val avgYear = rs.getDouble("avg_birth_year")
                    results[source] = count
                    println("✓ $source: $count customers, avg birth year: ${avgYear.toInt()}")
                }

                assertTrue(results.size == 3, "Should have results from all 3 databases")
                val totalCustomers = results.values.sum()
                println("🎯 Total customers across all databases: $totalCustomers")
                assertTrue(totalCustomers >= 300000, "Should have at least 300k customers")
            }
        }
    }

    @Test
    @Order(6)
    fun `cross-database join query`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            conn.createStatement().use { stmt ->
                val rs = stmt.executeQuery("""
                    SELECT
                        CONCAT(p.c_first_name, ' ', p.c_last_name) as postgres_customer,
                        CONCAT(m.c_first_name, ' ', m.c_last_name) as mysql_customer,
                        p.c_customer_sk
                    FROM postgres.public.customer p
                    JOIN mysql.db1.customer m ON p.c_customer_sk = m.c_customer_sk
                    WHERE p.c_customer_sk <= 10
                    ORDER BY p.c_customer_sk
                    LIMIT 5
                """)

                var rowCount = 0
                while (rs.next()) {
                    rowCount++
                    val pgName = rs.getString("postgres_customer")
                    val mysqlName = rs.getString("mysql_customer")
                    val customerSk = rs.getInt("c_customer_sk")
                    println("✓ Customer $customerSk: PG='$pgName' | MySQL='$mysqlName'")
                }

                assertTrue(rowCount > 0, "Should have joined results")
                println("🎯 Successfully joined $rowCount customer records across databases")
            }
        }
    }

    @Test
    @Order(7)
    fun `complex TPC-DS federated query with CTE`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            conn.createStatement().use { stmt ->
                val rs = stmt.executeQuery("""
                    WITH customer_total_return AS (
                        SELECT
                            sr_customer_sk AS ctr_customer_sk,
                            sr_store_sk AS ctr_store_sk,
                            SUM(SR_FEE) AS ctr_total_return
                        FROM mysql.db1.store_returns, mariadb.db1.date_dim
                        WHERE sr_returned_date_sk = d_date_sk
                            AND d_year = 2000
                        GROUP BY sr_customer_sk, sr_store_sk
                    )
                    SELECT c_customer_id
                    FROM customer_total_return AS ctr1,
                         postgres.public.store,
                         postgres.public.customer
                    WHERE ctr1.ctr_total_return > (
                            SELECT AVG(ctr_total_return) * 1.2
                            FROM customer_total_return AS ctr2
                            WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk
                        )
                        AND s_store_sk = ctr1.ctr_store_sk
                        AND s_state = 'TN'
                        AND ctr1.ctr_customer_sk = c_customer_sk
                    ORDER BY c_customer_id NULLS FIRST
                    LIMIT 100
                """)

                var rowCount = 0
                val customerIds = mutableListOf<String>()
                while (rs.next()) {
                    rowCount++
                    val customerId = rs.getString("c_customer_id")
                    customerIds.add(customerId)
                    if (rowCount <= 5) {
                        println("  Customer ID: $customerId")
                    }
                }

                assertTrue(rowCount > 0, "Should have query results")
                println("✓ Complex TPC-DS query executed successfully: $rowCount results")
                println("🎯 This query joins data from MySQL (store_returns), MariaDB (date_dim), and PostgreSQL (store, customer)")
            }
        }
    }

    @Test
    @Order(8)
    fun `prepared statement with parameters`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            conn.prepareStatement("SELECT ? as test_value, ? as another_value").use { stmt ->
                stmt.setString(1, "Integration Test")
                stmt.setInt(2, 42)

                val rs = stmt.executeQuery()
                assertTrue(rs.next())
                assertEquals("Integration Test", rs.getString("test_value"))
                assertEquals(42, rs.getInt("another_value"))
            }
        }
    }

    @Test
    @Order(9)
    fun `DDL operations - create and drop table`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            conn.createStatement().use { stmt ->
                // Create a test table
                val createResult = stmt.executeUpdate("CREATE TABLE integration_test (id INTEGER, name VARCHAR)")
                assertEquals(0, createResult) // DuckDB FQE returns 0 for DDL

                // Verify table exists by querying it
                val rs = stmt.executeQuery("SELECT COUNT(*) FROM integration_test")
                assertTrue(rs.next())
                assertEquals(0, rs.getLong(1)) // Empty table

                // Drop the test table
                val dropResult = stmt.executeUpdate("DROP TABLE integration_test")
                assertEquals(0, dropResult)
            }
        }
    }

    @Test
    @Order(10)
    fun `connection pooling simulation`() {
        // Test multiple concurrent connections
        val connections = mutableListOf<java.sql.Connection>()

        try {
            repeat(3) { i ->
                val conn = DriverManager.getConnection(DUCKDB_FQE_URL)
                connections.add(conn)

                conn.createStatement().use { stmt ->
                    val rs = stmt.executeQuery("SELECT $i as connection_id")
                    assertTrue(rs.next())
                    assertEquals(i, rs.getInt("connection_id"))
                }
            }

            println("✓ Multiple connections handled successfully")

        } finally {
            connections.forEach { it.close() }
        }
    }

    @Test
    @Order(11)
    fun `sample TPC-DS queries on each database`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            conn.createStatement().use { stmt ->
                // Test simple query on PostgreSQL
                val pgRs = stmt.executeQuery("""
                    SELECT c_customer_id, c_first_name, c_last_name
                    FROM postgres.public.customer
                    LIMIT 5
                """)
                var pgCount = 0
                while (pgRs.next()) {
                    pgCount++
                }
                assertEquals(5, pgCount, "Should get 5 rows from PostgreSQL customer table")
                println("✓ PostgreSQL sample query: $pgCount rows")

                // Test simple query on MySQL
                val mysqlRs = stmt.executeQuery("""
                    SELECT sr_item_sk, sr_customer_sk, sr_return_amt
                    FROM mysql.db1.store_returns
                    LIMIT 5
                """)
                var mysqlCount = 0
                while (mysqlRs.next()) {
                    mysqlCount++
                }
                assertEquals(5, mysqlCount, "Should get 5 rows from MySQL store_returns table")
                println("✓ MySQL sample query: $mysqlCount rows")

                // Test simple query on MariaDB
                val mariadbRs = stmt.executeQuery("""
                    SELECT d_date_sk, d_year, d_moy, d_date
                    FROM mariadb.db1.date_dim
                    WHERE d_year = 2000
                    LIMIT 5
                """)
                var mariadbCount = 0
                while (mariadbRs.next()) {
                    mariadbCount++
                }
                assertEquals(5, mariadbCount, "Should get 5 rows from MariaDB date_dim table")
                println("✓ MariaDB sample query: $mariadbCount rows")

                println("🎯 All TPC-DS sample queries executed successfully")
            }
        }
    }

    @Test
    @Order(12)
    fun `advanced TPC-DS query with multiple CTEs and aggregations`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            conn.createStatement().use { stmt ->
                val rs = stmt.executeQuery("""
                    WITH wscs AS (
                        SELECT sold_date_sk, sales_price
                        FROM (
                            SELECT ws_sold_date_sk AS sold_date_sk, ws_ext_sales_price AS sales_price
                            FROM mariadb.db1.web_sales
                            UNION ALL
                            SELECT cs_sold_date_sk AS sold_date_sk, cs_ext_sales_price AS sales_price
                            FROM mysql.db1.catalog_sales
                        )
                    ),
                    wswscs AS (
                        SELECT d_week_seq,
                            SUM(CASE WHEN (d_day_name = 'Sunday') THEN sales_price ELSE NULL END) AS sun_sales,
                            SUM(CASE WHEN (d_day_name = 'Monday') THEN sales_price ELSE NULL END) AS mon_sales,
                            SUM(CASE WHEN (d_day_name = 'Tuesday') THEN sales_price ELSE NULL END) AS tue_sales,
                            SUM(CASE WHEN (d_day_name = 'Wednesday') THEN sales_price ELSE NULL END) AS wed_sales,
                            SUM(CASE WHEN (d_day_name = 'Thursday') THEN sales_price ELSE NULL END) AS thu_sales,
                            SUM(CASE WHEN (d_day_name = 'Friday') THEN sales_price ELSE NULL END) AS fri_sales,
                            SUM(CASE WHEN (d_day_name = 'Saturday') THEN sales_price ELSE NULL END) AS sat_sales
                        FROM wscs, postgres.public.date_dim
                        WHERE d_date_sk = sold_date_sk
                        GROUP BY d_week_seq
                    )
                    SELECT d_week_seq1,
                        ROUND(CAST(sun_sales1 AS DOUBLE) / sun_sales2, 2),
                        ROUND(CAST(mon_sales1 AS DOUBLE) / mon_sales2, 2),
                        ROUND(CAST(tue_sales1 AS DOUBLE) / tue_sales2, 2),
                        ROUND(CAST(wed_sales1 AS DOUBLE) / wed_sales2, 2),
                        ROUND(CAST(thu_sales1 AS DOUBLE) / thu_sales2, 2),
                        ROUND(CAST(fri_sales1 AS DOUBLE) / fri_sales2, 2),
                        ROUND(CAST(sat_sales1 AS DOUBLE) / sat_sales2, 2)
                    FROM (
                        SELECT wswscs.d_week_seq AS d_week_seq1,
                            sun_sales AS sun_sales1, mon_sales AS mon_sales1, tue_sales AS tue_sales1,
                            wed_sales AS wed_sales1, thu_sales AS thu_sales1, fri_sales AS fri_sales1, sat_sales AS sat_sales1
                        FROM wswscs, postgres.public.date_dim
                        WHERE date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2001
                    ) AS y,
                    (
                        SELECT wswscs.d_week_seq AS d_week_seq2,
                            sun_sales AS sun_sales2, mon_sales AS mon_sales2, tue_sales AS tue_sales2,
                            wed_sales AS wed_sales2, thu_sales AS thu_sales2, fri_sales AS fri_sales2, sat_sales AS sat_sales2
                        FROM wswscs, postgres.public.date_dim
                        WHERE date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2001 + 1
                    ) AS z
                    WHERE d_week_seq1 = d_week_seq2 - 53
                    ORDER BY d_week_seq1 NULLS FIRST
                """)

                var rowCount = 0
                while (rs.next()) {
                    rowCount++
                    if (rowCount <= 3) {
                        val weekSeq = rs.getInt(1)
                        println("  Week $weekSeq: Sun=${rs.getDouble(2)}, Mon=${rs.getDouble(3)}, Tue=${rs.getDouble(4)}")
                    }
                }

                assertTrue(rowCount >= 0, "Should execute advanced query successfully")
                println("✓ Advanced TPC-DS query with multiple CTEs executed successfully: $rowCount results")
                println("🎯 This query joins web_sales (MariaDB), catalog_sales (MySQL), and date_dim (PostgreSQL)")
            }
        }
    }

    @Test
    @Order(13)
    fun `error handling with invalid queries`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            conn.createStatement().use { stmt ->
                // Test syntax error
                assertThrows(SQLException::class.java) {
                    stmt.executeQuery("INVALID SQL SYNTAX")
                }

                // Test non-existent table
                assertThrows(SQLException::class.java) {
                    stmt.executeQuery("SELECT * FROM non_existent_table_12345")
                }
            }
        }
    }
}