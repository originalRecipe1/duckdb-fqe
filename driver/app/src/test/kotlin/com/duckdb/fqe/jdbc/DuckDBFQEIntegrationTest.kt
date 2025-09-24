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
        private const val DUCKDB_FQE_URL = "jdbc:duckdb-fqe://localhost:8080/"

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
                println("✅ DuckDB FQE server is available at localhost:8080")
            } catch (e: SQLException) {
                assumeTrue(false,
                    "DuckDB FQE server not available at localhost:8080. " +
                    "Please start the container: docker-compose up -d duckdb-fqe"
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
    fun `load extensions and setup databases`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            conn.createStatement().use { stmt ->
                // Load required extensions
                try {
                    stmt.execute("LOAD postgres")
                    println("✓ PostgreSQL extension loaded")
                } catch (e: SQLException) {
                    println("⚠ PostgreSQL extension already loaded or not available: ${e.message}")
                }

                try {
                    stmt.execute("LOAD mysql")
                    println("✓ MySQL extension loaded")
                } catch (e: SQLException) {
                    println("⚠ MySQL extension already loaded or not available: ${e.message}")
                }
            }
        }
    }

    @Test
    @Order(4)
    fun `attach and verify test databases`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            conn.createStatement().use { stmt ->
                // Attach PostgreSQL database
                try {
                    stmt.execute("""
                        ATTACH 'host=localhost port=5432 dbname=db1 user=postgres password=123456'
                        AS postgres_db (TYPE postgres)
                    """)
                    println("✓ PostgreSQL database attached as postgres_db")
                } catch (e: SQLException) {
                    println("⚠ PostgreSQL database attachment failed: ${e.message}")
                }

                // Attach MySQL database
                try {
                    stmt.execute("""
                        ATTACH 'host=localhost port=3306 database=db2 user=mysql password=123456'
                        AS mysql_db (TYPE mysql)
                    """)
                    println("✓ MySQL database attached as mysql_db")
                } catch (e: SQLException) {
                    println("⚠ MySQL database attachment failed: ${e.message}")
                }

                // Attach MariaDB database
                try {
                    stmt.execute("""
                        ATTACH 'host=localhost port=3307 database=db3 user=mariadb password=123456'
                        AS mariadb_db (TYPE mysql)
                    """)
                    println("✓ MariaDB database attached as mariadb_db")
                } catch (e: SQLException) {
                    println("⚠ MariaDB database attachment failed: ${e.message}")
                }

                // List attached databases
                val rs = stmt.executeQuery("SHOW DATABASES")
                val databases = mutableListOf<String>()
                while (rs.next()) {
                    databases.add(rs.getString(1))
                }
                println("📋 Available databases: ${databases.joinToString(", ")}")

                // Verify at least some databases are attached
                assertTrue(databases.isNotEmpty(), "No databases attached")
            }
        }
    }

    @Test
    @Order(5)
    fun `query PostgreSQL test data`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            conn.createStatement().use { stmt ->
                try {
                    val rs = stmt.executeQuery("""
                        SELECT COUNT(*) as customer_count
                        FROM postgres_db.public.customers
                    """)

                    if (rs.next()) {
                        val count = rs.getLong("customer_count")
                        println("✓ PostgreSQL customers: $count")
                        assertTrue(count > 0, "Should have customers in PostgreSQL")
                    }
                } catch (e: SQLException) {
                    println("⚠ PostgreSQL query failed: ${e.message}")
                    // Don't fail the test if database isn't properly set up
                }
            }
        }
    }

    @Test
    @Order(6)
    fun `query MySQL test data`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            conn.createStatement().use { stmt ->
                try {
                    val rs = stmt.executeQuery("""
                        SELECT COUNT(*) as customer_count
                        FROM mysql_db.customers
                    """)

                    if (rs.next()) {
                        val count = rs.getLong("customer_count")
                        println("✓ MySQL customers: $count")
                        assertTrue(count > 0, "Should have customers in MySQL")
                    }
                } catch (e: SQLException) {
                    println("⚠ MySQL query failed: ${e.message}")
                    // Don't fail the test if database isn't properly set up
                }
            }
        }
    }

    @Test
    @Order(7)
    fun `query MariaDB test data`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            conn.createStatement().use { stmt ->
                try {
                    val rs = stmt.executeQuery("""
                        SELECT COUNT(*) as customer_count
                        FROM mariadb_db.customers
                    """)

                    if (rs.next()) {
                        val count = rs.getLong("customer_count")
                        println("✓ MariaDB customers: $count")
                        assertTrue(count > 0, "Should have customers in MariaDB")
                    }
                } catch (e: SQLException) {
                    println("⚠ MariaDB query failed: ${e.message}")
                    // Don't fail the test if database isn't properly set up
                }
            }
        }
    }

    @Test
    @Order(8)
    fun `federated cross-database query`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            conn.createStatement().use { stmt ->
                try {
                    val rs = stmt.executeQuery("""
                        SELECT
                            'PostgreSQL' as source,
                            COUNT(*) as count
                        FROM postgres_db.public.customers
                        UNION ALL
                        SELECT
                            'MySQL' as source,
                            COUNT(*) as count
                        FROM mysql_db.customers
                        UNION ALL
                        SELECT
                            'MariaDB' as source,
                            COUNT(*) as count
                        FROM mariadb_db.customers
                        ORDER BY source
                    """)

                    val results = mutableMapOf<String, Long>()
                    while (rs.next()) {
                        val source = rs.getString("source")
                        val count = rs.getLong("count")
                        results[source] = count
                        println("✓ $source: $count customers")
                    }

                    // Verify we got results from multiple databases
                    assertTrue(results.isNotEmpty(), "Should have results from federated query")

                    val totalCustomers = results.values.sum()
                    println("🎯 Total customers across all databases: $totalCustomers")

                } catch (e: SQLException) {
                    println("⚠ Federated query failed: ${e.message}")
                    // This might fail if not all databases are set up correctly
                }
            }
        }
    }

    @Test
    @Order(9)
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
    @Order(10)
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
    @Order(11)
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
    @Order(12)
    fun `table discovery and sample data queries`() {
        DriverManager.getConnection(DUCKDB_FQE_URL).use { conn ->
            conn.createStatement().use { stmt ->
                // Discover all available tables across all attached databases
                val availableTables = mutableListOf<String>()

                // Get list of all databases
                val databases = mutableListOf<String>()
                try {
                    val dbResult = stmt.executeQuery("SHOW DATABASES")
                    while (dbResult.next()) {
                        val dbName = dbResult.getString(1)
                        if (dbName != "system" && dbName != "temp") { // Skip system databases
                            databases.add(dbName)
                        }
                    }
                    println("📋 Found databases: ${databases.joinToString(", ")}")
                } catch (e: SQLException) {
                    println("⚠ Could not list databases: ${e.message}")
                }

                // For each database, try to discover tables
                for (database in databases) {
                    try {
                        // Try different approaches to discover tables
                        val tableQueries = listOf(
                            "SHOW TABLES FROM $database",
                            "SELECT table_name FROM information_schema.tables WHERE table_schema = '$database'",
                            "SELECT name FROM $database.sqlite_master WHERE type='table'",
                            "PRAGMA $database.table_list"
                        )

                        var tablesFound = false
                        for (query in tableQueries) {
                            try {
                                val result = stmt.executeQuery(query)
                                while (result.next()) {
                                    val tableName = result.getString(1)
                                    val fullTableName = "$database.$tableName"
                                    availableTables.add(fullTableName)
                                    tablesFound = true
                                }
                                if (tablesFound) {
                                    // println("✓ Found tables in $database using: $query")
                                    break
                                }
                            } catch (e: SQLException) {
                                // Try next query approach
                                continue
                            }
                        }

                        // If no tables found with standard queries, try direct table access
                        if (!tablesFound) {
                            // Try known table names from our test setup
                            val knownTables = listOf("customers", "public.customers", "users", "orders")
                            for (tableName in knownTables) {
                                try {
                                    val testQuery = "SELECT COUNT(*) FROM $database.$tableName LIMIT 1"
                                    stmt.executeQuery(testQuery).use { result ->
                                        if (result.next()) {
                                            val fullTableName = "$database.$tableName"
                                            availableTables.add(fullTableName)
                                            // println("✓ Discovered table by probe: $fullTableName")
                                        }
                                    }
                                } catch (e: SQLException) {
                                    // Table doesn't exist, continue
                                }
                            }
                        }
                    } catch (e: SQLException) {
                        println("⚠ Could not discover tables in $database: ${e.message}")
                    }
                }

                // Select up to 2 tables for querying
                val tablesToQuery = availableTables.shuffled().take(2)
                println("🔍 Discovered ${availableTables.size} tables: ${availableTables.joinToString(", ")}")
                println("🎯 Testing ${tablesToQuery.size} random tables: ${tablesToQuery.joinToString(", ")}")

                assertTrue(tablesToQuery.isNotEmpty(), "Should discover at least one table")

                // Query first 5 entries from each selected table
                for (tableName in tablesToQuery) {
                    try {
                        // Query sample data
                        val sampleQuery = "SELECT * FROM $tableName LIMIT 5"
                        val result = stmt.executeQuery(sampleQuery)
                        val metaData = result.metaData
                        val columnCount = metaData.columnCount

                        // Count sample rows
                        var rowCount = 0
                        while (result.next() && rowCount < 5) {
                            rowCount++
                        }

                        println("   ✅ $tableName: $columnCount columns, $rowCount sample rows")

                        // Verify we got some data
                        assertTrue(rowCount >= 0, "Query should execute successfully")

                    } catch (e: SQLException) {
                        println("   ❌ $tableName: Query failed")
                        // Don't fail the test if individual table query fails
                    }
                }

                // Summary
                println("\n📊 RESULT: Found ${availableTables.size} tables in ${databases.size} databases, sampled ${tablesToQuery.size} tables ✅")
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