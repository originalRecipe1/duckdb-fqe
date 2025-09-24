package com.duckdb.fqe.jdbc.demo

import com.duckdb.fqe.jdbc.DuckDBFQEDriver
import java.sql.DriverManager
import java.sql.SQLException

/**
 * Demo application showing how to use the DuckDB FQE JDBC Driver
 */
fun main() {
    println("DuckDB FQE JDBC Driver Demo")
    println("===========================")

    // Driver is automatically registered via static initializer
    val url = "jdbc:duckdb-fqe://localhost:8080/"

    try {
        // Test connection
        println("Connecting to DuckDB FQE at: $url")
        DriverManager.getConnection(url).use { connection ->
            println("✓ Connection successful!")

            // Test basic query
            println("\n1. Testing basic query...")
            connection.createStatement().use { statement ->
                val resultSet = statement.executeQuery("SELECT 'Hello from DuckDB FQE!' as message")
                if (resultSet.next()) {
                    println("Result: ${resultSet.getString("message")}")
                }
            }

            // Test database attachment
            println("\n2. Setting up databases...")
            connection.createStatement().use { statement ->
                // Install extensions
                statement.execute("INSTALL postgres")
                statement.execute("INSTALL mysql")
                statement.execute("LOAD postgres")
                statement.execute("LOAD mysql")

                // Attach databases
                statement.execute("""
                    ATTACH 'host=postgres port=5432 dbname=db1 user=postgres password=123456'
                    AS postgres_db (TYPE postgres)
                """.trimIndent())

                statement.execute("""
                    ATTACH 'host=mysql port=3306 database=db1 user=mysql password=123456'
                    AS mysql_db (TYPE mysql)
                """.trimIndent())

                statement.execute("""
                    ATTACH 'host=mariadb port=3306 database=db1 user=mariadb password=123456'
                    AS mariadb_db (TYPE mysql)
                """.trimIndent())

                println("✓ Databases attached successfully!")
            }

            // Test showing databases
            println("\n3. Showing attached databases...")
            connection.createStatement().use { statement ->
                val resultSet = statement.executeQuery("SHOW DATABASES")
                println("Available databases:")
                while (resultSet.next()) {
                    println("  - ${resultSet.getString(1)}")
                }
            }

            // Test federated query
            println("\n4. Testing federated queries...")

            // Count customers in each database
            connection.createStatement().use { statement ->
                val queries = listOf(
                    "PostgreSQL" to "SELECT COUNT(*) FROM postgres_db.public.customer",
                    "MySQL" to "SELECT COUNT(*) FROM mysql_db.db1.customer",
                    "MariaDB" to "SELECT COUNT(*) FROM mariadb_db.db1.customer"
                )

                for ((dbName, query) in queries) {
                    try {
                        val resultSet = statement.executeQuery(query)
                        if (resultSet.next()) {
                            val count = resultSet.getLong(1)
                            println("$dbName customers: ${String.format("%,d", count)}")
                        }
                    } catch (e: SQLException) {
                        println("$dbName: Error - ${e.message}")
                    }
                }
            }

            // Test cross-database join
            println("\n5. Testing cross-database join...")
            connection.createStatement().use { statement ->
                val query = """
                    SELECT
                        p.c_name as postgres_customer,
                        m.c_name as mysql_customer,
                        p.c_custkey
                    FROM postgres_db.public.customer p
                    JOIN mysql_db.db1.customer m ON p.c_custkey = m.c_custkey
                    WHERE p.c_custkey <= 5
                    ORDER BY p.c_custkey
                    LIMIT 3
                """.trimIndent()

                try {
                    val resultSet = statement.executeQuery(query)
                    println("Cross-database join results:")
                    while (resultSet.next()) {
                        val custKey = resultSet.getInt("c_custkey")
                        val pgCustomer = resultSet.getString("postgres_customer")
                        val mysqlCustomer = resultSet.getString("mysql_customer")
                        println("  Customer $custKey: PG='$pgCustomer' | MySQL='$mysqlCustomer'")
                    }
                } catch (e: SQLException) {
                    println("Cross-database join failed: ${e.message}")
                }
            }

            // Test PreparedStatement
            println("\n6. Testing PreparedStatement...")
            val preparedQuery = """
                SELECT c_name, c_acctbal
                FROM postgres_db.public.customer
                WHERE c_custkey = ?
                ORDER BY c_custkey
            """.trimIndent()

            connection.prepareStatement(preparedQuery).use { preparedStatement ->
                preparedStatement.setInt(1, 1)
                val resultSet = preparedStatement.executeQuery()
                if (resultSet.next()) {
                    val name = resultSet.getString("c_name")
                    val balance = resultSet.getDouble("c_acctbal")
                    println("Customer 1: $name (Balance: $${String.format("%.2f", balance)})")
                }
            }

            // Test metadata
            println("\n7. Testing metadata...")
            val metaData = connection.metaData
            println("Driver: ${metaData.driverName} v${metaData.driverVersion}")
            println("Database: ${metaData.databaseProductName} v${metaData.databaseProductVersion}")
            println("JDBC: ${metaData.jdbcMajorVersion}.${metaData.jdbcMinorVersion}")

            println("\n✓ All tests completed successfully!")
        }

    } catch (e: SQLException) {
        println("✗ SQL Error: ${e.message}")
        e.printStackTrace()
    } catch (e: Exception) {
        println("✗ Error: ${e.message}")
        e.printStackTrace()
    }
}

/**
 * Example function showing basic connection usage
 */
fun basicExample() {
    val url = "jdbc:duckdb-fqe://localhost:8080/"

    DriverManager.getConnection(url).use { connection ->
        connection.createStatement().use { statement ->
            val resultSet = statement.executeQuery("SELECT version()")
            if (resultSet.next()) {
                println("DuckDB Version: ${resultSet.getString(1)}")
            }
        }
    }
}

/**
 * Example function showing prepared statement usage
 */
fun preparedStatementExample() {
    val url = "jdbc:duckdb-fqe://localhost:8080/"

    DriverManager.getConnection(url).use { connection ->
        val sql = "SELECT c_name FROM postgres_db.public.customer WHERE c_custkey = ?"

        connection.prepareStatement(sql).use { preparedStatement ->
            preparedStatement.setInt(1, 42)
            val resultSet = preparedStatement.executeQuery()

            while (resultSet.next()) {
                println("Customer: ${resultSet.getString("c_name")}")
            }
        }
    }
}