import java.sql.DriverManager

/**
 * Quick standalone test to verify the DuckDB FQE JDBC driver works
 *
 * Run with: kotlinc QuickTest.kt -include-runtime -d QuickTest.jar && java -jar QuickTest.jar
 * Or compile and run from IntelliJ/IDE
 */
fun main() {
    println("=== DuckDB FQE JDBC Driver Quick Test ===\n")

    val url = "jdbc:duckdb-fqe://localhost:8082/"

    try {
        // Test 1: Basic connection
        println("Test 1: Connecting to $url")
        DriverManager.getConnection(url).use { conn ->
            println("✓ Connected successfully\n")

            // Test 2: Simple query
            println("Test 2: Simple query")
            conn.createStatement().use { stmt ->
                val rs = stmt.executeQuery("SELECT 'Hello from DuckDB FQE!' as message")
                if (rs.next()) {
                    val message = rs.getString("message")
                    println("✓ Query result: $message\n")
                }
            }

            // Test 3: Show databases
            println("Test 3: Show databases")
            conn.createStatement().use { stmt ->
                val rs = stmt.executeQuery("SHOW DATABASES")
                print("✓ Available databases: ")
                val databases = mutableListOf<String>()
                while (rs.next()) {
                    databases.add(rs.getString(1))
                }
                println(databases.joinToString(", ") + "\n")
            }

            // Test 4: Count customers
            println("Test 4: Count customers from PostgreSQL")
            conn.createStatement().use { stmt ->
                val rs = stmt.executeQuery("SELECT COUNT(*) as count FROM postgres.public.customer")
                if (rs.next()) {
                    val count = rs.getLong("count")
                    println("✓ PostgreSQL customers: ${"%,d".format(count)}\n")
                }
            }

            // Test 5: Cross-database join
            println("Test 5: Cross-database join")
            conn.createStatement().use { stmt ->
                val rs = stmt.executeQuery("""
                    SELECT
                        CONCAT(p.c_first_name, ' ', p.c_last_name) as name,
                        p.c_customer_sk
                    FROM postgres.public.customer p
                    JOIN mysql.db1.customer m ON p.c_customer_sk = m.c_customer_sk
                    LIMIT 3
                """.trimIndent())

                var count = 0
                while (rs.next()) {
                    count++
                    val name = rs.getString("name")
                    val sk = rs.getInt("c_customer_sk")
                    println("  Customer $sk: $name")
                }
                println("✓ Joined $count records\n")
            }

            // Test 6: Complex TPC-DS query
            println("Test 6: Complex TPC-DS federated query with CTE")
            conn.createStatement().use { stmt ->
                val startTime = System.currentTimeMillis()
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
                """.trimIndent())

                var count = 0
                while (rs.next()) {
                    count++
                    if (count <= 3) {
                        println("  Customer ID: ${rs.getString("c_customer_id")}")
                    }
                }
                val elapsed = System.currentTimeMillis() - startTime
                println("✓ Query returned $count results in ${elapsed}ms")
                println("  (Joins MySQL store_returns + MariaDB date_dim + PostgreSQL store/customer)\n")
            }
        }

        println("🎉 All tests passed!")
        println("\n=== Summary ===")
        println("✓ Connection works")
        println("✓ Simple queries work")
        println("✓ Cross-database queries work")
        println("✓ Complex TPC-DS queries work")
        println("✓ JDBC driver is functioning correctly!")

    } catch (e: Exception) {
        println("❌ Error: ${e.message}")
        e.printStackTrace()
        System.exit(1)
    }
}
