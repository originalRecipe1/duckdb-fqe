package com.duckdb.fqe.jdbc

import io.mockk.*
import okhttp3.*
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.DriverManager
import java.sql.SQLException
import kotlin.test.*

class DuckDBFQEDriverTest {

    private lateinit var mockServer: MockWebServer
    private lateinit var driver: DuckDBFQEDriver

    @BeforeEach
    fun setUp() {
        mockServer = MockWebServer()
        mockServer.start()
        driver = DuckDBFQEDriver()
    }

    @AfterEach
    fun tearDown() {
        mockServer.shutdown()
    }

    @Test
    fun `driver accepts valid URLs`() {
        assertTrue(driver.acceptsURL("jdbc:duckdb-fqe://localhost:8080/"))
        assertTrue(driver.acceptsURL("jdbc:duckdb-fqe://example.com:9090/test"))
        assertTrue(driver.acceptsURL("jdbc:duckdb-fqe://127.0.0.1:3000/?user=test"))
    }

    @Test
    fun `driver rejects invalid URLs`() {
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost:5432/"))
        assertFalse(driver.acceptsURL("jdbc:mysql://localhost:3306/"))
        assertFalse(driver.acceptsURL("invalid-url"))
        assertFalse(driver.acceptsURL(null))
    }

    @Test
    fun `driver returns null for invalid URLs`() {
        assertNull(driver.connect("jdbc:postgresql://localhost:5432/", null))
        assertNull(driver.connect("invalid-url", null))
    }

    @Test
    fun `driver provides correct version information`() {
        assertEquals(1, driver.majorVersion)
        assertEquals(0, driver.minorVersion)
        assertFalse(driver.jdbcCompliant())
    }

    @Test
    fun `driver provides property info`() {
        val properties = driver.getPropertyInfo("jdbc:duckdb-fqe://localhost:8080/", null)

        assertTrue(properties.any { it.name == "user" })
        assertTrue(properties.any { it.name == "password" })
        assertTrue(properties.any { it.name == "timeout" })
        assertTrue(properties.any { it.name == "ssl" })
    }

    @Test
    fun `connection creation with mock server`() {
        // Mock successful health check
        mockServer.enqueue(MockResponse().setResponseCode(200).setBody("OK"))

        val url = "jdbc:duckdb-fqe://localhost:${mockServer.port}/"

        assertNotNull(driver.connect(url, null))
    }

    @Test
    fun `connection failure with unreachable server`() {
        val url = "jdbc:duckdb-fqe://localhost:99999/"

        assertFailsWith<SQLException> {
            driver.connect(url, null)
        }
    }

    @Test
    fun `URL parsing with various formats`() {
        // This tests the internal URL parsing logic indirectly
        val validUrls = listOf(
            "jdbc:duckdb-fqe://localhost:8080/",
            "jdbc:duckdb-fqe://example.com:9090/database",
            "jdbc:duckdb-fqe://127.0.0.1:3000/?user=test&password=secret",
            "jdbc:duckdb-fqe://host:1234/db?ssl=true&timeout=60"
        )

        for (url in validUrls) {
            assertTrue(driver.acceptsURL(url), "Should accept URL: $url")
        }
    }

    @Test
    fun `driver manager integration`() {
        // Test that the driver is registered with DriverManager
        val drivers = DriverManager.getDrivers().toList()
        assertTrue(drivers.any { it is DuckDBFQEDriver })
    }
}

class DuckDBFQEConnectionTest {

    private lateinit var mockServer: MockWebServer

    @BeforeEach
    fun setUp() {
        mockServer = MockWebServer()
        mockServer.start()
    }

    @AfterEach
    fun tearDown() {
        mockServer.shutdown()
    }

    @Test
    fun `connection basic operations`() {
        // Mock health check
        mockServer.enqueue(MockResponse().setResponseCode(200).setBody("OK"))

        val url = "jdbc:duckdb-fqe://localhost:${mockServer.port}/"
        val connection = DriverManager.getConnection(url)

        assertNotNull(connection)
        assertFalse(connection.isClosed)
        assertTrue(connection.autoCommit)

        connection.close()
        assertTrue(connection.isClosed)
    }

    @Test
    fun `connection metadata`() {
        mockServer.enqueue(MockResponse().setResponseCode(200).setBody("OK"))

        val url = "jdbc:duckdb-fqe://localhost:${mockServer.port}/"
        val connection = DriverManager.getConnection(url)

        val metaData = connection.metaData
        assertNotNull(metaData)
        assertEquals("DuckDB FQE JDBC Driver", metaData.driverName)
        assertEquals("DuckDB Federated Query Engine", metaData.databaseProductName)

        connection.close()
    }

    @Test
    fun `statement creation`() {
        mockServer.enqueue(MockResponse().setResponseCode(200).setBody("OK"))

        val url = "jdbc:duckdb-fqe://localhost:${mockServer.port}/"
        val connection = DriverManager.getConnection(url)

        val statement = connection.createStatement()
        assertNotNull(statement)
        assertFalse(statement.isClosed)

        val preparedStatement = connection.prepareStatement("SELECT ?")
        assertNotNull(preparedStatement)

        connection.close()
    }
}

class DuckDBFQEStatementTest {

    private lateinit var mockServer: MockWebServer

    @BeforeEach
    fun setUp() {
        mockServer = MockWebServer()
        mockServer.start()
    }

    @AfterEach
    fun tearDown() {
        mockServer.shutdown()
    }

    @Test
    fun `statement query execution`() {
        // Mock health check
        mockServer.enqueue(MockResponse().setResponseCode(200).setBody("OK"))

        // Mock query response
        val jsonResponse = """
            {
                "meta": [{"name": "message", "type": "VARCHAR"}],
                "data": [["Hello World"]],
                "rows": 1,
                "statistics": {"elapsed": 0.001, "rows_read": 0, "bytes_read": 0}
            }
        """.trimIndent()
        mockServer.enqueue(MockResponse().setResponseCode(200).setBody(jsonResponse))

        val url = "jdbc:duckdb-fqe://localhost:${mockServer.port}/"
        val connection = DriverManager.getConnection(url)
        val statement = connection.createStatement()

        val resultSet = statement.executeQuery("SELECT 'Hello World' as message")
        assertNotNull(resultSet)
        assertTrue(resultSet.next())
        assertEquals("Hello World", resultSet.getString("message"))
        assertFalse(resultSet.next())

        connection.close()
    }

    @Test
    fun `statement update execution`() {
        mockServer.enqueue(MockResponse().setResponseCode(200).setBody("OK"))

        // Mock proper DuckDB DDL response format
        val ddlResponse = """
            {
                "meta": [{"name": "Count", "type": "BIGINT"}],
                "data": [],
                "rows": 0,
                "statistics": {"elapsed": 0.0, "rows_read": 0, "bytes_read": 0}
            }
        """.trimIndent()
        mockServer.enqueue(MockResponse().setResponseCode(200).setBody(ddlResponse))

        val url = "jdbc:duckdb-fqe://localhost:${mockServer.port}/"
        val connection = DriverManager.getConnection(url)
        val statement = connection.createStatement()

        val updateCount = statement.executeUpdate("CREATE TABLE test (id INTEGER)")
        assertEquals(0, updateCount) // DuckDB FQE doesn't provide update counts

        connection.close()
    }

    @Test
    fun `prepared statement parameter setting`() {
        mockServer.enqueue(MockResponse().setResponseCode(200).setBody("OK"))

        val jsonResponse = """
            {
                "meta": [{"name": "result", "type": "INTEGER"}],
                "data": [[42]],
                "rows": 1,
                "statistics": {"elapsed": 0.001, "rows_read": 0, "bytes_read": 0}
            }
        """.trimIndent()
        mockServer.enqueue(MockResponse().setResponseCode(200).setBody(jsonResponse))

        val url = "jdbc:duckdb-fqe://localhost:${mockServer.port}/"
        val connection = DriverManager.getConnection(url)
        val preparedStatement = connection.prepareStatement("SELECT ? as result")

        preparedStatement.setInt(1, 42)
        val resultSet = preparedStatement.executeQuery()

        assertTrue(resultSet.next())
        assertEquals(42, resultSet.getInt("result"))

        connection.close()
    }
}