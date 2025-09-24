package com.duckdb.fqe.jdbc

import org.slf4j.LoggerFactory
import java.sql.*
import java.util.*

/**
 * JDBC Driver for DuckDB Federated Query Engine
 *
 * This driver connects to a DuckDB FQE instance via HTTP and provides
 * standard JDBC interfaces for executing SQL queries across federated databases.
 *
 * URL Format: jdbc:duckdb-fqe://host:port[/database][?property1=value1&property2=value2]
 *
 * Example: jdbc:duckdb-fqe://localhost:8080/?user=admin&password=secret
 */
class DuckDBFQEDriver : Driver {
    companion object {
        const val DRIVER_NAME = "DuckDB FQE JDBC Driver"
        const val DRIVER_VERSION = "1.0.0"
        const val MAJOR_VERSION = 1
        const val MINOR_VERSION = 0
        const val URL_PREFIX = "jdbc:duckdb-fqe://"

        private val logger = LoggerFactory.getLogger(DuckDBFQEDriver::class.java)

        init {
            try {
                DriverManager.registerDriver(DuckDBFQEDriver())
                logger.info("DuckDB FQE JDBC Driver registered successfully")
            } catch (e: SQLException) {
                logger.error("Failed to register DuckDB FQE JDBC Driver", e)
                throw RuntimeException("Failed to register driver", e)
            }
        }
    }

    override fun connect(url: String?, info: Properties?): Connection? {
        if (!acceptsURL(url)) {
            return null
        }

        logger.debug("Attempting to connect to: {}", url)

        try {
            val connectionInfo = parseUrl(url!!)
            val properties = Properties().apply {
                info?.let { putAll(it) }
                putAll(connectionInfo.properties)
            }

            return DuckDBFQEConnection(connectionInfo, properties)
        } catch (e: Exception) {
            logger.error("Failed to create connection to: {}", url, e)
            throw SQLException("Failed to connect to $url", e)
        }
    }

    override fun acceptsURL(url: String?): Boolean {
        return url?.startsWith(URL_PREFIX) == true
    }

    override fun getPropertyInfo(url: String?, info: Properties?): Array<DriverPropertyInfo> {
        val properties = mutableListOf<DriverPropertyInfo>()

        properties.add(DriverPropertyInfo("user", info?.getProperty("user")).apply {
            description = "Username for authentication"
            required = false
        })

        properties.add(DriverPropertyInfo("password", info?.getProperty("password")).apply {
            description = "Password for authentication"
            required = false
        })

        properties.add(DriverPropertyInfo("timeout", info?.getProperty("timeout", "30")).apply {
            description = "Connection timeout in seconds"
            required = false
        })

        properties.add(DriverPropertyInfo("ssl", info?.getProperty("ssl", "false")).apply {
            description = "Use SSL connection"
            required = false
            choices = arrayOf("true", "false")
        })

        return properties.toTypedArray()
    }

    override fun getMajorVersion(): Int = MAJOR_VERSION

    override fun getMinorVersion(): Int = MINOR_VERSION

    override fun jdbcCompliant(): Boolean = false // Not fully compliant yet

    override fun getParentLogger(): java.util.logging.Logger {
        return java.util.logging.Logger.getLogger(DuckDBFQEDriver::class.java.name)
    }

    /**
     * Parse JDBC URL and extract connection information
     */
    private fun parseUrl(url: String): ConnectionInfo {
        if (!url.startsWith(URL_PREFIX)) {
            throw SQLException("Invalid URL format. Expected: $URL_PREFIX")
        }

        val urlWithoutPrefix = url.substring(URL_PREFIX.length)
        val parts = urlWithoutPrefix.split("?", limit = 2)
        val hostPart = parts[0]
        val queryPart = if (parts.size > 1) parts[1] else ""

        // Parse host:port/database
        val hostPortDb = hostPart.split("/", limit = 2)
        val hostPort = hostPortDb[0]
        val database = if (hostPortDb.size > 1) hostPortDb[1] else ""

        // Parse host and port
        val hostPortParts = hostPort.split(":", limit = 2)
        val host = hostPortParts[0].ifEmpty { "localhost" }
        val port = if (hostPortParts.size > 1) hostPortParts[1].toIntOrNull() ?: 8080 else 8080

        // Parse query parameters
        val properties = Properties()
        queryPart.split("&").forEach { param ->
            val keyValue = param.split("=", limit = 2)
            if (keyValue.size == 2) {
                properties[keyValue[0]] = keyValue[1]
            }
        }

        return ConnectionInfo(host, port, database, properties)
    }
}

/**
 * Data class to hold connection information parsed from JDBC URL
 */
data class ConnectionInfo(
    val host: String,
    val port: Int,
    val database: String,
    val properties: Properties
)