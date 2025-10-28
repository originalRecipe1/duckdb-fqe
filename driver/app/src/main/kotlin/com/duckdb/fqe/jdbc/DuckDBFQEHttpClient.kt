package com.duckdb.fqe.jdbc

import com.squareup.moshi.Json
import com.squareup.moshi.JsonClass
import com.squareup.moshi.Moshi
import com.squareup.moshi.kotlin.reflect.KotlinJsonAdapterFactory
import okhttp3.*
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import java.io.IOException
import java.sql.SQLException
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * HTTP client for communicating with DuckDB FQE
 */
class DuckDBFQEHttpClient(
    private val connectionInfo: ConnectionInfo,
    private val properties: Properties
) {
    private val logger = LoggerFactory.getLogger(DuckDBFQEHttpClient::class.java)

    private val client: OkHttpClient
    private val moshi: Moshi
    private val baseUrl: String

    init {
        val timeout = properties.getProperty("timeout", "30").toLongOrNull() ?: 30L

        client = OkHttpClient.Builder()
            .connectTimeout(timeout, TimeUnit.SECONDS)
            .readTimeout(timeout, TimeUnit.SECONDS)
            .writeTimeout(timeout, TimeUnit.SECONDS)
            .build()

        moshi = Moshi.Builder()
            .add(KotlinJsonAdapterFactory())
            .build()

        baseUrl = buildBaseUrl()
        logger.debug("HTTP client initialized for: {}", baseUrl)
    }

    /**
     * Test connection to DuckDB FQE
     */
    fun testConnection() {
        val request = Request.Builder()
            .url(baseUrl)
            .get()
            .build()

        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                throw SQLException("Connection test failed: HTTP ${response.code}")
            }
        }
    }

    /**
     * Execute SQL query and return result
     */
    fun executeQuery(sql: String): QueryResult {
        logger.debug("Executing query: {}", sql)

        val queryUrl = "$baseUrl?add_http_cors_header=1&default_format=JSONCompact&max_result_rows=10000"

        val requestBody = sql.toRequestBody("text/plain".toMediaType())

        val request = Request.Builder()
            .url(queryUrl)
            .post(requestBody)
            .apply {
                // Add authentication if provided
                val user = properties.getProperty("user")
                val password = properties.getProperty("password")
                if (user != null && password != null) {
                    val credentials = Credentials.basic(user, password)
                    header("Authorization", credentials)
                }
            }
            .build()

        try {
            client.newCall(request).execute().use { response ->
                val responseBody = response.body?.string() ?: ""

                if (!response.isSuccessful) {
                    val errorMessage = "Query failed: HTTP ${response.code} - $responseBody"
                    logger.error(errorMessage)
                    throw SQLException(errorMessage)
                }

                return parseQueryResult(responseBody)
            }
        } catch (e: IOException) {
            logger.error("Network error executing query", e)
            throw SQLException("Network error: ${e.message}", e)
        }
    }

    /**
     * Execute update/DDL statement
     */
    fun executeUpdate(sql: String): Int {
        logger.debug("Executing update: {}", sql)

        val queryUrl = "$baseUrl?add_http_cors_header=1&default_format=JSONCompact&max_result_rows=10000"
        val requestBody = sql.toRequestBody("text/plain".toMediaType())

        val request = Request.Builder()
            .url(queryUrl)
            .post(requestBody)
            .apply {
                // Add authentication if provided
                val user = properties.getProperty("user")
                val password = properties.getProperty("password")
                if (user != null && password != null) {
                    val credentials = Credentials.basic(user, password)
                    header("Authorization", credentials)
                }
            }
            .build()

        return try {
            client.newCall(request).execute().use { response ->
                val responseBody = response.body?.string() ?: ""

                if (!response.isSuccessful) {
                    val errorMessage = "Update failed: HTTP ${response.code} - $responseBody"
                    logger.error(errorMessage)
                    throw SQLException(errorMessage)
                }

                // Parse the response to validate success
                val result = parseQueryResult(responseBody)

                // For DDL statements, DuckDB returns a result with empty data but valid metadata
                // Return 0 as update count since DuckDB doesn't provide row counts for DDL
                return 0
            }
        } catch (e: IOException) {
            logger.error("Network error executing update: {}", sql, e)
            throw SQLException("Network error: ${e.message}", e)
        }
    }

    /**
     * Close the HTTP client
     */
    fun close() {
        client.dispatcher.executorService.shutdown()
        client.connectionPool.evictAll()
    }

    private fun buildBaseUrl(): String {
        val protocol = if (properties.getProperty("ssl", "false").toBoolean()) "https" else "http"
        return "$protocol://${connectionInfo.host}:${connectionInfo.port}/"
    }

    private fun parseQueryResult(responseBody: String): QueryResult {
        return try {
            val adapter = moshi.adapter(QueryResult::class.java)
            adapter.fromJson(responseBody) ?: QueryResult(
                meta = emptyList(),
                data = emptyList(),
                rows = 0,
                statistics = QueryStatistics(0.0, 0, 0)
            )
        } catch (e: Exception) {
            logger.error("Failed to parse query result: {}", responseBody, e)
            throw SQLException("Failed to parse query result: ${e.message}", e)
        }
    }
}

/**
 * Data classes for DuckDB FQE JSON response format
 */
@JsonClass(generateAdapter = true)
data class QueryResult(
    val meta: List<ColumnMetadata>,
    val data: List<List<Any?>>,
    val rows: Int,
    val statistics: QueryStatistics
)

@JsonClass(generateAdapter = true)
data class ColumnMetadata(
    val name: String,
    val type: String
)

@JsonClass(generateAdapter = true)
data class QueryStatistics(
    val elapsed: Double,
    @field:Json(name = "rows_read") val rowsRead: Long? = 0,
    @field:Json(name = "bytes_read") val bytesRead: Long? = 0
)