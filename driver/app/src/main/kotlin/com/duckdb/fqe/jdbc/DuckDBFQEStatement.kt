package com.duckdb.fqe.jdbc

import org.slf4j.LoggerFactory
import java.sql.*

/**
 * JDBC Statement implementation for DuckDB FQE
 */
open class DuckDBFQEStatement(
    private val connection: DuckDBFQEConnection,
    private val httpClient: DuckDBFQEHttpClient,
    private val resultSetType: Int = ResultSet.TYPE_FORWARD_ONLY,
    private val resultSetConcurrency: Int = ResultSet.CONCUR_READ_ONLY,
    private val resultSetHoldability: Int = ResultSet.HOLD_CURSORS_OVER_COMMIT
) : Statement {

    private val logger = LoggerFactory.getLogger(DuckDBFQEStatement::class.java)

    @Volatile
    private var closed = false

    @Volatile
    private var cancelled = false

    private var currentResultSet: ResultSet? = null
    private var updateCount = -1
    private var maxRows = 0
    private var queryTimeout = 0
    private var fetchSize = 0
    private var fetchDirection = ResultSet.FETCH_FORWARD

    override fun executeQuery(sql: String?): ResultSet {
        checkClosed()
        if (sql.isNullOrBlank()) {
            throw SQLException("SQL query cannot be null or empty")
        }

        logger.debug("Executing query: {}", sql)

        try {
            val result = httpClient.executeQuery(sql)
            currentResultSet = DuckDBFQEResultSet(this, result)
            updateCount = -1
            return currentResultSet!!
        } catch (e: Exception) {
            logger.error("Failed to execute query: {}", sql, e)
            throw SQLException("Query execution failed: ${e.message}", e)
        }
    }

    override fun executeUpdate(sql: String?): Int {
        checkClosed()
        if (sql.isNullOrBlank()) {
            throw SQLException("SQL statement cannot be null or empty")
        }

        logger.debug("Executing update: {}", sql)

        try {
            updateCount = httpClient.executeUpdate(sql)
            currentResultSet = null
            return updateCount
        } catch (e: Exception) {
            logger.error("Failed to execute update: {}", sql, e)
            throw SQLException("Update execution failed: ${e.message}", e)
        }
    }

    override fun close() {
        if (!closed) {
            closed = true
            currentResultSet?.close()
            currentResultSet = null
            logger.debug("Statement closed")
        }
    }

    override fun getMaxFieldSize(): Int = 0

    override fun setMaxFieldSize(max: Int) {
        checkClosed()
        // No-op
    }

    override fun getMaxRows(): Int {
        checkClosed()
        return maxRows
    }

    override fun setMaxRows(max: Int) {
        checkClosed()
        this.maxRows = max
    }

    override fun setEscapeProcessing(enable: Boolean) {
        checkClosed()
        // No-op
    }

    override fun getQueryTimeout(): Int {
        checkClosed()
        return queryTimeout
    }

    override fun setQueryTimeout(seconds: Int) {
        checkClosed()
        this.queryTimeout = seconds
    }

    override fun cancel() {
        cancelled = true
        // DuckDB FQE doesn't support query cancellation yet
        logger.debug("Statement cancellation requested (not supported)")
    }

    override fun getWarnings(): SQLWarning? {
        checkClosed()
        return null
    }

    override fun clearWarnings() {
        checkClosed()
        // No-op
    }

    override fun setCursorName(name: String?) {
        checkClosed()
        throw SQLFeatureNotSupportedException("Named cursors not supported")
    }

    override fun execute(sql: String?): Boolean {
        checkClosed()
        if (sql.isNullOrBlank()) {
            throw SQLException("SQL statement cannot be null or empty")
        }

        logger.debug("Executing statement: {}", sql)

        return try {
            val trimmedSql = sql.trim().uppercase()
            if (trimmedSql.startsWith("SELECT") ||
                trimmedSql.startsWith("SHOW") ||
                trimmedSql.startsWith("DESCRIBE") ||
                trimmedSql.startsWith("EXPLAIN")
            ) {
                // Query statement
                executeQuery(sql)
                true
            } else {
                // Update statement
                executeUpdate(sql)
                false
            }
        } catch (e: Exception) {
            logger.error("Failed to execute statement: {}", sql, e)
            throw SQLException("Statement execution failed: ${e.message}", e)
        }
    }

    override fun getResultSet(): ResultSet? {
        checkClosed()
        return currentResultSet
    }

    override fun getUpdateCount(): Int {
        checkClosed()
        return updateCount
    }

    override fun getMoreResults(): Boolean {
        checkClosed()
        currentResultSet?.close()
        currentResultSet = null
        updateCount = -1
        return false
    }

    override fun setFetchDirection(direction: Int) {
        checkClosed()
        if (direction != ResultSet.FETCH_FORWARD &&
            direction != ResultSet.FETCH_REVERSE &&
            direction != ResultSet.FETCH_UNKNOWN
        ) {
            throw SQLException("Invalid fetch direction: $direction")
        }
        this.fetchDirection = direction
    }

    override fun getFetchDirection(): Int {
        checkClosed()
        return fetchDirection
    }

    override fun setFetchSize(rows: Int) {
        checkClosed()
        if (rows < 0) {
            throw SQLException("Fetch size cannot be negative")
        }
        this.fetchSize = rows
    }

    override fun getFetchSize(): Int {
        checkClosed()
        return fetchSize
    }

    override fun getResultSetConcurrency(): Int {
        checkClosed()
        return resultSetConcurrency
    }

    override fun getResultSetType(): Int {
        checkClosed()
        return resultSetType
    }

    override fun addBatch(sql: String?) {
        throw SQLFeatureNotSupportedException("Batch operations not supported")
    }

    override fun clearBatch() {
        throw SQLFeatureNotSupportedException("Batch operations not supported")
    }

    override fun executeBatch(): IntArray {
        throw SQLFeatureNotSupportedException("Batch operations not supported")
    }

    override fun getConnection(): Connection {
        checkClosed()
        return connection
    }

    override fun getMoreResults(current: Int): Boolean {
        checkClosed()
        currentResultSet?.close()
        currentResultSet = null
        updateCount = -1
        return false
    }

    override fun getGeneratedKeys(): ResultSet {
        throw SQLFeatureNotSupportedException("Generated keys not supported")
    }

    override fun executeUpdate(sql: String?, autoGeneratedKeys: Int): Int {
        return executeUpdate(sql)
    }

    override fun executeUpdate(sql: String?, columnIndexes: IntArray?): Int {
        return executeUpdate(sql)
    }

    override fun executeUpdate(sql: String?, columnNames: Array<out String>?): Int {
        return executeUpdate(sql)
    }

    override fun execute(sql: String?, autoGeneratedKeys: Int): Boolean {
        return execute(sql)
    }

    override fun execute(sql: String?, columnIndexes: IntArray?): Boolean {
        return execute(sql)
    }

    override fun execute(sql: String?, columnNames: Array<out String>?): Boolean {
        return execute(sql)
    }

    override fun getResultSetHoldability(): Int {
        checkClosed()
        return resultSetHoldability
    }

    override fun isClosed(): Boolean = closed

    override fun setPoolable(poolable: Boolean) {
        checkClosed()
        // No-op
    }

    override fun isPoolable(): Boolean {
        checkClosed()
        return false
    }

    override fun closeOnCompletion() {
        checkClosed()
        // No-op
    }

    override fun isCloseOnCompletion(): Boolean {
        checkClosed()
        return false
    }

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        if (iface?.isInstance(this) == true) {
            @Suppress("UNCHECKED_CAST")
            return this as T
        }
        throw SQLException("Cannot unwrap to ${iface?.name}")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        return iface?.isInstance(this) == true
    }

    private fun checkClosed() {
        if (closed) {
            throw SQLException("Statement is closed")
        }
    }
}