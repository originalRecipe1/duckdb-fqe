package com.duckdb.fqe.jdbc

import org.slf4j.LoggerFactory
import java.sql.*
import java.util.*
import java.util.concurrent.Executor

/**
 * JDBC Connection implementation for DuckDB FQE
 */
class DuckDBFQEConnection(
    private val connectionInfo: ConnectionInfo,
    private val properties: Properties
) : Connection {

    private val logger = LoggerFactory.getLogger(DuckDBFQEConnection::class.java)
    private val httpClient = DuckDBFQEHttpClient(connectionInfo, properties)

    @Volatile
    private var closed = false

    @Volatile
    private var autoCommit = true

    @Volatile
    private var readOnly = false

    private var transactionIsolation = Connection.TRANSACTION_NONE
    private var clientInfo = Properties()

    init {
        logger.debug("Creating connection to {}:{}", connectionInfo.host, connectionInfo.port)

        // Test connection
        try {
            httpClient.testConnection()
            logger.info("Successfully connected to DuckDB FQE at {}:{}", connectionInfo.host, connectionInfo.port)
        } catch (e: Exception) {
            logger.error("Failed to connect to DuckDB FQE", e)
            throw SQLException("Failed to connect to DuckDB FQE", e)
        }
    }

    override fun createStatement(): Statement {
        checkClosed()
        return DuckDBFQEStatement(this, httpClient)
    }

    override fun prepareStatement(sql: String?): PreparedStatement {
        checkClosed()
        return DuckDBFQEPreparedStatement(this, httpClient, sql ?: "")
    }

    override fun prepareCall(sql: String?): CallableStatement {
        throw SQLFeatureNotSupportedException("CallableStatement not supported")
    }

    override fun nativeSQL(sql: String?): String {
        checkClosed()
        return sql ?: ""
    }

    override fun setAutoCommit(autoCommit: Boolean) {
        checkClosed()
        this.autoCommit = autoCommit
        logger.debug("Auto-commit set to: {}", autoCommit)
    }

    override fun getAutoCommit(): Boolean {
        checkClosed()
        return autoCommit
    }

    override fun commit() {
        checkClosed()
        if (autoCommit) {
            throw SQLException("Cannot commit when auto-commit is enabled")
        }
        // DuckDB FQE doesn't support transactions yet
        logger.debug("Commit called (no-op)")
    }

    override fun rollback() {
        checkClosed()
        if (autoCommit) {
            throw SQLException("Cannot rollback when auto-commit is enabled")
        }
        // DuckDB FQE doesn't support transactions yet
        logger.debug("Rollback called (no-op)")
    }

    override fun close() {
        if (!closed) {
            closed = true
            httpClient.close()
            logger.debug("Connection closed")
        }
    }

    override fun isClosed(): Boolean = closed

    override fun getMetaData(): DatabaseMetaData {
        checkClosed()
        return DuckDBFQEDatabaseMetaData(this)
    }

    override fun setReadOnly(readOnly: Boolean) {
        checkClosed()
        this.readOnly = readOnly
        logger.debug("Read-only set to: {}", readOnly)
    }

    override fun isReadOnly(): Boolean {
        checkClosed()
        return readOnly
    }

    override fun setCatalog(catalog: String?) {
        checkClosed()
        // DuckDB FQE uses database attachments instead of catalogs
        logger.debug("Set catalog called with: {} (no-op)", catalog)
    }

    override fun getCatalog(): String? {
        checkClosed()
        return connectionInfo.database.ifEmpty { null }
    }

    override fun setTransactionIsolation(level: Int) {
        checkClosed()
        this.transactionIsolation = level
        logger.debug("Transaction isolation set to: {}", level)
    }

    override fun getTransactionIsolation(): Int {
        checkClosed()
        return transactionIsolation
    }

    override fun getWarnings(): SQLWarning? {
        checkClosed()
        return null
    }

    override fun clearWarnings() {
        checkClosed()
        // No-op
    }

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement {
        checkClosed()
        return DuckDBFQEStatement(this, httpClient, resultSetType, resultSetConcurrency)
    }

    override fun prepareStatement(sql: String?, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement {
        checkClosed()
        return DuckDBFQEPreparedStatement(this, httpClient, sql ?: "", resultSetType, resultSetConcurrency)
    }

    override fun prepareCall(sql: String?, resultSetType: Int, resultSetConcurrency: Int): CallableStatement {
        throw SQLFeatureNotSupportedException("CallableStatement not supported")
    }

    override fun getTypeMap(): MutableMap<String, Class<*>> {
        checkClosed()
        return mutableMapOf()
    }

    override fun setTypeMap(map: MutableMap<String, Class<*>>?) {
        checkClosed()
        // No-op
    }

    override fun setHoldability(holdability: Int) {
        checkClosed()
        // No-op
    }

    override fun getHoldability(): Int {
        checkClosed()
        return ResultSet.HOLD_CURSORS_OVER_COMMIT
    }

    override fun setSavepoint(): Savepoint {
        throw SQLFeatureNotSupportedException("Savepoints not supported")
    }

    override fun setSavepoint(name: String?): Savepoint {
        throw SQLFeatureNotSupportedException("Savepoints not supported")
    }

    override fun rollback(savepoint: Savepoint?) {
        throw SQLFeatureNotSupportedException("Savepoints not supported")
    }

    override fun releaseSavepoint(savepoint: Savepoint?) {
        throw SQLFeatureNotSupportedException("Savepoints not supported")
    }

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement {
        checkClosed()
        return DuckDBFQEStatement(this, httpClient, resultSetType, resultSetConcurrency, resultSetHoldability)
    }

    override fun prepareStatement(sql: String?, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): PreparedStatement {
        checkClosed()
        return DuckDBFQEPreparedStatement(this, httpClient, sql ?: "", resultSetType, resultSetConcurrency, resultSetHoldability)
    }

    override fun prepareCall(sql: String?, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): CallableStatement {
        throw SQLFeatureNotSupportedException("CallableStatement not supported")
    }

    override fun prepareStatement(sql: String?, autoGeneratedKeys: Int): PreparedStatement {
        checkClosed()
        return DuckDBFQEPreparedStatement(this, httpClient, sql ?: "")
    }

    override fun prepareStatement(sql: String?, columnIndexes: IntArray?): PreparedStatement {
        checkClosed()
        return DuckDBFQEPreparedStatement(this, httpClient, sql ?: "")
    }

    override fun prepareStatement(sql: String?, columnNames: Array<out String>?): PreparedStatement {
        checkClosed()
        return DuckDBFQEPreparedStatement(this, httpClient, sql ?: "")
    }

    override fun createClob(): Clob {
        throw SQLFeatureNotSupportedException("Clob not supported")
    }

    override fun createBlob(): Blob {
        throw SQLFeatureNotSupportedException("Blob not supported")
    }

    override fun createNClob(): NClob {
        throw SQLFeatureNotSupportedException("NClob not supported")
    }

    override fun createSQLXML(): SQLXML {
        throw SQLFeatureNotSupportedException("SQLXML not supported")
    }

    override fun isValid(timeout: Int): Boolean {
        if (closed) return false

        return try {
            httpClient.testConnection()
            true
        } catch (e: Exception) {
            logger.debug("Connection validation failed", e)
            false
        }
    }

    override fun setClientInfo(name: String?, value: String?) {
        checkClosed()
        if (name != null && value != null) {
            clientInfo[name] = value
        }
    }

    override fun setClientInfo(properties: Properties?) {
        checkClosed()
        if (properties != null) {
            clientInfo.putAll(properties)
        }
    }

    override fun getClientInfo(name: String?): String? {
        checkClosed()
        return clientInfo.getProperty(name)
    }

    override fun getClientInfo(): Properties {
        checkClosed()
        return Properties().apply { putAll(clientInfo) }
    }

    override fun createArrayOf(typeName: String?, elements: kotlin.Array<out Any>?): java.sql.Array {
        throw SQLFeatureNotSupportedException("Array not supported")
    }

    override fun createStruct(typeName: String?, attributes: kotlin.Array<out Any>?): Struct {
        throw SQLFeatureNotSupportedException("Struct not supported")
    }

    override fun setSchema(schema: String?) {
        checkClosed()
        // No-op for now
    }

    override fun getSchema(): String? {
        checkClosed()
        return null
    }

    override fun abort(executor: Executor?) {
        close()
    }

    override fun setNetworkTimeout(executor: Executor?, milliseconds: Int) {
        checkClosed()
        // No-op for now
    }

    override fun getNetworkTimeout(): Int {
        checkClosed()
        return 0
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

    /**
     * Check if connection is closed and throw exception if it is
     */
    private fun checkClosed() {
        if (closed) {
            throw SQLException("Connection is closed")
        }
    }

    /**
     * Get the HTTP client for internal use
     */
    internal fun getHttpClient(): DuckDBFQEHttpClient = httpClient
}