package com.duckdb.fqe.jdbc

import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Array
import java.sql.Date
import java.util.*

/**
 * PreparedStatement implementation for DuckDB FQE
 * Currently provides basic functionality with parameter substitution
 */
class DuckDBFQEPreparedStatement(
    connection: DuckDBFQEConnection,
    httpClient: DuckDBFQEHttpClient,
    private val sql: String,
    resultSetType: Int = ResultSet.TYPE_FORWARD_ONLY,
    resultSetConcurrency: Int = ResultSet.CONCUR_READ_ONLY,
    resultSetHoldability: Int = ResultSet.HOLD_CURSORS_OVER_COMMIT
) : DuckDBFQEStatement(connection, httpClient, resultSetType, resultSetConcurrency, resultSetHoldability), PreparedStatement {

    private val parameters = mutableMapOf<Int, Any?>()

    override fun executeQuery(): ResultSet {
        return executeQuery(buildSqlWithParameters())
    }

    override fun executeUpdate(): Int {
        return executeUpdate(buildSqlWithParameters())
    }

    override fun execute(): Boolean {
        return execute(buildSqlWithParameters())
    }

    override fun setNull(parameterIndex: Int, sqlType: Int) {
        parameters[parameterIndex] = null
    }

    override fun setBoolean(parameterIndex: Int, x: Boolean) {
        parameters[parameterIndex] = x
    }

    override fun setByte(parameterIndex: Int, x: Byte) {
        parameters[parameterIndex] = x
    }

    override fun setShort(parameterIndex: Int, x: Short) {
        parameters[parameterIndex] = x
    }

    override fun setInt(parameterIndex: Int, x: Int) {
        parameters[parameterIndex] = x
    }

    override fun setLong(parameterIndex: Int, x: Long) {
        parameters[parameterIndex] = x
    }

    override fun setFloat(parameterIndex: Int, x: Float) {
        parameters[parameterIndex] = x
    }

    override fun setDouble(parameterIndex: Int, x: Double) {
        parameters[parameterIndex] = x
    }

    override fun setBigDecimal(parameterIndex: Int, x: BigDecimal?) {
        parameters[parameterIndex] = x
    }

    override fun setString(parameterIndex: Int, x: String?) {
        parameters[parameterIndex] = x
    }

    override fun setBytes(parameterIndex: Int, x: ByteArray?) {
        parameters[parameterIndex] = x
    }

    override fun setDate(parameterIndex: Int, x: Date?) {
        parameters[parameterIndex] = x
    }

    override fun setTime(parameterIndex: Int, x: Time?) {
        parameters[parameterIndex] = x
    }

    override fun setTimestamp(parameterIndex: Int, x: Timestamp?) {
        parameters[parameterIndex] = x
    }

    override fun setObject(parameterIndex: Int, x: Any?) {
        parameters[parameterIndex] = x
    }

    override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: Int) {
        parameters[parameterIndex] = x
    }

    override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: Int, scaleOrLength: Int) {
        parameters[parameterIndex] = x
    }

    override fun clearParameters() {
        parameters.clear()
    }

    override fun addBatch() {
        throw SQLFeatureNotSupportedException("Batch operations not supported")
    }

    override fun getMetaData(): ResultSetMetaData {
        throw SQLFeatureNotSupportedException("PreparedStatement metadata not available")
    }

    override fun getParameterMetaData(): ParameterMetaData {
        throw SQLFeatureNotSupportedException("Parameter metadata not supported")
    }

    private fun buildSqlWithParameters(): String {
        var result = sql
        var paramIndex = 1

        // Simple parameter substitution - replace ? with actual values
        while (result.contains("?") && parameters.containsKey(paramIndex)) {
            val value = parameters[paramIndex]
            val paramValue = when (value) {
                null -> "NULL"
                is String -> "'${value.replace("'", "''")}'"
                is Date -> "'$value'"
                is Time -> "'$value'"
                is Timestamp -> "'$value'"
                else -> value.toString()
            }

            result = result.replaceFirst("?", paramValue)
            paramIndex++
        }

        return result
    }

    // Unsupported operations
    override fun setAsciiStream(parameterIndex: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
    @Deprecated("Use setCharacterStream instead")
    override fun setUnicodeStream(parameterIndex: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun setBinaryStream(parameterIndex: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun setCharacterStream(parameterIndex: Int, reader: Reader?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun setRef(parameterIndex: Int, x: Ref?) = throw SQLFeatureNotSupportedException()
    override fun setBlob(parameterIndex: Int, x: Blob?) = throw SQLFeatureNotSupportedException()
    override fun setClob(parameterIndex: Int, x: Clob?) = throw SQLFeatureNotSupportedException()
    override fun setArray(parameterIndex: Int, x: Array?) = throw SQLFeatureNotSupportedException()
    override fun setDate(parameterIndex: Int, x: Date?, cal: Calendar?) = setDate(parameterIndex, x)
    override fun setTime(parameterIndex: Int, x: Time?, cal: Calendar?) = setTime(parameterIndex, x)
    override fun setTimestamp(parameterIndex: Int, x: Timestamp?, cal: Calendar?) = setTimestamp(parameterIndex, x)
    override fun setNull(parameterIndex: Int, sqlType: Int, typeName: String?) = setNull(parameterIndex, sqlType)
    override fun setURL(parameterIndex: Int, x: URL?) = throw SQLFeatureNotSupportedException()
    override fun setRowId(parameterIndex: Int, x: RowId?) = throw SQLFeatureNotSupportedException()
    override fun setNString(parameterIndex: Int, value: String?) = setString(parameterIndex, value)
    override fun setNCharacterStream(parameterIndex: Int, value: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun setNClob(parameterIndex: Int, value: NClob?) = throw SQLFeatureNotSupportedException()
    override fun setClob(parameterIndex: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun setBlob(parameterIndex: Int, inputStream: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun setNClob(parameterIndex: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun setSQLXML(parameterIndex: Int, xmlObject: SQLXML?) = throw SQLFeatureNotSupportedException()
    override fun setAsciiStream(parameterIndex: Int, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun setBinaryStream(parameterIndex: Int, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun setCharacterStream(parameterIndex: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun setAsciiStream(parameterIndex: Int, x: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun setBinaryStream(parameterIndex: Int, x: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun setCharacterStream(parameterIndex: Int, reader: Reader?) = throw SQLFeatureNotSupportedException()
    override fun setNCharacterStream(parameterIndex: Int, value: Reader?) = throw SQLFeatureNotSupportedException()
    override fun setClob(parameterIndex: Int, reader: Reader?) = throw SQLFeatureNotSupportedException()
    override fun setBlob(parameterIndex: Int, inputStream: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun setNClob(parameterIndex: Int, reader: Reader?) = throw SQLFeatureNotSupportedException()
}