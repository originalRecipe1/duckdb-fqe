package com.duckdb.fqe.jdbc

import org.slf4j.LoggerFactory
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Array
import java.sql.Date
import java.util.*

/**
 * JDBC ResultSet implementation for DuckDB FQE
 */
class DuckDBFQEResultSet(
    private val statement: Statement,
    private val queryResult: QueryResult
) : ResultSet {

    private val logger = LoggerFactory.getLogger(DuckDBFQEResultSet::class.java)

    @Volatile
    private var closed = false

    private var currentRow = 0
    private var wasNull = false

    // Column name to index mapping for faster lookups
    private val columnNameToIndex: Map<String, Int> = queryResult.meta
        .mapIndexed { index, metadata -> metadata.name.lowercase() to index + 1 }
        .toMap()

    override fun next(): Boolean {
        checkClosed()
        if (currentRow < queryResult.data.size) {
            currentRow++
            return true
        }
        return false
    }

    override fun close() {
        if (!closed) {
            closed = true
            logger.debug("ResultSet closed")
        }
    }

    override fun wasNull(): Boolean = wasNull

    override fun getString(columnIndex: Int): String? {
        checkClosed()
        val value = getValue(columnIndex)
        wasNull = value == null
        return value?.toString()
    }

    override fun getBoolean(columnIndex: Int): Boolean {
        checkClosed()
        val value = getValue(columnIndex)
        wasNull = value == null
        return when (value) {
            null -> false
            is Boolean -> value
            is Number -> value.toInt() != 0
            is String -> value.lowercase() in setOf("true", "1", "yes", "on")
            else -> false
        }
    }

    override fun getByte(columnIndex: Int): Byte {
        checkClosed()
        val value = getValue(columnIndex)
        wasNull = value == null
        return when (value) {
            null -> 0
            is Number -> value.toByte()
            is String -> value.toByteOrNull() ?: 0
            else -> 0
        }
    }

    override fun getShort(columnIndex: Int): Short {
        checkClosed()
        val value = getValue(columnIndex)
        wasNull = value == null
        return when (value) {
            null -> 0
            is Number -> value.toShort()
            is String -> value.toShortOrNull() ?: 0
            else -> 0
        }
    }

    override fun getInt(columnIndex: Int): Int {
        checkClosed()
        val value = getValue(columnIndex)
        wasNull = value == null
        return when (value) {
            null -> 0
            is Number -> value.toInt()
            is String -> value.toIntOrNull() ?: 0
            else -> 0
        }
    }

    override fun getLong(columnIndex: Int): Long {
        checkClosed()
        val value = getValue(columnIndex)
        wasNull = value == null
        return when (value) {
            null -> 0L
            is Number -> value.toLong()
            is String -> value.toLongOrNull() ?: 0L
            else -> 0L
        }
    }

    override fun getFloat(columnIndex: Int): Float {
        checkClosed()
        val value = getValue(columnIndex)
        wasNull = value == null
        return when (value) {
            null -> 0f
            is Number -> value.toFloat()
            is String -> value.toFloatOrNull() ?: 0f
            else -> 0f
        }
    }

    override fun getDouble(columnIndex: Int): Double {
        checkClosed()
        val value = getValue(columnIndex)
        wasNull = value == null
        return when (value) {
            null -> 0.0
            is Number -> value.toDouble()
            is String -> value.toDoubleOrNull() ?: 0.0
            else -> 0.0
        }
    }

    @Deprecated("Use getBigDecimal(int) instead")
    override fun getBigDecimal(columnIndex: Int, scale: Int): BigDecimal? {
        checkClosed()
        val value = getValue(columnIndex)
        wasNull = value == null
        return when (value) {
            null -> null
            is Number -> BigDecimal.valueOf(value.toDouble()).setScale(scale, java.math.RoundingMode.HALF_UP)
            is String -> value.toBigDecimalOrNull()?.setScale(scale, java.math.RoundingMode.HALF_UP)
            else -> null
        }
    }

    override fun getBigDecimal(columnIndex: Int): BigDecimal? {
        checkClosed()
        val value = getValue(columnIndex)
        wasNull = value == null
        return when (value) {
            null -> null
            is Number -> BigDecimal.valueOf(value.toDouble())
            is String -> value.toBigDecimalOrNull()
            else -> null
        }
    }

    override fun getBytes(columnIndex: Int): ByteArray? {
        checkClosed()
        val value = getValue(columnIndex)
        wasNull = value == null
        return when (value) {
            null -> null
            is String -> value.toByteArray()
            is ByteArray -> value
            else -> value.toString().toByteArray()
        }
    }

    override fun getDate(columnIndex: Int): Date? {
        checkClosed()
        val value = getValue(columnIndex)
        wasNull = value == null
        return when (value) {
            null -> null
            is Date -> value
            is String -> try {
                Date.valueOf(value)
            } catch (e: IllegalArgumentException) {
                null
            }
            else -> null
        }
    }

    override fun getTime(columnIndex: Int): Time? {
        checkClosed()
        val value = getValue(columnIndex)
        wasNull = value == null
        return when (value) {
            null -> null
            is Time -> value
            is String -> try {
                Time.valueOf(value)
            } catch (e: IllegalArgumentException) {
                null
            }
            else -> null
        }
    }

    override fun getTimestamp(columnIndex: Int): Timestamp? {
        checkClosed()
        val value = getValue(columnIndex)
        wasNull = value == null
        return when (value) {
            null -> null
            is Timestamp -> value
            is String -> try {
                Timestamp.valueOf(value)
            } catch (e: IllegalArgumentException) {
                null
            }
            else -> null
        }
    }

    override fun getObject(columnIndex: Int): Any? {
        checkClosed()
        val value = getValue(columnIndex)
        wasNull = value == null
        return value
    }

    // Column name-based getters
    override fun getString(columnLabel: String?): String? = getString(findColumn(columnLabel))
    override fun getBoolean(columnLabel: String?): Boolean = getBoolean(findColumn(columnLabel))
    override fun getByte(columnLabel: String?): Byte = getByte(findColumn(columnLabel))
    override fun getShort(columnLabel: String?): Short = getShort(findColumn(columnLabel))
    override fun getInt(columnLabel: String?): Int = getInt(findColumn(columnLabel))
    override fun getLong(columnLabel: String?): Long = getLong(findColumn(columnLabel))
    override fun getFloat(columnLabel: String?): Float = getFloat(findColumn(columnLabel))
    override fun getDouble(columnLabel: String?): Double = getDouble(findColumn(columnLabel))
    @Deprecated("Use getBigDecimal(String) instead")
    @Suppress("DEPRECATION")
    override fun getBigDecimal(columnLabel: String?, scale: Int): BigDecimal? = getBigDecimal(findColumn(columnLabel), scale)
    override fun getBigDecimal(columnLabel: String?): BigDecimal? = getBigDecimal(findColumn(columnLabel))
    override fun getBytes(columnLabel: String?): ByteArray? = getBytes(findColumn(columnLabel))
    override fun getDate(columnLabel: String?): Date? = getDate(findColumn(columnLabel))
    override fun getTime(columnLabel: String?): Time? = getTime(findColumn(columnLabel))
    override fun getTimestamp(columnLabel: String?): Timestamp? = getTimestamp(findColumn(columnLabel))
    override fun getObject(columnLabel: String?): Any? = getObject(findColumn(columnLabel))

    override fun findColumn(columnLabel: String?): Int {
        checkClosed()
        if (columnLabel == null) {
            throw SQLException("Column label cannot be null")
        }
        return columnNameToIndex[columnLabel.lowercase()]
            ?: throw SQLException("Column not found: $columnLabel")
    }

    override fun getMetaData(): ResultSetMetaData {
        checkClosed()
        return DuckDBFQEResultSetMetaData(queryResult.meta)
    }

    override fun getWarnings(): SQLWarning? {
        checkClosed()
        return null
    }

    override fun clearWarnings() {
        checkClosed()
        // No-op
    }

    override fun getCursorName(): String {
        throw SQLFeatureNotSupportedException("Named cursors not supported")
    }

    override fun getStatement(): Statement {
        checkClosed()
        return statement
    }

    override fun isBeforeFirst(): Boolean {
        checkClosed()
        return currentRow == 0 && queryResult.data.isNotEmpty()
    }

    override fun isAfterLast(): Boolean {
        checkClosed()
        return currentRow > queryResult.data.size
    }

    override fun isFirst(): Boolean {
        checkClosed()
        return currentRow == 1
    }

    override fun isLast(): Boolean {
        checkClosed()
        return currentRow == queryResult.data.size && queryResult.data.isNotEmpty()
    }

    override fun beforeFirst() {
        checkClosed()
        currentRow = 0
    }

    override fun afterLast() {
        checkClosed()
        currentRow = queryResult.data.size + 1
    }

    override fun first(): Boolean {
        checkClosed()
        currentRow = 1
        return queryResult.data.isNotEmpty()
    }

    override fun last(): Boolean {
        checkClosed()
        currentRow = queryResult.data.size
        return queryResult.data.isNotEmpty()
    }

    override fun getRow(): Int {
        checkClosed()
        return if (currentRow > 0 && currentRow <= queryResult.data.size) currentRow else 0
    }

    override fun absolute(row: Int): Boolean {
        checkClosed()
        currentRow = when {
            row == 0 -> 0
            row > 0 -> minOf(row, queryResult.data.size + 1)
            else -> maxOf(queryResult.data.size + row + 1, 0)
        }
        return currentRow > 0 && currentRow <= queryResult.data.size
    }

    override fun relative(rows: Int): Boolean {
        checkClosed()
        return absolute(currentRow + rows)
    }

    override fun previous(): Boolean {
        checkClosed()
        if (currentRow > 1) {
            currentRow--
            return true
        }
        currentRow = 0
        return false
    }

    override fun setFetchDirection(direction: Int) {
        checkClosed()
        // No-op for now
    }

    override fun getFetchDirection(): Int {
        checkClosed()
        return ResultSet.FETCH_FORWARD
    }

    override fun setFetchSize(rows: Int) {
        checkClosed()
        // No-op
    }

    override fun getFetchSize(): Int {
        checkClosed()
        return 0
    }

    override fun getType(): Int {
        checkClosed()
        return ResultSet.TYPE_SCROLL_INSENSITIVE
    }

    override fun getConcurrency(): Int {
        checkClosed()
        return ResultSet.CONCUR_READ_ONLY
    }

    override fun isClosed(): Boolean = closed

    override fun getHoldability(): Int {
        checkClosed()
        return ResultSet.HOLD_CURSORS_OVER_COMMIT
    }

    // Unsupported operations
    override fun getAsciiStream(columnIndex: Int): InputStream? = throw SQLFeatureNotSupportedException()
    @Deprecated("Use getCharacterStream instead")
    override fun getUnicodeStream(columnIndex: Int): InputStream? = throw SQLFeatureNotSupportedException()
    override fun getBinaryStream(columnIndex: Int): InputStream? = throw SQLFeatureNotSupportedException()
    override fun getAsciiStream(columnLabel: String?): InputStream? = throw SQLFeatureNotSupportedException()
    @Deprecated("Use getCharacterStream instead")
    override fun getUnicodeStream(columnLabel: String?): InputStream? = throw SQLFeatureNotSupportedException()
    override fun getBinaryStream(columnLabel: String?): InputStream? = throw SQLFeatureNotSupportedException()
    override fun getCharacterStream(columnIndex: Int): Reader? = throw SQLFeatureNotSupportedException()
    override fun getCharacterStream(columnLabel: String?): Reader? = throw SQLFeatureNotSupportedException()
    override fun rowUpdated(): Boolean = false
    override fun rowInserted(): Boolean = false
    override fun rowDeleted(): Boolean = false
    override fun updateNull(columnIndex: Int) = throw SQLFeatureNotSupportedException()
    override fun updateBoolean(columnIndex: Int, x: Boolean) = throw SQLFeatureNotSupportedException()
    override fun updateByte(columnIndex: Int, x: Byte) = throw SQLFeatureNotSupportedException()
    override fun updateShort(columnIndex: Int, x: Short) = throw SQLFeatureNotSupportedException()
    override fun updateInt(columnIndex: Int, x: Int) = throw SQLFeatureNotSupportedException()
    override fun updateLong(columnIndex: Int, x: Long) = throw SQLFeatureNotSupportedException()
    override fun updateFloat(columnIndex: Int, x: Float) = throw SQLFeatureNotSupportedException()
    override fun updateDouble(columnIndex: Int, x: Double) = throw SQLFeatureNotSupportedException()
    override fun updateBigDecimal(columnIndex: Int, x: BigDecimal?) = throw SQLFeatureNotSupportedException()
    override fun updateString(columnIndex: Int, x: String?) = throw SQLFeatureNotSupportedException()
    override fun updateBytes(columnIndex: Int, x: ByteArray?) = throw SQLFeatureNotSupportedException()
    override fun updateDate(columnIndex: Int, x: Date?) = throw SQLFeatureNotSupportedException()
    override fun updateTime(columnIndex: Int, x: Time?) = throw SQLFeatureNotSupportedException()
    override fun updateTimestamp(columnIndex: Int, x: Timestamp?) = throw SQLFeatureNotSupportedException()
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun updateObject(columnIndex: Int, x: Any?, scaleOrLength: Int) = throw SQLFeatureNotSupportedException()
    override fun updateObject(columnIndex: Int, x: Any?) = throw SQLFeatureNotSupportedException()
    override fun updateNull(columnLabel: String?) = throw SQLFeatureNotSupportedException()
    override fun updateBoolean(columnLabel: String?, x: Boolean) = throw SQLFeatureNotSupportedException()
    override fun updateByte(columnLabel: String?, x: Byte) = throw SQLFeatureNotSupportedException()
    override fun updateShort(columnLabel: String?, x: Short) = throw SQLFeatureNotSupportedException()
    override fun updateInt(columnLabel: String?, x: Int) = throw SQLFeatureNotSupportedException()
    override fun updateLong(columnLabel: String?, x: Long) = throw SQLFeatureNotSupportedException()
    override fun updateFloat(columnLabel: String?, x: Float) = throw SQLFeatureNotSupportedException()
    override fun updateDouble(columnLabel: String?, x: Double) = throw SQLFeatureNotSupportedException()
    override fun updateBigDecimal(columnLabel: String?, x: BigDecimal?) = throw SQLFeatureNotSupportedException()
    override fun updateString(columnLabel: String?, x: String?) = throw SQLFeatureNotSupportedException()
    override fun updateBytes(columnLabel: String?, x: ByteArray?) = throw SQLFeatureNotSupportedException()
    override fun updateDate(columnLabel: String?, x: Date?) = throw SQLFeatureNotSupportedException()
    override fun updateTime(columnLabel: String?, x: Time?) = throw SQLFeatureNotSupportedException()
    override fun updateTimestamp(columnLabel: String?, x: Timestamp?) = throw SQLFeatureNotSupportedException()
    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun updateCharacterStream(columnLabel: String?, x: Reader?, length: Int) = throw SQLFeatureNotSupportedException()
    override fun updateObject(columnLabel: String?, x: Any?, scaleOrLength: Int) = throw SQLFeatureNotSupportedException()
    override fun updateObject(columnLabel: String?, x: Any?) = throw SQLFeatureNotSupportedException()
    override fun insertRow() = throw SQLFeatureNotSupportedException()
    override fun updateRow() = throw SQLFeatureNotSupportedException()
    override fun deleteRow() = throw SQLFeatureNotSupportedException()
    override fun refreshRow() = throw SQLFeatureNotSupportedException()
    override fun cancelRowUpdates() = throw SQLFeatureNotSupportedException()
    override fun moveToInsertRow() = throw SQLFeatureNotSupportedException()
    override fun moveToCurrentRow() = throw SQLFeatureNotSupportedException()
    override fun getRef(columnIndex: Int): Ref? = throw SQLFeatureNotSupportedException()
    override fun getBlob(columnIndex: Int): Blob? = throw SQLFeatureNotSupportedException()
    override fun getClob(columnIndex: Int): Clob? = throw SQLFeatureNotSupportedException()
    override fun getArray(columnIndex: Int): Array? = throw SQLFeatureNotSupportedException()
    override fun getRef(columnLabel: String?): Ref? = throw SQLFeatureNotSupportedException()
    override fun getBlob(columnLabel: String?): Blob? = throw SQLFeatureNotSupportedException()
    override fun getClob(columnLabel: String?): Clob? = throw SQLFeatureNotSupportedException()
    override fun getArray(columnLabel: String?): Array? = throw SQLFeatureNotSupportedException()
    override fun getDate(columnIndex: Int, cal: Calendar?): Date? = getDate(columnIndex)
    override fun getDate(columnLabel: String?, cal: Calendar?): Date? = getDate(columnLabel)
    override fun getTime(columnIndex: Int, cal: Calendar?): Time? = getTime(columnIndex)
    override fun getTime(columnLabel: String?, cal: Calendar?): Time? = getTime(columnLabel)
    override fun getTimestamp(columnIndex: Int, cal: Calendar?): Timestamp? = getTimestamp(columnIndex)
    override fun getTimestamp(columnLabel: String?, cal: Calendar?): Timestamp? = getTimestamp(columnLabel)
    override fun getURL(columnIndex: Int): URL? = throw SQLFeatureNotSupportedException()
    override fun getURL(columnLabel: String?): URL? = throw SQLFeatureNotSupportedException()
    override fun updateRef(columnIndex: Int, x: Ref?) = throw SQLFeatureNotSupportedException()
    override fun updateRef(columnLabel: String?, x: Ref?) = throw SQLFeatureNotSupportedException()
    override fun updateBlob(columnIndex: Int, x: Blob?) = throw SQLFeatureNotSupportedException()
    override fun updateBlob(columnLabel: String?, x: Blob?) = throw SQLFeatureNotSupportedException()
    override fun updateClob(columnIndex: Int, x: Clob?) = throw SQLFeatureNotSupportedException()
    override fun updateClob(columnLabel: String?, x: Clob?) = throw SQLFeatureNotSupportedException()
    override fun updateArray(columnIndex: Int, x: Array?) = throw SQLFeatureNotSupportedException()
    override fun updateArray(columnLabel: String?, x: Array?) = throw SQLFeatureNotSupportedException()
    override fun getRowId(columnIndex: Int): RowId? = throw SQLFeatureNotSupportedException()
    override fun getRowId(columnLabel: String?): RowId? = throw SQLFeatureNotSupportedException()
    override fun updateRowId(columnIndex: Int, x: RowId?) = throw SQLFeatureNotSupportedException()
    override fun updateRowId(columnLabel: String?, x: RowId?) = throw SQLFeatureNotSupportedException()
    override fun updateNString(columnIndex: Int, nString: String?) = throw SQLFeatureNotSupportedException()
    override fun updateNString(columnLabel: String?, nString: String?) = throw SQLFeatureNotSupportedException()
    override fun updateNClob(columnIndex: Int, nClob: NClob?) = throw SQLFeatureNotSupportedException()
    override fun updateNClob(columnLabel: String?, nClob: NClob?) = throw SQLFeatureNotSupportedException()
    override fun getNClob(columnIndex: Int): NClob? = throw SQLFeatureNotSupportedException()
    override fun getNClob(columnLabel: String?): NClob? = throw SQLFeatureNotSupportedException()
    override fun getSQLXML(columnIndex: Int): SQLXML? = throw SQLFeatureNotSupportedException()
    override fun getSQLXML(columnLabel: String?): SQLXML? = throw SQLFeatureNotSupportedException()
    override fun updateSQLXML(columnIndex: Int, xmlObject: SQLXML?) = throw SQLFeatureNotSupportedException()
    override fun updateSQLXML(columnLabel: String?, xmlObject: SQLXML?) = throw SQLFeatureNotSupportedException()
    override fun getNString(columnIndex: Int): String? = getString(columnIndex)
    override fun getNString(columnLabel: String?): String? = getString(columnLabel)
    override fun getNCharacterStream(columnIndex: Int): Reader? = throw SQLFeatureNotSupportedException()
    override fun getNCharacterStream(columnLabel: String?): Reader? = throw SQLFeatureNotSupportedException()
    override fun updateNCharacterStream(columnIndex: Int, x: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateBlob(columnIndex: Int, inputStream: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateBlob(columnLabel: String?, inputStream: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateClob(columnIndex: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateClob(columnLabel: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateNClob(columnIndex: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateNClob(columnLabel: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
    override fun updateNCharacterStream(columnIndex: Int, x: Reader?) = throw SQLFeatureNotSupportedException()
    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?) = throw SQLFeatureNotSupportedException()
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun updateCharacterStream(columnIndex: Int, x: Reader?) = throw SQLFeatureNotSupportedException()
    override fun updateAsciiStream(columnLabel: String?, x: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun updateBinaryStream(columnLabel: String?, x: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun updateCharacterStream(columnLabel: String?, reader: Reader?) = throw SQLFeatureNotSupportedException()
    override fun updateBlob(columnIndex: Int, inputStream: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun updateBlob(columnLabel: String?, inputStream: InputStream?) = throw SQLFeatureNotSupportedException()
    override fun updateClob(columnIndex: Int, reader: Reader?) = throw SQLFeatureNotSupportedException()
    override fun updateClob(columnLabel: String?, reader: Reader?) = throw SQLFeatureNotSupportedException()
    override fun updateNClob(columnIndex: Int, reader: Reader?) = throw SQLFeatureNotSupportedException()
    override fun updateNClob(columnLabel: String?, reader: Reader?) = throw SQLFeatureNotSupportedException()
    override fun <T : Any?> getObject(columnIndex: Int, type: Class<T>?): T? = throw SQLFeatureNotSupportedException()
    override fun <T : Any?> getObject(columnLabel: String?, type: Class<T>?): T? = throw SQLFeatureNotSupportedException()
    override fun getObject(columnIndex: Int, map: MutableMap<String, Class<*>>?): Any? = getObject(columnIndex)
    override fun getObject(columnLabel: String?, map: MutableMap<String, Class<*>>?): Any? = getObject(columnLabel)

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
     * Get value from current row at specified column index
     */
    private fun getValue(columnIndex: Int): Any? {
        checkClosed()
        if (currentRow < 1 || currentRow > queryResult.data.size) {
            throw SQLException("Invalid row position: $currentRow")
        }
        if (columnIndex < 1 || columnIndex > queryResult.meta.size) {
            throw SQLException("Invalid column index: $columnIndex")
        }

        return queryResult.data[currentRow - 1][columnIndex - 1]
    }

    private fun checkClosed() {
        if (closed) {
            throw SQLException("ResultSet is closed")
        }
    }
}