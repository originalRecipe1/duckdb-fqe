package com.duckdb.fqe.jdbc

import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Types

/**
 * ResultSetMetaData implementation for DuckDB FQE
 */
class DuckDBFQEResultSetMetaData(
    private val columnMetadata: List<ColumnMetadata>
) : ResultSetMetaData {

    override fun getColumnCount(): Int = columnMetadata.size

    override fun isAutoIncrement(column: Int): Boolean {
        checkColumnIndex(column)
        return false
    }

    override fun isCaseSensitive(column: Int): Boolean {
        checkColumnIndex(column)
        return true
    }

    override fun isSearchable(column: Int): Boolean {
        checkColumnIndex(column)
        return true
    }

    override fun isCurrency(column: Int): Boolean {
        checkColumnIndex(column)
        return false
    }

    override fun isNullable(column: Int): Int {
        checkColumnIndex(column)
        val type = columnMetadata[column - 1].type.uppercase()
        return if (type.startsWith("NULLABLE(")) {
            ResultSetMetaData.columnNullable
        } else {
            ResultSetMetaData.columnNullableUnknown
        }
    }

    override fun isSigned(column: Int): Boolean {
        checkColumnIndex(column)
        val type = getDuckDBType(column)
        return when (type) {
            "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE", "DECIMAL" -> true
            else -> false
        }
    }

    override fun getColumnDisplaySize(column: Int): Int {
        checkColumnIndex(column)
        val type = getDuckDBType(column)
        return when (type) {
            "BOOLEAN" -> 5
            "TINYINT" -> 4
            "SMALLINT" -> 6
            "INTEGER" -> 11
            "BIGINT" -> 20
            "REAL" -> 13
            "DOUBLE" -> 22
            "VARCHAR" -> 255
            "DATE" -> 10
            "TIME" -> 8
            "TIMESTAMP" -> 19
            else -> 255
        }
    }

    override fun getColumnLabel(column: Int): String {
        checkColumnIndex(column)
        return columnMetadata[column - 1].name
    }

    override fun getColumnName(column: Int): String {
        checkColumnIndex(column)
        return columnMetadata[column - 1].name
    }

    override fun getSchemaName(column: Int): String {
        checkColumnIndex(column)
        return ""
    }

    override fun getPrecision(column: Int): Int {
        checkColumnIndex(column)
        val type = getDuckDBType(column)
        return when (type) {
            "TINYINT" -> 3
            "SMALLINT" -> 5
            "INTEGER" -> 10
            "BIGINT" -> 19
            "REAL" -> 7
            "DOUBLE" -> 15
            else -> 0
        }
    }

    override fun getScale(column: Int): Int {
        checkColumnIndex(column)
        val type = getDuckDBType(column)
        return when (type) {
            "REAL", "DOUBLE" -> 0
            else -> 0
        }
    }

    override fun getTableName(column: Int): String {
        checkColumnIndex(column)
        return ""
    }

    override fun getCatalogName(column: Int): String {
        checkColumnIndex(column)
        return ""
    }

    override fun getColumnType(column: Int): Int {
        checkColumnIndex(column)
        val type = getDuckDBType(column)
        return when (type) {
            "BOOLEAN" -> Types.BOOLEAN
            "TINYINT" -> Types.TINYINT
            "SMALLINT" -> Types.SMALLINT
            "INTEGER" -> Types.INTEGER
            "BIGINT" -> Types.BIGINT
            "REAL" -> Types.REAL
            "DOUBLE" -> Types.DOUBLE
            "DECIMAL" -> Types.DECIMAL
            "VARCHAR" -> Types.VARCHAR
            "DATE" -> Types.DATE
            "TIME" -> Types.TIME
            "TIMESTAMP" -> Types.TIMESTAMP
            "BLOB" -> Types.BLOB
            else -> Types.OTHER
        }
    }

    override fun getColumnTypeName(column: Int): String {
        checkColumnIndex(column)
        return getDuckDBType(column)
    }

    override fun isReadOnly(column: Int): Boolean {
        checkColumnIndex(column)
        return true
    }

    override fun isWritable(column: Int): Boolean {
        checkColumnIndex(column)
        return false
    }

    override fun isDefinitelyWritable(column: Int): Boolean {
        checkColumnIndex(column)
        return false
    }

    override fun getColumnClassName(column: Int): String {
        checkColumnIndex(column)
        val type = getDuckDBType(column)
        return when (type) {
            "BOOLEAN" -> "java.lang.Boolean"
            "TINYINT" -> "java.lang.Byte"
            "SMALLINT" -> "java.lang.Short"
            "INTEGER" -> "java.lang.Integer"
            "BIGINT" -> "java.lang.Long"
            "REAL" -> "java.lang.Float"
            "DOUBLE" -> "java.lang.Double"
            "DECIMAL" -> "java.math.BigDecimal"
            "VARCHAR" -> "java.lang.String"
            "DATE" -> "java.sql.Date"
            "TIME" -> "java.sql.Time"
            "TIMESTAMP" -> "java.sql.Timestamp"
            "BLOB" -> "[B"
            else -> "java.lang.Object"
        }
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

    private fun checkColumnIndex(column: Int) {
        if (column < 1 || column > columnMetadata.size) {
            throw SQLException("Invalid column index: $column")
        }
    }

    private fun getDuckDBType(column: Int): String {
        val type = columnMetadata[column - 1].type.uppercase()

        // Handle nullable types
        if (type.startsWith("NULLABLE(") && type.endsWith(")")) {
            return type.substring(9, type.length - 1)
        }

        return type
    }
}