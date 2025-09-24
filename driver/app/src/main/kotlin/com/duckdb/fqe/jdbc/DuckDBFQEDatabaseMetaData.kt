package com.duckdb.fqe.jdbc

import java.sql.*

/**
 * DatabaseMetaData implementation for DuckDB FQE
 * Provides basic metadata about the DuckDB FQE driver and capabilities
 */
class DuckDBFQEDatabaseMetaData(
    private val connection: DuckDBFQEConnection
) : DatabaseMetaData {

    override fun allProceduresAreCallable(): Boolean = false
    override fun allTablesAreSelectable(): Boolean = true
    override fun getURL(): String = "jdbc:duckdb-fqe://"
    override fun getUserName(): String = "unknown"
    override fun isReadOnly(): Boolean = connection.isReadOnly
    override fun nullsAreSortedHigh(): Boolean = false
    override fun nullsAreSortedLow(): Boolean = true
    override fun nullsAreSortedAtStart(): Boolean = false
    override fun nullsAreSortedAtEnd(): Boolean = false

    override fun getDatabaseProductName(): String = "DuckDB Federated Query Engine"
    override fun getDatabaseProductVersion(): String = "1.0.0"
    override fun getDriverName(): String = DuckDBFQEDriver.DRIVER_NAME
    override fun getDriverVersion(): String = DuckDBFQEDriver.DRIVER_VERSION
    override fun getDriverMajorVersion(): Int = DuckDBFQEDriver.MAJOR_VERSION
    override fun getDriverMinorVersion(): Int = DuckDBFQEDriver.MINOR_VERSION

    override fun usesLocalFiles(): Boolean = false
    override fun usesLocalFilePerTable(): Boolean = false
    override fun supportsMixedCaseIdentifiers(): Boolean = true
    override fun storesUpperCaseIdentifiers(): Boolean = false
    override fun storesLowerCaseIdentifiers(): Boolean = false
    override fun storesMixedCaseIdentifiers(): Boolean = true
    override fun supportsMixedCaseQuotedIdentifiers(): Boolean = true
    override fun storesUpperCaseQuotedIdentifiers(): Boolean = false
    override fun storesLowerCaseQuotedIdentifiers(): Boolean = false
    override fun storesMixedCaseQuotedIdentifiers(): Boolean = true

    override fun getIdentifierQuoteString(): String = "\""
    override fun getSQLKeywords(): String = ""
    override fun getNumericFunctions(): String = ""
    override fun getStringFunctions(): String = ""
    override fun getSystemFunctions(): String = ""
    override fun getTimeDateFunctions(): String = ""
    override fun getSearchStringEscape(): String = "\\"
    override fun getExtraNameCharacters(): String = ""

    override fun supportsAlterTableWithAddColumn(): Boolean = false
    override fun supportsAlterTableWithDropColumn(): Boolean = false
    override fun supportsColumnAliasing(): Boolean = true
    override fun nullPlusNonNullIsNull(): Boolean = true
    override fun supportsConvert(): Boolean = false
    override fun supportsConvert(fromType: Int, toType: Int): Boolean = false
    override fun supportsTableCorrelationNames(): Boolean = true
    override fun supportsDifferentTableCorrelationNames(): Boolean = true
    override fun supportsExpressionsInOrderBy(): Boolean = true
    override fun supportsOrderByUnrelated(): Boolean = true
    override fun supportsGroupBy(): Boolean = true
    override fun supportsGroupByUnrelated(): Boolean = true
    override fun supportsGroupByBeyondSelect(): Boolean = true
    override fun supportsLikeEscapeClause(): Boolean = true
    override fun supportsMultipleResultSets(): Boolean = false
    override fun supportsMultipleTransactions(): Boolean = false
    override fun supportsNonNullableColumns(): Boolean = true
    override fun supportsMinimumSQLGrammar(): Boolean = true
    override fun supportsCoreSQLGrammar(): Boolean = true
    override fun supportsExtendedSQLGrammar(): Boolean = false
    override fun supportsANSI92EntryLevelSQL(): Boolean = true
    override fun supportsANSI92IntermediateSQL(): Boolean = false
    override fun supportsANSI92FullSQL(): Boolean = false
    override fun supportsIntegrityEnhancementFacility(): Boolean = false
    override fun supportsOuterJoins(): Boolean = true
    override fun supportsFullOuterJoins(): Boolean = true
    override fun supportsLimitedOuterJoins(): Boolean = true
    override fun getMaxBinaryLiteralLength(): Int = 0
    override fun getMaxCharLiteralLength(): Int = 0
    override fun getMaxColumnNameLength(): Int = 255
    override fun getMaxColumnsInGroupBy(): Int = 0
    override fun getMaxColumnsInIndex(): Int = 0
    override fun getMaxColumnsInOrderBy(): Int = 0
    override fun getMaxColumnsInSelect(): Int = 0
    override fun getMaxColumnsInTable(): Int = 0
    override fun getMaxConnections(): Int = 0
    override fun getMaxCursorNameLength(): Int = 0
    override fun getMaxIndexLength(): Int = 0
    override fun getMaxSchemaNameLength(): Int = 255
    override fun getMaxProcedureNameLength(): Int = 255
    override fun getMaxCatalogNameLength(): Int = 255
    override fun getMaxRowSize(): Int = 0
    override fun doesMaxRowSizeIncludeBlobs(): Boolean = false
    override fun getMaxStatementLength(): Int = 0
    override fun getMaxStatements(): Int = 0
    override fun getMaxTableNameLength(): Int = 255
    override fun getMaxTablesInSelect(): Int = 0
    override fun getMaxUserNameLength(): Int = 255

    override fun getDefaultTransactionIsolation(): Int = Connection.TRANSACTION_NONE
    override fun supportsTransactions(): Boolean = false
    override fun supportsTransactionIsolationLevel(level: Int): Boolean = level == Connection.TRANSACTION_NONE
    override fun supportsDataDefinitionAndDataManipulationTransactions(): Boolean = false
    override fun supportsDataManipulationTransactionsOnly(): Boolean = false
    override fun dataDefinitionCausesTransactionCommit(): Boolean = false
    override fun dataDefinitionIgnoredInTransactions(): Boolean = false

    override fun supportsResultSetType(type: Int): Boolean = when (type) {
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE -> true
        else -> false
    }

    override fun supportsResultSetConcurrency(type: Int, concurrency: Int): Boolean = when (concurrency) {
        ResultSet.CONCUR_READ_ONLY -> supportsResultSetType(type)
        else -> false
    }

    override fun ownUpdatesAreVisible(type: Int): Boolean = false
    override fun ownDeletesAreVisible(type: Int): Boolean = false
    override fun ownInsertsAreVisible(type: Int): Boolean = false
    override fun othersUpdatesAreVisible(type: Int): Boolean = false
    override fun othersDeletesAreVisible(type: Int): Boolean = false
    override fun othersInsertsAreVisible(type: Int): Boolean = false
    override fun updatesAreDetected(type: Int): Boolean = false
    override fun deletesAreDetected(type: Int): Boolean = false
    override fun insertsAreDetected(type: Int): Boolean = false

    override fun supportsBatchUpdates(): Boolean = false
    override fun supportsUnion(): Boolean = true
    override fun supportsUnionAll(): Boolean = true
    override fun supportsOpenCursorsAcrossCommit(): Boolean = false
    override fun supportsOpenCursorsAcrossRollback(): Boolean = false
    override fun supportsOpenStatementsAcrossCommit(): Boolean = false
    override fun supportsOpenStatementsAcrossRollback(): Boolean = false

    override fun getResultSetHoldability(): Int = ResultSet.HOLD_CURSORS_OVER_COMMIT
    override fun supportsResultSetHoldability(holdability: Int): Boolean =
        holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT

    override fun getDatabaseMajorVersion(): Int = 1
    override fun getDatabaseMinorVersion(): Int = 0
    override fun getJDBCMajorVersion(): Int = 4
    override fun getJDBCMinorVersion(): Int = 0
    override fun getSQLStateType(): Int = DatabaseMetaData.sqlStateSQL99
    override fun locatorsUpdateCopy(): Boolean = false
    override fun supportsStatementPooling(): Boolean = false
    override fun getRowIdLifetime(): RowIdLifetime = RowIdLifetime.ROWID_UNSUPPORTED
    override fun supportsStoredFunctionsUsingCallSyntax(): Boolean = false
    override fun autoCommitFailureClosesAllResultSets(): Boolean = false

    override fun getConnection(): Connection = connection

    // Methods that return ResultSets (not implemented for basic functionality)
    override fun getProcedures(catalog: String?, schemaPattern: String?, procedureNamePattern: String?): ResultSet {
        throw SQLFeatureNotSupportedException("getProcedures not implemented")
    }

    override fun getProcedureColumns(catalog: String?, schemaPattern: String?, procedureNamePattern: String?, columnNamePattern: String?): ResultSet {
        throw SQLFeatureNotSupportedException("getProcedureColumns not implemented")
    }

    override fun getTables(catalog: String?, schemaPattern: String?, tableNamePattern: String?, types: Array<out String>?): ResultSet {
        throw SQLFeatureNotSupportedException("getTables not implemented")
    }

    override fun getSchemas(): ResultSet {
        throw SQLFeatureNotSupportedException("getSchemas not implemented")
    }

    override fun getCatalogs(): ResultSet {
        throw SQLFeatureNotSupportedException("getCatalogs not implemented")
    }

    override fun getTableTypes(): ResultSet {
        throw SQLFeatureNotSupportedException("getTableTypes not implemented")
    }

    override fun getColumns(catalog: String?, schemaPattern: String?, tableNamePattern: String?, columnNamePattern: String?): ResultSet {
        throw SQLFeatureNotSupportedException("getColumns not implemented")
    }

    override fun getColumnPrivileges(catalog: String?, schema: String?, table: String?, columnNamePattern: String?): ResultSet {
        throw SQLFeatureNotSupportedException("getColumnPrivileges not implemented")
    }

    override fun getTablePrivileges(catalog: String?, schemaPattern: String?, tableNamePattern: String?): ResultSet {
        throw SQLFeatureNotSupportedException("getTablePrivileges not implemented")
    }

    override fun getBestRowIdentifier(catalog: String?, schema: String?, table: String?, scope: Int, nullable: Boolean): ResultSet {
        throw SQLFeatureNotSupportedException("getBestRowIdentifier not implemented")
    }

    override fun getVersionColumns(catalog: String?, schema: String?, table: String?): ResultSet {
        throw SQLFeatureNotSupportedException("getVersionColumns not implemented")
    }

    override fun getPrimaryKeys(catalog: String?, schema: String?, table: String?): ResultSet {
        throw SQLFeatureNotSupportedException("getPrimaryKeys not implemented")
    }

    override fun getImportedKeys(catalog: String?, schema: String?, table: String?): ResultSet {
        throw SQLFeatureNotSupportedException("getImportedKeys not implemented")
    }

    override fun getExportedKeys(catalog: String?, schema: String?, table: String?): ResultSet {
        throw SQLFeatureNotSupportedException("getExportedKeys not implemented")
    }

    override fun getCrossReference(parentCatalog: String?, parentSchema: String?, parentTable: String?, foreignCatalog: String?, foreignSchema: String?, foreignTable: String?): ResultSet {
        throw SQLFeatureNotSupportedException("getCrossReference not implemented")
    }

    override fun getTypeInfo(): ResultSet {
        throw SQLFeatureNotSupportedException("getTypeInfo not implemented")
    }

    override fun getIndexInfo(catalog: String?, schema: String?, table: String?, unique: Boolean, approximate: Boolean): ResultSet {
        throw SQLFeatureNotSupportedException("getIndexInfo not implemented")
    }

    override fun getUDTs(catalog: String?, schemaPattern: String?, typeNamePattern: String?, types: IntArray?): ResultSet {
        throw SQLFeatureNotSupportedException("getUDTs not implemented")
    }

    override fun getSuperTypes(catalog: String?, schemaPattern: String?, typeNamePattern: String?): ResultSet {
        throw SQLFeatureNotSupportedException("getSuperTypes not implemented")
    }

    override fun getSuperTables(catalog: String?, schemaPattern: String?, tableNamePattern: String?): ResultSet {
        throw SQLFeatureNotSupportedException("getSuperTables not implemented")
    }

    override fun getAttributes(catalog: String?, schemaPattern: String?, typeNamePattern: String?, attributeNamePattern: String?): ResultSet {
        throw SQLFeatureNotSupportedException("getAttributes not implemented")
    }

    override fun getSchemas(catalog: String?, schemaPattern: String?): ResultSet {
        throw SQLFeatureNotSupportedException("getSchemas not implemented")
    }

    override fun getClientInfoProperties(): ResultSet {
        throw SQLFeatureNotSupportedException("getClientInfoProperties not implemented")
    }

    override fun getFunctions(catalog: String?, schemaPattern: String?, functionNamePattern: String?): ResultSet {
        throw SQLFeatureNotSupportedException("getFunctions not implemented")
    }

    override fun getFunctionColumns(catalog: String?, schemaPattern: String?, functionNamePattern: String?, columnNamePattern: String?): ResultSet {
        throw SQLFeatureNotSupportedException("getFunctionColumns not implemented")
    }

    override fun getPseudoColumns(catalog: String?, schemaPattern: String?, tableNamePattern: String?, columnNamePattern: String?): ResultSet {
        throw SQLFeatureNotSupportedException("getPseudoColumns not implemented")
    }

    override fun generatedKeyAlwaysReturned(): Boolean = false

    // Additional required methods
    override fun getSchemaTerm(): String = "schema"
    override fun getProcedureTerm(): String = "procedure"
    override fun getCatalogTerm(): String = "catalog"
    override fun isCatalogAtStart(): Boolean = true
    override fun getCatalogSeparator(): String = "."
    override fun supportsSchemasInDataManipulation(): Boolean = true
    override fun supportsSchemasInProcedureCalls(): Boolean = false
    override fun supportsSchemasInTableDefinitions(): Boolean = true
    override fun supportsSchemasInIndexDefinitions(): Boolean = false
    override fun supportsSchemasInPrivilegeDefinitions(): Boolean = false
    override fun supportsCatalogsInDataManipulation(): Boolean = true
    override fun supportsCatalogsInProcedureCalls(): Boolean = false
    override fun supportsCatalogsInTableDefinitions(): Boolean = true
    override fun supportsCatalogsInIndexDefinitions(): Boolean = false
    override fun supportsCatalogsInPrivilegeDefinitions(): Boolean = false
    override fun supportsPositionedDelete(): Boolean = false
    override fun supportsPositionedUpdate(): Boolean = false
    override fun supportsSelectForUpdate(): Boolean = false
    override fun supportsStoredProcedures(): Boolean = false
    override fun supportsSubqueriesInComparisons(): Boolean = true
    override fun supportsSubqueriesInExists(): Boolean = true
    override fun supportsSubqueriesInIns(): Boolean = true
    override fun supportsSubqueriesInQuantifieds(): Boolean = true
    override fun supportsCorrelatedSubqueries(): Boolean = true
    override fun supportsSavepoints(): Boolean = false
    override fun supportsNamedParameters(): Boolean = false
    override fun supportsMultipleOpenResults(): Boolean = false
    override fun supportsGetGeneratedKeys(): Boolean = false

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
}