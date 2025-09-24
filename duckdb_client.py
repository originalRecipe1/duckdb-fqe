#!/usr/bin/env python3
"""
DuckDB Federated Query Engine HTTP Client
Provides a Python interface for executing queries against the DuckDB FQE
"""

import requests
import json
import time
from typing import Dict, List, Optional, Any
import pandas as pd


class DuckDBFQEClient:
    """Client for interacting with DuckDB Federated Query Engine via HTTP API"""

    def __init__(self, base_url: str = "http://localhost:8080", timeout: int = 30):
        """
        Initialize the DuckDB FQE client

        Args:
            base_url: Base URL of the DuckDB HTTP server
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()

    def is_healthy(self) -> bool:
        """Check if the DuckDB service is healthy and responsive"""
        try:
            response = self.session.get(f"{self.base_url}/health", timeout=5)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False

    def wait_for_ready(self, max_wait: int = 60) -> bool:
        """
        Wait for the DuckDB service to be ready

        Args:
            max_wait: Maximum time to wait in seconds

        Returns:
            True if service becomes ready, False if timeout
        """
        start_time = time.time()
        while time.time() - start_time < max_wait:
            if self.is_healthy():
                return True
            time.sleep(2)
        return False

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a SQL query

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            Query results as dictionary

        Raises:
            Exception: If query execution fails
        """
        payload = {"query": query}
        if params:
            payload["params"] = params

        try:
            response = self.session.post(
                f"{self.base_url}/query",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=self.timeout
            )

            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"Query failed with status {response.status_code}: {response.text}")

        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {e}")

    def execute_query_to_dataframe(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Execute a query and return results as a pandas DataFrame

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            Query results as pandas DataFrame
        """
        result = self.execute_query(query, params)

        # Assuming the result format includes 'data' and 'columns'
        if 'data' in result and 'columns' in result:
            return pd.DataFrame(result['data'], columns=result['columns'])
        elif isinstance(result, list):
            return pd.DataFrame(result)
        else:
            # Try to convert the result directly
            return pd.DataFrame([result])

    def get_databases(self) -> List[Dict[str, str]]:
        """Get list of attached databases"""
        result = self.execute_query("SHOW DATABASES")
        return result

    def get_tables(self, database: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Get list of tables, optionally filtered by database

        Args:
            database: Optional database name to filter by

        Returns:
            List of table information
        """
        if database:
            query = f"SHOW TABLES FROM {database}"
        else:
            query = "SELECT * FROM federated_tables"

        return self.execute_query(query)

    def describe_table(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Describe the structure of a table

        Args:
            table_name: Full table name (database.schema.table)

        Returns:
            Table schema information
        """
        query = f"DESCRIBE {table_name}"
        return self.execute_query(query)

    def count_rows(self, table_name: str) -> int:
        """
        Count rows in a table

        Args:
            table_name: Full table name (database.schema.table)

        Returns:
            Number of rows
        """
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.execute_query(query)

        # Extract count from result
        if isinstance(result, list) and len(result) > 0:
            return result[0].get('count', 0)
        elif isinstance(result, dict) and 'data' in result:
            return result['data'][0][0] if result['data'] else 0
        else:
            return 0

    def execute_federated_join(self, tables: List[str], join_conditions: List[str],
                             select_columns: List[str] = None,
                             where_conditions: List[str] = None,
                             limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a federated join query across multiple databases

        Args:
            tables: List of table names to join
            join_conditions: List of JOIN conditions
            select_columns: Columns to select (default: *)
            where_conditions: Optional WHERE conditions
            limit: Optional LIMIT clause

        Returns:
            Query results
        """
        # Build SELECT clause
        if select_columns:
            select_clause = ", ".join(select_columns)
        else:
            select_clause = "*"

        # Build FROM clause with JOINs
        from_clause = tables[0]
        for i, table in enumerate(tables[1:], 1):
            if i <= len(join_conditions):
                from_clause += f" JOIN {table} ON {join_conditions[i-1]}"

        # Build WHERE clause
        where_clause = ""
        if where_conditions:
            where_clause = " WHERE " + " AND ".join(where_conditions)

        # Build LIMIT clause
        limit_clause = ""
        if limit:
            limit_clause = f" LIMIT {limit}"

        # Construct final query
        query = f"SELECT {select_clause} FROM {from_clause}{where_clause}{limit_clause}"

        return self.execute_query(query)

    def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the DuckDB connection and attached databases"""
        info = {
            "base_url": self.base_url,
            "healthy": self.is_healthy(),
            "databases": None,
            "version": None
        }

        try:
            # Get DuckDB version
            version_result = self.execute_query("SELECT version()")
            if version_result:
                info["version"] = version_result

            # Get attached databases
            info["databases"] = self.get_databases()

        except Exception as e:
            info["error"] = str(e)

        return info

    def close(self):
        """Close the HTTP session"""
        self.session.close()

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


# Example usage
if __name__ == "__main__":
    # Example usage of the client
    with DuckDBFQEClient() as client:
        # Wait for service to be ready
        if not client.wait_for_ready():
            print("DuckDB service is not available")
            exit(1)

        # Get connection info
        info = client.get_connection_info()
        print("Connection Info:", json.dumps(info, indent=2))

        # Execute a simple query
        try:
            result = client.execute_query("SELECT 'Hello from DuckDB FQE!' as message")
            print("Query Result:", result)

            # Get list of databases
            databases = client.get_databases()
            print("Databases:", databases)

            # Get federated tables
            tables = client.get_tables()
            print("Federated Tables:", tables)

        except Exception as e:
            print(f"Error: {e}")