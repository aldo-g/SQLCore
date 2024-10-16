import os
import pyodbc
from queue import Queue
from typing import Any, Dict, List, Optional


class DatabaseConnector:
    """A class to handle synchronous database connections, execute SQL queries, and manage connection pooling."""

    def __init__(self, conn_string: Optional[str] = None, pool_limit: Optional[int] = 5) -> None:
        """Initializes the DatabaseConnector with a connection string and an optional pool limit of connections."""
        self.conn_string = conn_string or os.getenv("SQL_CONN_STRING")
        if not self.conn_string:
            raise ValueError("SQL connection string is not set")

        self.pool_limit = pool_limit
        self.pool = None

        if pool_limit is not None and pool_limit > 0:
            # Create a pool of connections with a maximum size
            self.pool = Queue(maxsize=pool_limit)
            for _ in range(pool_limit):
                self.pool.put(pyodbc.connect(self.conn_string, autocommit=False))

    def get_connection(self):
        """Retrieve a connection from the pool or create a new one if the pool is unlimited."""
        if self.pool:
            # If using connection pool, retrieve from pool
            return self.pool.get()
        else:
            # If no pool limit is defined, create a new connection each time
            return pyodbc.connect(self.conn_string, autocommit=False)

    def release_connection(self, conn):
        """Release a connection back to the pool or close it if the pool is unlimited."""
        if self.pool:
            # Return connection to the pool if using connection pool
            self.pool.put(conn)
        else:
            # Close the connection if no pooling
            conn.close()

    def execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[List[Dict[str, Any]]]:
        """Synchronously executes the given SQL query with optional parameters and returns the result as a list of dictionaries."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            if cursor.description:
                columns = [col[0] for col in cursor.description]
                result = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return result or None
            else:
                cursor.commit()
        except pyodbc.Error as e:
            print(f"Error executing query: {e}")
            return None
        finally:
            cursor.close()
            self.release_connection(conn)

    def execute_stored_procedure(self, proc_name: str, *args: Any) -> None:
        """Synchronously executes a stored procedure with the given name and parameters."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            param_placeholders = ", ".join(["?"] * len(args))
            cursor.execute(f"EXEC {proc_name} {param_placeholders}", *args)
            cursor.commit()
        except pyodbc.Error as e:
            print(f"Error executing stored procedure '{proc_name}': {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            self.release_connection(conn)

    def execute_and_return_stored_procedure(self, proc_name: str, *args: Any) -> Optional[List[Dict[str, Any]]]:
        """Executes a stored procedure and returns the result if available."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            param_placeholders = ", ".join(["?"] * len(args))
            cursor.execute(f"EXEC {proc_name} {param_placeholders}", *args)

            # Fetch result if it exists
            if cursor.description:
                columns = [col[0] for col in cursor.description]
                result = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return result or None
            else:
                cursor.commit()
                return None
        except pyodbc.Error as e:
            print(f"Error executing stored procedure '{proc_name}': {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            self.release_connection(conn)

    def execute_tvf_and_fetch_results(self, tvf_name: str, *parameters: Any) -> Optional[List[Dict[str, Any]]]:
        """Executes a table-valued function (TVF) with optional parameters and returns the results."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            if parameters:
                cursor.execute(f"SELECT * FROM {tvf_name}({','.join(['?'] * len(parameters))})", parameters)
            else:
                cursor.execute(f"SELECT * FROM {tvf_name}()")

            if cursor.description:
                columns = [col[0] for col in cursor.description]
                result = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return result or None
        except pyodbc.Error as e:
            print(f"Error executing TVF '{tvf_name}': {e}")
            return None
        finally:
            cursor.close()
            self.release_connection(conn)

    def close(self) -> None:
        """Closes all connections in the pool or if not using pool, nothing to close."""
        if self.pool:
            while not self.pool.empty():
                conn = self.pool.get()
                conn.close()
