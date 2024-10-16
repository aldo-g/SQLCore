import os
import asyncio
import pyodbc
from queue import Queue
from typing import Any, Dict, List, Optional


class DatabaseConnector:
    """A class to handle database connections, execute SQL queries, and manage connection pooling and async support."""
    
    def __init__(self, conn_string: Optional[str] = None, pool_size: int = 5) -> None:
        """Initializes the DatabaseConnector with a connection string and a pool of connections."""
        self.conn_string = conn_string or os.getenv("SQL_CONN_STRING")
        if not self.conn_string:
            raise ValueError("SQL connection string is not set")

        # Create a pool of connections
        self.pool = Queue(maxsize=pool_size)
        for _ in range(pool_size):
            self.pool.put(pyodbc.connect(self.conn_string, autocommit=False))

    def get_connection(self):
        """Retrieve a connection from the pool."""
        return self.pool.get()

    def release_connection(self, conn):
        """Release a connection back to the pool."""
        self.pool.put(conn)

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

    async def async_execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[List[Dict[str, Any]]]:
        """Asynchronously executes the given SQL query with optional parameters."""
        loop = asyncio.get_running_loop()
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            if params:
                await loop.run_in_executor(None, cursor.execute, query, params)
            else:
                await loop.run_in_executor(None, cursor.execute, query)
            if cursor.description:
                columns = [col[0] for col in cursor.description]
                result = await loop.run_in_executor(None, lambda: [dict(zip(columns, row)) for row in cursor.fetchall()])
                return result or None
            else:
                await loop.run_in_executor(None, cursor.commit)
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

    def close(self) -> None:
        """Closes all connections in the pool."""
        while not self.pool.empty():
            conn = self.pool.get()
            conn.close()
