import os
import asyncio
import pyodbc
from queue import Queue
from typing import Any, Dict, List, Optional


class AsyncDatabaseConnector:
    """A class to handle asynchronous database connections, execute SQL queries, and manage connection pooling."""

    def __init__(self, conn_string: Optional[str] = None, pool_limit: Optional[int] = 5) -> None:
        """Initializes the AsyncDatabaseConnector with a connection string and an optional pool limit of connections."""
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

    async def get_connection(self):
        """Retrieve a connection from the pool or create a new one if the pool is unlimited."""
        if self.pool:
            # If using connection pool, retrieve from pool
            return self.pool.get()
        else:
            # If no pool limit is defined, create a new connection each time
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, pyodbc.connect, self.conn_string, False)

    async def release_connection(self, conn):
        """Release a connection back to the pool or close it if the pool is unlimited."""
        if self.pool:
            # Return connection to the pool if using connection pool
            self.pool.put(conn)
        else:
            # Close the connection if no pooling
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, conn.close)

    async def async_execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[List[Dict[str, Any]]]:
        """Asynchronously executes the given SQL query with optional parameters and returns the result as a list of dictionaries."""
        conn = await self.get_connection()
        try:
            loop = asyncio.get_running_loop()
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
            await self.release_connection(conn)

    async def async_execute_stored_procedure(self, proc_name: str, *args: Any) -> None:
        """Asynchronously executes a stored procedure with the given name and parameters."""
        conn = await self.get_connection()
        try:
            loop = asyncio.get_running_loop()
            cursor = conn.cursor()
            param_placeholders = ", ".join(["?"] * len(args))
            await loop.run_in_executor(None, cursor.execute, f"EXEC {proc_name} {param_placeholders}", *args)
            await loop.run_in_executor(None, cursor.commit)
        except pyodbc.Error as e:
            print(f"Error executing stored procedure '{proc_name}': {e}")
            await loop.run_in_executor(None, conn.rollback)
            raise
        finally:
            cursor.close()
            await self.release_connection(conn)

    async def async_execute_and_return_stored_procedure(self, proc_name: str, *args: Any) -> Optional[List[Dict[str, Any]]]:
        """Asynchronously executes a stored procedure and returns the result if available."""
        conn = await self.get_connection()
        try:
            loop = asyncio.get_running_loop()
            cursor = conn.cursor()
            param_placeholders = ", ".join(["?"] * len(args))
            await loop.run_in_executor(None, cursor.execute, f"EXEC {proc_name} {param_placeholders}", *args)

            # Fetch result if it exists
            if cursor.description:
                columns = [col[0] for col in cursor.description]
                result = await loop.run_in_executor(None, lambda: [dict(zip(columns, row)) for row in cursor.fetchall()])
                return result or None
            else:
                await loop.run_in_executor(None, cursor.commit)
                return None
        except pyodbc.Error as e:
            print(f"Error executing stored procedure '{proc_name}': {e}")
            await loop.run_in_executor(None, conn.rollback)
            raise
        finally:
            cursor.close()
            await self.release_connection(conn)

    async def async_execute_tvf_and_fetch_results(self, tvf_name: str, *parameters: Any) -> Optional[List[Dict[str, Any]]]:
        """Asynchronously executes a table-valued function (TVF) with optional parameters and returns the results."""
        conn = await self.get_connection()
        try:
            loop = asyncio.get_running_loop()
            cursor = conn.cursor()
            if parameters:
                await loop.run_in_executor(None, cursor.execute, f"SELECT * FROM {tvf_name}({','.join(['?'] * len(parameters))})", parameters)
            else:
                await loop.run_in_executor(None, cursor.execute, f"SELECT * FROM {tvf_name}()")

            if cursor.description:
                columns = [col[0] for col in cursor.description]
                result = await loop.run_in_executor(None, lambda: [dict(zip(columns, row)) for row in cursor.fetchall()])
                return result or None
        except pyodbc.Error as e:
            print(f"Error executing TVF '{tvf_name}': {e}")
            return None
        finally:
            cursor.close()
            await self.release_connection(conn)

    async def close(self) -> None:
        """Closes all connections in the pool or if not using pool, nothing to close."""
        if self.pool:
            while not self.pool.empty():
                conn = self.pool.get()
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, conn.close)
