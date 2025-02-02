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
            self.pool = Queue(maxsize=pool_limit)
            for _ in range(pool_limit):
                # Note: autocommit is set to False so that we can explicitly control transactions.
                self.pool.put(pyodbc.connect(self.conn_string, autocommit=False))

    async def get_connection(self):
        """Retrieve a connection from the pool or create a new one if the pool is unlimited."""
        if self.pool:
            return self.pool.get()
        else:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, pyodbc.connect, self.conn_string, False)

    async def release_connection(self, conn):
        """Release a connection back to the pool or close it if the pool is unlimited."""
        if self.pool:
            self.pool.put(conn)
        else:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, conn.close)

    async def async_execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Asynchronously executes the given SQL query with optional parameters and returns the result as a list of dictionaries.
        For SELECT queries (which return a result set), a rollback is issued afterward to clear the transaction.
        """
        conn = await self.get_connection()
        try:
            loop = asyncio.get_running_loop()
            cursor = conn.cursor()
            if params:
                await loop.run_in_executor(None, cursor.execute, query, params)
            else:
                await loop.run_in_executor(None, cursor.execute, query)

            if cursor.description:  # Query returned rows
                columns = [col[0] for col in cursor.description]
                result = await loop.run_in_executor(
                    None, lambda: [dict(zip(columns, row)) for row in cursor.fetchall()]
                )
                # Clear the transaction so that subsequent queries on this connection are not affected.
                await loop.run_in_executor(None, conn.rollback)
                return result
            else:
                # For queries that do not return rows, commit the transaction.
                await loop.run_in_executor(None, cursor.commit)
                return []
        except pyodbc.ProgrammingError as e:
            raise ValueError(f"Query execution failed: {query}. Error: {str(e)}") from e
        except pyodbc.Error as e:
            raise pyodbc.Error(f"Database error occurred: {str(e)}") from e
        finally:
            cursor.close()
            await self.release_connection(conn)

    async def async_execute_stored_procedure(self, proc_name: str, *args: Any) -> None:
        """
        Asynchronously executes a stored procedure with the given name and parameters.
        (This version does not return results.)
        """
        conn = await self.get_connection()
        try:
            loop = asyncio.get_running_loop()
            cursor = conn.cursor()
            param_placeholders = ", ".join(["?"] * len(args))
            await loop.run_in_executor(
                None, cursor.execute, f"EXEC {proc_name} {param_placeholders}", *args
            )
            await loop.run_in_executor(None, cursor.commit)
        except pyodbc.ProgrammingError as e:
            raise ValueError(
                f"Stored procedure execution failed: '{proc_name}' with parameters {args}. Error: {str(e)}"
            ) from e
        except pyodbc.Error as e:
            await loop.run_in_executor(None, conn.rollback)
            raise pyodbc.Error(
                f"Database error while executing stored procedure: '{proc_name}' with parameters {args}. Error: {str(e)}"
            ) from e
        finally:
            cursor.close()
            await self.release_connection(conn)

    async def async_execute_and_return_stored_procedure(self, proc_name: str, *args: Any) -> List[Dict[str, Any]]:
        """
        Asynchronously executes a stored procedure and returns the result if available.
        For procedures that return a result set, a rollback is issued afterward.
        """
        conn = await self.get_connection()
        try:
            loop = asyncio.get_running_loop()
            cursor = conn.cursor()
            param_placeholders = ", ".join(["?"] * len(args))
            await loop.run_in_executor(
                None, cursor.execute, f"EXEC {proc_name} {param_placeholders}", *args
            )

            if cursor.description:
                columns = [col[0] for col in cursor.description]
                result = await loop.run_in_executor(
                    None, lambda: [dict(zip(columns, row)) for row in cursor.fetchall()]
                )
                await loop.run_in_executor(None, conn.rollback)
                return result
            else:
                await loop.run_in_executor(None, cursor.commit)
                return []
        except pyodbc.ProgrammingError as e:
            raise ValueError(
                f"Stored procedure execution failed: '{proc_name}' with parameters {args}. Error: {str(e)}"
            ) from e
        except pyodbc.Error as e:
            await loop.run_in_executor(None, conn.rollback)
            raise pyodbc.Error(
                f"Database error while executing stored procedure: '{proc_name}' with parameters {args}. Error: {str(e)}"
            ) from e
        finally:
            cursor.close()
            await self.release_connection(conn)

    async def async_execute_tvf_and_fetch_results(self, tvf_name: str, *parameters: Any) -> List[Dict[str, Any]]:
        """
        Asynchronously executes a table-valued function (TVF) with optional parameters and returns the results.
        The query prepends the 'dbo.' schema. A rollback is issued after fetching the result.
        """
        conn = await self.get_connection()
        try:
            loop = asyncio.get_running_loop()
            cursor = conn.cursor()
            if parameters:
                param_placeholders = ",".join(["?"] * len(parameters))
                query = f"SELECT * FROM dbo.{tvf_name}({param_placeholders})"
                await loop.run_in_executor(None, cursor.execute, query, parameters)
            else:
                query = f"SELECT * FROM dbo.{tvf_name}()"
                await loop.run_in_executor(None, cursor.execute, query)

            if cursor.description:
                columns = [col[0] for col in cursor.description]
                result = await loop.run_in_executor(
                    None, lambda: [dict(zip(columns, row)) for row in cursor.fetchall()]
                )
                await loop.run_in_executor(None, conn.rollback)
                return result
            else:
                return []
        except pyodbc.ProgrammingError as e:
            raise ValueError(
                f"TVF execution failed: '{tvf_name}' with parameters {parameters}. Error: {str(e)}"
            ) from e
        except pyodbc.Error as e:
            raise pyodbc.Error(
                f"Database error while executing TVF: '{tvf_name}' with parameters {parameters}. Error: {str(e)}"
            ) from e
        finally:
            cursor.close()
            await self.release_connection(conn)

    async def close(self) -> None:
        """Closes all connections in the pool or, if not using a pool, nothing to close."""
        if self.pool:
            loop = asyncio.get_running_loop()
            while not self.pool.empty():
                conn = self.pool.get()
                await loop.run_in_executor(None, conn.close)