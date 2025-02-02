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
            self.pool = Queue(maxsize=pool_limit)
            for _ in range(pool_limit):
                self.pool.put(pyodbc.connect(self.conn_string, autocommit=False))

    def get_connection(self):
        """Retrieve a connection from the pool or create a new one if the pool is unlimited."""
        if self.pool:
            return self.pool.get()
        else:
            return pyodbc.connect(self.conn_string, autocommit=False)

    def release_connection(self, conn):
        """Release a connection back to the pool or close it if the pool is unlimited."""
        if self.pool:
            self.pool.put(conn)
        else:
            conn.close()

    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Synchronously executes the given SQL query with optional parameters and returns the result as a list of dictionaries.

        Args:
            query (str): The SQL query to execute.
            params (Optional[tuple]): Parameters to pass to the query.

        Returns:
            List[Dict[str, Any]]: Query result as a list of dictionaries.

        Raises:
            ValueError: If the query fails or parameters are invalid.
            pyodbc.Error: For any database-related errors.
        """
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
                return result
            else:
                cursor.commit()
                return []
        except pyodbc.ProgrammingError as e:
            raise ValueError(f"Query execution failed: {query}. Error: {str(e)}") from e
        except pyodbc.Error as e:
            raise pyodbc.Error(f"Database error occurred: {str(e)}") from e
        finally:
            cursor.close()
            self.release_connection(conn)
            
    def execute_stored_procedure(self, proc_name: str, *args: Any) -> None:
        """
        Synchronously executes a stored procedure with the given name and parameters.

        Args:
            proc_name (str): The name of the stored procedure to execute.
            args (Any): Parameters to pass to the stored procedure.

        Raises:
            ValueError: If the stored procedure execution fails due to an invalid name or parameters.
            pyodbc.Error: For any database-related errors.
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            param_placeholders = ", ".join(["?"] * len(args))
            query = f"EXEC {proc_name} {param_placeholders}"

            cursor.execute(query, *args)
            cursor.commit()
        except pyodbc.ProgrammingError as e:
            raise ValueError(
                f"Stored procedure execution failed: '{proc_name}' with parameters {args}. Error: {str(e)}"
            ) from e
        except pyodbc.Error as e:
            raise pyodbc.Error(
                f"Database error while executing stored procedure: '{proc_name}' with parameters {args}. Error: {str(e)}"
            ) from e
        finally:
            cursor.close()
            self.release_connection(conn)

    def execute_and_return_stored_procedure(self, proc_name: str, *args: Any) -> List[Dict[str, Any]]:
        """
        Executes a stored procedure and returns the result if available.

        Args:
            proc_name (str): The name of the stored procedure to execute.
            args (Any): Parameters to pass to the stored procedure.

        Returns:
            List[Dict[str, Any]]: Result of the stored procedure as a list of dictionaries.

        Raises:
            ValueError: If the stored procedure execution fails due to an invalid name or parameters.
            pyodbc.Error: For any database-related errors.
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            param_placeholders = ", ".join(["?"] * len(args))
            query = f"EXEC {proc_name} {param_placeholders}"

            cursor.execute(query, *args)

            if cursor.description:
                columns = [col[0] for col in cursor.description]
                result = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return result
            else:
                cursor.commit()
                return []
        except pyodbc.ProgrammingError as e:
            raise ValueError(
                f"Stored procedure execution failed: '{proc_name}' with parameters {args}. Error: {str(e)}"
            ) from e
        except pyodbc.Error as e:
            raise pyodbc.Error(
                f"Database error while executing stored procedure: '{proc_name}' with parameters {args}. Error: {str(e)}"
            ) from e
        finally:
            cursor.close()
            self.release_connection(conn)

    def execute_tvf_and_fetch_results(self, tvf_name: str, *parameters: Any) -> List[Dict[str, Any]]:
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            if parameters:
                param_placeholders = ",".join(["?" for _ in parameters])
                query = f"SELECT * FROM dbo.{tvf_name}({param_placeholders})"
                cursor.execute(query, parameters)
            else:
                query = f"SELECT * FROM dbo.{tvf_name}()"
                cursor.execute(query)
    
            if cursor.description:
                columns = [col[0] for col in cursor.description]
                result = [dict(zip(columns, row)) for row in cursor.fetchall()]
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
            self.release_connection(conn)

    def close(self) -> None:
        """Closes all connections in the pool."""
        if self.pool:
            while not self.pool.empty():
                conn = self.pool.get()
                conn.close()