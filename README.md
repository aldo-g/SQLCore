# SQLCore

**SQLCore** is a lightweight Python package for managing both synchronous and asynchronous database connections, executing raw SQL queries, stored procedures, and table-valued functions, with optional connection pooling.

## Features

- **Synchronous and Asynchronous Support**: Provides separate classes for handling sync (`DatabaseConnector`) and async (`AsyncDatabaseConnector`) database interactions.
- **Connection Pooling**: Supports optional connection pooling with user-defined limits.
- **Raw SQL Execution**: Execute raw SQL queries, stored procedures, and table-valued functions (TVF) easily.
- **Lightweight**: Designed to be minimal and focus on direct interactions with the database.

---

## Installation

You can install the package from source or add it to your project:

```bash
pip install sqlcore
```

## Usage

Synchronous Database Connection (DatabaseConnector)
The DatabaseConnector class handles synchronous database operations. It supports connection pooling, executing raw SQL queries, stored procedures, and fetching results from table-valued functions.

```python

from sqlcore.connector import DatabaseConnector

# Initialize the DatabaseConnector
db = DatabaseConnector(conn_string="DRIVER={SQL Server};SERVER=your_server;DATABASE=your_db;", pool_limit=5)

# Synchronously execute a query
result = db.execute_query("SELECT * FROM users WHERE age > ?", (30,))
print(result)

# Execute a stored procedure
db.execute_stored_procedure("MyStoredProcedure", "param1", 123)

# Execute a stored procedure and return the result
sp_result = db.execute_and_return_stored_procedure("GetUserInfo", 123)
print(sp_result)

# Execute a table-valued function and fetch results
tvf_result = db.execute_tvf_and_fetch_results("MyTVF", "param1")
print(tvf_result)

# Close the connection pool when finished
db.close()
```

## Asynchronous Database Connection (AsyncDatabaseConnector)

The AsyncDatabaseConnector class handles asynchronous database operations, designed for use in asynchronous applications (e.g., web apps, APIs). Like the sync class, it supports connection pooling, raw SQL execution, and stored procedures.

```python

import asyncio
from sqlcore.async_connector import AsyncDatabaseConnector

async def run_async_queries():
    # Initialize the AsyncDatabaseConnector
    db = AsyncDatabaseConnector(conn_string="DRIVER={SQL Server};SERVER=your_server;DATABASE=your_db;", pool_limit=5)

    # Asynchronously execute a query
    result = await db.async_execute_query("SELECT * FROM users WHERE age > ?", (30,))
    print(result)

    # Asynchronously execute a stored procedure
    await db.async_execute_stored_procedure("MyStoredProcedure", "param1", 123)

    # Asynchronously execute a stored procedure and return the result
    sp_result = await db.async_execute_and_return_stored_procedure("GetUserInfo", 123)
    print(sp_result)

    # Asynchronously execute a table-valued function and fetch results
    tvf_result = await db.async_execute_tvf_and_fetch_results("MyTVF", "param1")
    print(tvf_result)

    # Close the database connection pool when finished
    await db.close()

# Run the async operations
asyncio.run(run_async_queries())

```

## Connection Pooling

You can control the number of database connections used by setting the pool_limit parameter:

Limited Pool: Use a fixed number of connections, e.g., pool_limit=5.
Unlimited Pool: To allow an unlimited number of connections (i.e., one connection per query), set pool_limit=None.

```python
# Create a connection pool with a limit of 5 connections
db = DatabaseConnector(conn_string="...", pool_limit=5)

# Create an unlimited pool (one connection per query)
db_unlimited = DatabaseConnector(conn_string="...", pool_limit=None)
```

## Error Handling

Both DatabaseConnector and AsyncDatabaseConnector include basic error handling for SQL execution. Errors encountered during query execution, stored procedures, or TVF executions are caught and printed, allowing for easier debugging.

## Testing

Unit tests for both synchronous and asynchronous operations can be added under the tests/ directory. Here's an example of how you might write a test for the synchronous connector:

```python
import unittest
from sqlcore.connector import DatabaseConnector

class TestDatabaseConnector(unittest.TestCase):

    def test_execute_query(self):
        db = DatabaseConnector(conn_string="DRIVER={SQL Server};SERVER=test_server;DATABASE=test_db;")
        result = db.execute_query("SELECT 1")
        self.assertEqual(result, [{'1': 1}])

if __name__ == '__main__':
    unittest.main()
```

To run the tests:

```bash
python -m unittest discover tests
```

## Contributing

Contributions to SQLCore are welcome! Please feel free to submit a pull request or open an issue if you encounter a bug or have a feature request.

## License
This project is licensed under the MIT License - see the LICENSE file for details.