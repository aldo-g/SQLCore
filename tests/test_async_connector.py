import pytest
import pytest_asyncio
import pyodbc
from sqlcore.async_connector import AsyncDatabaseConnector

# Use the same connection string as your other tests.
TEST_CONN_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=testdb;"
    "UID=sa;"
    "PWD=YourStrong@Password"
)

# Define an asynchronous fixture using pytest_asyncio.fixture
@pytest_asyncio.fixture(scope="module")
async def async_db_connector():
    connector = AsyncDatabaseConnector(conn_string=TEST_CONN_STRING, pool_limit=3)
    yield connector
    await connector.close()

@pytest.mark.asyncio
async def test_async_execute_query(async_db_connector):
    query = "SELECT * FROM users"
    result = await async_db_connector.async_execute_query(query)
    # Expect two rows as defined in your setup.sql.
    assert isinstance(result, list)
    assert len(result) == 2
    # Check that the first row contains the expected name.
    assert result[0]["name"] == "Alice"

@pytest.mark.asyncio
async def test_async_execute_query_with_params(async_db_connector):
    query = "SELECT name FROM users WHERE id = ?"
    result = await async_db_connector.async_execute_query(query, (1,))
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["name"] == "Alice"

@pytest.mark.asyncio
async def test_async_execute_stored_procedure(async_db_connector):
    # The stored procedure sp_get_users should return all rows from the users table (2 rows).
    result = await async_db_connector.async_execute_and_return_stored_procedure("sp_get_users")
    assert isinstance(result, list)
    assert len(result) == 2
    names = [row["name"] for row in result]
    assert "Alice" in names
    assert "Bob" in names

@pytest.mark.asyncio
async def test_async_execute_tvf_and_fetch_results_fn_get_users(async_db_connector):
    # The TVF fn_get_users should return exactly 2 rows.
    result = await async_db_connector.async_execute_tvf_and_fetch_results("fn_get_users")
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["name"] == "Alice"

@pytest.mark.asyncio
async def test_async_execute_tvf_and_fetch_results_fn_get_all_users(async_db_connector):
    # The TVF fn_get_all_users should return 3 rows.
    result = await async_db_connector.async_execute_tvf_and_fetch_results("fn_get_all_users")
    assert isinstance(result, list)
    assert len(result) == 3

@pytest.mark.asyncio
async def test_async_execute_tvf_and_fetch_results_fn_trigger_error(async_db_connector):
    # The TVF fn_trigger_error is defined to trigger a runtime error (e.g. division by zero)
    # and should raise a pyodbc.Error with a matching message.
    with pytest.raises(pyodbc.Error, match="Database error while executing TVF"):
        await async_db_connector.async_execute_tvf_and_fetch_results("fn_trigger_error")