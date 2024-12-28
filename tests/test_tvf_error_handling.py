import pytest
from sqlcore.connector import DatabaseConnector
import pyodbc

TEST_CONN_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=testdb;"
    "UID=sa;"
    "PWD=YourStrong@Password"
)

@pytest.fixture(scope="module")
def db_connector():
    """Fixture to initialize and clean up the DatabaseConnector."""
    connector = DatabaseConnector(conn_string=TEST_CONN_STRING, pool_limit=3)
    yield connector
    connector.close()

@pytest.mark.parametrize("tvf_name,params,expected_count", [
    ("fn_get_users", (25,), 2),  # Assuming 2 users with age >= 25
    ("fn_get_all_users", (), 3),  # Assuming 3 users in total
])
def test_execute_tvf_valid(db_connector, tvf_name, params, expected_count):
    """Test valid table-valued functions with parameters."""
    result = db_connector.execute_tvf_and_fetch_results(tvf_name, *params)
    assert len(result) == expected_count
    assert "name" in result[0]  # Ensures the result includes the 'name' column

@pytest.mark.parametrize("tvf_name", [
    ("fn_non_existent"),
    ("fn_invalid_name"),
])
def test_execute_tvf_invalid_name(db_connector, tvf_name):
    """Test invalid table-valued function names."""
    with pytest.raises(ValueError, match="TVF execution failed"):
        db_connector.execute_tvf_and_fetch_results(tvf_name)

@pytest.mark.parametrize("tvf_name,params", [
    ("fn_get_users", ("invalid_param",)),  # Passing invalid parameter type
    ("fn_get_users", (-999,)),  # Edge case: invalid numeric parameter
])
def test_execute_tvf_invalid_params(db_connector, tvf_name, params):
    """Test invalid parameters for table-valued functions."""
    with pytest.raises(ValueError, match="TVF execution failed"):
        db_connector.execute_tvf_and_fetch_results(tvf_name, *params)

@pytest.mark.parametrize("tvf_name,params", [
    ("fn_trigger_error", (1,)),  # Simulates a database-level error
])
def test_execute_tvf_db_error(db_connector, tvf_name, params):
    """Test database-level errors in table-valued functions."""
    with pytest.raises(pyodbc.Error, match="Database error while executing TVF"):
        db_connector.execute_tvf_and_fetch_results(tvf_name, *params)