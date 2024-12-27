import pytest
from sqlcore.connector import DatabaseConnector

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

def test_e2e_execute_query(db_connector):
    """E2E test for executing a query."""
    query = "SELECT * FROM users"
    result = db_connector.execute_query(query)
    assert len(result) == 2
    assert result[0]["name"] == "Alice"

def test_e2e_stored_procedure(db_connector):
    """E2E test for executing a stored procedure."""
    result = db_connector.execute_and_return_stored_procedure("sp_get_users")
    assert len(result) == 2
    assert result[1]["name"] == "Bob"

def test_e2e_table_valued_function(db_connector):
    """E2E test for executing a table-valued function."""
    result = db_connector.execute_tvf_and_fetch_results("fn_get_users")
    assert len(result) == 2
    assert result[0]["age"] == 25