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

@pytest.mark.parametrize("user_id,expected_name", [
    (1, "Alice"),
    (2, "Bob"),
])
def test_query_with_parameters(db_connector, user_id, expected_name):
    """Test executing a query with parameters."""
    query = "SELECT name FROM users WHERE id = ?"
    result = db_connector.execute_query(query, (user_id,))
    assert len(result) == 1
    assert result[0]["name"] == expected_name


@pytest.mark.parametrize("user_id,expected_error", [
    ("INVALID_ID", "Conversion failed"),
    (-1, "No results found"),  # Assuming this logic applies in the database
])
def test_query_with_invalid_parameters(db_connector, user_id, expected_error):
    """Test executing a query with invalid parameters."""
    query = "SELECT name FROM users WHERE id = ?"
    try:
        db_connector.execute_query(query, (user_id,))
    except Exception as e:
        assert expected_error in str(e)


@pytest.mark.parametrize("name_pattern,expected_count", [
    ("%A%", 1),  # Matches 'Alice'
    ("Bob%", 1),  # Matches 'Bob'
])
def test_query_with_like_operator(db_connector, name_pattern, expected_count):
    """Test executing a query with LIKE operator in parameters."""
    query = "SELECT name FROM users WHERE name LIKE ?"
    result = db_connector.execute_query(query, (name_pattern,))
    assert len(result) == expected_count


@pytest.mark.parametrize("age_min,age_max,expected_count", [
    (20, 30, 2),  # Assuming two users fall in this age range
    (50, 60, 0),  # Assuming no users in this age range
])
def test_query_with_between_operator(db_connector, age_min, age_max, expected_count):
    """Test executing a query with BETWEEN operator in parameters."""
    query = "SELECT * FROM users WHERE age BETWEEN ? AND ?"
    result = db_connector.execute_query(query, (age_min, age_max))
    assert len(result) == expected_count