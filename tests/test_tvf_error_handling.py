# import pytest
# from sqlcore.connector import DatabaseConnector

# TEST_CONN_STRING = (
#     "DRIVER={ODBC Driver 17 for SQL Server};"
#     "SERVER=localhost,1433;"
#     "DATABASE=testdb;"
#     "UID=sa;"
#     "PWD=YourStrong@Password"
# )

# db_connector = DatabaseConnector(conn_string=TEST_CONN_STRING, pool_limit=3)

# def test_execute_tvf_valid(db_connector):
#     """Test a valid table-valued function with parameters."""
#     result = db_connector.execute_tvf_and_fetch_results("fn_get_users", 25)
#     assert len(result) > 0
#     assert "name" in result[0]

# def test_execute_tvf_no_parameters(db_connector):
#     """Test a valid table-valued function with no parameters."""
#     result = db_connector.execute_tvf_and_fetch_results("fn_get_all_users")
#     assert len(result) > 0

# def test_execute_tvf_invalid_name(db_connector):
#     """Test an invalid table-valued function name."""
#     with pytest.raises(ValueError, match="TVF execution failed"):
#         db_connector.execute_tvf_and_fetch_results("fn_non_existent", 1)

# def test_execute_tvf_invalid_params(db_connector):
#     """Test invalid parameters for a table-valued function."""
#     with pytest.raises(ValueError, match="TVF execution failed"):
#         db_connector.execute_tvf_and_fetch_results("fn_get_users", "invalid_param")

# def test_execute_tvf_db_error(db_connector):
#     """Test a database-level error in a table-valued function."""
#     with pytest.raises(pyodbc.Error, match="Database error while executing TVF"):
#         db_connector.execute_tvf_and_fetch_results("fn_trigger_error", 1)