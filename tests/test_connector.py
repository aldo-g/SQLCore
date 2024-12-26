import unittest
from unittest.mock import patch, MagicMock
from sqlcore.connector import DatabaseConnector

class TestDatabaseConnector(unittest.TestCase):

    @patch('pyodbc.connect')
    def setUp(self, mock_connect):
        # Mocking the pyodbc connection object
        self.mock_connection = MagicMock()
        mock_connect.return_value = self.mock_connection

        # Initialize the DatabaseConnector with mock connection
        self.db = DatabaseConnector(conn_string="DRIVER={SQL Server};SERVER=test_server;DATABASE=test_db;", pool_limit=1)

    @patch('pyodbc.connect')
    def test_execute_query(self, mock_connect):
        # Mocking the cursor and fetchall behavior
        mock_cursor = self.mock_connection.cursor.return_value
        mock_cursor.description = [('id',), ('name',)]
        mock_cursor.fetchall.return_value = [(1, 'Test User')]

        # Perform the query
        result = self.db.execute_query("SELECT * FROM users")

        # Assert that the result matches expected data
        self.assertEqual(result, [{'id': 1, 'name': 'Test User'}])

        # Assert that the correct query was executed
        mock_cursor.execute.assert_called_with("SELECT * FROM users")

    @patch('pyodbc.connect')
    def test_execute_stored_procedure(self, mock_connect):
        # Mocking cursor execution for stored procedure
        mock_cursor = self.mock_connection.cursor.return_value
        self.db.execute_stored_procedure("MyStoredProcedure", "param1", 123)

        # Assert that the stored procedure was executed with correct parameters
        mock_cursor.execute.assert_called_with("EXEC MyStoredProcedure ?, ?", "param1", 123)

    @patch('pyodbc.connect')
    def test_execute_and_return_stored_procedure(self, mock_connect):
        # Mocking the cursor and fetchall behavior for the stored procedure with return value
        mock_cursor = self.mock_connection.cursor.return_value
        mock_cursor.description = [('id',), ('value',)]
        mock_cursor.fetchall.return_value = [(1, 'Return Value')]

        # Perform the stored procedure call
        result = self.db.execute_and_return_stored_procedure("ReturnStoredProcedure", 1)

        # Assert that the result matches expected data
        self.assertEqual(result, [{'id': 1, 'value': 'Return Value'}])

        # Assert that the stored procedure was executed with correct parameters
        mock_cursor.execute.assert_called_with("EXEC ReturnStoredProcedure ?", 1)

    @patch('pyodbc.connect')
    def test_execute_tvf_and_fetch_results(self, mock_connect):
        # Mocking the cursor and fetchall behavior for TVF execution
        mock_cursor = self.mock_connection.cursor.return_value
        mock_cursor.description = [('id',), ('result',)]
        mock_cursor.fetchall.return_value = [(1, 'TVF Result')]

        # Execute TVF and fetch results
        result = self.db.execute_tvf_and_fetch_results("MyTVF", "param1")

        # Assert that the result matches expected data
        self.assertEqual(result, [{'id': 1, 'result': 'TVF Result'}])

        # Assert that the correct query was executed
        mock_cursor.execute.assert_called_with("SELECT * FROM MyTVF(?)", "param1")

if __name__ == "__main__":
    unittest.main()
