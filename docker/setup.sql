-- Create a new database
CREATE DATABASE testdb;
GO

-- Switch to the new database
USE testdb;
GO

-- Create a sample table
CREATE TABLE users (
    id INT PRIMARY KEY IDENTITY(1,1),
    name NVARCHAR(100),
    age INT
);
GO

-- Insert sample data
INSERT INTO users (name, age) VALUES ('Alice', 25), ('Bob', 30);
GO

-- Create a stored procedure
CREATE PROCEDURE sp_get_users
AS
BEGIN
    SELECT * FROM users;
END;
GO

-- Create a table-valued function
CREATE FUNCTION fn_get_users()
RETURNS TABLE
AS
RETURN (
    SELECT * FROM users
);
GO