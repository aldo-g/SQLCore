CREATE DATABASE testdb;
USE testdb;

-- Create a sample table
CREATE TABLE users (
    id INT PRIMARY KEY IDENTITY(1,1),
    name NVARCHAR(100),
    age INT
);

-- Insert test data
INSERT INTO users (name, age) VALUES ('Alice', 25), ('Bob', 30);

-- Create a stored procedure
CREATE PROCEDURE sp_get_users
AS
BEGIN
    SELECT * FROM users;
END;

-- Create a table-valued function
CREATE FUNCTION fn_get_users()
RETURNS TABLE
AS
RETURN (
    SELECT * FROM users
);