CREATE DATABASE testdb;
GO

USE testdb;
GO

CREATE TABLE users (
    id INT PRIMARY KEY IDENTITY(1,1),
    name NVARCHAR(100),
    age INT
);
GO

INSERT INTO users (name, age) VALUES ('Alice', 25), ('Bob', 30);
GO

CREATE PROCEDURE sp_get_users
AS
BEGIN
    SELECT * FROM users;
END;
GO

CREATE FUNCTION fn_get_users()
RETURNS TABLE
AS
RETURN (
    SELECT * FROM users
);
GO

CREATE FUNCTION fn_get_all_users()
RETURNS TABLE
AS
RETURN (
    SELECT * FROM users
    UNION ALL
    SELECT 0 AS id, 'Extra' AS name, 40 AS age
);
GO

CREATE FUNCTION fn_trigger_error()
RETURNS TABLE
AS
RETURN (
    SELECT 1/0 AS error
);
GO