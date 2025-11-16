-- File: 10_create_customers.sql
-- Compatible with SQLite, DuckDB, dbt (DuckDB), and SQLMesh
-- DATE Field Note:
-- - SQLite: Uses TEXT for date fields (store as 'YYYY-MM-DD' ISO format)
-- - DuckDB and others: Support native DATE type (can parse ISO-formatted TEXT)
-- - CSV Columns: CustomerID,Name,Region,JoinDate

DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    name TEXT,
    region TEXT,
    join_date TEXT,  -- use ISO format: 'YYYY-MM-DD'
    number_of_purchases INTEGER,
    shopping_frequency TEXT
);
