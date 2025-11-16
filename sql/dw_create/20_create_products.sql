-- File: 20_create_products.sql
-- Compatible with SQLite, DuckDB, dbt (DuckDB), and SQLMesh
-- - CSV Columns: ProductID,ProductName,Category,UnitPrice

DROP TABLE IF EXISTS products;

CREATE TABLE products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    unit_price REAL,
    stock_quantity INTEGER,
    supplier TEXT
);
