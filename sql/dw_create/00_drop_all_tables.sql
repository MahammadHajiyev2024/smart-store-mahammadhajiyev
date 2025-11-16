-- 00_drop_all_tables.sql
-- Drop in reverse order of creation to avoid foreign key constraints
-- Additional DROP TABLE in each file doesn't hurt, but is redundant
-- and can be removed if desired.

DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;
