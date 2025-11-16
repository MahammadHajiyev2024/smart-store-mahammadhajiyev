"""
scripts/dw_create/create_dw_duckdb.py

This script creates the DuckDB data warehouse using .sql files.

File locations:
- data/prepared          : cleaned and prepared CSVs (used later in ETL)
- dw/                    : data warehouse folder
- dw/smart_sales.duckdb  : DuckDB database file
- sql/dw_create          : folder for .sql files used to create tables

Notes:
- DuckDB supports DATE and TIMESTAMP types natively.

Complete the TODOs to create and populate a DuckDB data warehouse.
"""

# import from Python standard libraries
import pathlib
import sys

# import from external libraries
import duckdb
import pandas as pd

# Add project root to sys.path for local imports
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent.parent))

# import from local modules
from utils.logger import logger

# === Path Constants ===

# Python script directories
SCRIPTS_DW_CREATE_DIR = pathlib.Path(__file__).resolve().parent
SCRIPTS_DIR = SCRIPTS_DW_CREATE_DIR.parent

# Project root directory
PROJECT_ROOT = SCRIPTS_DIR.parent

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
DATA_PREPARED_DIR = DATA_DIR / "prepared"

# SQL directories
SQL_DIR = PROJECT_ROOT / "sql"
SQL_DW_CREATE_DIR = SQL_DIR / "dw_create"

# Output DW directory
DW_DIR = PROJECT_ROOT / "dw"

# Output DW SQLite database path
DW_PATH = DW_DIR / "smart_sales.duckdb"

# Ensure necessary directories exist
DW_DIR.mkdir(parents=True, exist_ok=True)
SQL_DW_CREATE_DIR.mkdir(parents=True, exist_ok=True)

# === Functions ===


def run_sql_file(conn, file_path: pathlib.Path) -> None:
    """Run a SQL script file using the given DuckDB connection."""
    logger.info(f"RUN: {file_path}")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            sql = f.read()
            conn.execute(sql)
    except Exception as e:
        logger.error(f"Error running {file_path.name}: {e}")
        raise


def create_dw() -> None:
    logger.info("Creating data warehouse using DuckDB...")

    sql_files = list(SQL_DW_CREATE_DIR.glob("*.sql"))
    if not sql_files:
        logger.error(
            f"No .sql files in {SQL_DW_CREATE_DIR}. Please add schema files before running."
        )
        exit(22)

    conn = None
    try:
        conn = duckdb.connect(DW_PATH)

        # Run SQL scripts for creating tables
        # Use version 91 for sales table
        run_sql_file(conn, SQL_DW_CREATE_DIR / "00_drop_all_tables.sql")
        run_sql_file(conn, SQL_DW_CREATE_DIR / "10_create_customers.sql")
        run_sql_file(conn, SQL_DW_CREATE_DIR / "20_create_products.sql")
        run_sql_file(conn, SQL_DW_CREATE_DIR / "91_create_sales.sql")

        logger.info("DuckDB warehouse created successfully.")

    except Exception as e:
        logger.error(f"Error creating DuckDB: {e}")
        exit(30)
    finally:
        if conn:
            conn.close()


def populate_dw() -> None:
    """Load and insert prepared CSV data into DuckDB with column mapping."""
    logger.info("Populating DuckDB data warehouse from prepared CSVs...")

    try:
        conn = duckdb.connect(DW_PATH)

        # --- Load and insert customers ---
        # TODO: Add your new columns
        # TODO: Change as needed to reflect YOUR data and file names, etc.

        df_customers = pd.read_csv(DATA_PREPARED_DIR / "customers_prepared.csv")
        df_customers = df_customers.rename(
            columns={
                "CustomerID": "customer_id",
                "Name": "name",
                "Region": "region",
                "JoinDate": "join_date",
                "NumberOfPurshases": "number_of_purchases",
                "ShoppingFrequency": "shopping_frequency",
            }
        )

        conn.register("df_customers", df_customers)
        conn.execute("INSERT INTO customers SELECT * FROM df_customers")
        logger.info(f"Inserted {len(df_customers)} rows into customers table.")

        # --- Load and insert products ---
        # TODO: Add your new columns
        # TODO: Change as needed to reflect YOUR data and file names, etc.

        df_products = pd.read_csv(DATA_PREPARED_DIR / "products_prepared.csv")
        df_products = df_products.rename(
            columns={
                "ProductID": "product_id",
                "ProductName": "product_name",
                "Category": "category",
                "UnitPrice": "unit_price",
                "stockquantity": "stock_quantity",
                "supplier": "supplier",
            }
        )

        conn.register("df_products", df_products)
        conn.execute("INSERT INTO products SELECT * FROM df_products")
        logger.info(f"Inserted {len(df_products)} rows into products table.")

        # --- Load and insert sales ---
        # TODO: Add your new columns
        # TODO: Change as needed to reflect YOUR data and file names, etc.

        df_sales = pd.read_csv(DATA_PREPARED_DIR / "sales_prepared.csv")
        df_sales = df_sales.rename(
            columns={
                "TransactionID": "sale_id",
                "SaleDate": "date",
                "CustomerID": "customer_id",
                "ProductID": "product_id",
                "StoreID": "store_id",
                "CampaignID": "campaign_id",
                "SaleAmount": "sales_amount",
                "Shipping": "shipping",
                "State": "state",
            }
        )

        # TODO: Decide if you want to drop any unused columns in any of the tables
        # Example of how to drop columns:
        expected_columns = [
            "sale_id",
            "date",
            "customer_id",
            "product_id",
            "store_id",
            "campaign_id",
            "sales_amount",
            "shipping",
            "state",
        ]
        df_sales = df_sales[[col for col in expected_columns if col in df_sales.columns]]

        conn.register("df_sales", df_sales)
        conn.execute("INSERT INTO sales SELECT * FROM df_sales")
        logger.info(f"Inserted {len(df_sales)} rows into sales table.")

        logger.info("All tables populated successfully.")

    except Exception as e:
        logger.error(f"Error populating DuckDB data warehouse: {e}")
        exit(1)
    finally:
        if conn:
            conn.close()


def main() -> None:
    logger.info("========================================")
    logger.info("Starting: create_dw_duckdb.py")
    logger.info("========================================")
    logger.info(f"Root:   {PROJECT_ROOT}")
    logger.info(f"Scripts:{SCRIPTS_DW_CREATE_DIR}")
    logger.info(f"SQL:    {SQL_DW_CREATE_DIR}")
    logger.info(f"Input:  {DATA_PREPARED_DIR}")
    logger.info(f"Output: {DW_PATH}")
    create_dw()
    populate_dw()
    logger.info("========================================")
    logger.info("Finished: create_dw_duckdb.py")
    logger.info("========================================")


if __name__ == "__main__":
    main()
