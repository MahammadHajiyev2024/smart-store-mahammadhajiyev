"""
scripts/dw_create/verify_dw_dbt.py

This script verifies that your DuckDB data warehouse
has been successfully created and populated.
It checks for:
- Presence of the database file
- Existence of the expected tables: customers, products, and sales
- Row counts for each table
- A sample of 5 rows for data

** UNLESS YOU ADD TABLES OR CHANGE THE SCHEMA,
  THIS SCRIPT SHOULD NOT NEED TO BE MODIFIED**
"""

# import from Python standard libraries
import pathlib
import sys

# import from external libraries
import duckdb

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

# dbt project directory
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
DATA_PREPARED_DIR = DATA_DIR / "prepared"

# Output DW directory
DW_DIR = PROJECT_ROOT / "dw"

# Output DW SQLite database path
DW_PATH = DW_DIR / "smart_sales.duckdb"

# Ensure necessary directories exist
DW_DIR.mkdir(parents=True, exist_ok=True)

# === Functions ===


def verify_dw():
    """Verify the structure and data of the DuckDB Data Warehouse."""
    logger.info("Verifying DuckDB Data Warehouse...")

    # Check if the database exists
    if not DW_PATH.exists():
        logger.error(f"Database file not found at {DW_PATH}")
        exit(1)  # Exit with error code 1

    # Connect to DuckDB
    try:
        conn = duckdb.connect(database=str(DW_PATH), read_only=True)
        logger.info(f"Connected to database at {DW_PATH}")
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        exit(2)  # Exit with error code 2

    # Define expected tables - no prefix is required, but can be specified
    tables = ["customers", "products", "sales"]

    # Verify each table
    for table in tables:
        logger.info(f"--- Verifying table: {table} ---")
        try:
            # Check if the table exists
            result = conn.execute(f"SELECT * FROM {table} LIMIT 5").fetchdf()
            if result.empty:
                logger.warning(f"Table '{table}' is empty.")
            else:
                logger.info(f"Sample data from '{table}':\n{result}")

            # Count rows
            row_count = conn.execute(f"SELECT COUNT(*) AS row_count FROM {table}").fetchone()
            logger.info(f"Row count for '{table}': {row_count[0]}")

        except Exception as e:
            logger.error(f"Error verifying '{table}': {e}")
            exit(3)  # Exit with error code 3

    # Close the connection
    conn.close()
    logger.info("Verification complete.")


def main():
    logger.info("========================================")
    logger.info("Starting: verify_dw_dbt.py")
    logger.info("========================================")
    verify_dw()
    logger.info("========================================")
    logger.info("Finished: verify_dw_dbt.py")
    logger.info("========================================")


if __name__ == "__main__":
    main()
