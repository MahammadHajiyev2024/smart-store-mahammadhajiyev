# Imports at the top

from pathlib import Path
import sqlite3

import pandas as pd

from analytics_project.utils_logger import logger

# Global constants for paths and key directories

PROJECT_ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT_DIR / "data"
PREPARED_DATA_DIR = DATA_DIR / "prepared"
DW_DIR = DATA_DIR / "dw"
DW_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DW_DIR / "smart_store_dw.db"


def create_schema(cursor: sqlite3.Cursor) -> None:
    """Create tables in the data warehouse if they don't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer (
            customer_id INTEGER PRIMARY KEY,
            name TEXT,
            region TEXT,
            join_date TEXT,
            number_of_purchases INTEGER,
            shopping_frequency TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            unit_price REAL,
            stock_quantity INTEGER,
            supplier TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sale (
            sale_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            store_id,
            campaign_id,
            sale_amount REAL,
            sale_date TEXT,
            shipping REAL,
            state TEXT,
            FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
            FOREIGN KEY (product_id) REFERENCES product (product_id)
        )
    """)


def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """Delete all existing records from the customer, product, and sale tables."""
    cursor.execute("DELETE FROM customer")
    cursor.execute("DELETE FROM product")
    cursor.execute("DELETE FROM sale")


def insert_customers(customers_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert customer data into the customer table."""
    logger.info(f"Inserting {len(customers_df)} customer rows.")
    customers_df.to_sql("customer", cursor.connection, if_exists="append", index=False)


def insert_products(products_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert product data into the product table."""
    logger.info(f"Inserting {len(products_df)} product rows.")
    products_df.to_sql("product", cursor.connection, if_exists="append", index=False)


def insert_sales(sales_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert sales data into the sales table."""
    logger.info(f"Inserting {len(sales_df)} sale rows.")
    sales_df.to_sql("sale", cursor.connection, if_exists="append", index=False)


def load_data_to_db() -> None:
    """Load clean data into the data warehouse."""
    logger.info("Starting ETL: loading clean data into the warehouse.")

    # Make sure the warehouse directory exists
    DW_DIR.mkdir(parents=True, exist_ok=True)

    # If an old database exists, remove and recreate with the latest table definitions.
    if DB_PATH.exists():
        logger.info(f"Removing existing warehouse database at: {DB_PATH}")
        DB_PATH.unlink()

    # Initialize a connection variable
    # before the try block so we can close it in finally
    conn: sqlite3.Connection | None = None

    try:
        # Connect to SQLite. Create the file if it doesn't exist
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Create schema and clear existing records
        create_schema(cursor)
        delete_existing_records(cursor)

        # Load prepared data using pandas
        customers_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("customers_prepared.csv"))
        products_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("products_prepared.csv"))
        # TODO: Uncomment after implementing sales data preparation
        sales_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("sales_prepared.csv"))

        # Rename clean columns to match database schema if necessary
        # Clean column name : Database column name
        customers_df = customers_df.rename(
            columns={
                "CustomerID": "customer_id",
                "Name": "name",
                "Region": "region",
                "JoinDate": "join_date",
                "NumberOfPurshases": "number_of_purchases",
                "ShoppingFrequency": "shopping_frequency",
            }
        )
        logger.info(f"Customer columns (cleaned): {list(customers_df.columns)}")

        # Rename clean columns to match database schema if necessary
        # Clean column name : Database column name
        products_df = products_df.rename(
            columns={
                "ProductID": "product_id",
                "ProductName": "product_name",
                "Category": "category",
                "UnitPrice": "unit_price",
                "StockQuantity": "stock_quantity",
                "Supplier": "supplier",
            }
        )
        logger.info(f"Product columns (cleaned):  {list(products_df.columns)}")

        # TODO: Rename sales_df columns to match database schema if necessary
        sales_df = sales_df.rename(
            columns={
                "TransactionID": "sale_id",
                "SaleDate": "sale_date",
                "CustomerID": "customer_id",
                "ProductID": "product_id",
                "StoreID": "store_id",
                "CampaignID": "campaign_id",
                "SaleAmount": "sale_amount",
                "Shipping": "shipping",
                "State": "state",
            }
        )
        # Insert data into the database for all tables

        insert_customers(customers_df, cursor)

        insert_products(products_df, cursor)

        # TODO: Uncomment after implementing sales data preparation
        insert_sales(sales_df, cursor)

        conn.commit()
        logger.info("ETL finished successfully. Data loaded into the warehouse.")
    finally:
        # Regardless of success or failure, close the DB connection if it exists
        if conn is not None:
            logger.info("Closing database connection.")
            conn.close()


if __name__ == "__main__":
    load_data_to_db()
