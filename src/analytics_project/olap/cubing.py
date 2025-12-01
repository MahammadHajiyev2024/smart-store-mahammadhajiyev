# Module 6: OLAP Cubing Script

import pathlib
import sqlite3
import pandas as pd

from analytics_project.utils_logger import logger

# Define paths
THIS_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent
DW_DIR: pathlib.Path = THIS_DIR  ## src/analytics_project/olap/
PACKAGE_DIR: pathlib.Path = DW_DIR.parent  ## src/analytics_project
SRC_DIR: pathlib.Path = PACKAGE_DIR.parent  ## src
PROJECT_ROOT_DIR: pathlib.Path = SRC_DIR.parent  ## project_root/

# Data directories
DATA_DIR: pathlib.Path = PROJECT_ROOT_DIR / "data"
WAREHOUSE_DIR: pathlib.Path = DATA_DIR / "dw"

# Warehouse database location
DB_PATH: pathlib.Path = WAREHOUSE_DIR / "smart_store_dw.db"

# Output directory for OLAP cubes
OLAP_OUTPUT_DIR: pathlib.Path = DATA_DIR / "olap_cubing_outputs"

# Log paths and key directories for debugging
logger.info(f"THIS_DIR:            {THIS_DIR}")
logger.info(f"DW_DIR:              {DW_DIR}")
logger.info(f"PACKAGE_DIR:         {PACKAGE_DIR}")
logger.info(f"SRC_DIR:             {SRC_DIR}")
logger.info(f"PROJECT_ROOT_DIR:    {PROJECT_ROOT_DIR}")

logger.info(f"DATA_DIR:            {DATA_DIR}")
logger.info(f"WAREHOUSE_DIR:       {WAREHOUSE_DIR}")
logger.info(f"DB_PATH:             {DB_PATH}")
logger.info(f"OLAP_OUTPUT_DIR:     {OLAP_OUTPUT_DIR}")

# Create output directory if it doesn't exist
OLAP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def ingest_sales_data_from_dw() -> pd.DataFrame:
    """Ingest sales data from SQLite data warehouse."""

    try:
        conn = sqlite3.connect(DB_PATH)
        sales_df = pd.read_sql_query("SELECT * FROM sale", conn)
        conn.close()
        logger.info("Successfully ingested sales data from the data warehouse.")
        return sales_df
    except Exception as e:
        logger.error(f"Error ingesting sales data: {e}")
        raise


def create_olap_cube(sales_df: pd.DataFrame, dimensions: list, measures: dict) -> pd.DataFrame:
    """Create an OLAP cube by aggregating data across multiple dimensions.

    Args:
        sales_df (pd.DataFrame): The sales data.
        dimensions (list): List of column names to group by.
        measures (dict): Dictionary of aggregation functions for measures.

    Returns:
        pd.DataFrame: The multidimensional OLAP cube.
    """

    try:
        # Group by the product ID
        grouped = sales_df.groupby(dimensions)

        # Aggregate using sale_amount
        cube = grouped.agg(measures).reset_index()

        # Add a list of sale IDs for traceability
        """cube['sale_ids'] = grouped["sale_id"].apply(list).reset_index(drop=True)"""

        # Generate explicit column names
        explict_columns = generate_column_names(dimensions, measures)
        """explict_columns.append('sale_ids')"""  # Include the traceability column
        cube.columns = explict_columns
        logger.info("Successfully created OLAP cube.")
        return cube
    except Exception as e:
        logger.error(f"Error creating OLAP cube: {e}")
        raise


def generate_column_names(dimensions: list, measures: dict) -> list:
    """Generate explicit column names for OLAP cube, ensuring no trailing underscores.

    Args:
        dimensions (list): List of dimension columns.
        measures (dict): Dictionary of measures with aggregation functions.

    Returns:
        list: Explicit column names.
    """
    column_names = dimensions.copy()
    for column, agg_func in measures.items():
        if isinstance(agg_func, list):
            for func in agg_func:
                column_names.append(f"{column}_{func}")

        else:
            column_names.append(f"{column}_{agg_func}")
    # Remove trailing underscores from all column names
    column_names = [name.rstrip('_') for name in column_names]
    logger.info(f"Generated column names: {column_names}")
    return column_names


def write_cube_to_csv(cube: pd.DataFrame, filename: str) -> None:
    """Write the OLAP cube to a CSV file."""
    try:
        output_path = OLAP_OUTPUT_DIR.joinpath(filename)
        cube.to_csv(output_path, index=False)
        logger.info(f"Successfully saved OLAP cube to {output_path}")
    except Exception as e:
        logger.error(f"Error writing OLAP cube to CSV: {e}")
        raise


def main():
    """Execute the OLAP cubing process."""
    logger.info("Starting OLAP cubing process...")

    # Ingest sales data
    sales_df = ingest_sales_data_from_dw()

    if sales_df.empty:
        logger.warning(
            "Warning: The sales table is empty. "
            "The OLAP cube will only contain column headers."
            "Fix: Prepare raw data and run the ETL step to load the data warehouse."
        )

    # Define dimensions and measures for the OLAP cube
    dimensions = ["product_id"]
    measures = {"sale_amount": ["sum"]}

    # Create OLAP cube
    olap_cube = create_olap_cube(sales_df, dimensions, measures)

    # Save OLAP cube to CSV
    write_cube_to_csv(olap_cube, "sales_by_product_cube.csv")

    logger.info("OLAP Cubing process completed successfully.")
    logger.info(f"Please see outputs in {OLAP_OUTPUT_DIR}")


if __name__ == "__main__":
    main()
