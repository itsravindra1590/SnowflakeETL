import csv
from sqlalchemy import create_engine
from utils.db_connections import mysql_engine
import pandas as pd
from utils.logger import get_logger
logger = get_logger(__name__)


def extract_to_csv(engine, table_name, output_file, chunk_size=5):
    """
    Extract data from a SQL Server table and write to a CSV file in chunks.

    Args:
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine connected to SQL Server"""
    
    try:
        logger.info(f"Starting data extraction from {table_name} to {output_file}")
        query = f"SELECT * FROM {table_name} limit {chunk_size}"

        with engine.connect() as connection:
            df = pd.read_sql(query, connection) 
            df.to_csv(output_file, index=False)
            print(df.head(10))
        logger.info(f"Data extraction completed successfully for {table_name}")
        return "Data extracted successfully"
    except Exception as e:
        logger.info(f"Started extracting data from mysql databases")
        logger.exception(f"Error during data extraction using function {extract_to_csv} error: {e}")