import os
from utils.logger import get_logger
logger = get_logger(__name__)

def load_csv_to_snowflake(conn:str,cfg:str,csv_file:str,target_table:str):
    
    try:

        logger.info(f"Starting to load {csv_file} into Snowflake table {target_table}")
        cur = conn.cursor()
        cur.execute(f"""
                PUT file://{csv_file} @{cfg['stage']} 
                AUTO_COMPRESS=TRUE
                overwrite=true;
                """)

        cur.execute(f"""
                COPY INTO {target_table}
                FROM @{cfg['stage']}
                FILE_FORMAT = {cfg['file_format']}
                ON_ERROR = 'CONTINUE';
                """)
        cur.close()
        os.remove(csv_file)
        logger.info(f"Successfully loaded {csv_file} into Snowflake table {target_table}")
    except Exception as e:
        logger.exception(f"Error during data loading: {e}")