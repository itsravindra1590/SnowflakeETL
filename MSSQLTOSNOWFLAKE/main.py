import yaml
from pathlib import Path
from utils.db_connections import mysql_engine,snowflake_conn
from extract.sqlserver_extractor import extract_to_csv
from load.snowflake_loader import load_csv_to_snowflake
from utils.logger import get_logger
logger = get_logger(__name__)
 
BASE = Path(__file__).resolve().parent
config_path = BASE / "config" / "config.yaml"

with open(config_path,"r") as file:
    config = yaml.safe_load(file)
    print(config)

def main():
    engine = mysql_engine(config['mysql'])
    sf_conn = snowflake_conn(config['snowflake'])
   
    try:

        for table in config['tables']:
            source = table['source_table']
            target = table['target_table']
            csv_file = f"{source}.csv"
            print(f"extracting {source}")
            extract_to_csv(engine, source, csv_file)
            print(f"loading {csv_file} to snowflake")
            load_csv_to_snowflake(sf_conn, config['snowflake'], csv_file, target)
            logger.info(f"Loaded {source} to {target} successfully in snowflake")
            print(f"Loaded {source} to {target} successfully in snowflake")

        
   
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.exception(f"An error occurred in main ETL process: {e}")
    finally:
            engine.dispose()
            sf_conn.close()
      

if __name__ == "__main__":
    main()