from sqlalchemy import create_engine
import snowflake.connector
import pandas as pd
import pymysql
from urllib.parse import quote_plus
from utils.logger import get_logger
logger = get_logger(__name__)



def mysql_engine(cfg):
    """
    Create a MySQL SQLAlchemy engine using the provided configuration.

    Args:
        cfg (dict): Configuration dictionary with keys:
            - user: Database username
            - password: Database password
            - host: Database host
            - port: Database port
            - database: Database name

    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine for MySQL
    """

    required = ("username", "password", "host", "port", "database")
    missing = [k for k in required if k not in cfg or not cfg.get(k)]
    if missing:
        logger.error("MySQL configuration missing keys: %s", missing)
        raise ValueError(f"Missing MySQL configuration keys: {missing}")

    try:
        quoted_pw = quote_plus(cfg['password'])
        engine = create_engine(
            f"mysql+pymysql://{cfg['username']}:{quoted_pw}@{cfg['host']}:{cfg['port']}/{cfg['database']}",
            pool_pre_ping=True,
        )
        logger.info("MySQL engine created successfully for %s:%s", cfg['host'], cfg['database'])
        return engine
    except Exception as e:
        logger.exception("Error creating MySQL engine: %s", e)
        raise


def snowflake_conn(cfg):
    """
    Create a Snowflake connection using the provided configuration.

    Args:
        cfg (dict): Configuration dictionary with keys:
            - account: Snowflake account name
            - username: Snowflake username
            - password: Snowflake password
            - warehouse: Snowflake warehouse name
            - database: Snowflake database name
            - schema: Snowflake schema name
            - role: Snowflake role name

    Returns:
        snowflake.connector.SnowflakeConnection: Connection object for Snowflake
    """
    required = ("username", "password", "account", "warehouse", "database", "schema", "role")
    missing = [k for k in required if k not in cfg or not cfg.get(k)]
    if missing:
        logger.error("Snowflake configuration missing keys: %s", missing)
        raise ValueError(f"Missing Snowflake configuration keys: {missing}")

    try:
        conn = snowflake.connector.connect(
            user=cfg['username'],
            password=cfg['password'],
            account=cfg['account'],
            warehouse=cfg['warehouse'],
            database=cfg['database'],
            schema=cfg['schema'],
            role=cfg['role'],
        )
        logger.info("Snowflake connection established for account %s, database %s", cfg['account'], cfg['database'])
        return conn
    except Exception as e:
        logger.exception("Error creating Snowflake connection: %s", e)
        raise 