import sys
import os
BASE_DIR = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(BASE_DIR)
from src.utils import logger
from sqlalchemy import create_engine


class AzureDatabase:
    ''' Class DatabaseSRC initalize connection to database source by sqlalchemy engine.
    https://docs.sqlalchemy.org/en/14/core/engines.html
    '''
    __connection = None

    @staticmethod
    def initialize(*args, **kwargs):
        AzureDatabase.__connection = create_engine(*args, **kwargs)

    @staticmethod
    def get_engine():
        return AzureDatabase.__connection

    @staticmethod
    def get_connection():
        return AzureDatabase.__connection.raw_connection()

    @staticmethod
    def return_connection(connection):
        connection.close()

    @staticmethod
    def close_all_connections():
        AzureDatabase.__connection.dispose()


class CursorAzureDatabase:
    ''' Class CursorAzureDatabase initalize the cursor of AzureDatabase.
    '''
    def __init__(self):
        self.conn = None
        self.cursor = None

    def __enter__(self):
        self.conn = AzureDatabase.get_connection()
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if exception_value:  
            self.conn.rollback()
            logger.log('BUG', f'[INITIALIZE_CONNECTION][CursorAzureDatabase][Exception]: {exception_value} ')
        else:
            self.cursor.close()
            self.conn.commit()
        AzureDatabase.return_connection(self.conn)


class ConnectionAzureDatabase:
    ''' Class ConnectionAzureDatabase initalize the connection of DatabaseSRC.
    '''
    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = AzureDatabase.get_connection()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            self.conn.rollback()
            logger.log('BUG', f'[INITIALIZE_CONNECTION][ConnectionAzureDatabase][Exception]: {exc_val} ')
        else:
            self.conn.commit()
        AzureDatabase.return_connection(self.conn)

if __name__ == '__main__':
    frame = None
    try:
        from config.ConfigReader import config_reader
        import pandas as pd
        import warnings
        import urllib
        warnings.filterwarnings('ignore')
        server = config_reader.azure_sql_server['server']
        database = config_reader.azure_sql_server['database']
        username = config_reader.azure_sql_server['username']
        password = config_reader.azure_sql_server['password'].strip('"')
        driver = config_reader.azure_sql_server['driver'].strip('"')
        odbc = config_reader.azure_sql_server['odbc'].strip('"').format(
            driver = driver, server = server, database = database,
            username = username, password = password
        )
        odbc_parse_quote = urllib.parse.quote_plus(odbc)
        url = config_reader.azure_sql_server['url'].strip('"').format(odbc_parse_quote = odbc_parse_quote)
        AzureDatabase.initialize(url)
        logger.log('INFO', f'Connected to {url}')

        stm = '''
                SELECT
                    account_id,
                    account_name,
                    account_start_date,
                    account_address,
                    account_type,
                    account_create_timestamp,
                    account_notes,
                    is_active
                FROM
                    [database-demo].dbo.account;
        '''
        with ConnectionAzureDatabase() as conn:
            frame = pd.read_sql(stm, conn)
        
        if frame is None or frame.empty:
            total_records = 0
            logger.log('WARNING', f'Number of records: {total_records}')
        else:
            total_records = len(frame)
        logger.log('INFO', f'Number of records: {total_records}')
    except Exception as e:
        logger.log('BUG', f'Exception: {e}')
    