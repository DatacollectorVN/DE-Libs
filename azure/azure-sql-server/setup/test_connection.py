if __name__ == '__main__':
    import os
    import sys
    BASE_DIR = os.path.join(os.path.dirname(__file__), '..')
    sys.path.append(BASE_DIR)
    from config.ConfigReader import config_reader
    from src.utils import logger
    import urllib
    from sqlalchemy import create_engine
    import pandas as pd
    import warnings
    warnings.filterwarnings('ignore')

    def test_connection() -> None:
        engine = None
        try:
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
            engine = create_engine(url)
            conn = engine.raw_connection()
        except Exception as e:
            logger.log('BUG', f'Exception: {e}')
        finally:
            conn.commit()
            conn.close()
    
    # test_connection()
    
    def test_query() -> None:
        engine = None
        try:
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
            engine = create_engine(url)
            conn = engine.raw_connection()
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
            frame = pd.read_sql(stm, conn)
            if frame.empty:
                logger.log('WARNING', 'No data')
            print(frame)
        except Exception as e:
            logger.log('BUG', f'Exception: {e}')
        finally:
            conn.commit()
            conn.close()
    
    test_query()