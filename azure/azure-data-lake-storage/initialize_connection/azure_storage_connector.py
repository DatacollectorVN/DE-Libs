import os
import sys
BASE_DIR = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(BASE_DIR)
from config.ConfigReader import config_reader
from src.utils import logger
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings

class BlobServiceClientConnector():
    __blob_service_client = None

    @staticmethod
    def initialize_bsl_from_conn_string(*args, **kwargs):
        BlobServiceClientConnector.__blob_service_client = BlobServiceClient.from_connection_string(*args, **kwargs)
    
    @staticmethod
    def get_bsl():
        return BlobServiceClientConnector.__blob_service_client
    
    def get_container_client(container_name) -> object:
        return BlobServiceClientConnector.__blob_service_client.get_container_client(container_name)
    
    def get_blob_client(container_name, blob_path) -> object:
        return BlobServiceClientConnector.__blob_service_client.get_blob_client(container_name, blob_path)

class DataLakeServiceClientConnector():
    __service_client = None
    __file_system_client = None

    @staticmethod
    def initalize_dlsc_from_conn_string(*args, **kwargs):
        DataLakeServiceClientConnector.__service_client = DataLakeServiceClient.from_connection_string(*args, **kwargs)

    @staticmethod
    def initalize_dlsc_from_key(storage_account_name, storage_account_key):
        DataLakeServiceClientConnector.__service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential = storage_account_key)

    @staticmethod
    def initalize_fsc(*args, **kwargs):
        DataLakeServiceClientConnector.__file_system_client = DataLakeServiceClientConnector.__service_client.get_file_system_client(*args, **kwargs)

    @staticmethod
    def get_dlsc():
        return DataLakeServiceClientConnector.__service_client
    
    @staticmethod
    def get_fsc():
        return DataLakeServiceClientConnector.__file_system_client
    
    def close_fsc():
        try:
            DataLakeServiceClientConnector.__file_system_client.close()
            return True
        except Exception as e:
            logger.log('BUG', f'[close_fsc]__Exception: {e}')
            return False
    
    def get_dir_client(dir_path):
        file_system_client = DataLakeServiceClientConnector.get_fsc()
        return DataLakeServiceClientConnector.__service_client.get_directory_client(file_system_client.file_system_name, dir_path)

    def get_file_client(file_path):
        file_system_client = DataLakeServiceClientConnector.get_fsc()
        return DataLakeServiceClientConnector.__service_client.get_file_client(file_system_client.file_system_name, file_path)

if __name__ == '__main__':
    def test_bsc_connector() -> None:
        try:
            connect_str = config_reader.azure_storage['azure_storage_connection_string'].strip('"')
            BlobServiceClientConnector.initialize_bsl_from_conn_string(connect_str)
            blob_service_client = BlobServiceClientConnector.get_bsl()

            container_name = config_reader.azure_storage['container_name']
            container_client = BlobServiceClientConnector.get_container_client(container_name)
            
            blob_base = config_reader.azure_storage['blob_base']
            target_file = 'temp_upload_3.txt'
            blob_path = os.path.join(blob_base, target_file)
            blob_client = BlobServiceClientConnector.get_blob_client(container_name, blob_path)

            upload_file_path = os.path.join('azure', 'azure-data-lake-storage', 'sample-data', 'temp.txt')
            with open(upload_file_path, 'rb') as data:
                blob_client.upload_blob(data)
            
            logger.log('INFO', f'Upload file to {blob_path}')
        except Exception as e:
            logger.log('BUG', f'Exception: {e}')
        finally:
            # There is no need to explicitly close or release any resources when working with our clients. 
            # All the connections are managed by the underlying netty connection pool.
            pass
    
    # test_bsc_connector()

    def test_dlsc_connector():
        directory_client = None
        file_client = None
        try:
            storage_account_name = config_reader.azure_storage['azure_storage_account_name']
            storage_account_key = config_reader.azure_storage['azure_storage_account_key'].strip('"')
            container_name = config_reader.azure_storage['container_name']

            DataLakeServiceClientConnector.initalize_dlsc_from_key(storage_account_name, storage_account_key)
            DataLakeServiceClientConnector.initalize_fsc(container_name)

            dir_path = config_reader.azure_storage['blob_base']
            directory_client = DataLakeServiceClientConnector.get_dir_client(dir_path)
            
            if directory_client is not None and directory_client.exists():
                logger.log('INFO', f'Connected to {container_name}/{dir_path}')
            else:
                logger.log('WARNING', f'{container_name}/{dir_path} is not exist')
            
            target_file = 'temp_upload_4.txt'
            file_path = os.path.join(dir_path, target_file) 
            file_client = DataLakeServiceClientConnector.get_file_client(file_path)
            
            if file_client is not None and file_client.exists():
                logger.log('INFO', f'Connected to {file_path}')
            else:
                logger.log('WARNING', f'{file_path} is not exist')

        except Exception as e:
            logger.log('BUG', f'Exception: {e}')
        finally:
            flag = DataLakeServiceClientConnector.close_fsc()
            if flag:
                logger.log('INFO', 'Closed connection of flie system client')
            else:
                logger.log('INFO', 'Could not close connection of flie system client, please check the errors')
    
    test_dlsc_connector()