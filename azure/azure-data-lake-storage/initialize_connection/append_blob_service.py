# https://stackoverflow.com/questions/61317453/importerror-cannot-import-name-blockblobservice-from-azure-storage-blob
# please create a new python env and install 
'''
conda create -n DELibs-Azure-block-blob python=3.9 -y 
conda activate DELibs-Azure-block-blob
pip install azure-storage-blob==2.1.0 loguru pandas
'''

import os
import sys
BASE_DIR = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(BASE_DIR)
from config.ConfigReader import config_reader
from src.utils import logger
from azure.storage.blob import AppendBlobService

def append_data_from_file_path(append_blob_service: AppendBlobService, container_name: str, blob_path: str, 
                          file_path: str) -> None:
    try:
        append_blob_service.append_blob_from_path(container_name, blob_path, file_path)
        logger.log('INFO', f'Appended data {file_path} to {container_name}/{blob_path}')
    except Exception as e1:
        logger.log('WARNING', f'WARNING: {e1}')
        try:
            append_blob_service.create_blob(container_name, blob_path)
            logger.log('INFO', f'Created a new blob object in {container_name}/{blob_path}')
            append_blob_service.append_blob_from_path(container_name, blob_path, file_path)
            logger.log('INFO', f'Appended data {file_path} to {container_name}/{blob_path}')
        except Exception as e2:
            logger.log('BUG', f'[append_data_file_path]__Exception: {e2}')

def append_data_from_text(append_blob_service: AppendBlobService, container_name: str, blob_path: str, 
                     data: str) -> None:
    try:
        append_blob_service.append_blob_from_text(container_name, blob_path, data)
        logger.log('INFO', f'Appended data to {container_name}/{blob_path}')
    except Exception as e1:
        logger.log('WARNING', f'WARNING: {e1}')
        try:
            append_blob_service.create_blob(container_name, blob_path)
            logger.log('INFO', f'Created a new blob object in {container_name}/{blob_path}')
            append_blob_service.append_blob_from_text(container_name, blob_path, data)
            logger.log('INFO', f'Appended data to {container_name}/{blob_path}')
        except Exception as e2:
            logger.log('BUG', f'[append_data_file_path]__Exception: {e2}')

if __name__ == '__main__':
    def test_append_data_from_file_path() -> None:
        try:
            storage_account_name = config_reader.azure_storage['azure_storage_account_name']
            storage_account_key = config_reader.azure_storage['azure_storage_account_key'].strip('"')
            append_blob_service = AppendBlobService(account_name = storage_account_name, account_key = storage_account_key)
            container_name = config_reader.azure_storage['container_name']
            blob_base = config_reader.azure_storage['blob_base']
            target_file = 'append_temp_upload_2.txt'
            blob_path = os.path.join(blob_base, target_file)
            file_path = os.path.join('azure', 'azure-data-lake-storage', 'sample-data', 'temp.txt')
            append_data_from_file_path(append_blob_service, container_name, blob_path, file_path)
        except Exception as e:
            logger.log('BUG', f'Exception: {e}')
    
    # test_append_data_from_file_path()

    def test_append_data_from_text() -> None:
        try:
            storage_account_name = config_reader.azure_storage['azure_storage_account_name']
            storage_account_key = config_reader.azure_storage['azure_storage_account_key'].strip('"')
            append_blob_service = AppendBlobService(account_name = storage_account_name, account_key = storage_account_key)
            container_name = config_reader.azure_storage['container_name']
            blob_base = config_reader.azure_storage['blob_base']
            target_file = 'append_temp_upload_5.txt'
            blob_path = os.path.join(blob_base, target_file)
            data = '\nSome text'
            append_data_from_text(append_blob_service, container_name, blob_path, data)
        except Exception as e:
            logger.log('BUG', f'Exception: {e}')
    
    test_append_data_from_text()