import os
import sys
BASE_DIR = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(BASE_DIR)
from src.utils import logger

def blob_upload_file(blob_client, src_file_path, blob_path):
    try:
        with open(src_file_path, 'rb') as data:
            blob_client.upload_blob(data)
        
        logger.log('INFO', f'Uploaded {src_file_path} to {blob_path}')
    except Exception as e:
        logger.log('BUG', f'[blob_upload_file]__Exception: {e}')

def blob_download_file(blob_client, dest_file_path, blob_path):
    try:
        download_stream = blob_client.download_blob()
        with open(dest_file_path, "wb") as my_blob:
            download_stream = blob_client.download_blob()
            my_blob.write(download_stream.readall())
        
        logger.log('INFO', f'Downloaded {blob_path} to {dest_file_path}')
    except Exception as e:
        logger.log('BUG', f'[blob_upload_file]__Exception: {e}')

def data_lake_upload_file(file_client, src_file_path, dest_file_path):
    '''Upload small file .txt, .csv'''
    offset = 0
    try:
        with open(src_file_path, 'r') as file:
            file_contents = file.read()

        file_client.append_data(data = file_contents, offset = offset, length = len(file_contents))
        file_client.flush_data(len(file_contents))
        
        logger.log('INFO', f'Uploaded {src_file_path} to {dest_file_path}')
    except Exception as e:
        logger.log('BUG', f'[data_lake_upload_file]__Exception: {e}')
    
def data_lake_upload_large_file(file_client, src_file_path, dest_file_path):
    try:
        with open(src_file_path, 'r') as file:
            file_contents = file.read()
        
        file_client.upload_data(file_contents)
        logger.log('INFO', f'Uploaded {src_file_path} to {dest_file_path}')
    except Exception as e:
        logger.log('BUG', f'[data_lake_upload_file]__Exception: {e}')

if __name__ == '__main__':
    from config.ConfigReader import config_reader
    
    def test_blob_upload_data_file() -> None:
        from initialize_connection.azure_storage_connector import BlobServiceClientConnector
        try:
            connect_str = config_reader.azure_storage['azure_storage_connection_string'].strip('"')
            BlobServiceClientConnector.initialize_bsl_from_conn_string(connect_str)
            
            container_name = config_reader.azure_storage['container_name']
            blob_base = config_reader.azure_storage['blob_base']
            target_file = 'temp_img2.jpeg'
            blob_path = os.path.join(blob_base, target_file)
            blob_client = BlobServiceClientConnector.get_blob_client(container_name, blob_path)

            upload_file_path = os.path.join('azure', 'azure-data-lake-storage', 'sample-data', 'black_pink_image.jpeg')
            blob_upload_file(blob_client, upload_file_path, blob_path)
            
        except Exception as e:
            logger.log('BUG', f'Exception: {e}')
    
    # test_blob_upload_data_file()

    def test_blob_download_data_file() -> None:
        from initialize_connection.azure_storage_connector import BlobServiceClientConnector
        try:
            connect_str = config_reader.azure_storage['azure_storage_connection_string'].strip('"')
            BlobServiceClientConnector.initialize_bsl_from_conn_string(connect_str)
            
            container_name = config_reader.azure_storage['container_name']
            blob_base = config_reader.azure_storage['blob_base']
            target_file = 'temp_img2.jpeg'
            blob_path = os.path.join(blob_base, target_file)
            blob_client = BlobServiceClientConnector.get_blob_client(container_name, blob_path)

            dest_file_path = os.path.join('azure', 'azure-data-lake-storage', 'sample-data', 'black_pink_image_output.jpeg')
            blob_download_file(blob_client, dest_file_path, blob_path)
            
        except Exception as e:
            logger.log('BUG', f'Exception: {e}')
    
    # test_blob_download_data_file()

    def test_data_lake_upload_file() -> None:
        from initialize_connection.azure_storage_connector import DataLakeServiceClientConnector
        
        file_client = None
        try:
            storage_account_name = config_reader.azure_storage['azure_storage_account_name']
            storage_account_key = config_reader.azure_storage['azure_storage_account_key'].strip('"')
            container_name = config_reader.azure_storage['container_name']

            DataLakeServiceClientConnector.initalize_dlsc_from_key(storage_account_name, storage_account_key)
            DataLakeServiceClientConnector.initalize_fsc(container_name)

            dir_path = config_reader.azure_storage['blob_base']
            target_file = 'temp_upload_11.txt'
            file_path = os.path.join(dir_path, target_file) 
            file_client = DataLakeServiceClientConnector.get_file_client(file_path)
            
            if not file_client.exists():
                logger.log('WARNING', f'File client does not exist and start to create a new')
                file_client.create_file()

            src_file_path = os.path.join('azure', 'azure-data-lake-storage', 'sample-data', 'temp.txt')
            data_lake_upload_file(file_client, src_file_path, file_path, 10)
        except Exception as e:
            logger.log('BUG', f'Exception: {e}')
        finally:
            flag = DataLakeServiceClientConnector.close_fsc()
            if flag:
                logger.log('INFO', 'Closed connection of flie system client')
            else:
                logger.log('INFO', 'Could not close connection of flie system client, please check the errors')
    
    test_data_lake_upload_file()