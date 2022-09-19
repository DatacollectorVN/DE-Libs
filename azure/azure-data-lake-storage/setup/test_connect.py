import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

if __name__ == '__main__':
    try:
        # set env AZURE_STORAGE_CONNECTION_STRING before
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING') 

        # Create the BlobServiceClient object which will be used to create a container client
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        containers = list(blob_service_client.list_containers())
        for container in containers:
            print(f'Name: {container["name"]}')

    except Exception as e:
        print(e)
    finally:
        # There is no need to explicitly close or release any resources when working with our clients. 
        # All the connections are managed by the underlying netty connection pool.
        pass