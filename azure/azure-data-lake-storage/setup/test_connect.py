import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import uuid

if __name__ == '__main__':
    try:
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        # Create the BlobServiceClient object which will be used to create a container client
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # # Create a unique name for the container
        # container_name = str(uuid.uuid4())

        # print(container_name)
        # # Create the container
        # container_client = blob_service_client.create_container(container_name)
        containers = blob_service_client.list_containers(name_starts_with = 'demo')
        print(containers)
        print(list(containers))

    except Exception as e:
        print(e)