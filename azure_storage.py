from azure.storage.blob import BlobServiceClient
from azure.storage.blob import PublicAccess

AZURE_BLOB_CONNECTION = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10003/devstoreaccount1;"
CONTAINER = "dados-pregao-bolsa"

def save_file_to_blob(file_name, local_path_file):

    service = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)
    container = service.get_container_client(CONTAINER)
    try:
        service.create_container(CONTAINER, public_access=PublicAccess.Container)
    except Exception as e:
        pass #container ja existe

    with open(local_path_file, "rb") as data:
        container.upload_blob(name=file_name, data=data, overwrite=True)

def get_file_from_blob(file_name):
    service = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)
    container = service.get_container_client(CONTAINER)
    try:
        service.create_container(CONTAINER, public_access=PublicAccess.Container)
    except Exception as e:
        pass #container ja existe

    #Cria a referencia do arquivo no azure
    blob_client = container.get_blob_client(file_name)

    try:
        download_stream = blob_client.download_blob()
        blob_content = download_stream.readall().decode("utf-8")
        return blob_content
    except Exception as e:
        print("Error ao obter arquivo")

def list_blobs():
    """Lista todos os arquivos no blob storage"""
    service = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)
    container = service.get_container_client(CONTAINER)
    try:
        service.create_container(CONTAINER, public_access=PublicAccess.Container)
    except Exception as e:
        pass  # container ja existe

    print(f"Arquivos no container '{CONTAINER}':")
    try:
        blob_list = container.list_blobs()
        for blob in blob_list:
            print(f"  - {blob.name} (tamanho: {blob.size} bytes)")
    except Exception as e:
        print(f"Erro ao listar blobs: {e}")



