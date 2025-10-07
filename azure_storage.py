from azure.storage.blob import BlobServiceClient
from azure.storage.blob import PublicAccess
import os
import time
from datetime import datetime

AZURE_BLOB_CONNECTION = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10003/devstoreaccount1;"
CONTAINER = "dados-pregao-bolsa"

def print_timestamp():
    """Retorna timestamp formatado para logs"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def format_file_size(size_bytes):
    """Formata tamanho de arquivo em unidades legíveis"""
    if size_bytes == 0:
        return "0 B"
    size_names = ["B", "KB", "MB", "GB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024
        i += 1
    return f"{size_bytes:.1f} {size_names[i]}"

def save_file_to_blob(file_name, local_path_file):
    """
    Salva arquivo local no blob storage com feedback detalhado
    
    Args:
        file_name: Nome do arquivo no blob storage
        local_path_file: Caminho do arquivo local
    """
    upload_start = time.time()
    
    # Verificar se arquivo local existe
    if not os.path.exists(local_path_file):
        print(f"[{print_timestamp()}] [ERROR] ❌ Arquivo local não encontrado: {local_path_file}")
        return False
    
    file_size = os.path.getsize(local_path_file)
    print(f"[{print_timestamp()}] [INFO] ☁️ Iniciando upload para blob storage...")
    print(f"[{print_timestamp()}] [INFO] 📁 Arquivo local: {local_path_file} ({format_file_size(file_size)})")
    print(f"[{print_timestamp()}] [INFO] 🎯 Destino blob: {file_name}")
    
    try:
        # Conectar ao serviço
        connect_start = time.time()
        service = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)
        container = service.get_container_client(CONTAINER)
        connect_time = time.time() - connect_start
        
        print(f"[{print_timestamp()}] [OK] ✅ Conectado ao Azurite em {connect_time:.2f}s")
        
        # Criar container se não existir
        try:
            service.create_container(CONTAINER, public_access=PublicAccess.Container)
            print(f"[{print_timestamp()}] [INFO] 📦 Container '{CONTAINER}' criado")
        except Exception as e:
            print(f"[{print_timestamp()}] [INFO] 📦 Container '{CONTAINER}' já existe")

        # Upload do arquivo
        upload_file_start = time.time()
        with open(local_path_file, "rb") as data:
            print(f"[{print_timestamp()}] [INFO] ⬆️ Fazendo upload do arquivo...")
            
            # Upload com overwrite habilitado
            blob_client = container.upload_blob(name=file_name, data=data, overwrite=True)
            
        upload_file_time = time.time() - upload_file_start
        total_time = time.time() - upload_start
        
        # Verificar se upload foi bem-sucedido
        try:
            blob_props = container.get_blob_client(file_name).get_blob_properties()
            uploaded_size = blob_props.size
            
            print(f"[{print_timestamp()}] [OK] ✅ Upload concluído com sucesso!")
            print(f"[{print_timestamp()}] [INFO] 📊 ESTATÍSTICAS DO UPLOAD:")
            print(f"[{print_timestamp()}] [INFO]   📁 Arquivo: {file_name}")
            print(f"[{print_timestamp()}] [INFO]   📏 Tamanho: {format_file_size(uploaded_size)}")
            print(f"[{print_timestamp()}] [INFO]   ⏱️  Tempo upload: {upload_file_time:.2f}s")
            print(f"[{print_timestamp()}] [INFO]   ⏱️  Tempo total: {total_time:.2f}s")
            
            if upload_file_time > 0:
                rate = file_size / upload_file_time
                print(f"[{print_timestamp()}] [INFO]   📈 Taxa: {format_file_size(rate)}/s")
            
            return True
            
        except Exception as verify_error:
            print(f"[{print_timestamp()}] [WARN] ⚠️ Não foi possível verificar upload: {verify_error}")
            return True  # Upload provavelmente foi bem-sucedido
            
    except Exception as e:
        total_time = time.time() - upload_start
        print(f"[{print_timestamp()}] [ERROR] ❌ Falha no upload após {total_time:.2f}s: {e}")
        return False

def get_file_from_blob(file_name):
    """
    Baixa arquivo do blob storage com feedback detalhado
    
    Args:
        file_name: Nome do arquivo no blob storage
        
    Returns:
        str: Conteúdo do arquivo ou None em caso de erro
    """
    download_start = time.time()
    
    print(f"[{print_timestamp()}] [INFO] ☁️ Iniciando download do blob storage...")
    print(f"[{print_timestamp()}] [INFO] 🎯 Arquivo: {file_name}")
    
    try:
        # Conectar ao serviço
        connect_start = time.time()
        service = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)
        container = service.get_container_client(CONTAINER)
        connect_time = time.time() - connect_start
        
        print(f"[{print_timestamp()}] [OK] ✅ Conectado ao Azurite em {connect_time:.2f}s")
        
        # Verificar se container existe
        try:
            service.create_container(CONTAINER, public_access=PublicAccess.Container)
            print(f"[{print_timestamp()}] [INFO] 📦 Container '{CONTAINER}' criado")
        except Exception as e:
            print(f"[{print_timestamp()}] [INFO] 📦 Container '{CONTAINER}' já existe")

        # Obter referência do arquivo
        blob_client = container.get_blob_client(file_name)
        
        # Verificar se arquivo existe e obter propriedades
        try:
            blob_props = blob_client.get_blob_properties()
            blob_size = blob_props.size
            last_modified = blob_props.last_modified
            
            print(f"[{print_timestamp()}] [INFO] 📄 Arquivo encontrado:")
            print(f"[{print_timestamp()}] [INFO]   📏 Tamanho: {format_file_size(blob_size)}")
            print(f"[{print_timestamp()}] [INFO]   📅 Última modificação: {last_modified}")
            
        except Exception as props_error:
            print(f"[{print_timestamp()}] [ERROR] ❌ Arquivo não encontrado no blob storage: {file_name}")
            print(f"[{print_timestamp()}] [ERROR] Detalhes: {props_error}")
            return None

        # Download do arquivo
        download_file_start = time.time()
        print(f"[{print_timestamp()}] [INFO] ⬇️ Fazendo download do arquivo...")
        
        download_stream = blob_client.download_blob()
        blob_content = download_stream.readall().decode("utf-8")
        
        download_file_time = time.time() - download_file_start
        total_time = time.time() - download_start
        
        content_size = len(blob_content.encode('utf-8'))
        
        print(f"[{print_timestamp()}] [OK] ✅ Download concluído com sucesso!")
        print(f"[{print_timestamp()}] [INFO] 📊 ESTATÍSTICAS DO DOWNLOAD:")
        print(f"[{print_timestamp()}] [INFO]   📁 Arquivo: {file_name}")
        print(f"[{print_timestamp()}] [INFO]   📏 Tamanho: {format_file_size(content_size)}")
        print(f"[{print_timestamp()}] [INFO]   ⏱️  Tempo download: {download_file_time:.2f}s")
        print(f"[{print_timestamp()}] [INFO]   ⏱️  Tempo total: {total_time:.2f}s")
        
        if download_file_time > 0:
            rate = content_size / download_file_time
            print(f"[{print_timestamp()}] [INFO]   📈 Taxa: {format_file_size(rate)}/s")
        
        return blob_content
        
    except Exception as e:
        total_time = time.time() - download_start
        print(f"[{print_timestamp()}] [ERROR] ❌ Erro ao baixar arquivo após {total_time:.2f}s: {e}")
        return None

def list_blobs():
    """Lista todos os arquivos no blob storage com informações detalhadas"""
    list_start = time.time()
    
    print(f"[{print_timestamp()}] [INFO] 📋 Listando arquivos no blob storage...")
    print(f"[{print_timestamp()}] [INFO] 📦 Container: '{CONTAINER}'")
    
    try:
        # Conectar ao serviço
        connect_start = time.time()
        service = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)
        container = service.get_container_client(CONTAINER)
        connect_time = time.time() - connect_start
        
        print(f"[{print_timestamp()}] [OK] ✅ Conectado ao Azurite em {connect_time:.2f}s")
        
        # Verificar se container existe
        try:
            service.create_container(CONTAINER, public_access=PublicAccess.Container)
            print(f"[{print_timestamp()}] [INFO] 📦 Container '{CONTAINER}' criado")
        except Exception as e:
            print(f"[{print_timestamp()}] [INFO] 📦 Container '{CONTAINER}' já existe")

        # Listar blobs
        listing_start = time.time()
        blob_list = list(container.list_blobs())
        listing_time = time.time() - listing_start
        
        if not blob_list:
            print(f"[{print_timestamp()}] [INFO] 📂 Container vazio - nenhum arquivo encontrado")
            return
        
        total_size = 0
        print(f"[{print_timestamp()}] [INFO] 📋 Arquivos encontrados ({len(blob_list)} itens):")
        print(f"[{print_timestamp()}] [INFO] {'Nome':<30} {'Tamanho':<12} {'Última modificação'}")
        print(f"[{print_timestamp()}] [INFO] {'-' * 70}")
        
        for blob in blob_list:
            total_size += blob.size
            size_str = format_file_size(blob.size)
            modified_str = blob.last_modified.strftime("%Y-%m-%d %H:%M:%S") if blob.last_modified else "N/A"
            
            print(f"[{print_timestamp()}] [INFO] {blob.name:<30} {size_str:<12} {modified_str}")
        
        total_time = time.time() - list_start
        
        print(f"[{print_timestamp()}] [INFO] {'-' * 70}")
        print(f"[{print_timestamp()}] [OK] ✅ Listagem concluída em {total_time:.2f}s")
        print(f"[{print_timestamp()}] [INFO] 📊 RESUMO:")
        print(f"[{print_timestamp()}] [INFO]   📁 Arquivos: {len(blob_list)}")
        print(f"[{print_timestamp()}] [INFO]   📏 Tamanho total: {format_file_size(total_size)}")
        
    except Exception as e:
        total_time = time.time() - list_start
        print(f"[{print_timestamp()}] [ERROR] ❌ Erro ao listar arquivos após {total_time:.2f}s: {e}")
        print(f"[{print_timestamp()}] [INFO] 💡 Dica: Verifique se o Azurite está rodando com 'docker-compose up -d'")



