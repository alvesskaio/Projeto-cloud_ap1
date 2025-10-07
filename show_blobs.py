#!/usr/bin/env python3
"""
Script para visualizar conteúdo do Blob Storage (Azurite)

Exibe informações detalhadas sobre os arquivos armazenados
no container de dados da B3.
"""

from azure_storage import list_blobs
from datetime import datetime

def show_blob_storage():
    """
    Exibe o conteúdo do blob storage de forma organizada
    """
    print("=" * 60)
    print("📁 CONTEÚDO DO BLOB STORAGE (AZURITE)")
    print("=" * 60)

    try:
        from azure.storage.blob import BlobServiceClient
        from azure_storage import AZURE_BLOB_CONNECTION, CONTAINER

        service = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)
        container = service.get_container_client(CONTAINER)

        # Lista todos os blobs
        blob_list = list(container.list_blobs())

        if not blob_list:
            print("📭 Container vazio - nenhum arquivo encontrado")
            return

        print(f"📊 Container: {CONTAINER}")
        print(f"📄 Total de arquivos: {len(blob_list)}")
        print("-" * 60)

        total_size = 0
        for i, blob in enumerate(blob_list, 1):
            size_mb = blob.size / (1024 * 1024)
            total_size += blob.size

            # Tenta extrair data do nome do arquivo
            try:
                if "BVBG186_" in blob.name:
                    date_part = blob.name.replace("BVBG186_", "").replace(".xml", "")
                    if len(date_part) == 6:  # YYMMDD
                        year = 2000 + int(date_part[:2])
                        month = int(date_part[2:4])
                        day = int(date_part[4:6])
                        date_str = f"{day:02d}/{month:02d}/{year}"
                    else:
                        date_str = "Data não identificada"
                else:
                    date_str = "Formato não padrão"
            except:
                date_str = "Data inválida"

            print(f"{i:2d}. 📄 {blob.name}")
            print(f"    📅 Data: {date_str}")
            print(f"    📦 Tamanho: {size_mb:.1f} MB ({blob.size:,} bytes)")
            print(f"    🕒 Modificado: {blob.last_modified.strftime('%d/%m/%Y %H:%M:%S')}")
            print()

        total_size_mb = total_size / (1024 * 1024)
        print("=" * 60)
        print(f"📊 RESUMO:")
        print(f"   Total de arquivos: {len(blob_list)}")
        print(f"   Tamanho total: {total_size_mb:.1f} MB ({total_size:,} bytes)")
        print("=" * 60)

    except Exception as e:
        print(f"❌ Erro ao acessar blob storage: {e}")
        print("   Verifique se o Azurite está rodando: docker-compose up -d")

if __name__ == "__main__":
    show_blob_storage()