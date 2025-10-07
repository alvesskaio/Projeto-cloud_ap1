from datetime import datetime
from helpers import yymmdd
import requests
import os
import zipfile
from azure_storage import save_file_to_blob
import shutil
import time
from tqdm import tqdm

PATH_TO_SAVE = "./dados_b3"

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

def ensure_data_directory():
    """
    Garante que a pasta dados_b3 existe e exibe informações sobre ela
    """
    print(f"[{print_timestamp()}] [INFO] 📁 Verificando diretório de dados...")
    
    if not os.path.exists(PATH_TO_SAVE):
        os.makedirs(PATH_TO_SAVE, exist_ok=True)
        print(f"[{print_timestamp()}] [INFO] ✅ Pasta criada: {PATH_TO_SAVE}")
    else:
        print(f"[{print_timestamp()}] [INFO] ✅ Pasta dados_b3 já existe")

    # Mostrar conteúdo atual da pasta
    try:
        items = os.listdir(PATH_TO_SAVE)
        if items:
            print(f"[{print_timestamp()}] [INFO] 📋 Conteúdo atual de {PATH_TO_SAVE}:")
            total_size = 0
            file_count = 0
            dir_count = 0
            
            for item in items:
                item_path = os.path.join(PATH_TO_SAVE, item)
                if os.path.isdir(item_path):
                    dir_count += 1
                    print(f"  📁 {item}/ (pasta)")
                    # Mostrar arquivos dentro da pasta
                    try:
                        subitems = os.listdir(item_path)
                        for subitem in subitems:
                            subitem_path = os.path.join(item_path, subitem)
                            if os.path.isfile(subitem_path):
                                size = os.path.getsize(subitem_path)
                                total_size += size
                                print(f"    📄 {subitem} ({format_file_size(size)})")
                    except Exception as e:
                        print(f"    ⚠️ Erro ao listar: {e}")
                else:
                    file_count += 1
                    size = os.path.getsize(item_path)
                    total_size += size
                    print(f"  📄 {item} ({format_file_size(size)})")
            
            print(f"[{print_timestamp()}] [INFO] 📊 Resumo: {file_count} arquivos, {dir_count} pastas, {format_file_size(total_size)} total")
        else:
            print(f"[{print_timestamp()}] [INFO] 📂 Pasta {PATH_TO_SAVE} está vazia")
    except Exception as e:
        print(f"[{print_timestamp()}] [WARN] ⚠️ Erro ao listar conteúdo: {e}")

def build_url_download(date_to_download):
    return f"https://www.b3.com.br/pesquisapregao/download?filelist=SPRE{date_to_download}.zip"

def try_http_download(url):
    session = requests.Session()
    download_start = time.time()
    
    try:
        print(f"[{print_timestamp()}] [INFO] 🌐 Iniciando download de {url}")
        print(f"[{print_timestamp()}] [INFO] ⏳ Conectando ao servidor da B3...")
        
        resp = session.get(url, timeout=30, stream=True)
        
        if not resp.ok:
            print(f"[{print_timestamp()}] [ERROR] ❌ Servidor retornou status {resp.status_code}")
            return None, None
            
        # Verificar se temos informação sobre o tamanho do arquivo
        content_length = resp.headers.get('content-length')
        if content_length:
            total_size = int(content_length)
            print(f"[{print_timestamp()}] [INFO] 📦 Tamanho do arquivo: {format_file_size(total_size)}")
        else:
            total_size = None
            print(f"[{print_timestamp()}] [INFO] 📦 Tamanho do arquivo: desconhecido")
        
        # Download com barra de progresso
        content = b""
        downloaded = 0
        chunk_size = 8192
        
        # Configurar barra de progresso
        if total_size:
            progress_bar = tqdm(
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                desc=f"⬇️ Download",
                bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
            )
        else:
            progress_bar = tqdm(
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                desc=f"⬇️ Download",
                bar_format='{desc}: {n_fmt} [{elapsed}, {rate_fmt}]'
            )
        
        try:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    content += chunk
                    downloaded += len(chunk)
                    progress_bar.update(len(chunk))
        finally:
            progress_bar.close()
        
        print()  # Nova linha após o progresso
        download_time = time.time() - download_start
        
        if content and len(content) > 200:
            if content[:2] == b"PK":
                print(f"[{print_timestamp()}] [OK] ✅ Download concluído em {download_time:.2f}s ({format_file_size(len(content))})")
                return content, os.path.basename(url)
            else:
                print(f"[{print_timestamp()}] [ERROR] ❌ Arquivo não é um ZIP válido (cabeçalho: {content[:10]})")
        else:
            print(f"[{print_timestamp()}] [ERROR] ❌ Arquivo muito pequeno ou vazio ({len(content)} bytes)")
        
        return None, None
        
    except requests.Timeout:
        download_time = time.time() - download_start
        print(f"[{print_timestamp()}] [ERROR] ⏰ Timeout após {download_time:.2f}s - servidor demorou para responder")
        return None, None
    except requests.ConnectionError as e:
        download_time = time.time() - download_start
        print(f"[{print_timestamp()}] [ERROR] 🔌 Erro de conexão após {download_time:.2f}s: {e}")
        return None, None
    except requests.RequestException as e:
        download_time = time.time() - download_start
        print(f"[{print_timestamp()}] [ERROR] 🌐 Falha na requisição após {download_time:.2f}s: {e}")
        return None, None

def run(date_str=None):
    """
    Executa o processo de extração de dados da B3

    Args:
        date_str: Data no formato YYMMDD (ex: "251007"). Se None, usa data do dia anterior
                 (mais realista, pois dados ficam disponíveis após fechamento do pregão)
    """
    extraction_start = time.time()
    
    if date_str:
        dt = date_str
    else:
        # Usa data do dia anterior por padrão (dados mais prováveis de estar disponíveis)
        from datetime import timedelta
        yesterday = datetime.now() - timedelta(days=1)
        dt = yymmdd(yesterday)
        print(f"[{print_timestamp()}] [INFO] 📅 Usando data do dia anterior: {dt}")

    url_to_download = build_url_download(dt)

    print(f"[{print_timestamp()}] [INFO] 🎯 Extraindo dados para a data: {dt}")
    print(f"[{print_timestamp()}] [INFO] 🔗 URL: {url_to_download}")

    # Verificar/criar pasta dados_b3
    ensure_data_directory()
    
    # 1) Download do Zip
    print(f"[{print_timestamp()}] [INFO] 📥 Iniciando download do arquivo de cotações...")
    zip_bytes, zip_name = try_http_download(url_to_download)

    if not zip_bytes or not zip_name:
        elapsed = time.time() - extraction_start
        raise RuntimeError(f"❌ Não foi possível baixar o arquivo de cotações para a data {dt} após {elapsed:.2f}s. "
                         f"Verifique se a data é válida e se os dados estão disponíveis na B3.")

    print(f"[{print_timestamp()}] [OK] ✅ Download concluído: {zip_name}")

    # 2) Salvar o Zip
    save_start = time.time()
    zip_path = f"{PATH_TO_SAVE}/pregao_{dt}.zip"
    
    print(f"[{print_timestamp()}] [INFO] 💾 Salvando arquivo ZIP localmente...")
    with open(zip_path, "wb") as f:
        f.write(zip_bytes)
    
    save_time = time.time() - save_start
    zip_size = os.path.getsize(zip_path)
    print(f"[{print_timestamp()}] [OK] ✅ ZIP salvo em {zip_path} ({format_file_size(zip_size)}) em {save_time:.2f}s")

    # 3) Extrair os arquivos do zip
    extraction_start_inner = time.time()
    
    try:
        # Extrair a primeira pasta
        print(f"[{print_timestamp()}] [INFO] 📦 Extraindo arquivo principal: {zip_path}")
        with zipfile.ZipFile(zip_path, "r") as zf:
            file_list = zf.namelist()
            print(f"[{print_timestamp()}] [INFO] 📋 Arquivos no ZIP externo: {len(file_list)} itens")
            for file_name in file_list:
                print(f"[{print_timestamp()}] [INFO]   📄 {file_name}")
            zf.extractall(f"{PATH_TO_SAVE}/pregao_{dt}")

        # Extrair a segunda parte
        inner_zip_path = f"{PATH_TO_SAVE}/pregao_{dt}/SPRE{dt}.zip"
        if not os.path.exists(inner_zip_path):
            raise RuntimeError(f"❌ Arquivo interno não encontrado: {inner_zip_path}")

        inner_zip_size = os.path.getsize(inner_zip_path)
        print(f"[{print_timestamp()}] [INFO] 📦 Extraindo arquivo interno: {inner_zip_path} ({format_file_size(inner_zip_size)})")
        
        with zipfile.ZipFile(inner_zip_path, "r") as zf:
            inner_file_list = zf.namelist()
            print(f"[{print_timestamp()}] [INFO] 📋 Arquivos no ZIP interno: {len(inner_file_list)} itens")
            for file_name in inner_file_list:
                print(f"[{print_timestamp()}] [INFO]   📄 {file_name}")
            zf.extractall(f"{PATH_TO_SAVE}/SPRE{dt}")
        
        extraction_time = time.time() - extraction_start_inner
        print(f"[{print_timestamp()}] [OK] ✅ Extração concluída em {extraction_time:.2f}s")

    except zipfile.BadZipFile as e:
        raise RuntimeError(f"❌ Arquivo ZIP corrompido: {e}")
    except Exception as e:
        raise RuntimeError(f"❌ Erro na extração do ZIP: {e}")


    # Subir arquivo para o Blob Storage
    upload_start = time.time()
    print(f"[{print_timestamp()}] [INFO] ☁️ Preparando upload para Blob Storage...")
    
    arquivos = [f for f in os.listdir(f"{PATH_TO_SAVE}/SPRE{dt}")]

    if not arquivos:
        raise RuntimeError("❌ Nenhum arquivo encontrado após extração")

    print(f"[{print_timestamp()}] [INFO] 📁 Arquivos encontrados para upload: {len(arquivos)}")
    
    for i, arquivo in enumerate(arquivos, 1):
        # Usa o nome real do arquivo com prefixo padronizado para o blob
        blob_name = f"BVBG186_{dt}.xml"
        arquivo_path = f"{PATH_TO_SAVE}/SPRE{dt}/{arquivo}"
        
        arquivo_size = os.path.getsize(arquivo_path)
        print(f"[{print_timestamp()}] [INFO] ☁️ Enviando {arquivo} ({format_file_size(arquivo_size)}) para blob storage como {blob_name} ({i}/{len(arquivos)})")
        
        upload_file_start = time.time()
        save_file_to_blob(blob_name, arquivo_path)
        upload_file_time = time.time() - upload_file_start
        
        print(f"[{print_timestamp()}] [OK] ✅ Upload concluído em {upload_file_time:.2f}s")

        # Apagar arquivo XML local APÓS envio para blob storage
        try:
            os.remove(arquivo_path)
            print(f"[{print_timestamp()}] [INFO] 🗑️ Arquivo local removido: {arquivo_path}")
        except Exception as e:
            print(f"[{print_timestamp()}] [WARN] ⚠️ Erro ao remover arquivo local {arquivo_path}: {e}")
    
    upload_total_time = time.time() - upload_start
    print(f"[{print_timestamp()}] [OK] ✅ Todos os uploads concluídos em {upload_total_time:.2f}s")

    # Limpar arquivos temporários mantendo estrutura organizada
    cleanup_start = time.time()
    print(f"[{print_timestamp()}] [INFO] 🧹 Iniciando limpeza de arquivos temporários...")
    
    files_removed = 0
    dirs_removed = 0
    
    try:
        # Remove apenas os arquivos ZIP temporários, mantém os XMLs extraídos
        if os.path.exists(zip_path):
            zip_size = os.path.getsize(zip_path)
            os.remove(zip_path)
            files_removed += 1
            print(f"[{print_timestamp()}] [INFO] 🗑️ Removido ZIP temporário: {zip_path} ({format_file_size(zip_size)})")

        if os.path.exists(inner_zip_path):
            inner_zip_size = os.path.getsize(inner_zip_path)
            os.remove(inner_zip_path)
            files_removed += 1
            print(f"[{print_timestamp()}] [INFO] 🗑️ Removido ZIP interno temporário: {inner_zip_path} ({format_file_size(inner_zip_size)})")

        # Remove pasta temporária do pregão
        pregao_dir = f"{PATH_TO_SAVE}/pregao_{dt}"
        if os.path.exists(pregao_dir):
            shutil.rmtree(pregao_dir, ignore_errors=True)
            dirs_removed += 1
            print(f"[{print_timestamp()}] [INFO] 🗑️ Removida pasta temporária: {pregao_dir}")

        # Remove pasta SPRE vazia (já que XML foi removido)
        spre_dir = f"{PATH_TO_SAVE}/SPRE{dt}"
        if os.path.exists(spre_dir) and not os.listdir(spre_dir):
            os.rmdir(spre_dir)
            dirs_removed += 1
            print(f"[{print_timestamp()}] [INFO] 🗑️ Removida pasta vazia: {spre_dir}")

    except Exception as e:
        print(f"[{print_timestamp()}] [WARN] ⚠️ Erro ao limpar arquivos temporários: {e}")

    cleanup_time = time.time() - cleanup_start
    total_time = time.time() - extraction_start
    
    print(f"[{print_timestamp()}] [OK] ✅ Limpeza concluída em {cleanup_time:.2f}s ({files_removed} arquivos, {dirs_removed} pastas removidas)")
    print(f"[{print_timestamp()}] [SUCCESS] 🎉 Extração completa concluída em {total_time:.2f}s")
    print(f"[{print_timestamp()}] [INFO] 📊 Resumo: Arquivo baixado → extraído → enviado para blob storage → limpeza local realizada")


if __name__ == "__main__":
    run()