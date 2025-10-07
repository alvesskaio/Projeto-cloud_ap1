from datetime import datetime
from helpers import yymmdd
import requests
import os
import zipfile
from azure_storage import save_file_to_blob
import shutil

PATH_TO_SAVE = "./dados_b3"

def ensure_data_directory():
    """
    Garante que a pasta dados_b3 existe e exibe informações sobre ela
    """
    if not os.path.exists(PATH_TO_SAVE):
        os.makedirs(PATH_TO_SAVE, exist_ok=True)
        print(f"[INFO] Pasta criada: {PATH_TO_SAVE}")
    else:
        print(f"[INFO] Pasta dados_b3 já existe")

    # Mostrar conteúdo atual da pasta
    if os.listdir(PATH_TO_SAVE):
        print(f"[INFO] Conteúdo atual de {PATH_TO_SAVE}:")
        for item in os.listdir(PATH_TO_SAVE):
            item_path = os.path.join(PATH_TO_SAVE, item)
            if os.path.isdir(item_path):
                print(f"  📁 {item}/ (pasta)")
                # Mostrar arquivos dentro da pasta
                try:
                    for subitem in os.listdir(item_path):
                        print(f"    📄 {subitem}")
                except:
                    pass
            else:
                print(f"  📄 {item}")
    else:
        print(f"[INFO] Pasta {PATH_TO_SAVE} está vazia")

def build_url_download(date_to_download):
    return f"https://www.b3.com.br/pesquisapregao/download?filelist=SPRE{date_to_download}.zip"

def try_http_download(url):
    session = requests.Session()
    try:
        print(f"[INFO] Tentando {url}")
        resp = session.get(url, timeout=30)
        if (resp.ok) and resp.content and len(resp.content) > 200:
            if (resp.content[:2] == b"PK"):
                return resp.content, os.path.basename(url)
        print(f"[WARN] Resposta inválida ou arquivo muito pequeno para {url}")
        return None, None
    except requests.RequestException as e:
        print(f"[ERROR] Falha ao acessar {url}: {e}")
        return None, None

def run(date_str=None):
    """
    Executa o processo de extração de dados da B3

    Args:
        date_str: Data no formato YYMMDD (ex: "251007"). Se None, usa data do dia anterior
                 (mais realista, pois dados ficam disponíveis após fechamento do pregão)
    """
    if date_str:
        dt = date_str
    else:
        # Usa data do dia anterior por padrão (dados mais prováveis de estar disponíveis)
        from datetime import timedelta
        yesterday = datetime.now() - timedelta(days=1)
        dt = yymmdd(yesterday)
        print(f"[INFO] Usando data do dia anterior: {dt}")

    url_to_download = build_url_download(dt)

    print(f"[INFO] Extraindo dados para a data: {dt}")
    print(f"[INFO] URL: {url_to_download}")

    # Verificar/criar pasta dados_b3
    ensure_data_directory()    # 1) Download do Zip
    zip_bytes, zip_name = try_http_download(url_to_download)

    if not zip_bytes or not zip_name:
        raise RuntimeError(f"Não foi possível baixar o arquivo de cotações para a data {dt}. "
                         f"Verifique se a data é válida e se os dados estão disponíveis na B3.")

    print(f"[OK] Baixado arquivo de cotaçoes: {zip_name}")

    # 2) Salvar o Zip

    zip_path = f"{PATH_TO_SAVE}/pregao_{dt}.zip"
    with open(zip_path, "wb") as f:
        f.write(zip_bytes)

    print(f"[OK] Zip salvo em {zip_path}")

    # 3) Extrair os arquivos do zip

    try:
        #Extrair a primeira pasta
        print(f"[INFO] Extraindo arquivo principal: {zip_path}")
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(f"{PATH_TO_SAVE}/pregao_{dt}")

        #Extrair a segunda parte
        inner_zip_path = f"{PATH_TO_SAVE}/pregao_{dt}/SPRE{dt}.zip"
        if not os.path.exists(inner_zip_path):
            raise RuntimeError(f"Arquivo interno não encontrado: {inner_zip_path}")

        print(f"[INFO] Extraindo arquivo interno: {inner_zip_path}")
        with zipfile.ZipFile(inner_zip_path, "r") as zf:
            zf.extractall(f"{PATH_TO_SAVE}/SPRE{dt}")

    except zipfile.BadZipFile as e:
        raise RuntimeError(f"Arquivo ZIP corrompido: {e}")
    except Exception as e:
        raise RuntimeError(f"Erro na extração do ZIP: {e}")


    #Subir arquivo para o Blob Storage
    arquivos = [f for f in os.listdir(f"{PATH_TO_SAVE}/SPRE{dt}")]

    if not arquivos:
        raise RuntimeError("Nenhum arquivo encontrado após extração")

    for arquivo in arquivos:
        # Usa o nome real do arquivo com prefixo padronizado para o blob
        blob_name = f"BVBG186_{dt}.xml"
        arquivo_path = f"{PATH_TO_SAVE}/SPRE{dt}/{arquivo}"
        print(f"[INFO] Enviando {arquivo} para blob storage como {blob_name}")
        save_file_to_blob(blob_name, arquivo_path)

        # Apagar arquivo XML local APÓS envio para blob storage
        try:
            os.remove(arquivo_path)
            print(f"[INFO] Arquivo local removido: {arquivo_path}")
        except Exception as e:
            print(f"[WARN] Erro ao remover arquivo local {arquivo_path}: {e}")

    #Manter pasta dados_b3 com os arquivos extraídos - apenas limpar arquivos ZIP temporários
    try:
        # Remove apenas os arquivos ZIP temporários, mantém os XMLs extraídos
        if os.path.exists(zip_path):
            os.remove(zip_path)
            print(f"[INFO] Removido arquivo ZIP temporário: {zip_path}")

        if os.path.exists(inner_zip_path):
            os.remove(inner_zip_path)
            print(f"[INFO] Removido arquivo ZIP interno temporário: {inner_zip_path}")

        # Remove pasta temporária do pregão
        pregao_dir = f"{PATH_TO_SAVE}/pregao_{dt}"
        if os.path.exists(pregao_dir):
            shutil.rmtree(pregao_dir, ignore_errors=True)
            print(f"[INFO] Removida pasta temporária: {pregao_dir}")

        # Remove pasta SPRE vazia (já que XML foi removido)
        spre_dir = f"{PATH_TO_SAVE}/SPRE{dt}"
        if os.path.exists(spre_dir) and not os.listdir(spre_dir):
            os.rmdir(spre_dir)
            print(f"[INFO] Removida pasta vazia: {spre_dir}")

    except Exception as e:
        print(f"[WARN] Erro ao limpar arquivos temporários: {e}")

    print(f"[OK] Arquivos extraídos, enviados para blob storage e limpos do diretório local")


if __name__ == "__main__":
    run()