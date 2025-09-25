from datetime import datetime
from helpers import yymmdd
import requests
import os
import zipfile
from azure_storage import save_file_to_blob
import shutil

PATH_TO_SAVE = "./dados_b3"

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
    except requests.RequestException:
        print(f"[ERROR] Falha ao acessar a {url}")
        pass

def run():
    dt = "250923" #yymmdd(datetime.now())
    url_to_download = build_url_download(dt)

    # 1) Download do Zip
    zip_bytes, zip_name = try_http_download(url_to_download)

    if not zip_bytes:
        raise RuntimeError("Não foi possivel baixar o arquivo de cotações")
    
    print(f"[OK] Baixado arquivo de cotaçoes: {zip_name}")

    # 2) Salvar o Zip
    
    #Cria o diretorio que ira salvar o arquivo zip do download
    os.makedirs(PATH_TO_SAVE, exist_ok=True)
    zip_path = f"{PATH_TO_SAVE}/pregao_{dt}.zip"
    with open(zip_path, "wb") as f:
        f.write(zip_bytes)

    print(f"[OK] Zip salvo em {zip_path}")

    # 3) Extrair os arquivos do zip

    #Extrair a primeira pasta
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(f"{PATH_TO_SAVE}/pregao_{dt}")
        

    #Extrair a segunda parte
    with zipfile.ZipFile(f"{PATH_TO_SAVE}/pregao_{dt}/SPRE{dt}.zip", "r") as zf:
        zf.extractall(f"{PATH_TO_SAVE}/SPRE{dt}")


    #Subir arquivo para o Blob Storage
    arquivos = [f for f in os.listdir(f"{PATH_TO_SAVE}/SPRE{dt}")]
    
    for arquivo in arquivos:
        save_file_to_blob(f"BVBG186_{dt}.xml", f"{PATH_TO_SAVE}/SPRE{dt}/{arquivo}")

    #Apagar arquivos desnecessários
    shutil.rmtree(f"{PATH_TO_SAVE}", ignore_errors=True)

    print(f"[OK] Arquivos extraidos do zip com sucesso")
   

if __name__ == "__main__":
    run()