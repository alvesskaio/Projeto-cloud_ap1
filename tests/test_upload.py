#!/usr/bin/env python3
"""
Script de teste para upload do arquivo XML para o blob storage
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from azure_storage import save_file_to_blob

if __name__ == "__main__":
    xml_file = "dados_b3/BVBG.186.01_BV000471202510060001000062011403620.xml"
    blob_name = "BVBG186_250923.xml"

    if os.path.exists(xml_file):
        print(f"[INFO] Fazendo upload do arquivo {xml_file} para blob storage...")
        try:
            save_file_to_blob(blob_name, xml_file)
            print(f"[OK] Arquivo {blob_name} enviado para blob storage com sucesso")
        except Exception as e:
            print(f"[ERROR] Falha no upload: {e}")
    else:
        print(f"[ERROR] Arquivo {xml_file} não encontrado")