#!/usr/bin/env python3
"""
Script de teste para a função de transformação e carga
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transform_load import transform_and_load

if __name__ == "__main__":
    file_name = "BVBG186_250923.xml"

    print(f"[INFO] Testando transformação e carga do arquivo {file_name}")
    try:
        success = transform_and_load(file_name)
        if success:
            print("[SUCCESS] Teste de transformação e carga concluído com sucesso!")
        else:
            print("[FAILURE] Teste de transformação e carga falhou")
    except Exception as e:
        print(f"[ERROR] Erro durante o teste: {e}")
        import traceback
        traceback.print_exc()