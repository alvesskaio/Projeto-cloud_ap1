#!/usr/bin/env python3
"""
Exemplo de uso do pipeline com diferentes datas
"""

from main import run_pipeline
from helpers import yymmdd
from datetime import datetime, timedelta
import sys

def demo_pipeline():
    """
    Demonstra o uso do pipeline com diferentes cenários
    """
    print("=== DEMO DO PIPELINE DE COTAÇÕES B3 ===\n")

    # Cenário 1: Data atual (pode não ter dados disponíveis)
    print("1. Tentando com data atual...")
    data_atual = yymmdd(datetime.now())
    print(f"   Data atual: {data_atual}")

    try:
        success = run_pipeline(data_atual)
        if success:
            print("   ✅ Sucesso com data atual!")
        else:
            print("   ❌ Falha com data atual")
    except Exception as e:
        print(f"   ❌ Erro: {e}")

    print("\n" + "-"*50 + "\n")

    # Cenário 2: Data de alguns dias atrás (mais provável de ter dados)
    print("2. Tentando com data de dias anteriores...")
    for i in [1, 2, 3, 7]:  # 1, 2, 3 e 7 dias atrás
        data_anterior = yymmdd(datetime.now() - timedelta(days=i))
        print(f"   Tentando {i} dia(s) atrás: {data_anterior}")

        try:
            success = run_pipeline(data_anterior)
            if success:
                print(f"   ✅ Sucesso com data {data_anterior}!")
                break
            else:
                print(f"   ❌ Falha com data {data_anterior}")
        except Exception as e:
            print(f"   ❌ Erro: {e}")

    print("\n" + "="*50)
    print("Demo concluído!")

if __name__ == "__main__":
    demo_pipeline()