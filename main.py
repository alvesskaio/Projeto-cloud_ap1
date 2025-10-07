#!/usr/bin/env python3
"""
Pipeline Principal para Processamento de Cotações B3

Este script orquestra o pipeline completo:
1. Extração de dados da B3 (download e armazenamento no blob local)
2. Processamento do XML e inserção no PostgreSQL local

Requisitos:
- PostgreSQL executando em localhost:5432
- Azurite (emulador blob storage) executando em localhost:10000
- Dependências Python instaladas (requirements.txt)
"""

import sys
import os
from datetime import datetime
from helpers import yymmdd
from extract import run as extract_run
from transform_load import transform_and_load
from database import DatabaseManager

def check_prerequisites():
    """
    Verifica se os pré-requisitos estão disponíveis
    """
    print("[INFO] Verificando pré-requisitos...")

    # Verificar se conseguimos conectar ao banco
    db = DatabaseManager()
    if not db.connect():
        print("[ERROR] PostgreSQL não está disponível em localhost:5433")
        print("       Execute: docker-compose up -d")
        return False

    print("[OK] PostgreSQL disponível")

    # Verificar se o Azurite está rodando
    try:
        from azure_storage import BlobServiceClient, AZURE_BLOB_CONNECTION
        service = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)
        # Tentativa simples de listar containers
        list(service.list_containers())
        print("[OK] Azurite (Blob Storage) disponível")
    except Exception as e:
        print(f"[ERROR] Azurite não está disponível em localhost:10003")
        print(f"        Erro: {e}")
        print("        Execute: docker-compose up -d")
        return False

    return True

def run_pipeline(date_str=None, file_name=None):
    """
    Executa o pipeline completo de processamento

    Args:
        date_str: Data no formato YYMMDD (ex: "250923"). Se None, usa data atual
        file_name: Nome específico do arquivo. Se None, usa padrão baseado na data
    """

    # Define data e nome do arquivo
    if not date_str:
        date_str = yymmdd(datetime.now())

    if not file_name:
        file_name = f"BVBG186_{date_str}.xml"

    print(f"[INFO] Iniciando pipeline para data: {date_str}")
    print(f"[INFO] Arquivo alvo: {file_name}")
    print("=" * 60)

    try:
        # Etapa 1: Extração (download da B3 e upload para blob local)
        print("\n[ETAPA 1] Extração de dados da B3...")
        try:
            extract_run()
            print("[OK] Extração concluída com sucesso")
        except Exception as e:
            print(f"[ERROR] Falha na extração: {e}")
            return False

        # Etapa 2: Transformação e Carga (processamento XML e inserção no PostgreSQL)
        print("\n[ETAPA 2] Transformação e carga no PostgreSQL...")
        try:
            success = transform_and_load(file_name)
            if success:
                print("[OK] Transformação e carga concluídas com sucesso")
            else:
                print("[ERROR] Falha na transformação e carga")
                return False
        except Exception as e:
            print(f"[ERROR] Falha na transformação e carga: {e}")
            return False

        print("\n" + "=" * 60)
        print("[SUCCESS] Pipeline executado com sucesso!")
        print(f"[INFO] Dados da data {date_str} foram processados e inseridos no PostgreSQL")
        return True

    except KeyboardInterrupt:
        print("\n[INFO] Pipeline interrompido pelo usuário")
        return False
    except Exception as e:
        print(f"\n[ERROR] Erro inesperado no pipeline: {e}")
        return False

def show_help():
    """Exibe ajuda sobre como usar o script"""
    help_text = """
Pipeline de Processamento de Cotações B3

USO:
    python main.py                    # Executa com data atual
    python main.py YYMMDD            # Executa com data específica
    python main.py --check          # Verifica pré-requisitos
    python main.py --help           # Exibe esta ajuda

EXEMPLOS:
    python main.py                   # Processa cotações de hoje
    python main.py 250923           # Processa cotações de 23/09/2025
    python main.py --check          # Verifica se PostgreSQL e Azurite estão rodando

PRÉ-REQUISITOS:
    1. Docker Compose executando PostgreSQL e Azurite:
       docker-compose up -d

    2. Dependências Python instaladas:
       pip install -r requirements.txt

ESTRUTURA DO PIPELINE:
    1. Extração: Download de dados da B3 → Blob Storage local (Azurite)
    2. Transformação: Processamento XML → Dados estruturados
    3. Carga: Inserção no PostgreSQL local
    """
    print(help_text)

def main():
    """Função principal"""

    # Verificar argumentos da linha de comando
    if len(sys.argv) > 1:
        arg = sys.argv[1]

        if arg in ['--help', '-h']:
            show_help()
            return
        elif arg in ['--check', '-c']:
            if check_prerequisites():
                print("[OK] Todos os pré-requisitos estão disponíveis")
                sys.exit(0)
            else:
                print("[ERROR] Alguns pré-requisitos não estão disponíveis")
                sys.exit(1)
        elif arg.isdigit() and len(arg) == 6:
            # Data fornecida no formato YYMMDD
            date_str = arg
        else:
            print(f"[ERROR] Argumento inválido: {arg}")
            print("Use --help para ver opções disponíveis")
            sys.exit(1)
    else:
        date_str = None

    # Verificar pré-requisitos
    if not check_prerequisites():
        print("\n[ERROR] Pré-requisitos não atendidos. Pipeline não pode continuar.")
        print("Use 'python main.py --help' para mais informações")
        sys.exit(1)

    # Executar pipeline
    print("\n" + "=" * 60)
    print("PIPELINE DE PROCESSAMENTO DE COTAÇÕES B3")
    print("=" * 60)

    success = run_pipeline(date_str)

    if success:
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()